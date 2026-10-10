"""Learner SQLite transaction: local effect, inbox, journal and outbox together.

This transaction does not include HTTP, a payment provider, or a broker commit.
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from .envelope import Envelope, Operation, Payload, Refusal


class CrashAfterCommit(RuntimeError):
    """Commit succeeded; response delivery failed. Retry the same key."""


@dataclass(frozen=True, slots=True)
class Principal:
    tenant: str
    owner: str


@dataclass(frozen=True, slots=True)
class Receipt:
    key: str
    operation: Operation
    payload: Payload
    event_id: str

    def wire(self) -> dict[str, object]:
        return {"key": self.key, "operation": self.operation.value,
                "payload": self.payload.wire(), "event_id": self.event_id}


def receipt_from_wire(value: dict[str, object]) -> Receipt:
    """Decode trusted local records through typed payload validation."""
    body = value["payload"]
    if not isinstance(body, dict):
        raise Refusal("invalid stored receipt")
    return Receipt(str(value["key"]), Operation(value["operation"]),
                   Payload(**body), str(value["event_id"]))  # type: ignore[arg-type]


class Store:
    """Named operations over a caller-owned learner database."""

    def __init__(self, path: Path) -> None:
        self.db = sqlite3.connect(path, timeout=2.0, isolation_level=None)
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS ownership (
          tenant TEXT NOT NULL, run TEXT NOT NULL, owner TEXT NOT NULL,
          fence INTEGER NOT NULL, PRIMARY KEY(tenant,run));
        CREATE TABLE IF NOT EXISTS inbox (
          tenant TEXT NOT NULL, key TEXT NOT NULL, intent TEXT NOT NULL,
          result TEXT NOT NULL, PRIMARY KEY(tenant,key));
        CREATE TABLE IF NOT EXISTS journal (
          seq INTEGER PRIMARY KEY AUTOINCREMENT, tenant TEXT NOT NULL,
          run TEXT NOT NULL, event_id TEXT NOT NULL UNIQUE,
          kind TEXT NOT NULL, data TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS outbox (
          event_id TEXT PRIMARY KEY REFERENCES journal(event_id),
          payload TEXT NOT NULL, sent INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS counters (
          tenant TEXT NOT NULL, run TEXT NOT NULL, reserved INTEGER NOT NULL,
          charged INTEGER NOT NULL, provisioned INTEGER NOT NULL,
          notified INTEGER NOT NULL, PRIMARY KEY(tenant,run));
        CREATE TABLE IF NOT EXISTS reversals (
          tenant TEXT NOT NULL, original_key TEXT NOT NULL,
          reverse_key TEXT NOT NULL, PRIMARY KEY(tenant,original_key));
        """)

    def close(self) -> None:
        """Close the explicit database connection."""
        self.db.close()

    def acquire(self, principal: Principal, run: str) -> int:
        """Issue a new monotonic fence in a local transaction."""
        self.db.execute("BEGIN IMMEDIATE")
        try:
            row = self.db.execute(
                "SELECT fence FROM ownership WHERE tenant=? AND run=?",
                (principal.tenant, run)).fetchone()
            fence = int(row[0]) + 1 if row else 1
            self.db.execute("INSERT INTO ownership VALUES(?,?,?,?) "
                            "ON CONFLICT(tenant,run) DO UPDATE SET "
                            "owner=excluded.owner,fence=excluded.fence",
                            (principal.tenant, run, principal.owner, fence))
            self.db.execute("COMMIT")
            return fence
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def execute(self, command: Envelope, principal: Principal,
                *, fault: str = "", original_key: str | None = None) -> Receipt:
        """Atomically apply a local effect or return the exact prior receipt."""
        if (command.tenant, command.owner) != (principal.tenant, principal.owner):
            raise Refusal("principal mismatch; correlation is not authority")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            owner = self.db.execute(
                "SELECT owner,fence FROM ownership WHERE tenant=? AND run=?",
                (command.tenant, command.run_id)).fetchone()
            if owner != (command.owner, command.fence):
                raise Refusal("stale fence or owner")
            old = self.db.execute("SELECT intent,result FROM inbox "
                                  "WHERE tenant=? AND key=?",
                                  (command.tenant, command.key)).fetchone()
            if old:
                if old[0] != command.intent():
                    raise Refusal("idempotency key reused with changed intent")
                if command.operation in {Operation.RELEASE, Operation.REFUND,
                                         Operation.DEPROVISION}:
                    reverse = self.db.execute("SELECT original_key FROM reversals "
                                              "WHERE tenant=? AND reverse_key=?",
                                              (command.tenant, command.key)).fetchone()
                    if reverse != (original_key,):
                        raise Refusal("retry changed the original compensation key")
                self.db.execute("COMMIT")
                return receipt_from_wire(json.loads(old[1]))
            receipt = Receipt(command.key, command.operation, command.payload,
                              f"{command.tenant}:{command.key}")
            row = self.db.execute("SELECT reserved,charged,provisioned,notified "
                                  "FROM counters WHERE tenant=? AND run=?",
                                  (command.tenant, command.run_id)).fetchone()
            counts = list(row or (0, 0, 0, 0))
            index, sign = {
                Operation.RESERVE: (0, 1), Operation.RELEASE: (0, -1),
                Operation.CHARGE: (1, 1), Operation.REFUND: (1, -1),
                Operation.PROVISION: (2, 1), Operation.DEPROVISION: (2, -1),
                Operation.NOTIFY: (3, 1)}[command.operation]
            amount = (command.payload.quantity if index in (0, 2)
                      else command.payload.cents if index == 1 else 1)
            if sign == -1:
                if counts[3] > 0:
                    raise Refusal("observed notification requires forward recovery")
                if original_key is None:
                    raise Refusal("compensation needs original receipt key")
                original = self.db.execute("SELECT result FROM inbox "
                                           "WHERE tenant=? AND key=?",
                                           (command.tenant, original_key)).fetchone()
                if not original:
                    raise Refusal("original effect is unknown")
                prior = receipt_from_wire(json.loads(original[0]))
                original_run = self.db.execute("SELECT run FROM journal "
                                               "WHERE tenant=? AND event_id=?",
                                               (command.tenant, prior.event_id)).fetchone()
                if original_run != (command.run_id,):
                    raise Refusal("compensation cannot cross workflow runs")
                pairs = {Operation.RELEASE: Operation.RESERVE,
                         Operation.REFUND: Operation.CHARGE,
                         Operation.DEPROVISION: Operation.PROVISION}
                if prior.operation != pairs[command.operation] or (
                    prior.payload != command.payload):
                    raise Refusal("compensation differs from realized receipt")
                self.db.execute("INSERT INTO reversals VALUES(?,?,?)",
                                (command.tenant, original_key, command.key))
            if command.operation == Operation.REFUND and counts[2] > 0:
                raise Refusal("deprovision before refund under learner policy")
            if command.operation == Operation.RELEASE and (counts[1] > 0 or counts[2] > 0):
                raise Refusal("clear dependent effects before releasing reservation")
            if command.operation == Operation.CHARGE and counts[0] < command.payload.quantity:
                raise Refusal("charge requires the declared reservation")
            counts[index] += sign * amount
            if min(counts) < 0 or counts[0] > 100:
                raise Refusal("local inventory/balance invariant violated")
            if command.operation == Operation.PROVISION and (
                counts[0] < command.payload.quantity or
                counts[1] < command.payload.cents):
                raise Refusal("provision requires reservation and payment")
            if command.operation == Operation.NOTIFY and counts[2] == 0:
                raise Refusal("notification requires provision")
            self.db.execute("INSERT INTO counters VALUES(?,?,?,?,?,?) "
                            "ON CONFLICT(tenant,run) DO UPDATE SET "
                            "reserved=excluded.reserved,charged=excluded.charged,"
                            "provisioned=excluded.provisioned,notified=excluded.notified",
                            (command.tenant, command.run_id, *counts))
            body = json.dumps(receipt.wire(), sort_keys=True)
            self.db.execute("INSERT INTO inbox VALUES(?,?,?,?)",
                            (command.tenant, command.key, command.intent(), body))
            self.db.execute("INSERT INTO journal(tenant,run,event_id,kind,data) "
                            "VALUES(?,?,?,?,?)", (command.tenant, command.run_id,
                            receipt.event_id, "effect", body))
            self.db.execute("INSERT INTO outbox(event_id,payload) VALUES(?,?)",
                            (receipt.event_id, body))
            if fault == "before_commit":
                raise RuntimeError("injected crash before commit")
            self.db.execute("COMMIT")
        except BaseException:
            if self.db.in_transaction:
                self.db.execute("ROLLBACK")
            raise
        if fault == "after_commit":
            raise CrashAfterCommit("injected lost response after commit")
        return receipt

    def counts(self, tenant: str, run: str) -> tuple[int, int, int, int]:
        """Read the local projection (reserved,charged,provisioned,notified)."""
        row = self.db.execute("SELECT reserved,charged,provisioned,notified "
                              "FROM counters WHERE tenant=? AND run=?",
                              (tenant, run)).fetchone()
        return tuple(row or (0, 0, 0, 0))  # type: ignore[return-value]

    def receipts(self, tenant: str, run: str) -> tuple[Receipt, ...]:
        """Read realized receipts in durable event sequence."""
        rows = self.db.execute("SELECT data FROM journal WHERE tenant=? "
                               "AND run=? AND kind='effect' ORDER BY seq",
                               (tenant, run)).fetchall()
        return tuple(receipt_from_wire(json.loads(row[0])) for row in rows)

    def record_state(self, principal: Principal, run: str, fence: int,
                     state: str, detail: str = "",
                     pending_operation: Operation | None = None) -> None:
        """Append a state observation; never rewrite prior history."""
        import uuid
        self.db.execute("BEGIN IMMEDIATE")
        try:
            owner = self.db.execute("SELECT owner,fence FROM ownership "
                                    "WHERE tenant=? AND run=?",
                                    (principal.tenant, run)).fetchone()
            if owner != (principal.owner, fence):
                raise Refusal("stale checkpoint owner or fence")
            allowed = {"new", "running", "completed", "compensating", "compensated",
                       "reconciling", "recovery_required", "forward_recovery"}
            if state not in allowed:
                raise Refusal("unknown workflow state")
            if state == "reconciling" and pending_operation not in {
                Operation.RESERVE, Operation.CHARGE, Operation.PROVISION, Operation.NOTIFY
            }:
                raise Refusal("reconciliation requires a typed pending operation")
            if state == "completed" and self.counts(principal.tenant, run)[3] < 1:
                raise Refusal("completion requires the declared pivot receipt")
            if state == "compensated" and any(self.counts(principal.tenant, run)):
                raise Refusal("unresolved effects cannot be called compensated")
            self.db.execute("INSERT INTO journal(tenant,run,event_id,kind,data) "
                            "VALUES(?,?,?,?,?)", (principal.tenant, run,
                            str(uuid.uuid4()), "state", json.dumps({
                            "state": state, "detail": detail,
                            "pending_operation": (pending_operation.value
                                                  if pending_operation else None)})))
            self.db.execute("COMMIT")
        except BaseException:
            if self.db.in_transaction:
                self.db.execute("ROLLBACK")
            raise

    def state(self, tenant: str, run: str) -> str:
        """Read the latest checkpoint bound to this tenant and run."""
        row = self.db.execute("SELECT data FROM journal WHERE tenant=? AND run=? "
                              "AND kind='state' ORDER BY seq DESC LIMIT 1",
                              (tenant, run)).fetchone()
        return json.loads(row[0])["state"] if row else "new"

    def pending(self) -> tuple[tuple[str, str], ...]:
        """Relay candidates. A crash before sent marking duplicates delivery."""
        return tuple(self.db.execute("SELECT event_id,payload FROM outbox "
                                     "WHERE sent=0 ORDER BY event_id"))

    def pending_unknown(self, tenant: str, run: str) -> Operation:
        """Read the exact operation awaiting authoritative reconciliation."""
        row = self.db.execute("SELECT data FROM journal WHERE tenant=? AND run=? "
                              "AND kind='state' ORDER BY seq DESC LIMIT 1",
                              (tenant, run)).fetchone()
        data = json.loads(row[0]) if row else {}
        if data.get("state") != "reconciling":
            raise Refusal("no pending unknown outcome")
        try:
            return Operation(data["pending_operation"])
        except (KeyError, ValueError, TypeError) as exc:
            raise Refusal("invalid pending reconciliation record") from exc

    def mark_sent(self, event_id: str) -> None:
        """Call only after the downstream consumer's receipt is confirmed."""
        self.db.execute("UPDATE outbox SET sent=1 WHERE event_id=?", (event_id,))

    def replay(self, tenant: str, run: str) -> tuple[int, int, int, int]:
        """Pure projection from validated effects; replay performs no effects."""
        counts = [0, 0, 0, 0]
        mapping = {Operation.RESERVE: (0, 1), Operation.RELEASE: (0, -1),
                   Operation.CHARGE: (1, 1), Operation.REFUND: (1, -1),
                   Operation.PROVISION: (2, 1), Operation.DEPROVISION: (2, -1),
                   Operation.NOTIFY: (3, 1)}
        for item in self.receipts(tenant, run):
            idx, sign = mapping[item.operation]
            amount = (item.payload.quantity if idx in (0, 2)
                      else item.payload.cents if idx == 1 else 1)
            counts[idx] += sign * amount
            if min(counts) < 0:
                raise Refusal("corrupt event sequence")
        return tuple(counts)  # type: ignore[return-value]
