"""Complete deterministic references for extra exercises; not provider evidence."""
from __future__ import annotations

import asyncio
import json
import sqlite3
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from .envelope import Refusal
from .store import Store, receipt_from_wire


class Consumer:
    """E1: own local transaction for inbox and downstream accepted-effect count."""
    def __init__(self, path: Path) -> None:
        self.db = sqlite3.connect(path, isolation_level=None)
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS deliveries (
          event_id TEXT PRIMARY KEY, body TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS totals (
          singleton INTEGER PRIMARY KEY CHECK(singleton=1), accepted INTEGER NOT NULL);
        INSERT OR IGNORE INTO totals VALUES(1,0);
        """)

    def accept(self, event_id: str, body: str) -> str:
        value = json.loads(body)
        if not isinstance(value, dict) or set(value) != {
            "key", "operation", "payload", "event_id"
        }:
            raise Refusal("invalid downstream receipt schema")
        receipt = receipt_from_wire(value)
        if receipt.event_id != event_id:
            raise Refusal("event identity mismatch")
        canonical = json.dumps(value, sort_keys=True, separators=(",", ":"))
        self.db.execute("BEGIN IMMEDIATE")
        try:
            old = self.db.execute("SELECT body FROM deliveries WHERE event_id=?",
                                  (event_id,)).fetchone()
            if old and old[0] != canonical:
                raise Refusal("event id reused with changed body")
            if not old:
                self.db.execute("INSERT INTO deliveries VALUES(?,?)",
                                (event_id, canonical))
                self.db.execute("UPDATE totals SET accepted=accepted+1 WHERE singleton=1")
            self.db.execute("COMMIT")
            return event_id
        except BaseException:
            if self.db.in_transaction:
                self.db.execute("ROLLBACK")
            raise

    def count(self) -> int:
        return int(self.db.execute("SELECT accepted FROM totals").fetchone()[0])

    def close(self) -> None:
        self.db.close()


def relay_once(store: Store, consumer: Consumer, *, lose_ack: bool = False) -> int:
    """E1: loss after consumer commit leaves the producer event pending."""
    delivered = 0
    for event_id, body in store.pending():
        confirmed = consumer.accept(event_id, body)
        if lose_ack:
            raise RuntimeError("authored failure after consume, before sent marking")
        if confirmed != event_id:
            raise Refusal("ack identity mismatch")
        store.mark_sent(event_id)
        delivered += 1
    return delivered


async def three_ordered_locks() -> tuple[int, int]:
    """E2: callers provide requests in different orders; acquisition sorts IDs."""
    locks = {i: asyncio.Lock() for i in range(3)}

    async def worker(order: tuple[int, ...]) -> int:
        acquired: list[asyncio.Lock] = []
        try:
            async with asyncio.timeout(1):
                for resource in sorted(set(order)):
                    await locks[resource].acquire()
                    acquired.append(locks[resource])
                await asyncio.sleep(0)
                return len(acquired)
        finally:
            for lock in reversed(acquired):
                lock.release()

    return tuple(await asyncio.gather(worker((2, 0, 1)), worker((1, 2, 0))))


@dataclass(frozen=True, slots=True)
class Ballot:
    voter: str
    revision: int
    choice: str


def majority(ballots: tuple[Ballot, ...], voters: frozenset[str],
             revision: int) -> str | None:
    """E3: fixed-membership deterministic agreement, not Byzantine consensus."""
    current: dict[str, str] = {}
    for ballot in ballots:
        if ballot.voter not in voters or ballot.revision != revision:
            raise Refusal("unknown voter or stale proposal revision")
        if ballot.voter in current:
            raise Refusal("a voter may submit only one ballot per revision")
        current[ballot.voter] = ballot.choice
    for choice in set(current.values()):
        if sum(v == choice for v in current.values()) > len(voters) // 2:
            return choice
    return None


def may_archive(age_days: int, *, retry_days: int, delayed_delivery_days: int,
                unresolved: bool, archive_queryable: bool) -> bool:
    """E4: a deliberately conservative contract; archival keeps query evidence."""
    if min(age_days, retry_days, delayed_delivery_days) < 0:
        raise Refusal("durations must be non-negative")
    return (age_days > retry_days + delayed_delivery_days and not unresolved
            and archive_queryable)


class QueryState(StrEnum):
    APPLIED = "applied"
    DEFINITIVELY_ABSENT = "definitively_absent"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class QueryEvidence:
    """E6: require authentic provider evidence before using this data in effects."""
    tenant: str
    key: str
    state: QueryState
    receipt_id: str | None = None
    no_later_commit: bool = False

    def validate(self, expected_tenant: str, expected_key: str) -> None:
        if (self.tenant, self.key) != (expected_tenant, expected_key):
            raise Refusal("query identity mismatch")
        if self.state == QueryState.APPLIED and not self.receipt_id:
            raise Refusal("applied requires an authoritative receipt")
        if self.state == QueryState.DEFINITIVELY_ABSENT and not self.no_later_commit:
            raise Refusal("absence without a no-later-commit contract remains unknown")
