"""Explicit learner coordinator; scripted decisions are deterministic fixtures."""
from __future__ import annotations

import uuid
from dataclasses import dataclass
from enum import StrEnum

from .envelope import Envelope, Operation, Payload, Refusal
from .store import Principal, Receipt, Store


class SagaState(StrEnum):
    NEW = "new"
    RUNNING = "running"
    COMPLETED = "completed"
    COMPENSATING = "compensating"
    COMPENSATED = "compensated"
    RECONCILING = "reconciling"
    RECOVERY_REQUIRED = "recovery_required"
    FORWARD_RECOVERY = "forward_recovery"


class KnownFailure(RuntimeError):
    """The fixture guarantees the selected operation never took effect."""


class UnknownOutcome(RuntimeError):
    """Outcome cannot be established; neither retry nor compensate blindly."""


@dataclass(frozen=True, slots=True)
class ScriptedPeer:
    """Recorded decision variation, never an LLM or MCP/A2A execution."""
    quantity: int = 2
    cents: int = 2400

    def decide(self, order: str) -> Payload:
        return Payload(order, self.quantity, self.cents)


class Coordinator:
    """One learner Mediator with visible State/Command/checkpoint boundaries.

    Local effect completion is durable. Unknown provider outcomes are modeled
    as blocked reconciliation, with no claim of distributed atomicity.
    """
    def __init__(self, store: Store, principal: Principal, run: str,
                 *, fence: int | None = None) -> None:
        self.store, self.principal, self.run = store, principal, run
        self.fence = fence if fence is not None else store.acquire(principal, run)

    def command(self, operation: Operation, body: Payload,
                *, key: str | None = None) -> Envelope:
        return Envelope(str(uuid.uuid4()), self.run, self.run, None,
                        self.principal.tenant, self.principal.owner, self.fence,
                        key or f"{self.run}:{operation.value}", operation, body)

    def checkpoint(self, state: SagaState, detail: str = "", *,
                   pending_operation: Operation | None = None) -> None:
        self.store.record_state(self.principal, self.run, self.fence,
                                state.value, detail, pending_operation)

    def forward(self, peer: ScriptedPeer, *, fail_at: str = "",
                unknown_at: str = "", cancel_at: str = "",
                compensation_failure: str = "") -> SagaState:
        state = SagaState(self.store.state(self.principal.tenant, self.run))
        if state == SagaState.COMPLETED:
            return state
        if state == SagaState.COMPENSATING:
            return self.compensate(fail_at=compensation_failure)
        if state in {SagaState.RECONCILING, SagaState.RECOVERY_REQUIRED,
                     SagaState.FORWARD_RECOVERY, SagaState.COMPENSATED}:
            raise Refusal("explicit reconciliation/recovery required")
        receipts = self.store.receipts(self.principal.tenant, self.run)
        # Reuse the first realized decision after restart; do not ask a planner
        # to regenerate the action that was already committed.
        payload = receipts[0].payload if receipts else peer.decide(self.run)
        self.checkpoint(SagaState.RUNNING)
        try:
            for operation in (Operation.RESERVE, Operation.CHARGE,
                              Operation.PROVISION, Operation.NOTIFY):
                if any(r.operation == operation for r in receipts):
                    continue
                if operation.value == unknown_at:
                    self.checkpoint(SagaState.RECONCILING,
                                    f"outcome unknown for {operation.value}; "
                                    f"query key {self.run}:{operation.value}",
                                    pending_operation=operation)
                    return SagaState.RECONCILING
                if operation.value in {fail_at, cancel_at}:
                    raise KnownFailure(f"known no-effect: {operation.value}")
                result = self.store.execute(self.command(operation, payload),
                                            self.principal)
                receipts += (result,)
            self.checkpoint(SagaState.COMPLETED)
            return SagaState.COMPLETED
        except KnownFailure:
            return self.compensate(fail_at=compensation_failure)

    def compensate(self, *, fail_at: str = "") -> SagaState:
        if self.store.state(self.principal.tenant, self.run) == "reconciling":
            raise Refusal("resolve the unknown effect before compensation")
        receipts = self.store.receipts(self.principal.tenant, self.run)
        if any(r.operation == Operation.NOTIFY for r in receipts):
            # The learner contract treats sent notifications as a pivot.
            self.checkpoint(SagaState.FORWARD_RECOVERY,
                            "notification observed; human/forward recovery")
            return SagaState.FORWARD_RECOVERY
        self.checkpoint(SagaState.COMPENSATING)
        reverse = {Operation.PROVISION: Operation.DEPROVISION,
                   Operation.CHARGE: Operation.REFUND,
                   Operation.RESERVE: Operation.RELEASE}
        for receipt in reversed(receipts):
            operation = reverse.get(receipt.operation)
            if operation is None:
                continue
            key = f"{receipt.key}:compensate"
            if any(r.key == key for r in receipts):
                continue
            if operation.value == fail_at:
                self.checkpoint(SagaState.RECOVERY_REQUIRED,
                                f"compensation failed definitively: {operation}")
                return SagaState.RECOVERY_REQUIRED
            try:
                self.store.execute(self.command(operation, receipt.payload,
                                               key=key), self.principal,
                                   original_key=receipt.key)
            except Exception as exc:
                self.checkpoint(SagaState.RECOVERY_REQUIRED,
                                f"compensation refused: {type(exc).__name__}")
                return SagaState.RECOVERY_REQUIRED
        self.checkpoint(SagaState.COMPENSATED)
        return SagaState.COMPENSATED

    def reconcile_known_absence(self, operation: Operation,
                                *, definitive_no_effect: bool) -> SagaState:
        """A fixture-only resolution requiring explicit authoritative absence.

        A real provider adapter must query the original key, verify tenant,
        operation and effect receipt, and persist that evidence first. A 404
        alone is not authoritative absence if provider work can still commit.
        """
        if self.store.state(self.principal.tenant, self.run) != "reconciling":
            raise Refusal("not awaiting reconciliation")
        if operation != self.store.pending_unknown(self.principal.tenant, self.run):
            raise Refusal("resolution does not match the pending operation")
        if not definitive_no_effect:
            raise UnknownOutcome("reconciliation remains unknown")
        self.checkpoint(SagaState.RUNNING,
                        f"fixture guarantees {operation.value} did not happen")
        return self.forward(ScriptedPeer())
