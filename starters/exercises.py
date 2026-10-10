"""Intentional learner blanks; never a production implementation or solution."""
from __future__ import annotations

from coordination.envelope import Envelope, Operation
from coordination.saga import Coordinator, SagaState, ScriptedPeer
from coordination.store import Principal, Receipt, Store


def lab11_decode(raw: dict[str, object]) -> Envelope:
    """Implement strict v1/v2 validation and one-way normalization."""
    raise NotImplementedError("LEARNER BLANK: implement envelope decoder")


def lab22_recover(coordinator: Coordinator, peer: ScriptedPeer) -> SagaState:
    """Implement realized-action journaling and compensation on known failure."""
    raise NotImplementedError("LEARNER BLANK: implement saga recovery")


def lab27_apply(store: Store, command: Envelope, principal: Principal) -> Receipt:
    """Implement one local transaction for effect/inbox/journal/outbox."""
    raise NotImplementedError("LEARNER BLANK: implement durable application")


def lab33_unknown(coordinator: Coordinator) -> SagaState:
    """Stop a charge timeout in reconciliation, without another charge."""
    raise NotImplementedError("LEARNER BLANK: implement unknown-outcome gate")
