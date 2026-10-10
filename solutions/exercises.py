"""Complete reference bindings; teaching core implementations are readable."""
from coordination.envelope import Envelope, decode
from coordination.saga import Coordinator, SagaState, ScriptedPeer
from coordination.store import Principal, Receipt, Store


def lab11_decode(raw: dict[str, object]) -> Envelope:
    return decode(raw)


def lab22_recover(coordinator: Coordinator, peer: ScriptedPeer) -> SagaState:
    return coordinator.forward(peer, fail_at="provision")


def lab27_apply(store: Store, command: Envelope, principal: Principal) -> Receipt:
    return store.execute(command, principal)


def lab33_unknown(coordinator: Coordinator) -> SagaState:
    return coordinator.forward(ScriptedPeer(), unknown_at="charge")
