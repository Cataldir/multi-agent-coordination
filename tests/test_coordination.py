"""Authored deterministic tests. Execution is held in the authoring workspace."""
from __future__ import annotations

import asyncio
import itertools
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from coordination.deadlock import ordered, reproduce
from coordination.envelope import Envelope, Operation, Payload, Refusal, decode
from coordination.saga import Coordinator, SagaState, ScriptedPeer, UnknownOutcome
from coordination.store import CrashAfterCommit, Principal, Store


class CoordinationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "learner.sqlite"
        self.store = Store(self.path)
        self.principal = Principal("teaching", "owner")
        self.coordinator = Coordinator(self.store, self.principal, "order-1")

    def tearDown(self) -> None:
        self.store.close()
        self.directory.cleanup()

    def command(self) -> Envelope:
        return self.coordinator.command(Operation.RESERVE, Payload("order-1", 2, 2400))

    def test_duplicate_attempts_have_one_local_effect(self) -> None:
        command = self.command()
        first = self.store.execute(command, self.principal)
        again = self.store.execute(replace(command, message_id="attempt-2"),
                                   self.principal)
        self.assertEqual(first, again)
        self.assertEqual(self.store.counts("teaching", "order-1"), (2, 0, 0, 0))
        self.assertEqual(len(self.store.pending()), 1)

    def test_changed_intent_conflicts(self) -> None:
        command = self.command()
        self.store.execute(command, self.principal)
        with self.assertRaises(Refusal):
            self.store.execute(replace(command, payload=Payload("order-1", 3, 2400)),
                               self.principal)

    def test_before_commit_rolls_back_all_four_surfaces(self) -> None:
        with self.assertRaises(RuntimeError):
            self.store.execute(self.command(), self.principal, fault="before_commit")
        self.assertEqual(self.store.counts("teaching", "order-1"), (0, 0, 0, 0))
        self.assertEqual(self.store.receipts("teaching", "order-1"), ())
        self.assertEqual(self.store.pending(), ())
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM inbox").fetchone()[0], 0)

    def test_after_commit_restart_returns_receipt(self) -> None:
        command = self.command()
        with self.assertRaises(CrashAfterCommit):
            self.store.execute(command, self.principal, fault="after_commit")
        self.store.close()
        self.store = Store(self.path)
        result = self.store.execute(command, self.principal)
        self.assertEqual(result.payload.quantity, 2)
        self.assertEqual(len(self.store.pending()), 1)

    def test_stale_fence_is_rejected_even_for_retry(self) -> None:
        command = self.command()
        self.store.acquire(Principal("teaching", "new-owner"), "order-1")
        with self.assertRaises(Refusal):
            self.store.execute(command, self.principal)

    def test_cross_tenant_refused(self) -> None:
        with self.assertRaises(Refusal):
            self.store.execute(self.command(), Principal("other", "owner"))

    def test_stale_checkpoint_cannot_change_workflow_state(self) -> None:
        self.store.acquire(Principal("teaching", "new-owner"), "order-1")
        with self.assertRaises(Refusal):
            self.coordinator.checkpoint(SagaState.COMPLETED)
        self.assertEqual(self.store.state("teaching", "order-1"), "new")

    def test_false_completion_is_refused(self) -> None:
        with self.assertRaises(Refusal):
            self.coordinator.checkpoint(SagaState.COMPLETED)

    def test_wrong_realized_reverse_and_dependency_are_refused(self) -> None:
        payload = Payload("order-1", 2, 2400)
        for operation in (Operation.RESERVE, Operation.CHARGE, Operation.PROVISION):
            self.store.execute(self.coordinator.command(operation, payload), self.principal)
        with self.assertRaises(Refusal):
            self.store.execute(self.coordinator.command(Operation.REFUND, payload),
                               self.principal, original_key="order-1:charge")
        with self.assertRaises(Refusal):
            wrong = self.coordinator.command(Operation.DEPROVISION, Payload("order-1", 3, 2400))
            self.store.execute(wrong, self.principal, original_key="order-1:provision")
        self.assertEqual(self.store.counts("teaching", "order-1"), (2, 2400, 2, 0))

    def test_success_and_replay(self) -> None:
        self.assertEqual(self.coordinator.forward(ScriptedPeer()), SagaState.COMPLETED)
        self.assertEqual(self.store.counts("teaching", "order-1"), (2, 2400, 2, 1))
        self.assertEqual(self.store.replay("teaching", "order-1"), (2, 2400, 2, 1))
        self.assertEqual(self.coordinator.forward(ScriptedPeer(9, 9000)), SagaState.COMPLETED)
        self.assertEqual(len(self.store.receipts("teaching", "order-1")), 4)

    def test_failure_each_step_compensates_realized_values(self) -> None:
        for index, operation in enumerate(("reserve", "charge", "provision", "notify")):
            with self.subTest(operation=operation):
                run = f"failure-{index}"
                c = Coordinator(self.store, self.principal, run)
                self.assertEqual(c.forward(ScriptedPeer(3, 3100), fail_at=operation),
                                 SagaState.COMPENSATED)
                self.assertEqual(self.store.counts("teaching", run), (0, 0, 0, 0))

    def test_compensation_failure_stays_visible_and_can_recover(self) -> None:
        result = self.coordinator.forward(ScriptedPeer(3, 3100), fail_at="provision",
                                          compensation_failure="refund")
        self.assertEqual(result, SagaState.RECOVERY_REQUIRED)
        self.assertEqual(self.store.counts("teaching", "order-1"), (3, 3100, 0, 0))
        self.assertEqual(self.coordinator.compensate(), SagaState.COMPENSATED)
        self.assertEqual(self.coordinator.compensate(), SagaState.COMPENSATED)
        self.assertEqual(self.store.counts("teaching", "order-1"), (0, 0, 0, 0))

    def test_unknown_outcome_neither_retries_nor_compensates(self) -> None:
        self.assertEqual(self.coordinator.forward(ScriptedPeer(), unknown_at="charge"),
                         SagaState.RECONCILING)
        self.assertEqual(self.store.counts("teaching", "order-1"), (2, 0, 0, 0))
        with self.assertRaises(Refusal):
            self.coordinator.forward(ScriptedPeer())
        with self.assertRaises(Refusal):
            self.coordinator.compensate()
        with self.assertRaises(Refusal):
            self.coordinator.reconcile_known_absence(Operation.PROVISION,
                                                     definitive_no_effect=True)
        with self.assertRaises(UnknownOutcome):
            self.coordinator.reconcile_known_absence(Operation.CHARGE,
                                                     definitive_no_effect=False)

    def test_fixture_authoritative_absence_allows_forward_recovery(self) -> None:
        self.coordinator.forward(ScriptedPeer(3, 3100), unknown_at="charge")
        self.assertEqual(self.coordinator.reconcile_known_absence(
            Operation.CHARGE, definitive_no_effect=True), SagaState.COMPLETED)
        self.assertEqual(self.store.counts("teaching", "order-1"), (3, 3100, 3, 1))

    def test_cancel_before_pivot_compensates(self) -> None:
        self.assertEqual(self.coordinator.forward(ScriptedPeer(), cancel_at="provision"),
                         SagaState.COMPENSATED)

    def test_interrupted_compensation_resumes_in_reverse_direction(self) -> None:
        payload = Payload("order-1", 2, 2400)
        for operation in (Operation.RESERVE, Operation.CHARGE):
            self.store.execute(self.coordinator.command(operation, payload), self.principal)
        self.coordinator.checkpoint(SagaState.COMPENSATING)
        self.store.close()
        self.store = Store(self.path)
        c = Coordinator(self.store, self.principal, "order-1")
        self.assertEqual(c.forward(ScriptedPeer(9, 9000)), SagaState.COMPENSATED)
        self.assertEqual(self.store.counts("teaching", "order-1"), (0, 0, 0, 0))

    def test_compensation_retry_cannot_change_original_key(self) -> None:
        reserve = self.command()
        self.store.execute(reserve, self.principal)
        release = self.coordinator.command(Operation.RELEASE, reserve.payload,
                                           key="order-1:reserve:compensate")
        self.store.execute(release, self.principal, original_key=reserve.key)
        with self.assertRaises(Refusal):
            self.store.execute(release, self.principal, original_key="another-reservation")

    def test_after_pivot_requires_forward_recovery(self) -> None:
        self.coordinator.forward(ScriptedPeer())
        self.assertEqual(self.coordinator.compensate(), SagaState.FORWARD_RECOVERY)
        self.assertEqual(self.store.counts("teaching", "order-1"), (2, 2400, 2, 1))

    def test_resume_uses_realized_decision(self) -> None:
        self.store.execute(self.command(), self.principal)
        self.assertEqual(self.coordinator.forward(ScriptedPeer(9, 9000)), SagaState.COMPLETED)
        self.assertEqual(self.store.counts("teaching", "order-1"), (2, 2400, 2, 1))

    def test_all_duplicate_interleavings_preserve_projection(self) -> None:
        # Exhaustive small schedule set, not universal proof/property framework.
        for index, sequence in enumerate(itertools.permutations((0, 0, 1))):
            run = f"permutation-{index}"
            c = Coordinator(self.store, self.principal, run)
            cmd = c.command(Operation.RESERVE, Payload(run, 2, 2400))
            for attempt in sequence:
                self.store.execute(replace(cmd, message_id=f"m-{attempt}"), self.principal)
            self.assertEqual(self.store.counts("teaching", run), (2, 0, 0, 0))


class EnvelopeTests(unittest.TestCase):
    def wire(self) -> dict[str, object]:
        return {"message_id": "m1", "run_id": "r1", "correlation_id": "c1",
                "causation_id": None, "tenant": "teaching", "owner": "owner",
                "fence": 1, "key": "reserve-1", "operation": "reserve",
                "schema_version": 1,
                "payload": {"order_id": "o1", "quantity": 2, "cents": 2400}}

    def test_v1_upcasts_to_v2(self) -> None:
        command = decode(self.wire())
        self.assertEqual(command.schema_version, 2)
        self.assertEqual(command.payload.currency, "USD")

    def test_unknown_version_and_fields_and_bool_quantity(self) -> None:
        for mutation in ({"schema_version": 3}, {"authority": "admin"},
                         {"payload": {"order_id": "o1", "quantity": True, "cents": 5}}):
            with self.subTest(mutation=mutation), self.assertRaises(Refusal):
                decode(self.wire() | mutation)


class LivenessTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_lock_cycle_aborts(self) -> None:
        self.assertIn("timeout aborted", await reproduce())

    async def test_global_order_completes(self) -> None:
        self.assertEqual(await ordered(), ("A completed", "B completed"))

    async def test_bounded_queue_exerts_backpressure(self) -> None:
        queue: asyncio.Queue[int] = asyncio.Queue(maxsize=1)
        await queue.put(1)
        with self.assertRaises(TimeoutError):
            async with asyncio.timeout(0.05):
                await queue.put(2)
        self.assertEqual(await queue.get(), 1)


if __name__ == "__main__":
    unittest.main()
