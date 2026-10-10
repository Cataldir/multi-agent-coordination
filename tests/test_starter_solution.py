"""Set COURSE_EXERCISES=starters.exercises to observe intentional failures."""
import importlib
import os
import tempfile
import unittest
from pathlib import Path

from coordination.envelope import Operation, Payload
from coordination.saga import Coordinator, SagaState, ScriptedPeer
from coordination.store import Principal, Store

exercises = importlib.import_module(os.environ.get("COURSE_EXERCISES", "solutions.exercises"))


class ExerciseContractTests(unittest.TestCase):
    def test_lab11(self) -> None:
        raw = {"message_id": "m1", "run_id": "r1", "correlation_id": "r1",
               "causation_id": None, "tenant": "teaching", "owner": "owner",
               "fence": 1, "key": "r1:reserve", "operation": "reserve",
               "schema_version": 1,
               "payload": {"order_id": "r1", "quantity": 2, "cents": 2400}}
        self.assertEqual(exercises.lab11_decode(raw).schema_version, 2)

    def test_lab22(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "test.sqlite")
            try:
                c = Coordinator(store, Principal("teaching", "owner"), "lab22")
                self.assertEqual(exercises.lab22_recover(c, ScriptedPeer(3, 3100)),
                                 SagaState.COMPENSATED)
                self.assertEqual(store.counts("teaching", "lab22"), (0, 0, 0, 0))
            finally:
                store.close()

    def test_lab27(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "test.sqlite")
            try:
                principal = Principal("teaching", "owner")
                c = Coordinator(store, principal, "lab27")
                command = c.command(Operation.RESERVE, Payload("lab27", 2, 2400))
                exercises.lab27_apply(store, command, principal)
                exercises.lab27_apply(store, command, principal)
                self.assertEqual(store.counts("teaching", "lab27"), (2, 0, 0, 0))
            finally:
                store.close()

    def test_lab33(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / "test.sqlite")
            try:
                c = Coordinator(store, Principal("teaching", "owner"), "lab33")
                self.assertEqual(exercises.lab33_unknown(c), SagaState.RECONCILING)
                self.assertEqual(store.counts("teaching", "lab33"), (2, 0, 0, 0))
            finally:
                store.close()
