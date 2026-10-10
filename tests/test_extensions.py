"""Authored extra-exercise references. No test was run in this workspace."""
import asyncio
import tempfile
import unittest
from pathlib import Path

from coordination.envelope import Operation, Payload, Refusal
from coordination.extensions import (Ballot, Consumer, QueryEvidence, QueryState,
                                     majority, may_archive, relay_once, three_ordered_locks)
from coordination.saga import Coordinator
from coordination.store import Principal, Store


class ExtensionTests(unittest.TestCase):
    def test_duplicate_relay_after_lost_ack(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            store = Store(Path(folder) / "producer.sqlite")
            consumer = Consumer(Path(folder) / "consumer.sqlite")
            try:
                p = Principal("teaching", "owner")
                c = Coordinator(store, p, "relay")
                store.execute(c.command(Operation.RESERVE, Payload("relay", 2, 2400)), p)
                with self.assertRaises(RuntimeError):
                    relay_once(store, consumer, lose_ack=True)
                self.assertEqual(consumer.count(), 1)
                consumer.close()
                consumer = Consumer(Path(folder) / "consumer.sqlite")
                self.assertEqual(relay_once(store, consumer), 1)
                self.assertEqual(consumer.count(), 1)
                self.assertEqual(store.pending(), ())
            finally:
                consumer.close()
                store.close()

    def test_vote_revision_and_membership(self) -> None:
        voters = frozenset({"a", "b", "c"})
        self.assertEqual(majority((Ballot("a", 2, "Friday"), Ballot("b", 2, "Friday")),
                                  voters, 2), "Friday")
        with self.assertRaises(Refusal):
            majority((Ballot("a", 1, "Friday"),), voters, 2)
        with self.assertRaises(Refusal):
            majority((Ballot("a", 2, "Friday"), Ballot("a", 2, "Friday")), voters, 2)

    def test_retention_contract(self) -> None:
        self.assertFalse(may_archive(7, retry_days=7, delayed_delivery_days=2,
                                     unresolved=False, archive_queryable=True))
        self.assertTrue(may_archive(10, retry_days=7, delayed_delivery_days=2,
                                    unresolved=False, archive_queryable=True))
        self.assertFalse(may_archive(10, retry_days=7, delayed_delivery_days=2,
                                     unresolved=True, archive_queryable=True))

    def test_provider_query_contract_is_not_fixture_confirmation(self) -> None:
        QueryEvidence("teaching", "charge-1", QueryState.UNKNOWN).validate("teaching", "charge-1")
        with self.assertRaises(Refusal):
            QueryEvidence("teaching", "charge-1", QueryState.DEFINITIVELY_ABSENT).validate(
                "teaching", "charge-1")
        with self.assertRaises(Refusal):
            QueryEvidence("another", "charge-1", QueryState.APPLIED, "receipt").validate(
                "teaching", "charge-1")


class OrderedExtensionTests(unittest.IsolatedAsyncioTestCase):
    async def test_three_resources_complete(self) -> None:
        self.assertEqual(await three_ordered_locks(), (3, 3))
