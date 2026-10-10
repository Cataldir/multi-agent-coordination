# Additional guided exercises

These exercises extend the same code and guarantees. All reference code and tests are authored; expected conditions below have not been observed. Start with a small failing contract, explain the boundary, then inspect the reference. Preserve learner databases and failed transcripts.

## E1 — Lost acknowledgment and duplicate consumer delivery (O5, O6)

The producer commits an event. The consumer accepts it, but the producer fails before marking it sent. Implement a consumer inbox whose dedupe insert and accepted-effect count share one transaction. Retry the original event identity after restarting the consumer. Reusing that identity with changed content must conflict.

Hint: a successful network send is insufficient evidence of consumer commit; marking sent before consumption can lose work. The complete local reference is Consumer and relay_once in coordination/extensions.py. test_duplicate_relay_after_lost_ack expects consumer count one before and after restart, then no producer event pending. This deterministic local exercise is not a real broker acknowledgment proof. Extend with a broker only after its delivery/ack contract is qualified.

## E2 — Three resource ordering and cancellation (O3)

Add a third lock and submit resource requests in different orders. Define one global ordering by resource ID; acquire in that order and release acquired locks in reverse in a finally block. Include a deadline and cancellation cleanup.

Hint: sorting only the first pair does not order the whole resource set. The reference three_ordered_locks requests (2,0,1) and (1,2,0), sorts IDs and expects both workers to finish with three resources. The original deadlock demonstration still forces a two-resource cycle. A timeout bounds waiting; common ordering prevents this specified lock cycle. This does not prove fairness across arbitrary workloads.

## E3 — Revision-bound agreement (O1, O3)

Three declared members vote on a proposed delivery date. Require a strict majority of fixed membership. Reject unknown members, stale proposal revisions and duplicate ballots; absence of a majority remains undecided. A changed proposal requires a new revision.

Hint: two identical strings from one member are still one member, and a vote for revision one cannot approve revision two. Ballot and majority are complete deterministic references. Tests expect two of three Friday votes to select Friday and stale/duplicate ballots to refuse. This is an application agreement exercise, not a Byzantine or distributed consensus implementation. Persist revision ownership before proposing external effects.

## E4 — Retention without breaking retry safety (O5)

Assume a seven-day retry contract and up to two additional days of delayed delivery. Specify whether unresolved operations can be archived and how an old key is answered after archive. Avoid treating dedupe TTL expiry as authorization to repeat a payment.

Hint: retention binds the maximum replay horizon, archive query contract and unknown-outcome obligations. may_archive conservatively requires age greater than the summed horizon, no unresolved obligation and queryable archive evidence. Reference tests reject day seven and unresolved day ten, and accept resolved day ten only with queryable evidence. Production retention requires the actual owner contract, legal requirements and migration evidence; this function supplies none of those.

## E5 — Partial-order compensation (O4)

Add a warehouse reservation and a hotel reservation that are independent, followed by a payment depending on both. Draw reserve-warehouse → charge and reserve-hotel → charge. Refund must precede either release under the declared policy. The two release operations may commute only if their effect owners and resources are disjoint.

Reference solution: journal both realized reservation receipts; bind one unique reverse key to each original receipt; reverse charge first; then release warehouse and hotel in either order. A failed hotel release leaves recovery_required with that receipt pending, while a confirmed warehouse release stays accepted. Restart must not repeat its business effect. Duplicate compensation returns the old reverse receipt. The existing Saga/store illustrate these mechanics for one reservation; this two-owner extension is intentionally a design exercise, not falsely claimed implemented production behavior.

## E6 — Authoritative reconciliation contract (O2, O4, O5)

Design a provider query returning applied, definitively absent or unknown for the original tenant and operation key. Applied requires an authentic receipt. Definitive absence requires a documented guarantee that this request cannot commit later. Timeout, inconclusive 404 and identity mismatch cannot authorize a fresh retry or compensation.

QueryEvidence.validate is a complete typed boundary reference. It checks the expected tenant/key and required evidence fields; test_provider_query_contract_is_not_fixture_confirmation covers unknown, unqualified absence and wrong identity. Authenticity and provider no-later-commit guarantees belong to the real adapter and remain unimplemented until one is qualified. Do not pass a synthetic QueryEvidence instance into production. Preserve its receipt and query transcript before the coordinator transitions.

Submit each exercise with outcome IDs, exact source hashes, failure/expected-result table, practical transcript after admission, and the limits of its evidence.
