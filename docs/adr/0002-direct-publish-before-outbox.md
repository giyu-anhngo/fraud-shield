# ADR 0002 — Publish events directly (outbox deferred to Phase 2)

Status: Accepted (temporary) · Date: 2026-06-30

## Context
When the Transaction Service records a payment, it must also publish a
`TransactionEvent` to Kafka. The reliable approach is the **outbox pattern**:
write the event to an `outbox` table in the same DB transaction as the payment,
and let a relay publish it to Kafka. The build plan schedules the outbox for
Phase 2.

## Decision
For Phase 1, save the payment to PostgreSQL and then publish to Kafka
**directly** from the service, immediately after the DB commit. The code carries
a `// TODO Phase 2: replace with outbox` marker at the publish site.

## Consequences
- Simple and fast to build; unblocks the end-to-end Phase 1 flow.
- **Accepted risk — dual write:** if the DB commit succeeds but the Kafka publish
  fails (or the process dies in between), the payment exists with no event, and
  downstream fraud scoring never sees it. We accept this gap for Phase 1.
- Phase 2 closes the gap by introducing the outbox table + relay; this ADR will
  be superseded by the outbox ADR at that point.
