# Secondary Index Optimization Commit Message

## Subject
`holo_fusion: optimize secondary index backfill throughput and metadata reliability`

## Description
Improve secondary index creation performance and operational reliability without changing transactional correctness.

- Add adaptive backfill page sizing so index build iterations are sized from backfill write pipeline depth, batch entry limits, and in-flight budgets.
- Increase the default backfill page cap from 512 to 32768 to reduce per-iteration control overhead on large backfills.
- Add budget-aware backfill retry behavior: on write-plan in-flight budget rejection, shrink page size and retry deterministically from the same cursor.
- Preserve ACID/linearizability semantics by keeping conditional-write conflict handling and rollback behavior unchanged.
- Strengthen secondary index metadata persistence resilience with broader transient-error retry classification and idempotent recovery when metadata becomes visible during retries.
- Add focused unit coverage for backfill page planning and shrink classification behavior.
- Update cluster startup defaults and runbook documentation for the new backfill tuning knobs.

## Validation
- `cargo fmt -p holo_fusion`
- `cargo test -p holo_fusion --lib index_backfill_page_plan -- --nocapture`
- `cargo test -p holo_fusion --lib backfill_page_shrink_classifier -- --nocapture`
- `cargo test -p holo_fusion --lib --no-run`

## Commit Body (Copy/Paste)
```text
Improve secondary index creation performance and operational reliability
without changing transactional correctness.

- Add adaptive backfill page sizing so index build iterations are sized
  from backfill write pipeline depth, batch entry limits, and in-flight
  budgets.
- Increase the default backfill page cap from 512 to 32768 to reduce
  per-iteration control overhead on large backfills.
- Add budget-aware backfill retry behavior: on write-plan in-flight
  budget rejection, shrink page size and retry deterministically from
  the same cursor.
- Preserve ACID/linearizability semantics by keeping conditional-write
  conflict handling and rollback behavior unchanged.
- Strengthen secondary index metadata persistence resilience with broader
  transient-error retry classification and idempotent recovery when
  metadata becomes visible during retries.
- Add focused unit coverage for backfill page planning and shrink
  classification behavior.
- Update cluster startup defaults and runbook documentation for the new
  backfill tuning knobs.
```
