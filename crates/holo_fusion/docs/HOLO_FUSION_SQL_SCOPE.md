# HoloFusion SQL Scope (MVP Matrix)

## Purpose

Define the SQL surface for HoloFusion MVP and the expected behavior for
unsupported or deferred statements.

This is a contract for:
- client expectations,
- implementation prioritization,
- test coverage.

## Versioning model

- `MVP-R` (read MVP): through Phase 3 (`SELECT` + distributed reads).
- `MVP-W` (write MVP): Phase 4 adds `INSERT`.
- `MVP-DML`: Phase 5 adds controlled `UPDATE` / `DELETE`.

Unless stated otherwise, unsupported statements must return:
- SQLSTATE `0A000` (`feature_not_supported`),
- message prefix: `not supported in current HoloFusion scope`.

## Supported statements

| Category | SQL form | Status | Target phase | Notes |
|---|---|---|---|---|
| Utility | `SELECT 1` | Supported | Phase 1 | Health/smoke test path. |
| Query | `SELECT ... FROM ...` | Supported | Phase 2 | Single-table scans first. |
| Query | `SELECT ... WHERE ...` | Supported | Phase 2 | Filter pushdown where possible. |
| Query | `SELECT ... ORDER BY ... LIMIT ...` | Supported | Phase 2 | Full sort may spill depending on engine settings. |
| Query | `SELECT ... JOIN ...` | Supported | Phase 3 | Distributed path may be used via Ballista policy. |
| Query | `SELECT` with aggregates (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`) | Supported | Phase 3 | Subject to DataFusion function support. |
| DML | `INSERT INTO t VALUES (...)` | Supported | Phase 4 | Must route writes via embedded HoloStore APIs. |
| DML | `INSERT INTO t SELECT ...` | Supported | Phase 4 | Same idempotency/retry guarantees as other inserts. |
| DML | `UPDATE ... WHERE ...` | Supported (controlled) | Phase 5 | Predicate support may be narrowed initially. |
| DML | `DELETE FROM ... WHERE ...` | Supported (controlled) | Phase 5 | Full-table delete may be disallowed initially. |
| Txn | `BEGIN` / `START TRANSACTION` | Supported (session-managed) | Phase 6 | Backed by HoloStore transaction/session layer. |
| Txn | `COMMIT` | Supported (session-managed) | Phase 6 | Commits HoloStore transaction context. |
| Txn | `ROLLBACK` | Supported (session-managed) | Phase 6 | Aborts HoloStore transaction context. |
| DDL | `CREATE TABLE` | Supported (controlled) | Phase 7 | Persists table metadata in HoloStore; current table model requires `orders_v1` column shape and `PRIMARY KEY(order_id)`. |

## Explicitly rejected or deferred statements

| Category | SQL form | Decision | SQLSTATE | Behavior |
|---|---|---|---|---|
| DDL | `CREATE DATABASE` | Deferred | `0A000` | Return feature-not-supported for MVP scope. |
| DDL | `DROP DATABASE` | Deferred | `0A000` | Return feature-not-supported for MVP scope. |
| DDL | `CREATE SCHEMA` / `DROP SCHEMA` | Deferred | `0A000` | Return feature-not-supported for MVP scope. |
| DDL | `ALTER TABLE` / `DROP TABLE` | Deferred | `0A000` | Schema evolution/drop lifecycle remains deferred. |
| DDL | `CREATE INDEX` / `DROP INDEX` | Deferred | `0A000` | Index lifecycle managed outside SQL in MVP. |
| DML | `MERGE` | Rejected (initially) | `0A000` | Not in MVP; revisit after core DML hardening. |
| DML | `INSERT OVERWRITE` / `INSERT REPLACE` | Rejected (initially) | `0A000` | Not in MVP scope. |
| DML | `TRUNCATE` | Rejected (initially) | `0A000` | Not in MVP scope. |
| Query | `SELECT ... FOR UPDATE` | Rejected (initially) | `0A000` | Row-locking syntax not supported in MVP. |
| Txn | `SAVEPOINT`, `RELEASE SAVEPOINT`, `ROLLBACK TO SAVEPOINT` | Rejected (initially) | `0A000` | Nested transaction control not supported in MVP. |
| Session | `SET TRANSACTION ...` | Deferred | `0A000` | Isolation-level tuning deferred to post-MVP. |
| Utility | `COPY ...` | Deferred | `0A000` | Bulk load/unload deferred. |
| Utility | `EXPLAIN ANALYZE` | Deferred | `0A000` | Plain `EXPLAIN` may be enabled later; analyze deferred. |

## Required SQLSTATE behavior for common errors

These apply to supported statements:

| Condition | SQLSTATE | Expected behavior |
|---|---|---|
| Unknown table/schema | `42P01` / `3F000` | Return relation/schema-not-found from catalog resolver. |
| Unknown column | `42703` | Return undefined-column. |
| Type mismatch / invalid cast | `42804` | Return datatype mismatch. |
| Unique key violation (when enforced) | `23505` | Return unique_violation. |
| Serialization/conflict abort | `40001` | Return serialization_failure and require retry. |
| Statement timeout / cancelled execution | `57014` | Return query_canceled. |
| Internal engine error | `XX000` | Return internal_error with correlation id in logs. |

## Guardrails for MVP

1. Unsupported statements must fail fast before execution.
2. Error code/message shape must be deterministic for client compatibility.
3. SQL path must use embedded HoloStore APIs and native cluster RPCs, not Redis.
4. Any newly enabled statement must add:
- integration test,
- SQLSTATE assertion for failure paths,
- update to this matrix.

## Exit criteria to expand scope

Move beyond MVP scope only when:
- all supported statement classes have passing multi-node tests,
- retry/idempotency behavior is validated for distributed writes,
- SQLSTATE behavior is stable across regression suites.
