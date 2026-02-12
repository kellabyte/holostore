# HoloFusion Metadata Migration and Backfill

## Purpose

This document defines the Phase 8 #3 migration/backfill contract for existing
clusters that were created before schema-driven metadata fields were persisted.

Primary goals:
- preserve online startup (no manual offline migration required)
- keep migration idempotent across retries/restarts
- support concurrent node startup without split-brain metadata state
- provide deterministic completion marker for rollout safety

## Versioning Model

- Metadata schema baseline: `v1` (legacy clusters)
- Metadata schema current: `v2`
- State key: `0x09 0x01` (`metadata_schema_state`)
- State payload fields:
- `schema_version`
- `checkpoint_table_id` (last processed table id)
- `updated_at_unix_ms`

## Backfill Scope

Current `v1 -> v2` backfill migrates legacy `orders_v1` metadata rows to include:
- canonical persisted columns:
- `order_id BIGINT NOT NULL`
- `customer_id BIGINT NOT NULL`
- `status TEXT NULL`
- `total_cents BIGINT NOT NULL`
- `created_at TIMESTAMP NOT NULL`
- `primary_key_column = "order_id"`
- non-zero `page_size` (default `2048` if absent/invalid)

This backfill is intentionally narrow to avoid risky semantic changes:
- only `orders_v1` rows are rewritten
- `row_v1` rows are not rewritten by this phase

## Execution Semantics

Migration runs during node startup before catalog bootstrap:
- load schema-state record (defaults to `v1` when absent)
- if version is already `v2`, skip
- resume from `checkpoint_table_id` if prior run was interrupted
- scan metadata rows in deterministic `table_id` order
- rewrite each candidate row via conditional write (`expected_version`)
- persist checkpoint in batches
- publish final state `schema_version = v2` and clear checkpoint

## Correctness and Resiliency

Idempotency:
- backfill rewrite is deterministic for each table id
- rerunning migration after success performs zero rewrites

Concurrency safety:
- writes use storage-level expected-version checks
- on conflict, migrator reloads latest row and retries
- if another node already migrated a row, local node treats it as success

Crash safety:
- checkpointing allows restart-resume from prior progress
- completion marker (`schema_version = v2`) is durable and cluster-visible

Rollback posture:
- migration is additive/normalizing metadata only
- row data is unchanged
- rollback to old binaries remains possible because `orders_v1` runtime path
  still accepts canonicalized metadata

## Operational Notes

- Migration starts automatically on process startup.
- Operators should watch startup logs for:
- schema version transitions (`from`, `to`)
- resumed checkpoint id (if any)
- migrated row count
- cluster is considered fully migrated when all nodes report `v2` and no
  additional rewrites occur on restart.
