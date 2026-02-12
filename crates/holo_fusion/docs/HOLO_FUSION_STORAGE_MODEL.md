# HoloFusion Storage Model (Phase 0 Spec)

## Status

- Status: Draft for Phase 0 compatibility spike.
- Scope: defines how SQL database/schema/table/row/column/index primitives map
  onto HoloStore key/value storage.
- Non-goals: this document does not define full SQL semantics or query planning.

## Design goals

1. Keep keys lexicographically ordered for efficient range scans.
2. Keep primary-row keys stable across updates (MVCC versions live in HoloStore
   version dimension, not in key suffixes).
3. Encode enough metadata to map SQL objects to stable IDs.
4. Support secondary-index lookups without requiring table scans.
5. Make DDL and schema evolution deterministic and replay-safe.

## Logical model

HoloFusion maps SQL primitives to two logical keyspaces:

- `meta` keyspace:
  - database, schema, table, column, and index metadata
  - name-to-id mappings
- `data` keyspace:
  - primary table rows
  - secondary-index entries

All IDs are unsigned 64-bit integers (`u64`) allocated by monotonic counters in
metadata.

## Key encoding

All keys are binary and lexicographically ordered.

- Integer encoding: big-endian fixed width.
- Variable-length byte fields: length-prefixed with `u16` big-endian.
- Prefix byte determines record family.

### Prefix map

- `0x01`: database metadata record
- `0x02`: schema metadata record
- `0x03`: table metadata record
- `0x04`: column metadata record
- `0x05`: index metadata record
- `0x06`: database name -> id mapping
- `0x07`: schema name -> id mapping
- `0x08`: table name -> id mapping
- `0x20`: primary-row record
- `0x30`: secondary-index record

### Metadata keys

1. Database record key:
- `0x01 | db_id:u64`

2. Schema record key:
- `0x02 | db_id:u64 | schema_id:u64`

3. Table record key:
- `0x03 | db_id:u64 | schema_id:u64 | table_id:u64`

4. Column record key:
- `0x04 | table_id:u64 | column_id:u64`

5. Index record key:
- `0x05 | table_id:u64 | index_id:u64`

6. Database name map key:
- `0x06 | len(db_name):u16 | db_name:bytes`

7. Schema name map key:
- `0x07 | db_id:u64 | len(schema_name):u16 | schema_name:bytes`

8. Table name map key:
- `0x08 | db_id:u64 | schema_id:u64 | len(table_name):u16 | table_name:bytes`

### Data keys

1. Primary row key:
- `0x20 | table_id:u64 | pk_tuple_encoded:bytes`

2. Secondary index key:
- `0x30 | table_id:u64 | index_id:u64 | index_tuple_encoded:bytes | pk_tuple_encoded:bytes`

Notes:
- `pk_tuple_encoded` and `index_tuple_encoded` use the same canonical tuple
  encoding (see "Tuple encoding").
- Including `pk_tuple_encoded` in index keys guarantees uniqueness and supports
  index-only seek to row keys.

## Value encoding

HoloStore already provides per-key versioned writes and visibility, so row
updates use the same key and write a new versioned value.

### Metadata value

Metadata values are encoded as versioned protobuf or serde structs (to be fixed
in implementation). Minimum required fields:

- `DatabaseMeta`: `db_id`, `db_name`, `owner`, `created_at`
- `SchemaMeta`: `schema_id`, `db_id`, `schema_name`, `created_at`
- `TableMeta`: `table_id`, `db_id`, `schema_id`, `table_name`,
  `primary_key_column_ids`, `created_at`
- `ColumnMeta`: `column_id`, `table_id`, `name`, `logical_type`, `nullable`,
  `default_expr`, `ordinal`
- `IndexMeta`: `index_id`, `table_id`, `name`, `column_ids`, `unique`,
  `created_at`

### Row value

Row value layout (v1):

- `format_version:u8`
- `column_count:u16`
- `null_bitmap_len:u16`
- `null_bitmap:bytes`
- repeated column payloads in `column_id` order:
  - `payload_len:u32`
  - `payload:bytes` (omitted if null bitmap marks null)

Column payloads use canonical binary encoding per logical type:
- `BOOLEAN`: `u8` (0/1)
- `INT64`: `i64` big-endian
- `UTF8`: `u32 len + utf8 bytes`
- `TIMESTAMP_NS`: `i64` big-endian
- `DECIMAL`: fixed-width two's-complement bytes + scale in column metadata

### Tombstones

- Row delete: write a tombstone row value (`format_version` + tombstone flag).
- Index delete: write tombstone at index key.
- Physical GC is deferred to HoloStore compaction/GC policy.

## Tuple encoding

Tuple encoding is order-preserving for index/range scans:

- For each tuple element:
  - `type_tag:u8`
  - `is_null:u8`
  - if not null: normalized binary payload for that type
- Composite tuple is concatenation of encoded elements.

Primary key tuple order is exactly the table primary-key column order.
Secondary index tuple order is exactly index column order.

## Mapping SQL DDL to keys

Given:
- `db_name`
- `schema_name`
- `table_name`
- ordered column list
- primary key and index definitions

The mapping process is:

1. Resolve or allocate `db_id`.
2. Resolve or allocate `schema_id`.
3. Allocate `table_id`.
4. Allocate stable `column_id` values in DDL order.
5. Allocate `index_id` values for each secondary index.
6. Write all metadata records and name-map records in one metadata transaction.

## Shard/range routing rules

Routing is based on full binary key bytes.

- Metadata keys are routed through metadata ranges/groups.
- Data keys are routed through data ranges/groups.
- Primary row locality is by table prefix then primary key tuple ordering.
- Secondary index locality is by table + index prefix then indexed tuple ordering.

This allows:
- point gets by primary key (`0x20|table_id|pk`)
- bounded scans (`0x20|table_id|pk_start` to `...pk_end`)
- index range scans (`0x30|table_id|index_id|idx_start` to `...idx_end`)

## Schema evolution

Rules:

1. `column_id` is immutable once assigned.
2. Dropped columns remain tombstoned in metadata for compatibility.
3. New nullable columns append new `column_id`; old rows read as null/default.
4. Type changes require explicit migration path (out of MVP).

## Worked example (DDL -> keys/values)

### DDL

```sql
CREATE DATABASE app;
CREATE SCHEMA app.public;
CREATE TABLE app.public.orders (
  order_id BIGINT PRIMARY KEY,
  customer_id BIGINT NOT NULL,
  status TEXT,
  total_cents BIGINT NOT NULL,
  created_at TIMESTAMP NOT NULL
);
CREATE INDEX idx_orders_customer ON app.public.orders (customer_id);
```

### Assigned IDs

- `db_id = 1` (`app`)
- `schema_id = 1` (`public`)
- `table_id = 100` (`orders`)
- `column_id(order_id)=1`
- `column_id(customer_id)=2`
- `column_id(status)=3`
- `column_id(total_cents)=4`
- `column_id(created_at)=5`
- `index_id(idx_orders_customer)=1`

### Metadata keys (human-readable form)

- Database: `0x01|1`
- Schema: `0x02|1|1`
- Table: `0x03|1|1|100`
- Columns:
  - `0x04|100|1`
  - `0x04|100|2`
  - `0x04|100|3`
  - `0x04|100|4`
  - `0x04|100|5`
- Index: `0x05|100|1`
- Name maps:
  - `0x06|"app"`
  - `0x07|1|"public"`
  - `0x08|1|1|"orders"`

### Insert example

```sql
INSERT INTO app.public.orders
  (order_id, customer_id, status, total_cents, created_at)
VALUES
  (1001, 42, 'paid', 1299, TIMESTAMP '2026-02-11 10:00:00');
```

Derived data records:

1. Primary row key:
- `0x20|100|tuple(order_id=1001)`

2. Primary row value:
- row-format-v1 payload for columns 1..5

3. Secondary index key (`idx_orders_customer`):
- `0x30|100|1|tuple(customer_id=42)|tuple(order_id=1001)`

4. Secondary index value:
- empty payload or minimal marker bytes (implementation choice)

On update:
- same primary key, new value version written by HoloStore.

On delete:
- tombstone written for primary row key and all affected secondary index keys.

## MVP implementation constraints

- MVP supports a single tenant namespace.
- MVP supports scalar SQL types used in HoloStore workloads first (`BIGINT`,
  `TEXT`, `BOOLEAN`, `TIMESTAMP`, `DECIMAL`).
- Unique secondary indexes beyond primary key are deferred unless explicitly
  enabled with conflict checks.

