use std::collections::BTreeMap;

use anyhow::{anyhow, Result};
use datafusion::common::ScalarValue;

use crate::indexing::{SecondaryIndexDistribution, SecondaryIndexRecord};
use crate::metadata::{TableColumnRecord, TableColumnType};

const DATA_PREFIX_SECONDARY_INDEX_ROW: u8 = 0x21;
const DATA_PREFIX_SECONDARY_INDEX_UNIQUE: u8 = 0x22;
const HASH_BUCKET_TAG: u8 = 0x68;
const TUPLE_TAG_PRIMARY_KEY: u8 = 0x02;
const KEY_NULL_MARKER: u8 = 0x00;
const KEY_NOT_NULL_MARKER: u8 = 0x01;
const ROW_FORMAT_VERSION_V2: u8 = 2;
const ROW_FLAG_TOMBSTONE: u8 = 0x01;
const SIGN_FLIP_MASK: u64 = 1u64 << 63;

fn encode_i64_ordered(value: i64) -> [u8; 8] {
    (value as u64 ^ SIGN_FLIP_MASK).to_be_bytes()
}

fn decode_i64_ordered(bytes: [u8; 8]) -> i64 {
    let raw = u64::from_be_bytes(bytes) ^ SIGN_FLIP_MASK;
    raw as i64
}

fn hash_bucket(bytes: &[u8], bucket_count: usize) -> u16 {
    let count = bucket_count.max(1).min(u16::MAX as usize);
    let mut mixed: u64 = 0x9E37_79B9_7F4A_7C15;
    for b in bytes {
        mixed ^= u64::from(*b);
        mixed = mixed.wrapping_mul(0xBF58_476D_1CE4_E5B9);
        mixed ^= mixed >> 27;
        mixed = mixed.wrapping_mul(0x94D0_49BB_1331_11EB);
        mixed ^= mixed >> 31;
    }
    (mixed % (count as u64)) as u16
}

fn scalar_is_null(value: &ScalarValue) -> bool {
    matches!(
        value,
        ScalarValue::Null
            | ScalarValue::Int8(None)
            | ScalarValue::Int16(None)
            | ScalarValue::Int32(None)
            | ScalarValue::Int64(None)
            | ScalarValue::UInt8(None)
            | ScalarValue::UInt16(None)
            | ScalarValue::UInt32(None)
            | ScalarValue::UInt64(None)
            | ScalarValue::Float32(None)
            | ScalarValue::Float64(None)
            | ScalarValue::Boolean(None)
            | ScalarValue::Utf8(None)
            | ScalarValue::LargeUtf8(None)
            | ScalarValue::TimestampNanosecond(None, _)
            | ScalarValue::TimestampMicrosecond(None, _)
            | ScalarValue::TimestampMillisecond(None, _)
            | ScalarValue::TimestampSecond(None, _)
    )
}

fn scalar_to_i64(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::Int8(Some(v)) => Some(i64::from(*v)),
        ScalarValue::Int16(Some(v)) => Some(i64::from(*v)),
        ScalarValue::Int32(Some(v)) => Some(i64::from(*v)),
        ScalarValue::Int64(Some(v)) => Some(*v),
        ScalarValue::UInt8(Some(v)) => Some(i64::from(*v)),
        ScalarValue::UInt16(Some(v)) => Some(i64::from(*v)),
        ScalarValue::UInt32(Some(v)) => Some(i64::from(*v)),
        ScalarValue::UInt64(Some(v)) => i64::try_from(*v).ok(),
        _ => None,
    }
}

fn scalar_to_timestamp_ns(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::TimestampNanosecond(Some(v), _) => Some(*v),
        ScalarValue::TimestampMicrosecond(Some(v), _) => Some(v.saturating_mul(1_000)),
        ScalarValue::TimestampMillisecond(Some(v), _) => Some(v.saturating_mul(1_000_000)),
        ScalarValue::TimestampSecond(Some(v), _) => Some(v.saturating_mul(1_000_000_000)),
        _ => None,
    }
}

fn encode_scalar_payload_for_column(
    value: &ScalarValue,
    column: &TableColumnRecord,
) -> Result<Option<Vec<u8>>> {
    if scalar_is_null(value) {
        if !column.nullable {
            return Err(anyhow!(
                "null value violates not-null constraint for column '{}'",
                column.name
            ));
        }
        return Ok(None);
    }

    let payload = match column.column_type {
        TableColumnType::Int8 => {
            let v = scalar_to_i64(value)
                .and_then(|raw| i8::try_from(raw).ok())
                .ok_or_else(|| anyhow!("invalid value type for Int8 column '{}'", column.name))?;
            vec![v as u8]
        }
        TableColumnType::Int16 => {
            let v = scalar_to_i64(value)
                .and_then(|raw| i16::try_from(raw).ok())
                .ok_or_else(|| anyhow!("invalid value type for Int16 column '{}'", column.name))?;
            v.to_be_bytes().to_vec()
        }
        TableColumnType::Int32 => {
            let v = scalar_to_i64(value)
                .and_then(|raw| i32::try_from(raw).ok())
                .ok_or_else(|| anyhow!("invalid value type for Int32 column '{}'", column.name))?;
            v.to_be_bytes().to_vec()
        }
        TableColumnType::Int64 => {
            let v = scalar_to_i64(value)
                .ok_or_else(|| anyhow!("invalid value type for Int64 column '{}'", column.name))?;
            v.to_be_bytes().to_vec()
        }
        TableColumnType::UInt8 => {
            let v = scalar_to_i64(value)
                .and_then(|raw| u8::try_from(raw).ok())
                .ok_or_else(|| anyhow!("invalid value type for UInt8 column '{}'", column.name))?;
            vec![v]
        }
        TableColumnType::UInt16 => {
            let v = scalar_to_i64(value)
                .and_then(|raw| u16::try_from(raw).ok())
                .ok_or_else(|| anyhow!("invalid value type for UInt16 column '{}'", column.name))?;
            v.to_be_bytes().to_vec()
        }
        TableColumnType::UInt32 => {
            let v = scalar_to_i64(value)
                .and_then(|raw| u32::try_from(raw).ok())
                .ok_or_else(|| anyhow!("invalid value type for UInt32 column '{}'", column.name))?;
            v.to_be_bytes().to_vec()
        }
        TableColumnType::UInt64 => {
            let v = match value {
                ScalarValue::UInt64(Some(v)) => *v,
                ScalarValue::UInt32(Some(v)) => u64::from(*v),
                ScalarValue::UInt16(Some(v)) => u64::from(*v),
                ScalarValue::UInt8(Some(v)) => u64::from(*v),
                ScalarValue::Int64(Some(v)) if *v >= 0 => *v as u64,
                ScalarValue::Int32(Some(v)) if *v >= 0 => *v as u64,
                ScalarValue::Int16(Some(v)) if *v >= 0 => *v as u64,
                ScalarValue::Int8(Some(v)) if *v >= 0 => *v as u64,
                _ => {
                    return Err(anyhow!(
                        "invalid value type for UInt64 column '{}'",
                        column.name
                    ));
                }
            };
            v.to_be_bytes().to_vec()
        }
        TableColumnType::Float64 => {
            let v = match value {
                ScalarValue::Float64(Some(v)) => *v,
                ScalarValue::Float32(Some(v)) => f64::from(*v),
                ScalarValue::Int64(Some(v)) => *v as f64,
                ScalarValue::Int32(Some(v)) => *v as f64,
                ScalarValue::Int16(Some(v)) => *v as f64,
                ScalarValue::Int8(Some(v)) => *v as f64,
                _ => {
                    return Err(anyhow!(
                        "invalid value type for Float64 column '{}'",
                        column.name
                    ));
                }
            };
            v.to_be_bytes().to_vec()
        }
        TableColumnType::Boolean => match value {
            ScalarValue::Boolean(Some(v)) => vec![u8::from(*v)],
            _ => {
                return Err(anyhow!(
                    "invalid value type for Boolean column '{}'",
                    column.name
                ));
            }
        },
        TableColumnType::Utf8 => match value {
            ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => v.as_bytes().to_vec(),
            _ => {
                return Err(anyhow!(
                    "invalid value type for Utf8 column '{}'",
                    column.name
                ));
            }
        },
        TableColumnType::TimestampNanosecond => {
            let ts = scalar_to_timestamp_ns(value).ok_or_else(|| {
                anyhow!(
                    "invalid value type for TimestampNanosecond column '{}'",
                    column.name
                )
            })?;
            ts.to_be_bytes().to_vec()
        }
    };

    Ok(Some(payload))
}

fn table_column_by_name<'a>(
    columns: &'a [TableColumnRecord],
    by_name: &'a BTreeMap<String, usize>,
    name: &str,
) -> Result<(usize, &'a TableColumnRecord)> {
    let normalized = name.to_ascii_lowercase();
    let idx = by_name
        .get(normalized.as_str())
        .copied()
        .ok_or_else(|| anyhow!("column '{}' not found in table metadata", name))?;
    let column = columns
        .get(idx)
        .ok_or_else(|| anyhow!("column '{}' metadata index out of bounds", name))?;
    Ok((idx, column))
}

fn encode_index_tuple_bytes(
    index: &SecondaryIndexRecord,
    columns: &[TableColumnRecord],
    row_values: &[ScalarValue],
    by_name: &BTreeMap<String, usize>,
) -> Result<(Vec<u8>, bool)> {
    let mut tuple = Vec::new();
    let mut any_null = false;

    for key_column in &index.key_columns {
        let (idx, column) = table_column_by_name(columns, by_name, key_column.as_str())?;
        let value = row_values
            .get(idx)
            .ok_or_else(|| anyhow!("row value index {} out of bounds", idx))?;
        if scalar_is_null(value) {
            tuple.push(KEY_NULL_MARKER);
            any_null = true;
            continue;
        }

        let payload = encode_scalar_payload_for_column(value, column)?.ok_or_else(|| {
            anyhow!(
                "non-null scalar encoded to null payload for '{}': impossible",
                key_column
            )
        })?;
        tuple.push(KEY_NOT_NULL_MARKER);
        tuple.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        tuple.extend_from_slice(&payload);
    }

    Ok((tuple, any_null))
}

pub fn encode_secondary_index_lookup_prefix(
    index: &SecondaryIndexRecord,
    columns: &[TableColumnRecord],
    row_values: &[ScalarValue],
) -> Result<Vec<u8>> {
    let by_name = columns
        .iter()
        .enumerate()
        .map(|(idx, c)| (c.name.to_ascii_lowercase(), idx))
        .collect::<BTreeMap<_, _>>();
    let (tuple, _any_null) = encode_index_tuple_bytes(index, columns, row_values, &by_name)?;

    let mut out = Vec::with_capacity(1 + 8 + 8 + tuple.len() + 3);
    out.push(DATA_PREFIX_SECONDARY_INDEX_ROW);
    out.extend_from_slice(&index.table_id.to_be_bytes());
    out.extend_from_slice(&index.index_id.to_be_bytes());

    if index.distribution == SecondaryIndexDistribution::Hash {
        let bucket_count = index.hash_bucket_count.unwrap_or(1).max(1);
        out.push(HASH_BUCKET_TAG);
        out.extend_from_slice(&hash_bucket(tuple.as_slice(), bucket_count).to_be_bytes());
    }

    out.extend_from_slice(&tuple);
    Ok(out)
}

pub fn encode_secondary_index_lookup_prefix_for_prefix(
    index: &SecondaryIndexRecord,
    columns: &[TableColumnRecord],
    key_prefix_values: &[ScalarValue],
) -> Result<Vec<u8>> {
    if key_prefix_values.is_empty() {
        return Err(anyhow!(
            "secondary index lookup prefix requires at least one key value"
        ));
    }
    if key_prefix_values.len() > index.key_columns.len() {
        return Err(anyhow!(
            "secondary index lookup prefix has {} values but index '{}' has {} key columns",
            key_prefix_values.len(),
            index.index_name,
            index.key_columns.len()
        ));
    }
    if index.distribution == SecondaryIndexDistribution::Hash
        && key_prefix_values.len() != index.key_columns.len()
    {
        return Err(anyhow!(
            "hash secondary index '{}' requires full key prefix ({} values), got {}",
            index.index_name,
            index.key_columns.len(),
            key_prefix_values.len()
        ));
    }

    let by_name = columns
        .iter()
        .enumerate()
        .map(|(idx, c)| (c.name.to_ascii_lowercase(), idx))
        .collect::<BTreeMap<_, _>>();

    let mut tuple = Vec::<u8>::new();
    for (idx, key_column) in index
        .key_columns
        .iter()
        .take(key_prefix_values.len())
        .enumerate()
    {
        let (_, column) = table_column_by_name(columns, &by_name, key_column.as_str())?;
        let value = key_prefix_values
            .get(idx)
            .ok_or_else(|| anyhow!("key prefix value index {} out of bounds", idx))?;
        if scalar_is_null(value) {
            tuple.push(KEY_NULL_MARKER);
            continue;
        }
        let payload = encode_scalar_payload_for_column(value, column)?.ok_or_else(|| {
            anyhow!(
                "non-null scalar encoded to null payload for '{}': impossible",
                key_column
            )
        })?;
        tuple.push(KEY_NOT_NULL_MARKER);
        tuple.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        tuple.extend_from_slice(&payload);
    }

    let mut out = Vec::with_capacity(1 + 8 + 8 + tuple.len() + 3);
    out.push(DATA_PREFIX_SECONDARY_INDEX_ROW);
    out.extend_from_slice(&index.table_id.to_be_bytes());
    out.extend_from_slice(&index.index_id.to_be_bytes());

    if index.distribution == SecondaryIndexDistribution::Hash {
        let bucket_count = index.hash_bucket_count.unwrap_or(1).max(1);
        out.push(HASH_BUCKET_TAG);
        out.extend_from_slice(&hash_bucket(tuple.as_slice(), bucket_count).to_be_bytes());
    }

    out.extend_from_slice(&tuple);
    Ok(out)
}

pub fn encode_secondary_index_row_key(
    index: &SecondaryIndexRecord,
    columns: &[TableColumnRecord],
    row_values: &[ScalarValue],
    primary_key: i64,
) -> Result<Vec<u8>> {
    let mut out = encode_secondary_index_lookup_prefix(index, columns, row_values)?;
    out.push(TUPLE_TAG_PRIMARY_KEY);
    out.push(0);
    out.extend_from_slice(&encode_i64_ordered(primary_key));
    Ok(out)
}

pub fn encode_secondary_index_unique_key(
    index: &SecondaryIndexRecord,
    columns: &[TableColumnRecord],
    row_values: &[ScalarValue],
) -> Result<Option<Vec<u8>>> {
    if !index.unique {
        return Ok(None);
    }

    let by_name = columns
        .iter()
        .enumerate()
        .map(|(idx, c)| (c.name.to_ascii_lowercase(), idx))
        .collect::<BTreeMap<_, _>>();
    let (tuple, any_null) = encode_index_tuple_bytes(index, columns, row_values, &by_name)?;
    // Match PostgreSQL default UNIQUE behavior (`NULLS DISTINCT`): rows with
    // any NULL key component do not conflict with each other.
    if any_null {
        return Ok(None);
    }

    let mut out = Vec::with_capacity(1 + 8 + 8 + tuple.len() + 3);
    out.push(DATA_PREFIX_SECONDARY_INDEX_UNIQUE);
    out.extend_from_slice(&index.table_id.to_be_bytes());
    out.extend_from_slice(&index.index_id.to_be_bytes());
    if index.distribution == SecondaryIndexDistribution::Hash {
        let bucket_count = index.hash_bucket_count.unwrap_or(1).max(1);
        out.push(HASH_BUCKET_TAG);
        out.extend_from_slice(&hash_bucket(tuple.as_slice(), bucket_count).to_be_bytes());
    }
    out.extend_from_slice(&tuple);
    Ok(Some(out))
}

pub fn encode_secondary_index_row_value(
    index: &SecondaryIndexRecord,
    columns: &[TableColumnRecord],
    row_values: &[ScalarValue],
) -> Result<Vec<u8>> {
    let by_name = columns
        .iter()
        .enumerate()
        .map(|(idx, c)| (c.name.to_ascii_lowercase(), idx))
        .collect::<BTreeMap<_, _>>();

    let mut payloads = Vec::with_capacity(index.include_columns.len());
    for include in &index.include_columns {
        let (idx, column) = table_column_by_name(columns, &by_name, include.as_str())?;
        let value = row_values
            .get(idx)
            .ok_or_else(|| anyhow!("row value index {} out of bounds", idx))?;
        payloads.push(encode_scalar_payload_for_column(value, column)?);
    }

    Ok(encode_v2_payload(payloads))
}

pub fn encode_secondary_index_unique_value(primary_key: i64) -> Vec<u8> {
    let payloads = vec![Some(primary_key.to_be_bytes().to_vec())];
    encode_v2_payload(payloads)
}

fn encode_v2_payload(payloads: Vec<Option<Vec<u8>>>) -> Vec<u8> {
    let column_count = payloads.len() as u16;
    let null_bitmap_len = payloads.len().div_ceil(8);
    let mut null_bitmap = vec![0u8; null_bitmap_len];
    for (idx, payload) in payloads.iter().enumerate() {
        if payload.is_none() {
            let byte_idx = idx / 8;
            let bit_idx = idx % 8;
            null_bitmap[byte_idx] |= 1u8 << bit_idx;
        }
    }

    let mut out = Vec::new();
    out.push(ROW_FORMAT_VERSION_V2);
    out.push(0);
    out.extend_from_slice(&column_count.to_be_bytes());
    out.extend_from_slice(&(null_bitmap_len as u16).to_be_bytes());
    out.extend_from_slice(&null_bitmap);
    for payload in payloads {
        if let Some(payload) = payload {
            out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            out.extend_from_slice(&payload);
        }
    }
    out
}

pub fn index_tombstone_value() -> Vec<u8> {
    vec![ROW_FORMAT_VERSION_V2, ROW_FLAG_TOMBSTONE]
}

pub fn is_sql_tombstone_value(value: &[u8]) -> bool {
    value.len() >= 2 && value[0] == ROW_FORMAT_VERSION_V2 && (value[1] & ROW_FLAG_TOMBSTONE != 0)
}

pub fn decode_secondary_index_primary_key(index: &SecondaryIndexRecord, key: &[u8]) -> Result<i64> {
    if key.len() < 1 + 8 + 8 + 10 {
        return Err(anyhow!("secondary index key too short"));
    }

    let mut cursor = 0usize;
    let prefix = key[cursor];
    cursor += 1;
    if prefix != DATA_PREFIX_SECONDARY_INDEX_ROW {
        return Err(anyhow!("invalid secondary index row-key prefix {}", prefix));
    }

    let mut table_bytes = [0u8; 8];
    table_bytes.copy_from_slice(&key[cursor..cursor + 8]);
    cursor += 8;
    let table_id = u64::from_be_bytes(table_bytes);
    if table_id != index.table_id {
        return Err(anyhow!(
            "secondary index key table_id mismatch: expected={}, got={}",
            index.table_id,
            table_id
        ));
    }

    let mut index_bytes = [0u8; 8];
    index_bytes.copy_from_slice(&key[cursor..cursor + 8]);
    cursor += 8;
    let index_id = u64::from_be_bytes(index_bytes);
    if index_id != index.index_id {
        return Err(anyhow!(
            "secondary index key index_id mismatch: expected={}, got={}",
            index.index_id,
            index_id
        ));
    }

    if index.distribution == SecondaryIndexDistribution::Hash {
        if key.len() < cursor + 3 + 10 {
            return Err(anyhow!("secondary index hash key too short"));
        }
        if key[cursor] != HASH_BUCKET_TAG {
            return Err(anyhow!(
                "secondary index key missing hash bucket tag for hash distribution"
            ));
        }
    }

    if key.len() < 10 {
        return Err(anyhow!("secondary index key missing primary key suffix"));
    }
    let suffix_start = key.len() - 10;
    if key[suffix_start] != TUPLE_TAG_PRIMARY_KEY || key[suffix_start + 1] != 0 {
        return Err(anyhow!(
            "secondary index key has invalid primary key suffix"
        ));
    }
    let mut payload = [0u8; 8];
    payload.copy_from_slice(&key[(suffix_start + 2)..]);
    Ok(decode_i64_ordered(payload))
}

pub fn decode_secondary_index_row_values(
    index: &SecondaryIndexRecord,
    columns: &[TableColumnRecord],
    primary_key_column: &str,
    key: &[u8],
    value: &[u8],
) -> Result<Vec<ScalarValue>> {
    let mut by_name = BTreeMap::<String, usize>::new();
    for (idx, column) in columns.iter().enumerate() {
        by_name.insert(column.name.to_ascii_lowercase(), idx);
    }
    let mut row = vec![ScalarValue::Null; columns.len()];

    let primary_key = decode_secondary_index_primary_key(index, key)?;
    let primary_key_idx = by_name
        .get(&primary_key_column.to_ascii_lowercase())
        .copied()
        .ok_or_else(|| {
            anyhow!(
                "primary key column '{}' not found while decoding index '{}'",
                primary_key_column,
                index.index_name
            )
        })?;
    row[primary_key_idx] = decode_primary_key_scalar(primary_key, &columns[primary_key_idx])?;

    let mut cursor = 0usize;
    if key.get(cursor).copied() != Some(DATA_PREFIX_SECONDARY_INDEX_ROW) {
        return Err(anyhow!("invalid secondary index row-key prefix"));
    }
    cursor += 1;
    cursor = cursor.saturating_add(8); // table id
    cursor = cursor.saturating_add(8); // index id
    if index.distribution == SecondaryIndexDistribution::Hash {
        if key.get(cursor).copied() != Some(HASH_BUCKET_TAG) {
            return Err(anyhow!(
                "secondary index key missing hash bucket tag for hash distribution"
            ));
        }
        cursor += 1;
        cursor = cursor.saturating_add(2); // bucket
    }

    for key_column in &index.key_columns {
        let column_idx = by_name
            .get(&key_column.to_ascii_lowercase())
            .copied()
            .ok_or_else(|| {
                anyhow!(
                    "index key column '{}' missing from table metadata",
                    key_column
                )
            })?;
        let marker = *key
            .get(cursor)
            .ok_or_else(|| anyhow!("secondary index key truncated while decoding marker"))?;
        cursor += 1;
        if marker == KEY_NULL_MARKER {
            row[column_idx] = ScalarValue::Null;
            continue;
        }
        if marker != KEY_NOT_NULL_MARKER {
            return Err(anyhow!("invalid secondary index key marker {}", marker));
        }
        if key.len() < cursor + 4 {
            return Err(anyhow!(
                "secondary index key truncated while decoding payload length"
            ));
        }
        let mut length_bytes = [0u8; 4];
        length_bytes.copy_from_slice(&key[cursor..cursor + 4]);
        cursor += 4;
        let payload_len = u32::from_be_bytes(length_bytes) as usize;
        if key.len() < cursor + payload_len {
            return Err(anyhow!(
                "secondary index key truncated while decoding payload"
            ));
        }
        let payload = &key[cursor..cursor + payload_len];
        cursor += payload_len;
        row[column_idx] = decode_scalar_payload_for_column(Some(payload), &columns[column_idx])?;
    }

    let include_payloads = decode_v2_payload(value, index.include_columns.len())?;
    for (include_column, payload) in index
        .include_columns
        .iter()
        .zip(include_payloads.into_iter())
    {
        let column_idx = by_name
            .get(&include_column.to_ascii_lowercase())
            .copied()
            .ok_or_else(|| {
                anyhow!(
                    "index include column '{}' missing from table metadata",
                    include_column
                )
            })?;
        row[column_idx] =
            decode_scalar_payload_for_column(payload.as_deref(), &columns[column_idx])?;
    }

    Ok(row)
}

fn decode_primary_key_scalar(primary_key: i64, column: &TableColumnRecord) -> Result<ScalarValue> {
    match column.column_type {
        TableColumnType::Int8 => i8::try_from(primary_key)
            .map(|v| ScalarValue::Int8(Some(v)))
            .map_err(|_| anyhow!("primary key out of range for Int8 column '{}'", column.name)),
        TableColumnType::Int16 => i16::try_from(primary_key)
            .map(|v| ScalarValue::Int16(Some(v)))
            .map_err(|_| {
                anyhow!(
                    "primary key out of range for Int16 column '{}'",
                    column.name
                )
            }),
        TableColumnType::Int32 => i32::try_from(primary_key)
            .map(|v| ScalarValue::Int32(Some(v)))
            .map_err(|_| {
                anyhow!(
                    "primary key out of range for Int32 column '{}'",
                    column.name
                )
            }),
        TableColumnType::Int64 => Ok(ScalarValue::Int64(Some(primary_key))),
        _ => Err(anyhow!(
            "primary key column '{}' must be signed integer",
            column.name
        )),
    }
}

fn decode_scalar_payload_for_column(
    payload: Option<&[u8]>,
    column: &TableColumnRecord,
) -> Result<ScalarValue> {
    let Some(payload) = payload else {
        if column.nullable {
            return Ok(ScalarValue::Null);
        }
        return Err(anyhow!(
            "missing non-nullable payload for column '{}'",
            column.name
        ));
    };

    match column.column_type {
        TableColumnType::Int8 => {
            if payload.len() != 1 {
                return Err(anyhow!("invalid Int8 payload length for '{}'", column.name));
            }
            Ok(ScalarValue::Int8(Some(payload[0] as i8)))
        }
        TableColumnType::Int16 => {
            if payload.len() != 2 {
                return Err(anyhow!(
                    "invalid Int16 payload length for '{}'",
                    column.name
                ));
            }
            let mut bytes = [0u8; 2];
            bytes.copy_from_slice(payload);
            Ok(ScalarValue::Int16(Some(i16::from_be_bytes(bytes))))
        }
        TableColumnType::Int32 => {
            if payload.len() != 4 {
                return Err(anyhow!(
                    "invalid Int32 payload length for '{}'",
                    column.name
                ));
            }
            let mut bytes = [0u8; 4];
            bytes.copy_from_slice(payload);
            Ok(ScalarValue::Int32(Some(i32::from_be_bytes(bytes))))
        }
        TableColumnType::Int64 => {
            if payload.len() != 8 {
                return Err(anyhow!(
                    "invalid Int64 payload length for '{}'",
                    column.name
                ));
            }
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(payload);
            Ok(ScalarValue::Int64(Some(i64::from_be_bytes(bytes))))
        }
        TableColumnType::UInt8 => {
            if payload.len() != 1 {
                return Err(anyhow!(
                    "invalid UInt8 payload length for '{}'",
                    column.name
                ));
            }
            Ok(ScalarValue::UInt8(Some(payload[0])))
        }
        TableColumnType::UInt16 => {
            if payload.len() != 2 {
                return Err(anyhow!(
                    "invalid UInt16 payload length for '{}'",
                    column.name
                ));
            }
            let mut bytes = [0u8; 2];
            bytes.copy_from_slice(payload);
            Ok(ScalarValue::UInt16(Some(u16::from_be_bytes(bytes))))
        }
        TableColumnType::UInt32 => {
            if payload.len() != 4 {
                return Err(anyhow!(
                    "invalid UInt32 payload length for '{}'",
                    column.name
                ));
            }
            let mut bytes = [0u8; 4];
            bytes.copy_from_slice(payload);
            Ok(ScalarValue::UInt32(Some(u32::from_be_bytes(bytes))))
        }
        TableColumnType::UInt64 => {
            if payload.len() != 8 {
                return Err(anyhow!(
                    "invalid UInt64 payload length for '{}'",
                    column.name
                ));
            }
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(payload);
            Ok(ScalarValue::UInt64(Some(u64::from_be_bytes(bytes))))
        }
        TableColumnType::Float64 => {
            if payload.len() != 8 {
                return Err(anyhow!(
                    "invalid Float64 payload length for '{}'",
                    column.name
                ));
            }
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(payload);
            Ok(ScalarValue::Float64(Some(f64::from_be_bytes(bytes))))
        }
        TableColumnType::Boolean => {
            if payload.len() != 1 {
                return Err(anyhow!(
                    "invalid Boolean payload length for '{}'",
                    column.name
                ));
            }
            Ok(ScalarValue::Boolean(Some(payload[0] != 0)))
        }
        TableColumnType::Utf8 => {
            let value = std::str::from_utf8(payload)
                .map_err(|err| anyhow!("invalid Utf8 payload for '{}': {}", column.name, err))?;
            Ok(ScalarValue::Utf8(Some(value.to_string())))
        }
        TableColumnType::TimestampNanosecond => {
            if payload.len() != 8 {
                return Err(anyhow!(
                    "invalid Timestamp payload length for '{}'",
                    column.name
                ));
            }
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(payload);
            Ok(ScalarValue::TimestampNanosecond(
                Some(i64::from_be_bytes(bytes)),
                None,
            ))
        }
    }
}

fn decode_v2_payload(bytes: &[u8], column_count: usize) -> Result<Vec<Option<Vec<u8>>>> {
    if bytes.len() < 2 {
        return Err(anyhow!("row value too short"));
    }
    if bytes[0] != ROW_FORMAT_VERSION_V2 {
        return Err(anyhow!(
            "unsupported row format version {} while decoding index value",
            bytes[0]
        ));
    }
    if bytes[1] & ROW_FLAG_TOMBSTONE != 0 {
        return Ok(vec![None; column_count]);
    }

    let mut cursor = 2usize;
    let payload_column_count = read_u16(bytes, &mut cursor)? as usize;
    if payload_column_count != column_count {
        return Err(anyhow!(
            "index payload column count mismatch: expected={}, got={}",
            column_count,
            payload_column_count
        ));
    }
    let null_bitmap_len = read_u16(bytes, &mut cursor)? as usize;
    if bytes.len() < cursor + null_bitmap_len {
        return Err(anyhow!("index payload null bitmap truncated"));
    }
    let null_bitmap = &bytes[cursor..cursor + null_bitmap_len];
    cursor += null_bitmap_len;

    let mut out = Vec::with_capacity(column_count);
    for column_idx in 0..column_count {
        if is_null(null_bitmap, column_idx) {
            out.push(None);
            continue;
        }
        let payload_len = read_u32(bytes, &mut cursor)? as usize;
        if bytes.len() < cursor + payload_len {
            return Err(anyhow!("index payload value truncated"));
        }
        out.push(Some(bytes[cursor..cursor + payload_len].to_vec()));
        cursor += payload_len;
    }
    Ok(out)
}

fn read_u16(bytes: &[u8], cursor: &mut usize) -> Result<u16> {
    if bytes.len() < *cursor + 2 {
        return Err(anyhow!("buffer underflow while decoding u16"));
    }
    let mut out = [0u8; 2];
    out.copy_from_slice(&bytes[*cursor..*cursor + 2]);
    *cursor += 2;
    Ok(u16::from_be_bytes(out))
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32> {
    if bytes.len() < *cursor + 4 {
        return Err(anyhow!("buffer underflow while decoding u32"));
    }
    let mut out = [0u8; 4];
    out.copy_from_slice(&bytes[*cursor..*cursor + 4]);
    *cursor += 4;
    Ok(u32::from_be_bytes(out))
}

fn is_null(bitmap: &[u8], column_idx: usize) -> bool {
    let byte_idx = column_idx / 8;
    let bit_idx = column_idx % 8;
    if byte_idx >= bitmap.len() {
        return false;
    }
    (bitmap[byte_idx] & (1u8 << bit_idx)) != 0
}
