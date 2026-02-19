//! Secondary-index subsystem for HoloFusion.
//!
//! This module keeps index-specific logic isolated from core table/provider
//! codepaths. It owns:
//! - persisted index metadata records,
//! - secondary-key encoding/decoding helpers,
//! - row-to-index mutation planning helpers used by write paths.

mod keys;
mod metadata;

pub use keys::{
    decode_secondary_index_primary_key, decode_secondary_index_row_values,
    encode_secondary_index_lookup_prefix, encode_secondary_index_lookup_prefix_for_prefix,
    encode_secondary_index_row_key, encode_secondary_index_row_value,
    encode_secondary_index_unique_key, encode_secondary_index_unique_value, index_tombstone_value,
    is_sql_tombstone_value,
};
pub use metadata::{
    create_secondary_index_metadata, drop_secondary_index_metadata_by_name,
    list_secondary_index_metadata, list_secondary_index_metadata_for_table,
    update_secondary_index_state, CreateSecondaryIndexMetadataOutcome,
    CreateSecondaryIndexMetadataSpec, SecondaryIndexDistribution, SecondaryIndexRecord,
    SecondaryIndexState, DEFAULT_SECONDARY_INDEX_HASH_BUCKETS, MAX_SECONDARY_INDEX_HASH_BUCKETS,
};
