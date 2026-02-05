//! Snapshot/compaction recovery test (placeholder).
//!
//! This test should verify that recovery succeeds across a snapshot boundary
//! and that WAL GC does not drop required state. It is ignored until snapshotting
//! is implemented.

#[test]
#[ignore = "snapshotting/compaction not implemented yet"]
fn snapshot_recovery_boundary() {
    // TODO: implement once snapshotting exists.
}
