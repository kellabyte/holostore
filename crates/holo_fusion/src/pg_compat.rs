//! PostgreSQL compatibility helpers layered onto DataFusion.
//!
//! Some tools (for example IDEs) probe PostgreSQL-specific functions at
//! connect-time. This module registers small compatibility UDFs that keep those
//! probes from failing.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{create_udf, ScalarUDF, Volatility};
use datafusion::physical_plan::ColumnarValue;
use datafusion::prelude::SessionContext;

/// Registers PostgreSQL compatibility UDFs in the provided session context.
pub fn register_pg_compat_udfs(session_context: &SessionContext, postmaster_start_time_ns: i64) {
    session_context.register_udf(create_pg_postmaster_start_time_udf(
        postmaster_start_time_ns,
    ));
}

/// Returns the current UNIX timestamp in nanoseconds.
pub fn current_unix_timestamp_ns() -> Result<i64> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before UNIX_EPOCH")?;
    i64::try_from(elapsed.as_nanos()).context("unix timestamp nanos overflowed i64")
}

/// Creates a stable `pg_postmaster_start_time()` UDF bound to process start.
fn create_pg_postmaster_start_time_udf(postmaster_start_time_ns: i64) -> ScalarUDF {
    // Decision: ignore call arguments because PostgreSQL's
    // `pg_postmaster_start_time()` has no parameters and always returns the
    // server boot time.
    let func = move |_args: &[ColumnarValue]| {
        Ok(ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
            Some(postmaster_start_time_ns),
            None,
        )))
    };

    create_udf(
        "pg_postmaster_start_time",
        vec![],
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        Volatility::Stable,
        Arc::new(func),
    )
}
