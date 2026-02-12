//! PostgreSQL compatibility helpers layered onto DataFusion.
//!
//! Some tools (for example IDEs) probe PostgreSQL-specific functions at
//! connect-time. This module registers compatibility UDFs that keep those
//! probes from failing while preserving stable server-side semantics.

use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use chrono::{Datelike, NaiveDate, Timelike, Utc};
use datafusion::arrow::array::types::IntervalMonthDayNano;
use datafusion::arrow::array::{Array, ArrayRef, Int64Array, IntervalMonthDayNanoArray};
use datafusion::arrow::datatypes::{DataType, IntervalMonthDayNanoType, IntervalUnit, TimeUnit};
use datafusion::common::cast::{
    as_date32_array, as_date64_array, as_int32_array, as_int64_array,
    as_timestamp_microsecond_array, as_timestamp_millisecond_array, as_timestamp_nanosecond_array,
    as_timestamp_second_array, as_uint32_array, as_uint64_array,
};
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{
    create_udf, ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::physical_plan::ColumnarValue as PhysicalColumnarValue;
use datafusion::prelude::SessionContext;

use crate::mutation::TxnIntrospection;

const NANOS_PER_MICRO: i64 = 1_000;
const NANOS_PER_MILLI: i64 = 1_000_000;
const NANOS_PER_SEC: i64 = 1_000_000_000;
const NANOS_PER_DAY: i64 = 86_400_000_000_000;

/// Registers PostgreSQL compatibility UDFs in the provided session context.
pub fn register_pg_compat_udfs(
    session_context: &SessionContext,
    postmaster_start_time_ns: i64,
    txn_introspection: TxnIntrospection,
) {
    session_context.register_udf(create_pg_postmaster_start_time_udf(
        postmaster_start_time_ns,
    ));
    session_context.register_udf(create_age_udf(txn_introspection.clone()));
    session_context.register_udf(create_pg_is_in_recovery_udf());
    session_context.register_udf(create_txid_current_udf(txn_introspection));
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
    let func = move |_args: &[PhysicalColumnarValue]| {
        Ok(PhysicalColumnarValue::Scalar(
            ScalarValue::TimestampNanosecond(Some(postmaster_start_time_ns), None),
        ))
    };

    create_udf(
        "pg_postmaster_start_time",
        vec![],
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        Volatility::Stable,
        Arc::new(func),
    )
}

/// Creates `pg_is_in_recovery()` with PostgreSQL-compatible semantics.
fn create_pg_is_in_recovery_udf() -> ScalarUDF {
    // Decision: HoloFusion does not expose PostgreSQL-style startup/WAL-recovery
    // mode to clients, so this node always reports "not in recovery".
    let func = move |_args: &[PhysicalColumnarValue]| {
        Ok(PhysicalColumnarValue::Scalar(ScalarValue::Boolean(Some(
            false,
        ))))
    };

    create_udf(
        "pg_is_in_recovery",
        vec![],
        DataType::Boolean,
        Volatility::Stable,
        Arc::new(func),
    )
}

/// Creates `txid_current()` backed by HoloFusion's transaction-id allocator.
fn create_txid_current_udf(txn_introspection: TxnIntrospection) -> ScalarUDF {
    let func = move |_args: &[PhysicalColumnarValue]| {
        let txid = i64::try_from(txn_introspection.current_txid_hint()).map_err(|_| {
            DataFusionError::Execution("current transaction id exceeded i64".to_string())
        })?;
        Ok(PhysicalColumnarValue::Scalar(ScalarValue::Int64(Some(
            txid,
        ))))
    };

    create_udf(
        "txid_current",
        vec![],
        DataType::Int64,
        Volatility::Volatile,
        Arc::new(func),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AgeArgKind {
    Temporal,
    Xid,
}

#[derive(Debug, Clone)]
struct PgAgeUdf {
    signature: Signature,
    txn_introspection: TxnIntrospection,
}

impl PgAgeUdf {
    fn new(txn_introspection: TxnIntrospection) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Stable),
            txn_introspection,
        }
    }

    fn validate_arg_types(&self, arg_types: &[DataType]) -> DFResult<AgeArgKind> {
        if !(arg_types.len() == 1 || arg_types.len() == 2) {
            return Err(DataFusionError::Plan(format!(
                "age() expects 1 or 2 arguments, got {}",
                arg_types.len()
            )));
        }

        let temporal = arg_types.iter().all(is_temporal_arg_type);
        if temporal {
            return Ok(AgeArgKind::Temporal);
        }

        let xid = arg_types.iter().all(is_xid_arg_type);
        if xid {
            return Ok(AgeArgKind::Xid);
        }

        Err(DataFusionError::Plan(format!(
            "age() argument types are not supported: {arg_types:?}"
        )))
    }
}

impl PartialEq for PgAgeUdf {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for PgAgeUdf {}

impl Hash for PgAgeUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "pg_catalog.age".hash(state);
    }
}

impl ScalarUDFImpl for PgAgeUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "age"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        match self.validate_arg_types(arg_types)? {
            AgeArgKind::Temporal => Ok(DataType::Interval(IntervalUnit::MonthDayNano)),
            AgeArgKind::Xid => Ok(DataType::Int64),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        self.validate_arg_types(arg_types)?;
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let arg_types = arrays
            .iter()
            .map(|array| array.data_type().clone())
            .collect::<Vec<_>>();

        match self.validate_arg_types(arg_types.as_slice())? {
            AgeArgKind::Temporal => {
                let result = evaluate_temporal_age(arrays.as_slice())?;
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            AgeArgKind::Xid => {
                let current_xid = i64::try_from(self.txn_introspection.current_txid_hint())
                    .map_err(|_| {
                        DataFusionError::Execution("current xid exceeded i64".to_string())
                    })?;
                let result = evaluate_xid_age(arrays.as_slice(), current_xid)?;
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
        }
    }
}

fn create_age_udf(txn_introspection: TxnIntrospection) -> ScalarUDF {
    ScalarUDF::new_from_impl(PgAgeUdf::new(txn_introspection))
}

fn is_temporal_arg_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(TimeUnit::Second, _)
            | DataType::Timestamp(TimeUnit::Millisecond, _)
            | DataType::Timestamp(TimeUnit::Microsecond, _)
            | DataType::Timestamp(TimeUnit::Nanosecond, _)
    )
}

fn is_xid_arg_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int32 | DataType::Int64 | DataType::UInt32 | DataType::UInt64
    )
}

fn evaluate_temporal_age(arrays: &[ArrayRef]) -> DFResult<IntervalMonthDayNanoArray> {
    let len = arrays
        .first()
        .map(|array| array.len())
        .ok_or_else(|| DataFusionError::Execution("age() called without arguments".to_string()))?;
    let current_midnight_ns = current_utc_midnight_ns()?;

    let mut values: Vec<Option<IntervalMonthDayNano>> = Vec::with_capacity(len);
    for idx in 0..len {
        let left = temporal_value_as_ns(&arrays[0], idx)?;
        let value = if arrays.len() == 1 {
            left.map(|arg| compute_symbolic_age_interval(current_midnight_ns, arg))
                .transpose()?
        } else {
            let right = temporal_value_as_ns(&arrays[1], idx)?;
            match (left, right) {
                (Some(lhs), Some(rhs)) => Some(compute_symbolic_age_interval(lhs, rhs)?),
                _ => None,
            }
        };
        values.push(value);
    }

    Ok(IntervalMonthDayNanoArray::from(values))
}

fn evaluate_xid_age(arrays: &[ArrayRef], current_xid: i64) -> DFResult<Int64Array> {
    let len = arrays
        .first()
        .map(|array| array.len())
        .ok_or_else(|| DataFusionError::Execution("age() called without arguments".to_string()))?;
    let mut values = Vec::with_capacity(len);

    for idx in 0..len {
        let left = xid_value_as_i64(&arrays[0], idx)?;
        let value = if arrays.len() == 1 {
            left.map(|xid| current_xid.saturating_sub(xid))
        } else {
            let right = xid_value_as_i64(&arrays[1], idx)?;
            match (left, right) {
                (Some(lhs), Some(rhs)) => Some(lhs.saturating_sub(rhs)),
                _ => None,
            }
        };
        values.push(value);
    }

    Ok(Int64Array::from(values))
}

fn temporal_value_as_ns(array: &ArrayRef, index: usize) -> DFResult<Option<i64>> {
    match array.data_type() {
        DataType::Date32 => {
            let values = as_date32_array(array.as_ref())?;
            if values.is_null(index) {
                Ok(None)
            } else {
                let days = values.value(index) as i64;
                days.checked_mul(NANOS_PER_DAY).map(Some).ok_or_else(|| {
                    DataFusionError::Execution("date32 to nanos overflow".to_string())
                })
            }
        }
        DataType::Date64 => {
            let values = as_date64_array(array.as_ref())?;
            if values.is_null(index) {
                Ok(None)
            } else {
                let millis = values.value(index);
                millis
                    .checked_mul(NANOS_PER_MILLI)
                    .map(Some)
                    .ok_or_else(|| {
                        DataFusionError::Execution("date64 to nanos overflow".to_string())
                    })
            }
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let values = as_timestamp_second_array(array.as_ref())?;
            if values.is_null(index) {
                Ok(None)
            } else {
                values
                    .value(index)
                    .checked_mul(NANOS_PER_SEC)
                    .map(Some)
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "timestamp(second) to nanos overflow".to_string(),
                        )
                    })
            }
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let values = as_timestamp_millisecond_array(array.as_ref())?;
            if values.is_null(index) {
                Ok(None)
            } else {
                values
                    .value(index)
                    .checked_mul(NANOS_PER_MILLI)
                    .map(Some)
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "timestamp(millisecond) to nanos overflow".to_string(),
                        )
                    })
            }
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let values = as_timestamp_microsecond_array(array.as_ref())?;
            if values.is_null(index) {
                Ok(None)
            } else {
                values
                    .value(index)
                    .checked_mul(NANOS_PER_MICRO)
                    .map(Some)
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "timestamp(microsecond) to nanos overflow".to_string(),
                        )
                    })
            }
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let values = as_timestamp_nanosecond_array(array.as_ref())?;
            if values.is_null(index) {
                Ok(None)
            } else {
                Ok(Some(values.value(index)))
            }
        }
        other => Err(DataFusionError::Execution(format!(
            "age() temporal argument type is not supported: {other:?}"
        ))),
    }
}

fn xid_value_as_i64(array: &ArrayRef, index: usize) -> DFResult<Option<i64>> {
    match array.data_type() {
        DataType::Int32 => {
            let values = as_int32_array(array.as_ref())?;
            if values.is_null(index) {
                Ok(None)
            } else {
                Ok(Some(values.value(index) as i64))
            }
        }
        DataType::Int64 => {
            let values = as_int64_array(array.as_ref())?;
            if values.is_null(index) {
                Ok(None)
            } else {
                Ok(Some(values.value(index)))
            }
        }
        DataType::UInt32 => {
            let values = as_uint32_array(array.as_ref())?;
            if values.is_null(index) {
                Ok(None)
            } else {
                Ok(Some(values.value(index) as i64))
            }
        }
        DataType::UInt64 => {
            let values = as_uint64_array(array.as_ref())?;
            if values.is_null(index) {
                Ok(None)
            } else {
                let value = values.value(index);
                i64::try_from(value).map(Some).map_err(|_| {
                    DataFusionError::Execution("uint64 xid exceeds i64 range".to_string())
                })
            }
        }
        other => Err(DataFusionError::Execution(format!(
            "age() xid argument type is not supported: {other:?}"
        ))),
    }
}

fn current_utc_midnight_ns() -> DFResult<i64> {
    let today = Utc::now().date_naive();
    let midnight = today.and_hms_opt(0, 0, 0).ok_or_else(|| {
        DataFusionError::Execution("failed to build current UTC midnight".to_string())
    })?;
    midnight.and_utc().timestamp_nanos_opt().ok_or_else(|| {
        DataFusionError::Execution("current UTC midnight exceeds i64 nanos".to_string())
    })
}

fn compute_symbolic_age_interval(end_ns: i64, start_ns: i64) -> DFResult<IntervalMonthDayNano> {
    let (sign, lhs_ns, rhs_ns) = if end_ns >= start_ns {
        (1, end_ns, start_ns)
    } else {
        (-1, start_ns, end_ns)
    };

    let lhs = chrono::DateTime::<Utc>::from_timestamp_nanos(lhs_ns).naive_utc();
    let rhs = chrono::DateTime::<Utc>::from_timestamp_nanos(rhs_ns).naive_utc();

    let mut years = lhs.year() - rhs.year();
    let mut months = lhs.month() as i32 - rhs.month() as i32;
    let mut days = lhs.day() as i32 - rhs.day() as i32;

    let lhs_time_ns = lhs.time().num_seconds_from_midnight() as i64 * NANOS_PER_SEC
        + lhs.time().nanosecond() as i64;
    let rhs_time_ns = rhs.time().num_seconds_from_midnight() as i64 * NANOS_PER_SEC
        + rhs.time().nanosecond() as i64;
    let mut nanos = lhs_time_ns - rhs_time_ns;

    if nanos < 0 {
        nanos += NANOS_PER_DAY;
        days -= 1;
    }
    if days < 0 {
        months -= 1;
        let (borrow_year, borrow_month) = previous_month(lhs.year(), lhs.month());
        days += days_in_month(borrow_year, borrow_month) as i32;
    }
    if months < 0 {
        years -= 1;
        months += 12;
    }

    let mut total_months = years.saturating_mul(12).saturating_add(months);
    if sign < 0 {
        total_months = -total_months;
        days = -days;
        nanos = -nanos;
    }

    Ok(IntervalMonthDayNanoType::make_value(
        total_months,
        days,
        nanos,
    ))
}

fn previous_month(year: i32, month: u32) -> (i32, u32) {
    if month == 1 {
        (year - 1, 12)
    } else {
        (year, month - 1)
    }
}

fn days_in_month(year: i32, month: u32) -> u32 {
    let first = NaiveDate::from_ymd_opt(year, month, 1)
        .expect("previous_month should always return valid year/month");
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    let next = NaiveDate::from_ymd_opt(next_year, next_month, 1)
        .expect("computed next month should always be valid");
    (next - first).num_days() as u32
}
