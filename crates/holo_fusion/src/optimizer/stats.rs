use std::collections::BTreeMap;

use datafusion::common::ScalarValue;

use crate::indexing::SecondaryIndexRecord;
use crate::metadata::TableColumnRecord;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StatsScalar {
    Null,
    Int(i128),
    Bool(bool),
    Utf8(String),
    FloatBits(u64),
}

impl StatsScalar {
    pub fn from_scalar(value: &ScalarValue) -> Option<Self> {
        match value {
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
            | ScalarValue::TimestampSecond(None, _) => Some(Self::Null),
            ScalarValue::Int8(Some(v)) => Some(Self::Int(i128::from(*v))),
            ScalarValue::Int16(Some(v)) => Some(Self::Int(i128::from(*v))),
            ScalarValue::Int32(Some(v)) => Some(Self::Int(i128::from(*v))),
            ScalarValue::Int64(Some(v)) => Some(Self::Int(i128::from(*v))),
            ScalarValue::UInt8(Some(v)) => Some(Self::Int(i128::from(*v))),
            ScalarValue::UInt16(Some(v)) => Some(Self::Int(i128::from(*v))),
            ScalarValue::UInt32(Some(v)) => Some(Self::Int(i128::from(*v))),
            ScalarValue::UInt64(Some(v)) => Some(Self::Int(i128::from(*v))),
            ScalarValue::TimestampNanosecond(Some(v), _) => Some(Self::Int(i128::from(*v))),
            ScalarValue::TimestampMicrosecond(Some(v), _) => Some(Self::Int(i128::from(*v))),
            ScalarValue::TimestampMillisecond(Some(v), _) => Some(Self::Int(i128::from(*v))),
            ScalarValue::TimestampSecond(Some(v), _) => Some(Self::Int(i128::from(*v))),
            ScalarValue::Boolean(Some(v)) => Some(Self::Bool(*v)),
            ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => {
                Some(Self::Utf8(v.clone()))
            }
            ScalarValue::Float32(Some(v)) => Some(Self::FloatBits(f64::from(*v).to_bits())),
            ScalarValue::Float64(Some(v)) => Some(Self::FloatBits(v.to_bits())),
            _ => None,
        }
    }

    pub fn is_orderable(&self) -> bool {
        matches!(
            self,
            Self::Int(_) | Self::Utf8(_) | Self::Bool(_) | Self::FloatBits(_)
        )
    }
}

#[derive(Debug, Clone)]
pub struct McvEntry {
    pub value: StatsScalar,
    pub frequency: f64,
}

#[derive(Debug, Clone)]
pub struct HistogramBin {
    pub lower: StatsScalar,
    pub upper: StatsScalar,
    pub frequency: f64,
}

#[derive(Debug, Clone, Default)]
pub struct ColumnStats {
    pub ndv: u64,
    pub null_fraction: f64,
    pub mcv: Vec<McvEntry>,
    pub histogram: Vec<HistogramBin>,
}

#[derive(Debug, Clone)]
pub struct MultiColumnMcvEntry {
    pub values: Vec<StatsScalar>,
    pub frequency: f64,
}

#[derive(Debug, Clone, Default)]
pub struct MultiColumnStats {
    pub columns: Vec<String>,
    pub ndv: u64,
    pub mcv: Vec<MultiColumnMcvEntry>,
}

#[derive(Debug, Clone, Default)]
pub struct IndexStats {
    pub row_count: u64,
    pub leading_key_ndv: u64,
}

#[derive(Debug, Clone, Default)]
pub struct TableStats {
    pub collected_at_unix_ms: u64,
    pub sample_row_count: usize,
    pub estimated_row_count: u64,
    pub avg_row_width_bytes: u64,
    pub confidence: f64,
    pub columns: BTreeMap<String, ColumnStats>,
    pub indexes: BTreeMap<String, IndexStats>,
    pub multi_column: BTreeMap<String, MultiColumnStats>,
}

pub fn build_table_stats_from_sample(
    columns: &[TableColumnRecord],
    indexes: &[SecondaryIndexRecord],
    sample_rows: &[Vec<ScalarValue>],
    approx_row_count: u64,
    collected_at_unix_ms: u64,
) -> TableStats {
    let sample_count = sample_rows.len();
    let estimated_row_count = approx_row_count.max(sample_count as u64);

    let mut table_stats = TableStats {
        collected_at_unix_ms,
        sample_row_count: sample_count,
        estimated_row_count,
        avg_row_width_bytes: estimate_average_row_width(sample_rows),
        confidence: confidence_for_sample(sample_count, estimated_row_count),
        columns: BTreeMap::new(),
        indexes: BTreeMap::new(),
        multi_column: BTreeMap::new(),
    };

    for (column_idx, column) in columns.iter().enumerate() {
        let mut null_count = 0usize;
        let mut frequencies = BTreeMap::<StatsScalar, usize>::new();

        for row in sample_rows {
            if column_idx >= row.len() {
                continue;
            }
            let Some(value) = StatsScalar::from_scalar(&row[column_idx]) else {
                continue;
            };
            if matches!(value, StatsScalar::Null) {
                null_count = null_count.saturating_add(1);
                continue;
            }
            *frequencies.entry(value).or_default() += 1;
        }

        let non_null_sample = sample_count.saturating_sub(null_count);
        let ndv = estimate_ndv(frequencies.len(), non_null_sample, estimated_row_count);
        let null_fraction = if sample_count == 0 {
            0.0
        } else {
            null_count as f64 / sample_count as f64
        };

        let mut mcv = frequencies
            .iter()
            .map(|(value, count)| McvEntry {
                value: value.clone(),
                frequency: (*count as f64) / (sample_count.max(1) as f64),
            })
            .collect::<Vec<_>>();
        mcv.sort_by(|left, right| {
            right
                .frequency
                .partial_cmp(&left.frequency)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        mcv.truncate(8);

        let histogram = build_histogram(&frequencies, sample_count.max(1));

        table_stats.columns.insert(
            column.name.to_ascii_lowercase(),
            ColumnStats {
                ndv,
                null_fraction,
                mcv,
                histogram,
            },
        );
    }

    for index in indexes {
        let leading_key_ndv = index
            .key_columns
            .first()
            .and_then(|name| table_stats.columns.get(&name.to_ascii_lowercase()))
            .map(|stats| stats.ndv)
            .unwrap_or(0);
        table_stats.indexes.insert(
            index.index_name.to_ascii_lowercase(),
            IndexStats {
                row_count: estimated_row_count,
                leading_key_ndv,
            },
        );

        for prefix_len in 2..=index.key_columns.len().min(3) {
            let prefix_columns = index
                .key_columns
                .iter()
                .take(prefix_len)
                .map(|column| column.to_ascii_lowercase())
                .collect::<Vec<_>>();
            let key = prefix_columns.join(",");
            if table_stats.multi_column.contains_key(&key) {
                continue;
            }

            let mut tuple_frequencies = BTreeMap::<Vec<StatsScalar>, usize>::new();
            for row in sample_rows {
                let mut tuple = Vec::with_capacity(prefix_len);
                let mut supported = true;
                for column in &prefix_columns {
                    let Some(column_idx) = columns
                        .iter()
                        .position(|candidate| candidate.name.eq_ignore_ascii_case(column))
                    else {
                        supported = false;
                        break;
                    };
                    let Some(value) = row.get(column_idx) else {
                        supported = false;
                        break;
                    };
                    let Some(value) = StatsScalar::from_scalar(value) else {
                        supported = false;
                        break;
                    };
                    tuple.push(value);
                }
                if supported {
                    *tuple_frequencies.entry(tuple).or_default() += 1;
                }
            }

            let ndv = estimate_ndv(tuple_frequencies.len(), sample_count, estimated_row_count);
            let mut mcv = tuple_frequencies
                .into_iter()
                .map(|(values, count)| MultiColumnMcvEntry {
                    values,
                    frequency: (count as f64) / (sample_count.max(1) as f64),
                })
                .collect::<Vec<_>>();
            mcv.sort_by(|left, right| {
                right
                    .frequency
                    .partial_cmp(&left.frequency)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            mcv.truncate(8);

            table_stats.multi_column.insert(
                key,
                MultiColumnStats {
                    columns: prefix_columns,
                    ndv,
                    mcv,
                },
            );
        }
    }

    table_stats
}

fn estimate_average_row_width(sample_rows: &[Vec<ScalarValue>]) -> u64 {
    if sample_rows.is_empty() {
        return 128;
    }

    let total_bytes = sample_rows
        .iter()
        .map(|row| row.iter().map(estimate_scalar_width).sum::<u64>())
        .sum::<u64>();
    (total_bytes / sample_rows.len() as u64).max(1)
}

fn estimate_scalar_width(value: &ScalarValue) -> u64 {
    match value {
        ScalarValue::Null => 1,
        ScalarValue::Int8(_) | ScalarValue::UInt8(_) | ScalarValue::Boolean(_) => 1,
        ScalarValue::Int16(_) | ScalarValue::UInt16(_) => 2,
        ScalarValue::Int32(_)
        | ScalarValue::UInt32(_)
        | ScalarValue::Float32(_)
        | ScalarValue::TimestampSecond(_, _)
        | ScalarValue::TimestampMillisecond(_, _)
        | ScalarValue::TimestampMicrosecond(_, _) => 4,
        ScalarValue::Int64(_)
        | ScalarValue::UInt64(_)
        | ScalarValue::Float64(_)
        | ScalarValue::TimestampNanosecond(_, _) => 8,
        ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => v.len() as u64,
        ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) => 1,
        _ => 16,
    }
}

fn confidence_for_sample(sample_count: usize, estimated_row_count: u64) -> f64 {
    if estimated_row_count == 0 {
        return 1.0;
    }
    // Coverage alone underestimates confidence for large tables where we still
    // collect a reasonably sized random sample. Blend coverage with an
    // absolute sample-size signal to keep uncertainty calibrated.
    let coverage_signal = (sample_count as f64 / estimated_row_count as f64)
        .clamp(0.0, 1.0)
        .sqrt();
    let sample_size_signal = ((sample_count as f64) + 1.0).ln() / (8_192.0_f64 + 1.0).ln();
    let blended =
        (coverage_signal * 0.65 + sample_size_signal.clamp(0.0, 1.0) * 0.35).clamp(0.0, 1.0);
    (0.12 + blended * 0.88).clamp(0.12, 1.0)
}

fn estimate_ndv(sample_ndv: usize, sample_size: usize, estimated_row_count: u64) -> u64 {
    if sample_size == 0 || sample_ndv == 0 {
        return 0;
    }
    let sample_ndv = sample_ndv as f64;
    let sample_size = sample_size as f64;
    let estimated_row_count = estimated_row_count as f64;

    let scaled = (sample_ndv / sample_size) * estimated_row_count;
    scaled
        .ceil()
        .max(sample_ndv)
        .min(estimated_row_count)
        .max(1.0) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn confidence_increases_with_sample_count() {
        let small = confidence_for_sample(256, 1_000_000);
        let medium = confidence_for_sample(4_096, 1_000_000);
        let large = confidence_for_sample(16_384, 1_000_000);
        assert!(small < medium);
        assert!(medium < large);
    }

    #[test]
    fn confidence_stays_reasonable_for_large_tables_with_4096_sample() {
        let confidence = confidence_for_sample(4_096, 1_000_000);
        assert!(confidence > 0.40, "confidence={confidence}");
    }

    #[test]
    fn confidence_is_bounded() {
        assert!((0.12..=1.0).contains(&confidence_for_sample(0, 10_000_000)));
        assert_eq!(confidence_for_sample(10_000, 0), 1.0);
        assert_eq!(confidence_for_sample(1_000_000, 1_000_000), 1.0);
    }
}

fn build_histogram(
    frequencies: &BTreeMap<StatsScalar, usize>,
    sample_count: usize,
) -> Vec<HistogramBin> {
    let mut ordered = frequencies
        .iter()
        .filter(|(value, _)| value.is_orderable())
        .flat_map(|(value, count)| std::iter::repeat_n(value.clone(), *count))
        .collect::<Vec<_>>();
    if ordered.len() < 8 {
        return Vec::new();
    }
    ordered.sort();

    let bucket_count = ordered.len().min(16);
    let mut bins = Vec::with_capacity(bucket_count);
    for bucket in 0..bucket_count {
        let start = bucket * ordered.len() / bucket_count;
        let mut end = (bucket + 1) * ordered.len() / bucket_count;
        if end == start {
            end = (start + 1).min(ordered.len());
        }
        if end <= start {
            continue;
        }

        let lower = ordered[start].clone();
        let upper = ordered[end - 1].clone();
        let frequency = (end - start) as f64 / sample_count.max(1) as f64;
        bins.push(HistogramBin {
            lower,
            upper,
            frequency,
        });
    }
    bins
}
