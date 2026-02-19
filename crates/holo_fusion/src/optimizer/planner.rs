use std::collections::{BTreeMap, BTreeSet};

use datafusion::common::ScalarValue;

use crate::indexing::{SecondaryIndexDistribution, SecondaryIndexRecord, SecondaryIndexState};
use crate::metadata::TableColumnRecord;
use crate::optimizer::cost::{CandidateKind, CostEstimate, CostModel, CostRequest};
use crate::optimizer::feedback::AdaptiveFeedbackController;
use crate::optimizer::learned::{
    LearnedFeatures, LearnedPrediction, ShadowLearnedCardinalityModel,
};
use crate::optimizer::predicates::{ColumnPredicate, PredicateSummary};
use crate::optimizer::stats::{ColumnStats, StatsScalar, TableStats};

#[derive(Debug, Clone)]
pub struct PlannerConfig {
    pub enable_cbo: bool,
    pub enable_ordered_index_limit: bool,
    pub enable_learned_shadow: bool,
    pub max_index_probes: usize,
    /// Uncertainty guardrail threshold. Above this, the planner may fallback
    /// from a lookup-style index path to table scan.
    pub fallback_uncertainty_threshold: f64,
    /// Maximum relative table-scan penalty tolerated by the uncertainty
    /// guardrail fallback. Example: 0.03 allows table scan up to 3% costlier.
    pub fallback_table_scan_cost_slack: f64,
    pub min_table_rows_for_index: u64,
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self {
            enable_cbo: true,
            enable_ordered_index_limit: true,
            enable_learned_shadow: true,
            max_index_probes: 64,
            fallback_uncertainty_threshold: 0.60,
            fallback_table_scan_cost_slack: 0.03,
            min_table_rows_for_index: 128,
        }
    }
}

#[derive(Debug, Clone)]
pub struct QueryShape {
    pub required_columns: BTreeSet<String>,
    pub limit: Option<usize>,
    pub preferred_shard_count: usize,
}

#[derive(Debug, Clone)]
pub struct IndexPrefixConstraint {
    pub column: String,
    pub values: Vec<ScalarValue>,
}

#[derive(Debug, Clone)]
pub enum AccessPath {
    TableScan,
    Index {
        index_name: String,
        prefix: Vec<IndexPrefixConstraint>,
        index_only_table_covering: bool,
        ordered_limit_scan: bool,
        probe_count: usize,
    },
}

#[derive(Debug, Clone)]
pub struct PlannedAccessPath {
    pub path: AccessPath,
    pub estimated_output_rows: f64,
    pub estimated_scan_rows: f64,
    pub uncertainty: f64,
    pub total_cost: f64,
    pub signature: String,
    pub feedback_multiplier: f64,
    pub learned_shadow: Option<LearnedPrediction>,
    pub fallback_decision: Option<FallbackDecision>,
    pub considered: Vec<CostedCandidateSummary>,
}

#[derive(Debug, Clone)]
pub struct FallbackDecision {
    /// Stable identifier for why the fallback was applied.
    pub reason: &'static str,
    /// Candidate selected before fallback.
    pub source_candidate: String,
    pub source_cost: f64,
    /// Candidate selected by fallback.
    pub fallback_candidate: String,
    pub fallback_cost: f64,
    pub uncertainty: f64,
    pub uncertainty_threshold: f64,
    /// Relative penalty versus the original winner: `(fallback/source)-1`.
    pub fallback_cost_penalty_ratio: f64,
}

#[derive(Debug, Clone)]
pub struct CostedCandidateSummary {
    pub name: String,
    pub estimated_output_rows: f64,
    pub estimated_scan_rows: f64,
    pub uncertainty: f64,
    pub feedback_multiplier: f64,
    pub total_cost: f64,
}

#[derive(Debug, Clone)]
struct Candidate {
    path: AccessPath,
    kind: CandidateKind,
    estimated_output_rows: f64,
    estimated_scan_rows: f64,
    uncertainty: f64,
    has_residual_predicate: bool,
    signature: String,
    name: String,
}

pub fn choose_access_path(
    config: &PlannerConfig,
    cost_model: &dyn CostModel,
    feedback: &AdaptiveFeedbackController,
    learned: &ShadowLearnedCardinalityModel,
    _columns: &[TableColumnRecord],
    indexes: &[SecondaryIndexRecord],
    primary_key_column: &str,
    stats: &TableStats,
    predicates: &PredicateSummary,
    query_shape: &QueryShape,
) -> PlannedAccessPath {
    let table_rows = stats.estimated_row_count.max(1) as f64;
    let base_selectivity = estimate_predicate_selectivity(predicates, stats, table_rows);
    let base_output_rows = if predicates.total_terms == 0 {
        table_rows
    } else {
        (table_rows * base_selectivity).clamp(1.0, table_rows)
    };

    let mut candidates = Vec::<Candidate>::new();
    candidates.push(Candidate {
        path: AccessPath::TableScan,
        kind: CandidateKind::TableScan,
        estimated_output_rows: base_output_rows,
        estimated_scan_rows: table_rows,
        uncertainty: table_scan_uncertainty(stats, predicates),
        has_residual_predicate: predicates.residual_terms > 0,
        signature: build_signature("table", None, predicates),
        name: "table_scan".to_string(),
    });

    if stats.estimated_row_count >= config.min_table_rows_for_index {
        let mut index_candidates = enumerate_index_candidates(
            config,
            indexes,
            primary_key_column,
            stats,
            predicates,
            query_shape,
            base_output_rows,
            table_rows,
        );
        candidates.append(&mut index_candidates);
    }

    if !config.enable_cbo {
        if let Some(table) = candidates
            .iter()
            .find(|candidate| matches!(candidate.path, AccessPath::TableScan))
            .cloned()
        {
            return materialize_plan(table, vec![], 1.0, None, CostEstimate::default(), None);
        }
    }

    let mut costed = Vec::<(Candidate, CostEstimate, f64, Option<LearnedPrediction>)>::new();
    let mut summaries = Vec::<CostedCandidateSummary>::new();

    for candidate in candidates {
        let (adjusted_output, multiplier) = feedback.adjust_estimate(
            candidate.signature.as_str(),
            candidate.estimated_output_rows,
        );
        let adjusted_scan = (candidate.estimated_scan_rows * multiplier)
            .max(adjusted_output)
            .max(1.0);

        let learned_prediction = if config.enable_learned_shadow {
            Some(learned.predict(
                candidate.signature.as_str(),
                LearnedFeatures {
                    estimated_rows: adjusted_output,
                    table_rows,
                    predicate_terms: predicates.total_terms,
                    index_prefix_columns: index_prefix_column_count(&candidate.path),
                    limit: query_shape.limit,
                },
            ))
        } else {
            None
        };

        let estimate = cost_model.estimate(CostRequest {
            kind: candidate.kind,
            estimated_output_rows: adjusted_output,
            estimated_scan_rows: adjusted_scan,
            scan_fraction_of_table: (adjusted_scan / table_rows.max(1.0)).clamp(0.0, 1.0),
            avg_row_width_bytes: stats.avg_row_width_bytes.max(1) as f64,
            fanout: query_shape.preferred_shard_count.max(1),
            probe_count: index_probe_count(&candidate.path),
            limit: query_shape.limit,
            uncertainty: candidate.uncertainty,
            has_residual_predicate: candidate.has_residual_predicate,
        });

        summaries.push(CostedCandidateSummary {
            name: candidate.name.clone(),
            estimated_output_rows: adjusted_output,
            estimated_scan_rows: adjusted_scan,
            uncertainty: candidate.uncertainty,
            feedback_multiplier: multiplier,
            total_cost: estimate.total_cost,
        });
        costed.push((candidate, estimate, multiplier, learned_prediction));
    }

    costed.sort_by(|left, right| {
        left.1
            .total_cost
            .partial_cmp(&right.1.total_cost)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut winner = costed.remove(0);
    let mut fallback_decision = None;
    if winner.0.kind == CandidateKind::IndexLookup
        && winner.0.uncertainty > config.fallback_uncertainty_threshold
    {
        if let Some(table_candidate) = costed
            .iter()
            .find(|candidate| matches!(candidate.0.path, AccessPath::TableScan))
            .cloned()
        {
            let source_cost = winner.1.total_cost.max(0.0);
            let fallback_cost = table_candidate.1.total_cost.max(0.0);
            let fallback_cost_penalty_ratio = if source_cost <= f64::EPSILON {
                0.0
            } else {
                (fallback_cost / source_cost) - 1.0
            };
            // Uncertainty guardrail: for non-covering lookup plans only, prefer
            // a table scan when costs are effectively a near tie.
            if fallback_cost <= source_cost * (1.0 + config.fallback_table_scan_cost_slack) {
                fallback_decision = Some(FallbackDecision {
                    reason: "uncertainty_guardrail",
                    source_candidate: winner.0.name.clone(),
                    source_cost,
                    fallback_candidate: table_candidate.0.name.clone(),
                    fallback_cost,
                    uncertainty: winner.0.uncertainty,
                    uncertainty_threshold: config.fallback_uncertainty_threshold,
                    fallback_cost_penalty_ratio,
                });
                winner = table_candidate;
            }
        }
    }

    materialize_plan(
        winner.0,
        summaries,
        winner.2,
        winner.3,
        winner.1,
        fallback_decision,
    )
}

fn materialize_plan(
    candidate: Candidate,
    considered: Vec<CostedCandidateSummary>,
    feedback_multiplier: f64,
    learned_shadow: Option<LearnedPrediction>,
    estimate: CostEstimate,
    fallback_decision: Option<FallbackDecision>,
) -> PlannedAccessPath {
    PlannedAccessPath {
        path: candidate.path,
        estimated_output_rows: candidate.estimated_output_rows.max(1.0) * feedback_multiplier,
        estimated_scan_rows: candidate.estimated_scan_rows.max(1.0) * feedback_multiplier,
        uncertainty: candidate.uncertainty,
        total_cost: estimate.total_cost,
        signature: candidate.signature,
        feedback_multiplier,
        learned_shadow,
        fallback_decision,
        considered,
    }
}

fn enumerate_index_candidates(
    config: &PlannerConfig,
    indexes: &[SecondaryIndexRecord],
    primary_key_column: &str,
    stats: &TableStats,
    predicates: &PredicateSummary,
    query_shape: &QueryShape,
    base_output_rows: f64,
    table_rows: f64,
) -> Vec<Candidate> {
    let mut out = Vec::new();

    for index in indexes {
        if !matches!(index.state, SecondaryIndexState::Public) {
            continue;
        }

        let mut prefix = Vec::<IndexPrefixConstraint>::new();
        let mut probe_count = 1usize;
        let mut prefix_selectivity = 1.0f64;

        for key_column in &index.key_columns {
            let normalized = key_column.to_ascii_lowercase();
            let Some(column_predicate) = predicates.column(normalized.as_str()) else {
                break;
            };
            if column_predicate.equals.is_empty() {
                break;
            }

            let mut values = column_predicate.equals.clone();
            values.truncate(16);
            if values.is_empty() {
                break;
            }
            let next_probe_count = probe_count.saturating_mul(values.len());
            if next_probe_count > config.max_index_probes {
                probe_count = config.max_index_probes.saturating_add(1);
                break;
            }
            probe_count = next_probe_count;

            prefix.push(IndexPrefixConstraint {
                column: normalized,
                values,
            });
            prefix_selectivity = estimate_prefix_selectivity(prefix.as_slice(), stats, table_rows);
        }

        if prefix.is_empty() {
            continue;
        }
        if probe_count == 0 || probe_count > config.max_index_probes {
            continue;
        }
        if matches!(index.distribution, SecondaryIndexDistribution::Hash)
            && prefix.len() != index.key_columns.len()
        {
            continue;
        }

        let index_only_table_covering =
            is_index_query_covering(index, primary_key_column, &query_shape.required_columns);
        let ordered_limit_scan = config.enable_ordered_index_limit
            && query_shape.limit.is_some()
            && matches!(index.distribution, SecondaryIndexDistribution::Range);

        let has_residual_predicate = has_residual_predicates(predicates, index, prefix.len());
        let estimated_scan_rows = (table_rows * prefix_selectivity)
            .clamp(1.0, table_rows)
            .max(base_output_rows.min(table_rows * 0.05));
        let selectivity = estimated_scan_rows / table_rows.max(1.0);
        // Guardrail: for non-covering lookups with high selectivity and no top-k
        // limit signal, prefer table scan to avoid lookup amplification.
        if !index_only_table_covering
            && !ordered_limit_scan
            && query_shape.limit.is_none()
            && selectivity >= 0.35
        {
            continue;
        }
        let uncertainty =
            index_uncertainty(stats, predicates, prefix.len(), has_residual_predicate);
        let kind = if index_only_table_covering {
            CandidateKind::IndexOnly
        } else if ordered_limit_scan {
            CandidateKind::OrderedIndex
        } else {
            CandidateKind::IndexLookup
        };
        let path = AccessPath::Index {
            index_name: index.index_name.to_ascii_lowercase(),
            prefix,
            index_only_table_covering,
            ordered_limit_scan,
            probe_count,
        };

        let candidate_name = match kind {
            CandidateKind::IndexOnly => {
                format!("index_only:{}", index.index_name.to_ascii_lowercase())
            }
            CandidateKind::OrderedIndex => {
                format!("ordered_index:{}", index.index_name.to_ascii_lowercase())
            }
            CandidateKind::IndexLookup => {
                format!("index_lookup:{}", index.index_name.to_ascii_lowercase())
            }
            CandidateKind::TableScan => "table_scan".to_string(),
        };

        out.push(Candidate {
            path,
            kind,
            estimated_output_rows: base_output_rows,
            estimated_scan_rows,
            uncertainty,
            has_residual_predicate,
            signature: build_signature("index", Some(index.index_name.as_str()), predicates),
            name: candidate_name,
        });
    }

    out
}

fn table_scan_uncertainty(stats: &TableStats, predicates: &PredicateSummary) -> f64 {
    let confidence = stats.confidence.clamp(0.1, 1.0);
    let residual = predicates.residual_fraction().clamp(0.0, 1.0);
    (1.0 - confidence * (1.0 - 0.5 * residual)).clamp(0.0, 1.0)
}

fn index_uncertainty(
    stats: &TableStats,
    _predicates: &PredicateSummary,
    prefix_columns: usize,
    has_residual: bool,
) -> f64 {
    let confidence = stats.confidence.clamp(0.1, 1.0);
    let residual_penalty = if has_residual { 0.25 } else { 0.0 };
    let prefix_bonus = if prefix_columns > 1 { 0.15 } else { 0.0 };
    (1.0 - confidence + residual_penalty - prefix_bonus).clamp(0.0, 1.0)
}

fn estimate_predicate_selectivity(
    predicates: &PredicateSummary,
    stats: &TableStats,
    table_rows: f64,
) -> f64 {
    if predicates.total_terms == 0 {
        return 1.0;
    }

    let mut selectivity = 1.0f64;
    for (column, predicate) in predicates.columns() {
        if !predicate.has_any_constraint() {
            continue;
        }
        let column_stats = stats.columns.get(column.as_str());
        selectivity *= estimate_column_predicate_selectivity(predicate, column_stats, table_rows);
    }

    let residual_fraction = predicates.residual_fraction();
    // Guardrail: when residual predicates exist and stats are uncertain, avoid
    // over-aggressive underestimation that can destabilize path choice.
    if residual_fraction > 0.0 {
        selectivity = selectivity.max(0.001 + residual_fraction * 0.05);
    }

    selectivity.clamp(1.0 / table_rows.max(1.0), 1.0)
}

fn estimate_prefix_selectivity(
    prefix: &[IndexPrefixConstraint],
    stats: &TableStats,
    table_rows: f64,
) -> f64 {
    if prefix.is_empty() {
        return 1.0;
    }

    if let Some(multi_selectivity) =
        estimate_multi_column_prefix_selectivity(prefix, stats, table_rows)
    {
        return multi_selectivity.clamp(1.0 / table_rows.max(1.0), 1.0);
    }

    let mut selectivity = 1.0f64;
    for constraint in prefix {
        let column_stats = stats.columns.get(constraint.column.as_str());
        let eq_selectivity =
            estimate_equality_selectivity(constraint.values.as_slice(), column_stats, table_rows);
        selectivity *= eq_selectivity;
    }
    selectivity.clamp(1.0 / table_rows.max(1.0), 1.0)
}

fn estimate_multi_column_prefix_selectivity(
    prefix: &[IndexPrefixConstraint],
    stats: &TableStats,
    table_rows: f64,
) -> Option<f64> {
    if prefix.len() < 2 {
        return None;
    }

    let key = prefix
        .iter()
        .map(|constraint| constraint.column.as_str())
        .collect::<Vec<_>>()
        .join(",");
    let multi_stats = stats.multi_column.get(key.as_str())?;
    if multi_stats.columns.len() != prefix.len() {
        return None;
    }

    let mcv_lookup = multi_stats
        .mcv
        .iter()
        .map(|entry| (entry.values.clone(), entry.frequency.max(0.0)))
        .collect::<BTreeMap<_, _>>();
    let mcv_sum = multi_stats
        .mcv
        .iter()
        .map(|entry| entry.frequency.max(0.0))
        .sum::<f64>();
    let remaining_mass = (1.0 - mcv_sum).max(0.0);
    let remaining_ndv = multi_stats
        .ndv
        .saturating_sub(multi_stats.mcv.len() as u64)
        .max(1) as f64;
    let fallback = (remaining_mass / remaining_ndv).max(1.0 / table_rows.max(1.0));

    let mut total = 0.0f64;
    let mut current = Vec::<ScalarValue>::with_capacity(prefix.len());
    accumulate_multi_column_selectivity(prefix, 0, &mut current, &mcv_lookup, fallback, &mut total);
    Some(total.clamp(1.0 / table_rows.max(1.0), 1.0))
}

fn accumulate_multi_column_selectivity(
    prefix: &[IndexPrefixConstraint],
    depth: usize,
    current: &mut Vec<ScalarValue>,
    mcv_lookup: &BTreeMap<Vec<StatsScalar>, f64>,
    fallback: f64,
    total: &mut f64,
) {
    if depth >= prefix.len() {
        let mut tuple = Vec::<StatsScalar>::with_capacity(current.len());
        for value in current {
            let Some(stats_value) = StatsScalar::from_scalar(value) else {
                *total += fallback;
                return;
            };
            tuple.push(stats_value);
        }
        *total += mcv_lookup
            .get(tuple.as_slice())
            .copied()
            .unwrap_or(fallback);
        return;
    }

    for value in &prefix[depth].values {
        current.push(value.clone());
        accumulate_multi_column_selectivity(
            prefix,
            depth + 1,
            current,
            mcv_lookup,
            fallback,
            total,
        );
        current.pop();
    }
}

fn estimate_column_predicate_selectivity(
    predicate: &ColumnPredicate,
    stats: Option<&ColumnStats>,
    table_rows: f64,
) -> f64 {
    let mut selectivity = 1.0f64;

    if predicate.is_null {
        selectivity *= stats
            .map(|s| s.null_fraction)
            .unwrap_or(0.1)
            .clamp(1.0 / table_rows, 1.0);
    }
    if predicate.is_not_null {
        let null_fraction = stats.map(|s| s.null_fraction).unwrap_or(0.1);
        selectivity *= (1.0 - null_fraction).clamp(1.0 / table_rows, 1.0);
    }

    if !predicate.equals.is_empty() {
        return estimate_equality_selectivity(predicate.equals.as_slice(), stats, table_rows)
            .clamp(1.0 / table_rows, 1.0);
    }

    if predicate.lower.is_some() || predicate.upper.is_some() {
        return estimate_range_selectivity(predicate, stats, table_rows)
            .clamp(1.0 / table_rows, 1.0);
    }

    selectivity.clamp(1.0 / table_rows, 1.0)
}

fn estimate_equality_selectivity(
    values: &[ScalarValue],
    stats: Option<&ColumnStats>,
    table_rows: f64,
) -> f64 {
    if values.is_empty() {
        return 1.0;
    }
    let Some(stats) = stats else {
        return (values.len() as f64 / table_rows.max(1.0)).clamp(1.0 / table_rows.max(1.0), 1.0);
    };

    let mut mcv_sum = 0.0f64;
    for entry in &stats.mcv {
        mcv_sum += entry.frequency.max(0.0);
    }
    let remaining_mass = (1.0 - stats.null_fraction - mcv_sum).max(0.0);
    let remaining_ndv = stats.ndv.saturating_sub(stats.mcv.len() as u64).max(1) as f64;
    let fallback = (remaining_mass / remaining_ndv).max(1.0 / table_rows.max(1.0));

    let mut total = 0.0f64;
    for value in values {
        let scalar = StatsScalar::from_scalar(value);
        let mut found = false;
        if let Some(scalar) = scalar {
            for entry in &stats.mcv {
                if entry.value == scalar {
                    total += entry.frequency;
                    found = true;
                    break;
                }
            }
        }
        if !found {
            total += fallback;
        }
    }

    total.clamp(1.0 / table_rows.max(1.0), 1.0)
}

fn estimate_range_selectivity(
    predicate: &ColumnPredicate,
    stats: Option<&ColumnStats>,
    table_rows: f64,
) -> f64 {
    let Some(stats) = stats else {
        return 0.33;
    };
    if stats.histogram.is_empty() {
        return 0.33;
    }

    let lower = predicate
        .lower
        .as_ref()
        .and_then(|bound| scalar_to_f64(&bound.value));
    let upper = predicate
        .upper
        .as_ref()
        .and_then(|bound| scalar_to_f64(&bound.value));

    let mut coverage = 0.0f64;
    for bin in &stats.histogram {
        let Some(bin_lower) = stats_scalar_to_f64(&bin.lower) else {
            continue;
        };
        let Some(bin_upper) = stats_scalar_to_f64(&bin.upper) else {
            continue;
        };
        if bin_upper < bin_lower {
            continue;
        }

        let left = lower.unwrap_or(bin_lower);
        let right = upper.unwrap_or(bin_upper);
        if right < bin_lower || left > bin_upper {
            continue;
        }

        let overlap_left = left.max(bin_lower);
        let overlap_right = right.min(bin_upper);
        let width = (bin_upper - bin_lower).abs().max(1.0);
        let overlap = (overlap_right - overlap_left).abs().max(0.0);
        coverage += bin.frequency * (overlap / width).clamp(0.0, 1.0);
    }

    if coverage <= 0.0 {
        coverage = 0.1;
    }
    coverage.clamp(1.0 / table_rows.max(1.0), 1.0)
}

fn scalar_to_f64(value: &ScalarValue) -> Option<f64> {
    match value {
        ScalarValue::Int8(Some(v)) => Some(f64::from(*v)),
        ScalarValue::Int16(Some(v)) => Some(f64::from(*v)),
        ScalarValue::Int32(Some(v)) => Some(f64::from(*v)),
        ScalarValue::Int64(Some(v)) => Some(*v as f64),
        ScalarValue::UInt8(Some(v)) => Some(f64::from(*v)),
        ScalarValue::UInt16(Some(v)) => Some(f64::from(*v)),
        ScalarValue::UInt32(Some(v)) => Some(f64::from(*v)),
        ScalarValue::UInt64(Some(v)) => Some(*v as f64),
        ScalarValue::Float32(Some(v)) => Some(f64::from(*v)),
        ScalarValue::Float64(Some(v)) => Some(*v),
        ScalarValue::TimestampNanosecond(Some(v), _)
        | ScalarValue::TimestampMicrosecond(Some(v), _)
        | ScalarValue::TimestampMillisecond(Some(v), _)
        | ScalarValue::TimestampSecond(Some(v), _) => Some(*v as f64),
        _ => None,
    }
}

fn stats_scalar_to_f64(value: &StatsScalar) -> Option<f64> {
    match value {
        StatsScalar::Int(v) => Some(*v as f64),
        StatsScalar::FloatBits(bits) => Some(f64::from_bits(*bits)),
        StatsScalar::Bool(v) => Some(if *v { 1.0 } else { 0.0 }),
        _ => None,
    }
}

fn has_residual_predicates(
    predicates: &PredicateSummary,
    index: &SecondaryIndexRecord,
    prefix_len: usize,
) -> bool {
    if predicates.residual_terms > 0 {
        return true;
    }

    let mut covered = BTreeSet::<String>::new();
    for column in index.key_columns.iter().take(prefix_len) {
        covered.insert(column.to_ascii_lowercase());
    }

    for (column, predicate) in predicates.columns() {
        if !predicate.has_any_constraint() {
            continue;
        }
        if !covered.contains(column.as_str()) {
            return true;
        }
    }

    false
}

fn is_index_query_covering(
    index: &SecondaryIndexRecord,
    primary_key_column: &str,
    required_columns: &BTreeSet<String>,
) -> bool {
    if required_columns.is_empty() {
        return false;
    }
    let mut covered = BTreeSet::<String>::new();
    covered.insert(primary_key_column.to_ascii_lowercase());
    for column in &index.key_columns {
        covered.insert(column.to_ascii_lowercase());
    }
    for column in &index.include_columns {
        covered.insert(column.to_ascii_lowercase());
    }

    required_columns
        .iter()
        .all(|column| covered.contains(&column.to_ascii_lowercase()))
}

fn index_prefix_column_count(path: &AccessPath) -> usize {
    match path {
        AccessPath::TableScan => 0,
        AccessPath::Index { prefix, .. } => prefix.len(),
    }
}

fn index_probe_count(path: &AccessPath) -> usize {
    match path {
        AccessPath::TableScan => 1,
        AccessPath::Index { probe_count, .. } => *probe_count,
    }
}

fn build_signature(kind: &str, index_name: Option<&str>, predicates: &PredicateSummary) -> String {
    let mut signature = String::new();
    signature.push_str(kind);
    signature.push('|');
    if let Some(index) = index_name {
        signature.push_str(index);
    } else {
        signature.push('-');
    }
    signature.push('|');

    let mut columns = predicates
        .by_column
        .iter()
        .map(|(name, pred)| {
            let mut marker = String::new();
            marker.push_str(name);
            marker.push(':');
            if !pred.equals.is_empty() {
                marker.push_str("eq");
                marker.push_str(pred.equals.len().to_string().as_str());
            }
            if pred.lower.is_some() {
                marker.push_str("l");
            }
            if pred.upper.is_some() {
                marker.push_str("u");
            }
            if pred.is_null {
                marker.push_str("n");
            }
            if pred.is_not_null {
                marker.push_str("nn");
            }
            marker
        })
        .collect::<Vec<_>>();
    columns.sort();
    signature.push_str(columns.join(",").as_str());
    signature.push('|');
    signature.push_str(format!("res{}", predicates.residual_terms).as_str());
    signature
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{col, lit};

    use crate::metadata::{TableColumnRecord, TableColumnType};
    use crate::optimizer::cost::{
        CandidateKind, CostEstimate, CostModel, CostRequest, DistributedCostModel,
    };
    use crate::optimizer::predicates::extract_predicate_summary;
    use crate::optimizer::stats::{ColumnStats, McvEntry, StatsScalar, TableStats};

    fn sales_columns() -> Vec<TableColumnRecord> {
        vec![
            TableColumnRecord {
                name: "order_id".to_string(),
                column_type: TableColumnType::Int64,
                nullable: false,
                default_value: None,
            },
            TableColumnRecord {
                name: "merchant_id".to_string(),
                column_type: TableColumnType::Int64,
                nullable: false,
                default_value: None,
            },
            TableColumnRecord {
                name: "event_day".to_string(),
                column_type: TableColumnType::Int64,
                nullable: false,
                default_value: None,
            },
            TableColumnRecord {
                name: "status".to_string(),
                column_type: TableColumnType::Utf8,
                nullable: false,
                default_value: None,
            },
            TableColumnRecord {
                name: "amount_cents".to_string(),
                column_type: TableColumnType::Int64,
                nullable: false,
                default_value: None,
            },
        ]
    }

    fn sales_status_day_index() -> SecondaryIndexRecord {
        SecondaryIndexRecord {
            db_id: 1,
            schema_id: 1,
            table_id: 77,
            index_id: 5,
            table_name: "sales".to_string(),
            index_name: "sales_status_day_idx".to_string(),
            unique: false,
            key_columns: vec![
                "status".to_string(),
                "event_day".to_string(),
                "merchant_id".to_string(),
            ],
            include_columns: vec!["amount_cents".to_string()],
            distribution: SecondaryIndexDistribution::Range,
            hash_bucket_count: None,
            state: SecondaryIndexState::Public,
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        }
    }

    fn sales_status_day_non_covering_index() -> SecondaryIndexRecord {
        SecondaryIndexRecord {
            include_columns: Vec::new(),
            ..sales_status_day_index()
        }
    }

    fn baseline_stats() -> TableStats {
        let mut stats = TableStats {
            estimated_row_count: 1_000_000,
            avg_row_width_bytes: 48,
            confidence: 0.9,
            ..Default::default()
        };
        stats.columns.insert(
            "status".to_string(),
            ColumnStats {
                ndv: 4,
                null_fraction: 0.0,
                mcv: vec![
                    McvEntry {
                        value: StatsScalar::Utf8("paid".to_string()),
                        frequency: 0.60,
                    },
                    McvEntry {
                        value: StatsScalar::Utf8("shipped".to_string()),
                        frequency: 0.20,
                    },
                ],
                histogram: Vec::new(),
            },
        );
        stats.columns.insert(
            "event_day".to_string(),
            ColumnStats {
                ndv: 365,
                null_fraction: 0.0,
                mcv: Vec::new(),
                histogram: Vec::new(),
            },
        );
        stats
    }

    #[derive(Debug, Clone, Copy)]
    struct FixedCostModel {
        table_scan: f64,
        index_lookup: f64,
        index_only: f64,
        ordered_index: f64,
    }

    impl CostModel for FixedCostModel {
        fn name(&self) -> &'static str {
            "fixed_test"
        }

        fn estimate(&self, request: CostRequest) -> CostEstimate {
            let total_cost = match request.kind {
                CandidateKind::TableScan => self.table_scan,
                CandidateKind::IndexLookup => self.index_lookup,
                CandidateKind::IndexOnly => self.index_only,
                CandidateKind::OrderedIndex => self.ordered_index,
            };
            CostEstimate {
                total_cost,
                ..CostEstimate::default()
            }
        }
    }

    #[test]
    fn cbo_prefers_index_for_selective_prefix_predicates() {
        let columns = sales_columns();
        let indexes = vec![sales_status_day_index()];
        let stats = baseline_stats();
        let filters = vec![
            col("status").eq(lit("shipped")),
            col("event_day").eq(lit(10i64)),
        ];
        let predicates = extract_predicate_summary(filters.as_slice());
        let query_shape = QueryShape {
            required_columns: ["merchant_id", "event_day", "amount_cents"]
                .into_iter()
                .map(|name| name.to_string())
                .collect(),
            limit: Some(200),
            preferred_shard_count: 3,
        };
        let config = PlannerConfig::default();
        let feedback = AdaptiveFeedbackController::default();
        let learned = ShadowLearnedCardinalityModel::default();
        let plan = choose_access_path(
            &config,
            &DistributedCostModel::default(),
            &feedback,
            &learned,
            columns.as_slice(),
            indexes.as_slice(),
            "order_id",
            &stats,
            &predicates,
            &query_shape,
        );

        match plan.path {
            AccessPath::Index { .. } => {}
            AccessPath::TableScan => panic!("expected index plan for selective predicate"),
        }
    }

    #[test]
    fn index_only_selection_uses_query_covering_not_table_covering() {
        let columns = sales_columns();
        let indexes = vec![sales_status_day_index()];
        let stats = baseline_stats();
        let filters = vec![col("status")
            .eq(lit("paid"))
            .and(col("event_day").eq(lit(1i64)))];
        let predicates = extract_predicate_summary(filters.as_slice());
        let query_shape = QueryShape {
            required_columns: ["merchant_id", "event_day", "status", "amount_cents"]
                .into_iter()
                .map(|name| name.to_string())
                .collect(),
            limit: None,
            preferred_shard_count: 3,
        };
        let config = PlannerConfig::default();
        let feedback = AdaptiveFeedbackController::default();
        let learned = ShadowLearnedCardinalityModel::default();
        let plan = choose_access_path(
            &config,
            &DistributedCostModel::default(),
            &feedback,
            &learned,
            columns.as_slice(),
            indexes.as_slice(),
            "order_id",
            &stats,
            &predicates,
            &query_shape,
        );

        match plan.path {
            AccessPath::Index {
                index_only_table_covering,
                ..
            } => assert!(
                index_only_table_covering,
                "expected index-only when query columns are covered"
            ),
            AccessPath::TableScan => panic!("expected index plan"),
        }
    }

    #[test]
    fn cbo_avoids_non_covering_index_for_low_selectivity() {
        let columns = sales_columns();
        let indexes = vec![sales_status_day_non_covering_index()];
        let mut stats = baseline_stats();
        stats.columns.insert(
            "event_day".to_string(),
            ColumnStats {
                ndv: 1,
                null_fraction: 0.0,
                mcv: vec![McvEntry {
                    value: StatsScalar::Int(1),
                    frequency: 1.0,
                }],
                histogram: Vec::new(),
            },
        );
        let filters = vec![col("status")
            .in_list(vec![lit("paid"), lit("shipped")], false)
            .and(col("event_day").eq(lit(1i64)))];
        let predicates = extract_predicate_summary(filters.as_slice());
        let query_shape = QueryShape {
            required_columns: ["merchant_id", "event_day", "status", "amount_cents"]
                .into_iter()
                .map(|name| name.to_string())
                .collect(),
            limit: None,
            preferred_shard_count: 3,
        };
        let config = PlannerConfig::default();
        let feedback = AdaptiveFeedbackController::default();
        let learned = ShadowLearnedCardinalityModel::default();
        let plan = choose_access_path(
            &config,
            &DistributedCostModel::default(),
            &feedback,
            &learned,
            columns.as_slice(),
            indexes.as_slice(),
            "order_id",
            &stats,
            &predicates,
            &query_shape,
        );

        assert!(
            matches!(plan.path, AccessPath::TableScan),
            "expected table scan for low-selectivity non-covering index predicate"
        );
    }

    #[test]
    fn cbo_builds_multi_probe_prefix_for_trailing_inlist() {
        let columns = sales_columns();
        let indexes = vec![sales_status_day_index()];
        let stats = baseline_stats();
        let filters = vec![col("status").eq(lit("paid")).and(
            col("event_day").in_list(vec![lit(20i64), lit(21i64), lit(22i64), lit(23i64)], false),
        )];
        let predicates = extract_predicate_summary(filters.as_slice());
        let query_shape = QueryShape {
            required_columns: ["merchant_id", "event_day", "amount_cents"]
                .into_iter()
                .map(|name| name.to_string())
                .collect(),
            limit: Some(200),
            preferred_shard_count: 3,
        };
        let config = PlannerConfig::default();
        let feedback = AdaptiveFeedbackController::default();
        let learned = ShadowLearnedCardinalityModel::default();
        let plan = choose_access_path(
            &config,
            &DistributedCostModel::default(),
            &feedback,
            &learned,
            columns.as_slice(),
            indexes.as_slice(),
            "order_id",
            &stats,
            &predicates,
            &query_shape,
        );

        match plan.path {
            AccessPath::Index {
                prefix,
                probe_count,
                ..
            } => {
                assert_eq!(prefix.len(), 2);
                assert_eq!(probe_count, 4);
            }
            AccessPath::TableScan => panic!("expected index plan for trailing IN-list predicate"),
        }
    }

    #[test]
    fn uncertainty_guardrail_does_not_override_index_only_winner() {
        let columns = sales_columns();
        let indexes = vec![sales_status_day_index()];
        let mut stats = baseline_stats();
        stats.confidence = 0.20;

        let filters = vec![col("status").eq(lit("paid")).and(
            col("event_day").in_list(vec![lit(20i64), lit(21i64), lit(22i64), lit(23i64)], false),
        )];
        let predicates = extract_predicate_summary(filters.as_slice());
        let query_shape = QueryShape {
            required_columns: ["merchant_id", "event_day", "amount_cents"]
                .into_iter()
                .map(|name| name.to_string())
                .collect(),
            limit: Some(200),
            preferred_shard_count: 3,
        };
        let config = PlannerConfig::default();
        let feedback = AdaptiveFeedbackController::default();
        let learned = ShadowLearnedCardinalityModel::default();
        let plan = choose_access_path(
            &config,
            &FixedCostModel {
                table_scan: 100.0,
                index_lookup: 95.0,
                index_only: 80.0,
                ordered_index: 90.0,
            },
            &feedback,
            &learned,
            columns.as_slice(),
            indexes.as_slice(),
            "order_id",
            &stats,
            &predicates,
            &query_shape,
        );

        match plan.path {
            AccessPath::Index {
                index_only_table_covering,
                ..
            } => assert!(index_only_table_covering, "expected index-only winner"),
            AccessPath::TableScan => panic!("expected index-only winner"),
        }
        assert!(
            plan.fallback_decision.is_none(),
            "index-only winners should not be uncertainty-fallback candidates"
        );
    }

    #[test]
    fn uncertainty_guardrail_overrides_near_tie_index_lookup_winner() {
        let columns = sales_columns();
        let indexes = vec![sales_status_day_non_covering_index()];
        let mut stats = baseline_stats();
        stats.confidence = 0.20;

        let filters = vec![col("status")
            .eq(lit("shipped"))
            .and(col("event_day").eq(lit(10i64)))];
        let predicates = extract_predicate_summary(filters.as_slice());
        let query_shape = QueryShape {
            required_columns: ["merchant_id", "event_day", "amount_cents"]
                .into_iter()
                .map(|name| name.to_string())
                .collect(),
            limit: None,
            preferred_shard_count: 3,
        };
        let config = PlannerConfig::default();
        let feedback = AdaptiveFeedbackController::default();
        let learned = ShadowLearnedCardinalityModel::default();
        let plan = choose_access_path(
            &config,
            &FixedCostModel {
                table_scan: 102.0,
                index_lookup: 100.0,
                index_only: 120.0,
                ordered_index: 120.0,
            },
            &feedback,
            &learned,
            columns.as_slice(),
            indexes.as_slice(),
            "order_id",
            &stats,
            &predicates,
            &query_shape,
        );

        assert!(
            matches!(plan.path, AccessPath::TableScan),
            "expected uncertainty fallback to choose table scan for near-tie lookup"
        );
        let fallback = plan
            .fallback_decision
            .as_ref()
            .expect("expected fallback decision");
        assert_eq!(fallback.reason, "uncertainty_guardrail");
        assert!(fallback.source_candidate.starts_with("index_lookup"));
        assert_eq!(fallback.fallback_candidate, "table_scan");
    }
}
