//! Modular SQL optimizer components for HoloFusion distributed scans.
//!
//! This module intentionally separates:
//! - predicate analysis,
//! - statistics modeling,
//! - pluggable cost model(s),
//! - access-path planning,
//! - runtime feedback loops,
//! - learned-cardinality shadow experimentation.

use std::sync::Arc;
use std::time::Duration;

use crate::indexing::SecondaryIndexRecord;
use crate::metadata::TableColumnRecord;

pub mod cost;
pub mod feedback;
pub mod learned;
pub mod planner;
pub mod predicates;
pub mod stats;

use cost::{CostModel, DistributedCostModel};
use feedback::{AdaptiveFeedbackController, FeedbackObservation};
use learned::ShadowLearnedCardinalityModel;
use planner::{choose_access_path, PlannedAccessPath, PlannerConfig, QueryShape};
use predicates::PredicateSummary;
use stats::TableStats;

#[derive(Debug, Clone)]
pub struct OptimizerRuntimeConfig {
    pub planner: PlannerConfig,
    pub stats_sample_rows: usize,
    pub stats_refresh_interval: Duration,
}

impl Default for OptimizerRuntimeConfig {
    fn default() -> Self {
        Self {
            planner: PlannerConfig {
                enable_cbo: parse_bool_env("HOLO_FUSION_CBO_ENABLED", true),
                enable_ordered_index_limit: parse_bool_env(
                    "HOLO_FUSION_CBO_ENABLE_ORDERED_INDEX_LIMIT",
                    true,
                ),
                enable_learned_shadow: parse_bool_env(
                    "HOLO_FUSION_CBO_ENABLE_LEARNED_SHADOW",
                    true,
                ),
                max_index_probes: parse_usize_env("HOLO_FUSION_CBO_MAX_INDEX_PROBES", 64).max(1),
                fallback_uncertainty_threshold: parse_f64_env(
                    "HOLO_FUSION_CBO_FALLBACK_UNCERTAINTY",
                    0.60,
                )
                .clamp(0.0, 1.0),
                fallback_table_scan_cost_slack: parse_f64_env(
                    "HOLO_FUSION_CBO_FALLBACK_TABLE_SCAN_COST_SLACK",
                    0.03,
                )
                .clamp(0.0, 1.0),
                min_table_rows_for_index: parse_u64_env(
                    "HOLO_FUSION_CBO_MIN_TABLE_ROWS_FOR_INDEX",
                    128,
                )
                .max(1),
            },
            stats_sample_rows: parse_usize_env("HOLO_FUSION_CBO_STATS_SAMPLE_ROWS", 4096).max(128),
            stats_refresh_interval: Duration::from_millis(
                parse_u64_env("HOLO_FUSION_CBO_STATS_REFRESH_MS", 30_000).max(1_000),
            ),
        }
    }
}

#[derive(Clone)]
pub struct OptimizerEngine {
    config: OptimizerRuntimeConfig,
    cost_model: Arc<dyn CostModel>,
    feedback: Arc<AdaptiveFeedbackController>,
    learned_shadow: Arc<ShadowLearnedCardinalityModel>,
}

impl std::fmt::Debug for OptimizerEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OptimizerEngine")
            .field("config", &self.config)
            .field("cost_model", &self.cost_model.name())
            .finish()
    }
}

impl Default for OptimizerEngine {
    fn default() -> Self {
        Self::new(OptimizerRuntimeConfig::default())
    }
}

impl OptimizerEngine {
    pub fn new(config: OptimizerRuntimeConfig) -> Self {
        Self {
            config,
            cost_model: Arc::new(DistributedCostModel::default()),
            feedback: Arc::new(AdaptiveFeedbackController::default()),
            learned_shadow: Arc::new(ShadowLearnedCardinalityModel::default()),
        }
    }

    pub fn config(&self) -> &OptimizerRuntimeConfig {
        &self.config
    }

    pub fn choose_access_path(
        &self,
        columns: &[TableColumnRecord],
        indexes: &[SecondaryIndexRecord],
        primary_key_column: &str,
        stats: &TableStats,
        predicates: &PredicateSummary,
        query_shape: &QueryShape,
    ) -> PlannedAccessPath {
        choose_access_path(
            &self.config.planner,
            self.cost_model.as_ref(),
            self.feedback.as_ref(),
            self.learned_shadow.as_ref(),
            columns,
            indexes,
            primary_key_column,
            stats,
            predicates,
            query_shape,
        )
    }

    pub fn observe_execution(
        &self,
        plan: &PlannedAccessPath,
        actual_rows: u64,
    ) -> FeedbackObservation {
        let observation = self.feedback.observe(
            plan.signature.as_str(),
            plan.estimated_output_rows,
            actual_rows as f64,
        );
        self.learned_shadow.observe(
            plan.signature.as_str(),
            plan.estimated_output_rows,
            actual_rows as f64,
        );
        observation
    }
}

fn parse_bool_env(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse::<bool>().ok())
        .unwrap_or(default)
}

fn parse_usize_env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(default)
}

fn parse_u64_env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(default)
}

fn parse_f64_env(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse::<f64>().ok())
        .unwrap_or(default)
}
