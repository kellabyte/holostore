use std::collections::BTreeMap;
use std::sync::Mutex;

#[derive(Debug, Clone, Copy, Default)]
pub struct LearnedFeatures {
    pub estimated_rows: f64,
    pub table_rows: f64,
    pub predicate_terms: usize,
    pub index_prefix_columns: usize,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct LearnedPrediction {
    pub predicted_rows: f64,
    pub multiplier: f64,
}

#[derive(Debug, Clone)]
struct LearnedState {
    multiplier: f64,
    samples: u64,
}

impl Default for LearnedState {
    fn default() -> Self {
        Self {
            multiplier: 1.0,
            samples: 0,
        }
    }
}

#[derive(Debug, Default)]
pub struct ShadowLearnedCardinalityModel {
    by_signature: Mutex<BTreeMap<String, LearnedState>>,
}

impl ShadowLearnedCardinalityModel {
    pub fn predict(&self, signature: &str, features: LearnedFeatures) -> LearnedPrediction {
        let map = self
            .by_signature
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let state = map.get(signature);
        let learned_multiplier = state.map(|entry| entry.multiplier).unwrap_or(1.0);

        // Lightweight feature prior to keep shadow predictions stable while model
        // is still cold; keeps behavior deterministic and bounded.
        let limit_factor = features
            .limit
            .map(|limit| (limit as f64 / features.table_rows.max(1.0)).clamp(0.05, 1.0))
            .unwrap_or(1.0);
        let predicate_factor = (1.0 / (features.predicate_terms.max(1) as f64)).clamp(0.2, 1.0);
        let prefix_factor = if features.index_prefix_columns == 0 {
            1.0
        } else {
            (1.0 / features.index_prefix_columns as f64).clamp(0.15, 1.0)
        };

        let prior = (limit_factor * predicate_factor * prefix_factor).clamp(0.05, 1.0);
        let multiplier = (0.5 * learned_multiplier + 0.5 * prior).clamp(0.05, 20.0);

        LearnedPrediction {
            predicted_rows: (features.estimated_rows.max(1.0) * multiplier).max(1.0),
            multiplier,
        }
    }

    pub fn observe(&self, signature: &str, estimated_rows: f64, actual_rows: f64) {
        let estimated_rows = estimated_rows.max(1.0);
        let ratio = (actual_rows.max(0.0) / estimated_rows).clamp(0.05, 20.0);

        let mut map = self
            .by_signature
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let state = map.entry(signature.to_string()).or_default();
        let alpha = if state.samples < 16 { 0.2 } else { 0.08 };
        state.multiplier = (1.0 - alpha) * state.multiplier + alpha * ratio;
        state.samples = state.samples.saturating_add(1);
    }
}
