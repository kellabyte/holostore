use std::collections::BTreeMap;
use std::sync::Mutex;

#[derive(Debug, Clone, Copy, Default)]
pub struct FeedbackObservation {
    pub multiplier_before: f64,
    pub multiplier_after: f64,
    pub absolute_error_ratio: f64,
    pub bad_miss: bool,
}

#[derive(Debug, Clone)]
struct FeedbackState {
    multiplier_ema: f64,
    samples: u64,
}

impl Default for FeedbackState {
    fn default() -> Self {
        Self {
            multiplier_ema: 1.0,
            samples: 0,
        }
    }
}

#[derive(Debug, Default)]
pub struct AdaptiveFeedbackController {
    inner: Mutex<BTreeMap<String, FeedbackState>>,
}

impl AdaptiveFeedbackController {
    pub fn adjust_estimate(&self, signature: &str, estimate: f64) -> (f64, f64) {
        let map = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let multiplier = map
            .get(signature)
            .map(|state| state.multiplier_ema)
            .unwrap_or(1.0)
            .clamp(0.05, 20.0);
        (estimate.max(1.0) * multiplier, multiplier)
    }

    pub fn observe(
        &self,
        signature: &str,
        estimated_rows: f64,
        actual_rows: f64,
    ) -> FeedbackObservation {
        let estimated_rows = estimated_rows.max(1.0);
        let actual_rows = actual_rows.max(0.0);
        let ratio = (actual_rows / estimated_rows).clamp(0.05, 20.0);

        let mut map = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let state = map.entry(signature.to_string()).or_default();
        let before = state.multiplier_ema;
        let abs_error_ratio = if ratio >= 1.0 { ratio } else { 1.0 / ratio };
        let mut alpha: f64 = if state.samples < 8 { 0.35 } else { 0.15 };
        if abs_error_ratio >= 4.0 {
            // Converge faster when we observe a large miss to reduce repeated bad plans.
            alpha = alpha.max(0.55);
        }
        state.multiplier_ema =
            ((1.0 - alpha) * state.multiplier_ema + alpha * ratio).clamp(0.05, 20.0);
        state.samples = state.samples.saturating_add(1);

        FeedbackObservation {
            multiplier_before: before,
            multiplier_after: state.multiplier_ema,
            absolute_error_ratio: abs_error_ratio,
            bad_miss: abs_error_ratio >= 4.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AdaptiveFeedbackController;

    #[test]
    fn feedback_fast_tracks_bad_miss() {
        let controller = AdaptiveFeedbackController::default();
        let observation = controller.observe("sig", 100.0, 2_000.0);
        assert!(observation.bad_miss);
        assert!(observation.multiplier_after >= 10.0);
    }

    #[test]
    fn adjusted_estimate_uses_wider_multiplier_range() {
        let controller = AdaptiveFeedbackController::default();
        controller.observe("sig", 100.0, 2_000.0);
        let (adjusted, multiplier) = controller.adjust_estimate("sig", 100.0);
        assert!(multiplier > 1.0);
        assert!((adjusted - (100.0 * multiplier)).abs() < f64::EPSILON);
    }
}
