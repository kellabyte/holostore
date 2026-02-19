#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CandidateKind {
    TableScan,
    IndexLookup,
    IndexOnly,
    OrderedIndex,
}

#[derive(Debug, Clone, Copy)]
pub struct CostRequest {
    pub kind: CandidateKind,
    pub estimated_output_rows: f64,
    pub estimated_scan_rows: f64,
    pub scan_fraction_of_table: f64,
    pub avg_row_width_bytes: f64,
    pub fanout: usize,
    pub probe_count: usize,
    pub limit: Option<usize>,
    pub uncertainty: f64,
    pub has_residual_predicate: bool,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CostEstimate {
    pub startup_cost: f64,
    pub cpu_cost: f64,
    pub io_cost: f64,
    pub network_cost: f64,
    pub uncertainty_penalty: f64,
    pub total_cost: f64,
}

pub trait CostModel: Send + Sync {
    fn name(&self) -> &'static str;
    fn estimate(&self, request: CostRequest) -> CostEstimate;
}

#[derive(Debug, Clone, Copy)]
pub struct DistributedCostWeights {
    pub cpu_per_row: f64,
    pub io_per_kb: f64,
    pub rpc_fanout_cost: f64,
    pub probe_rpc_cost: f64,
    pub primary_lookup_cost: f64,
    pub residual_predicate_cost: f64,
    pub ordered_limit_bonus_factor: f64,
    pub uncertainty_cost_multiplier: f64,
}

impl Default for DistributedCostWeights {
    fn default() -> Self {
        Self {
            cpu_per_row: 1.0,
            io_per_kb: 0.2,
            rpc_fanout_cost: 45.0,
            probe_rpc_cost: 8.0,
            primary_lookup_cost: 3.5,
            residual_predicate_cost: 0.3,
            ordered_limit_bonus_factor: 0.5,
            uncertainty_cost_multiplier: 0.8,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DistributedCostModel {
    pub weights: DistributedCostWeights,
}

impl Default for DistributedCostModel {
    fn default() -> Self {
        Self {
            weights: DistributedCostWeights::default(),
        }
    }
}

impl CostModel for DistributedCostModel {
    fn name(&self) -> &'static str {
        "distributed_v1"
    }

    fn estimate(&self, request: CostRequest) -> CostEstimate {
        let mut scan_rows = request.estimated_scan_rows.max(1.0);
        let output_rows = request.estimated_output_rows.max(0.0);
        if matches!(request.kind, CandidateKind::OrderedIndex) {
            if let Some(limit) = request.limit {
                let limited = (limit as f64 * 2.0).max(1.0);
                scan_rows = scan_rows.min(limited);
            }
        }

        let row_width_kb = (request.avg_row_width_bytes.max(1.0)) / 1024.0;
        let cpu_cost = self.weights.cpu_per_row * scan_rows;
        let io_cost = self.weights.io_per_kb * row_width_kb * scan_rows;
        let mut network_cost = self.weights.rpc_fanout_cost * request.fanout.max(1) as f64
            + self.weights.probe_rpc_cost * request.probe_count.max(1) as f64;

        match request.kind {
            CandidateKind::TableScan => {}
            CandidateKind::IndexLookup => {
                // Penalize high-selectivity non-covering lookups; they degrade into
                // large random key probes and are often slower than table scans.
                let lookup_penalty = if request.scan_fraction_of_table >= 0.35 {
                    2.5
                } else {
                    1.0
                };
                network_cost += self.weights.primary_lookup_cost * output_rows * lookup_penalty;
            }
            CandidateKind::IndexOnly => {
                network_cost += self.weights.primary_lookup_cost * output_rows * 0.15;
            }
            CandidateKind::OrderedIndex => {
                network_cost += self.weights.primary_lookup_cost * output_rows * 0.2;
            }
        }

        let residual_cost = if request.has_residual_predicate {
            self.weights.residual_predicate_cost * output_rows
        } else {
            0.0
        };
        let startup_cost = if matches!(request.kind, CandidateKind::TableScan) {
            2.0
        } else {
            6.0
        };

        let mut total_cost = startup_cost + cpu_cost + io_cost + network_cost + residual_cost;
        if matches!(request.kind, CandidateKind::OrderedIndex) {
            total_cost *= self.weights.ordered_limit_bonus_factor;
        }

        let uncertainty_penalty = total_cost
            * request.uncertainty.clamp(0.0, 1.0)
            * self.weights.uncertainty_cost_multiplier;
        total_cost += uncertainty_penalty;

        CostEstimate {
            startup_cost,
            cpu_cost,
            io_cost,
            network_cost,
            uncertainty_penalty,
            total_cost,
        }
    }
}
