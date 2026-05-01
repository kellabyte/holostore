//! Dependency-chain diagnostics used by execution and recovery.
//!
//! Purpose:
//! - Build compact blocked-transaction chains for stall logging and recovery
//!   target selection.
//!
//! Design:
//! - Helpers operate on an already-locked `State` reference and perform bounded
//!   scans so diagnostics do not become unbounded critical-section work.
//!
//! Inputs:
//! - The group state record table and committed queue.
//!
//! Outputs:
//! - Bounded blocked chains and selected recovery candidates.

use super::*;

#[derive(Debug)]
pub(super) struct BlockedStep {
    id: TxnId,
    status: Status,
    blocking_dep: Option<TxnId>,
    blocking_status: Option<Status>,
    blocking_missing: bool,
}

pub(super) fn build_blocking_chain(state: &State) -> (Option<TxnId>, Vec<BlockedStep>) {
    const MAX_CHAIN: usize = 8;
    let root = state
        .committed_queue
        .iter()
        .find_map(|(_, id)| {
            let rec = state.records.get(id)?;
            if rec.status != Status::Committed {
                return None;
            }
            let has_blocking = rec.deps.iter().any(|dep| !state.is_executed(dep));
            if has_blocking {
                Some(*id)
            } else {
                None
            }
        })
        .or_else(|| state.committed_queue.iter().next().map(|(_, id)| *id));

    let Some(mut current) = root else {
        return (None, Vec::new());
    };

    let mut chain = Vec::new();
    let mut seen = HashSet::new();
    for _ in 0..MAX_CHAIN {
        if !seen.insert(current) {
            break;
        }
        let (status, blocking_dep, blocking_status, blocking_missing) =
            match state.records.get(&current) {
                Some(rec) => {
                    let dep = rec.deps.iter().find(|dep| !state.is_executed(dep)).copied();
                    let dep_status = dep.and_then(|d| state.records.get(&d).map(|r| r.status));
                    (rec.status, dep, dep_status, false)
                }
                None => (Status::None, None, None, true),
            };

        chain.push(BlockedStep {
            id: current,
            status,
            blocking_dep,
            blocking_status,
            blocking_missing,
        });

        let Some(next) = blocking_dep else {
            break;
        };
        current = next;
    }

    (root, chain)
}

pub(super) fn build_blocking_chain_from(
    state: &State,
    root: TxnId,
    limit: usize,
) -> Vec<BlockedStep> {
    let mut chain = Vec::new();
    let mut seen = HashSet::new();
    let mut current = root;

    for _ in 0..limit {
        if !seen.insert(current) {
            break;
        }
        let (status, blocking_dep, blocking_status, blocking_missing) =
            match state.records.get(&current) {
                Some(rec) => {
                    let dep = rec.deps.iter().find(|dep| !state.is_executed(dep)).copied();
                    let dep_status = dep.and_then(|d| state.records.get(&d).map(|r| r.status));
                    (rec.status, dep, dep_status, false)
                }
                None => (Status::None, None, None, true),
            };

        chain.push(BlockedStep {
            id: current,
            status,
            blocking_dep,
            blocking_status,
            blocking_missing,
        });

        let Some(next) = blocking_dep else {
            break;
        };
        current = next;
    }

    chain
}

pub(super) fn pick_recovery_from_chain(chain: &[BlockedStep]) -> Option<TxnId> {
    if chain.is_empty() {
        return None;
    }

    let mut index = HashMap::with_capacity(chain.len());
    for (idx, step) in chain.iter().enumerate() {
        index.insert(step.id, idx);
    }

    let mut cycle_range: Option<(usize, usize)> = None;
    for (idx, step) in chain.iter().enumerate() {
        if let Some(dep) = step.blocking_dep {
            if let Some(&dep_idx) = index.get(&dep) {
                let start = dep_idx.min(idx);
                let end = dep_idx.max(idx);
                cycle_range = Some((start, end));
                break;
            }
        }
    }

    if let Some((start, end)) = cycle_range {
        let mut candidates = Vec::new();
        for step in &chain[start..=end] {
            if step.status < Status::Committed || step.blocking_missing {
                candidates.push(step.id);
            }
        }
        if candidates.is_empty() {
            candidates.extend(chain[start..=end].iter().map(|s| s.id));
        }
        candidates.sort();
        return candidates.first().copied();
    }

    for step in chain {
        if step.blocking_missing {
            return Some(step.id);
        }
        if let Some(dep) = step.blocking_dep {
            let needs_recovery = step
                .blocking_status
                .map(|s| s < Status::Committed)
                .unwrap_or(true);
            if needs_recovery {
                return Some(dep);
            }
        }
    }

    None
}

pub(super) fn first_blocking_dep(chain: &[BlockedStep]) -> Option<(TxnId, Option<Status>, bool)> {
    let mut fallback: Option<(TxnId, Option<Status>, bool)> = None;
    for step in chain {
        if let Some(dep) = step.blocking_dep {
            let status = step.blocking_status;
            if status.map(|s| s < Status::Committed).unwrap_or(true) {
                return Some((dep, status, step.blocking_missing));
            }
            if fallback.is_none() {
                fallback = Some((dep, status, step.blocking_missing));
            }
        }
        if step.blocking_missing {
            return Some((step.id, None, true));
        }
    }
    fallback
}
