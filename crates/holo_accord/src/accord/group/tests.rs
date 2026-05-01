//! Unit tests for the Accord group implementation.
//!
//! Purpose:
//! - Keep protocol model tests and deterministic worker/RPC fakes away from the
//!   production implementation modules.

use super::proposal::ProposeOnceError;
use super::recovery::{choose_recovery_value, RecoveryChoice};
use super::*;
use crate::accord::{CommitDurabilityMode, GroupId, TXN_COUNTER_SHARD_SHIFT};
use async_trait::async_trait;
use std::collections::BTreeSet as StdBTreeSet;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc as std_mpsc, Arc, Mutex};
use std::time::Duration as StdDuration;

/// Scripted response for one `accept` RPC target in round tests.
///
/// Purpose:
/// - Control per-peer latency and success/rejection behavior for Accept rounds.
///
/// Design:
/// - Optional delay simulates slow peers.
/// - Outcome captures either a concrete protocol response or a transport error.
///
/// Inputs:
/// - Delay budget and response outcome.
///
/// Outputs:
/// - Deterministic scripted `accept` behavior in tests.
#[derive(Clone, Debug)]
struct AcceptPeerPlan {
    delay: StdDuration,
    outcome: AcceptPeerOutcome,
}

#[derive(Clone, Debug)]
/// Scripted Accept RPC outcome for one peer in tests.
///
/// Purpose:
/// - Represent either a valid protocol response or an injected transport failure.
///
/// Design:
/// - `Response` mirrors `AcceptResponse` fields used by quorum logic.
/// - `TransportError` simulates failed RPC delivery/processing.
///
/// Inputs:
/// - `ok`/`promised` or static error message.
///
/// Outputs:
/// - Deterministic Accept behavior for one target peer.
enum AcceptPeerOutcome {
    Response { ok: bool, promised: Ballot },
    TransportError(&'static str),
}

/// Scripted response for one `pre_accept` RPC target in proposal tests.
///
/// Purpose:
/// - Control remote PreAccept replies so fast-path tests can exercise
///   proposal code without live peer groups.
///
/// Design:
/// - Optional delay simulates slow peers.
/// - Outcome captures either a concrete protocol response or a transport error.
///
/// Inputs:
/// - Delay budget and response outcome.
///
/// Outputs:
/// - Deterministic scripted `pre_accept` behavior in tests.
#[derive(Clone, Debug)]
struct PreAcceptPeerPlan {
    delay: StdDuration,
    outcome: PreAcceptPeerOutcome,
}

#[derive(Clone, Debug)]
/// Scripted PreAccept RPC outcome for one peer in tests.
///
/// Purpose:
/// - Represent either a valid protocol response or an injected transport failure.
///
/// Design:
/// - `Response` mirrors `PreAcceptResponse` fields used by fast-path merge.
///
/// Inputs:
/// - `ok`/`promised` plus sequence/dependency metadata.
///
/// Outputs:
/// - Deterministic PreAccept behavior for one target peer.
enum PreAcceptPeerOutcome {
    Response {
        ok: bool,
        promised: Ballot,
        seq: u64,
        deps: Vec<TxnId>,
    },
}

/// Scripted response for one `commit` RPC target in round tests.
///
/// Purpose:
/// - Control per-peer latency and success/failure behavior for Commit rounds.
///
/// Design:
/// - Optional delay simulates slow peers.
/// - Outcome captures either a protocol ACK or a transport error.
///
/// Inputs:
/// - Delay budget and response outcome.
///
/// Outputs:
/// - Deterministic scripted `commit` behavior in tests.
#[derive(Clone, Debug)]
struct CommitPeerPlan {
    delay: StdDuration,
    outcome: CommitPeerOutcome,
}

#[derive(Clone, Debug)]
/// Scripted Commit RPC outcome for one peer in tests.
///
/// Purpose:
/// - Represent either a valid commit ACK or an injected transport failure.
///
/// Design:
/// - `Response` carries the `ok` bit consumed by quorum counting.
/// - `TransportError` simulates failed RPC delivery/processing.
///
/// Inputs:
/// - `ok` flag or static error message.
///
/// Outputs:
/// - Deterministic Commit behavior for one target peer.
enum CommitPeerOutcome {
    Response { ok: bool },
    TransportError(&'static str),
}

/// Scriptable transport used by group-round unit tests.
///
/// Purpose:
/// - Provide deterministic per-peer behavior for PreAccept/Accept/Commit RPCs.
///
/// Design:
/// - PreAccept/Accept/Commit methods use configured per-peer plans.
/// - All other RPC methods fail loudly to catch accidental test misuse.
///
/// Inputs:
/// - Optional preaccept, accept, and commit plans keyed by peer id.
///
/// Outputs:
/// - Transport behavior tailored to one test scenario.
struct NoopTransport {
    pre_accept_plans: Arc<Mutex<HashMap<NodeId, PreAcceptPeerPlan>>>,
    accept_plans: Arc<Mutex<HashMap<NodeId, AcceptPeerPlan>>>,
    commit_plans: Arc<Mutex<HashMap<NodeId, CommitPeerPlan>>>,
}

impl NoopTransport {
    /// Build a transport with no scripted peer responses.
    ///
    /// Purpose:
    /// - Preserve prior "always fail" behavior for tests that do not use remote RPC paths.
    ///
    /// Design:
    /// - Initializes empty per-peer plan tables.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Outputs:
    /// - `NoopTransport` where all RPC methods fail unless explicitly scripted.
    fn new() -> Self {
        Self {
            pre_accept_plans: Arc::new(Mutex::new(HashMap::new())),
            accept_plans: Arc::new(Mutex::new(HashMap::new())),
            commit_plans: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Build a transport with scripted PreAccept/Accept/Commit peer plans.
    ///
    /// Purpose:
    /// - Configure deterministic remote behavior for proposal/round tests.
    ///
    /// Design:
    /// - Stores per-peer plans in shared mutex maps for lightweight lookup.
    ///
    /// Inputs:
    /// - `pre_accept_plans`: per-peer PreAccept behaviors.
    /// - `accept_plans`: per-peer Accept behaviors.
    /// - `commit_plans`: per-peer Commit behaviors.
    ///
    /// Outputs:
    /// - `NoopTransport` with scripted PreAccept/Accept/Commit responses.
    fn with_plans(
        pre_accept_plans: HashMap<NodeId, PreAcceptPeerPlan>,
        accept_plans: HashMap<NodeId, AcceptPeerPlan>,
        commit_plans: HashMap<NodeId, CommitPeerPlan>,
    ) -> Self {
        Self {
            pre_accept_plans: Arc::new(Mutex::new(pre_accept_plans)),
            accept_plans: Arc::new(Mutex::new(accept_plans)),
            commit_plans: Arc::new(Mutex::new(commit_plans)),
        }
    }
}

#[async_trait]
impl Transport for NoopTransport {
    async fn pre_accept(
        &self,
        target: NodeId,
        _req: PreAcceptRequest,
    ) -> anyhow::Result<PreAcceptResponse> {
        let plan = self
            .pre_accept_plans
            .lock()
            .expect("pre_accept plan lock")
            .get(&target)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("transport not used in this test"))?;
        if !plan.delay.is_zero() {
            time::sleep(plan.delay).await;
        }
        match plan.outcome {
            PreAcceptPeerOutcome::Response {
                ok,
                promised,
                seq,
                deps,
            } => Ok(PreAcceptResponse {
                ok,
                promised,
                seq,
                deps,
            }),
        }
    }

    /// Execute scripted `accept` behavior for one target peer.
    ///
    /// Purpose:
    /// - Feed deterministic Accept outcomes into quorum-round tests.
    ///
    /// Design:
    /// - Reads one plan by target id, applies optional delay, then emits either
    ///   a protocol response or a transport error.
    ///
    /// Inputs:
    /// - `target`: peer id used to select the plan.
    /// - `_req`: protocol request payload (unused by scripted responses).
    ///
    /// Outputs:
    /// - `AcceptResponse`/error according to the configured per-peer plan.
    async fn accept(&self, target: NodeId, _req: AcceptRequest) -> anyhow::Result<AcceptResponse> {
        let plan = self
            .accept_plans
            .lock()
            .expect("accept plan lock")
            .get(&target)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("transport not used in this test"))?;
        if !plan.delay.is_zero() {
            // Simulate a slow peer without blocking executor threads.
            time::sleep(plan.delay).await;
        }
        match plan.outcome {
            AcceptPeerOutcome::Response { ok, promised } => Ok(AcceptResponse { ok, promised }),
            AcceptPeerOutcome::TransportError(msg) => Err(anyhow::anyhow!(msg)),
        }
    }

    /// Execute scripted `commit` behavior for one target peer.
    ///
    /// Purpose:
    /// - Feed deterministic Commit outcomes into quorum-round tests.
    ///
    /// Design:
    /// - Reads one plan by target id, applies optional delay, then emits either
    ///   a protocol ACK or a transport error.
    ///
    /// Inputs:
    /// - `target`: peer id used to select the plan.
    /// - `_req`: protocol request payload (unused by scripted responses).
    ///
    /// Outputs:
    /// - `CommitResponse`/error according to the configured per-peer plan.
    async fn commit(&self, target: NodeId, _req: CommitRequest) -> anyhow::Result<CommitResponse> {
        let plan = self
            .commit_plans
            .lock()
            .expect("commit plan lock")
            .get(&target)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("transport not used in this test"))?;
        if !plan.delay.is_zero() {
            // Simulate a slow peer without blocking executor threads.
            time::sleep(plan.delay).await;
        }
        match plan.outcome {
            CommitPeerOutcome::Response { ok } => Ok(CommitResponse { ok }),
            CommitPeerOutcome::TransportError(msg) => Err(anyhow::anyhow!(msg)),
        }
    }

    async fn recover(
        &self,
        _target: NodeId,
        _req: RecoverRequest,
    ) -> anyhow::Result<RecoverResponse> {
        Err(anyhow::anyhow!("transport not used in this test"))
    }

    async fn fetch_command(
        &self,
        _target: NodeId,
        _group_id: GroupId,
        _txn_id: TxnId,
    ) -> anyhow::Result<Option<Bytes>> {
        Err(anyhow::anyhow!("transport not used in this test"))
    }

    async fn report_executed(
        &self,
        _target: NodeId,
        _req: ReportExecutedRequest,
    ) -> anyhow::Result<ReportExecutedResponse> {
        Err(anyhow::anyhow!("transport not used in this test"))
    }

    async fn last_executed_prefix(
        &self,
        _target: NodeId,
        _group_id: GroupId,
    ) -> anyhow::Result<Vec<ExecutedPrefix>> {
        Err(anyhow::anyhow!("transport not used in this test"))
    }

    async fn executed(
        &self,
        _target: NodeId,
        _group_id: GroupId,
        _txn_id: TxnId,
    ) -> anyhow::Result<bool> {
        Err(anyhow::anyhow!("transport not used in this test"))
    }

    async fn mark_visible(
        &self,
        _target: NodeId,
        _group_id: GroupId,
        _txn_id: TxnId,
    ) -> anyhow::Result<bool> {
        Err(anyhow::anyhow!("transport not used in this test"))
    }
}

/// Minimal state machine that classifies every non-empty command as a write.
///
/// Inputs:
/// - `data`: opaque command bytes from Accord.
///
/// Output:
/// - One synthetic write key for non-empty commands, enabling write commit
///   bookkeeping in `rpc_commit` during unit tests.
struct TestStateMachine;

impl StateMachine for TestStateMachine {
    fn command_keys(&self, data: &[u8]) -> anyhow::Result<CommandKeys> {
        if data.is_empty() {
            return Ok(CommandKeys::default());
        }
        Ok(CommandKeys {
            reads: Vec::new(),
            writes: vec![b"unit-test-key".to_vec()],
        })
    }

    fn apply(&self, _data: &[u8], _meta: ExecMeta) -> anyhow::Result<()> {
        Ok(())
    }
}

/// State-machine test double that counts direct apply and visibility calls.
///
/// Purpose:
/// - Verify executor visibility bookkeeping without relying on KV storage.
///
/// Design:
/// - Classifies non-empty commands as writes.
/// - Counts `apply` and `mark_visible` invocations independently so tests
///   can assert direct-published writes do not re-enter compatibility
///   visibility publication.
#[derive(Default)]
struct VisibilityTrackingStateMachine {
    apply_count: AtomicU64,
    mark_visible_count: AtomicU64,
}

impl StateMachine for VisibilityTrackingStateMachine {
    fn command_keys(&self, data: &[u8]) -> anyhow::Result<CommandKeys> {
        if data.is_empty() {
            return Ok(CommandKeys::default());
        }
        Ok(CommandKeys {
            reads: Vec::new(),
            writes: vec![b"unit-test-key".to_vec()],
        })
    }

    fn apply(&self, _data: &[u8], _meta: ExecMeta) -> anyhow::Result<()> {
        self.apply_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn mark_visible(&self, _data: &[u8], _meta: ExecMeta) -> anyhow::Result<()> {
        self.mark_visible_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

/// Enumerate fixed-size subsets for small protocol model tests.
///
/// Purpose:
/// - Keep quorum-intersection checks exhaustive without introducing a
///   property-test dependency.
///
/// Design:
/// - Depth-first combination generator over tiny test inputs.
///
/// Inputs:
/// - `items`: candidate node ids.
/// - `size`: desired subset cardinality.
///
/// Outputs:
/// - All subsets of `items` with exactly `size` elements.
fn subsets_of_size(items: &[usize], size: usize) -> Vec<StdBTreeSet<usize>> {
    fn rec(
        items: &[usize],
        size: usize,
        idx: usize,
        cur: &mut StdBTreeSet<usize>,
        out: &mut Vec<StdBTreeSet<usize>>,
    ) {
        if cur.len() == size {
            out.push(cur.clone());
            return;
        }
        if idx >= items.len() {
            return;
        }
        let remaining_needed = size.saturating_sub(cur.len());
        if items.len().saturating_sub(idx) < remaining_needed {
            return;
        }

        cur.insert(items[idx]);
        rec(items, size, idx + 1, cur, out);
        cur.remove(&items[idx]);
        rec(items, size, idx + 1, cur, out);
    }

    let mut out = Vec::new();
    let mut cur = StdBTreeSet::new();
    rec(items, size, 0, &mut cur, &mut out);
    out
}

/// Commit log that blocks durable appends until the test releases it.
///
/// Design:
/// - Sends a one-time start signal when the durable append begins.
/// - Waits on `release_rx` before returning success.
struct BlockingDurableCommitLog {
    started_tx: Mutex<Option<std_mpsc::Sender<()>>>,
    release_rx: Mutex<std_mpsc::Receiver<()>>,
    append_calls: AtomicU64,
}

impl BlockingDurableCommitLog {
    /// Construct a blocking commit log with external start/release channels.
    ///
    /// Inputs:
    /// - `started_tx`: signaled once the first durable append starts.
    /// - `release_rx`: gate that must be released before append returns.
    ///
    /// Output:
    /// - Commit-log test double that lets tests control durable-append timing.
    fn new(started_tx: std_mpsc::Sender<()>, release_rx: std_mpsc::Receiver<()>) -> Self {
        Self {
            started_tx: Mutex::new(Some(started_tx)),
            release_rx: Mutex::new(release_rx),
            append_calls: AtomicU64::new(0),
        }
    }
}

impl CommitLog for BlockingDurableCommitLog {
    fn append_commits_with_options(
        &self,
        _entries: Vec<CommitLogEntry>,
        options: CommitLogAppendOptions,
    ) -> anyhow::Result<()> {
        self.append_calls.fetch_add(1, Ordering::Relaxed);
        if options.require_durable {
            if let Some(tx) = self.started_tx.lock().expect("started tx lock").take() {
                let _ = tx.send(());
            }
            self.release_rx
                .lock()
                .expect("release rx lock")
                .recv()
                .map_err(|_| anyhow::anyhow!("release signal dropped"))?;
        }
        Ok(())
    }

    fn mark_executed(&self, _txn_id: TxnId) -> anyhow::Result<()> {
        Ok(())
    }

    fn load(&self) -> anyhow::Result<Vec<CommitLogEntry>> {
        Ok(Vec::new())
    }

    fn compact(&self, _max_delete: usize) -> anyhow::Result<usize> {
        Ok(0)
    }
}

/// Commit log that injects an error whenever durable append is requested.
///
/// Design:
/// - Used to verify sync-commit failure propagation from WAL to RPC caller.
struct FailingDurableCommitLog;

impl CommitLog for FailingDurableCommitLog {
    fn append_commits_with_options(
        &self,
        _entries: Vec<CommitLogEntry>,
        options: CommitLogAppendOptions,
    ) -> anyhow::Result<()> {
        if options.require_durable {
            anyhow::bail!("injected durable append failure");
        }
        Ok(())
    }

    fn mark_executed(&self, _txn_id: TxnId) -> anyhow::Result<()> {
        Ok(())
    }

    fn load(&self) -> anyhow::Result<Vec<CommitLogEntry>> {
        Ok(Vec::new())
    }

    fn compact(&self, _max_delete: usize) -> anyhow::Result<usize> {
        Ok(0)
    }
}

/// Build a compact, single-node group config for commit-path unit tests.
///
/// Input:
/// - `mode`: commit durability mode under test.
///
/// Output:
/// - Deterministic config with short timeouts and enabled commit-log batching.
fn test_config(mode: CommitDurabilityMode) -> Config {
    Config {
        group_id: 1,
        node_id: 1,
        txn_epoch: 1,
        members: vec![Member { id: 1 }],
        rpc_timeout: StdDuration::from_millis(200),
        propose_timeout: StdDuration::from_secs(2),
        recovery_min_delay: StdDuration::from_millis(10),
        stall_recover_interval: StdDuration::from_millis(10),
        preaccept_stall_hits: 1,
        execute_batch_max: 16,
        inline_command_in_accept_commit: true,
        executed_command_cache_max_bytes: 64 * 1024 * 1024,
        commit_log_batch_max: 16,
        commit_log_batch_wait: StdDuration::from_micros(50),
        commit_durability_mode: mode,
        fast_path_1rtt: false,
    }
}

/// Build a multi-node config tuned for quorum-round unit tests.
///
/// Purpose:
/// - Provide deterministic quorum sizing and timeout behavior for Accept/Commit tests.
///
/// Design:
/// - Uses caller-supplied members and timeouts with otherwise stable defaults.
/// - Supports "observer local node" tests by allowing `node_id` outside the voter set.
///
/// Inputs:
/// - `node_id`: local test node id.
/// - `members`: runtime voter/member ids.
/// - `rpc_timeout`: round-level peer wait budget.
/// - `propose_timeout`: per-RPC timeout budget for commit fanout.
///
/// Outputs:
/// - Config suitable for direct `run_accept_round`/`run_commit_round` tests.
fn round_test_config(
    node_id: NodeId,
    members: Vec<NodeId>,
    rpc_timeout: StdDuration,
    propose_timeout: StdDuration,
) -> Config {
    Config {
        group_id: 1,
        node_id,
        txn_epoch: 1,
        members: members.into_iter().map(|id| Member { id }).collect(),
        rpc_timeout,
        propose_timeout,
        recovery_min_delay: StdDuration::from_millis(10),
        stall_recover_interval: StdDuration::from_millis(10),
        preaccept_stall_hits: 1,
        execute_batch_max: 16,
        inline_command_in_accept_commit: true,
        executed_command_cache_max_bytes: 64 * 1024 * 1024,
        commit_log_batch_max: 16,
        commit_log_batch_wait: StdDuration::from_micros(50),
        commit_durability_mode: CommitDurabilityMode::AsyncCommit,
        fast_path_1rtt: false,
    }
}

/// Build a deterministic commit request for local `rpc_commit` tests.
///
/// Input:
/// - `counter`: transaction counter to make test txn IDs unique.
///
/// Output:
/// - Valid commit request with fixed command payload and digest.
fn test_commit_request(counter: u64) -> CommitRequest {
    let command = Bytes::from_static(b"set unit-test-key value");
    CommitRequest {
        group_id: 1,
        txn_id: TxnId {
            node_id: 1,
            counter,
        },
        ballot: Ballot::initial(1),
        command: command.clone(),
        command_digest: command_digest(&command),
        has_command: true,
        seq: 1,
        deps: Vec::new(),
    }
}

fn encoded_txn(node_id: NodeId, epoch: u64, seq: u64) -> TxnId {
    TxnId {
        node_id,
        counter: make_txn_counter(1, epoch, seq).expect("encode txn counter"),
    }
}

/// Ensure direct-published executed writes are visible to GC bookkeeping.
///
/// Purpose:
/// - Protect the collapsed apply+visibility path: once `apply_batch`
///   succeeds, the executed-log entry must be considered visible even
///   though no compatibility `mark_visible` call rewrote storage.
///
/// Design:
/// - Commit and execute one local write with a tracking state machine.
/// - Verify `mark_visible` short-circuits without calling the state machine.
/// - Force the executed-log GC window and confirm the visible executed entry
///   is reclaimable.
///
/// Outputs:
/// - Assertion failure if direct-published writes stay invisible.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn direct_published_writes_are_marked_visible_for_gc() {
    let sm = Arc::new(VisibilityTrackingStateMachine::default());
    let group = Arc::new(Group::new(
        test_config(CommitDurabilityMode::AsyncCommit),
        Arc::new(NoopTransport::new()),
        sm.clone(),
        None,
    ));
    let req = test_commit_request(1);
    let txn_id = req.txn_id;

    let commit = group.rpc_commit(req).await;
    assert!(commit.ok, "local commit should succeed");
    assert!(
        group.execute_progress().await.expect("execute progress"),
        "executor should apply the committed write"
    );
    assert_eq!(sm.apply_count.load(Ordering::Relaxed), 1);

    {
        let state = group.state.lock().await;
        let entry = state
            .executed_log
            .get(&txn_id)
            .expect("executed log should keep the write");
        assert!(entry.visible);
    }

    assert!(group.mark_visible(txn_id).await.expect("mark visible"));
    assert_eq!(
        sm.mark_visible_count.load(Ordering::Relaxed),
        0,
        "compatibility mark_visible should not republish direct-applied writes"
    );

    {
        let mut state = group.state.lock().await;
        let now = time::Instant::now();
        state.last_executed_gc_at = now - StdDuration::from_secs(1);
        let removed = Group::maybe_gc_executed_log_locked(
            group.config.node_id,
            &group.config.members,
            &mut state,
            now,
            group.config.executed_command_cache_max_bytes,
        );
        assert_eq!(removed, 1);
        assert!(!state.executed_log.contains_key(&txn_id));
    }
}

#[test]
fn encoded_counter_execution_advances_dense_epoch_prefix() {
    let mut state = State::new();
    let stream = TxnProgressKey {
        node_id: 2,
        epoch: 7,
    };
    let first = encoded_txn(2, 7, 1);
    let second = encoded_txn(2, 7, 2);
    let third = encoded_txn(2, 7, 3);

    state.mark_executed(third);
    assert_eq!(
        state.executed_prefix_by_stream.get(&stream).copied(),
        Some(0)
    );
    assert!(state.executed_out_of_order.contains(&third));

    state.mark_executed(first);
    assert_eq!(
        state.executed_prefix_by_stream.get(&stream).copied(),
        Some(1)
    );
    assert!(state.executed_out_of_order.contains(&third));

    state.mark_executed(second);
    assert_eq!(
        state.executed_prefix_by_stream.get(&stream).copied(),
        Some(3)
    );
    assert!(state.executed_out_of_order.is_empty());
    assert!(state.is_executed(&third));
}

#[test]
fn executed_log_gc_uses_epoch_sequence_prefix_not_raw_counter() {
    let now = time::Instant::now();
    let txn_id = encoded_txn(2, 7, 1);
    let stream = txn_progress_key(txn_id);
    let mut state = State::new();
    state.mark_executed(txn_id);
    state.record_executed_value(
        txn_id,
        ExecutedLogEntry {
            command: Some(Bytes::from_static(b"set unit-test-key value")),
            command_digest: None,
            keys: vec![b"unit-test-key".to_vec()],
            seq: 1,
            deps: Vec::new(),
            visible: true,
        },
    );
    state
        .reported_executed_prefix_by_peer
        .entry(2)
        .or_default()
        .insert(stream, 1);
    state
        .reported_executed_prefix_by_peer
        .entry(3)
        .or_default()
        .insert(stream, 1);
    state.last_executed_gc_at = now - StdDuration::from_secs(1);

    let members = vec![Member { id: 1 }, Member { id: 2 }, Member { id: 3 }];
    let removed = Group::maybe_gc_executed_log_locked(1, &members, &mut state, now, 1);

    assert_eq!(removed, 1);
    assert!(!state.executed_log.contains_key(&txn_id));
    assert_eq!(state.executed_log_bytes, 0);
}

#[test]
fn executed_log_gc_prunes_stable_key_indexes() {
    let now = time::Instant::now();
    let txn_id = encoded_txn(2, 7, 1);
    let stream = txn_progress_key(txn_id);
    let key = b"unit-test-key".to_vec();
    let mut state = State::new();
    state.mark_executed(txn_id);
    state.last_write_by_key.insert(key.clone(), txn_id);
    state
        .last_committed_write_by_key
        .insert(key.clone(), (txn_id, 42));
    state.record_executed_value(
        txn_id,
        ExecutedLogEntry {
            command: Some(Bytes::from_static(b"set unit-test-key value")),
            command_digest: None,
            keys: vec![key.clone()],
            seq: 1,
            deps: Vec::new(),
            visible: true,
        },
    );
    state
        .reported_executed_prefix_by_peer
        .entry(2)
        .or_default()
        .insert(stream, 1);
    state
        .reported_executed_prefix_by_peer
        .entry(3)
        .or_default()
        .insert(stream, 1);
    state.last_executed_gc_at = now - StdDuration::from_secs(1);

    let members = vec![Member { id: 1 }, Member { id: 2 }, Member { id: 3 }];
    let removed = Group::maybe_gc_executed_log_locked(1, &members, &mut state, now, 1);

    assert_eq!(removed, 1);
    assert!(!state.executed_log.contains_key(&txn_id));
    assert!(!state.last_write_by_key.contains_key(&key));
    assert!(!state.last_committed_write_by_key.contains_key(&key));
}

#[test]
fn stable_committed_key_hint_is_not_reintroduced_as_dependency() {
    let config = test_config(CommitDurabilityMode::AsyncCommit);
    let committed = encoded_txn(1, config.txn_epoch, 1);
    let next = encoded_txn(1, config.txn_epoch, 2);
    let key = b"unit-test-key".to_vec();
    let keys = CommandKeys {
        reads: Vec::new(),
        writes: vec![key.clone()],
    };
    let mut state = State::new();
    state.mark_executed(committed);
    state
        .last_committed_write_by_key
        .insert(key.clone(), (committed, 7));
    state.last_write_by_key.insert(key, committed);

    let (seq, deps) = state.compute_seq_deps(&config, next, &keys);

    assert_eq!(seq, 1);
    assert!(
        deps.is_empty(),
        "globally stable committed hints should not extend dependency chains"
    );
}

/// Ensure sync-commit mode never ACKs before the durable append completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sync_commit_waits_for_durable_append_before_ack() {
    let (started_tx, started_rx) = std_mpsc::channel();
    let (release_tx, release_rx) = std_mpsc::channel();
    let commit_log = Arc::new(BlockingDurableCommitLog::new(started_tx, release_rx));
    let group = Arc::new(Group::new(
        test_config(CommitDurabilityMode::SyncCommit),
        Arc::new(NoopTransport::new()),
        Arc::new(TestStateMachine),
        Some(commit_log),
    ));

    let req = test_commit_request(10);
    let group_task = group.clone();
    let join = tokio::spawn(async move { group_task.rpc_commit(req).await });

    tokio::task::block_in_place(|| {
        started_rx
            .recv_timeout(StdDuration::from_secs(1))
            .expect("durable append should start")
    });
    assert!(
        !join.is_finished(),
        "sync commit returned before durable append completed"
    );

    release_tx.send(()).expect("release durable append");
    let resp = tokio::time::timeout(StdDuration::from_secs(1), join)
        .await
        .expect("commit should finish after release")
        .expect("join should succeed");
    assert!(
        resp.ok,
        "commit should succeed after durable append completion"
    );
}

/// Ensure commit-round metrics expose synchronous durable wait on the local path.
///
/// Purpose:
/// - Prove the new passive breakdown isolates WAL/fsync wait without
///   changing sync-commit ACK semantics.
///
/// Design:
/// - Run a single-node commit round against the blocking durable commit log.
/// - Assert the returned timings and exported debug stats both report a
///   non-zero durable-wait sample.
///
/// Inputs:
/// - One blocking durable commit-log test double.
///
/// Outputs:
/// - Non-zero local durable timing metrics and zero remote-quorum samples.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sync_commit_round_records_local_durable_wait_metrics() {
    let (started_tx, started_rx) = std_mpsc::channel();
    let (release_tx, release_rx) = std_mpsc::channel();
    let commit_log = Arc::new(BlockingDurableCommitLog::new(started_tx, release_rx));
    let group = Arc::new(Group::new(
        test_config(CommitDurabilityMode::SyncCommit),
        Arc::new(NoopTransport::new()),
        Arc::new(TestStateMachine),
        Some(commit_log),
    ));

    let command = Bytes::from_static(b"sync-commit-round-metrics");
    let group_task = group.clone();
    let join = tokio::spawn(async move {
        group_task
            .run_commit_round(
                TxnId {
                    node_id: 1,
                    counter: 12,
                },
                Ballot::initial(1),
                command.clone(),
                command_digest(&command),
                1,
                Vec::new(),
                true,
            )
            .await
    });

    tokio::task::block_in_place(|| {
        started_rx
            .recv_timeout(StdDuration::from_secs(1))
            .expect("durable append should start")
    });
    assert!(
        !join.is_finished(),
        "commit round returned before durable append completed"
    );

    release_tx.send(()).expect("release durable append");
    let timings = tokio::time::timeout(StdDuration::from_secs(1), join)
        .await
        .expect("commit round should finish after release")
        .expect("join should succeed")
        .expect("commit round should succeed");

    assert!(
        timings.local_durable_wait_us > 0,
        "commit round should report a durable wait sample"
    );
    assert_eq!(
        timings.remote_quorum_wait_us, 0,
        "single-node commit should not report remote quorum wait"
    );

    let stats = group.debug_stats().await;
    assert_eq!(stats.commit_local_durable_count, 1);
    assert!(
        stats.commit_local_durable_total_us > 0,
        "exported durable wait should be non-zero"
    );
    assert_eq!(stats.commit_local_log_queue_count, 1);
    assert_eq!(stats.commit_local_log_append_count, 1);
    assert!(
        stats.commit_local_log_append_total_us > 0,
        "exported append execution should be non-zero"
    );
    assert_eq!(stats.commit_local_post_durable_state_count, 1);
    assert_eq!(stats.commit_remote_quorum_count, 0);
    assert_eq!(stats.commit_tail_count, 0);
}

/// Ensure sync-commit mode surfaces durable append failures to callers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sync_commit_fails_when_durable_append_fails() {
    let group = Arc::new(Group::new(
        test_config(CommitDurabilityMode::SyncCommit),
        Arc::new(NoopTransport::new()),
        Arc::new(TestStateMachine),
        Some(Arc::new(FailingDurableCommitLog)),
    ));

    let resp = group.rpc_commit(test_commit_request(11)).await;
    assert!(!resp.ok, "sync commit must fail when durable append fails");

    let stats = group.debug_stats().await;
    assert_eq!(
        stats.records_status_committed_len, 0,
        "failed durable commit must not transition record to committed"
    );
    assert_eq!(
        stats.committed_queue_len, 0,
        "failed durable commit must not enqueue execution"
    );
}

/// Verify Accept round returns immediately once quorum ACKs, without waiting
/// for a much slower peer response.
///
/// Purpose:
/// - Validate Stage 4A behavior: quorum completion should not block on tail peers.
///
/// Design:
/// - Two peers ACK quickly and one peer responds much later.
/// - The test asserts wall-clock completion well below the slow-peer delay.
///
/// Inputs:
/// - Scripted per-peer Accept delays and responses.
///
/// Outputs:
/// - Successful Accept response with bounded elapsed time.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accept_round_reaches_quorum_without_waiting_for_slowest_peer() {
    let ballot = Ballot::initial(7);
    let transport = NoopTransport::with_plans(
        HashMap::new(),
        HashMap::from([
            (
                1,
                AcceptPeerPlan {
                    delay: StdDuration::from_millis(5),
                    outcome: AcceptPeerOutcome::Response {
                        ok: true,
                        promised: ballot,
                    },
                },
            ),
            (
                2,
                AcceptPeerPlan {
                    delay: StdDuration::from_millis(10),
                    outcome: AcceptPeerOutcome::Response {
                        ok: true,
                        promised: ballot,
                    },
                },
            ),
            (
                3,
                AcceptPeerPlan {
                    delay: StdDuration::from_millis(400),
                    outcome: AcceptPeerOutcome::Response {
                        ok: true,
                        promised: ballot,
                    },
                },
            ),
        ]),
        HashMap::new(),
    );
    let group = Group::new(
        round_test_config(
            99,
            vec![1, 2, 3],
            StdDuration::from_millis(120),
            StdDuration::from_secs(1),
        ),
        Arc::new(transport),
        Arc::new(TestStateMachine),
        None,
    );

    let command = Bytes::from_static(b"accept-round-test");
    let start = time::Instant::now();
    let resp = group
        .run_accept_round(
            TxnId {
                node_id: 7,
                counter: 1,
            },
            ballot,
            command.clone(),
            command_digest(&command),
            1,
            Vec::new(),
            true,
        )
        .await
        .expect("accept round should reach quorum");
    let elapsed = start.elapsed();

    assert!(resp.ok, "accept quorum should succeed");
    assert!(
            elapsed < StdDuration::from_millis(200),
            "accept round should return at quorum instead of waiting for 400ms slow peer (elapsed={elapsed:?})"
        );
}

/// Verify Accept round reports rejection when a higher promised ballot is observed.
///
/// Purpose:
/// - Preserve ballot monotonicity behavior while changing fanout implementation.
///
/// Design:
/// - One peer returns `ok=false` with a higher promised ballot and the others fail.
///
/// Inputs:
/// - Scripted Accept plans with mixed rejection and transport failures.
///
/// Outputs:
/// - Non-quorum `AcceptResponse` carrying the higher promised ballot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accept_round_returns_rejection_when_higher_ballot_observed() {
    let ballot = Ballot::initial(7);
    let higher = Ballot {
        counter: ballot.counter.saturating_add(3),
        node_id: 3,
    };
    let transport = NoopTransport::with_plans(
        HashMap::new(),
        HashMap::from([
            (
                1,
                AcceptPeerPlan {
                    delay: StdDuration::from_millis(5),
                    outcome: AcceptPeerOutcome::Response {
                        ok: false,
                        promised: higher,
                    },
                },
            ),
            (
                2,
                AcceptPeerPlan {
                    delay: StdDuration::from_millis(5),
                    outcome: AcceptPeerOutcome::TransportError("injected accept failure"),
                },
            ),
            (
                3,
                AcceptPeerPlan {
                    delay: StdDuration::from_millis(5),
                    outcome: AcceptPeerOutcome::TransportError("injected accept failure"),
                },
            ),
        ]),
        HashMap::new(),
    );
    let group = Group::new(
        round_test_config(
            99,
            vec![1, 2, 3],
            StdDuration::from_millis(80),
            StdDuration::from_secs(1),
        ),
        Arc::new(transport),
        Arc::new(TestStateMachine),
        None,
    );

    let command = Bytes::from_static(b"accept-reject-test");
    let resp = group
        .run_accept_round(
            TxnId {
                node_id: 7,
                counter: 2,
            },
            ballot,
            command.clone(),
            command_digest(&command),
            1,
            Vec::new(),
            true,
        )
        .await
        .expect("accept round should surface rejection instead of no-quorum");

    assert!(!resp.ok, "accept should reject on higher promised ballot");
    assert_eq!(
        resp.promised, higher,
        "accept rejection should carry highest promised ballot"
    );
}

/// Verify Commit round returns immediately once voter quorum ACKs, without
/// waiting for a much slower peer response.
///
/// Purpose:
/// - Validate Stage 4A behavior on commit fanout.
///
/// Design:
/// - Two peers ACK quickly and one peer responds much later.
/// - The test asserts wall-clock completion well below the slow-peer delay.
///
/// Inputs:
/// - Scripted per-peer Commit delays and responses.
///
/// Outputs:
/// - Successful commit result with bounded elapsed time.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_round_reaches_quorum_without_waiting_for_slowest_peer() {
    let ballot = Ballot::initial(7);
    let transport = NoopTransport::with_plans(
        HashMap::new(),
        HashMap::new(),
        HashMap::from([
            (
                1,
                CommitPeerPlan {
                    delay: StdDuration::from_millis(5),
                    outcome: CommitPeerOutcome::Response { ok: true },
                },
            ),
            (
                2,
                CommitPeerPlan {
                    delay: StdDuration::from_millis(10),
                    outcome: CommitPeerOutcome::Response { ok: true },
                },
            ),
            (
                3,
                CommitPeerPlan {
                    delay: StdDuration::from_millis(400),
                    outcome: CommitPeerOutcome::Response { ok: true },
                },
            ),
        ]),
    );
    let group = Group::new(
        round_test_config(
            99,
            vec![1, 2, 3],
            StdDuration::from_millis(120),
            StdDuration::from_secs(1),
        ),
        Arc::new(transport),
        Arc::new(TestStateMachine),
        None,
    );

    let command = Bytes::from_static(b"commit-round-test");
    let start = time::Instant::now();
    group
        .run_commit_round(
            TxnId {
                node_id: 7,
                counter: 3,
            },
            ballot,
            command.clone(),
            command_digest(&command),
            1,
            Vec::new(),
            true,
        )
        .await
        .expect("commit round should reach quorum");
    let elapsed = start.elapsed();

    assert!(
            elapsed < StdDuration::from_millis(200),
            "commit round should return at quorum instead of waiting for 400ms slow peer (elapsed={elapsed:?})"
        );

    time::sleep(StdDuration::from_millis(450)).await;
    let stats = group.debug_stats().await;
    assert_eq!(stats.commit_remote_quorum_count, 1);
    assert!(
        stats.commit_remote_quorum_total_us > 0,
        "commit round should record remote quorum wait"
    );
    assert_eq!(stats.commit_tail_count, 1);
    assert!(
        stats.commit_tail_total_us > 0,
        "commit round should record the follower tail after quorum"
    );
    assert_eq!(stats.commit_quorum_closer_top.len(), 1);
    assert_eq!(stats.commit_quorum_closer_top[0].node_id, 2);
    assert_eq!(stats.commit_quorum_closer_top[0].count, 1);
}

/// Verify Commit round returns a no-quorum error when enough voter ACKs are
/// not observed before timeout.
///
/// Purpose:
/// - Preserve existing error behavior while changing fanout implementation.
///
/// Design:
/// - One peer ACKs quickly, one fails, and one is slower than the round timeout.
///
/// Inputs:
/// - Scripted per-peer Commit delays and responses.
///
/// Outputs:
/// - `ProposeOnceError::NoQuorum` result.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_round_returns_no_quorum_when_acks_are_insufficient() {
    let ballot = Ballot::initial(7);
    let transport = NoopTransport::with_plans(
        HashMap::new(),
        HashMap::new(),
        HashMap::from([
            (
                1,
                CommitPeerPlan {
                    delay: StdDuration::from_millis(5),
                    outcome: CommitPeerOutcome::Response { ok: true },
                },
            ),
            (
                2,
                CommitPeerPlan {
                    delay: StdDuration::from_millis(5),
                    outcome: CommitPeerOutcome::TransportError("injected commit failure"),
                },
            ),
            (
                3,
                CommitPeerPlan {
                    delay: StdDuration::from_millis(300),
                    outcome: CommitPeerOutcome::Response { ok: true },
                },
            ),
        ]),
    );
    let group = Group::new(
        round_test_config(
            99,
            vec![1, 2, 3],
            StdDuration::from_millis(80),
            StdDuration::from_secs(1),
        ),
        Arc::new(transport),
        Arc::new(TestStateMachine),
        None,
    );

    let command = Bytes::from_static(b"commit-noquorum-test");
    let res = group
        .run_commit_round(
            TxnId {
                node_id: 7,
                counter: 4,
            },
            ballot,
            command.clone(),
            command_digest(&command),
            1,
            Vec::new(),
            true,
        )
        .await;

    match res {
        Err(ProposeOnceError::NoQuorum(_)) => {}
        other => panic!("expected no-quorum error, got {other:?}"),
    }
}

/// Verify 1RTT fast-path writes do not wait for remote Commit quorum.
///
/// Purpose:
/// - Protect the client-visible optimization: after identical PreAccept
///   quorum and successful local publish, the proposal may return before
///   slow remote Commit RPCs finish.
///
/// Design:
/// - Script one fast PreAccept peer to close quorum.
/// - Script all Commit peers slower than the proposal RPC timeout; the old
///   client-blocking Commit path would fail or wait, while the 1RTT path
///   succeeds after local publish.
///
/// Inputs:
/// - Three-voter group with `fast_path_1rtt=true` and async commit durability.
///
/// Outputs:
/// - Applied proposal result and local committed/barrier-visible state.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fast_path_1rtt_write_returns_after_local_publish_without_remote_commit_quorum() {
    let ballot = Ballot::initial(1);
    let transport = NoopTransport::with_plans(
        HashMap::from([
            (
                2,
                PreAcceptPeerPlan {
                    delay: StdDuration::from_millis(5),
                    outcome: PreAcceptPeerOutcome::Response {
                        ok: true,
                        promised: ballot,
                        seq: 1,
                        deps: Vec::new(),
                    },
                },
            ),
            (
                3,
                PreAcceptPeerPlan {
                    delay: StdDuration::from_millis(500),
                    outcome: PreAcceptPeerOutcome::Response {
                        ok: true,
                        promised: ballot,
                        seq: 1,
                        deps: Vec::new(),
                    },
                },
            ),
        ]),
        HashMap::new(),
        HashMap::from([
            (
                2,
                CommitPeerPlan {
                    delay: StdDuration::from_millis(500),
                    outcome: CommitPeerOutcome::Response { ok: true },
                },
            ),
            (
                3,
                CommitPeerPlan {
                    delay: StdDuration::from_millis(500),
                    outcome: CommitPeerOutcome::Response { ok: true },
                },
            ),
        ]),
    );
    let mut cfg = round_test_config(
        1,
        vec![1, 2, 3],
        StdDuration::from_millis(80),
        StdDuration::from_secs(1),
    );
    cfg.fast_path_1rtt = true;
    let group = Arc::new(Group::new(
        cfg,
        Arc::new(transport),
        Arc::new(TestStateMachine),
        None,
    ));

    let start = time::Instant::now();
    let res = group
        .propose(Bytes::from_static(b"fast-path-1rtt-write"))
        .await
        .expect("fast-path proposal should succeed without remote commit quorum");
    let elapsed = start.elapsed();

    assert!(matches!(res, ProposalResult::Applied));
    assert!(
        elapsed < StdDuration::from_millis(200),
        "1RTT proposal should return before slow Commit RPCs finish (elapsed={elapsed:?})"
    );

    let key = b"unit-test-key".as_slice();
    let barriers = group.last_committed_for_key_slices(&[key]).await;
    let txn_id = barriers[0]
        .map(|item| item.0)
        .expect("fast-path write should be barrier-visible");
    {
        let state = group.state.lock().await;
        let rec = state.records.get(&txn_id).expect("local committed record");
        assert_eq!(rec.status, Status::Committed);
    }
}

/// Verify read barriers expose uncommitted fast-path candidates.
///
/// Purpose:
/// - Prevent linearizable reads from missing a write that may have already
///   been ACKed by another coordinator while Commit dissemination is still
///   in flight.
///
/// Design:
/// - Insert a local PreAccepted write and query the per-key barrier helper
///   with 1RTT mode enabled.
///
/// Inputs:
/// - One PreAccept request for the test write key.
///
/// Outputs:
/// - Barrier target includes the PreAccepted transaction.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fast_path_barrier_reports_preaccepted_write() {
    let mut cfg = round_test_config(
        1,
        vec![1, 2, 3],
        StdDuration::from_millis(80),
        StdDuration::from_secs(1),
    );
    cfg.fast_path_1rtt = true;
    let group = Group::new(
        cfg,
        Arc::new(NoopTransport::new()),
        Arc::new(TestStateMachine),
        None,
    );
    let txn_id = TxnId {
        node_id: 2,
        counter: (1u64 << TXN_COUNTER_SHARD_SHIFT) | 7,
    };
    let resp = group
        .rpc_pre_accept(PreAcceptRequest {
            group_id: 1,
            txn_id,
            ballot: Ballot::initial(2),
            command: Bytes::from_static(b"preaccepted-write"),
            seq: 0,
            deps: Vec::new(),
        })
        .await;
    assert!(resp.ok, "preaccept should succeed");

    let key = b"unit-test-key".as_slice();
    let barriers = group.last_committed_for_key_slices(&[key]).await;
    assert_eq!(barriers[0].map(|item| item.0), Some(txn_id));
    assert_eq!(barriers[0].map(|item| item.1), Some(resp.seq));
}

/// Verify recovery can force a fast-path value from quorum intersection.
///
/// Purpose:
/// - Capture the core 1RTT recovery rule: if a recovery quorum intersects
///   the ACKing fast quorum at one PreAccepted replica, the command bytes
///   and metadata must be enough to continue recovery.
///
/// Design:
/// - Merge one PreAccepted reply carrying the command with one Unknown reply.
///
/// Inputs:
/// - Quorum replies representing the surviving intersection case.
///
/// Outputs:
/// - Recovery choice keeps the PreAccepted command instead of choosing NOOP.
#[test]
fn choose_recovery_value_uses_preaccepted_fast_path_command() {
    let cmd = Bytes::from_static(b"acked-fast-path-command");
    let reply = RecoverResponse {
        ok: true,
        promised: Ballot::zero(),
        status: TxnStatus::PreAccepted,
        accepted_ballot: Some(Ballot::initial(1)),
        command: cmd.clone(),
        command_digest: Some(command_digest(&cmd)),
        has_command: true,
        seq: 11,
        deps: Vec::new(),
    };
    let unknown = RecoverResponse {
        ok: true,
        promised: Ballot::zero(),
        status: TxnStatus::Unknown,
        accepted_ballot: None,
        command: Bytes::new(),
        command_digest: None,
        has_command: false,
        seq: 0,
        deps: Vec::new(),
    };

    let choice =
        choose_recovery_value(&[reply, unknown]).expect("preaccepted recovery should merge");
    match choice {
        RecoveryChoice::Ready(value) => {
            assert_eq!(value.command, cmd);
            assert_eq!(value.seq, 11);
        }
        other => panic!("expected ready recovery value, got {other:?}"),
    }
}

/// Exhaustively check quorum intersections used by 1RTT recovery.
///
/// Purpose:
/// - Provide a small model checker for the fast-path proof obligation:
///   every recovery/read quorum must intersect the ACKing fast quorum, even
///   after any tolerated crash-stop failures.
///
/// Design:
/// - Enumerates majority quorums for 3, 5, and 7 voters.
/// - Enumerates every failure set up to `f=(n-1)/2` and every live recovery
///   quorum that remains.
///
/// Inputs:
/// - Synthetic node ids `0..n`.
///
/// Outputs:
/// - Assertion failure if any recovery quorum can miss the fast quorum.
#[test]
fn fast_path_quorum_model_checks_recovery_and_read_intersections() {
    for n in [3usize, 5, 7] {
        let quorum = (n / 2) + 1;
        let faults = (n - 1) / 2;
        let nodes = (0..n).collect::<Vec<_>>();
        let quorums = subsets_of_size(&nodes, quorum);

        for fast_quorum in &quorums {
            for recovery_quorum in &quorums {
                assert!(
                    !fast_quorum.is_disjoint(recovery_quorum),
                    "recovery quorum missed fast quorum for n={n}"
                );
            }

            for failure_count in 0..=faults {
                for failed in subsets_of_size(&nodes, failure_count) {
                    let live = nodes
                        .iter()
                        .copied()
                        .filter(|node| !failed.contains(node))
                        .collect::<Vec<_>>();
                    if live.len() < quorum {
                        continue;
                    }
                    for recovery_quorum in subsets_of_size(&live, quorum) {
                        assert!(
                            !fast_quorum.is_disjoint(&recovery_quorum),
                            "live recovery quorum missed fast quorum for n={n}, failed={failed:?}"
                        );
                    }
                }
            }
        }
    }
}

/// Model-check read/recovery quorums after coordinator crash.
///
/// Purpose:
/// - Make the 1RTT crash proof explicit: once a coordinator ACKs after a
///   PreAccept quorum, later read/recovery quorums must still find at least
///   one surviving PreAccepted record even if async Commit was lost.
///
/// Design:
/// - Enumerates every fast quorum for 3/5/7 voters.
/// - Forces the coordinator to be in the failed set and then enumerates all
///   tolerated crash sets and live read/recovery quorums.
///
/// Inputs:
/// - Synthetic node ids `0..n`.
///
/// Outputs:
/// - Assertion failure if a live quorum can miss the ACKing fast quorum.
#[test]
fn fast_path_quorum_model_survives_coordinator_crash_lost_async_commit() {
    for n in [3usize, 5, 7] {
        let quorum = (n / 2) + 1;
        let faults = (n - 1) / 2;
        let nodes = (0..n).collect::<Vec<_>>();
        let quorums = subsets_of_size(&nodes, quorum);

        for fast_quorum in &quorums {
            let coordinator = *fast_quorum.iter().next().expect("non-empty fast quorum");
            for failure_count in 1..=faults {
                for failed in subsets_of_size(&nodes, failure_count) {
                    if !failed.contains(&coordinator) {
                        continue;
                    }
                    let live = nodes
                        .iter()
                        .copied()
                        .filter(|node| !failed.contains(node))
                        .collect::<Vec<_>>();
                    if live.len() < quorum {
                        continue;
                    }

                    // Every live read/recovery quorum must retain a fast
                    // quorum member. It intentionally does not require the
                    // crashed coordinator because 1RTT safety must survive
                    // losing coordinator-local volatile state.
                    for live_quorum in subsets_of_size(&live, quorum) {
                        let surviving_intersection =
                            live_quorum.iter().any(|node| fast_quorum.contains(node));
                        assert!(
                                surviving_intersection,
                                "live quorum missed fast quorum after coordinator crash for n={n}, failed={failed:?}"
                            );
                    }
                }
            }
        }
    }
}

/// Model-check conflicting fast-path quorum responses.
///
/// Purpose:
/// - Cover the conflicting-write case for the 1RTT fast path: two writes on
///   the same key cannot both fast-commit with incompatible dependency
///   metadata because their fast quorums intersect at a serialized replica.
///
/// Design:
/// - Simulates write A reaching its fast quorum first.
/// - Simulates write B reaching another fast quorum where intersection
///   replicas report A as a dependency and non-intersection replicas do not.
/// - Asserts B can be fast only when every B response contains the same dep
///   set, which means B depends on A at all responders.
///
/// Inputs:
/// - Synthetic majority quorums for 3, 5, and 7 voters.
///
/// Outputs:
/// - Assertion failure if two conflicting writes can both fast-commit
///   without a dependency edge.
#[test]
fn fast_path_conflict_model_requires_intersection_dependency() {
    const A: usize = 10_001;
    for n in [3usize, 5, 7] {
        let quorum = (n / 2) + 1;
        let nodes = (0..n).collect::<Vec<_>>();
        let quorums = subsets_of_size(&nodes, quorum);

        for a_quorum in &quorums {
            for b_quorum in &quorums {
                let mut b_response_deps = Vec::new();
                for node in b_quorum {
                    let saw_a = a_quorum.contains(node);
                    let deps = if saw_a {
                        StdBTreeSet::from([A])
                    } else {
                        StdBTreeSet::new()
                    };
                    b_response_deps.push(deps);
                }

                let b_fast = b_response_deps
                    .iter()
                    .all(|deps| deps == &b_response_deps[0]);
                if b_fast {
                    assert!(
                        b_response_deps[0].contains(&A),
                        "conflicting B fast-pathed without depending on A for n={n}"
                    );
                }
            }
        }
    }
}

/// Verify recovery rejects conflicting fast-path command bytes.
///
/// Purpose:
/// - Cover the failure path where a recovery quorum reports incompatible
///   PreAccepted payloads for the same transaction id.
///
/// Design:
/// - Builds two PreAccepted recover replies with distinct command bytes and
///   matching ballots.
/// - `choose_recovery_value` must fail instead of choosing either value.
///
/// Inputs:
/// - Two conflicting recover replies.
///
/// Outputs:
/// - Error containing `conflicting command bytes`.
#[test]
fn choose_recovery_value_rejects_conflicting_preaccepted_commands() {
    let a = Bytes::from_static(b"fast-path-a");
    let b = Bytes::from_static(b"fast-path-b");
    let reply_a = RecoverResponse {
        ok: true,
        promised: Ballot::zero(),
        status: TxnStatus::PreAccepted,
        accepted_ballot: Some(Ballot::initial(1)),
        command: a.clone(),
        command_digest: Some(command_digest(&a)),
        has_command: true,
        seq: 9,
        deps: Vec::new(),
    };
    let reply_b = RecoverResponse {
        ok: true,
        promised: Ballot::zero(),
        status: TxnStatus::PreAccepted,
        accepted_ballot: Some(Ballot::initial(1)),
        command: b.clone(),
        command_digest: Some(command_digest(&b)),
        has_command: true,
        seq: 9,
        deps: Vec::new(),
    };

    let err = choose_recovery_value(&[reply_a, reply_b])
        .expect_err("conflicting preaccepted commands should fail");
    assert!(
        err.to_string().contains("conflicting command bytes"),
        "unexpected error: {err:?}"
    );
}

/// Verify barriers ignore PreAccepted writes when 1RTT is disabled.
///
/// Purpose:
/// - Preserve legacy read-barrier semantics for groups that do not use 1RTT
///   fast-path ACKs.
///
/// Design:
/// - PreAccepts one write locally with `fast_path_1rtt=false`.
/// - Queries barrier state and expects no target until Commit occurs.
///
/// Inputs:
/// - One local PreAccept request for the unit-test write key.
///
/// Outputs:
/// - Empty barrier target for that key.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fast_path_barrier_ignores_preaccepted_write_when_1rtt_disabled() {
    let cfg = round_test_config(
        1,
        vec![1, 2, 3],
        StdDuration::from_millis(80),
        StdDuration::from_secs(1),
    );
    let group = Group::new(
        cfg,
        Arc::new(NoopTransport::new()),
        Arc::new(TestStateMachine),
        None,
    );
    let txn_id = TxnId {
        node_id: 2,
        counter: (1u64 << TXN_COUNTER_SHARD_SHIFT) | 8,
    };
    let resp = group
        .rpc_pre_accept(PreAcceptRequest {
            group_id: 1,
            txn_id,
            ballot: Ballot::initial(2),
            command: Bytes::from_static(b"preaccepted-write-disabled"),
            seq: 0,
            deps: Vec::new(),
        })
        .await;
    assert!(resp.ok, "preaccept should succeed");

    let key = b"unit-test-key".as_slice();
    let barriers = group.last_committed_for_key_slices(&[key]).await;
    assert_eq!(barriers[0], None);
}

/// Verify barriers choose the highest-sequence fast-path candidate.
///
/// Purpose:
/// - Ensure quorum read barriers wait on the strongest visible dependency
///   when multiple PreAccepted/Accepted writes exist for the same key.
///
/// Design:
/// - PreAccepts two writes on the same synthetic key with 1RTT enabled.
/// - The second write observes the first as a dependency and receives a
///   higher sequence; the barrier must report the second transaction.
///
/// Inputs:
/// - Two local PreAccept requests for the same key.
///
/// Outputs:
/// - Barrier target points at the higher-sequence transaction.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fast_path_barrier_reports_highest_sequence_candidate() {
    let mut cfg = round_test_config(
        1,
        vec![1, 2, 3],
        StdDuration::from_millis(80),
        StdDuration::from_secs(1),
    );
    cfg.fast_path_1rtt = true;
    let group = Group::new(
        cfg,
        Arc::new(NoopTransport::new()),
        Arc::new(TestStateMachine),
        None,
    );
    let first = TxnId {
        node_id: 2,
        counter: (1u64 << TXN_COUNTER_SHARD_SHIFT) | 9,
    };
    let second = TxnId {
        node_id: 3,
        counter: (1u64 << TXN_COUNTER_SHARD_SHIFT) | 10,
    };

    let first_resp = group
        .rpc_pre_accept(PreAcceptRequest {
            group_id: 1,
            txn_id: first,
            ballot: Ballot::initial(2),
            command: Bytes::from_static(b"preaccepted-write-first"),
            seq: 0,
            deps: Vec::new(),
        })
        .await;
    assert!(first_resp.ok, "first preaccept should succeed");

    let second_resp = group
        .rpc_pre_accept(PreAcceptRequest {
            group_id: 1,
            txn_id: second,
            ballot: Ballot::initial(3),
            command: Bytes::from_static(b"preaccepted-write-second"),
            seq: 0,
            deps: Vec::new(),
        })
        .await;
    assert!(second_resp.ok, "second preaccept should succeed");
    assert!(
        second_resp.seq > first_resp.seq,
        "second conflicting write should receive higher sequence"
    );

    let key = b"unit-test-key".as_slice();
    let barriers = group.last_committed_for_key_slices(&[key]).await;
    assert_eq!(barriers[0].map(|item| item.0), Some(second));
    assert_eq!(barriers[0].map(|item| item.1), Some(second_resp.seq));
}

/// Verify recovery merge accepts explicitly-encoded committed NOOP replies.
///
/// Purpose:
/// - Prevent false "missing command" failures when committed value is the
///   empty NOOP command.
///
/// Design:
/// - Build one committed reply with `has_command=true` and empty command.
/// - Assert merged choice is a ready NOOP value.
///
/// Inputs:
/// - One committed recover reply with empty command payload.
///
/// Outputs:
/// - `RecoveryChoice::Ready` containing empty command bytes.
#[test]
fn choose_recovery_value_accepts_explicit_committed_noop() {
    let reply = RecoverResponse {
        ok: true,
        promised: Ballot::zero(),
        status: TxnStatus::Committed,
        accepted_ballot: None,
        command: Bytes::new(),
        command_digest: Some(command_digest(&[])),
        has_command: true,
        seq: 7,
        deps: Vec::new(),
    };

    let choice = choose_recovery_value(&[reply]).expect("noop recovery merge should succeed");
    match choice {
        RecoveryChoice::Ready(value) => {
            assert!(value.command.is_empty(), "expected merged NOOP command");
            assert_eq!(value.seq, 7);
        }
        other => panic!("expected ready recovery value, got {other:?}"),
    }
}

/// Verify committed recovery values dominate stale lower-status preaccepts.
///
/// Purpose:
/// - Prevent recovery from getting pinned forever when a previous recovery
///   already committed a value but a lagging replica still reports a
///   different PreAccepted command for the same txn id.
///
/// Design:
/// - Mix one committed NOOP reply with one stale PreAccepted write reply.
/// - Assert the committed value wins and the stale digest is ignored.
///
/// Inputs:
/// - One committed reply and one conflicting preaccepted reply.
///
/// Outputs:
/// - `RecoveryChoice::Ready` containing the committed NOOP.
#[test]
fn choose_recovery_value_committed_dominates_stale_preaccepted_digest() {
    let stale = Bytes::from_static(b"stale-preaccepted-command");
    let committed = RecoverResponse {
        ok: true,
        promised: Ballot::zero(),
        status: TxnStatus::Committed,
        accepted_ballot: Some(Ballot {
            counter: 1,
            node_id: 1,
        }),
        command: Bytes::new(),
        command_digest: Some(command_digest(&[])),
        has_command: true,
        seq: 4,
        deps: Vec::new(),
    };
    let preaccepted = RecoverResponse {
        ok: true,
        promised: Ballot::zero(),
        status: TxnStatus::PreAccepted,
        accepted_ballot: Some(Ballot::initial(1)),
        command: stale.clone(),
        command_digest: Some(command_digest(&stale)),
        has_command: true,
        seq: 9,
        deps: Vec::new(),
    };

    let choice = choose_recovery_value(&[committed, preaccepted])
        .expect("committed value should dominate stale preaccept");
    match choice {
        RecoveryChoice::Ready(value) => {
            assert!(value.command.is_empty(), "expected committed NOOP");
            assert_eq!(value.seq, 4);
        }
        other => panic!("expected ready recovery value, got {other:?}"),
    }
}

/// Verify recovery merge asks caller to fetch bytes when digest is known
/// but no committed reply includes command bytes.
///
/// Purpose:
/// - Cover the recovery edge path that previously produced repeated
///   committed-missing-command stalls.
///
/// Design:
/// - Build one committed reply with digest metadata and `has_command=false`.
/// - Assert merge output requests command fetch for that digest.
///
/// Inputs:
/// - One committed recover reply with missing command bytes.
///
/// Outputs:
/// - `RecoveryChoice::MissingCommittedCommand`.
#[test]
fn choose_recovery_value_flags_missing_committed_command() {
    let cmd = Bytes::from_static(b"recover-me");
    let digest = command_digest(&cmd);
    let reply = RecoverResponse {
        ok: true,
        promised: Ballot::zero(),
        status: TxnStatus::Committed,
        accepted_ballot: None,
        command: Bytes::new(),
        command_digest: Some(digest),
        has_command: false,
        seq: 3,
        deps: Vec::new(),
    };

    let choice =
        choose_recovery_value(&[reply]).expect("missing-command recovery merge should succeed");
    match choice {
        RecoveryChoice::MissingCommittedCommand { digest: got } => {
            assert_eq!(got, digest);
        }
        other => panic!("expected missing-command recovery choice, got {other:?}"),
    }
}

/// Verify recovery merge rejects conflicting committed digests.
///
/// Purpose:
/// - Preserve safety by rejecting incompatible committed values.
///
/// Design:
/// - Feed two committed replies with different digest metadata.
/// - Assert merge returns an error.
///
/// Inputs:
/// - Two committed recover replies with distinct digests.
///
/// Outputs:
/// - Error from `choose_recovery_value`.
#[test]
fn choose_recovery_value_rejects_conflicting_committed_digests() {
    let a = RecoverResponse {
        ok: true,
        promised: Ballot::zero(),
        status: TxnStatus::Committed,
        accepted_ballot: None,
        command: Bytes::new(),
        command_digest: Some(command_digest(b"a")),
        has_command: false,
        seq: 1,
        deps: Vec::new(),
    };
    let b = RecoverResponse {
        ok: true,
        promised: Ballot::zero(),
        status: TxnStatus::Committed,
        accepted_ballot: None,
        command: Bytes::new(),
        command_digest: Some(command_digest(b"b")),
        has_command: false,
        seq: 1,
        deps: Vec::new(),
    };

    let err = choose_recovery_value(&[a, b]).expect_err("conflicting digests should fail");
    assert!(
        err.to_string().contains("conflicting command digests"),
        "unexpected error: {err}"
    );
}
