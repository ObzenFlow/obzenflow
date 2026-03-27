// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::system_event::{EdgeLivenessState, StageActivity, SystemEvent};
use obzenflow_core::event::types::SeqNo;
use obzenflow_core::event::{EventId, SystemEventType, WriterId};
use obzenflow_core::journal::Journal;
use obzenflow_core::StageId;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::Instant;

const HANDLER_ACTIVITY_POLLING: u8 = 0;
const HANDLER_ACTIVITY_PROCESSING: u8 = 1;

const STAGE_MODE_RUNNING: u8 = 0;
const STAGE_MODE_DRAINING: u8 = 1;

#[derive(Clone)]
pub struct HeartbeatConfig {
    /// How often the heartbeat task ticks (evaluates state and updates in-memory liveness).
    pub interval: Duration,

    /// Warn threshold for a blocked handler.
    pub handler_warn_threshold: Duration,

    /// Stall threshold for a blocked handler.
    pub handler_stall_threshold: Duration,

    /// How long an edge can be quiet before it is classified as Idle.
    pub idle_threshold: Duration,

    /// Whether the heartbeat task is enabled for this stage.
    pub enabled: bool,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(2),
            handler_warn_threshold: Duration::from_secs(30),
            handler_stall_threshold: Duration::from_secs(120),
            idle_threshold: Duration::from_secs(6),
            enabled: true,
        }
    }
}

#[derive(Debug)]
struct EdgeProgress {
    upstream: StageId,
    reader_seq: AtomicU64,
    last_read_offset_ms: AtomicU64,
    last_event_id: Mutex<Option<EventId>>,
}

impl EdgeProgress {
    fn new(upstream: StageId) -> Self {
        Self {
            upstream,
            reader_seq: AtomicU64::new(0),
            last_read_offset_ms: AtomicU64::new(0),
            last_event_id: Mutex::new(None),
        }
    }
}

/// Shared state between a stage supervisor and its heartbeat task.
///
/// The supervisor writes. The heartbeat task reads.
pub struct HeartbeatState {
    created_at: Instant,
    handler_activity: AtomicU8,
    stage_mode: AtomicU8,
    completed: AtomicBool,
    handler_entered_at: Mutex<Option<Instant>>,
    processing_event_id: Mutex<Option<EventId>>,
    processing_upstream: Mutex<Option<StageId>>,
    last_consumed_event_id: Mutex<Option<EventId>>,
    last_output_event_id: Mutex<Option<EventId>>,
    edges: Vec<EdgeProgress>,
    edge_index: HashMap<StageId, usize>,
}

impl HeartbeatState {
    pub fn new(upstreams: Vec<StageId>) -> Arc<Self> {
        let mut edges = Vec::with_capacity(upstreams.len());
        let mut edge_index = HashMap::with_capacity(upstreams.len());
        for (idx, upstream) in upstreams.into_iter().enumerate() {
            edges.push(EdgeProgress::new(upstream));
            edge_index.insert(upstream, idx);
        }

        Arc::new(Self {
            created_at: Instant::now(),
            handler_activity: AtomicU8::new(HANDLER_ACTIVITY_POLLING),
            stage_mode: AtomicU8::new(STAGE_MODE_RUNNING),
            completed: AtomicBool::new(false),
            handler_entered_at: Mutex::new(None),
            processing_event_id: Mutex::new(None),
            processing_upstream: Mutex::new(None),
            last_consumed_event_id: Mutex::new(None),
            last_output_event_id: Mutex::new(None),
            edges,
            edge_index,
        })
    }

    pub fn mark_polling(&self) {
        self.handler_activity
            .store(HANDLER_ACTIVITY_POLLING, Ordering::Release);
        *self
            .handler_entered_at
            .lock()
            .expect("handler_entered_at lock") = None;
        *self
            .processing_event_id
            .lock()
            .expect("processing_event_id lock") = None;
        *self
            .processing_upstream
            .lock()
            .expect("processing_upstream lock") = None;
    }

    pub fn mark_processing(&self, upstream: Option<StageId>, event_id: EventId) {
        self.handler_activity
            .store(HANDLER_ACTIVITY_PROCESSING, Ordering::Release);
        *self
            .handler_entered_at
            .lock()
            .expect("handler_entered_at lock") = Some(Instant::now());
        *self
            .processing_event_id
            .lock()
            .expect("processing_event_id lock") = Some(event_id);
        *self
            .processing_upstream
            .lock()
            .expect("processing_upstream lock") = upstream;
    }

    pub fn mark_draining(&self) {
        self.stage_mode
            .store(STAGE_MODE_DRAINING, Ordering::Release);
    }

    pub fn mark_completed(&self) {
        self.completed.store(true, Ordering::Release);
    }

    pub fn record_data_read(&self, upstream: StageId, event_id: EventId) {
        let Some(index) = self.edge_index.get(&upstream).copied() else {
            return;
        };
        let edge = &self.edges[index];

        edge.reader_seq.fetch_add(1, Ordering::Relaxed);
        edge.last_read_offset_ms
            .store(self.now_offset_ms(), Ordering::Relaxed);
        *edge.last_event_id.lock().expect("last_event_id lock") = Some(event_id);
    }

    pub fn record_last_consumed(&self, event_id: EventId) {
        *self
            .last_consumed_event_id
            .lock()
            .expect("last_consumed_event_id lock") = Some(event_id);
    }

    pub fn record_last_output(&self, event_id: EventId) {
        *self
            .last_output_event_id
            .lock()
            .expect("last_output_event_id lock") = Some(event_id);
    }

    fn now_offset_ms(&self) -> u64 {
        Instant::now()
            .duration_since(self.created_at)
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX)
    }

    fn handler_blocked_ms(&self) -> Option<u64> {
        if self.handler_activity.load(Ordering::Acquire) != HANDLER_ACTIVITY_PROCESSING {
            return None;
        }

        let entered_at = self
            .handler_entered_at
            .lock()
            .expect("handler_entered_at lock");
        let entered_at = (*entered_at)?;
        Some(
            Instant::now()
                .duration_since(entered_at)
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX),
        )
    }

    fn current_activity(&self) -> StageActivity {
        match self.handler_activity.load(Ordering::Acquire) {
            HANDLER_ACTIVITY_POLLING => {
                if self.completed.load(Ordering::Acquire) {
                    StageActivity::Completed
                } else if self.stage_mode.load(Ordering::Acquire) == STAGE_MODE_DRAINING {
                    StageActivity::Draining
                } else {
                    StageActivity::Polling
                }
            }
            HANDLER_ACTIVITY_PROCESSING => {
                let event_id = self
                    .processing_event_id
                    .lock()
                    .expect("processing_event_id lock")
                    .unwrap_or_default();
                let elapsed_ms = self.handler_blocked_ms().unwrap_or(0);
                StageActivity::Processing {
                    event_id,
                    elapsed_ms,
                }
            }
            _ => StageActivity::Polling,
        }
    }

    fn processing_upstream(&self) -> Option<StageId> {
        *self
            .processing_upstream
            .lock()
            .expect("processing_upstream lock")
    }

    fn edge_idle_ms(&self, index: usize) -> u64 {
        let last = self.edges[index]
            .last_read_offset_ms
            .load(Ordering::Relaxed);
        self.now_offset_ms().saturating_sub(last)
    }

    fn edge_reader_seq(&self, index: usize) -> u64 {
        self.edges[index].reader_seq.load(Ordering::Relaxed)
    }

    fn edge_last_event_id(&self, index: usize) -> Option<EventId> {
        *self.edges[index]
            .last_event_id
            .lock()
            .expect("last_event_id lock")
    }
}

pub struct HeartbeatProcessingGuard {
    state: Arc<HeartbeatState>,
}

impl HeartbeatProcessingGuard {
    pub fn new(state: Arc<HeartbeatState>, upstream: Option<StageId>, event_id: EventId) -> Self {
        state.mark_processing(upstream, event_id);
        Self { state }
    }
}

impl Drop for HeartbeatProcessingGuard {
    fn drop(&mut self) {
        self.state.mark_polling();
    }
}

#[derive(Clone, Debug)]
pub struct EdgeLivenessSnapshot {
    pub upstream: StageId,
    pub reader: StageId,
    pub state: EdgeLivenessState,
    pub idle_ms: u64,
    pub last_reader_seq: u64,
    pub last_event_id: Option<EventId>,
}

#[derive(Clone, Debug)]
pub struct StageLivenessSnapshot {
    pub stage_id: StageId,
    pub stage_name: String,
    pub heartbeat_seq: u64,
    pub activity: StageActivity,
    pub handler_blocked_ms: Option<u64>,
    pub edges: Vec<EdgeLivenessSnapshot>,
}

pub type LivenessRegistry = Arc<RwLock<HashMap<StageId, StageLivenessSnapshot>>>;

pub struct HeartbeatHandle {
    pub state: Arc<HeartbeatState>,
    cancel: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl HeartbeatHandle {
    pub fn cancel(&self) {
        let _ = self.cancel.send(true);
        self.task.abort();
    }
}

pub fn spawn_heartbeat(
    stage_id: StageId,
    stage_name: String,
    system_journal: Arc<dyn Journal<SystemEvent>>,
    registry: LivenessRegistry,
    state: Arc<HeartbeatState>,
    config: HeartbeatConfig,
    is_replay: bool,
) -> HeartbeatHandle {
    let (cancel, mut cancel_rx) = watch::channel(false);
    let writer_id = WriterId::from(stage_id);
    let state_for_task = state.clone();

    let task = tokio::spawn(async move {
        if is_replay {
            tracing::debug!(
                stage_name = %stage_name,
                "Heartbeat suppressed: replay mode"
            );
            return;
        }

        if !config.enabled {
            return;
        }

        let mut heartbeat_seq: u64 = 0;
        let mut prev_stable_states: Vec<EdgeLivenessState> =
            vec![EdgeLivenessState::Healthy; state_for_task.edges.len()];

        loop {
            tokio::select! {
                _ = tokio::time::sleep(config.interval) => {
                    heartbeat_seq = heartbeat_seq.saturating_add(1);

                    let activity = state_for_task.current_activity();
                    let handler_blocked_ms = state_for_task.handler_blocked_ms();
                    let processing_upstream = state_for_task.processing_upstream();

                    let stable_state_for_tick = match handler_blocked_ms {
                        None => None,
                        Some(ms) => {
                            let warn_ms = config.handler_warn_threshold.as_millis() as u64;
                            let stall_ms = config.handler_stall_threshold.as_millis() as u64;
                            if ms >= stall_ms {
                                Some(EdgeLivenessState::Stalled)
                            } else if ms >= warn_ms {
                                Some(EdgeLivenessState::Suspect)
                            } else {
                                None
                            }
                        }
                    };

                    // Update the in-memory registry on every tick.
                    let processing_below_warn =
                        handler_blocked_ms.is_some() && stable_state_for_tick.is_none();
                    let edges_snapshot: Vec<EdgeLivenessSnapshot> = state_for_task
                        .edges
                        .iter()
                        .enumerate()
                        .map(|(index, edge)| {
                            let idle_ms = state_for_task.edge_idle_ms(index);
                            let last_reader_seq = state_for_task.edge_reader_seq(index);
                            let last_event_id = state_for_task.edge_last_event_id(index);

                                let state_for_registry = if let Some(state) = stable_state_for_tick {
                                    state
                                } else if processing_below_warn
                                    && processing_upstream.is_some_and(|u| u == edge.upstream)
                                {
                                // Keep the active upstream edge healthy while an event is in-flight.
                                EdgeLivenessState::Healthy
                            } else if idle_ms >= config.idle_threshold.as_millis() as u64 {
                                EdgeLivenessState::Idle
                            } else {
                                EdgeLivenessState::Healthy
                            };

                            EdgeLivenessSnapshot {
                                upstream: edge.upstream,
                                reader: stage_id,
                                state: state_for_registry,
                                idle_ms,
                                last_reader_seq,
                                last_event_id,
                            }
                        })
                        .collect();

                    {
                        let mut guard = registry.write().expect("liveness registry lock");
                        guard.insert(
                            stage_id,
                            StageLivenessSnapshot {
                                stage_id,
                                stage_name: stage_name.clone(),
                                heartbeat_seq,
                                    activity,
                                    handler_blocked_ms,
                                    edges: edges_snapshot.clone(),
                                },
                            );
                        }

                    // Journal only EdgeLiveness transitions (FLOWIP-063e).
                        for (index, edge_snapshot) in edges_snapshot.iter().enumerate() {
                            let stable_state = edge_snapshot.state;

                        if prev_stable_states[index] == stable_state {
                            continue;
                        }

                            let emitted_state = if stable_state == EdgeLivenessState::Healthy
                                && prev_stable_states[index] != EdgeLivenessState::Healthy
                            {
                                EdgeLivenessState::Recovered
                            } else {
                                stable_state
                            };

                        let system_event = SystemEvent::new(
                            writer_id,
                            SystemEventType::EdgeLiveness {
                                upstream: edge_snapshot.upstream,
                                reader: stage_id,
                                state: emitted_state,
                                idle_ms: edge_snapshot.idle_ms,
                                last_reader_seq: Some(SeqNo(edge_snapshot.last_reader_seq)),
                                last_event_id: edge_snapshot.last_event_id,
                            },
                        );

                        let append_timeout = config.interval * 2;
                        match tokio::time::timeout(append_timeout, system_journal.append(system_event, None)).await {
                            Ok(Ok(_)) => {
                                prev_stable_states[index] = stable_state;
                            }
                            Ok(Err(e)) => {
                                tracing::warn!(
                                    stage_name = %stage_name,
                                    upstream = ?edge_snapshot.upstream,
                                    error = %e,
                                    "Failed to append EdgeLiveness; skipping"
                                );
                            }
                            Err(_) => {
                                tracing::warn!(
                                    stage_name = %stage_name,
                                    upstream = ?edge_snapshot.upstream,
                                    "EdgeLiveness journal append timed out; skipping"
                                );
                            }
                        }
                    }
                }
                changed = cancel_rx.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    if *cancel_rx.borrow() {
                        break;
                    }
                }
            }
        }
    });

    HeartbeatHandle {
        state,
        cancel,
        task,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use obzenflow_core::event::event_envelope::EventEnvelope;
    use obzenflow_core::event::identity::JournalWriterId;
    use obzenflow_core::event::journal_event::JournalEvent;
    use obzenflow_core::event::vector_clock::CausalOrderingService;
    use obzenflow_core::id::{JournalId, SystemId};
    use obzenflow_core::journal::journal_error::JournalError;
    use obzenflow_core::journal::journal_owner::JournalOwner;
    use obzenflow_core::journal::journal_reader::JournalReader;
    use obzenflow_core::journal::Journal;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct TestJournal<T: JournalEvent> {
        id: JournalId,
        owner: Option<JournalOwner>,
        seq: AtomicU64,
        events: Arc<Mutex<Vec<EventEnvelope<T>>>>,
    }

    impl<T: JournalEvent> TestJournal<T> {
        fn new(owner: JournalOwner) -> Self {
            Self {
                id: JournalId::new(),
                owner: Some(owner),
                seq: AtomicU64::new(0),
                events: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn next_seq(&self) -> u64 {
            self.seq.fetch_add(1, Ordering::Relaxed).saturating_add(1)
        }
    }

    struct TestJournalReader<T: JournalEvent> {
        events: Arc<Mutex<Vec<EventEnvelope<T>>>>,
        pos: usize,
    }

    #[async_trait]
    impl<T: JournalEvent + 'static> Journal<T> for TestJournal<T> {
        fn id(&self) -> &JournalId {
            &self.id
        }

        fn owner(&self) -> Option<&JournalOwner> {
            self.owner.as_ref()
        }

        async fn append(
            &self,
            event: T,
            parent: Option<&EventEnvelope<T>>,
        ) -> Result<EventEnvelope<T>, JournalError> {
            let mut env = EventEnvelope::new(JournalWriterId::from(self.id), event);

            if let Some(parent) = parent {
                CausalOrderingService::update_with_parent(
                    &mut env.vector_clock,
                    &parent.vector_clock,
                );
            }

            let writer_key = env.event.writer_id().to_string();
            let seq = self.next_seq();
            env.vector_clock.clocks.insert(writer_key, seq);

            let mut guard = self.events.lock().expect("journal events lock");
            guard.push(env.clone());
            Ok(env)
        }

        async fn read_causally_ordered(&self) -> Result<Vec<EventEnvelope<T>>, JournalError> {
            let guard = self.events.lock().expect("journal events lock");
            Ok(guard.clone())
        }

        async fn read_causally_after(
            &self,
            _after_event_id: &EventId,
        ) -> Result<Vec<EventEnvelope<T>>, JournalError> {
            Ok(Vec::new())
        }

        async fn read_event(
            &self,
            _event_id: &EventId,
        ) -> Result<Option<EventEnvelope<T>>, JournalError> {
            Ok(None)
        }

        async fn reader(&self) -> Result<Box<dyn JournalReader<T>>, JournalError> {
            Ok(Box::new(TestJournalReader {
                events: self.events.clone(),
                pos: 0,
            }))
        }

        async fn reader_from(
            &self,
            position: u64,
        ) -> Result<Box<dyn JournalReader<T>>, JournalError> {
            Ok(Box::new(TestJournalReader {
                events: self.events.clone(),
                pos: position as usize,
            }))
        }

        async fn read_last_n(&self, count: usize) -> Result<Vec<EventEnvelope<T>>, JournalError> {
            let guard = self.events.lock().expect("journal events lock");
            let len = guard.len();
            let start = len.saturating_sub(count);
            Ok(guard[start..].iter().rev().cloned().collect())
        }
    }

    #[async_trait]
    impl<T: JournalEvent + 'static> JournalReader<T> for TestJournalReader<T> {
        async fn next(&mut self) -> Result<Option<EventEnvelope<T>>, JournalError> {
            let guard = self.events.lock().expect("journal events lock");
            if self.pos >= guard.len() {
                Ok(None)
            } else {
                let env = guard.get(self.pos).cloned();
                self.pos += 1;
                Ok(env)
            }
        }

        async fn skip(&mut self, n: u64) -> Result<u64, JournalError> {
            let start = self.pos as u64;
            self.pos = (self.pos as u64 + n) as usize;
            Ok((self.pos as u64).saturating_sub(start))
        }

        fn position(&self) -> u64 {
            self.pos as u64
        }

        fn is_at_end(&self) -> bool {
            let guard = self.events.lock().expect("journal events lock");
            self.pos >= guard.len()
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn processing_edge_stays_healthy_while_inflight_below_warn_threshold() {
        tokio::time::pause();

        let upstream = StageId::new();
        let stage_id = StageId::new();

        let state = HeartbeatState::new(vec![upstream]);
        let registry: LivenessRegistry = Arc::new(RwLock::new(HashMap::new()));
        let system_journal: Arc<dyn Journal<SystemEvent>> =
            Arc::new(TestJournal::new(JournalOwner::system(SystemId::new())));

        let config = HeartbeatConfig {
            interval: Duration::from_secs(1),
            handler_warn_threshold: Duration::from_secs(30),
            handler_stall_threshold: Duration::from_secs(120),
            idle_threshold: Duration::from_secs(6),
            enabled: true,
        };

        let handle = spawn_heartbeat(
            stage_id,
            "test_stage".to_string(),
            system_journal,
            registry.clone(),
            state.clone(),
            config,
            /* is_replay */ false,
        );

        tokio::task::yield_now().await;

        let event_id = EventId::new();
        state.record_data_read(upstream, event_id);
        let _processing = HeartbeatProcessingGuard::new(state.clone(), Some(upstream), event_id);

        // Advance past idle_threshold but stay under warn threshold.
        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::task::yield_now().await;

        let mut snapshot = None;
        for _ in 0..3 {
            snapshot = registry
                .read()
                .expect("liveness registry lock")
                .get(&stage_id)
                .cloned();
            if snapshot.is_some() {
                break;
            }
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }
        let snapshot = snapshot.expect("stage liveness snapshot present");
        let edge = snapshot
            .edges
            .iter()
            .find(|e| e.upstream == upstream)
            .expect("edge snapshot present");

        assert_eq!(
            edge.state,
            EdgeLivenessState::Healthy,
            "active edge should remain Healthy while processing is in-flight"
        );

        handle.cancel();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stage_activity_reflects_draining_mode_when_not_processing() {
        tokio::time::pause();

        let upstream = StageId::new();
        let stage_id = StageId::new();

        let state = HeartbeatState::new(vec![upstream]);
        let registry: LivenessRegistry = Arc::new(RwLock::new(HashMap::new()));
        let system_journal: Arc<dyn Journal<SystemEvent>> =
            Arc::new(TestJournal::new(JournalOwner::system(SystemId::new())));

        let config = HeartbeatConfig {
            interval: Duration::from_secs(1),
            handler_warn_threshold: Duration::from_secs(30),
            handler_stall_threshold: Duration::from_secs(120),
            idle_threshold: Duration::from_secs(6),
            enabled: true,
        };

        let handle = spawn_heartbeat(
            stage_id,
            "test_stage".to_string(),
            system_journal,
            registry.clone(),
            state.clone(),
            config,
            /* is_replay */ false,
        );

        tokio::task::yield_now().await;

        state.mark_draining();
        let mut snapshot = None;
        for _ in 0..3 {
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
            snapshot = registry
                .read()
                .expect("liveness registry lock")
                .get(&stage_id)
                .cloned();
            if snapshot.is_some() {
                break;
            }
        }
        let snapshot = snapshot.expect("stage liveness snapshot present");
        assert!(
            matches!(snapshot.activity, StageActivity::Draining),
            "expected Draining when not processing in drain mode"
        );

        handle.cancel();
    }
}
