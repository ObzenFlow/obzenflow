// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::system_event::{EdgeLivenessState, StageActivity, SystemEvent};
use obzenflow_core::event::types::SeqNo;
use obzenflow_core::event::{EventId, SystemEventType, WriterId};
use obzenflow_core::journal::Journal;
use obzenflow_core::StageId;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::Instant;

const ACTIVITY_POLLING: u8 = 0;
const ACTIVITY_PROCESSING: u8 = 1;
const ACTIVITY_DRAINING: u8 = 2;
const ACTIVITY_COMPLETED: u8 = 3;

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
    activity: AtomicU8,
    handler_entered_at: Mutex<Option<Instant>>,
    processing_event_id: Mutex<Option<EventId>>,
    last_consumed_event_id: Mutex<Option<EventId>>,
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
            activity: AtomicU8::new(ACTIVITY_POLLING),
            handler_entered_at: Mutex::new(None),
            processing_event_id: Mutex::new(None),
            last_consumed_event_id: Mutex::new(None),
            edges,
            edge_index,
        })
    }

    pub fn mark_polling(&self) {
        self.activity.store(ACTIVITY_POLLING, Ordering::Release);
        *self
            .handler_entered_at
            .lock()
            .expect("handler_entered_at lock") = None;
        *self
            .processing_event_id
            .lock()
            .expect("processing_event_id lock") = None;
    }

    pub fn mark_processing(&self, event_id: EventId) {
        self.activity.store(ACTIVITY_PROCESSING, Ordering::Release);
        *self
            .handler_entered_at
            .lock()
            .expect("handler_entered_at lock") = Some(Instant::now());
        *self
            .processing_event_id
            .lock()
            .expect("processing_event_id lock") = Some(event_id);
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

    fn now_offset_ms(&self) -> u64 {
        Instant::now()
            .duration_since(self.created_at)
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX)
    }

    fn handler_blocked_ms(&self) -> Option<u64> {
        if self.activity.load(Ordering::Acquire) != ACTIVITY_PROCESSING {
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
        match self.activity.load(Ordering::Acquire) {
            ACTIVITY_POLLING => StageActivity::Polling,
            ACTIVITY_PROCESSING => {
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
            ACTIVITY_DRAINING => StageActivity::Draining,
            ACTIVITY_COMPLETED => StageActivity::Completed,
            _ => StageActivity::Polling,
        }
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
    pub fn new(state: Arc<HeartbeatState>, event_id: EventId) -> Self {
        state.mark_processing(event_id);
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
                    let edges_snapshot: Vec<EdgeLivenessSnapshot> = state_for_task
                        .edges
                        .iter()
                        .enumerate()
                        .map(|(index, edge)| {
                            let idle_ms = state_for_task.edge_idle_ms(index);
                            let last_reader_seq = state_for_task.edge_reader_seq(index);
                            let last_event_id = state_for_task.edge_last_event_id(index);

                            let state_for_registry = match stable_state_for_tick.clone() {
                                Some(state) => state,
                                None => {
                                    if idle_ms >= config.idle_threshold.as_millis() as u64 {
                                        EdgeLivenessState::Idle
                                    } else {
                                        EdgeLivenessState::Healthy
                                    }
                                }
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
                                activity: activity.clone(),
                                handler_blocked_ms,
                                edges: edges_snapshot.clone(),
                            },
                        );
                    }

                    // Journal only EdgeLiveness transitions (FLOWIP-063e).
                    for (index, edge_snapshot) in edges_snapshot.iter().enumerate() {
                        let stable_state = edge_snapshot.state.clone();

                        if prev_stable_states[index] == stable_state {
                            continue;
                        }

                        let emitted_state = if stable_state == EdgeLivenessState::Healthy
                            && prev_stable_states[index] != EdgeLivenessState::Healthy
                        {
                            EdgeLivenessState::Recovered
                        } else {
                            stable_state.clone()
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
