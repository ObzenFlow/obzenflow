// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Backpressure contracts
//!
//! Phase 1 implementation: in-process, per-edge, consumption-driven credits.
//! - Credits are replenished when downstream acks consumption of *data* events.
//! - Writers are gated by barrier semantics across all enabled downstream edges.
//! - Backpressure is opt-in via a `BackpressurePlan`.
//! - This module is data-only: stage supervisors must bypass gating/accounting for
//!   control/lifecycle/observability events by not calling `reserve`/`ack_consumed`.

use crate::id_conversions::StageIdExt;
use crate::stages::common::control_strategies::CreditWaker;
use obzenflow_core::StageId;
use obzenflow_topology::Topology;
use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackpressureEdgeMode {
    Disabled,
    /// State allocated and atomics maintained, but reserve() never blocks.
    TrackOnly,
    /// Full backpressure with a credit window and a stall ceiling.
    Enforced {
        window: NonZeroU64,
        stall_timeout: Duration,
    },
}

#[derive(Clone, Debug, Default)]
pub struct BackpressurePlan {
    stage_defaults: HashMap<StageId, (NonZeroU64, Duration)>,
    edge_overrides: HashMap<(StageId, StageId), BackpressureEdgeMode>,
}

impl BackpressurePlan {
    pub fn disabled() -> Self {
        Self::default()
    }

    pub fn with_stage_enforced(
        mut self,
        stage_id: StageId,
        window: NonZeroU64,
        stall_timeout: Duration,
    ) -> Self {
        self.stage_defaults.insert(stage_id, (window, stall_timeout));
        self
    }

    pub fn with_edge_enforced(
        mut self,
        upstream: StageId,
        downstream: StageId,
        window: NonZeroU64,
        stall_timeout: Duration,
    ) -> Self {
        self.edge_overrides.insert(
            (upstream, downstream),
            BackpressureEdgeMode::Enforced {
                window,
                stall_timeout,
            },
        );
        self
    }

    pub fn disable_edge(mut self, upstream: StageId, downstream: StageId) -> Self {
        self.edge_overrides
            .insert((upstream, downstream), BackpressureEdgeMode::Disabled);
        self
    }

    pub fn track_only_edge(mut self, upstream: StageId, downstream: StageId) -> Self {
        self.edge_overrides
            .insert((upstream, downstream), BackpressureEdgeMode::TrackOnly);
        self
    }

    fn mode_for_edge(&self, upstream: StageId, downstream: StageId) -> BackpressureEdgeMode {
        match self.edge_overrides.get(&(upstream, downstream)) {
            Some(mode) => *mode,
            None => self
                .stage_defaults
                .get(&upstream)
                .copied()
                .map(|(window, stall_timeout)| BackpressureEdgeMode::Enforced {
                    window,
                    stall_timeout,
                })
                .unwrap_or(BackpressureEdgeMode::Disabled),
        }
    }

    pub fn auto_enable_scc_internal_edges(&mut self, topology: &Topology) {
        for edge in topology.edges() {
            let upstream = StageId::from_topology_id(edge.from);
            let downstream = StageId::from_topology_id(edge.to);

            let Some(up_scc) = topology.scc_id(edge.from) else {
                continue;
            };
            if topology.scc_id(edge.to) != Some(up_scc) {
                continue;
            }

            if self.mode_for_edge(upstream, downstream) == BackpressureEdgeMode::Disabled {
                self.edge_overrides
                    .insert((upstream, downstream), BackpressureEdgeMode::TrackOnly);
            }
        }
    }
}

#[derive(Debug)]
struct EdgeState {
    upstream: StageId,
    downstream: StageId,
    window: u64,
    stall_timeout: Duration,
    reader_seq: AtomicU64,
    /// The upstream writer's waker; fired by `ack_consumed` after credit is
    /// published, so a blocked writer wakes on the ack rather than a timer.
    upstream_waker: CreditWaker,
}

#[derive(Debug)]
struct StageState {
    writer_seq: AtomicU64,
    reserved: AtomicU64,
    wait_nanos_total: AtomicU64,
    credit_waker: CreditWaker,
    downstream_edges: Vec<Arc<EdgeState>>,
}

#[derive(Clone, Debug)]
pub struct BackpressureRegistry {
    stages: HashMap<StageId, Arc<StageState>>,
    edges: HashMap<(StageId, StageId), Arc<EdgeState>>,
}

#[derive(Clone, Debug, Default)]
pub struct BackpressureMetricsSnapshot {
    pub edge_window: HashMap<(StageId, StageId), u64>,
    pub edge_in_flight: HashMap<(StageId, StageId), u64>,
    pub edge_credits: HashMap<(StageId, StageId), u64>,
    pub stage_blocked: HashMap<StageId, bool>,
    pub stage_min_reader_seq: HashMap<StageId, u64>,
    pub stage_writer_seq: HashMap<StageId, u64>,
    pub stage_wait_nanos_total: HashMap<StageId, u64>,
}

impl BackpressureRegistry {
    pub fn new(topology: &Topology, plan: &BackpressurePlan) -> Self {
        let stage_ids: Vec<StageId> = topology
            .stages()
            .map(|stage| StageId::from_topology_id(stage.id))
            .collect();

        // Wakers exist before the edge pass so every EdgeState can hold its
        // upstream writer's waker (edges are built before stage states).
        let wakers: HashMap<StageId, CreditWaker> = stage_ids
            .iter()
            .map(|id| (*id, CreditWaker::new()))
            .collect();

        let mut outgoing: HashMap<StageId, Vec<Arc<EdgeState>>> = HashMap::new();
        let mut edges: HashMap<(StageId, StageId), Arc<EdgeState>> = HashMap::new();

        for edge in topology.edges() {
            let upstream = StageId::from_topology_id(edge.from);
            let downstream = StageId::from_topology_id(edge.to);

            let (window, stall_timeout) = match plan.mode_for_edge(upstream, downstream) {
                BackpressureEdgeMode::Disabled => continue,
                // TrackOnly never blocks, so it can never be the limiting
                // edge of a stall episode; the ceiling here is unreachable.
                BackpressureEdgeMode::TrackOnly => (u64::MAX, Duration::MAX),
                BackpressureEdgeMode::Enforced {
                    window,
                    stall_timeout,
                } => (window.get(), stall_timeout),
            };

            let edge_state = Arc::new(EdgeState {
                upstream,
                downstream,
                window,
                stall_timeout,
                reader_seq: AtomicU64::new(0),
                upstream_waker: wakers
                    .get(&upstream)
                    .cloned()
                    .expect("edge upstream is a topology stage"),
            });

            edges.insert((upstream, downstream), edge_state.clone());
            outgoing.entry(upstream).or_default().push(edge_state);
        }

        let mut stages: HashMap<StageId, Arc<StageState>> = HashMap::new();
        for stage_id in stage_ids {
            let downstream_edges = outgoing.remove(&stage_id).unwrap_or_default();
            let credit_waker = wakers
                .get(&stage_id)
                .cloned()
                .expect("waker created for every stage");
            stages.insert(
                stage_id,
                Arc::new(StageState {
                    writer_seq: AtomicU64::new(0),
                    reserved: AtomicU64::new(0),
                    wait_nanos_total: AtomicU64::new(0),
                    credit_waker,
                    downstream_edges,
                }),
            );
        }

        Self { stages, edges }
    }

    pub fn metrics_snapshot(&self) -> BackpressureMetricsSnapshot {
        let mut snapshot = BackpressureMetricsSnapshot::default();

        for (stage_id, stage) in &self.stages {
            if stage.downstream_edges.is_empty() {
                continue;
            }

            let writer_seq = stage.writer_seq.load(Ordering::Acquire);
            let reserved = stage.reserved.load(Ordering::Acquire);
            let effective_writer = writer_seq.saturating_add(reserved);

            snapshot.stage_writer_seq.insert(*stage_id, writer_seq);
            snapshot
                .stage_wait_nanos_total
                .insert(*stage_id, stage.wait_nanos_total.load(Ordering::Relaxed));

            let mut min_reader_seq = u64::MAX;
            let mut min_credit = u64::MAX;

            for edge in &stage.downstream_edges {
                let reader_seq = edge.reader_seq.load(Ordering::Acquire);
                min_reader_seq = min_reader_seq.min(reader_seq);

                let allowed = reader_seq.saturating_add(edge.window);
                let credit = allowed.saturating_sub(effective_writer);
                min_credit = min_credit.min(credit);

                let edge_key = (edge.upstream, edge.downstream);
                snapshot.edge_window.insert(edge_key, edge.window);
                snapshot
                    .edge_in_flight
                    .insert(edge_key, effective_writer.saturating_sub(reader_seq));
                snapshot.edge_credits.insert(edge_key, credit);
            }

            snapshot
                .stage_min_reader_seq
                .insert(*stage_id, min_reader_seq);
            snapshot.stage_blocked.insert(*stage_id, min_credit == 0);
        }

        snapshot
    }

    pub fn writer(&self, stage_id: StageId) -> BackpressureWriter {
        BackpressureWriter {
            state: self.stages.get(&stage_id).cloned(),
        }
    }

    pub fn reader(&self, upstream: StageId, downstream: StageId) -> BackpressureReader {
        BackpressureReader {
            state: self.edges.get(&(upstream, downstream)).cloned(),
        }
    }

    /// Compute edge_in_flight for a single (upstream, downstream) pair.
    /// Returns None if the edge is not tracked.
    pub fn edge_in_flight(&self, upstream: StageId, downstream: StageId) -> Option<u64> {
        let stage = self.stages.get(&upstream)?;
        let edge = self.edges.get(&(upstream, downstream))?;

        let effective_writer = stage
            .writer_seq
            .load(Ordering::Acquire)
            .saturating_add(stage.reserved.load(Ordering::Acquire));
        let reader_seq = edge.reader_seq.load(Ordering::Acquire);
        Some(effective_writer.saturating_sub(reader_seq))
    }
}

#[derive(Clone, Debug)]
pub struct BackpressureWriter {
    state: Option<Arc<StageState>>,
}

impl BackpressureWriter {
    pub fn disabled() -> Self {
        Self { state: None }
    }

    pub fn is_enabled(&self) -> bool {
        self.state
            .as_ref()
            .is_some_and(|s| !s.downstream_edges.is_empty())
    }

    pub fn min_downstream_credit(&self) -> u64 {
        let Some(state) = &self.state else {
            return u64::MAX;
        };

        if state.downstream_edges.is_empty() {
            return u64::MAX;
        }

        let effective_writer = state
            .writer_seq
            .load(Ordering::Acquire)
            .saturating_add(state.reserved.load(Ordering::Acquire));

        state
            .downstream_edges
            .iter()
            .map(|edge| {
                let allowed = edge
                    .reader_seq
                    .load(Ordering::Acquire)
                    .saturating_add(edge.window);
                allowed.saturating_sub(effective_writer)
            })
            .min()
            .unwrap_or(u64::MAX)
    }

    /// The writer's credit waker, for the blocked-wait select. Present iff
    /// the writer has backpressure state.
    pub fn credit_waker(&self) -> Option<CreditWaker> {
        self.state.as_ref().map(|s| s.credit_waker.clone())
    }

    /// The limiting downstream edge of the current credit computation:
    /// minimum credit, ties broken by lowest downstream stage id so the
    /// stall record is deterministic.
    pub fn limiting_detail(&self) -> Option<LimitingEdgeDetail> {
        let state = self.state.as_ref()?;
        if state.downstream_edges.is_empty() {
            return None;
        }

        let effective_writer = state
            .writer_seq
            .load(Ordering::Acquire)
            .saturating_add(state.reserved.load(Ordering::Acquire));

        state
            .downstream_edges
            .iter()
            .map(|edge| {
                let reader_seq = edge.reader_seq.load(Ordering::Acquire);
                let allowed = reader_seq.saturating_add(edge.window);
                LimitingEdgeDetail {
                    credit: allowed.saturating_sub(effective_writer),
                    downstream: edge.downstream,
                    window: edge.window,
                    stall_timeout: edge.stall_timeout,
                    in_flight: effective_writer.saturating_sub(reader_seq),
                }
            })
            .min_by_key(|detail| (detail.credit, detail.downstream))
    }

    pub fn min_downstream_credit_detail(&self) -> Option<(u64, StageId)> {
        let state = self.state.as_ref()?;
        if state.downstream_edges.is_empty() {
            return None;
        }

        let effective_writer = state
            .writer_seq
            .load(Ordering::Acquire)
            .saturating_add(state.reserved.load(Ordering::Acquire));

        state
            .downstream_edges
            .iter()
            .map(|edge| {
                let allowed = edge
                    .reader_seq
                    .load(Ordering::Acquire)
                    .saturating_add(edge.window);
                let credit = allowed.saturating_sub(effective_writer);
                (credit, edge.downstream)
            })
            .min_by_key(|(credit, _)| *credit)
    }

    pub fn record_wait(&self, delay: Duration) {
        let Some(state) = &self.state else {
            return;
        };

        if state.downstream_edges.is_empty() {
            return;
        }

        let nanos = delay.as_nanos().min(u64::MAX as u128) as u64;
        state.wait_nanos_total.fetch_add(nanos, Ordering::Relaxed);
    }

    pub fn wait_nanos_total(&self) -> Option<u64> {
        let state = self.state.as_ref()?;
        if state.downstream_edges.is_empty() {
            return None;
        }
        Some(state.wait_nanos_total.load(Ordering::Relaxed))
    }

    pub fn is_bypass_enabled() -> bool {
        bypass_enabled()
    }

    /// Track-mode reservation: accounting advances, never blocks.
    /// Reconstruction-scoped commits use this so the resume handoff computes
    /// true lag from catch-up-era sequences (FLOWIP-115e).
    pub fn reserve_tracked(&self, n: u64) -> BackpressureReservation {
        let Some(state) = &self.state else {
            return BackpressureReservation::noop();
        };

        if state.downstream_edges.is_empty() {
            return BackpressureReservation::noop();
        }

        state.reserved.fetch_add(n, Ordering::AcqRel);
        BackpressureReservation {
            state: Some(state.clone()),
            reserved: n,
            committed_or_released: false,
        }
    }

    pub fn reserve(&self, n: u64) -> Option<BackpressureReservation> {
        let Some(state) = &self.state else {
            return Some(BackpressureReservation::noop());
        };

        if state.downstream_edges.is_empty() {
            return Some(BackpressureReservation::noop());
        }

        if bypass_enabled() {
            return Some(self.reserve_tracked(n));
        }

        loop {
            let writer_seq = state.writer_seq.load(Ordering::Acquire);
            let reserved = state.reserved.load(Ordering::Acquire);
            let effective_writer = writer_seq.saturating_add(reserved);

            let min_allowed = state
                .downstream_edges
                .iter()
                .map(|edge| {
                    edge.reader_seq
                        .load(Ordering::Acquire)
                        .saturating_add(edge.window)
                })
                .min()
                .unwrap_or(u64::MAX);

            if effective_writer.saturating_add(n) > min_allowed {
                return None;
            }

            if state
                .reserved
                .compare_exchange(reserved, reserved + n, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Some(BackpressureReservation {
                    state: Some(state.clone()),
                    reserved: n,
                    committed_or_released: false,
                });
            }
        }
    }
}

impl Default for BackpressureWriter {
    fn default() -> Self {
        Self::disabled()
    }
}

/// The limiting edge of a writer's credit computation (FLOWIP-115e): the
/// stall ceiling reads its timeout and the `backpressure.stalled` fact
/// carries its identity, window, and in-flight count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LimitingEdgeDetail {
    pub credit: u64,
    pub downstream: StageId,
    pub window: u64,
    pub stall_timeout: Duration,
    pub in_flight: u64,
}

#[derive(Debug)]
pub struct BackpressureReservation {
    state: Option<Arc<StageState>>,
    reserved: u64,
    committed_or_released: bool,
}

impl BackpressureReservation {
    /// A settled reservation for the no-backpressure cases.
    fn noop() -> Self {
        Self {
            state: None,
            reserved: 0,
            committed_or_released: true,
        }
    }

    pub fn commit(mut self, used: u64) {
        let Some(state) = &self.state else {
            self.committed_or_released = true;
            return;
        };

        let used = used.min(self.reserved);

        // Preserve the invariant that `writer_seq + reserved` is never
        // transiently lower than the true effective writer position.
        state.writer_seq.fetch_add(used, Ordering::AcqRel);
        state.reserved.fetch_sub(self.reserved, Ordering::AcqRel);

        self.committed_or_released = true;
    }

    pub fn release(mut self) {
        let Some(state) = &self.state else {
            self.committed_or_released = true;
            return;
        };

        state.reserved.fetch_sub(self.reserved, Ordering::AcqRel);
        self.committed_or_released = true;
    }
}

impl Drop for BackpressureReservation {
    fn drop(&mut self) {
        if self.committed_or_released {
            return;
        }
        let Some(state) = &self.state else {
            return;
        };
        state.reserved.fetch_sub(self.reserved, Ordering::AcqRel);
        self.committed_or_released = true;
    }
}

#[derive(Clone, Debug)]
pub struct BackpressureReader {
    state: Option<Arc<EdgeState>>,
}

impl BackpressureReader {
    pub fn disabled() -> Self {
        Self { state: None }
    }

    pub fn is_enabled(&self) -> bool {
        self.state.is_some()
    }

    pub fn ack_consumed(&self, n: u64) {
        let Some(state) = &self.state else {
            return;
        };
        // Publish credit first; the wake is a prompt to re-check, never a grant.
        state.reader_seq.fetch_add(n, Ordering::AcqRel);
        state.upstream_waker.notify();
    }

    pub fn edge_ids(&self) -> Option<(StageId, StageId)> {
        self.state.as_ref().map(|s| (s.upstream, s.downstream))
    }
}

impl Default for BackpressureReader {
    fn default() -> Self {
        Self::disabled()
    }
}

fn bypass_enabled() -> bool {
    static BYPASS: OnceLock<bool> = OnceLock::new();
    *BYPASS.get_or_init(|| std::env::var_os("OBZENFLOW_BACKPRESSURE_DISABLED").is_some())
}
