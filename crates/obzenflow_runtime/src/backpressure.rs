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
use futures::task::AtomicWaker;
use obzenflow_core::EventType;
use obzenflow_core::StageId;
use obzenflow_topology::Topology;
use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
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
        self.stage_defaults
            .insert(stage_id, (window, stall_timeout));
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
    /// Authoritative physical writer position used by admission and metrics.
    ///
    /// A reservation advances this position immediately. Committing keeps the
    /// used portion in place and releasing removes it. Keeping this separate
    /// from the diagnostic `writer_seq` and `reserved` counters prevents a
    /// snapshot from observing the commit handoff as either two rows or no
    /// row while those counters are updated independently.
    effective_writer: AtomicU64,
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
                    effective_writer: AtomicU64::new(0),
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
            let effective_writer = stage.effective_writer.load(Ordering::Acquire);

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
                let credit = allowed.saturating_sub(effective_writer).min(edge.window);
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
            direct_fact_admission: None,
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

        let effective_writer = stage.effective_writer.load(Ordering::Acquire);
        let reader_seq = edge.reader_seq.load(Ordering::Acquire);
        Some(effective_writer.saturating_sub(reader_seq))
    }
}

#[derive(Clone, Debug)]
pub struct BackpressureWriter {
    state: Option<Arc<StageState>>,
    direct_fact_admission: Option<DirectFactAdmission>,
}

impl BackpressureWriter {
    pub fn disabled() -> Self {
        Self {
            state: None,
            direct_fact_admission: None,
        }
    }

    pub(crate) fn with_direct_fact_admission(mut self, admission: DirectFactAdmission) -> Self {
        self.direct_fact_admission = Some(admission);
        self
    }

    pub(crate) fn direct_fact_admission(&self) -> Option<&DirectFactAdmission> {
        self.direct_fact_admission.as_ref()
    }

    pub fn is_enabled(&self) -> bool {
        self.state
            .as_ref()
            .is_some_and(|s| !s.downstream_edges.is_empty())
    }

    pub(crate) fn has_enforced_edges(&self) -> bool {
        self.state.as_ref().is_some_and(|state| {
            state
                .downstream_edges
                .iter()
                .any(|edge| edge.window != u64::MAX)
        })
    }

    pub(crate) fn has_tracked_edges(&self) -> bool {
        self.state.as_ref().is_some_and(|state| {
            state
                .downstream_edges
                .iter()
                .any(|edge| edge.window == u64::MAX)
        })
    }

    /// Validate a generated descriptor's whole-continuation row bound against
    /// every participating enforced physical edge.
    #[doc(hidden)]
    pub fn validate_generated_direct_bound(&self, bound: NonZeroU64) -> Result<(), String> {
        let Some(state) = &self.state else {
            return Ok(());
        };
        let enforced = state
            .downstream_edges
            .iter()
            .filter(|edge| edge.window != u64::MAX)
            .collect::<Vec<_>>();
        if enforced.is_empty() {
            return Ok(());
        }
        if Self::is_bypass_enabled() {
            return Err(format!(
                "generated role requires enforced direct admission for {} physical Data rows, \
                 but OBZENFLOW_BACKPRESSURE_DISABLED is set; unset it or configure the edge \
                 explicitly as track/off",
                bound
            ));
        }
        if let Some(edge) = enforced.iter().find(|edge| edge.window < bound.get()) {
            return Err(format!(
                "generated role requires {} live physical Data credits; edge to stage '{}' \
                 resolved enforced window {}; set runtime.backpressure.window to at least {}",
                bound, edge.downstream, edge.window, bound
            ));
        }
        Ok(())
    }

    pub fn min_downstream_credit(&self) -> u64 {
        let Some(state) = &self.state else {
            return u64::MAX;
        };

        if state.downstream_edges.is_empty() {
            return u64::MAX;
        }

        let effective_writer = state.effective_writer.load(Ordering::Acquire);

        state
            .downstream_edges
            .iter()
            .map(|edge| {
                let allowed = edge
                    .reader_seq
                    .load(Ordering::Acquire)
                    .saturating_add(edge.window);
                allowed.saturating_sub(effective_writer).min(edge.window)
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

        let effective_writer = state.effective_writer.load(Ordering::Acquire);

        state
            .downstream_edges
            .iter()
            .map(|edge| {
                let reader_seq = edge.reader_seq.load(Ordering::Acquire);
                let allowed = reader_seq.saturating_add(edge.window);
                LimitingEdgeDetail {
                    credit: allowed.saturating_sub(effective_writer).min(edge.window),
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

        let effective_writer = state.effective_writer.load(Ordering::Acquire);

        state
            .downstream_edges
            .iter()
            .map(|edge| {
                let allowed = edge
                    .reader_seq
                    .load(Ordering::Acquire)
                    .saturating_add(edge.window);
                let credit = allowed.saturating_sub(effective_writer).min(edge.window);
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

        state.effective_writer.fetch_add(n, Ordering::AcqRel);
        state.reserved.fetch_add(n, Ordering::AcqRel);
        BackpressureReservation {
            state: Some(state.clone()),
            remaining: n,
            committed_or_released: false,
        }
    }

    pub fn reserve(&self, n: u64) -> Option<BackpressureReservation> {
        if bypass_enabled() {
            return Some(self.reserve_tracked(n));
        }
        self.reserve_strict(n)
    }

    /// Enforced reservation that never honours the global debug bypass.
    ///
    /// Generated bounded continuations use this entry point after
    /// materialisation has selected enforce mode. A debug environment toggle
    /// must not silently turn that finite-window claim into track-only
    /// accounting.
    pub(crate) fn reserve_strict(&self, n: u64) -> Option<BackpressureReservation> {
        let Some(state) = &self.state else {
            return Some(BackpressureReservation::noop());
        };

        if state.downstream_edges.is_empty() {
            return Some(BackpressureReservation::noop());
        }

        loop {
            let effective_writer = state.effective_writer.load(Ordering::Acquire);

            let min_credit = state
                .downstream_edges
                .iter()
                .map(|edge| {
                    edge.reader_seq
                        .load(Ordering::Acquire)
                        .saturating_add(edge.window)
                        .saturating_sub(effective_writer)
                        .min(edge.window)
                })
                .min()
                .unwrap_or(u64::MAX);

            if n > min_credit {
                return None;
            }

            let next_effective_writer = effective_writer.checked_add(n)?;

            if state
                .effective_writer
                .compare_exchange(
                    effective_writer,
                    next_effective_writer,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                state.reserved.fetch_add(n, Ordering::AcqRel);
                return Some(BackpressureReservation {
                    state: Some(state.clone()),
                    remaining: n,
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
    remaining: u64,
    committed_or_released: bool,
}

impl BackpressureReservation {
    /// A settled reservation for the no-backpressure cases.
    fn noop() -> Self {
        Self {
            state: None,
            remaining: 0,
            committed_or_released: true,
        }
    }

    /// Settle a durable subset of an existing reservation without releasing
    /// the remaining capacity.
    pub(crate) fn commit_partial(&mut self, used: u64) -> Result<(), &'static str> {
        if self.committed_or_released || used > self.remaining {
            return Err("backpressure reservation over-commit");
        }
        let Some(state) = &self.state else {
            if used != 0 {
                return Err("no-op backpressure reservation cannot commit physical rows");
            }
            return Ok(());
        };

        state.writer_seq.fetch_add(used, Ordering::AcqRel);
        state.reserved.fetch_sub(used, Ordering::AcqRel);
        self.remaining -= used;
        Ok(())
    }

    pub fn commit(mut self, used: u64) {
        let Some(state) = &self.state else {
            self.committed_or_released = true;
            return;
        };

        let used = used.min(self.remaining);

        // `effective_writer` already includes the reservation. A commit keeps
        // its used rows admitted and removes only the unused remainder.
        state.writer_seq.fetch_add(used, Ordering::AcqRel);
        state.reserved.fetch_sub(self.remaining, Ordering::AcqRel);
        state
            .effective_writer
            .fetch_sub(self.remaining.saturating_sub(used), Ordering::AcqRel);
        self.remaining = 0;

        self.committed_or_released = true;
    }

    pub fn release(mut self) {
        let Some(state) = &self.state else {
            self.committed_or_released = true;
            return;
        };

        state.reserved.fetch_sub(self.remaining, Ordering::AcqRel);
        state
            .effective_writer
            .fetch_sub(self.remaining, Ordering::AcqRel);
        self.remaining = 0;
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
        state.reserved.fetch_sub(self.remaining, Ordering::AcqRel);
        state
            .effective_writer
            .fetch_sub(self.remaining, Ordering::AcqRel);
        self.remaining = 0;
        self.committed_or_released = true;
    }
}

/// Supervisor-owned affine capacity for one descriptor-bounded direct-fact
/// continuation.
///
/// The lease itself is not cloneable. Internal commit claims share its private
/// state so `OutputCommitter` can settle durable subsets while supervision
/// retains authority to release the unused tail.
#[derive(Debug)]
pub(crate) struct DirectFactLease {
    state: Arc<Mutex<DirectFactLeaseState>>,
}

#[derive(Debug)]
struct DirectFactLeaseState {
    reservation: Option<BackpressureReservation>,
    remaining: u64,
    in_flight: u64,
    committed: u64,
    closed: bool,
}

#[derive(Debug)]
pub(crate) struct DirectFactClaim {
    state: Arc<Mutex<DirectFactLeaseState>>,
    rows: u64,
    accounting: DirectFactClaimAccounting,
    settled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DirectFactClaimAccounting {
    LiveReservation,
    ReconstructionTrack,
}

#[derive(Clone, Debug)]
pub(crate) struct DirectFactAdmission {
    inner: Arc<DirectFactAdmissionInner>,
}

#[derive(Debug)]
struct DirectFactAdmissionInner {
    event_type: EventType,
    bound: NonZeroU64,
    slot: Mutex<DirectFactAdmissionSlot>,
    waker: AtomicWaker,
}

#[derive(Debug)]
enum DirectFactAdmissionSlot {
    ReconstructionTrack(DirectFactLease),
    Requested {
        reconstructed_committed: u64,
    },
    LiveLeased {
        lease: DirectFactLease,
        reconstructed_committed: u64,
    },
    Closed,
}

impl DirectFactLease {
    fn reconstruction_track() -> Self {
        Self {
            state: Arc::new(Mutex::new(DirectFactLeaseState {
                reservation: None,
                remaining: u64::MAX,
                in_flight: 0,
                committed: 0,
                closed: false,
            })),
        }
    }

    pub(crate) fn try_acquire(
        writer: &BackpressureWriter,
        bound: NonZeroU64,
    ) -> Result<Option<Self>, String> {
        let rows = bound.get();
        let reservation = if writer.has_enforced_edges() {
            if BackpressureWriter::is_bypass_enabled() {
                return Err(
                    "generated direct-fact admission requires enforce mode, but \
                     OBZENFLOW_BACKPRESSURE_DISABLED is set"
                        .to_string(),
                );
            }
            let Some(reservation) = writer.reserve_strict(rows) else {
                return Ok(None);
            };
            Some(reservation)
        } else if writer.has_tracked_edges() {
            Some(writer.reserve_tracked(rows))
        } else {
            None
        };
        Ok(Some(Self {
            state: Arc::new(Mutex::new(DirectFactLeaseState {
                reservation,
                remaining: rows,
                in_flight: 0,
                committed: 0,
                closed: false,
            })),
        }))
    }

    pub(crate) fn claim(&self, rows: u64) -> Result<DirectFactClaim, String> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.closed {
            return Err("direct-fact lease is closed".to_string());
        }
        if rows > state.remaining {
            return Err(format!(
                "direct-fact lease over-claim: requested {rows}, remaining {}",
                state.remaining
            ));
        }
        state.remaining -= rows;
        state.in_flight = state
            .in_flight
            .checked_add(rows)
            .ok_or_else(|| "direct-fact in-flight row overflow".to_string())?;
        drop(state);
        Ok(DirectFactClaim {
            state: self.state.clone(),
            rows,
            accounting: DirectFactClaimAccounting::LiveReservation,
            settled: false,
        })
    }

    pub(crate) fn close(self) -> Result<u64, String> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.closed {
            return Err("direct-fact lease closed more than once".to_string());
        }
        if state.in_flight != 0 {
            return Err(format!(
                "direct-fact lease closed with {} rows still in flight",
                state.in_flight
            ));
        }
        state.closed = true;
        state.remaining = 0;
        let committed = state.committed;
        let reservation = state.reservation.take();
        drop(state);
        drop(reservation);
        Ok(committed)
    }
}

impl DirectFactAdmission {
    pub(crate) fn new(event_type: EventType, bound: NonZeroU64) -> Self {
        Self {
            inner: Arc::new(DirectFactAdmissionInner {
                event_type,
                bound,
                slot: Mutex::new(DirectFactAdmissionSlot::ReconstructionTrack(
                    DirectFactLease::reconstruction_track(),
                )),
                waker: AtomicWaker::new(),
            }),
        }
    }

    pub(crate) fn event_type(&self) -> &EventType {
        &self.inner.event_type
    }

    pub(crate) fn bound(&self) -> NonZeroU64 {
        self.inner.bound
    }

    pub(crate) fn is_requested(&self) -> bool {
        matches!(
            &*self
                .inner
                .slot
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            DirectFactAdmissionSlot::Requested { .. }
        )
    }

    pub(crate) async fn request_live(&self) -> Result<(), String> {
        futures::future::poll_fn(|cx| {
            let mut slot = self
                .inner
                .slot
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match &*slot {
                DirectFactAdmissionSlot::LiveLeased { .. } => std::task::Poll::Ready(Ok(())),
                DirectFactAdmissionSlot::Closed => {
                    std::task::Poll::Ready(Err("direct-fact admission is closed".to_string()))
                }
                DirectFactAdmissionSlot::Requested { .. } => {
                    self.inner.waker.register(cx.waker());
                    std::task::Poll::Pending
                }
                DirectFactAdmissionSlot::ReconstructionTrack(_) => {
                    self.inner.waker.register(cx.waker());
                    let previous = std::mem::replace(&mut *slot, DirectFactAdmissionSlot::Closed);
                    let DirectFactAdmissionSlot::ReconstructionTrack(track) = previous else {
                        unreachable!("slot was matched above")
                    };
                    match track.close() {
                        Ok(reconstructed_committed) => {
                            *slot = DirectFactAdmissionSlot::Requested {
                                reconstructed_committed,
                            };
                            std::task::Poll::Pending
                        }
                        Err(error) => std::task::Poll::Ready(Err(error)),
                    }
                }
            }
        })
        .await
    }

    pub(crate) fn grant(&self, lease: DirectFactLease) -> Result<(), String> {
        let mut slot = self
            .inner
            .slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let previous = std::mem::replace(&mut *slot, DirectFactAdmissionSlot::Closed);
        let reconstructed_committed = match previous {
            DirectFactAdmissionSlot::ReconstructionTrack(track) => track.close()?,
            DirectFactAdmissionSlot::Requested {
                reconstructed_committed,
            } => reconstructed_committed,
            DirectFactAdmissionSlot::LiveLeased { .. } | DirectFactAdmissionSlot::Closed => {
                return Err("direct-fact admission grant is not single-use".to_string())
            }
        };
        *slot = DirectFactAdmissionSlot::LiveLeased {
            lease,
            reconstructed_committed,
        };
        drop(slot);
        self.inner.waker.wake();
        Ok(())
    }

    pub(crate) fn claim(&self, rows: u64) -> Result<Option<DirectFactClaim>, String> {
        let slot = self
            .inner
            .slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match &*slot {
            DirectFactAdmissionSlot::ReconstructionTrack(track) => {
                let mut claim = track.claim(rows)?;
                claim.accounting = DirectFactClaimAccounting::ReconstructionTrack;
                Ok(Some(claim))
            }
            DirectFactAdmissionSlot::Requested { .. } => Err(
                "direct-fact append attempted while live admission was still pending".to_string(),
            ),
            DirectFactAdmissionSlot::LiveLeased { lease, .. } => lease.claim(rows).map(Some),
            DirectFactAdmissionSlot::Closed => {
                Err("direct-fact append attempted after lease closure".to_string())
            }
        }
    }

    pub(crate) fn close(&self) -> Result<u64, String> {
        let mut slot = self
            .inner
            .slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let previous = std::mem::replace(&mut *slot, DirectFactAdmissionSlot::Closed);
        drop(slot);
        self.inner.waker.wake();
        match previous {
            DirectFactAdmissionSlot::LiveLeased {
                lease,
                reconstructed_committed,
            } => lease
                .close()?
                .checked_add(reconstructed_committed)
                .ok_or_else(|| "direct-fact committed-row overflow".to_string()),
            DirectFactAdmissionSlot::ReconstructionTrack(track) => track.close(),
            DirectFactAdmissionSlot::Requested {
                reconstructed_committed,
            } => Ok(reconstructed_committed),
            DirectFactAdmissionSlot::Closed => {
                Err("direct-fact admission closed more than once".to_string())
            }
        }
    }
}

impl DirectFactClaim {
    pub(crate) fn requires_track_accounting(&self) -> bool {
        matches!(
            self.accounting,
            DirectFactClaimAccounting::ReconstructionTrack
        )
    }

    pub(crate) fn commit(mut self) -> Result<(), String> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.closed || state.in_flight < self.rows {
            return Err("direct-fact claim settlement state is inconsistent".to_string());
        }
        if let Some(reservation) = state.reservation.as_mut() {
            reservation
                .commit_partial(self.rows)
                .map_err(str::to_string)?;
        }
        state.in_flight -= self.rows;
        state.committed = state
            .committed
            .checked_add(self.rows)
            .ok_or_else(|| "direct-fact committed-row overflow".to_string())?;
        self.settled = true;
        Ok(())
    }
}

impl Drop for DirectFactClaim {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.closed {
            return;
        }
        state.in_flight = state.in_flight.saturating_sub(self.rows);
        state.remaining = state.remaining.saturating_add(self.rows);
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

/// Complete physical `Data` rows consumed by a transport filter.
///
/// The map is keyed by upstream stage, which is also the identity of the one
/// physical stage-journal edge. Logical feed keys never enter this accounting,
/// so selecting several feeds from the same journal cannot multiply credit.
pub(crate) fn complete_filtered_data_rows(
    readers: &HashMap<StageId, BackpressureReader>,
    upstream: StageId,
    completed_data_rows: u64,
) {
    if completed_data_rows == 0 {
        return;
    }

    if let Some(reader) = readers.get(&upstream) {
        reader.ack_consumed(completed_data_rows);
    } else {
        tracing::warn!(
            ?upstream,
            completed_data_rows,
            "filtered Data row has no physical backpressure reader"
        );
    }
}

fn bypass_enabled() -> bool {
    static BYPASS: OnceLock<bool> = OnceLock::new();
    *BYPASS.get_or_init(|| std::env::var_os("OBZENFLOW_BACKPRESSURE_DISABLED").is_some())
}

#[cfg(test)]
mod direct_fact_tests {
    use super::*;

    fn enforced_writer(window: u64) -> (BackpressureWriter, BackpressureReader, Arc<StageState>) {
        let upstream = StageId::new();
        let downstream = StageId::new();
        let credit_waker = CreditWaker::new();
        let edge = Arc::new(EdgeState {
            upstream,
            downstream,
            window,
            stall_timeout: Duration::from_secs(1),
            reader_seq: AtomicU64::new(0),
            upstream_waker: credit_waker.clone(),
        });
        let state = Arc::new(StageState {
            effective_writer: AtomicU64::new(0),
            writer_seq: AtomicU64::new(0),
            reserved: AtomicU64::new(0),
            wait_nanos_total: AtomicU64::new(0),
            credit_waker,
            downstream_edges: vec![edge.clone()],
        });
        (
            BackpressureWriter {
                state: Some(state.clone()),
                direct_fact_admission: None,
            },
            BackpressureReader { state: Some(edge) },
            state,
        )
    }

    #[test]
    fn partial_claims_conserve_one_whole_continuation_reservation() {
        let (writer, _reader, state) = enforced_writer(3);
        let lease =
            DirectFactLease::try_acquire(&writer, NonZeroU64::new(3).expect("non-zero bound"))
                .expect("admission calculation succeeds")
                .expect("three credits are available");

        assert_eq!(state.effective_writer.load(Ordering::Acquire), 3);
        assert_eq!(state.reserved.load(Ordering::Acquire), 3);

        lease
            .claim(1)
            .expect("first row fits")
            .commit()
            .expect("first row settles");
        assert_eq!(state.writer_seq.load(Ordering::Acquire), 1);
        assert_eq!(state.reserved.load(Ordering::Acquire), 2);
        assert_eq!(state.effective_writer.load(Ordering::Acquire), 3);

        drop(lease.claim(2).expect("a dropped claim is reversible"));
        lease
            .claim(2)
            .expect("dropped rows return to the lease")
            .commit()
            .expect("remaining rows settle");
        assert!(lease.claim(1).is_err(), "the bound cannot be over-claimed");
        assert_eq!(lease.close().expect("lease closes"), 3);

        assert_eq!(state.writer_seq.load(Ordering::Acquire), 3);
        assert_eq!(state.reserved.load(Ordering::Acquire), 0);
        assert_eq!(state.effective_writer.load(Ordering::Acquire), 3);
    }

    #[test]
    fn closing_releases_the_unused_tail_without_refunding_committed_rows() {
        let (writer, reader, state) = enforced_writer(3);
        let lease =
            DirectFactLease::try_acquire(&writer, NonZeroU64::new(3).expect("non-zero bound"))
                .expect("admission calculation succeeds")
                .expect("three credits are available");
        lease
            .claim(1)
            .expect("one row fits")
            .commit()
            .expect("one row settles");

        assert_eq!(lease.close().expect("lease closes"), 1);
        assert_eq!(state.writer_seq.load(Ordering::Acquire), 1);
        assert_eq!(state.reserved.load(Ordering::Acquire), 0);
        assert_eq!(state.effective_writer.load(Ordering::Acquire), 1);
        assert!(
            DirectFactLease::try_acquire(&writer, NonZeroU64::new(3).expect("non-zero bound"))
                .expect("admission calculation succeeds")
                .is_none(),
            "the committed row still occupies one physical credit"
        );

        reader.ack_consumed(1);
        assert!(
            DirectFactLease::try_acquire(&writer, NonZeroU64::new(3).expect("non-zero bound"))
                .expect("admission calculation succeeds")
                .is_some(),
            "acknowledging the committed row restores the full window"
        );
    }

    #[test]
    fn reconstruction_claims_count_only_committed_physical_rows() {
        let admission = DirectFactAdmission::new(
            EventType::from("effect-record"),
            NonZeroU64::new(3).expect("non-zero bound"),
        );

        let committed = admission
            .claim(4)
            .expect("reconstruction claim succeeds")
            .expect("reconstruction uses a counter-only claim");
        assert!(
            committed.requires_track_accounting(),
            "reconstruction must retain physical track accounting"
        );
        committed.commit().expect("claim commits");

        let dropped = admission
            .claim(2)
            .expect("second reconstruction claim succeeds")
            .expect("reconstruction uses a counter-only claim");
        drop(dropped);

        assert_eq!(
            admission.close().expect("admission closes"),
            4,
            "only durable reconstruction rows enter completion accounting"
        );
    }

    #[tokio::test]
    async fn resume_carries_reconstruction_rows_into_live_lease_accounting() {
        let admission = DirectFactAdmission::new(
            EventType::from("effect-record"),
            NonZeroU64::new(3).expect("non-zero bound"),
        );
        admission
            .claim(2)
            .expect("reconstruction claim succeeds")
            .expect("reconstruction uses a counter-only claim")
            .commit()
            .expect("reconstruction claim commits");

        let waiter = {
            let admission = admission.clone();
            tokio::spawn(async move { admission.request_live().await })
        };
        tokio::task::yield_now().await;
        assert!(admission.is_requested(), "resume requests live authority");

        let lease = DirectFactLease::try_acquire(
            &BackpressureWriter::disabled(),
            NonZeroU64::new(3).expect("non-zero bound"),
        )
        .expect("admission calculation succeeds")
        .expect("disabled writer needs no physical reservation");
        admission
            .grant(lease)
            .expect("supervisor grants live lease");
        waiter
            .await
            .expect("request task joins")
            .expect("request receives grant");

        let live = admission
            .claim(1)
            .expect("live claim succeeds")
            .expect("live lease supplies a claim");
        assert!(
            !live.requires_track_accounting(),
            "live claims settle through the affine lease"
        );
        live.commit().expect("live claim commits");

        assert_eq!(
            admission.close().expect("admission closes"),
            3,
            "completion includes reconstruction and resumed live rows"
        );
    }
}
