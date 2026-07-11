// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Exact composite boundary metrics (FLOWIP-128a B3).
//!
//! A composite boundary is a named graph cut. Physical topology edges carry
//! the port they cross, while wide stage events carry exact Data input and
//! output counts. The projection in this module never guesses a boundary from
//! a first entry/exit member, stage names, payload heuristics, or display state.

use crate::event::chain_event::ChainEventContent;
use crate::event::system_event::{ContractName, ContractResultStatusLabel, SystemFeedRole};
use crate::event::ChainEvent;
use crate::id::{CompositeId, StageId};
use crate::metrics::snapshots::{ContractMetricsSnapshot, ContractViolationCauseLabel};
use crate::{EventId, EventType};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Which side of a named composite boundary a physical edge crosses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BoundaryDirection {
    Inbound,
    Outbound,
}

impl BoundaryDirection {
    pub const fn as_str(self) -> &'static str {
        match self {
            BoundaryDirection::Inbound => "inbound",
            BoundaryDirection::Outbound => "outbound",
        }
    }
}

/// One declared named boundary port.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompositeBoundaryPort {
    pub name: String,
    pub direction: BoundaryDirection,
    pub member: StageId,
    pub payload_event_types: Vec<EventType>,
}

impl CompositeBoundaryPort {
    pub fn accepts(&self, event_type: &str) -> bool {
        self.payload_event_types
            .iter()
            .any(|candidate| candidate.as_str() == event_type)
    }
}

/// One physical graph-cut edge bound to a named port.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompositeBoundaryEdge {
    pub port: String,
    pub direction: BoundaryDirection,
    pub member: StageId,
    pub peer: StageId,
    pub upstream: StageId,
    pub downstream: StageId,
}

/// A composite's exact named graph cut, derived from the topology manifest and
/// persisted edge-port bindings.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompositeBoundary {
    pub composite_id: CompositeId,
    pub members: Vec<StageId>,
    pub ports: Vec<CompositeBoundaryPort>,
    pub edges: Vec<CompositeBoundaryEdge>,
}

/// Read-only view of the cumulative Data counters needed by the graph-cut
/// projection. Inputs retain the physical upstream dimension so internal
/// member traffic cannot leak into an input-port count.
#[doc(hidden)]
pub trait BoundaryMetricsView {
    fn data_inputs(&self, member: StageId, upstream: StageId, event_type: &EventType) -> u64;
    fn data_outputs(&self, member: StageId, event_type: &EventType) -> u64;
    fn errors(&self, member: StageId) -> u64;
}

/// Logical throughput at one named boundary port.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CompositePortTraffic {
    pub composite: CompositeId,
    pub port: String,
    pub direction: BoundaryDirection,
    pub events_total: u64,
}

impl CompositePortTraffic {
    /// Construct one exporter-facing named-port traffic snapshot.
    pub fn new(
        composite: CompositeId,
        port: impl Into<String>,
        direction: BoundaryDirection,
        events_total: u64,
    ) -> Self {
        Self {
            composite,
            port: port.into(),
            direction,
            events_total,
        }
    }

    /// Project one counter per connected named port. Fan-out does not multiply
    /// output traffic: the member/event-type counter is read once per port.
    #[doc(hidden)]
    pub fn project(boundary: &CompositeBoundary, metrics: &impl BoundaryMetricsView) -> Vec<Self> {
        let mut projected = Vec::new();

        for port in &boundary.ports {
            let edges: Vec<_> = boundary
                .edges
                .iter()
                .filter(|edge| edge.port == port.name && edge.direction == port.direction)
                .collect();

            // An unbound or legacy port has no graph-cut traffic identity. Do
            // not manufacture a zero from member-wide counters.
            if edges.is_empty() {
                continue;
            }

            let events_total = match port.direction {
                BoundaryDirection::Inbound => {
                    let unique_upstreams: HashSet<StageId> =
                        edges.iter().map(|edge| edge.upstream).collect();
                    unique_upstreams
                        .into_iter()
                        .flat_map(|upstream| {
                            port.payload_event_types
                                .iter()
                                .map(move |event_type| (upstream, event_type))
                        })
                        .map(|(upstream, event_type)| {
                            metrics.data_inputs(port.member, upstream, event_type)
                        })
                        .fold(0, u64::saturating_add)
                }
                BoundaryDirection::Outbound => port
                    .payload_event_types
                    .iter()
                    .map(|event_type| metrics.data_outputs(port.member, event_type))
                    .fold(0, u64::saturating_add),
            };

            projected.push(Self {
                composite: boundary.composite_id.clone(),
                port: port.name.clone(),
                direction: port.direction,
                events_total,
            });
        }

        projected.sort_by(|left, right| {
            (left.direction.as_str(), left.port.as_str())
                .cmp(&(right.direction.as_str(), right.port.as_str()))
        });
        projected
    }
}

/// Component-health volume for all members of one composite. This is not an
/// inferred failed-activation count.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CompositeMemberHealth {
    pub composite: CompositeId,
    pub member_errors_total: u64,
}

impl CompositeMemberHealth {
    /// Construct one exporter-facing member-health snapshot.
    pub fn new(composite: CompositeId, member_errors_total: u64) -> Self {
        Self {
            composite,
            member_errors_total,
        }
    }

    #[doc(hidden)]
    pub fn project(boundary: &CompositeBoundary, metrics: &impl BoundaryMetricsView) -> Self {
        Self {
            composite: boundary.composite_id.clone(),
            member_errors_total: boundary
                .members
                .iter()
                .map(|member| metrics.errors(*member))
                .fold(0, u64::saturating_add),
        }
    }
}

/// One composite boundary edge's contract facts, re-keyed without discarding
/// its named port, selected event type, or logical feed role.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CompositeContract {
    pub composite: CompositeId,
    pub port: String,
    pub peer: StageId,
    pub direction: BoundaryDirection,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_event_type: Option<EventType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub feed_role: Option<SystemFeedRole>,
    pub results: Vec<(ContractName, ContractResultStatusLabel, u64)>,
    pub violations: Vec<(ContractName, ContractViolationCauseLabel, u64)>,
    pub reader_seq: Option<u64>,
    pub advertised_writer_seq: Option<u64>,
}

type ContractProjectionKey = (
    StageId,
    BoundaryDirection,
    String,
    Option<EventType>,
    Option<SystemFeedRole>,
);

impl CompositeContract {
    /// Construct an empty exporter-facing contract view for one exact boundary
    /// edge/feed identity. Result, violation, and sequence evidence may then be
    /// populated through the public fields or serde.
    pub fn new(
        composite: CompositeId,
        port: impl Into<String>,
        peer: StageId,
        direction: BoundaryDirection,
        selected_event_type: Option<EventType>,
        feed_role: Option<SystemFeedRole>,
    ) -> Self {
        Self {
            composite,
            port: port.into(),
            peer,
            direction,
            selected_event_type,
            feed_role,
            results: Vec::new(),
            violations: Vec::new(),
            reader_seq: None,
            advertised_writer_seq: None,
        }
    }

    fn empty(composite: CompositeId, key: &ContractProjectionKey) -> Self {
        Self {
            composite,
            peer: key.0,
            direction: key.1,
            port: key.2.clone(),
            selected_event_type: key.3.clone(),
            feed_role: key.4,
            results: Vec::new(),
            violations: Vec::new(),
            reader_seq: None,
            advertised_writer_seq: None,
        }
    }

    /// Relabel only exact physical edges in the durable graph cut. Internal
    /// member edges and unrelated edges cannot be classified accidentally.
    #[doc(hidden)]
    pub fn project(boundary: &CompositeBoundary, contracts: &ContractMetricsSnapshot) -> Vec<Self> {
        let classify = |upstream: StageId, downstream: StageId| {
            boundary
                .edges
                .iter()
                .find(|edge| edge.upstream == upstream && edge.downstream == downstream)
                .map(|edge| (edge.peer, edge.direction, edge.port.clone()))
        };

        let key_for = |edge: &crate::metrics::snapshots::ContractMetricEdgeKey| {
            let (peer, direction, port) = classify(edge.upstream, edge.downstream)?;
            Some((
                peer,
                direction,
                port,
                edge.selected_event_type.clone(),
                edge.feed_role,
            ))
        };

        let mut projected: HashMap<ContractProjectionKey, CompositeContract> = HashMap::new();

        for (result, count) in &contracts.results_total {
            if let Some(key) = key_for(&result.edge) {
                projected
                    .entry(key.clone())
                    .or_insert_with(|| Self::empty(boundary.composite_id.clone(), &key))
                    .results
                    .push((result.edge.contract.clone(), result.status, *count));
            }
        }
        for (violation, count) in &contracts.violations_total {
            if let Some(key) = key_for(&violation.edge) {
                projected
                    .entry(key.clone())
                    .or_insert_with(|| Self::empty(boundary.composite_id.clone(), &key))
                    .violations
                    .push((
                        violation.edge.contract.clone(),
                        violation.cause.clone(),
                        *count,
                    ));
            }
        }
        for (edge, seq) in &contracts.reader_seq {
            if let Some(key) = key_for(edge) {
                let entry = projected
                    .entry(key.clone())
                    .or_insert_with(|| Self::empty(boundary.composite_id.clone(), &key));
                entry.reader_seq = Some(entry.reader_seq.map_or(*seq, |current| current.max(*seq)));
            }
        }
        for (edge, seq) in &contracts.advertised_writer_seq {
            if let Some(key) = key_for(edge) {
                let entry = projected
                    .entry(key.clone())
                    .or_insert_with(|| Self::empty(boundary.composite_id.clone(), &key));
                entry.advertised_writer_seq = Some(
                    entry
                        .advertised_writer_seq
                        .map_or(*seq, |current| current.max(*seq)),
                );
            }
        }

        let mut result: Vec<_> = projected.into_values().collect();
        for entry in &mut result {
            entry.results.sort_by(|left, right| {
                (left.0.as_str(), left.1.as_str()).cmp(&(right.0.as_str(), right.1.as_str()))
            });
            entry.violations.sort_by(|left, right| {
                (left.0.as_str(), left.1.as_str()).cmp(&(right.0.as_str(), right.1.as_str()))
            });
        }
        result.sort_by(|left, right| {
            (
                left.direction.as_str(),
                left.port.as_str(),
                left.peer,
                left.selected_event_type.as_ref(),
                left.feed_role.map(SystemFeedRole::as_str),
            )
                .cmp(&(
                    right.direction.as_str(),
                    right.port.as_str(),
                    right.peer,
                    right.selected_event_type.as_ref(),
                    right.feed_role.map(SystemFeedRole::as_str),
                ))
        });
        result
    }
}

/// One cumulative Prometheus histogram bucket.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CompositeDurationBucket {
    pub upper_bound_seconds: f64,
    pub cumulative_count: u64,
}

impl CompositeDurationBucket {
    pub fn new(upper_bound_seconds: f64, cumulative_count: u64) -> Self {
        Self {
            upper_bound_seconds,
            cumulative_count,
        }
    }
}

/// Exact paired boundary-duration histogram for one entry-port/exit-port pair.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CompositeDurationHistogram {
    pub composite: CompositeId,
    pub entry_port: String,
    pub exit_port: String,
    pub buckets: Vec<CompositeDurationBucket>,
    pub count: u64,
    pub sum_seconds: f64,
}

impl CompositeDurationHistogram {
    pub fn new(
        composite: CompositeId,
        entry_port: impl Into<String>,
        exit_port: impl Into<String>,
        buckets: Vec<CompositeDurationBucket>,
        count: u64,
        sum_seconds: f64,
    ) -> Self {
        Self {
            composite,
            entry_port: entry_port.into(),
            exit_port: exit_port.into(),
            buckets,
            count,
            sum_seconds,
        }
    }
}

/// Rejected duration evidence. Reasons are a fixed vocabulary and all identity
/// labels come from the topology manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CompositeDurationInvalid {
    pub composite: CompositeId,
    pub entry_port: String,
    pub exit_port: String,
    pub reason: String,
    pub total: u64,
}

impl CompositeDurationInvalid {
    pub fn new(
        composite: CompositeId,
        entry_port: impl Into<String>,
        exit_port: impl Into<String>,
        reason: impl Into<String>,
        total: u64,
    ) -> Self {
        Self {
            composite,
            entry_port: entry_port.into(),
            exit_port: exit_port.into(),
            reason: reason.into(),
            total,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DurationSeriesKey {
    composite: CompositeId,
    entry_port: String,
    exit_port: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DurationObservationKey {
    series: DurationSeriesKey,
    activation: EventId,
    exit_event: EventId,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct InvalidDurationKey {
    series: DurationSeriesKey,
    reason: &'static str,
}

#[derive(Debug, Clone)]
struct DurationHistogramState {
    cumulative_buckets: Vec<u64>,
    count: u64,
    sum_seconds: f64,
}

/// Replayable fold of output-boundary facts into exact paired histograms.
///
/// The seen set is a projection idempotency key, not an activation registry:
/// no entry waits in memory for an exit. Every observation is self-contained
/// on the durable exit fact through `CompositeActivationContext`.
#[derive(Debug, Clone)]
#[doc(hidden)]
pub struct CompositeDurationAccumulator {
    bucket_upper_bounds_seconds: Vec<f64>,
    histograms: HashMap<DurationSeriesKey, DurationHistogramState>,
    invalid: HashMap<InvalidDurationKey, u64>,
    seen: HashSet<DurationObservationKey>,
}

impl Default for CompositeDurationAccumulator {
    fn default() -> Self {
        Self::new(vec![
            0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0,
        ])
    }
}

impl CompositeDurationAccumulator {
    pub fn new(mut bucket_upper_bounds_seconds: Vec<f64>) -> Self {
        bucket_upper_bounds_seconds.retain(|bound| bound.is_finite() && *bound >= 0.0);
        bucket_upper_bounds_seconds.sort_by(f64::total_cmp);
        bucket_upper_bounds_seconds.dedup_by(|left, right| left.total_cmp(right).is_eq());
        Self {
            bucket_upper_bounds_seconds,
            histograms: HashMap::new(),
            invalid: HashMap::new(),
            seen: HashSet::new(),
        }
    }

    /// Observe one committed stage data-journal fact. Non-Data facts, internal
    /// traffic, unmatched output types, and facts without activation context do
    /// not produce observations.
    pub fn observe_event(
        &mut self,
        boundaries: &[CompositeBoundary],
        journal_stage: StageId,
        event: &ChainEvent,
    ) {
        let ChainEventContent::Data { event_type, .. } = &event.content else {
            return;
        };

        for boundary in boundaries {
            let output_ports: Vec<_> = boundary
                .ports
                .iter()
                .filter(|port| {
                    port.direction == BoundaryDirection::Outbound
                        && port.member == journal_stage
                        && port.accepts(event_type)
                        && boundary.edges.iter().any(|edge| {
                            edge.direction == BoundaryDirection::Outbound
                                && edge.port == port.name
                                && edge.member == journal_stage
                        })
                })
                .collect();
            if output_ports.is_empty() {
                continue;
            }

            for activation in event
                .composite_activations()
                .iter()
                .filter(|activation| activation.composite_id == boundary.composite_id)
            {
                let declared_entry = boundary.ports.iter().any(|port| {
                    port.direction == BoundaryDirection::Inbound
                        && port.name == activation.entry_port
                });

                for output_port in &output_ports {
                    let entry_port = if declared_entry {
                        activation.entry_port.clone()
                    } else {
                        // Keep Prometheus label cardinality topology-bounded for
                        // malformed historical metadata.
                        "<unknown>".to_string()
                    };
                    let series = DurationSeriesKey {
                        composite: boundary.composite_id.clone(),
                        entry_port,
                        exit_port: output_port.name.clone(),
                    };
                    let observation = DurationObservationKey {
                        series: series.clone(),
                        activation: activation.activation,
                        exit_event: event.id,
                    };
                    if !self.seen.insert(observation) {
                        continue;
                    }

                    if !declared_entry {
                        let total = self
                            .invalid
                            .entry(InvalidDurationKey {
                                series,
                                reason: "unknown_entry_port",
                            })
                            .or_insert(0);
                        *total = total.saturating_add(1);
                        continue;
                    }

                    let Some(duration_ms) = event
                        .processing_info
                        .event_time
                        .checked_sub(activation.entered_at_ms)
                    else {
                        let total = self
                            .invalid
                            .entry(InvalidDurationKey {
                                series,
                                reason: "exit_precedes_entry",
                            })
                            .or_insert(0);
                        *total = total.saturating_add(1);
                        continue;
                    };

                    let duration_seconds = duration_ms as f64 / 1_000.0;
                    let histogram =
                        self.histograms
                            .entry(series)
                            .or_insert_with(|| DurationHistogramState {
                                cumulative_buckets: vec![0; self.bucket_upper_bounds_seconds.len()],
                                count: 0,
                                sum_seconds: 0.0,
                            });
                    for (index, upper_bound) in self.bucket_upper_bounds_seconds.iter().enumerate()
                    {
                        if duration_seconds <= *upper_bound {
                            histogram.cumulative_buckets[index] =
                                histogram.cumulative_buckets[index].saturating_add(1);
                        }
                    }
                    histogram.count = histogram.count.saturating_add(1);
                    histogram.sum_seconds += duration_seconds;
                }
            }
        }
    }

    pub fn histograms(&self) -> Vec<CompositeDurationHistogram> {
        let mut snapshots: Vec<_> = self
            .histograms
            .iter()
            .map(|(key, state)| CompositeDurationHistogram {
                composite: key.composite.clone(),
                entry_port: key.entry_port.clone(),
                exit_port: key.exit_port.clone(),
                buckets: self
                    .bucket_upper_bounds_seconds
                    .iter()
                    .zip(&state.cumulative_buckets)
                    .map(
                        |(upper_bound_seconds, cumulative_count)| CompositeDurationBucket {
                            upper_bound_seconds: *upper_bound_seconds,
                            cumulative_count: *cumulative_count,
                        },
                    )
                    .collect(),
                count: state.count,
                sum_seconds: state.sum_seconds,
            })
            .collect();
        snapshots.sort_by(|left, right| {
            (
                left.composite.as_str(),
                left.entry_port.as_str(),
                left.exit_port.as_str(),
            )
                .cmp(&(
                    right.composite.as_str(),
                    right.entry_port.as_str(),
                    right.exit_port.as_str(),
                ))
        });
        snapshots
    }

    pub fn invalid_evidence(&self) -> Vec<CompositeDurationInvalid> {
        let mut snapshots: Vec<_> = self
            .invalid
            .iter()
            .map(|(key, total)| CompositeDurationInvalid {
                composite: key.series.composite.clone(),
                entry_port: key.series.entry_port.clone(),
                exit_port: key.series.exit_port.clone(),
                reason: key.reason.to_string(),
                total: *total,
            })
            .collect();
        snapshots.sort_by(|left, right| {
            (
                left.composite.as_str(),
                left.entry_port.as_str(),
                left.exit_port.as_str(),
                left.reason.as_str(),
            )
                .cmp(&(
                    right.composite.as_str(),
                    right.entry_port.as_str(),
                    right.exit_port.as_str(),
                    right.reason.as_str(),
                ))
        });
        snapshots
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::context::CompositeActivationContext;
    use crate::event::system_event::SystemFeedRole;
    use crate::event::{ChainEventFactory, WriterId};
    use crate::metrics::snapshots::{ContractMetricEdgeKey, ContractMetricResultKey};
    use serde_json::json;

    #[derive(Default)]
    struct FakeMetrics {
        inputs: HashMap<(StageId, StageId, EventType), u64>,
        outputs: HashMap<(StageId, EventType), u64>,
        errors: HashMap<StageId, u64>,
    }

    impl BoundaryMetricsView for FakeMetrics {
        fn data_inputs(&self, member: StageId, upstream: StageId, event_type: &EventType) -> u64 {
            self.inputs
                .get(&(member, upstream, event_type.clone()))
                .copied()
                .unwrap_or(0)
        }

        fn data_outputs(&self, member: StageId, event_type: &EventType) -> u64 {
            self.outputs
                .get(&(member, event_type.clone()))
                .copied()
                .unwrap_or(0)
        }

        fn errors(&self, member: StageId) -> u64 {
            self.errors.get(&member).copied().unwrap_or(0)
        }
    }

    fn boundary() -> (
        CompositeBoundary,
        StageId,
        StageId,
        StageId,
        StageId,
        StageId,
    ) {
        let input = StageId::new();
        let success = StageId::new();
        let failed = StageId::new();
        let producer = StageId::new();
        let consumer = StageId::new();
        let composite_id = CompositeId::new("saga:checkout");
        (
            CompositeBoundary {
                composite_id,
                members: vec![input, success, failed],
                ports: vec![
                    CompositeBoundaryPort {
                        name: "commands".into(),
                        direction: BoundaryDirection::Inbound,
                        member: input,
                        payload_event_types: vec![EventType::from("checkout.command.v1")],
                    },
                    CompositeBoundaryPort {
                        name: "completed".into(),
                        direction: BoundaryDirection::Outbound,
                        member: success,
                        payload_event_types: vec![EventType::from("checkout.completed.v1")],
                    },
                    CompositeBoundaryPort {
                        name: "failed".into(),
                        direction: BoundaryDirection::Outbound,
                        member: failed,
                        payload_event_types: vec![EventType::from("checkout.failed.v1")],
                    },
                ],
                edges: vec![
                    CompositeBoundaryEdge {
                        port: "commands".into(),
                        direction: BoundaryDirection::Inbound,
                        member: input,
                        peer: producer,
                        upstream: producer,
                        downstream: input,
                    },
                    CompositeBoundaryEdge {
                        port: "completed".into(),
                        direction: BoundaryDirection::Outbound,
                        member: success,
                        peer: consumer,
                        upstream: success,
                        downstream: consumer,
                    },
                    CompositeBoundaryEdge {
                        port: "failed".into(),
                        direction: BoundaryDirection::Outbound,
                        member: failed,
                        peer: consumer,
                        upstream: failed,
                        downstream: consumer,
                    },
                ],
            },
            input,
            success,
            failed,
            producer,
            consumer,
        )
    }

    #[test]
    fn named_ports_use_exact_cut_counters_and_distinct_output_members() {
        let (boundary, input, success, failed, producer, _) = boundary();
        let mut metrics = FakeMetrics::default();
        metrics.inputs.insert(
            (input, producer, EventType::from("checkout.command.v1")),
            11,
        );
        metrics
            .inputs
            .insert((input, success, EventType::from("checkout.command.v1")), 99);
        metrics
            .outputs
            .insert((success, EventType::from("checkout.completed.v1")), 7);
        metrics
            .outputs
            .insert((failed, EventType::from("checkout.failed.v1")), 4);

        let projected = CompositePortTraffic::project(&boundary, &metrics);
        assert_eq!(projected.len(), 3);
        assert_eq!(projected[0].port, "commands");
        assert_eq!(projected[0].events_total, 11);
        assert_eq!(projected[1].port, "completed");
        assert_eq!(projected[1].events_total, 7);
        assert_eq!(projected[2].port, "failed");
        assert_eq!(projected[2].events_total, 4);
    }

    #[test]
    fn outbound_fanout_counts_one_authored_fact_once() {
        let (mut boundary, _, success, _, _, consumer) = boundary();
        let second_consumer = StageId::new();
        boundary.edges.push(CompositeBoundaryEdge {
            port: "completed".into(),
            direction: BoundaryDirection::Outbound,
            member: success,
            peer: second_consumer,
            upstream: success,
            downstream: second_consumer,
        });
        let mut metrics = FakeMetrics::default();
        metrics
            .outputs
            .insert((success, EventType::from("checkout.completed.v1")), 7);
        let completed = CompositePortTraffic::project(&boundary, &metrics)
            .into_iter()
            .find(|metric| metric.port == "completed")
            .unwrap();
        assert_eq!(completed.events_total, 7);
        assert_ne!(consumer, boundary.edges.last().unwrap().peer);
    }

    #[test]
    fn inbound_fanin_sums_exact_admissions_from_each_bound_peer() {
        let (mut boundary, input, _, _, producer, _) = boundary();
        let second_producer = StageId::new();
        boundary.edges.push(CompositeBoundaryEdge {
            port: "commands".into(),
            direction: BoundaryDirection::Inbound,
            member: input,
            peer: second_producer,
            upstream: second_producer,
            downstream: input,
        });
        let event_type = EventType::from("checkout.command.v1");
        let mut metrics = FakeMetrics::default();
        metrics
            .inputs
            .insert((input, producer, event_type.clone()), 4);
        metrics
            .inputs
            .insert((input, second_producer, event_type), 6);

        let commands = CompositePortTraffic::project(&boundary, &metrics)
            .into_iter()
            .find(|metric| metric.port == "commands")
            .unwrap();
        assert_eq!(commands.events_total, 10);
    }

    #[test]
    fn contracts_require_an_exact_cut_edge_and_preserve_feed_identity() {
        let (boundary, input, success, _, producer, consumer) = boundary();
        let mut contracts = ContractMetricsSnapshot::default();
        let edge = ContractMetricEdgeKey {
            upstream: producer,
            downstream: input,
            contract: ContractName::new("TransportContract"),
            selected_event_type: Some(EventType::from("checkout.command.v1")),
            feed_role: Some(SystemFeedRole::Input),
        };
        contracts.results_total.insert(
            ContractMetricResultKey {
                edge: edge.clone(),
                status: ContractResultStatusLabel::Passed,
            },
            3,
        );
        // Touching a boundary member is insufficient: this internal edge is
        // absent from the durable cut and must not be relabelled.
        contracts.results_total.insert(
            ContractMetricResultKey {
                edge: ContractMetricEdgeKey {
                    upstream: input,
                    downstream: success,
                    ..edge
                },
                status: ContractResultStatusLabel::Passed,
            },
            99,
        );

        let projected = CompositeContract::project(&boundary, &contracts);
        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].port, "commands");
        assert_eq!(projected[0].peer, producer);
        assert_eq!(
            projected[0].selected_event_type,
            Some(EventType::from("checkout.command.v1"))
        );
        assert_eq!(projected[0].feed_role, Some(SystemFeedRole::Input));
        assert_eq!(projected[0].results[0].2, 3);
        assert_ne!(consumer, producer);
    }

    fn exit_event(
        boundary: &CompositeBoundary,
        exit_member: StageId,
        event_type: &str,
        entered_at_ms: u64,
        exited_at_ms: u64,
    ) -> ChainEvent {
        let mut entry = ChainEventFactory::data_event(
            WriterId::Stage(StageId::new()),
            "checkout.command.v1",
            json!({}),
        );
        entry.processing_info.event_time = entered_at_ms;
        let activation = entry.id;
        entry.add_composite_activation(CompositeActivationContext::new(
            boundary.composite_id.clone(),
            activation,
            "commands",
            entered_at_ms,
        ));
        let mut exit =
            ChainEventFactory::data_event(WriterId::Stage(exit_member), event_type, json!({}));
        exit.processing_info.event_time = exited_at_ms;
        exit.merge_composite_activations_from(&entry);
        exit
    }

    #[test]
    fn duration_is_exact_replay_idempotent_and_negative_time_is_invalid() {
        let (boundary, _, success, failed, _, _) = boundary();
        let valid = exit_event(&boundary, success, "checkout.completed.v1", 1_000, 1_250);
        let mut second_exit = valid.clone();
        second_exit.id = EventId::new();
        second_exit.processing_info.event_time = 1_300;
        let invalid = exit_event(&boundary, failed, "checkout.failed.v1", 2_000, 1_900);
        let mut accumulator = CompositeDurationAccumulator::new(vec![0.1, 0.25, 1.0]);
        accumulator.observe_event(std::slice::from_ref(&boundary), success, &valid);
        accumulator.observe_event(std::slice::from_ref(&boundary), success, &valid);
        accumulator.observe_event(std::slice::from_ref(&boundary), success, &second_exit);
        accumulator.observe_event(std::slice::from_ref(&boundary), failed, &invalid);

        let histograms = accumulator.histograms();
        assert_eq!(histograms.len(), 1);
        assert_eq!(histograms[0].entry_port, "commands");
        assert_eq!(histograms[0].exit_port, "completed");
        assert_eq!(histograms[0].count, 2);
        assert!((histograms[0].sum_seconds - 0.55).abs() < f64::EPSILON * 4.0);
        assert_eq!(
            histograms[0]
                .buckets
                .iter()
                .map(|bucket| bucket.cumulative_count)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );

        let invalid = accumulator.invalid_evidence();
        assert_eq!(invalid.len(), 1);
        assert_eq!(invalid[0].exit_port, "failed");
        assert_eq!(invalid[0].reason, "exit_precedes_entry");
        assert_eq!(invalid[0].total, 1);
    }

    #[test]
    fn default_duration_buckets_are_the_stable_prometheus_contract() {
        let (boundary, _, success, _, _, _) = boundary();
        let valid = exit_event(&boundary, success, "checkout.completed.v1", 1_000, 1_250);
        let mut accumulator = CompositeDurationAccumulator::default();
        accumulator.observe_event(std::slice::from_ref(&boundary), success, &valid);

        let histograms = accumulator.histograms();
        let bounds: Vec<_> = histograms[0]
            .buckets
            .iter()
            .map(|bucket| bucket.upper_bound_seconds)
            .collect();
        assert_eq!(
            bounds,
            vec![
                0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
                300.0,
            ]
        );
    }

    #[test]
    fn duration_excludes_signals_unmatched_data_and_internal_member_output() {
        let (boundary, input, success, _, _, _) = boundary();
        let mut unmatched = exit_event(&boundary, success, "internal.fact.v1", 10, 20);
        unmatched.content = ChainEventContent::Delivery(
            crate::event::payloads::delivery_payload::DeliveryPayload::success(
                crate::event::payloads::delivery_payload::DeliveryMethod::Noop,
                None,
            ),
        );
        let internal = exit_event(&boundary, input, "checkout.command.v1", 10, 20);
        let mut accumulator = CompositeDurationAccumulator::default();
        accumulator.observe_event(std::slice::from_ref(&boundary), success, &unmatched);
        accumulator.observe_event(std::slice::from_ref(&boundary), input, &internal);
        assert!(accumulator.histograms().is_empty());
        assert!(accumulator.invalid_evidence().is_empty());
    }
}
