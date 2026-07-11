// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-128a B4 public/internal contract firewall.

use obzenflow_core::composite::{
    CompositeDefinition, CompositeLifecycleProjection, CompositeStatus,
};
use obzenflow_core::event::context::CompositeActivationContext;
use obzenflow_core::event::system_event::SystemFeedRole;
use obzenflow_core::id::{CompositeId, RoleId, StageId};
use obzenflow_core::metrics::{
    AppMetricsSnapshot, BoundaryDirection, CompositeContract, CompositeDurationBucket,
    CompositeDurationHistogram, CompositeDurationInvalid, CompositeMemberHealth,
    CompositePortTraffic,
};
use obzenflow_core::{EventId, EventType};

fn assert_attribute_before(source: &str, declaration: &str, attribute: &str) {
    let declaration_offset = source
        .find(declaration)
        .unwrap_or_else(|| panic!("missing declaration `{declaration}`"));
    let prefix = &source[declaration_offset.saturating_sub(320)..declaration_offset];
    assert!(
        prefix.contains(attribute),
        "`{declaration}` must retain `{attribute}`"
    );
}

#[test]
fn stable_facade_and_exporter_dtos_are_constructible_from_an_external_crate() {
    let member = StageId::new();
    let composite = CompositeId::new("ai_map_reduce:digest");
    let projection = CompositeLifecycleProjection::new([CompositeDefinition::new(
        composite.clone(),
        vec![(member, RoleId::new("map"))],
    )])
    .unwrap();
    assert!(matches!(
        projection.status(&composite),
        Some(CompositeStatus::Waiting)
    ));

    let activation = CompositeActivationContext::new(composite.clone(), EventId::new(), "in", 100);
    assert_eq!(activation.entry_port, "in");

    let traffic =
        CompositePortTraffic::new(composite.clone(), "out", BoundaryDirection::Outbound, 0);
    let health = CompositeMemberHealth::new(composite.clone(), 0);
    let histogram = CompositeDurationHistogram::new(
        composite.clone(),
        "in",
        "out",
        vec![CompositeDurationBucket::new(0.005, 0)],
        0,
        0.0,
    );
    let invalid =
        CompositeDurationInvalid::new(composite.clone(), "in", "out", "exit_precedes_entry", 0);
    let contract = CompositeContract::new(
        composite,
        "out",
        StageId::new(),
        BoundaryDirection::Outbound,
        Some(EventType::from("demo.output.v1")),
        Some(SystemFeedRole::Input),
    );

    let mut snapshot = AppMetricsSnapshot::default();
    snapshot.composite_port_traffic.push(traffic);
    snapshot.composite_member_health.push(health);
    snapshot.composite_boundary_durations.push(histogram);
    snapshot.composite_boundary_duration_invalid.push(invalid);
    snapshot.composite_contracts.push(contract);
}

#[test]
fn extensible_and_hidden_surface_lists_are_source_guarded() {
    let projection = include_str!("../src/composite/projection.rs");
    for declaration in [
        "pub enum CompositeStatus",
        "pub enum CompositeProjectionError",
    ] {
        assert_attribute_before(projection, declaration, "#[non_exhaustive]");
    }

    let activation = include_str!("../src/event/context/composite_activation_context.rs");
    assert_attribute_before(
        activation,
        "pub struct CompositeActivationContext",
        "#[non_exhaustive]",
    );
    let observability = include_str!("../src/event/context/observability_context.rs");
    assert_attribute_before(
        observability,
        "pub struct ObservabilityContext",
        "#[non_exhaustive]",
    );

    let snapshots = include_str!("../src/metrics/snapshots.rs");
    assert_attribute_before(
        snapshots,
        "pub struct AppMetricsSnapshot",
        "#[non_exhaustive]",
    );

    let metrics = include_str!("../src/metrics/composite.rs");
    for declaration in [
        "pub struct CompositePortTraffic",
        "pub struct CompositeMemberHealth",
        "pub struct CompositeContract",
        "pub struct CompositeDurationBucket",
        "pub struct CompositeDurationHistogram",
        "pub struct CompositeDurationInvalid",
    ] {
        assert_attribute_before(metrics, declaration, "#[non_exhaustive]");
    }
    for declaration in [
        "pub struct CompositeBoundaryPort",
        "pub struct CompositeBoundaryEdge",
        "pub struct CompositeBoundary",
        "pub trait BoundaryMetricsView",
        "pub struct CompositeDurationAccumulator",
    ] {
        assert_attribute_before(metrics, declaration, "#[doc(hidden)]");
    }
}
