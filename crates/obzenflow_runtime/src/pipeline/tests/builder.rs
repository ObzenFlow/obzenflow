// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Pipeline construction and cleanup on preparation failures.

use super::*;
#[cfg(test)]
use crate::feed_plan::{FactVisibility, FeedRole, LogicalFeed, PayloadTypeDescriptor};
use crate::journal::FlowJournalFactory;
use crate::pipeline::tests::support::{new_system_journal, ControlledJournal};
#[cfg(test)]
use obzenflow_topology::{DirectedEdge, EdgeKind, StageInfo, StageType, TypeHintInfo};

use crate::pipeline::tests::support::{
    owned_test_stage, source_sink_topology_with_source, DiscardSnapshots, ShutdownProbe,
};
use obzenflow_core::event::context::StageType as CoreStageType;
use std::sync::atomic::Ordering;

#[test]
fn expected_contract_keys_preserve_multiple_logical_feeds_for_stage_pair() {
    let upstream = StageId::new();
    let downstream = StageId::new();
    let upstream_topology_id = upstream.to_topology_id();
    let downstream_topology_id = downstream.to_topology_id();
    let topology = Topology::new_unvalidated(
        vec![
            StageInfo::new(upstream_topology_id, "upstream", StageType::Transform),
            StageInfo::new(downstream_topology_id, "downstream", StageType::Join),
        ],
        vec![DirectedEdge::new(
            upstream_topology_id,
            downstream_topology_id,
            EdgeKind::Forward,
        )],
    )
    .expect("topology");

    let first_type = TypeHintInfo::exact("crate::FirstFact");
    let second_type = TypeHintInfo::exact("crate::SecondFact");
    let first_key = FeedKey::new(upstream, downstream, "test.first", FeedRole::Reference);
    let second_key = FeedKey::new(upstream, downstream, "test.second", FeedRole::Stream);
    let feed_plan = FeedPlan::new(
        HashMap::new(),
        vec![
            LogicalFeed {
                key: first_key.clone(),
                selected_payload: PayloadTypeDescriptor::from_type_hint(
                    first_type,
                    FactVisibility::Routable,
                ),
            },
            LogicalFeed {
                key: second_key.clone(),
                selected_payload: PayloadTypeDescriptor::from_type_hint(
                    second_type,
                    FactVisibility::Routable,
                ),
            },
        ],
    );

    let keys = derive_expected_contract_keys(&topology, &feed_plan);

    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&first_key));
    assert!(keys.contains(&second_key));
    assert!(!keys.contains(&FeedKey::legacy_stage_pair(upstream, downstream)));
}

#[test]
fn expected_contract_keys_fallback_to_legacy_stage_pair_without_feed_plan() {
    let upstream = StageId::new();
    let downstream = StageId::new();
    let upstream_topology_id = upstream.to_topology_id();
    let downstream_topology_id = downstream.to_topology_id();
    let topology = Topology::new_unvalidated(
        vec![
            StageInfo::new(upstream_topology_id, "upstream", StageType::Transform),
            StageInfo::new(downstream_topology_id, "downstream", StageType::Sink),
        ],
        vec![DirectedEdge::new(
            upstream_topology_id,
            downstream_topology_id,
            EdgeKind::Forward,
        )],
    )
    .expect("topology");

    let keys = derive_expected_contract_keys(&topology, &FeedPlan::default());

    assert_eq!(keys.len(), 1);
    assert!(keys.contains(&FeedKey::legacy_stage_pair(upstream, downstream)));
}

pub async fn subscription_or_metrics_preparation_failure_joins_every_supplied_stage(
    make_journals: fn() -> Box<dyn FlowJournalFactory>,
) {
    for fail_reader in [1, 2] {
        let system_id = SystemId::new();
        let mut journals = make_journals();
        let mut journal = ControlledJournal::new(new_system_journal(&mut *journals, system_id));
        journal.fail_reader = Some(fail_reader);
        let (topology, source, sink) = source_sink_topology_with_source();
        let probes = [ShutdownProbe::default(), ShutdownProbe::default()];
        let result =
            crate::pipeline::PipelineBuilder::new(topology, Arc::new(journal), FlowId::new())
                .with_sources(vec![Box::new(owned_test_stage(
                    source,
                    CoreStageType::FiniteSource,
                    Some(probes[0].clone()),
                ))])
                .with_stages(vec![Box::new(owned_test_stage(
                    sink,
                    CoreStageType::Sink,
                    Some(probes[1].clone()),
                ))])
                .with_metrics_exporter(Arc::new(DiscardSnapshots))
                .build()
                .await;
        assert!(
            result.is_err(),
            "reader {fail_reader} must fail construction"
        );
        for probe in probes {
            assert_eq!(probe.request_abort_count.load(Ordering::Relaxed), 1);
            assert_eq!(probe.abort_and_join_count.load(Ordering::Relaxed), 1);
        }
    }
}
