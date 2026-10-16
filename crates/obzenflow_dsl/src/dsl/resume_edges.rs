// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Which sink edges a resume reproduces position by position (FLOWIP-134k
//! B3). Core's order certification runs over the current flow and every
//! archive the resume reads; a sink classifies deliveries by its receipts
//! only on edges it passes against all of them. Each other edge follows
//! generation, and the resume prints a notice naming the stage that keeps it
//! there.

use obzenflow_core::journal::archive::certification::{
    certify_orders, Comparison, StageCertification, StageOrders,
};
use obzenflow_core::journal::archive::manifest::DeliveryOrder;
use obzenflow_core::journal::archive::ReplayArchive;
use obzenflow_core::StageId;
use obzenflow_runtime::stages::SinkResumeEdges;
use std::collections::{BTreeSet, HashMap};
use std::path::Path;

/// One resumed flow's sink plan: each sink's certified edges, by sink stage
/// id, and the notices for edges that follow generation.
#[derive(Debug, Default)]
pub(crate) struct SinkResumePlan {
    pub edges: HashMap<StageId, SinkResumeEdges>,
    pub notices: Vec<String>,
}

/// `current` holds the resumed flow's inbound edges and delivery orders by
/// stage key; `sinks` names each sink's key and stage id; `stage_ids` maps
/// every stage key to its current id.
pub(crate) fn sink_resume_plan(
    current: &StageOrders,
    sinks: &[(String, StageId)],
    stage_ids: &HashMap<String, StageId>,
    archive: &dyn ReplayArchive,
    run: &Path,
) -> SinkResumePlan {
    let members: Vec<StageOrders> = std::iter::once(archive.stage_orders())
        .chain(
            archive
                .ancestors()
                .iter()
                .map(|ancestor| ancestor.stage_orders()),
        )
        .collect();
    let certifications: Vec<_> = members
        .iter()
        .map(|recorded| certify_orders(recorded, current, Comparison::Replay))
        .collect();

    let mut plan = SinkResumePlan::default();
    let mut sinks = sinks.to_vec();
    sinks.sort();
    for (sink_key, sink_id) in sinks {
        let Some(sink) = current.0.get(&sink_key) else {
            continue;
        };
        let mut certified = HashMap::new();
        let mut blocking = BTreeSet::new();
        for upstream in &sink.inbound {
            let mut passes = true;
            for certification in &certifications {
                match certification.get(upstream) {
                    Some(StageCertification::Certified) => {}
                    Some(
                        StageCertification::NotOrderCertified { blocking: stages }
                        | StageCertification::OrderRecorded { blocking: stages },
                    ) => {
                        passes = false;
                        blocking.extend(stages.iter().cloned());
                    }
                    None => passes = false,
                }
            }
            if let (true, Some(upstream_id)) = (passes, stage_ids.get(upstream)) {
                certified.insert(*upstream_id, upstream.clone());
            }
        }
        for stage in &blocking {
            plan.notices.push(notice(&sink_key, stage, current, run));
        }
        plan.edges.insert(sink_id, SinkResumeEdges { certified });
    }
    plan
}

fn notice(sink: &str, stage: &str, current: &StageOrders, run: &Path) -> String {
    let arrival_order = current
        .0
        .get(stage)
        .is_some_and(|order| order.delivery_order == DeliveryOrder::Unordered);
    if arrival_order {
        format!(
            "Sink {sink} reads through {stage}, which takes its inputs in arrival order, so \
             deliveries it had not made before the stop are labelled [replay] and skip its \
             delivery policy."
        )
    } else {
        format!(
            "Sink {sink} reads through {stage}, whose input order changed since {} was \
             recorded, so deliveries it had not made before the stop are labelled [replay] and \
             skip its delivery policy.",
            run.display()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::journal::archive::certification::StageOrder;
    use std::sync::Arc;
    use DeliveryOrder::{SingleInput, Unordered};

    fn orders(stages: &[(&str, &[&str], DeliveryOrder)]) -> StageOrders {
        StageOrders(
            stages
                .iter()
                .map(|(key, inbound, delivery_order)| {
                    (
                        key.to_string(),
                        StageOrder {
                            inbound: inbound.iter().map(|s| s.to_string()).collect(),
                            delivery_order: *delivery_order,
                        },
                    )
                })
                .collect(),
        )
    }

    /// An archive that records only stage orders.
    struct Recorded(StageOrders, Vec<Arc<dyn ReplayArchive>>);

    #[async_trait::async_trait]
    impl ReplayArchive for Recorded {
        async fn open_source_reader(
            &self,
            _: &str,
            _: obzenflow_core::event::context::StageType,
        ) -> Result<
            Box<dyn obzenflow_core::journal::JournalReader<obzenflow_core::ChainEvent>>,
            obzenflow_core::journal::archive::ReplayError,
        > {
            unreachable!()
        }
        async fn open_effect_history(
            &self,
            _: &str,
        ) -> Result<
            Box<dyn obzenflow_core::journal::JournalReader<obzenflow_core::ChainEvent>>,
            obzenflow_core::journal::archive::ReplayError,
        > {
            unreachable!()
        }
        fn source_data_journal_path(
            &self,
            _: &str,
        ) -> Result<std::path::PathBuf, obzenflow_core::journal::archive::ReplayError> {
            unreachable!()
        }
        fn archive_flow_id(&self) -> &str {
            "flow_recorded"
        }
        fn archived_stage_id(
            &self,
            _: &str,
        ) -> Result<StageId, obzenflow_core::journal::archive::ReplayError> {
            unreachable!()
        }
        fn archive_status(&self) -> obzenflow_core::journal::ArchiveStatus {
            obzenflow_core::journal::ArchiveStatus::Unknown
        }
        fn status_derivation(&self) -> obzenflow_core::journal::StatusDerivation {
            unreachable!()
        }
        fn allow_incomplete_archive(&self) -> bool {
            false
        }
        fn source_stage_keys(&self) -> Vec<String> {
            Vec::new()
        }
        fn recorded_source_eof(
            &self,
            _: &str,
        ) -> Option<obzenflow_core::event::payloads::flow_control_payload::EofKind> {
            None
        }
        fn recorded_source_frontier(
            &self,
            _: &str,
        ) -> obzenflow_core::source::RecordedSourceFrontier {
            Default::default()
        }
        fn archive_path(&self) -> &Path {
            Path::new("/runs/r1")
        }
        fn claim_continuation(
            &self,
            _: obzenflow_core::FlowId,
            _: &obzenflow_core::journal::factory::CurrentRunLocator,
        ) -> Result<(), obzenflow_core::journal::archive::ReplayError> {
            unreachable!()
        }
        fn ancestors(&self) -> Vec<Arc<dyn ReplayArchive>> {
            self.1.clone()
        }
        fn stage_orders(&self) -> StageOrders {
            self.0.clone()
        }
    }

    fn ids(keys: &[&str]) -> HashMap<String, StageId> {
        keys.iter()
            .map(|key| (key.to_string(), StageId::new()))
            .collect()
    }

    #[test]
    fn each_sink_receives_the_edges_certification_passes() {
        let flow = orders(&[
            ("orders", &[], SingleInput),
            ("normalize", &["orders"], SingleInput),
            ("console_out", &["normalize"], SingleInput),
        ]);
        let stage_ids = ids(&["orders", "normalize", "console_out"]);
        let archive = Recorded(flow.clone(), Vec::new());
        let plan = sink_resume_plan(
            &flow,
            &[("console_out".to_string(), stage_ids["console_out"])],
            &stage_ids,
            &archive,
            Path::new("/runs/r1"),
        );
        assert_eq!(
            plan.edges[&stage_ids["console_out"]].certified,
            HashMap::from([(stage_ids["normalize"], "normalize".to_string())])
        );
        assert!(plan.notices.is_empty());
    }

    #[test]
    fn an_unordered_fan_in_and_a_changed_order_name_the_blocking_stage() {
        let flow = orders(&[
            ("web_orders", &[], SingleInput),
            ("store_orders", &[], SingleInput),
            ("normalize", &["store_orders", "web_orders"], Unordered),
            ("console_out", &["normalize"], SingleInput),
        ]);
        let stage_ids = ids(&["web_orders", "store_orders", "normalize", "console_out"]);
        let sinks = [("console_out".to_string(), stage_ids["console_out"])];
        let plan = sink_resume_plan(
            &flow,
            &sinks,
            &stage_ids,
            &Recorded(flow.clone(), Vec::new()),
            Path::new("/runs/r1"),
        );
        assert!(plan.edges[&stage_ids["console_out"]].certified.is_empty());
        assert_eq!(
            plan.notices,
            vec![
                "Sink console_out reads through normalize, which takes its inputs in arrival \
                 order, so deliveries it had not made before the stop are labelled [replay] and \
                 skip its delivery policy."
                    .to_string()
            ]
        );

        // An ancestor in the merged lineage that recorded another order
        // blocks the edge too.
        let ordered = orders(&[
            ("web_orders", &[], SingleInput),
            ("normalize", &["web_orders"], SingleInput),
            ("console_out", &["normalize"], SingleInput),
        ]);
        let recorded = orders(&[
            ("web_orders", &[], SingleInput),
            ("normalize", &["web_orders"], DeliveryOrder::Canonical),
            ("console_out", &["normalize"], SingleInput),
        ]);
        let ancestor: Arc<dyn ReplayArchive> = Arc::new(Recorded(recorded, Vec::new()));
        let plan = sink_resume_plan(
            &ordered,
            &sinks,
            &stage_ids,
            &Recorded(ordered.clone(), vec![ancestor]),
            Path::new("/runs/r1"),
        );
        assert!(plan.edges[&stage_ids["console_out"]].certified.is_empty());
        assert_eq!(
            plan.notices,
            vec![
                "Sink console_out reads through normalize, whose input order changed since \
                 /runs/r1 was recorded, so deliveries it had not made before the stop are \
                 labelled [replay] and skip its delivery policy."
                    .to_string()
            ]
        );
    }
}
