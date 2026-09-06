// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The same explicit sink presence governs preflight and Runtime collection.
use obzenflow::{sinks, sources};
use obzenflow_adapters::monitoring::MetricsReadModel;
use obzenflow_core::event::{ChainEvent, PipelineLifecycleEvent, SystemEvent, SystemEventType};
use obzenflow_core::journal::{
    journal_name::JournalName, journal_owner::JournalOwner, Journal, JournalError,
};
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, FlowDefinition};
use obzenflow_infra::journal::{memory_journals, MemoryJournalFactory};
use obzenflow_runtime::journal::{FlowJournalFactory, RunResourcePlan, RunSubstrateState};
use obzenflow_runtime::run_context::FlowBuildContext;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Observation(u64);
impl TypedPayload for Observation {
    const EVENT_TYPE: &'static str = "monitoring.injection";
}

struct RecordingFactory {
    inner: MemoryJournalFactory,
    admission: Arc<Mutex<Vec<bool>>>,
}
impl FlowJournalFactory for RecordingFactory {
    fn run_state(&self) -> RunSubstrateState {
        self.inner.run_state()
    }
    fn resource_preflight(&self, plan: &RunResourcePlan) -> Result<(), JournalError> {
        self.admission.lock().unwrap().push(plan.metrics_enabled);
        self.inner.resource_preflight(plan)
    }
    fn create_chain_journal(
        &mut self,
        name: JournalName,
        owner: JournalOwner,
    ) -> Result<Arc<dyn Journal<ChainEvent>>, JournalError> {
        self.inner.create_chain_journal(name, owner)
    }
    fn create_system_journal(
        &mut self,
        name: JournalName,
        owner: JournalOwner,
    ) -> Result<Arc<dyn Journal<SystemEvent>>, JournalError> {
        self.inner.create_system_journal(name, owner)
    }
}

#[tokio::test]
async fn ordinary_and_materialised_builds_use_only_the_injected_sink() {
    for materialised in [false, true] {
        for enabled in [false, true] {
            let model = Arc::new(MetricsReadModel::default());
            assert!(model.snapshot().app.is_none());
            assert!(model.snapshot().infra.is_none());
            let admission = Arc::new(Mutex::new(Vec::new()));
            let recorded = admission.clone();
            let factory = move |flow_id| {
                Ok(RecordingFactory {
                    inner: memory_journals()(flow_id)?,
                    admission: recorded.clone(),
                })
            };
            let input = sources::finite(vec![Observation(1), Observation(2)]);
            let output = sinks::debug::<Observation>();
            let definition = flow! {
                name: "monitoring_injection",
                journals: factory,
                stages: {
                    input = source!(Observation => input);
                    output = sink!(Observation => output);
                },
                topology: { input |> output; }
            };
            let definition = if materialised {
                FlowDefinition::materialize(move |_| Ok(definition))
            } else {
                definition
            };
            let mut context = FlowBuildContext::for_tests();
            if enabled {
                context = context.with_metrics_sink(model.clone());
            }
            let handle = definition.build(context).await.unwrap();
            let system = handle.system_journal().unwrap();
            handle.run().await.unwrap();
            assert_eq!(*admission.lock().unwrap(), vec![enabled]);
            assert_eq!(model.snapshot().app.is_some(), enabled);
            assert!(
                model.snapshot().infra.is_none(),
                "Runtime must not fabricate infrastructure observations"
            );
            let mut reader = system.reader_from(0).await.unwrap();
            let mut coordination_seen = false;
            let mut terminal_totals = None;
            while let Some(envelope) = reader.next().await.unwrap() {
                if let SystemEventType::PipelineLifecycle(PipelineLifecycleEvent::Completed {
                    metrics,
                    ..
                }) = &envelope.event.event
                {
                    terminal_totals = Some((
                        metrics.events_in_total,
                        metrics.events_out_total,
                        metrics.errors_total,
                    ));
                }
                coordination_seen |= matches!(
                    envelope.event.event,
                    SystemEventType::MetricsCoordination(_)
                );
            }
            assert_eq!(
                coordination_seen, enabled,
                "None must not start an aggregator"
            );
            assert_eq!(
                terminal_totals,
                Some((2, 2, 0)),
                "Studio's terminal lifecycle totals must survive without reporting"
            );
        }
    }
}
