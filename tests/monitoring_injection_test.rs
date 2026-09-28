// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The same explicit exporter presence governs preflight and Runtime collection.
use obzenflow::stages::{sinks, sources};
use obzenflow_adapters::monitoring::MetricsReadModel;
use obzenflow_core::event::{ChainEvent, PipelineLifecycleEvent, SystemEvent, SystemPayload};
use obzenflow_core::journal::factory::{FlowJournalFactory, RunResourcePlan, RunSubstrateState};
use obzenflow_core::journal::{
    journal_name::JournalName, journal_owner::JournalOwner, Journal, JournalError,
};
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, FlowDefinition};
use obzenflow_infra::journal::{memory_journals, MemoryJournalFactory};
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
async fn ordinary_and_materialised_builds_use_only_the_injected_exporter() {
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
                context = context.with_metrics_exporter(model.clone());
            }
            let handle = definition.build(context).await.unwrap();
            let system = handle.system_journal().unwrap();
            let metrics_journals = handle.metrics_journals();
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
                if let SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Completed {
                    metrics,
                    ..
                }) = &envelope.payload
                {
                    terminal_totals = Some((
                        metrics.events_in_total,
                        metrics.events_out_total,
                        metrics.errors_total,
                    ));
                }
            }
            assert_eq!(metrics_journals.is_some(), enabled);
            if let Some(journals) = metrics_journals {
                coordination_seen = journals
                    .coordination
                    .read_all_unordered()
                    .await
                    .unwrap()
                    .iter()
                    .any(|row| matches!(row.payload, SystemPayload::MetricsCoordination(_)));
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

#[cfg(feature = "prometheus")]
#[tokio::test]
async fn reporting_without_a_host_fails_before_building_or_opening_replay() {
    use obzenflow_infra::application::{ApplicationError, FlowApplication};

    let dir = tempfile::tempdir_in("target").unwrap();
    let config = dir.path().join("obzenflow.toml");
    for startup in ["auto", "manual"] {
        std::fs::write(
            &config,
            format!(
                "[server]\nenabled = false\nstartup_mode = \"{startup}\"\n[metrics]\nenabled = true\n"
            ),
        )
        .unwrap();
        for replay in [false, true] {
            let mut args = vec!["reporting-admission".into()];
            if replay {
                args.extend([
                    "--replay-from".into(),
                    dir.path().join("absent-archive").into_os_string(),
                ]);
            }
            let result = FlowApplication::builder()
                .with_config_file(&config)
                .with_cli_args(args)
                .run_async(FlowDefinition::materialize(|_| {
                    panic!("invalid reporting configuration must not materialise a flow")
                }))
                .await;
            let Err(ApplicationError::InvalidConfiguration(message)) = result else {
                panic!("reporting admission must precede archive opening: {result:?}");
            };
            assert_eq!(
                message,
                "config error at server.enabled: enabled Prometheus reporting requires true and the web-host capability"
            );
        }
    }
}

#[cfg(feature = "prometheus")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_scrapes_allow_publication_and_flow_settlement() {
    use obzenflow_core::metrics::{InfraMetricsSnapshot, MetricsSnapshotExporter};
    use obzenflow_core::web::{HttpEndpoint, HttpMethod, ManagedResponse, Request};
    use obzenflow_infra::web::endpoints::PrometheusMetricsEndpoint;
    use std::time::{Duration, Instant};

    const INPUTS: u64 = 1_000;
    const PUBLICATIONS: u64 = 10_000;
    const READERS: usize = 4;
    let model = Arc::new(MetricsReadModel::default());
    model.publish_infra_snapshot(InfraMetricsSnapshot::default());
    let held = model.snapshot();
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let (ready_tx, mut ready_rx) = tokio::sync::mpsc::channel(READERS);
    let mut readers = tokio::task::JoinSet::new();
    for _ in 0..READERS {
        let endpoint = PrometheusMetricsEndpoint::new(model.clone());
        let stopped = stopped.clone();
        let ready = ready_tx.clone();
        readers.spawn(async move {
            let mut scrapes = 0;
            loop {
                let finishing = *stopped.borrow();
                let ManagedResponse::Unary(response) = endpoint
                    .handle(Request::new(HttpMethod::Get, "/metrics".into()))
                    .await
                    .unwrap()
                else {
                    panic!("metrics must be unary");
                };
                assert_eq!(response.status, 200);
                assert_eq!(
                    response.headers["Content-Type"],
                    "text/plain; version=0.0.4; charset=utf-8"
                );
                let body = String::from_utf8(response.body).unwrap();
                scrapes += 1;
                if scrapes == 1 {
                    ready.send(()).await.unwrap();
                }
                if finishing {
                    assert!(body.contains("state=\"completed\""));
                    return scrapes;
                }
                tokio::task::yield_now().await;
            }
        });
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        for _ in 0..READERS {
            ready_rx.recv().await.unwrap();
        }
    })
    .await
    .expect("all scrape workers must start before execution");

    let input = sources::finite((0..INPUTS).map(Observation).collect::<Vec<_>>());
    let output = sinks::debug::<Observation>();
    let handle = flow! {
        name: "concurrent_scrapes",
        journals: memory_journals(),
        stages: {
            input = source!(Observation => input);
            output = sink!(Observation => output);
        },
        topology: { input |> output; }
    }
    .build(FlowBuildContext::for_tests().with_metrics_exporter(model.clone()))
    .await
    .unwrap();
    let system = handle.system_journal().unwrap();
    let started = Instant::now();
    let (execution, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(handle.run(), async {
            for count in 1..=PUBLICATIONS {
                let mut snapshot = InfraMetricsSnapshot::default();
                snapshot.journal_metrics.writes_total = count;
                model.publish_infra_snapshot(snapshot);
                tokio::task::yield_now().await;
            }
        })
    })
    .await
    .expect("publication and physical flow settlement must finish with scrapers still active");
    execution.unwrap();
    let elapsed = started.elapsed();
    stop.send(true).unwrap();
    let mut scrapes = 0;
    while let Some(reader) = readers.join_next().await {
        let count = reader.unwrap();
        assert!(count > 1);
        scrapes += count;
    }
    let view = model.snapshot();
    assert_eq!(view.app.as_ref().unwrap().pipeline_state, "completed");
    assert_eq!(
        view.infra.as_ref().unwrap().journal_metrics.writes_total,
        PUBLICATIONS
    );
    assert_eq!(held.infra.as_ref().unwrap().journal_metrics.writes_total, 0);

    // Journal facts, independently of the scrape bodies, establish execution
    // totals and completed settlement while the reporting workers were active.
    let mut journal = system.reader_from(0).await.unwrap();
    let mut totals = None;
    let mut drained = false;
    while let Some(row) = journal.next().await.unwrap() {
        if let SystemPayload::PipelineLifecycle(event) = &row.payload {
            match event {
                PipelineLifecycleEvent::Completed { metrics, .. } => {
                    totals = Some((
                        metrics.events_in_total,
                        metrics.events_out_total,
                        metrics.errors_total,
                    ));
                }
                PipelineLifecycleEvent::Drained => drained = true,
                _ => {}
            }
        }
    }
    assert_eq!(totals, Some((INPUTS, INPUTS, 0)));
    assert!(drained);
    println!("{INPUTS} inputs and {PUBLICATIONS} infrastructure publications settled in {elapsed:?} with {scrapes} concurrent scrapes");
}
