// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Fixtures for the exact archive, metrics and Studio operations used by the
//! expensive correctness proofs. Fixture creation and validation stay untimed.

use obzenflow_core::event::payloads::execution_payload::{ExecutionPayload, StageLifecycleFact};
use obzenflow_core::event::provenance::{ExecutionAccounting, FlowContext, RuntimeProvenance};
use obzenflow_core::event::{
    CausalFrontier, ChainEventFactory, ChainPayload, PipelineLifecycleEvent, SystemEvent,
    SystemPayload,
};
use obzenflow_core::id::SystemId;
use obzenflow_core::journal::AppendOptions;
use obzenflow_core::{ChainEvent, FlowId, Journal, JournalOwner, StageId, TypedPayload};
use obzenflow_dsl::{flow, sink, source, FlowDefinition};
use obzenflow_infra::application::{FlowApplication, LogLevel};
use obzenflow_infra::journal::{disk_journals, DiskJournal};
use obzenflow_runtime::stages::common::handlers::TypedFiniteSourceHandler;
use obzenflow_runtime::stages::sink::SinkTyped;
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::runtime::Runtime;

#[derive(Clone, Serialize, Deserialize)]
struct Tick {
    n: u64,
    body: String,
}
impl TypedPayload for Tick {
    const EVENT_TYPE: &'static str = "validation_benchmark.tick";
}
#[derive(Clone)]
struct Ticks {
    next: u64,
    count: u64,
}
impl TypedFiniteSourceHandler for Ticks {
    type Output = Tick;
    fn next(&mut self) -> Result<Option<Vec<Tick>>, SourceError> {
        if self.next == self.count {
            return Ok(None);
        }
        let end = (self.next + 500).min(self.count);
        let rows = (self.next..end)
            .map(|n| Tick {
                n,
                body: "x".repeat(256),
            })
            .collect();
        self.next = end;
        Ok(Some(rows))
    }
}
fn definition(base: PathBuf, count: u64) -> FlowDefinition {
    FlowDefinition::materialize(move |_| {
        let input = Ticks { next: 0, count };
        let output = SinkTyped::with_delivery(
            |_: Tick, _: obzenflow_runtime::stages::sink::DeliveryContext| std::future::ready(()),
        )
        .idempotent();
        Ok(flow! {
            name: "validation_benchmark",
            journals: disk_journals(base),
            stages: {
                ticks = source!(Tick => input);
                out = sink!(Tick => output);
            },
            topology: { ticks |> out; }
        })
    })
}

fn latest(base: &Path) -> PathBuf {
    let mut runs: Vec<_> = std::fs::read_dir(base.join("flows"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.join("run_manifest.json").is_file())
        .collect();
    runs.sort();
    runs.pop().expect("completed run archive")
}

pub struct Archive {
    pub baseline: PathBuf,
    pub replay: Option<PathBuf>,
    pub records: Vec<serde_json::Value>,
    pub inputs: u64,
    pub directory: tempfile::TempDir,
}
impl Archive {
    pub fn build(runtime: &Runtime, inputs: u64, replay: bool) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let base = directory.path().join("journals");
        let config = directory.path().join("obzenflow.toml");
        std::fs::write(
            &config,
            "[server]\nenabled = false\n[metrics]\nenabled = false\n",
        )
        .unwrap();
        runtime.block_on(async {
            tokio::time::timeout(
                super::DEADLINE,
                FlowApplication::builder()
                    .with_log_level(LogLevel::Warn)
                    .with_config_file(&config)
                    .with_cli_args(["validation-benchmark"])
                    .run_async(definition(base.clone(), inputs)),
            )
            .await
            .expect("archive fixture live watchdog")
            .unwrap();
        });
        let baseline = latest(&base);
        let replay = replay.then(|| {
            runtime.block_on(async {
                tokio::time::timeout(
                    super::DEADLINE,
                    FlowApplication::builder()
                        .with_log_level(LogLevel::Warn)
                        .with_config_file(&config)
                        .with_cli_args(vec![
                            OsString::from("validation-benchmark"),
                            OsString::from("--replay-from"),
                            baseline.as_os_str().to_owned(),
                        ])
                        .run_async(definition(base.clone(), inputs)),
                )
                .await
                .expect("archive fixture replay watchdog")
                .unwrap();
            });
            let candidate = latest(&base);
            assert_ne!(baseline, candidate);
            candidate
        });
        use obzenflow_core::journal::read::{RunRecordData, RunRecordKind};
        let read = runtime.block_on(Self::read(&baseline));
        let source: Vec<_> = read
            .iter()
            .filter(|row| {
                row.kind == RunRecordKind::SourceFact
                    && row
                        .journal
                        .stage
                        .as_ref()
                        .is_some_and(|stage| stage.key == "ticks")
            })
            .collect();
        assert_eq!(
            source.len(),
            inputs as usize,
            "fixture commits every source fact"
        );
        for (index, row) in source.into_iter().enumerate() {
            let RunRecordData::Chain(record) = &row.record else {
                panic!("source fact must be a chain record");
            };
            let ChainPayload::Fact(payload) = &record.payload else {
                panic!("source fact payload");
            };
            assert_eq!(payload["n"], index as u64, "source identity/order");
            assert_eq!(payload["body"].as_str().unwrap().len(), 256);
        }
        assert_eq!(
            read.iter()
                .filter(|row| row.kind == RunRecordKind::Delivery
                    && row
                        .journal
                        .stage
                        .as_ref()
                        .is_some_and(|stage| stage.key == "out"))
                .count(),
            inputs as usize,
            "fixture settles every sink delivery"
        );
        let records: Vec<_> = read
            .into_iter()
            .map(|row| serde_json::to_value(row.record).unwrap())
            .collect();
        Self {
            baseline,
            replay,
            records,
            inputs,
            directory,
        }
    }

    pub async fn read(path: &Path) -> Vec<obzenflow_core::journal::read::RunRecord> {
        let mut reader = obzenflow_infra::journal::read::open_disk_run(path)
            .await
            .unwrap();
        let mut records = Vec::new();
        while let Some(row) = reader.next().await.unwrap() {
            records.push(row);
        }
        records
    }
}

pub struct ObserverJournals {
    pub stage: StageId,
    pub data: Arc<dyn Journal<ChainEvent>>,
    pub error: Arc<dyn Journal<ChainEvent>>,
    pub system: Arc<dyn Journal<SystemEvent>>,
    pub inputs: usize,
    pub _directory: tempfile::TempDir,
}
impl ObserverJournals {
    pub async fn build(inputs: usize) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let stage = StageId::new();
        let system_id = SystemId::new();
        let flow = FlowId::new();
        let data: Arc<dyn Journal<ChainEvent>> = Arc::new(
            DiskJournal::with_owner_in_run(
                directory.path().join("data.log"),
                JournalOwner::stage(stage),
                flow,
            )
            .unwrap(),
        );
        let error: Arc<dyn Journal<ChainEvent>> = Arc::new(
            DiskJournal::with_owner_in_run(
                directory.path().join("error.log"),
                JournalOwner::stage(stage),
                flow,
            )
            .unwrap(),
        );
        let system: Arc<dyn Journal<SystemEvent>> = Arc::new(
            DiskJournal::with_owner_in_run(
                directory.path().join("system.log"),
                JournalOwner::system(system_id),
                flow,
            )
            .unwrap(),
        );
        system
            .append(
                SystemEvent::new(
                    system_id.into(),
                    SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Running {
                        stage_count: Some(1),
                    }),
                ),
                Default::default(),
            )
            .await
            .unwrap();
        let lifecycle = |fact| {
            ChainEventFactory::execution_event(stage.into(), ExecutionPayload::StageLifecycle(fact))
                .with_flow_context(FlowContext::new("observed", stage))
        };
        data.append(
            lifecycle(StageLifecycleFact::Running { stage_id: stage }),
            Default::default(),
        )
        .await
        .unwrap();
        for n in 0..inputs {
            data.append(
                ChainEventFactory::data_event(
                    stage.into(),
                    "observer.business",
                    std::num::NonZeroU32::MIN,
                    serde_json::json!({"n":n,"body":"x".repeat(256)}),
                ),
                Default::default(),
            )
            .await
            .unwrap();
        }
        let accounting = ExecutionAccounting {
            events_processed_total: inputs as u64,
            events_emitted_total: inputs as u64,
            errors_total: 2,
            ..Default::default()
        };
        let complete = data
            .append(
                lifecycle(StageLifecycleFact::Completed {
                    stage_id: stage,
                    accounting: Some(accounting.clone()),
                })
                .with_runtime_provenance(RuntimeProvenance {
                    accounting: accounting.clone(),
                }),
                Default::default(),
            )
            .await
            .unwrap();
        let mut error_event = ChainEventFactory::data_event(
            stage.into(),
            "observer.error",
            std::num::NonZeroU32::MIN,
            serde_json::json!({"kind":"fixture"}),
        )
        .with_runtime_provenance(RuntimeProvenance {
            accounting: ExecutionAccounting {
                errors_total: 4,
                ..accounting
            },
        });
        error_event.flow_context.stage_id = stage;
        let failed = error.append(error_event, Default::default()).await.unwrap();
        let mut frontier = CausalFrontier::from_record(&complete).unwrap();
        frontier
            .merge(&CausalFrontier::from_record(&failed).unwrap())
            .unwrap();
        system
            .append(
                SystemEvent::new(
                    system_id.into(),
                    SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Drained),
                ),
                AppendOptions::new(frontier),
            )
            .await
            .unwrap();
        assert_eq!(data.committed_position().await.unwrap(), inputs as u64 + 2);
        assert_eq!(error.committed_position().await.unwrap(), 1);
        assert_eq!(system.committed_position().await.unwrap(), 2);
        Self {
            stage,
            data,
            error,
            system,
            inputs,
            _directory: directory,
        }
    }
}
