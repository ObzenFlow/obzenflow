// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::journal::disk_journals;
use async_trait::async_trait;
use futures::future::BoxFuture;
use obzenflow_adapters::sources::ValuesSource;
use obzenflow_dsl::{flow, sink, source};
use obzenflow_runtime::events::{source_channel, Events, IngressDecoder};
use obzenflow_runtime::pipeline::PipelineState;
use obzenflow_runtime::queries::{FiniteRead, QueryError};
use obzenflow_runtime::stages::common::handlers::source::{Continuous, SourceRead};
use obzenflow_runtime::stages::common::handlers::{
    TypedFiniteSourceHandler, TypedInfiniteSourceHandler,
};
use obzenflow_runtime::systems::Port;
use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use tokio::sync::oneshot;

#[cfg(all(feature = "tokio-console", tokio_unstable))]
mod console_telemetry;
mod lifecycle;
#[cfg(feature = "prometheus")]
mod prometheus;
mod startup;

#[cfg(unix)]
#[tokio::test]
async fn verify_quoted_paths_preserve_arguments() {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;
    use std::process::Command;

    let dir = tempfile::tempdir().unwrap();
    let baseline = dir.path().join("Pat's Payment Runs/base");
    let candidate = dir
        .path()
        .join(OsStr::from_bytes(b"Pat's Payment Runs/replay-\xff"));
    let error =
        FlowApplication::run_post_replay_verification(baseline.clone(), Some(candidate.clone()))
            .await
            .unwrap_err();
    let ApplicationError::Verification {
        exit_code: 3,
        summary,
    } = error
    else {
        panic!("missing baseline must refuse verification: {error}");
    };
    let (_, operands) = summary.split_once("then run obzenflow verify ").unwrap();
    let output = Command::new("sh")
        .args(["-c", &format!(r"printf '%s\0' {operands}")])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let args: Vec<_> = output
        .stdout
        .strip_suffix(&[0])
        .unwrap()
        .split(|byte| *byte == 0)
        .collect();
    assert_eq!(
        args,
        [
            b"--baseline".as_slice(),
            baseline.as_os_str().as_bytes(),
            b"--candidate",
            candidate.as_os_str().as_bytes()
        ]
    );
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct IdlePayload;

impl TypedPayload for IdlePayload {
    const EVENT_TYPE: &'static str = "flow_application.idle";
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct NextInput;

#[obzenflow_runtime::effects::port]
#[async_trait]
trait TestInput {
    #[query(type = "flow_application.input.read")]
    async fn read(
        &self,
        from: &NextInput,
        ctx: &obzenflow_runtime::queries::QueryContext,
    ) -> Result<FiniteRead<IdlePayload, NextInput>, obzenflow_runtime::queries::QueryError>;
}

type Answer = Result<FiniteRead<IdlePayload, NextInput>, QueryError>;
type Answering = Arc<dyn Fn() -> BoxFuture<'static, Answer> + Send + Sync>;

struct ScriptedInput {
    answer: Answering,
}

#[async_trait]
impl TestInput for ScriptedInput {
    async fn read(
        &self,
        _from: &NextInput,
        _ctx: &obzenflow_runtime::queries::QueryContext,
    ) -> Answer {
        (self.answer)().await
    }
}

struct FiniteInput(Port<dyn TestInput>);

impl FiniteInput {
    fn new(ends: bool, reads: Arc<AtomicUsize>) -> Self {
        Self::answering(move || {
            reads.fetch_add(1, Ordering::SeqCst);
            if ends {
                Box::pin(async { Ok(FiniteRead::EndOfInput) })
            } else {
                Box::pin(std::future::pending())
            }
        })
    }

    fn waiting() -> Self {
        Self::new(false, Arc::default())
    }

    fn answering(answer: impl Fn() -> BoxFuture<'static, Answer> + Send + Sync + 'static) -> Self {
        Self(Port::to(ScriptedInput {
            answer: Arc::new(answer),
        }))
    }
}

impl TypedFiniteSourceHandler for FiniteInput {
    type Output = IdlePayload;

    fn read(&self) -> SourceRead<IdlePayload> {
        SourceRead::finite(self.0.clone(), NextInput, Ok)
    }
}

struct IdleInfiniteSource {
    port: Port<dyn Events>,
}

#[derive(Clone)]
struct IdleIngress;

impl IngressDecoder for IdleIngress {
    type Output = IdlePayload;
}

impl IdleInfiniteSource {
    fn new() -> Self {
        let (sender, receiver) = source_channel::<IdlePayload>(1);
        std::mem::forget(sender);
        Self {
            port: Port::to(receiver),
        }
    }
}

impl TypedInfiniteSourceHandler for IdleInfiniteSource {
    type Output = IdlePayload;

    fn read(&self) -> SourceRead<IdlePayload, Continuous> {
        SourceRead::events(self.port.clone(), IdleIngress)
    }
}

fn one_shot() -> ValuesSource<[IdlePayload; 1]> {
    ValuesSource::new([IdlePayload])
}

fn noop_sink() -> obzenflow_adapters::sinks::DiscardSink<IdlePayload> {
    obzenflow_adapters::sinks::DiscardSink::new()
}

async fn recorded_source(
    dir: &std::path::Path,
    lifecycle: impl FnOnce(
        &obzenflow_core::event::SystemEventFactory,
    ) -> Vec<obzenflow_core::event::SystemEvent>,
) {
    use obzenflow_core::journal::archive::manifest::{
        RunManifest, EFFECT_BINDING_DESCRIPTOR_CAPABILITY, JOURNAL_SCHEMA_VERSION,
        OBSERVABILITY_CAPTURE_CAPABILITY, RUN_MANIFEST_FILENAME,
    };
    use obzenflow_core::{FlowId, Journal, JournalOwner, SystemId};
    let system = SystemId::new();
    let flow = FlowId::new();
    let journal =
        crate::journal::DiskJournal::<obzenflow_core::event::SystemEvent>::with_owner_in_run(
            dir.join("system.log"),
            JournalOwner::system(system),
            flow,
            obzenflow_runtime::runtime_config::JournalSync::Off,
        )
        .unwrap();
    for event in lifecycle(&obzenflow_core::event::SystemEventFactory::new(system)) {
        journal.append(event, Default::default()).await.unwrap();
    }
    drop(journal);
    let manifest = RunManifest {
        journal_schema_version: JOURNAL_SCHEMA_VERSION.into(),
        obzenflow_version: env!("CARGO_PKG_VERSION").into(),
        flow_id: flow.to_string(),
        recorded_flow_id: None,
        pipeline_writer_id: system.into(),
        flow_name: "flow".into(),
        created_at: chrono::Utc::now(),
        replay: None,
        resume: None,
        interrupted_appends: Vec::new(),
        stages: Default::default(),
        system_journal_file: "system.log".into(),
        metrics_journals: None,
        effective_config: None,
        capabilities: [
            (OBSERVABILITY_CAPTURE_CAPABILITY.into(), 1),
            (EFFECT_BINDING_DESCRIPTOR_CAPABILITY.into(), 1),
        ]
        .into(),
        bounded_direct_fact_admission: Vec::new(),
        cycle_members: Vec::new(),
    };
    std::fs::write(
        dir.join(RUN_MANIFEST_FILENAME),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
}

fn completed(
    factory: &obzenflow_core::event::SystemEventFactory,
) -> obzenflow_core::event::SystemEvent {
    factory.pipeline_completed(
        obzenflow_core::event::types::DurationMs(1),
        obzenflow_core::metrics::FlowLifecycleMetricsSnapshot {
            events_in_total: 0,
            events_out_total: 0,
            errors_total: 0,
        },
    )
}

fn replay_of(source: &std::path::Path) -> crate::application::RunMode {
    crate::application::RunMode::Replay(crate::application::ReplayRunContext {
        archive_path: source.to_path_buf(),
        archive_flow_id: None,
        verify: false,
    })
}

#[tokio::test]
async fn replay_footer_sealed_source_offers_verify() {
    let dir = tempfile::tempdir().unwrap();
    recorded_source(dir.path(), |factory| {
        vec![factory.pipeline_starting(), completed(factory)]
    })
    .await;
    assert!(replay_verify_offered(&replay_of(dir.path())));
    let footer = footer_outcome(
        Some(obzenflow_core::journal::ArchiveStatus::Completed),
        "flow".into(),
        Some(CurrentRunLocator::new(std::path::PathBuf::from(
            "tmp/replay",
        ))),
        replay_of(dir.path()),
        ArchiveOffers {
            replay: true,
            resume: false,
            verify: true,
        },
    )
    .footer();
    assert!(
        footer.contains(&format!(
            "Verify it against the source archive: re-run with --replay-from {} --verify",
            shell_path(dir.path())
        )),
        "{footer}"
    );
}

#[tokio::test]
async fn replay_footer_unsealed_source_omits_verify() {
    for lifecycle in [
        |factory: &obzenflow_core::event::SystemEventFactory| vec![factory.pipeline_starting()],
        |factory: &obzenflow_core::event::SystemEventFactory| vec![factory.pipeline_not_started()],
    ] {
        let dir = tempfile::tempdir().unwrap();
        recorded_source(dir.path(), lifecycle).await;
        assert!(!replay_verify_offered(&replay_of(dir.path())));
    }
    let missing = tempfile::tempdir().unwrap();
    assert!(!replay_verify_offered(&replay_of(
        &missing.path().join("gone")
    )));
}

#[tokio::test]
async fn replay_footer_comparison_pending_omits_verify() {
    let dir = tempfile::tempdir().unwrap();
    recorded_source(dir.path(), |factory| {
        vec![factory.pipeline_starting(), completed(factory)]
    })
    .await;
    let verifying = crate::application::RunMode::Replay(crate::application::ReplayRunContext {
        archive_path: dir.path().to_path_buf(),
        archive_flow_id: None,
        verify: true,
    });
    assert!(!replay_verify_offered(&verifying));
}

#[test]
fn footer_unknown_status_reports_stopped() {
    use obzenflow_core::journal::ArchiveStatus;
    let first_line = |status| {
        footer_outcome(
            status,
            "flow".into(),
            None,
            crate::application::RunMode::Live,
            ArchiveOffers::default(),
        )
        .footer()
    };
    assert_eq!(
        first_line(Some(ArchiveStatus::Completed)),
        "flow completed."
    );
    for status in [
        None,
        Some(ArchiveStatus::Unknown),
        Some(ArchiveStatus::NotStarted),
        Some(ArchiveStatus::Cancelled),
        Some(ArchiveStatus::Failed),
    ] {
        assert_eq!(first_line(status), "flow stopped.", "{status:?}");
    }
}
