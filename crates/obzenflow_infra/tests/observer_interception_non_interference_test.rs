// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115m Part 2: ordinary observer panic quarantine and journal
//! non-interference through a fully materialised flow.

use async_trait::async_trait;
use obzenflow_adapters::middleware::handler_observer;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::event::{ChainEvent, ChainEventContent};
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::Journal;
use obzenflow_core::{StageId, TypedPayload};
use obzenflow_dsl::{flow, sink, source, transform, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::{disk_journals, DiskJournal};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, SinkDescription, SinkTerminalOutcome, SinkWriteContext, SinkWriteReport,
    TypedFiniteSourceHandler,
};
use obzenflow_runtime::stages::observer::{HandlerObserver, HandlerObserverContext};
use obzenflow_runtime::stages::transform::MapTyped;
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use uuid::Uuid;

const INPUT_COUNT: usize = 4;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Input {
    id: usize,
}

impl TypedPayload for Input {
    const EVENT_TYPE: &'static str = "observer_interception.input";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Output {
    id: usize,
}

impl TypedPayload for Output {
    const EVENT_TYPE: &'static str = "observer_interception.output";
}

#[derive(Clone, Debug)]
struct InputSource {
    next: usize,
}

impl TypedFiniteSourceHandler for InputSource {
    type Output = Input;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.next == INPUT_COUNT {
            return Ok(None);
        }
        let id = self.next;
        self.next += 1;
        Ok(Some(vec![Input { id }]))
    }
}

#[derive(Clone)]
struct PanickingObserver {
    calls: Arc<AtomicUsize>,
}

impl HandlerObserver for PanickingObserver {
    fn after_handle(&self, _ctx: &HandlerObserverContext<'_>, _outputs: &[ChainEvent]) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        panic!("intentional observer unwind");
    }
}

#[derive(Clone)]
struct CountingObserver {
    calls: Arc<AtomicUsize>,
    outputs: Arc<AtomicUsize>,
}

impl HandlerObserver for CountingObserver {
    fn after_handle(&self, _ctx: &HandlerObserverContext<'_>, outputs: &[ChainEvent]) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.outputs.fetch_add(
            outputs.iter().filter(|event| event.is_data()).count(),
            Ordering::SeqCst,
        );
    }
}

#[derive(Clone, Debug)]
struct CountingSink {
    deliveries: Arc<AtomicUsize>,
}

#[async_trait]
impl InlineSink for CountingSink {
    type Input = Output;

    fn describe(&self) -> SinkDescription {
        SinkDescription::unspecified()
    }

    async fn write(
        &mut self,
        _input: Output,
        _context: SinkWriteContext,
    ) -> Result<SinkWriteReport, HandlerError> {
        self.deliveries.fetch_add(1, Ordering::SeqCst);
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            DeliveryMethod::Custom("observer-test".to_string()),
            None,
        )))
    }
}

fn flow_dir(base: &Path) -> PathBuf {
    std::fs::read_dir(base.join("flows"))
        .expect("flows directory exists")
        .flatten()
        .map(|entry| entry.path())
        .find(|path| path.is_dir())
        .expect("one flow directory exists")
}

fn stage_log(flow_dir: &Path, prefix: &str) -> PathBuf {
    std::fs::read_dir(flow_dir)
        .expect("flow directory is readable")
        .flatten()
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(prefix) && name.ends_with(".log"))
        })
        .unwrap_or_else(|| panic!("no stage log beginning with {prefix:?}"))
}

fn build_flow(
    journal_root: PathBuf,
    attach_observers: bool,
    panicking_calls: Arc<AtomicUsize>,
    sibling_calls: Arc<AtomicUsize>,
    sibling_outputs: Arc<AtomicUsize>,
    deliveries: Arc<AtomicUsize>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let source = InputSource { next: 0 };
        let map = MapTyped::new(|input: Input| Output { id: input.id });
        let sink = CountingSink { deliveries };
        let observed = if attach_observers {
            transform!(Input -> Output => map, observers: [
                handler_observer(
                    "panicking-observer",
                    PanickingObserver { calls: panicking_calls }
                ),
                handler_observer(
                    "counting-sibling",
                    CountingObserver {
                        calls: sibling_calls,
                        outputs: sibling_outputs,
                    }
                )
            ])
        } else {
            transform!(Input -> Output => map)
        };

        Ok(flow! {
            name: "observer_interception_non_interference",
            journals: disk_journals(journal_root),

            stages: {
                input = source!(Input => source);
                observed = observed;
                output = sink!(Output => sink, delivery: idempotent);
            },

            topology: {
                input |> observed;
                observed |> output;
            }
        })
    })
}

async fn transform_outputs(journal_root: &Path) -> Vec<Output> {
    let log = stage_log(&flow_dir(journal_root), "Transform_observed_stage_");
    let journal = DiskJournal::with_owner(log, JournalOwner::stage(StageId::new()))
        .expect("open observed-stage journal");
    journal
        .read_causally_ordered()
        .await
        .expect("read observed-stage journal")
        .iter()
        .filter_map(|envelope| Output::from_event(&envelope.event))
        .collect()
}

#[tokio::test]
async fn observer_panic_is_quarantined_without_changing_business_or_journal_results() {
    let root = PathBuf::from(format!(
        "target/observer-interception-non-interference-{}",
        Uuid::new_v4()
    ));
    let control_root = root.join("control");
    let observed_root = root.join("observed");
    let replay_root = root.join("replay");

    let control_deliveries = Arc::new(AtomicUsize::new(0));
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            control_root.clone(),
            false,
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            control_deliveries.clone(),
        ))
        .await
        .expect("control flow completes");

    let panicking_calls = Arc::new(AtomicUsize::new(0));
    let sibling_calls = Arc::new(AtomicUsize::new(0));
    let sibling_outputs = Arc::new(AtomicUsize::new(0));
    let deliveries = Arc::new(AtomicUsize::new(0));
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            observed_root.clone(),
            true,
            panicking_calls.clone(),
            sibling_calls.clone(),
            sibling_outputs.clone(),
            deliveries.clone(),
        ))
        .await
        .expect("observer panic must not fail the flow");

    assert_eq!(control_deliveries.load(Ordering::SeqCst), INPUT_COUNT);
    assert_eq!(panicking_calls.load(Ordering::SeqCst), 1);
    assert_eq!(sibling_calls.load(Ordering::SeqCst), INPUT_COUNT);
    assert_eq!(sibling_outputs.load(Ordering::SeqCst), INPUT_COUNT);
    assert_eq!(deliveries.load(Ordering::SeqCst), INPUT_COUNT);

    assert_eq!(
        transform_outputs(&observed_root).await,
        transform_outputs(&control_root).await,
        "observer attachment must preserve the normalised domain projection"
    );

    let log = stage_log(&flow_dir(&observed_root), "Transform_observed_stage_");
    let journal = DiskJournal::with_owner(log, JournalOwner::stage(StageId::new()))
        .expect("open observed-stage journal");
    let events = journal
        .read_causally_ordered()
        .await
        .expect("read observed-stage journal");
    assert_eq!(
        events
            .iter()
            .filter(|envelope| Output::from_event(&envelope.event).is_some())
            .count(),
        INPUT_COUNT
    );
    assert!(events.iter().all(|envelope| !matches!(
        &envelope.event.content,
        ChainEventContent::Observability(
            obzenflow_core::event::payloads::observability_payload::ObservabilityPayload::Middleware(_)
        )
    )));

    let archive = flow_dir(&observed_root);
    let replay_panicking_calls = Arc::new(AtomicUsize::new(0));
    let replay_sibling_calls = Arc::new(AtomicUsize::new(0));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            archive.as_os_str().to_os_string(),
        ])
        .run_async(build_flow(
            replay_root,
            true,
            replay_panicking_calls.clone(),
            replay_sibling_calls.clone(),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
        ))
        .await
        .expect("strict replay completes without observer dispatch");

    assert_eq!(replay_panicking_calls.load(Ordering::SeqCst), 0);
    assert_eq!(replay_sibling_calls.load(Ordering::SeqCst), 0);

    std::fs::remove_dir_all(root).ok();
}
