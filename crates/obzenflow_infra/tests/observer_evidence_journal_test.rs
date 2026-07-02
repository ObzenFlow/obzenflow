// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115f canonical observer-evidence proof (AC 33, proof item 19).
//!
//! This is the automated counterpart to the `payment_gateway_resilience`
//! example: it runs a real flow with a latency `indicator()` on a handler stage
//! and a `log()` observer on a sink, then inspects the stage data journals to
//! prove that
//!
//! * a typed `IndicatorSample` is published once per handler execution, carrying
//!   the raw `value_ms` measurement and its identity, with no objective embedded;
//! * a logging `User` evidence row is published per sink delivery;
//! * neither indicator nor logging evidence is mirrored into `system.log`;
//! * enabling the observers does not change the domain output count.
//!
//! The objective (threshold) and the good/bad evaluation are deliberately not in
//! the wide event: applying a threshold and computing SLOs is a read-side concern
//! (FLOWIP-115l) over these raw samples.

use async_trait::async_trait;
use obzenflow_adapters::middleware::observability::{indicator, log, IndicatorKind};
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::payloads::observability_payload::{
    IndicatorSample, MiddlewareLifecycle, ObservabilityPayload,
};
use obzenflow_core::event::ChainEventContent;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::Journal;
use obzenflow_core::{StageId, TypedPayload, WriterId};
use obzenflow_dsl::{flow, sink, source, transform};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::journal::DiskJournal;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{FiniteSourceHandler, SinkHandler};
use obzenflow_runtime::stages::transform::Map;
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::path::{Path, PathBuf};
use uuid::Uuid;

const INPUT_COUNT: usize = 4;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Order {
    id: u64,
}
impl TypedPayload for Order {
    const EVENT_TYPE: &'static str = "observer_evidence.order";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Processed {
    id: u64,
}
impl TypedPayload for Processed {
    const EVENT_TYPE: &'static str = "observer_evidence.processed";
}

#[derive(Clone, Debug)]
struct OrderSource {
    remaining: usize,
    writer_id: WriterId,
}
impl OrderSource {
    fn new(count: usize) -> Self {
        Self {
            remaining: count,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}
impl FiniteSourceHandler for OrderSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id,
            Order::versioned_event_type(),
            json!({ "id": self.remaining as u64 + 1 }),
        )]))
    }
}

#[derive(Clone, Debug)]
struct Handoff;
#[async_trait]
impl SinkHandler for Handoff {
    async fn consume(&mut self, _event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        Ok(DeliveryPayload::success(
            DeliveryMethod::Custom("handoff".to_string()),
            None,
        ))
    }
}

fn flow_dir(base: &Path) -> PathBuf {
    std::fs::read_dir(base.join("flows"))
        .expect("flows dir exists")
        .flatten()
        .map(|entry| entry.path())
        .find(|path| path.is_dir())
        .expect("one flow directory was created")
}

fn stage_log(flow_dir: &Path, prefix: &str) -> PathBuf {
    std::fs::read_dir(flow_dir)
        .expect("flow dir readable")
        .flatten()
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.starts_with(prefix) && name.ends_with(".log"))
                .unwrap_or(false)
        })
        .unwrap_or_else(|| panic!("no stage log file with prefix `{prefix}` in {flow_dir:?}"))
}

async fn read_events(path: &Path) -> Vec<ChainEvent> {
    let journal = DiskJournal::with_owner(path.to_path_buf(), JournalOwner::stage(StageId::new()))
        .expect("open stage journal");
    journal
        .read_causally_ordered()
        .await
        .expect("read stage journal")
        .into_iter()
        .map(|envelope| envelope.event)
        .collect()
}

fn indicator_samples(events: &[ChainEvent]) -> Vec<IndicatorSample> {
    events
        .iter()
        .filter_map(|event| match &event.content {
            ChainEventContent::Observability(ObservabilityPayload::Middleware(
                MiddlewareLifecycle::Indicator(sample),
            )) => Some(sample.clone()),
            _ => None,
        })
        .collect()
}

fn logging_row_count(events: &[ChainEvent]) -> usize {
    events
        .iter()
        .filter(|event| {
            matches!(
                &event.content,
                ChainEventContent::Observability(ObservabilityPayload::Middleware(
                    MiddlewareLifecycle::User(user),
                )) if user.event_type == "obzenflow.logging"
            )
        })
        .count()
}

fn data_output_count(events: &[ChainEvent], event_type: &str) -> usize {
    events
        .iter()
        .filter(|event| event.is_data() && event.event_type() == event_type)
        .count()
}

#[tokio::test]
async fn observer_evidence_lands_in_journals_without_system_mirror() {
    let base = PathBuf::from(format!("target/observer-evidence-test-{}", Uuid::new_v4()));
    let journal_dir = base.clone();

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(flow! {
            name: "observer_evidence_test",
            journals: disk_journals(journal_dir),
            middleware: [],

            stages: {
                orders = source!(Order => OrderSource::new(INPUT_COUNT));
                process = transform!(Order -> Processed => Map::new(|event| {
                    let id = event.payload()["id"].as_u64().unwrap_or(0);
                    ChainEventFactory::data_event(
                        WriterId::from(StageId::new()),
                        Processed::versioned_event_type(),
                        json!({ "id": id }),
                    )
                }), [
                    indicator()
                        .operation("checkout.process")
                        .kind(IndicatorKind::Latency)
                        .indicator("process.latency")
                        .tag("dependency", "ledger")
                ]);
                handoff = sink!(Processed => Handoff, middleware: [
                    log().prefix("handoff")
                ]);
            },

            topology: {
                orders |> process;
                process |> handoff;
            }
        })
        .await
        .expect("observer evidence flow runs to completion");

    let flow_dir = flow_dir(&base);
    let process_events = read_events(&stage_log(&flow_dir, "Transform_process_stage_")).await;
    let handoff_events = read_events(&stage_log(&flow_dir, "Sink_handoff_stage_")).await;
    // `system.log` is a SystemEvent journal, so inspect it as raw text for the
    // markers unique to these observers rather than decoding it as ChainEvent.
    let system_log = std::fs::read_to_string(flow_dir.join("system.log")).unwrap_or_default();

    // 1. One typed indicator sample per handler execution (no fan-out duplication).
    let samples = indicator_samples(&process_events);
    assert_eq!(
        samples.len(),
        INPUT_COUNT,
        "exactly one indicator sample per processed order"
    );
    for sample in &samples {
        assert_eq!(sample.operation, "checkout.process");
        assert_eq!(sample.indicator, "process.latency");
        assert_eq!(sample.kind, IndicatorKind::Latency);
        assert_eq!(sample.tags.len(), 1);
        assert_eq!(sample.tags[0].key, "dependency");
        // The sample records the raw measurement only: `value_ms` is the SLI
        // input. No objective, threshold, or `met` flag is embedded; the type has
        // no such field, and applying a threshold is read-side (FLOWIP-115l).
    }

    // 2. Logging evidence is published per sink delivery. The logging observer
    //    emits a before/after pair per delivery, so each handoff produces two
    //    journalled `obzenflow.logging` rows.
    assert_eq!(
        logging_row_count(&handoff_events),
        INPUT_COUNT * 2,
        "a before/after logging evidence pair per sink delivery"
    );

    // 3. No indicator or logging evidence mirrors into the system journal.
    assert!(
        !system_log.contains("checkout.process"),
        "indicator samples must not mirror to system.log"
    );
    assert!(
        !system_log.contains("obzenflow.logging"),
        "user logging evidence must not mirror to system.log"
    );

    // 4. Non-interference: the observers do not drop or duplicate domain output.
    assert_eq!(
        data_output_count(&process_events, &Processed::versioned_event_type()),
        INPUT_COUNT,
        "every order produces exactly one processed domain output"
    );

    // 5. FLOWIP-115f regression: with TimingMiddleware deleted, the runtime output
    //    committer still stamps processing_time on stage outputs, from the
    //    instrumentation timer that already measures every stage.
    let all_stamped = process_events
        .iter()
        .filter(|event| event.is_data() && event.event_type() == Processed::versioned_event_type())
        .all(|event| event.processing_info.processing_time.as_nanos() > 0);
    assert!(
        all_stamped,
        "every processed output carries a stamped processing_time"
    );

    std::fs::remove_dir_all(&base).ok();
}
