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
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::payloads::observability_payload::{
    IndicatorSample, MiddlewareLifecycle, ObservabilityPayload,
};
use obzenflow_core::event::ChainEventContent;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::Journal;
use obzenflow_core::{StageId, TypedPayload};
use obzenflow_dsl::{flow, sink, source, transform, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::journal::DiskJournal;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{SinkHandler, TypedFiniteSourceHandler};
use obzenflow_runtime::stages::transform::MapTyped;
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
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
}
impl OrderSource {
    fn new(count: usize) -> Self {
        Self { remaining: count }
    }
}
impl TypedFiniteSourceHandler for OrderSource {
    type Output = Order;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        Ok(Some(vec![Order {
            id: self.remaining as u64 + 1,
        }]))
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

fn logging_rows(events: &[ChainEvent]) -> Vec<&serde_json::Value> {
    events
        .iter()
        .filter_map(|event| match &event.content {
            ChainEventContent::Observability(ObservabilityPayload::Middleware(
                MiddlewareLifecycle::User(user),
            )) if user.event_type == "obzenflow.logging" => Some(&user.payload),
            _ => None,
        })
        .collect()
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
    let flow_definition = FlowDefinition::materialize(move |_runtime_config| {
        let order_source = OrderSource::new(INPUT_COUNT);
        let process_orders = MapTyped::new(|event: Order| Processed { id: event.id });
        let handoff = Handoff;

        Ok(flow! {
            name: "observer_evidence_test",
            journals: disk_journals(journal_dir),

            stages: {
                orders = source!(Order => order_source);
                process = transform!(Order -> Processed => process_orders, observers: [
                    indicator()
                        .operation("checkout.process")
                        .kind(IndicatorKind::Latency)
                        .indicator("process.latency")
                        .tag("dependency", "ledger")
                ]);
                handoff = sink!(Processed => handoff, observers: [
                    log().prefix("handoff")
                ]);
            },

            topology: {
                orders |> process;
                process |> handoff;
            }
        })
    });

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(flow_definition)
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

    // 2. Logging evidence is published once per resolved sink delivery. This
    //    observer has only a post-outcome hook, so its action must describe the
    //    outcome it actually observed.
    let logging_rows = logging_rows(&handoff_events);
    assert_eq!(
        logging_rows.len(),
        INPUT_COUNT,
        "exactly one logging evidence row per sink delivery"
    );
    assert!(logging_rows.iter().all(|payload| {
        payload["action"] == "sink_delivery_observed"
            && payload["details"]["outcome"]["kind"] == "delivered"
            && payload["details"]["stage_input_position"].is_u64()
    }));
    assert!(logging_rows.iter().all(|payload| {
        payload["action"] != "before_sink_delivery" && payload["action"] != "after_sink_delivery"
    }));

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
