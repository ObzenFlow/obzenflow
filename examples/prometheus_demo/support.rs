// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The demo's flow and its typed events, shared with the journal integration
//! test that runs the same flow under `cargo test`.

use obzenflow::flow::{flow, sink, source, stateful, transform, FlowDefinition};
use obzenflow::journal::disk_journals;
use obzenflow::middleware::{circuit_breaker, rate_limit};
use obzenflow::schema::TypedPayload;
use obzenflow::stages::sinks::ConsoleSink;
use obzenflow::stages::sources::TypedFiniteSourceHandler;
use obzenflow::stages::sources::{SourceDiagnosticReason, SourceError};
use obzenflow::stages::stateful::emit;
use obzenflow::stages::transforms::TryMapTyped;
use obzenflow::stages::{stateful, transforms};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

pub(crate) const SOURCE_OUTAGE_INTERVAL: usize = 20_000;
const SOURCE_BREAKER_COOLDOWN: Duration = Duration::from_secs(5);

// ============================================================================
// FLOWIP-082a: Strongly-Typed Domain Events
// ============================================================================

/// Data request event from the source
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DataRequest {
    id: usize,
    should_fail: bool,
    batch: usize,
}

impl TypedPayload for DataRequest {
    const EVENT_TYPE: &'static str = "data.request";
    const SCHEMA_VERSION: u32 = 1;
}

/// Simulates an input service that periodically becomes unavailable. A failed
/// poll does not consume an input: the breaker waits, probes once unsuccessfully,
/// then recovers on its next probe and emits the same pending input.
#[derive(Clone, Debug)]
struct HighVolumeSource {
    next_id: usize,
    total_events: usize,
    outage_interval: usize,
    outage_failures: u8,
}

impl TypedFiniteSourceHandler for HighVolumeSource {
    type Output = DataRequest;

    fn next(&mut self) -> Result<Option<Vec<DataRequest>>, SourceError> {
        if self.next_id >= self.total_events {
            println!(
                "🏁 Source complete: Generated {} total events",
                self.next_id
            );
            return Ok(None);
        }

        if self.next_id > 0
            && self.next_id.is_multiple_of(self.outage_interval)
            && self.outage_failures < 2
        {
            self.outage_failures += 1;
            tracing::warn!(
                next_id = self.next_id,
                attempt = self.outage_failures,
                "Simulated source outage: circuit breaker will wait five seconds before probing"
            );
            return Err(SourceError::Timeout(
                SourceDiagnosticReason::TimedOut.into(),
            ));
        }

        self.outage_failures = 0;
        let current_id = self.next_id;
        self.next_id += 1;
        if self.next_id.is_multiple_of(10_000) {
            println!("📊 Generated {} events...", self.next_id);
        }
        Ok(Some(vec![DataRequest {
            id: current_id,
            should_fail: current_id.is_multiple_of(100),
            batch: current_id / 100,
        }]))
    }
}

/// Successfully processed event from the error-prone transform
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProcessedEvent {
    id: usize,
    should_fail: bool,
    batch: usize,
    processed: bool,
    processing_stage: String,
}

impl TypedPayload for ProcessedEvent {
    const EVENT_TYPE: &'static str = "processed.event";
    const SCHEMA_VERSION: u32 = 1;
}

// ============================================================================
// FLOWIP-114b: Typed transform helper for the error-prone stage
// ============================================================================

/// Transform that can fail on certain events.
///
/// Returns `TryMapTyped<DataRequest, ProcessedEvent, String, _>`, which both
/// satisfies `TransformTyping<Input = DataRequest, Output = ProcessedEvent>`
/// for the typed `transform!` macro. The transform supervisor owns terminal
/// error marking and error-journal routing.
fn error_prone_transform() -> TryMapTyped<
    DataRequest,
    ProcessedEvent,
    String,
    impl Fn(DataRequest) -> Result<ProcessedEvent, String> + Send + Sync + Clone,
> {
    transforms::try_map(|req: DataRequest| {
        if req.should_fail {
            Err("Simulated processing error".to_string())
        } else {
            Ok(ProcessedEvent {
                id: req.id,
                should_fail: req.should_fail,
                batch: req.batch,
                processed: true,
                processing_stage: "error_prone_transform".to_string(),
            })
        }
    })
}

// ============================================================================
// FLOWIP-080j: ReduceTyped for Type-Safe Event Counting
// ============================================================================

/// State for business-level event counting (FLOWIP-080j)
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct EventCountState {
    event_count: usize,
}

impl TypedPayload for EventCountState {
    const EVENT_TYPE: &'static str = "prometheus.event_count";
    const SCHEMA_VERSION: u32 = 1;
}

/// The example's flow, also exercised by its journal integration test.
pub(crate) fn flow_definition(total_events: usize, journal_root: PathBuf) -> FlowDefinition {
    flow_definition_with_outage_interval(total_events, journal_root, SOURCE_OUTAGE_INTERVAL)
}

/// The same demo with a shorter outage interval for bounded integration proofs.
/// The breaker duration, middleware, and business path stay intact.
pub(crate) fn flow_definition_with_outage_interval(
    total_events: usize,
    journal_root: PathBuf,
    outage_interval: usize,
) -> FlowDefinition {
    flow_definition_with_options(total_events, journal_root, outage_interval, true)
}

pub(crate) fn flow_definition_with_options(
    total_events: usize,
    journal_root: PathBuf,
    outage_interval: usize,
    source_rate_limit: bool,
) -> FlowDefinition {
    assert!(
        outage_interval > 0,
        "source outage interval must be positive"
    );
    FlowDefinition::materialize(move |_runtime_config| {
        let high_volume_source_handler = HighVolumeSource {
            next_id: 0,
            total_events,
            outage_interval,
            outage_failures: 0,
        };
        let error_processor_handler = error_prone_transform();
        let event_counter_handler = stateful::reduce(
            EventCountState::default(),
            |state: &mut EventCountState, _event: &ProcessedEvent| {
                state.event_count += 1;
                if state.event_count.is_multiple_of(10_000) {
                    println!("📊 Counted {} events so far...", state.event_count);
                }
            },
        )
        .emit(emit::on_eof);
        let summary_sink_handler = ConsoleSink::new(format_summary);

        Ok(flow! {
            name: "prometheus_demo",
            journals: disk_journals(journal_root),

            stages: {
                // The simulated service outage belongs at the live source boundary.
                // Pure transform errors still follow their existing error-journal path.
                high_volume_source = if source_rate_limit {
                    source!(DataRequest with {
                        circuit_breaker()
                            .consecutive_failures(1)
                            .open_for(SOURCE_BREAKER_COOLDOWN)
                            .probes(1),
                        rate_limit(1000.0)
                    } => high_volume_source_handler)
                } else {
                    source!(DataRequest with {
                        circuit_breaker()
                            .consecutive_failures(1)
                            .open_for(SOURCE_BREAKER_COOLDOWN)
                            .probes(1)
                    } => high_volume_source_handler)
                };
                error_processor = transform!(DataRequest -> ProcessedEvent => error_processor_handler);
                event_counter = stateful!(ProcessedEvent -> EventCountState => event_counter_handler);
                summary_sink = sink!(EventCountState => summary_sink_handler);
            },

            topology: {
                high_volume_source |> error_processor;
                error_processor |> event_counter;
                event_counter |> summary_sink;
            }
        })
    })
}

pub(crate) fn format_summary(summary: &EventCountState) -> String {
    let count = summary.event_count;
    format!(
        concat!(
            "\n=====================================\n",
            "📊 Processing summary:\n",
            "   Successfully processed: {count} events\n",
            "====================================="
        ),
        count = count
    )
}
