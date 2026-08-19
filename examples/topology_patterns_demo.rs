// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Demo: Topology Patterns - Homogeneous Fan-In with Content-Based Routing
//! (FLOWIP-080h, recast under FLOWIP-114c).
//!
//! This is the canonical example of **homogeneous fan-in**: three upstream
//! sources of the same `RawDataEvent` type feed a single typed aggregator.
//! The aggregator emits `RawDataEvent` to a router that fans out to three
//! priority-tiered sinks based on content. Every edge carries one type.
//!
//! For the **heterogeneous fan-in** case (three or more sources of different
//! concrete types feeding one downstream via per-branch alignment
//! transforms), see `examples/multi_source_ingest_demo/`.
//!
//! **Reference Example for**: Topology patterns, ETL pipelines, multi-source/sink architectures
//!
//! Key concepts demonstrated:
//! - Fan-in: Multiple sources → single aggregator (homogeneous on `RawDataEvent`)
//! - Fan-out: Single router → multiple sinks
//! - Diamond pattern: Combines both (realistic ETL)
//! - StatefulHandler for aggregation (no Arc<Mutex>)
//! - Independent journal readers (no coordination needed)
//! - Natural backpressure handling
//!
//! **FLOWIP-080h Update**: Replaced 38-line SmartRouter struct with a typed map helper

use anyhow::Result;
use async_trait::async_trait;
use obzenflow::sources;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, stateful, transform, FlowDefinition};
use obzenflow_infra::application::{Banner, FlowApplication, Presentation};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::SinkRedeliverySafety;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, SinkDescription, SinkTerminalOutcome, SinkWriteContext, SinkWriteReport,
    StatefulEmission, TypedStatefulHandler,
};
use obzenflow_runtime::stages::transform::MapTyped;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
struct RawDataEvent {
    source: String,
    id: usize,
    value: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    aggregation: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    route: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    route_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    route_value: Option<i64>,
}

impl TypedPayload for RawDataEvent {
    const EVENT_TYPE: &'static str = "data.raw";
    const SCHEMA_VERSION: u32 = 1;
}

/// State for aggregating data from multiple sources
#[derive(Clone, Debug, Default)]
struct AggregatorState {
    /// Track event count per source
    events_by_source: BTreeMap<String, usize>,
    /// Track total value sum per source
    value_by_source: BTreeMap<String, i64>,
    /// Expected counts for audit
    expected_counts: BTreeMap<String, usize>,
    /// Total events processed
    total_events: usize,
    /// Total output events emitted (for demo-local accounting)
    outputs_emitted: usize,
    /// Current event being processed (for emission)
    current_event: Option<RawDataEvent>,
    /// Flag when EOF has been observed for audit
    eof_seen: bool,
}

/// Stateful aggregator that merges events from multiple upstream sources (FAN-IN)
///
/// This demonstrates fan-in pattern: multiple sources → single aggregator
/// Each source has its own journal reader at independent positions
#[derive(Clone, Debug)]
struct MultiSourceAggregator {
    expected_counts: BTreeMap<String, usize>,
}

impl MultiSourceAggregator {
    fn new() -> Self {
        Self {
            expected_counts: BTreeMap::new(),
        }
    }

    fn with_expected(mut self, expected: BTreeMap<String, usize>) -> Self {
        self.expected_counts = expected;
        self
    }

    fn audit(&self, state: &AggregatorState) {
        for (src, expected) in &state.expected_counts {
            let got = state.events_by_source.get(src).cloned().unwrap_or(0);
            if got != *expected {
                println!("⚠️  AUDIT MISMATCH source={src} expected={expected} got={got}");
            }
        }

        println!();
        println!("╔══════════════════════════════════════╗");
        println!("║           FAN-IN SUMMARY             ║");
        println!("╠══════════════════════════════════════╣");
        println!(
            "║ Total events processed: {:>8}        ║",
            state.total_events
        );
        println!("╠════════════════════════╦═════════════╣");
        println!("║ Source                 ║ Events/Value║");
        println!("╠════════════════════════╬═════════════╣");
        for (source, count) in &state.events_by_source {
            let value = state.value_by_source.get(source).unwrap_or(&0);
            println!("║ {source:<22} ║ {count:>4} events / {value:>4} ║");
        }
        println!("╚════════════════════════╩═════════════╝");

        for (src, expected) in &state.expected_counts {
            let got = state.events_by_source.get(src).cloned().unwrap_or(0);
            if got != *expected {
                panic!(
                    "AUDIT FAILED: source={src} expected={expected} got={got} (eof_seen={})",
                    state.eof_seen
                );
            }
        }
        if state.outputs_emitted != state.total_events {
            panic!(
                "STATEFUL ACCOUNTING FAILED: inputs_observed={} outputs_emitted={} (eof_seen={})",
                state.total_events, state.outputs_emitted, state.eof_seen
            );
        }
    }
}

impl TypedStatefulHandler for MultiSourceAggregator {
    type State = AggregatorState;
    type Input = RawDataEvent;
    type Output = RawDataEvent;

    fn accumulate(&self, state: &mut Self::State, event: RawDataEvent) {
        let source = event.source.clone();
        let value = event.value;

        // Update statistics
        *state.events_by_source.entry(source.clone()).or_insert(0) += 1;
        *state.value_by_source.entry(source.clone()).or_insert(0) += value;
        state.total_events += 1;

        let event_count = *state.events_by_source.get(&source).unwrap();

        println!(
            "[FAN-IN] Aggregator received event from '{}' (#{} from this source, total: {})",
            source, event_count, state.total_events
        );

        // Store the current event for emission
        state.current_event = Some(event);
    }

    fn should_emit(&self, state: &Self::State) -> bool {
        // Emit after every event (immediate enrichment pattern)
        state.current_event.is_some()
    }

    fn emit(
        &self,
        state: &Self::State,
    ) -> Result<StatefulEmission<Self::State, Self::Output>, HandlerError> {
        let mut next_state = state.clone();
        let outputs = if let Some(mut event) = next_state.current_event.take() {
            let source = event.source.clone();

            let event_count = *state.events_by_source.get(&source).unwrap_or(&0);
            let total_value = *state.value_by_source.get(&source).unwrap_or(&0);

            // Enrich event with aggregation stats
            event.aggregation = Some(json!({
                "total_events": state.total_events,
                "source_event_count": event_count,
                "source_total_value": total_value,
                "sources_seen": state.events_by_source.len(),
            }));

            // Demo-local accounting: track outputs emitted so we can ensure
            // that every input observed by this stateful handler results in
            // a corresponding output (for this immediate-enrichment pattern).
            // FLOWIP-090c/090d: Once `StatefulAccountingContract` is available and wired
            // via the contract DSL, this counter/assertion is expected to be removed
            // in favor of a generic accounting contract; see FLOWIP-090d exit criteria.
            next_state.outputs_emitted += 1;

            vec![event]
        } else {
            vec![]
        };
        Ok(StatefulEmission::RetainEpoch {
            next_state,
            outputs,
        })
    }

    fn initial_state(&self) -> Self::State {
        AggregatorState {
            expected_counts: self.expected_counts.clone(),
            eof_seen: false,
            ..AggregatorState::default()
        }
    }

    fn drain(&self, state: &Self::State) -> Result<Vec<Self::Output>, HandlerError> {
        self.audit(state);
        Ok(vec![])
    }
}

// ============================================================================
// FLOWIP-080h: Map Helper for Smart Router
// ============================================================================

/// Router that sends events to different downstream stages based on criteria (FAN-OUT)
///
/// This demonstrates fan-out pattern: single router → multiple sinks
/// Each downstream stage creates its own journal reader
///
/// Replaces 38-line SmartRouter struct with a Map helper (FLOWIP-080h)
fn smart_router(
) -> MapTyped<RawDataEvent, RawDataEvent, impl Fn(RawDataEvent) -> RawDataEvent + Send + Sync + Clone>
{
    MapTyped::new(|mut event: RawDataEvent| {
        // Route to different channels based on value
        let route = if event.value < 30 {
            "low"
        } else if event.value < 50 {
            "medium"
        } else {
            "high"
        };

        println!(
            "[FAN-OUT] Router processing event from '{}' with value {} → route: {route}",
            event.source, event.value
        );

        event.route = Some(route.to_string());
        event.route_source = Some(event.source.clone());
        event.route_value = Some(event.value);
        event
    })
}

/// Sink that processes events for a specific priority
#[derive(Clone, Debug)]
struct PrioritySink {
    name: String,
    route_filter: String,
    event_count: Arc<AtomicUsize>,
    per_source: BTreeMap<String, usize>,
}

impl PrioritySink {
    fn new(name: &str, route_filter: &str, counter: Arc<AtomicUsize>) -> Self {
        Self {
            name: name.to_string(),
            route_filter: route_filter.to_string(),
            event_count: counter,
            per_source: BTreeMap::new(),
        }
    }
}

#[async_trait]
impl InlineSink for PrioritySink {
    type Input = RawDataEvent;

    fn describe(&self) -> SinkDescription {
        SinkDescription::unspecified().with_redelivery_safety(SinkRedeliverySafety::SafeToRepeat)
    }

    async fn write(
        &mut self,
        event: RawDataEvent,
        _context: SinkWriteContext,
    ) -> Result<SinkWriteReport, HandlerError> {
        // Only process events matching our filter
        if event.route.as_deref().unwrap_or("") == self.route_filter {
            let new_total = self.event_count.fetch_add(1, Ordering::Relaxed) + 1;
            *self.per_source.entry(event.source.clone()).or_insert(0) += 1;

            println!(
                "[SINK:{}] Processed event from '{}' (value: {}) - total: {}",
                self.name, event.source, event.value, new_total
            );
        }

        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            DeliveryMethod::Custom("Processed".to_string()),
            Some(1),
        )))
    }
}

fn main() -> Result<()> {
    let journal_path = std::path::PathBuf::from("target/topology_patterns_demo_journal");
    let low_counter = Arc::new(AtomicUsize::new(0));
    let med_counter = Arc::new(AtomicUsize::new(0));
    let high_counter = Arc::new(AtomicUsize::new(0));

    let footer_low = low_counter.clone();
    let footer_med = med_counter.clone();
    let footer_high = high_counter.clone();

    let presentation = Presentation::new(
        Banner::new("Topology Patterns Demo")
            .description("Fan-in, fan-out, and diamond topology patterns.")
            .bullets(
                "Demonstrating",
                [
                    "Fan-in: Multiple sources -> single aggregator",
                    "Fan-out: Single router -> multiple sinks",
                    "Diamond pattern: Realistic ETL topology",
                    "StatefulHandler for aggregation (no Arc<Mutex>)",
                    "Independent journal readers",
                ],
            )
            .section(
                "Topology",
                "kafka_source (5 events)  --┐\napi_source (4 events)    --┼--> aggregator (fan-in)\nfile_source (4 events)   --┘            |\n                                        v\n                                     router\n                                        |\n                      ┌─────────────────┼─────────────────┐\n                      v                 v                 v\n                 low_sink          med_sink          high_sink\n                (value<30)       (30<=value<70)      (value>=70)",
            ),
    )
    .with_footer(move |outcome| {
        let low = footer_low.load(Ordering::Relaxed);
        let med = footer_med.load(Ordering::Relaxed);
        let high = footer_high.load(Ordering::Relaxed);

        outcome
            .into_footer()
            .bullets(
                "Fan-out sink summary",
                [
                    format!("LOW (value<30): {low}"),
                    format!("MEDIUM (30<=value<70): {med}"),
                    format!("HIGH (value>=70): {high}"),
                ],
            )
            .bullets(
                "Key insights",
                [
                    "Fan-in: Aggregator subscribed to 3 upstream journals\n  Each source had an independent journal reader\n  Round-robin reading ensures fairness\n  No special merge primitive is needed",
                    "Fan-out: Each sink created its own journal reader\n  Readers progress independently\n  Natural backpressure means slow sinks do not block fast ones\n  All sinks see all events (broadcast behaviour)",
                    "Diamond pattern combines both\n  Multiple inputs are merged and processed\n  Results are distributed to multiple outputs\n  Common in ETL, event routing, and microservices",
                ],
            )
    });

    // Use FlowApplication for modern pattern
    let low_counter_flow = low_counter.clone();
    let med_counter_flow = med_counter.clone();
    let high_counter_flow = high_counter.clone();

    FlowApplication::builder()
        .with_presentation(presentation)
        .run_blocking(FlowDefinition::materialize(move |_runtime_config| {
            let kafka_source_handler = sources::finite_from_fn({
                let source = "kafka".to_string();
                move |index| {
                    if index >= 5 {
                        return None;
                    }
                    let id = index + 1;
                    Some(RawDataEvent {
                        source: source.clone(),
                        id,
                        value: (id * 20) as i64,
                        aggregation: None,
                        route: None,
                        route_source: None,
                        route_value: None,
                    })
                }
            });
            let api_source_handler = sources::finite_from_fn({
                let source = "api".to_string();
                move |index| {
                    if index >= 4 {
                        return None;
                    }
                    let id = index + 1;
                    Some(RawDataEvent {
                        source: source.clone(),
                        id,
                        value: (id * 20) as i64,
                        aggregation: None,
                        route: None,
                        route_source: None,
                        route_value: None,
                    })
                }
            });
            let file_source_handler = sources::finite_from_fn({
                let source = "file".to_string();
                move |index| {
                    if index >= 4 {
                        return None;
                    }
                    let id = index + 1;
                    Some(RawDataEvent {
                        source: source.clone(),
                        id,
                        value: (id * 20) as i64,
                        aggregation: None,
                        route: None,
                        route_source: None,
                        route_value: None,
                    })
                }
            });
            let aggregator_handler = MultiSourceAggregator::new().with_expected({
                let mut m = BTreeMap::new();
                m.insert("kafka".to_string(), 5);
                m.insert("api".to_string(), 4);
                m.insert("file".to_string(), 4);
                m
            });
            let router_handler = smart_router();
            let low_sink_handler = PrioritySink::new("LOW", "low", low_counter_flow.clone());
            let med_sink_handler = PrioritySink::new("MEDIUM", "medium", med_counter_flow.clone());
            let high_sink_handler = PrioritySink::new("HIGH", "high", high_counter_flow.clone());

            Ok(flow! {
                name: "topology_patterns",
                journals: disk_journals(journal_path.clone()),

                stages: {
                    // FAN-IN: Three sources feeding into one aggregator
                    kafka_source = source!(RawDataEvent => kafka_source_handler);
                    api_source = source!(RawDataEvent => api_source_handler);
                    file_source = source!(RawDataEvent => file_source_handler);

                    // Aggregator demonstrates fan-in with StatefulHandler
                    aggregator = stateful!(RawDataEvent -> RawDataEvent => aggregator_handler);

                    // ✨ FLOWIP-080h: Router distributes to multiple sinks using Map helper
                    router = transform!(RawDataEvent -> RawDataEvent => router_handler);

                    // FAN-OUT: Three sinks receiving from one router
                    low_sink = sink!(RawDataEvent => low_sink_handler);
                    med_sink = sink!(RawDataEvent => med_sink_handler);
                    high_sink = sink!(RawDataEvent => high_sink_handler);
                },

                topology: {
                    // FAN-IN: Multiple sources to single aggregator
                    kafka_source |> aggregator;
                    api_source |> aggregator;
                    file_source |> aggregator;

                    // Processing chain
                    aggregator |> router;

                    // FAN-OUT: Single router to multiple sinks
                    // Each sink creates its own independent journal reader
                    router |> low_sink;
                    router |> med_sink;
                    router |> high_sink;
                }
            })
        }))?;

    Ok(())
}
