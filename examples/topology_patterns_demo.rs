// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Demo: Topology Patterns - Fan-In, Fan-Out, and Diamond Patterns (FLOWIP-080h)
//!
//! This demonstrates how ObzenFlow naturally handles complex topologies through
//! independent journal readers and multiple upstream subscriptions.
//!
//! **Reference Example for**: Topology patterns, ETL pipelines, multi-source/sink architectures
//!
//! Key concepts demonstrated:
//! - Fan-in: Multiple sources → single aggregator
//! - Fan-out: Single router → multiple sinks
//! - Diamond pattern: Combines both (realistic ETL)
//! - StatefulHandler for aggregation (no Arc<Mutex>)
//! - Independent journal readers (no coordination needed)
//! - Natural backpressure handling
//!
//! **FLOWIP-080h Update**: Replaced 38-line SmartRouter struct with Map helper

use anyhow::Result;
use async_trait::async_trait;
use obzenflow::typed::sources;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    id::StageId,
    TypedPayload, WriterId,
};
use obzenflow_dsl::{flow, sink, source, stateful, transform};
use obzenflow_infra::application::{Banner, FlowApplication, Presentation};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{SinkHandler, StatefulHandler};
// ✨ FLOWIP-080h: Import Map helper
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_runtime::stages::transform::Map;
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
    current_event: Option<ChainEvent>,
    /// Flag when EOF has been observed for audit
    eof_seen: bool,
}

/// Stateful aggregator that merges events from multiple upstream sources (FAN-IN)
///
/// This demonstrates fan-in pattern: multiple sources → single aggregator
/// Each source has its own journal reader at independent positions
#[derive(Clone, Debug)]
struct MultiSourceAggregator {
    writer_id: WriterId,
    expected_counts: BTreeMap<String, usize>,
}

impl MultiSourceAggregator {
    fn new() -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
            expected_counts: BTreeMap::new(),
        }
    }

    fn with_expected(mut self, expected: BTreeMap<String, usize>) -> Self {
        self.expected_counts = expected;
        self
    }
}

#[async_trait]
impl StatefulHandler for MultiSourceAggregator {
    type State = AggregatorState;

    fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent) {
        let payload = event.payload();
        let source = payload["source"].as_str().unwrap_or("unknown").to_string();
        let value = payload["value"].as_i64().unwrap_or(0);

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

    fn emit(&self, state: &mut Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        if let Some(event) = state.current_event.take() {
            let payload = event.payload();
            let source = payload["source"].as_str().unwrap_or("unknown").to_string();

            let event_count = *state.events_by_source.get(&source).unwrap_or(&0);
            let total_value = *state.value_by_source.get(&source).unwrap_or(&0);

            // Enrich event with aggregation stats
            let mut enriched = payload.clone();
            enriched["aggregation"] = json!({
                "total_events": state.total_events,
                "source_event_count": event_count,
                "source_total_value": total_value,
                "sources_seen": state.events_by_source.len(),
            });

            let aggregated_event = ChainEventFactory::derived_data_event(
                self.writer_id,
                &event,
                "data.aggregated",
                enriched,
            );

            // Demo-local accounting: track outputs emitted so we can ensure
            // that every input observed by this stateful handler results in
            // a corresponding output (for this immediate-enrichment pattern).
            // FLOWIP-090c/090d: Once `StatefulAccountingContract` is available and wired
            // via the contract DSL, this counter/assertion is expected to be removed
            // in favor of a generic accounting contract; see FLOWIP-090d exit criteria.
            state.outputs_emitted += 1;

            Ok(vec![aggregated_event])
        } else {
            Ok(vec![])
        }
    }

    fn initial_state(&self) -> Self::State {
        AggregatorState {
            expected_counts: self.expected_counts.clone(),
            eof_seen: false,
            ..AggregatorState::default()
        }
    }

    fn create_events(&self, state: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        // Audit: verify counts match expectations, log loudly if not
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

        // Hard audit check for the demo: panic if any source is short
        for (src, expected) in &state.expected_counts {
            let got = state.events_by_source.get(src).cloned().unwrap_or(0);
            if got != *expected {
                let eof_seen = state.eof_seen;
                panic!("AUDIT FAILED: source={src} expected={expected} got={got} (eof_seen={eof_seen})");
            }
        }

        // Demo-local stateful accounting guard (FLOWIP-081b):
        // For this immediate-enrichment handler, every input that was
        // accumulated should have produced exactly one output event via
        // `emit`. If this ever diverges, it means the stateful FSM or
        // handler semantics have regressed and are silently dropping
        // inputs during draining or emission. FLOWIP-090c/090d will replace this
        // handler-local guard with a first-class `StatefulAccountingContract`
        // configured from the contract DSL; at that point this demo-local logic
        // should be removed and enforced purely via contracts.
        if state.outputs_emitted != state.total_events {
            panic!(
                "STATEFUL ACCOUNTING FAILED: inputs_observed={} outputs_emitted={} (eof_seen={})",
                state.total_events, state.outputs_emitted, state.eof_seen
            );
        }

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
fn smart_router() -> Map<impl Fn(ChainEvent) -> ChainEvent + Send + Sync + Clone> {
    Map::new(|event| {
        let payload = event.payload();
        let value = payload["value"].as_i64().unwrap_or(0);
        let source = payload["source"].as_str().unwrap_or("unknown");

        // Route to different channels based on value
        let route = if value < 30 {
            "low"
        } else if value < 50 {
            "medium"
        } else {
            "high"
        };

        println!(
            "[FAN-OUT] Router processing event from '{source}' with value {value} → route: {route}"
        );

        // Enrich with routing info
        let mut enriched = payload.clone();
        enriched["route"] = json!(route);
        enriched["route_source"] = json!(source);
        enriched["route_value"] = json!(value);

        ChainEventFactory::derived_data_event(event.writer_id, &event, "data.routed", enriched)
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
impl SinkHandler for PrioritySink {
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        let payload = event.payload();
        let route = payload["route"].as_str().unwrap_or("");

        // Only process events matching our filter
        if route == self.route_filter {
            let new_total = self.event_count.fetch_add(1, Ordering::Relaxed) + 1;
            let source = payload["source"].as_str().unwrap_or("unknown");
            let value = payload["value"].as_i64().unwrap_or(0);
            *self.per_source.entry(source.to_string()).or_insert(0) += 1;

            println!(
                "[SINK:{}] Processed event from '{}' (value: {}) - total: {}",
                self.name, source, value, new_total
            );
        }

        Ok(DeliveryPayload::success(
            &self.name,
            DeliveryMethod::Custom("Processed".to_string()),
            Some(1),
        ))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Set environment to use console exporter
    std::env::set_var("OBZENFLOW_METRICS_EXPORTER", "console");

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
            .config_block("Demonstrating:\n• Fan-in: Multiple sources → single aggregator\n• Fan-out: Single router → multiple sinks\n• Diamond pattern: Realistic ETL topology\n• StatefulHandler for aggregation (no Arc<Mutex>)\n• Independent journal readers\n\nTopology:\n  kafka_source (5 events)  ──┐\n  api_source (4 events)    ──┼──> aggregator (fan-in)\n  file_source (4 events)   ──┘            │\n                                           ▼\n                                        router\n                                           │\n                         ┌─────────────────┼─────────────────┐\n                         ▼                 ▼                 ▼\n                    low_sink          med_sink          high_sink\n                   (value<30)       (30≤value<70)      (value≥70)"),
    )
    .with_footer(move |outcome| {
        let mut out = outcome.default_footer();

        let low = footer_low.load(Ordering::Relaxed);
        let med = footer_med.load(Ordering::Relaxed);
        let high = footer_high.load(Ordering::Relaxed);

        out.push_str(&format!(
            "\n\nFAN-OUT sink summary:\n- LOW (value<30): {low}\n- MEDIUM (30≤value<70): {med}\n- HIGH (value≥70): {high}"
        ));

        out.push_str("\n\nKey insights:\n• Fan-in: Aggregator subscribed to 3 upstream journals\n  - Each source had independent journal reader\n  - Round-robin reading ensures fairness\n  - No special merge primitive needed\n• Fan-out: Each sink created its own journal reader\n  - Readers progress independently\n  - Natural backpressure (slow sinks do not block fast ones)\n  - All sinks see all events (broadcast behaviour)\n• Diamond pattern combines both:\n  - Multiple inputs merged and processed\n  - Results distributed to multiple outputs\n  - Common in ETL, event routing, microservices");
        out
    });

    // Use FlowApplication for modern pattern
    let low_counter_flow = low_counter.clone();
    let med_counter_flow = med_counter.clone();
    let high_counter_flow = high_counter.clone();

    FlowApplication::run_with_presentation(
        flow! {
            name: "topology_patterns",
            journals: disk_journals(journal_path.clone()),
            middleware: [],

            stages: {
                // FAN-IN: Three sources feeding into one aggregator
                kafka_source = source!(RawDataEvent => sources::finite_from_fn({
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
                        })
                    }
                }));
                api_source = source!(RawDataEvent => sources::finite_from_fn({
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
                        })
                    }
                }));
                file_source = source!(RawDataEvent => sources::finite_from_fn({
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
                        })
                    }
                }));

                // Aggregator demonstrates fan-in with StatefulHandler
                aggregator = stateful!(MultiSourceAggregator::new().with_expected({
                    let mut m = BTreeMap::new();
                    m.insert("kafka".to_string(), 5);
                    m.insert("api".to_string(), 4);
                    m.insert("file".to_string(), 4);
                    m
                }));

                // ✨ FLOWIP-080h: Router distributes to multiple sinks using Map helper
                router = transform!(smart_router());

                // FAN-OUT: Three sinks receiving from one router
                low_sink = sink!(PrioritySink::new("LOW", "low", low_counter_flow.clone()));
                med_sink = sink!(PrioritySink::new(
                    "MEDIUM",
                    "medium",
                    med_counter_flow.clone()
                ));
                high_sink = sink!(PrioritySink::new("HIGH", "high", high_counter_flow.clone()));
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
        },
        presentation,
    )
    .await?;

    Ok(())
}
