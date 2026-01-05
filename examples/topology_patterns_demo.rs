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
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    id::StageId,
    TypedPayload,
    WriterId,
};
use obzenflow_dsl_infra::{flow, sink, source, stateful, transform};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::common::handlers::{
    SinkHandler, StatefulHandler,
};
use obzenflow_runtime_services::stages::source::FiniteSourceTyped;
// ✨ FLOWIP-080h: Import Map helper
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_runtime_services::stages::transform::Map;
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
                self.writer_id.clone(),
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
                println!(
                    "⚠️  AUDIT MISMATCH source={} expected={} got={}",
                    src, expected, got
                );
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
            println!("║ {:<22} ║ {:>4} events / {:>4} ║", source, count, value);
        }
        println!("╚════════════════════════╩═════════════╝");

        // Hard audit check for the demo: panic if any source is short
        for (src, expected) in &state.expected_counts {
            let got = state.events_by_source.get(src).cloned().unwrap_or(0);
            if got != *expected {
                panic!(
                    "AUDIT FAILED: source={} expected={} got={} (eof_seen={})",
                    src, expected, got, state.eof_seen
                );
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
            "[FAN-OUT] Router processing event from '{}' with value {} → route: {}",
            source, value, route
        );

        // Enrich with routing info
        let mut enriched = payload.clone();
        enriched["route"] = json!(route);
        enriched["route_source"] = json!(source);
        enriched["route_value"] = json!(value);

        ChainEventFactory::derived_data_event(
            event.writer_id.clone(),
            &event,
            "data.routed",
            enriched,
        )
    })
}

fn print_sink_summary(name: &str, route: &str, count: usize) {
    // Fixed width for deterministic alignment
    println!();
    println!("┌──────────────────────────────────────────────┐");
    println!("│ FAN-OUT SINK SUMMARY {:>19} │", format!("({})", name));
    println!("├──────────────────────────────────────────────┤");
    println!("│ Route handled : {:<25}│", route);
    println!("│ Events seen   : {:<25}│", count);
    println!("└──────────────────────────────────────────────┘");
}

fn print_sink_summary_with_sources(
    name: &str,
    route: &str,
    count: usize,
    per_source: &BTreeMap<String, usize>,
) {
    print_sink_summary(name, route, count);
    if !per_source.is_empty() {
        println!("    per-source: {:?}", per_source);
    }
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

    println!("🕸️  Topology Patterns Demo - Fan-In, Fan-Out, and Diamond Pattern");
    println!("================================================================\n");
    println!("Demonstrating:");
    println!("  • Fan-in: Multiple sources → single aggregator");
    println!("  • Fan-out: Single router → multiple sinks");
    println!("  • Diamond pattern: Realistic ETL topology");
    println!("  • StatefulHandler for aggregation (no Arc<Mutex>)");
    println!("  • Independent journal readers\n");

    let journal_path = std::path::PathBuf::from("target/topology_patterns_demo_journal");
    let low_counter = Arc::new(AtomicUsize::new(0));
    let med_counter = Arc::new(AtomicUsize::new(0));
    let high_counter = Arc::new(AtomicUsize::new(0));

    println!("Topology:");
    println!("  kafka_source (5 events)  ──┐");
    println!("  api_source (4 events)    ──┼──> aggregator (fan-in)");
    println!("  file_source (4 events)   ──┘            │");
    println!("                                           ▼");
    println!("                                        router");
    println!("                                           │");
    println!("                         ┌─────────────────┼─────────────────┐");
    println!("                         ▼                 ▼                 ▼");
    println!("                    low_sink          med_sink          high_sink");
    println!("                   (value<30)       (30≤value<70)      (value≥70)\n");

    // Use FlowApplication for modern pattern
    let low_counter_flow = low_counter.clone();
    let med_counter_flow = med_counter.clone();
    let high_counter_flow = high_counter.clone();

    FlowApplication::run(flow! {
        name: "topology_patterns",
        journals: disk_journals(journal_path.clone()),
        middleware: [],

            stages: {
                // FAN-IN: Three sources feeding into one aggregator
                kafka = source!("kafka_source" => FiniteSourceTyped::from_item_fn({
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
                api = source!("api_source" => FiniteSourceTyped::from_item_fn({
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
                file_src = source!("file_source" => FiniteSourceTyped::from_item_fn({
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
                aggregator = stateful!("aggregator" => MultiSourceAggregator::new().with_expected({
                    let mut m = BTreeMap::new();
                    m.insert("kafka".to_string(), 5);
                    m.insert("api".to_string(), 4);
                    m.insert("file".to_string(), 4);
                    m
                }));

                // ✨ FLOWIP-080h: Router distributes to multiple sinks using Map helper
                router = transform!("router" => smart_router());

                // FAN-OUT: Three sinks receiving from one router
                low_priority = sink!("low_sink" => PrioritySink::new("LOW", "low", low_counter_flow.clone()));
                med_priority = sink!("med_sink" => PrioritySink::new("MEDIUM", "medium", med_counter_flow.clone()));
                high_priority = sink!("high_sink" => PrioritySink::new("HIGH", "high", high_counter_flow.clone()));
            },

            topology: {
                // FAN-IN: Multiple sources to single aggregator
                kafka |> aggregator;
                api |> aggregator;
                file_src |> aggregator;

                // Processing chain
                aggregator |> router;

                // FAN-OUT: Single router to multiple sinks
                // Each sink creates its own independent journal reader
                router |> low_priority;
                router |> med_priority;
                router |> high_priority;
            }
    })
    .await
    ?;

    // Deterministic, consolidated fan-out summaries
    print_sink_summary("LOW", "low", low_counter.load(Ordering::Relaxed));
    print_sink_summary("MEDIUM", "medium", med_counter.load(Ordering::Relaxed));
    print_sink_summary("HIGH", "high", high_counter.load(Ordering::Relaxed));

    println!("\n\nFlow completed successfully!");
    println!("\nKey insights:");
    println!("  • Fan-in: Aggregator subscribed to 3 upstream journals");
    println!("    - Each source had independent journal reader");
    println!("    - Round-robin reading ensures fairness");
    println!("    - No special merge primitive needed");
    println!("  • Fan-out: Each sink created its own journal reader");
    println!("    - Readers progress independently");
    println!("    - Natural backpressure (slow sinks don't block fast ones)");
    println!("    - All sinks see all events (broadcast behavior)");
    println!("  • Diamond pattern combines both:");
    println!("    - Multiple inputs merged and processed");
    println!("    - Results distributed to multiple outputs");
    println!("    - Common in ETL, event routing, microservices");

    Ok(())
}
