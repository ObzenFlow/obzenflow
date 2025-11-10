//! Web Analytics Pipeline - Using FLOWIP-080j Typed Accumulators
//!
//! Problem: Track user behavior on a website to understand engagement patterns.
//!
//! We process a stream of user events (page views, clicks, scrolls) and need to:
//! 1. Track active sessions - emit session data when session ends (TimeWindow)
//! 2. Monitor conversion funnel - emit after every N events to track progress (EveryN)
//! 3. Calculate daily metrics - emit final stats at end (OnEOF)
//!
//! This demonstrates FLOWIP-080j typed accumulators:
//! - GroupByTyped for per-user session tracking
//! - ReduceTyped for funnel and metrics aggregation
//! - Zero ChainEvent manipulation in business logic
//!
//! Run with: `cargo run -p obzenflow --example web_analytics_pipeline`

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    event::payloads::delivery_payload::{DeliveryPayload, DeliveryMethod},
    WriterId,
    id::StageId,
};
use obzenflow_dsl_infra::{flow, sink, source, stateful};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler,
};
// FLOWIP-080j: Typed stateful accumulators
use obzenflow_runtime_services::stages::stateful::accumulators::{GroupByTyped, ReduceTyped};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::time::Duration;

// FLOWIP-080j: Domain types for type-safe event processing
#[derive(Clone, Debug, Deserialize)]
struct UserEvent {
    user_id: String,
    event_type: String,
    #[serde(default)]
    page: Option<String>,
    #[serde(default)]
    duration_ms: Option<u64>,
    #[serde(default)]
    element: Option<String>,
    #[serde(default)]
    depth: Option<u32>,
    #[serde(default)]
    value: Option<f64>,
}

/// Source that simulates user behavior events
#[derive(Clone, Debug)]
struct UserEventSource {
    event_count: usize,
    max_events: usize,
    writer_id: WriterId,
    users: Vec<String>,
    pages: Vec<String>,
}

impl UserEventSource {
    fn new(max_events: usize) -> Self {
        Self {
            event_count: 0,
            max_events,
            writer_id: WriterId::from(StageId::new()),
            users: vec![
                "user_001".to_string(),
                "user_002".to_string(),
                "user_003".to_string(),
                "user_004".to_string(),
                "user_005".to_string(),
            ],
            pages: vec![
                "/home".to_string(),
                "/products".to_string(),
                "/cart".to_string(),
                "/checkout".to_string(),
                "/about".to_string(),
            ],
        }
    }

    fn generate_event_payload(&mut self) -> serde_json::Value {
        let user_idx = self.event_count % self.users.len();
        let user_id = &self.users[user_idx];

        // Simulate realistic user journey patterns with cart abandonment
        // Pattern repeats every 20 events to create ~40% cart-to-purchase conversion
        match self.event_count % 20 {
            // Home page visits (30% of traffic)
            0 | 1 | 2 | 3 | 4 | 5 => json!({
                "user_id": user_id,
                "event_type": "page_view",
                "page": self.pages[0], // Home
                "duration_ms": (3000 + (self.event_count * 17) % 2000) as u64,
            }),
            // Product browsing (25% of traffic)
            6 | 7 | 8 | 9 => json!({
                "user_id": user_id,
                "event_type": "page_view",
                "page": self.pages[1], // Products
                "duration_ms": (5000 + (self.event_count * 23) % 5000) as u64,
            }),
            // Product interactions (15% of traffic)
            10 | 11 | 12 => json!({
                "user_id": user_id,
                "event_type": "click",
                "element": "product_card",
                "page": self.pages[1],
            }),
            // Cart page views (10% - not everyone adds to cart)
            13 | 14 => json!({
                "user_id": user_id,
                "event_type": "page_view",
                "page": self.pages[2], // Cart
                "duration_ms": (2000 + (self.event_count * 13) % 1000) as u64,
            }),
            // Scroll events (5%)
            15 => json!({
                "user_id": user_id,
                "event_type": "scroll",
                "depth": 50 + (self.event_count % 50) as u32,
                "page": self.pages[1],
            }),
            // Checkout button clicks (5%)
            16 => json!({
                "user_id": user_id,
                "event_type": "click",
                "element": "checkout_button",
                "page": self.pages[2],
            }),
            // Bounces/abandoned carts (5%)
            17 => json!({
                "user_id": user_id,
                "event_type": "page_view",
                "page": self.pages[4], // About page (bounce)
                "duration_ms": (1000 + (self.event_count * 7) % 500) as u64,
            }),
            // Actual conversions (5% - realistic cart abandonment ~60%)
            18 => json!({
                "user_id": user_id,
                "event_type": "conversion",
                "value": 49.99 + (self.event_count as f64 * 1.23) % 150.0,
            }),
            // Additional product views (5%)
            _ => json!({
                "user_id": user_id,
                "event_type": "page_view",
                "page": self.pages[1], // Products
                "duration_ms": (4000 + (self.event_count * 19) % 3000) as u64,
            }),
        }
    }
}

impl FiniteSourceHandler for UserEventSource {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.event_count >= self.max_events {
            return None;
        }

        let payload = self.generate_event_payload();
        self.event_count += 1;

        Some(ChainEventFactory::data_event(
            self.writer_id.clone(),
            "user_event",
            payload,
        ))
    }

    fn is_complete(&self) -> bool {
        self.event_count >= self.max_events
    }
}

// FLOWIP-080j: Session tracking state (per user)
#[derive(Clone, Debug, Default, Serialize)]
struct SessionData {
    event_count: usize,
    pages_viewed: Vec<String>,
    total_duration_ms: u64,
    clicks: usize,
    max_scroll_depth: u32,
}

impl UserEvent {
    // Pure function: Update session state based on event
    fn update_session(&self, session: &mut SessionData) {
        session.event_count += 1;

        match self.event_type.as_str() {
            "page_view" => {
                if let Some(ref page) = self.page {
                    session.pages_viewed.push(page.clone());
                }
                if let Some(duration) = self.duration_ms {
                    session.total_duration_ms += duration;
                }
            }
            "click" => {
                session.clicks += 1;
            }
            "scroll" => {
                if let Some(depth) = self.depth {
                    session.max_scroll_depth = session.max_scroll_depth.max(depth);
                }
            }
            _ => {}
        }
    }
}

// FLOWIP-080j: Funnel tracking state
#[derive(Clone, Debug, Default, Serialize)]
struct FunnelState {
    total_users: HashMap<String, bool>,
    funnel_stages: HashMap<String, usize>, // page -> visitor count
    conversions: Vec<f64>,
}

impl UserEvent {
    // Pure function: Update funnel state based on event
    fn update_funnel(&self, state: &mut FunnelState) {
        state.total_users.insert(self.user_id.clone(), true);

        if self.event_type == "page_view" {
            if let Some(ref page) = self.page {
                *state.funnel_stages.entry(page.clone()).or_insert(0) += 1;
            }
        }

        if self.event_type == "conversion" {
            if let Some(value) = self.value {
                state.conversions.push(value);
            }
        }
    }
}

// FLOWIP-080j: Overall metrics state
#[derive(Clone, Debug, Default, Serialize)]
struct MetricsState {
    total_events: usize,
    events_by_type: HashMap<String, usize>,
    pages_by_popularity: HashMap<String, usize>,
    total_revenue: f64,
}

impl UserEvent {
    // Pure function: Update overall metrics
    fn update_metrics(&self, state: &mut MetricsState) {
        state.total_events += 1;

        *state.events_by_type.entry(self.event_type.clone()).or_insert(0) += 1;

        if let Some(ref page) = self.page {
            *state.pages_by_popularity.entry(page.clone()).or_insert(0) += 1;
        }

        if let Some(value) = self.value {
            state.total_revenue += value;
        }
    }
}

/// Sink that displays analytics reports
#[derive(Clone, Debug)]
struct AnalyticsSink {
    name: String,
}

impl AnalyticsSink {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }
}

#[async_trait]
impl SinkHandler for AnalyticsSink {
    async fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<DeliveryPayload> {
        let payload = event.payload();

        // GroupByTyped emits "grouped" events with key and result
        if event.event_type() == "grouped" {
            if let (Some(key), Some(result)) = (payload.get("key"), payload.get("result")) {
                // Session snapshot from GroupByTyped
                println!("\n📊 [{}] Session Update:", self.name);
                println!("   User: {}", key.as_str().unwrap_or("unknown"));
                println!("   - Events: {}", result["event_count"].as_u64().unwrap_or(0));
                println!("   - Pages: {}", result["pages_viewed"].as_array().map(|v| v.len()).unwrap_or(0));
                println!("   - Clicks: {}", result["clicks"].as_u64().unwrap_or(0));
                println!("   - Duration: {}ms", result["total_duration_ms"].as_u64().unwrap_or(0));
            }
        }
        // ReduceTyped emits "reduced" events with result
        else if event.event_type() == "reduced" {
            if let Some(result) = payload.get("result") {
                // Check if it's funnel or metrics by looking at the fields
                if result.get("funnel_stages").is_some() {
                    // Funnel update from ReduceTyped
                    println!("\n🎯 [{}] Funnel Progress:", self.name);
                    println!("   Unique users: {}", result["total_users"].as_object().map(|m| m.len()).unwrap_or(0));

                    if let Some(stages) = result["funnel_stages"].as_object() {
                        let home = stages.get("/home").and_then(|v| v.as_u64()).unwrap_or(0);
                        let products = stages.get("/products").and_then(|v| v.as_u64()).unwrap_or(0);
                        let cart = stages.get("/cart").and_then(|v| v.as_u64()).unwrap_or(0);
                        let conversions = result["conversions"].as_array().map(|v| v.len()).unwrap_or(0);
                        let total_revenue: f64 = result["conversions"].as_array()
                            .map(|arr| arr.iter().filter_map(|v| v.as_f64()).sum())
                            .unwrap_or(0.0);

                        if home > 0 {
                            println!("   Home → Product: {:.1}%", (products as f64 / home as f64) * 100.0);
                        }
                        if products > 0 {
                            println!("   Product → Cart: {:.1}%", (cart as f64 / products as f64) * 100.0);
                        }
                        if cart > 0 {
                            println!("   Cart → Purchase: {:.1}%", (conversions as f64 / cart as f64) * 100.0);
                        }
                        println!("   Revenue: ${:.2}", total_revenue);
                    }
                } else {
                    // Daily metrics from ReduceTyped
                    println!("\n📈 [{}] Daily Summary:", self.name);
                    println!("   Total events: {}", result["total_events"].as_u64().unwrap_or(0));
                    println!("   Total revenue: ${:.2}", result["total_revenue"].as_f64().unwrap_or(0.0));
                    if let Some(breakdown) = result["events_by_type"].as_object() {
                        println!("   Event types: {} unique", breakdown.len());
                    }
                }
            }
        }

        Ok(DeliveryPayload::success(
            &self.name,
            DeliveryMethod::Custom("Analytics".to_string()),
            Some(1),
        ))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    std::env::set_var("OBZENFLOW_METRICS_EXPORTER", "console");

    println!("🌐 Web Analytics Pipeline (FLOWIP-080j)");
    println!("========================================");
    println!();
    println!("Processing user behavior events with typed accumulators:");
    println!();
    println!("📊 Session Tracker   → GroupByTyped + TimeWindow(3s)");
    println!("🎯 Funnel Tracker    → ReduceTyped + EveryN(50)");
    println!("📈 Metrics           → ReduceTyped + OnEOF");
    println!();
    println!("Zero ChainEvent manipulation - all type-safe!\n");

    FlowApplication::run(async {
        flow! {
            name: "web_analytics",
            journals: disk_journals(std::path::PathBuf::from("target/web_analytics")),
            middleware: [],

            stages: {
                // User event stream
                events = source!("user_events" => UserEventSource::new(200));

                // FLOWIP-080j: GroupByTyped for per-user session tracking
                sessions = stateful!("session_tracker" =>
                    GroupByTyped::new(
                        |event: &UserEvent| event.user_id.clone(),
                        |session: &mut SessionData, event: &UserEvent| {  // CORRECTED: state first
                            event.update_session(session);
                        }
                    ).emit_within(Duration::from_secs(3))  // Time window emission
                );

                // FLOWIP-080j: ReduceTyped for funnel analysis
                funnel = stateful!("funnel_tracker" =>
                    ReduceTyped::new(
                        FunnelState::default(),
                        |state: &mut FunnelState, event: &UserEvent| {  // CORRECTED: state first
                            event.update_funnel(state);
                        }
                    ).emit_every_n(50)  // Periodic updates
                );

                // FLOWIP-080j: ReduceTyped for overall metrics
                metrics = stateful!("metrics" =>
                    ReduceTyped::new(
                        MetricsState::default(),
                        |state: &mut MetricsState, event: &UserEvent| {  // CORRECTED: state first
                            event.update_metrics(state);
                        }
                    ).emit_on_eof()  // Only at end
                );

                // Sinks for each analysis type
                session_sink = sink!("sessions" => AnalyticsSink::new("Sessions"));
                funnel_sink = sink!("funnel" => AnalyticsSink::new("Funnel"));
                metrics_sink = sink!("metrics" => AnalyticsSink::new("Metrics"));
            },

            topology: {
                // Fan out to all analyzers
                events |> sessions;
                events |> funnel;
                events |> metrics;

                // Each analyzer to its sink
                sessions |> session_sink;
                funnel |> funnel_sink;
                metrics |> metrics_sink;
            }
        }
        .await
        .map_err(|e| anyhow::anyhow!("Failed: {:?}", e))
    })
    .await?;

    println!("\n✅ Analytics pipeline complete!");
    println!("\n💡 Key Improvements (FLOWIP-080j):");
    println!("   • GroupByTyped: Type-safe per-user session tracking");
    println!("   • ReduceTyped: Type-safe funnel and metrics aggregation");
    println!("   • Zero ChainEvent manipulation in business logic");
    println!("   • Pure update functions: update_session(), update_funnel(), update_metrics()");
    println!("   • ~150 lines of custom StatefulHandler code eliminated!");

    Ok(())
}