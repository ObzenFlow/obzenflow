//! Web Analytics Pipeline
//!
//! Problem: Track user behavior on a website to understand engagement patterns.
//!
//! We process a stream of user events (page views, clicks, scrolls) and need to:
//! 1. Track active sessions - emit session data when session ends (TimeWindow)
//! 2. Monitor conversion funnel - emit after every N events to track progress (EveryN)
//! 3. Calculate daily metrics - emit final stats at end (OnEOF)
//!
//! Each handler does ONE thing well, with the emission strategy controlling
//! when the accumulated data is written to the journal.
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
    FiniteSourceHandler, SinkHandler, StatefulHandler,
    StatefulHandlerExt,
};
use obzenflow_runtime_services::stages::stateful::emission::{EveryN, TimeWindow};
use serde_json::json;
use std::collections::HashMap;
use std::time::Duration;

/// Types of user events we track
#[derive(Clone, Debug)]
enum EventType {
    PageView { page: String, duration_ms: u64 },
    Click { element: String, page: String },
    Scroll { depth: u32, page: String },
    Conversion { value: f64 },
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

    fn generate_event(&mut self) -> (String, EventType) {
        let user_idx = self.event_count % self.users.len();
        let user_id = self.users[user_idx].clone();

        // Simulate user journey patterns
        let event = match self.event_count % 10 {
            0 | 1 => EventType::PageView {
                page: self.pages[0].clone(), // Home
                duration_ms: (3000 + (self.event_count * 17) % 2000) as u64,
            },
            2 | 3 => EventType::Click {
                element: "product_card".to_string(),
                page: self.pages[0].clone(),
            },
            4 | 5 => EventType::PageView {
                page: self.pages[1].clone(), // Products
                duration_ms: (5000 + (self.event_count * 23) % 5000) as u64,
            },
            6 => EventType::Scroll {
                depth: 50 + (self.event_count % 50) as u32,
                page: self.pages[1].clone(),
            },
            7 => EventType::PageView {
                page: self.pages[2].clone(), // Cart
                duration_ms: (2000 + (self.event_count * 13) % 1000) as u64,
            },
            8 => EventType::Click {
                element: "checkout_button".to_string(),
                page: self.pages[2].clone(),
            },
            _ => EventType::Conversion {
                value: 49.99 + (self.event_count as f64 * 1.23) % 150.0,
            },
        };

        (user_id, event)
    }
}

impl FiniteSourceHandler for UserEventSource {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.event_count >= self.max_events {
            return None;
        }

        let (user_id, event_type) = self.generate_event();
        self.event_count += 1;

        let payload = match event_type {
            EventType::PageView { page, duration_ms } => {
                json!({
                    "user_id": user_id,
                    "event_type": "page_view",
                    "page": page,
                    "duration_ms": duration_ms,
                })
            }
            EventType::Click { element, page } => {
                json!({
                    "user_id": user_id,
                    "event_type": "click",
                    "element": element,
                    "page": page,
                })
            }
            EventType::Scroll { depth, page } => {
                json!({
                    "user_id": user_id,
                    "event_type": "scroll",
                    "depth": depth,
                    "page": page,
                })
            }
            EventType::Conversion { value } => {
                json!({
                    "user_id": user_id,
                    "event_type": "conversion",
                    "value": value,
                })
            }
        };

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

/// Tracks user sessions - accumulates events per user
#[derive(Clone, Debug, Default)]
struct SessionState {
    sessions: HashMap<String, SessionData>,
}

#[derive(Clone, Debug, Default)]
struct SessionData {
    event_count: usize,
    pages_viewed: Vec<String>,
    total_duration_ms: u64,
    clicks: usize,
    max_scroll_depth: u32,
}

#[derive(Clone, Debug)]
struct SessionTracker {
    writer_id: WriterId,
}

impl SessionTracker {
    fn new() -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

#[async_trait]
impl StatefulHandler for SessionTracker {
    type State = SessionState;

    fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent) {
        let payload = event.payload();
        if let Some(user_id) = payload["user_id"].as_str() {
            let session = state.sessions.entry(user_id.to_string()).or_default();
            session.event_count += 1;

            match payload["event_type"].as_str() {
                Some("page_view") => {
                    if let Some(page) = payload["page"].as_str() {
                        session.pages_viewed.push(page.to_string());
                    }
                    if let Some(duration) = payload["duration_ms"].as_u64() {
                        session.total_duration_ms += duration;
                    }
                }
                Some("click") => {
                    session.clicks += 1;
                }
                Some("scroll") => {
                    if let Some(depth) = payload["depth"].as_u64() {
                        session.max_scroll_depth = session.max_scroll_depth.max(depth as u32);
                    }
                }
                _ => {}
            }
        }
    }

    fn initial_state(&self) -> Self::State {
        SessionState::default()
    }

    fn create_events(&self, state: &Self::State) -> Vec<ChainEvent> {
        // Emit one event per active session
        state.sessions.iter().map(|(user_id, data)| {
            ChainEventFactory::data_event(
                self.writer_id.clone(),
                "session_snapshot",
                json!({
                    "user_id": user_id,
                    "events": data.event_count,
                    "pages": data.pages_viewed.len(),
                    "duration_ms": data.total_duration_ms,
                    "clicks": data.clicks,
                    "max_scroll": data.max_scroll_depth,
                    "engagement_score": (data.clicks as f64 * 2.0) +
                        (data.pages_viewed.len() as f64 * 1.5) +
                        (data.max_scroll_depth as f64 * 0.01),
                }),
            )
        }).collect()
    }
}

/// Tracks conversion funnel progress
#[derive(Clone, Debug, Default)]
struct FunnelState {
    total_users: HashMap<String, bool>,
    funnel_stages: HashMap<String, usize>, // page -> visitor count
    conversions: Vec<f64>,
}

#[derive(Clone, Debug)]
struct FunnelTracker {
    writer_id: WriterId,
}

impl FunnelTracker {
    fn new() -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

#[async_trait]
impl StatefulHandler for FunnelTracker {
    type State = FunnelState;

    fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent) {
        let payload = event.payload();

        if let Some(user_id) = payload["user_id"].as_str() {
            state.total_users.insert(user_id.to_string(), true);
        }

        if let Some("page_view") = payload["event_type"].as_str() {
            if let Some(page) = payload["page"].as_str() {
                *state.funnel_stages.entry(page.to_string()).or_insert(0) += 1;
            }
        }

        if let Some("conversion") = payload["event_type"].as_str() {
            if let Some(value) = payload["value"].as_f64() {
                state.conversions.push(value);
            }
        }
    }

    fn initial_state(&self) -> Self::State {
        FunnelState::default()
    }

    fn create_events(&self, state: &Self::State) -> Vec<ChainEvent> {
        let home_visitors = state.funnel_stages.get("/home").unwrap_or(&0);
        let product_visitors = state.funnel_stages.get("/products").unwrap_or(&0);
        let cart_visitors = state.funnel_stages.get("/cart").unwrap_or(&0);
        let total_revenue: f64 = state.conversions.iter().sum();

        vec![ChainEventFactory::data_event(
            self.writer_id.clone(),
            "funnel_update",
            json!({
                "unique_users": state.total_users.len(),
                "home_to_product": if *home_visitors > 0 {
                    (*product_visitors as f64 / *home_visitors as f64) * 100.0
                } else { 0.0 },
                "product_to_cart": if *product_visitors > 0 {
                    (*cart_visitors as f64 / *product_visitors as f64) * 100.0
                } else { 0.0 },
                "cart_to_purchase": if *cart_visitors > 0 {
                    (state.conversions.len() as f64 / *cart_visitors as f64) * 100.0
                } else { 0.0 },
                "conversions": state.conversions.len(),
                "total_revenue": total_revenue,
                "avg_order_value": if !state.conversions.is_empty() {
                    total_revenue / state.conversions.len() as f64
                } else { 0.0 },
            }),
        )]
    }
}

/// Overall metrics aggregator
#[derive(Clone, Debug, Default)]
struct MetricsState {
    total_events: usize,
    events_by_type: HashMap<String, usize>,
    pages_by_popularity: HashMap<String, usize>,
    total_revenue: f64,
}

#[derive(Clone, Debug)]
struct MetricsAggregator {
    writer_id: WriterId,
}

impl MetricsAggregator {
    fn new() -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

#[async_trait]
impl StatefulHandler for MetricsAggregator {
    type State = MetricsState;

    fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent) {
        state.total_events += 1;

        let payload = event.payload();

        if let Some(event_type) = payload["event_type"].as_str() {
            *state.events_by_type.entry(event_type.to_string()).or_insert(0) += 1;
        }

        if let Some(page) = payload["page"].as_str() {
            *state.pages_by_popularity.entry(page.to_string()).or_insert(0) += 1;
        }

        if let Some(value) = payload["value"].as_f64() {
            state.total_revenue += value;
        }
    }

    fn initial_state(&self) -> Self::State {
        MetricsState::default()
    }

    fn create_events(&self, state: &Self::State) -> Vec<ChainEvent> {
        vec![ChainEventFactory::data_event(
            self.writer_id.clone(),
            "daily_metrics",
            json!({
                "total_events": state.total_events,
                "event_breakdown": state.events_by_type,
                "popular_pages": state.pages_by_popularity,
                "total_revenue": state.total_revenue,
            }),
        )]
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

        if event.event_type() == "session_snapshot" {
            println!("\n📊 [{}] Session Update:", self.name);
            println!("   Active sessions tracked");
            println!("   Sample user: {}", payload["user_id"]);
            println!("   - Events: {}", payload["events"]);
            println!("   - Engagement: {:.1}", payload["engagement_score"].as_f64().unwrap_or(0.0));
        } else if event.event_type() == "funnel_update" {
            println!("\n🎯 [{}] Funnel Progress:", self.name);
            println!("   Unique users: {}", payload["unique_users"]);
            println!("   Home → Product: {:.1}%", payload["home_to_product"].as_f64().unwrap_or(0.0));
            println!("   Product → Cart: {:.1}%", payload["product_to_cart"].as_f64().unwrap_or(0.0));
            println!("   Cart → Purchase: {:.1}%", payload["cart_to_purchase"].as_f64().unwrap_or(0.0));
            println!("   Revenue: ${:.2}", payload["total_revenue"].as_f64().unwrap_or(0.0));
        } else if event.event_type() == "daily_metrics" {
            println!("\n📈 [{}] Daily Summary:", self.name);
            println!("   Total events: {}", payload["total_events"]);
            println!("   Total revenue: ${:.2}", payload["total_revenue"].as_f64().unwrap_or(0.0));
            if let Some(breakdown) = payload["event_breakdown"].as_object() {
                println!("   Event types: {} unique", breakdown.len());
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

    println!("🌐 Web Analytics Pipeline");
    println!("=========================");
    println!();
    println!("Processing user behavior events with different emission patterns:");
    println!();
    println!("📊 Session Tracker   → TimeWindow(3s)  → Session snapshots");
    println!("🎯 Funnel Tracker    → EveryN(50)      → Conversion progress");
    println!("📈 Metrics           → OnEOF (default) → Daily summary");
    println!();
    println!("Each handler does ONE thing, emission strategy controls WHEN.\n");

    FlowApplication::run(async {
        flow! {
            name: "web_analytics",
            journals: disk_journals(std::path::PathBuf::from("target/web_analytics")),
            middleware: [],

            stages: {
                // User event stream
                events = source!("user_events" => UserEventSource::new(200));

                // Session tracking with time windows (e.g., for real-time dashboard)
                sessions = stateful!("session_tracker" =>
                    SessionTracker::new()
                        .with_emission(TimeWindow::new(Duration::from_secs(3)))
                );

                // Funnel analysis with periodic updates
                funnel = stateful!("funnel_tracker" =>
                    FunnelTracker::new()
                        .with_emission(EveryN::new(50))
                );

                // Daily metrics with default OnEOF
                metrics = stateful!("metrics" => MetricsAggregator::new());

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
    println!("\nKey insights:");
    println!("• Sessions emitted every 3 seconds (real-time monitoring)");
    println!("• Funnel updated every 50 events (progress tracking)");
    println!("• Metrics emitted once at end (daily reporting)");
    println!("\nSame pattern, different timing = .with_emission() power!");

    Ok(())
}