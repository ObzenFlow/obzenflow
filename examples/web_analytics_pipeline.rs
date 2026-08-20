// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Web Analytics Pipeline - Using FLOWIP-080j & FLOWIP-082a
//!
//! Problem: Track user behavior on a website to understand engagement patterns.
//!
//! We process a stream of user events (page views, clicks, scrolls) and need to:
//! 1. Track active sessions - emit session data when session ends (TimeWindow)
//! 2. Monitor conversion funnel - emit after every N events to track progress (EveryN)
//! 3. Calculate daily metrics - emit final stats at end (OnEOF)
//!
//! This demonstrates:
//! - FLOWIP-080j: GroupByTyped, ReduceTyped for typed accumulators
//! - FLOWIP-082a: TypedPayload for strongly-typed events
//! - Zero ChainEvent manipulation in business logic
//!
//! Run with: `cargo run -p obzenflow --example web_analytics_pipeline`

use anyhow::Result;
use obzenflow::sources;
use obzenflow::stateful;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, stateful, FlowDefinition};
use obzenflow_infra::application::{Banner, FlowApplication, Presentation};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::sink::SinkTyped;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

// FLOWIP-082a: Strongly-typed domain events
#[derive(Clone, Debug, Deserialize, Serialize)]
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

impl TypedPayload for UserEvent {
    const EVENT_TYPE: &'static str = "analytics.user_event";
    const SCHEMA_VERSION: u32 = 1;
}

const USERS: [&str; 5] = ["user_001", "user_002", "user_003", "user_004", "user_005"];
const PAGES: [&str; 5] = ["/home", "/products", "/cart", "/checkout", "/about"];

fn base_event(user_id: String, event_type: &str) -> UserEvent {
    UserEvent {
        user_id,
        event_type: event_type.to_string(),
        page: None,
        duration_ms: None,
        element: None,
        depth: None,
        value: None,
    }
}

// FLOWIP-082a: Session tracking state (per user)
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct SessionData {
    event_count: usize,
    pages_viewed: Vec<String>,
    total_duration_ms: u64,
    clicks: usize,
    max_scroll_depth: u32,
}

impl TypedPayload for SessionData {
    const EVENT_TYPE: &'static str = "analytics.session_data";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SessionUpdate {
    key: String,
    result: SessionData,
}

impl TypedPayload for SessionUpdate {
    const EVENT_TYPE: &'static str = "analytics.session_update";
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

// FLOWIP-082a: Funnel tracking state
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct FunnelState {
    total_users: HashMap<String, bool>,
    funnel_stages: HashMap<String, usize>, // page -> visitor count
    conversions: Vec<f64>,
}

impl TypedPayload for FunnelState {
    const EVENT_TYPE: &'static str = "analytics.funnel_state";
    const SCHEMA_VERSION: u32 = 1;
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

// FLOWIP-082a: Overall metrics state
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct MetricsState {
    total_events: usize,
    events_by_type: HashMap<String, usize>,
    pages_by_popularity: HashMap<String, usize>,
    total_revenue: f64,
}

impl TypedPayload for MetricsState {
    const EVENT_TYPE: &'static str = "analytics.metrics_state";
    const SCHEMA_VERSION: u32 = 1;
}

impl UserEvent {
    // Pure function: Update overall metrics
    fn update_metrics(&self, state: &mut MetricsState) {
        state.total_events += 1;

        *state
            .events_by_type
            .entry(self.event_type.clone())
            .or_insert(0) += 1;

        if let Some(ref page) = self.page {
            *state.pages_by_popularity.entry(page.clone()).or_insert(0) += 1;
        }

        if let Some(value) = self.value {
            state.total_revenue += value;
        }
    }
}

fn main() -> Result<()> {
    let presentation = Presentation::new(
        Banner::new("Web Analytics Pipeline")
            .description("Processing user behaviour events with typed accumulators.")
            .bullets(
                "Pipeline",
                [
                    "Session Tracker -> GroupByTyped + TimeWindow(3s)",
                    "Funnel Tracker -> ReduceTyped + EveryN(50)",
                    "Metrics -> ReduceTyped + OnEOF",
                ],
            )
            .section("Why", "Zero ChainEvent manipulation, all type-safe!"),
    )
    .with_footer(|outcome| {
        outcome
            .into_footer()
            .bullets(
                "FLOWIP-082a TypedPayload",
                [
                    "UserEvent::EVENT_TYPE instead of \"user_event\"",
                    "SCHEMA_VERSION for all event types",
                    "Strongly-typed event structs",
                ],
            )
            .bullets(
                "FLOWIP-080j Typed accumulators",
                [
                    "GroupByTyped: Type-safe per-user session tracking",
                    "ReduceTyped: Type-safe funnel and metrics aggregation",
                    "Zero ChainEvent manipulation in business logic",
                    "Pure update functions: update_session(), update_funnel(), update_metrics()",
                    "~150 lines of custom StatefulHandler code eliminated",
                ],
            )
    });

    FlowApplication::builder()
        .with_presentation(presentation)
        .run_blocking(FlowDefinition::materialize(move |_runtime_config| {
            let user_events_handler = sources::finite_from_fn(move |index| {
                if index >= 200 {
                    return None;
                }

                let user_id = USERS[index % USERS.len()].to_string();
                Some(match index % 20 {
                    0..=5 => UserEvent {
                        page: Some(PAGES[0].to_string()),
                        duration_ms: Some((3000 + (index * 17) % 2000) as u64),
                        ..base_event(user_id, "page_view")
                    },
                    6..=9 => UserEvent {
                        page: Some(PAGES[1].to_string()),
                        duration_ms: Some((5000 + (index * 23) % 5000) as u64),
                        ..base_event(user_id, "page_view")
                    },
                    10..=12 => UserEvent {
                        element: Some("product_card".to_string()),
                        page: Some(PAGES[1].to_string()),
                        ..base_event(user_id, "click")
                    },
                    13 | 14 => UserEvent {
                        page: Some(PAGES[2].to_string()),
                        duration_ms: Some((2000 + (index * 13) % 1000) as u64),
                        ..base_event(user_id, "page_view")
                    },
                    15 => UserEvent {
                        depth: Some(50 + (index % 50) as u32),
                        page: Some(PAGES[1].to_string()),
                        ..base_event(user_id, "scroll")
                    },
                    16 => UserEvent {
                        element: Some("checkout_button".to_string()),
                        page: Some(PAGES[2].to_string()),
                        ..base_event(user_id, "click")
                    },
                    17 => UserEvent {
                        page: Some(PAGES[4].to_string()),
                        duration_ms: Some((1000 + (index * 7) % 500) as u64),
                        ..base_event(user_id, "page_view")
                    },
                    18 => UserEvent {
                        value: Some(49.99 + (index as f64 * 1.23) % 150.0),
                        ..base_event(user_id, "conversion")
                    },
                    _ => UserEvent {
                        page: Some(PAGES[1].to_string()),
                        duration_ms: Some((4000 + (index * 19) % 3000) as u64),
                        ..base_event(user_id, "page_view")
                    },
                })
            });
            let session_tracker_handler = stateful::group_by(
                |event: &UserEvent| event.user_id.clone(),
                |session: &mut SessionData, event: &UserEvent| {
                    event.update_session(session);
                },
                |key: &String, result: &SessionData| SessionUpdate {
                    key: key.clone(),
                    result: result.clone(),
                },
            )
            .emit_within(Duration::from_secs(3));
            let funnel_tracker_handler = stateful::reduce(
                FunnelState::default(),
                |state: &mut FunnelState, event: &UserEvent| {
                    event.update_funnel(state);
                },
            )
            .emit_every_n(50);
            let metrics_handler = stateful::reduce(
                MetricsState::default(),
                |state: &mut MetricsState, event: &UserEvent| {
                    event.update_metrics(state);
                },
            )
            .emit_on_eof();
            let sessions_sink = SinkTyped::new(|update: SessionUpdate| async move {
                println!("\n📊 [Sessions] Session Update:");
                println!("   User: {}", update.key);
                println!("   - Events: {}", update.result.event_count);
                println!("   - Pages: {}", update.result.pages_viewed.len());
                println!("   - Clicks: {}", update.result.clicks);
                println!("   - Duration: {}ms", update.result.total_duration_ms);
            })
            .idempotent();
            let funnel_sink = SinkTyped::new(|funnel: FunnelState| async move {
                println!("\n🎯 [Funnel] Funnel Progress:");
                println!("   Unique users: {}", funnel.total_users.len());

                let home = *funnel.funnel_stages.get("/home").unwrap_or(&0);
                let products = *funnel.funnel_stages.get("/products").unwrap_or(&0);
                let cart = *funnel.funnel_stages.get("/cart").unwrap_or(&0);
                let conversions = funnel.conversions.len();
                let total_revenue: f64 = funnel.conversions.iter().copied().sum();

                if home > 0 {
                    println!(
                        "   Home → Product: {:.1}%",
                        (products as f64 / home as f64) * 100.0
                    );
                }
                if products > 0 {
                    println!(
                        "   Product → Cart: {:.1}%",
                        (cart as f64 / products as f64) * 100.0
                    );
                }
                if cart > 0 {
                    println!(
                        "   Cart → Purchase: {:.1}%",
                        (conversions as f64 / cart as f64) * 100.0
                    );
                }
                println!("   Revenue: ${total_revenue:.2}");
            })
            .idempotent();
            let metrics_sink = SinkTyped::new(|metrics: MetricsState| async move {
                println!("\n📈 [Metrics] Daily Summary:");
                println!("   Total events: {}", metrics.total_events);
                println!("   Total revenue: ${:.2}", metrics.total_revenue);
                println!("   Event types: {} unique", metrics.events_by_type.len());
            })
            .idempotent();

            Ok(flow! {
            name: "web_analytics",
            journals: disk_journals(std::path::PathBuf::from("target/web_analytics")),

            stages: {
                user_events = source!(UserEvent => user_events_handler);
                session_tracker = stateful!(UserEvent -> SessionUpdate => session_tracker_handler);
                funnel_tracker = stateful!(UserEvent -> FunnelState => funnel_tracker_handler);
                metrics = stateful!(UserEvent -> MetricsState => metrics_handler);
                sessions = sink!(SessionUpdate => sessions_sink);
                funnel = sink!(FunnelState => funnel_sink);
                metrics_printer = sink!(MetricsState => metrics_sink);
            },

            topology: {
                // Fan out to all analyzers
                user_events |> session_tracker;
                user_events |> funnel_tracker;
                user_events |> metrics;

                // Each analyzer to its sink
                session_tracker |> sessions;
                funnel_tracker |> funnel;
                metrics |> metrics_printer;
            }
        })
        }))?;

    Ok(())
}
