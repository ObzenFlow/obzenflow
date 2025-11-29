//! TopN Leaderboard Demo - Using FLOWIP-080j & FLOWIP-082a
//!
//! Demonstrates the typed TopN accumulator for maintaining leaderboards
//! and "hottest items" lists with bounded memory usage.
//!
//! Run with: cargo run --package obzenflow --example top_n_leaderboard

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload},
    id::StageId,
    TypedPayload, WriterId,
};
use obzenflow_dsl_infra::{flow, sink, source, stateful};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handlers::{FiniteSourceHandler, SinkHandler};
use obzenflow_runtime_services::stages::stateful::strategies::accumulators::TopNTyped;
use serde::{Deserialize, Serialize};
use serde_json::json;

/// Domain type for game score events (FLOWIP-082a)
#[derive(Clone, Debug, Deserialize, Serialize)]
struct GameScore {
    player: String,
    score: f64,
    game_mode: String,
    timestamp: usize,
}

impl TypedPayload for GameScore {
    const EVENT_TYPE: &'static str = "game.score";
    const SCHEMA_VERSION: u32 = 1;
}

/// Source that generates player score events
#[derive(Clone, Debug)]
struct GameScoreSource {
    events: Vec<(String, f64, String)>, // (player, score, game_mode)
    current_index: usize,
    writer_id: WriterId,
}

impl GameScoreSource {
    fn new() -> Self {
        // Simulate player scores from various game modes
        let events = vec![
            ("Alice".to_string(), 1500.0, "Battle".to_string()),
            ("Bob".to_string(), 2200.0, "Battle".to_string()),
            ("Charlie".to_string(), 1800.0, "Racing".to_string()),
            ("David".to_string(), 900.0, "Battle".to_string()),
            ("Eve".to_string(), 3100.0, "Racing".to_string()),
            ("Frank".to_string(), 2500.0, "Battle".to_string()),
            ("Grace".to_string(), 1200.0, "Racing".to_string()),
            ("Henry".to_string(), 2800.0, "Battle".to_string()),
            ("Iris".to_string(), 1900.0, "Racing".to_string()),
            ("Jack".to_string(), 3500.0, "Battle".to_string()),
            // Some players play multiple times (score updates)
            ("Alice".to_string(), 2100.0, "Battle".to_string()), // Alice improves!
            ("Bob".to_string(), 1900.0, "Racing".to_string()),   // Bob tries racing
            ("Charlie".to_string(), 2400.0, "Racing".to_string()), // Charlie improves!
        ];

        Self {
            events,
            current_index: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for GameScoreSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, obzenflow_runtime_services::stages::common::handlers::source::traits::SourceError> {
        if self.current_index >= self.events.len() {
            return Ok(None);
        }

        let (player, score, game_mode) = &self.events[self.current_index];
        self.current_index += 1;

        println!(
            "📊 Score Update: {} scored {:.0} points in {} mode",
            player, score, game_mode
        );

        // ✨ FLOWIP-082a: Emit typed event using EVENT_TYPE constant
        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id.clone(),
            GameScore::EVENT_TYPE,
            json!({
                "player": player,
                "score": score,
                "game_mode": game_mode,
                "timestamp": self.current_index, // Simulated timestamp
            }),
        )]))
    }
}

/// Sink that displays the leaderboard
#[derive(Clone, Debug)]
struct LeaderboardDisplay;

impl LeaderboardDisplay {
    fn new() -> Self {
        Self
    }
}

#[async_trait]
impl SinkHandler for LeaderboardDisplay {
    async fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<DeliveryPayload> {
        // ✨ FLOWIP-082a: TopNTyped emits with input type's EVENT_TYPE
        if event.event_type() == GameScore::EVENT_TYPE {
            let payload = event.payload();
            let top_n = payload["top_n"].as_array().unwrap();
            let count = payload["count"].as_u64().unwrap();

            println!("\n🏆 LEADERBOARD UPDATE 🏆");
            println!("========================");
            println!("Total Players Tracked: {}\n", count);

            for (rank, entry) in top_n.iter().enumerate() {
                let player = entry["key"].as_str().unwrap();
                let score = entry["score"].as_f64().unwrap();
                let metadata = &entry["metadata"];
                let game_mode = metadata["game_mode"].as_str().unwrap_or("Unknown");

                let medal = match rank {
                    0 => "🥇",
                    1 => "🥈",
                    2 => "🥉",
                    _ => "  ",
                };

                println!(
                    "{} #{}: {} - {:.0} points ({})",
                    medal,
                    rank + 1,
                    player,
                    score,
                    game_mode
                );
            }
            println!("========================\n");
        }

        Ok(DeliveryPayload::success(
            "leaderboard",
            DeliveryMethod::Custom("Display".to_string()),
            None,
        ))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    std::env::set_var("OBZENFLOW_METRICS_EXPORTER", "console");

    println!("🎮 FlowState RS - TopN Leaderboard Demo");
    println!("=======================================");
    println!("✨ Using FLOWIP-080j TopNTyped & FLOWIP-082a TypedPayload");
    println!("");
    println!("This demo shows how TopNTyped maintains a leaderboard");
    println!("of the top 5 players by score with type-safe operations,");
    println!("automatically evicting lower scores as new high scores arrive.\n");

    println!("Starting game score stream...\n");

    FlowApplication::run(async {
        flow! {
            name: "leaderboard_demo",
            journals: disk_journals(std::path::PathBuf::from("target/leaderboard-logs")),
            middleware: [],

            stages: {
                scores = source!("scores" => GameScoreSource::new());

                // FLOWIP-080j: TopNTyped accumulator tracking top 5 players
                // Note: This takes the LATEST score for each player (key = player name)
                // When a player's score updates, it replaces the old entry
                leaderboard = stateful!("leaderboard" =>
                    TopNTyped::new(
                        5,
                        |score: &GameScore| score.player.clone(),  // Key: player name
                        |score: &GameScore| score.score            // Score: points earned
                    ).emit_on_eof()  // Emit final leaderboard at end
                );

                display = sink!("display" => LeaderboardDisplay::new());
            },

            topology: {
                scores |> leaderboard;
                leaderboard |> display;
            }
        }
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))
    })
    .await
    .map_err(|e| anyhow::anyhow!("Application failed: {:?}", e))?;

    println!("✅ Leaderboard demo completed!");
    println!("\n💡 Key Points:");
    println!("   FLOWIP-082a TypedPayload:");
    println!("   • GameScore::EVENT_TYPE instead of \"game.score\"");
    println!("   • SCHEMA_VERSION for evolution tracking");
    println!("");
    println!("   FLOWIP-080j TopNTyped:");
    println!("   • Maintains exactly N items in memory");
    println!("   • Automatically evicts lowest scores");
    println!("   • Perfect for leaderboards, trending items, hot keys");
    println!("   • O(N) memory usage regardless of stream size");
    println!("\n📝 Journal written to: target/leaderboard-logs/");

    Ok(())
}
