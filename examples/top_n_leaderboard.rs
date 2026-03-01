// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! TopN Leaderboard Demo - Using FLOWIP-080j & FLOWIP-082a
//!
//! Demonstrates the typed TopN accumulator for maintaining leaderboards
//! and "hottest items" lists with bounded memory usage.
//!
//! Run with: cargo run --package obzenflow --example top_n_leaderboard

use anyhow::Result;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, stateful};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::source::FiniteSourceTyped;
use obzenflow_runtime::stages::stateful::strategies::accumulators::TopNTyped;
use serde::{Deserialize, Serialize};

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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct LeaderboardEntry {
    rank: usize,
    key: String,
    score: f64,
    metadata: GameScore,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct LeaderboardUpdate {
    top_n: Vec<LeaderboardEntry>,
    capacity: usize,
    count: usize,
}

impl TypedPayload for LeaderboardUpdate {
    const EVENT_TYPE: &'static str = GameScore::EVENT_TYPE;
    const SCHEMA_VERSION: u32 = GameScore::SCHEMA_VERSION;
}

#[tokio::main]
async fn main() -> Result<()> {
    std::env::set_var("OBZENFLOW_METRICS_EXPORTER", "console");

    println!("🎮 FlowState RS - TopN Leaderboard Demo");
    println!("=======================================");
    println!("✨ Using FLOWIP-080j TopNTyped & FLOWIP-082a TypedPayload");
    println!();
    println!("This demo shows how TopNTyped maintains a leaderboard");
    println!("of the top 5 players by score with type-safe operations,");
    println!("automatically evicting lower scores as new high scores arrive.\n");

    println!("Starting game score stream...\n");

    // Simulate player scores from various game modes
    let score_events: Vec<(String, f64, String)> = vec![
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

    FlowApplication::run(flow! {
        name: "leaderboard_demo",
        journals: disk_journals(std::path::PathBuf::from("target/leaderboard-logs")),
        middleware: [],

            stages: {
                // FLOWIP-081: Typed finite sources (no WriterId/ChainEvent boilerplate)
                scores = source!("scores" => FiniteSourceTyped::from_item_fn(move |index| {
                    let (player, score, game_mode) = score_events.get(index)?;

                    println!("📊 Score Update: {player} scored {score:.0} points in {game_mode} mode");

                    Some(GameScore {
                        player: player.clone(),
                        score: *score,
                        game_mode: game_mode.clone(),
                        timestamp: index + 1, // Simulated timestamp
                    })
                }));

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

                display = sink!("display" => |update: LeaderboardUpdate| {
                    println!("\n🏆 LEADERBOARD UPDATE 🏆");
                    println!("========================");
                    println!("Total Players Tracked: {}\n", update.count);

                    for entry in &update.top_n {
                        let medal = match entry.rank {
                            1 => "🥇",
                            2 => "🥈",
                            3 => "🥉",
                            _ => "  ",
                        };

                        println!(
                            "{} #{}: {} - {:.0} points ({})",
                            medal,
                            entry.rank,
                            entry.key,
                            entry.score,
                            entry.metadata.game_mode
                        );
                    }
                    println!("========================\n");
                });
            },

            topology: {
                scores |> leaderboard;
                leaderboard |> display;
            }
    })
    .await?;

    println!("✅ Leaderboard demo completed!");
    println!("\n💡 Key Points:");
    println!("   FLOWIP-082a TypedPayload:");
    println!("   • GameScore::EVENT_TYPE instead of \"game.score\"");
    println!("   • SCHEMA_VERSION for evolution tracking");
    println!();
    println!("   FLOWIP-080j TopNTyped:");
    println!("   • Maintains exactly N items in memory");
    println!("   • Automatically evicts lowest scores");
    println!("   • Perfect for leaderboards, trending items, hot keys");
    println!("   • O(N) memory usage regardless of stream size");
    println!("\n📝 Journal written to: target/leaderboard-logs/");

    Ok(())
}
