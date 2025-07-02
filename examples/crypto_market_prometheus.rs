//! Cryptocurrency Market Simulator Example with Prometheus Export
//! 
//! This example shows a FlowState pipeline that:
//! 1. Generates simulated crypto market data 
//! 2. Analyzes price movements with different taxonomies
//! 3. Demonstrates the monitoring system architecture

use obzenflow_dsl_infra::{flow, source, transform, sink};
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, TransformHandler, SinkHandler
};
use obzenflow_infra::journal::DiskJournal;
use obzenflow_core::event::event_id::EventId;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::journal::writer_id::WriterId;
use obzenflow_adapters::monitoring::taxonomies::{
    golden_signals::GoldenSignals,
    red::RED,
    use_taxonomy::USE,
    saafe::SAAFE,
};
use serde_json::json;
use std::sync::Arc;
use anyhow::Result;
use async_trait::async_trait;

/// Source that generates crypto market data
struct CryptoMarketSource {
    total_events: u64,
    current_tick: usize,
    writer_id: WriterId,
}

impl CryptoMarketSource {
    fn new(total_events: u64) -> Self {
        Self {
            total_events,
            current_tick: 0,
            writer_id: WriterId::new(),
        }
    }
    
    fn generate_market_event(&self, tick: u64) -> ChainEvent {
        // Simulate multiple cryptocurrencies
        let coins = ["BTC", "ETH", "SOL", "AVAX", "MATIC"];
        let coin = coins[tick as usize % coins.len()];
        
        // Base prices with some variation
        let base_price = match coin {
            "BTC" => 45000.0,
            "ETH" => 2800.0,
            "SOL" => 120.0,
            "AVAX" => 40.0,
            "MATIC" => 1.2,
            _ => 100.0,
        };
        
        // Add simple price movements
        let time_factor = (tick as f64 * 0.1).sin();
        let volatility = ((tick * 17) % 100) as f64 / 5000.0 - 0.01; // Pseudo-random ±1%
        let price = base_price * (1.0 + time_factor * 0.1 + volatility);
        
        // Generate volume with occasional whale activity
        let base_volume = match coin {
            "BTC" => 0.1,
            "ETH" => 1.0,
            _ => 10.0,
        };
        let is_whale = tick % 37 == 0;
        let volume = if is_whale { base_volume * 100.0 } else { base_volume * (1.0 + volatility.abs() * 10.0) };
        
        ChainEvent::new(
            EventId::new(),
            self.writer_id.clone(),
            "MarketTick",
            json!({
                "coin": coin,
                "price": price,
                "volume": volume,
                "bid": price * 0.999,
                "ask": price * 1.001,
                "timestamp": format!("2024-01-01T00:00:{}Z", tick % 60),
                "tick": tick,
            })
        )
    }
}

impl FiniteSourceHandler for CryptoMarketSource {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.current_tick < self.total_events as usize {
            let tick = self.current_tick as u64;
            self.current_tick += 1;
            
            // Show progress
            if tick == 0 {
                println!("🚀 Market data generation started!");
            } else if tick % 25 == 0 {
                println!("📊 Generated {} market events...", tick);
            }
            
            Some(self.generate_market_event(tick))
        } else {
            println!("✨ Market data generation complete! {} events", self.total_events);
            None
        }
    }
    
    fn is_complete(&self) -> bool {
        self.current_tick >= self.total_events as usize
    }
}

/// Analyzes price movements
struct PriceAnalyzer;

impl PriceAnalyzer {
    fn new() -> Self {
        Self
    }
}

impl TransformHandler for PriceAnalyzer {
    fn process(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type == "MarketTick" {
            // Calculate price movement indicators
            if let (Some(_coin), Some(_price)) = (
                event.payload.get("coin").and_then(|v| v.as_str()),
                event.payload.get("price").and_then(|v| v.as_f64())
            ) {
                // Simulate simple moving average comparison
                let ma_factor = ((event.payload["tick"].as_u64().unwrap_or(0) * 13) % 20) as f64 / 10.0 - 1.0;
                let trend = if ma_factor > 0.5 { "bullish" } else if ma_factor < -0.5 { "bearish" } else { "neutral" };
                
                // Add analysis results
                event.payload["trend"] = json!(trend);
                event.payload["strength"] = json!(ma_factor.abs());
                event.payload["signal"] = json!(if ma_factor.abs() > 0.8 { "strong" } else { "weak" });
                
                // Calculate RSI-like indicator
                let rsi = 50.0 + ma_factor * 30.0;
                event.payload["rsi"] = json!(rsi);
                event.payload["oversold"] = json!(rsi < 30.0);
                event.payload["overbought"] = json!(rsi > 70.0);
            }
        }
        vec![event]
    }
}

/// Detects volume spikes
struct VolumeDetector {
    average_volume: f64,
}

impl VolumeDetector {
    fn new() -> Self {
        Self {
            average_volume: 0.03, // 3% baseline volume ratio
        }
    }
}

impl TransformHandler for VolumeDetector {
    fn process(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type == "MarketTick" {
            if let Some(volume) = event.payload.get("volume").and_then(|v| v.as_f64()) {
                // Detect volume anomalies
                let volume_ratio = volume / (event.payload["price"].as_f64().unwrap_or(1.0) * self.average_volume);
                let is_spike = volume_ratio > 10.0;
                
                event.payload["volume_ratio"] = json!(volume_ratio);
                event.payload["high_volume"] = json!(is_spike);
                
                // Classify volume pattern
                let volume_type = match volume_ratio {
                    r if r > 50.0 => "whale_activity",
                    r if r > 10.0 => "high_volume", 
                    r if r > 2.0 => "above_average",
                    r if r < 0.5 => "low_volume",
                    _ => "normal"
                };
                event.payload["volume_type"] = json!(volume_type);
            }
        }
        vec![event]
    }
}

/// Aggregates market statistics
struct MarketAggregator {
    stats: Arc<std::sync::Mutex<MarketStats>>,
}

#[derive(Default)]
struct MarketStats {
    total_volume: f64,
    events_processed: u64,
    high_volume_events: u64,
}

impl MarketAggregator {
    fn new() -> Self {
        Self {
            stats: Arc::new(std::sync::Mutex::new(MarketStats::default())),
        }
    }
}

#[async_trait]
impl SinkHandler for MarketAggregator {
    fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<()> {
        if event.event_type == "MarketTick" {
            // Process synchronously to avoid race conditions
            let volume = event.payload.get("volume").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let is_high_volume = event.payload.get("high_volume").and_then(|v| v.as_bool()).unwrap_or(false);
            
            // Use blocking lock since this is quick
            if let Ok(mut stats_guard) = self.stats.lock() {
                stats_guard.events_processed += 1;
                stats_guard.total_volume += volume;
                
                if is_high_volume {
                    stats_guard.high_volume_events += 1;
                }
                
                // Debug: print first few volume updates
                if stats_guard.events_processed <= 5 {
                    println!("DEBUG: Event {}: volume={}, total_volume={}", 
                        stats_guard.events_processed, volume, stats_guard.total_volume);
                }
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing for better error messages
    tracing_subscriber::fmt()
        .with_env_filter("obzenflow=debug,crypto_market_prometheus=debug")
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .init();

    println!("🪙 FlowState RS - Crypto Market Analysis");
    println!("=========================================");
    println!("📊 Simulating cryptocurrency market...");
    println!("");
    
    let aggregator = MarketAggregator::new();
    let stats = aggregator.stats.clone();
    
    println!("⏳ Initializing pipeline...");
    
    // Create a journal for the flow
    let journal_path = std::path::PathBuf::from("target/crypto-market-logs");
    std::fs::create_dir_all(&journal_path)?;
    let journal = Arc::new(DiskJournal::new(journal_path, "crypto_market").await?);
    
    // Create the flow using the new flow! macro
    let handle = flow! {
        journal: journal,
        middleware: [GoldenSignals::monitoring()],
        
        stages: {
            market = source!("market" => CryptoMarketSource::new(100), [RED::monitoring()]);  // 100 market events
            analyzer = transform!("analyzer" => PriceAnalyzer::new(), [GoldenSignals::monitoring()]);
            detector = transform!("detector" => VolumeDetector::new(), [USE::monitoring()]);
            agg = sink!("aggregator" => aggregator, [SAAFE::monitoring()]);
        },
        
        topology: {
            market |> analyzer;
            analyzer |> detector;
            detector |> agg;
        }
    }.await.map_err(|e| anyhow::anyhow!("Failed to create flow with DSL: {:?}", e))?;
    
    println!("📌 Pipeline created, starting execution...");
    
    // Start the pipeline
    handle.run().await
        .map_err(|e| anyhow::anyhow!("Failed to run pipeline: {:?}", e))?;
    
    // Small delay to ensure all stats are updated
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // Print final statistics
    println!("\n✅ Market simulation completed!");
    
    let final_stats = stats.lock().unwrap();
    println!("📊 Statistics:");
    println!("   - Events processed: {}", final_stats.events_processed);
    println!("   - Total volume: ${:.2}M", final_stats.total_volume / 1_000_000.0);
    println!("   - High volume events: {}", final_stats.high_volume_events);
    
    // Cleanup
    println!("\nJournal written to: target/crypto-market-logs/crypto_market.log");
    Ok(())
}