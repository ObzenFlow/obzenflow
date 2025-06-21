//! Cryptocurrency Market Simulator Example with Prometheus Export
//! 
//! This example shows a FlowState pipeline that:
//! 1. Generates simulated crypto market data 
//! 2. Analyzes price movements with different taxonomies
//! 3. Demonstrates the monitoring system architecture

use flowstate_rs::prelude::*;
use flowstate_rs::flow;
use flowstate_rs::lifecycle::{EventHandler, ProcessingMode};
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Source that generates crypto market data
struct CryptoMarketSource {
    total_events: u64,
    emitted: AtomicU64,
}

impl CryptoMarketSource {
    fn new(total_events: u64) -> Self {
        Self {
            total_events,
            emitted: AtomicU64::new(0),
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
        
        ChainEvent::new("MarketTick", json!({
            "coin": coin,
            "price": price,
            "volume": volume,
            "bid": price * 0.999,
            "ask": price * 1.001,
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "tick": tick,
        }))
    }
}

impl EventHandler for CryptoMarketSource {
    fn transform(&self, _event: ChainEvent) -> Vec<ChainEvent> {
        let tick = self.emitted.fetch_add(1, Ordering::Relaxed);
        if tick < self.total_events {
            // Show progress
            if tick == 0 {
                println!("🚀 Market data generation started!");
            } else if tick % 25 == 0 {
                println!("📊 Generated {} market events...", tick);
            }
            vec![self.generate_market_event(tick)]
        } else {
            if tick == self.total_events {
                println!("✨ Market data generation complete! {} events", self.total_events);
            }
            vec![]
        }
    }
    
    fn processing_mode(&self) -> ProcessingMode {
        ProcessingMode::Transform
    }
}

/// Analyzes price movements
struct PriceAnalyzer;

impl PriceAnalyzer {
    fn new() -> Self {
        Self
    }
}

impl EventHandler for PriceAnalyzer {
    fn transform(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type == "MarketTick" {
            // Calculate price movement indicators
            if let (Some(coin), Some(price)) = (
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
    
    fn processing_mode(&self) -> ProcessingMode {
        ProcessingMode::Transform
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

impl EventHandler for VolumeDetector {
    fn transform(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
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
    
    fn processing_mode(&self) -> ProcessingMode {
        ProcessingMode::Transform
    }
}

/// Aggregates market statistics
struct MarketAggregator {
    stats: Arc<tokio::sync::Mutex<MarketStats>>,
}

#[derive(Default)]
struct MarketStats {
    total_volume: f64,
    events_processed: u64,
    high_volume_events: u64,
}

impl MarketAggregator {
    fn new() -> (Self, Arc<tokio::sync::Mutex<MarketStats>>) {
        let stats = Arc::new(tokio::sync::Mutex::new(MarketStats::default()));
        (Self {
            stats: stats.clone(),
        }, stats)
    }
}

impl EventHandler for MarketAggregator {
    fn transform(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type == "MarketTick" {
            let stats = self.stats.clone();
            tokio::spawn(async move {
                let mut stats = stats.lock().await;
                stats.events_processed += 1;
                
                if let Some(volume) = event.payload.get("volume").and_then(|v| v.as_f64()) {
                    stats.total_volume += volume;
                }
                
                if event.payload.get("high_volume").and_then(|v| v.as_bool()).unwrap_or(false) {
                    stats.high_volume_events += 1;
                }
            });
        }
        vec![] // Sink consumes events
    }
    
    fn processing_mode(&self) -> ProcessingMode {
        ProcessingMode::Transform
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("🪙 FlowState RS - Crypto Market Analysis");
    println!("=========================================");
    println!("📊 Simulating cryptocurrency market...");
    println!("");
    
    let (aggregator, stats) = MarketAggregator::new();
    
    // Run the flow
    let handle = flow! {
        name: "crypto_market",
        flow_taxonomy: GoldenSignals,
        ("market" => CryptoMarketSource::new(100), [RED::monitoring()])  // 100 market events
        |> ("analyzer" => PriceAnalyzer::new(), [GoldenSignals::monitoring()])
        |> ("detector" => VolumeDetector::new(), [USE::monitoring()]) 
        |> ("aggregator" => aggregator, [SAAFE::monitoring()])
    }?;
    
    // CRITICAL: Allow time for subscriptions to be established
    // This prevents the race condition where events are emitted before stages subscribe
    println!("⏳ Initializing market data pipeline...");
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // Let it run
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    
    // Shutdown gracefully
    handle.shutdown().await?;
    
    // Print final statistics
    let final_stats = stats.lock().await;
    println!("\n✅ Market simulation completed!");
    println!("📊 Statistics:");
    println!("   - Events processed: {}", final_stats.events_processed);
    println!("   - Total volume: ${:.2}M", final_stats.total_volume / 1_000_000.0);
    println!("   - High volume events: {}", final_stats.high_volume_events);
    
    // Cleanup
    // Cleanup handled by tempdir
    Ok(())
}