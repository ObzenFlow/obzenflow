//! Simple Cryptocurrency Market Simulator
//! 
//! This example simulates a crypto trading market,
//! analyzing prices and detecting patterns using the new EventStore architecture.

use flowstate_rs::prelude::*;
use flowstate_rs::flow;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use chrono;

/// Generates realistic cryptocurrency market data
struct CryptoMarketSource {
    total_events: u64,
    emitted: AtomicU64,
    metrics: <RED as Taxonomy>::Metrics,
}

impl CryptoMarketSource {
    fn new(total_events: u64) -> Self {
        Self {
            total_events,
            emitted: AtomicU64::new(0),
            metrics: RED::create_metrics("CryptoMarketSource"),
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
            "BTC" => 1000000.0,
            "ETH" => 500000.0,
            _ => 100000.0,
        };
        // Create whale events approximately every 15-20 ticks
        let whale_multiplier = if (tick * 7) % 17 == 0 { 8.0 } else { 1.0 };
        let volume = base_volume * (1.0 + ((tick * 13) % 50) as f64 / 100.0) * whale_multiplier;
        
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

impl Step for CryptoMarketSource {
    type Taxonomy = RED;
    
    fn taxonomy(&self) -> &Self::Taxonomy {
        &RED
    }
    
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &self.metrics
    }
    
    fn step_type(&self) -> StepType {
        StepType::Source
    }
    
    fn handle(&self, _event: ChainEvent) -> Vec<ChainEvent> {
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
}

/// Analyzes price movements
struct PriceAnalyzer {
    metrics: <GoldenSignals as Taxonomy>::Metrics,
}

impl PriceAnalyzer {
    fn new() -> Self {
        Self {
            metrics: GoldenSignals::create_metrics("PriceAnalyzer"),
        }
    }
}

impl Step for PriceAnalyzer {
    type Taxonomy = GoldenSignals;
    
    fn taxonomy(&self) -> &Self::Taxonomy {
        &GoldenSignals
    }
    
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &self.metrics
    }
    
    fn step_type(&self) -> StepType {
        StepType::Stage
    }
    
    fn handle(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type == "MarketTick" {
            if let Some(price) = event.payload.get("price").and_then(|v| v.as_f64()) {
                // Simple price analysis
                let category = if price > 10000.0 {
                    "high"
                } else if price > 100.0 {
                    "medium"
                } else {
                    "low"
                };
                event.payload["price_category"] = json!(category);
                
                // Print significant price movements
                if let Some(tick) = event.payload.get("tick").and_then(|v| v.as_u64()) {
                    if tick % 20 == 0 {
                        println!("📈 {} @ ${:.2} ({})", 
                            event.payload.get("coin").and_then(|v| v.as_str()).unwrap_or("???"),
                            price,
                            category
                        );
                    }
                }
                
                // Record successful processing
                self.metrics.record_success(std::time::Duration::from_micros(100));
            }
        }
        vec![event]
    }
}

/// Detects volume anomalies
struct VolumeDetector {
    metrics: <USE as Taxonomy>::Metrics,
}

impl VolumeDetector {
    fn new() -> Self {
        Self {
            metrics: USE::create_metrics("VolumeDetector"),
        }
    }
}

impl Step for VolumeDetector {
    type Taxonomy = USE;
    
    fn taxonomy(&self) -> &Self::Taxonomy {
        &USE
    }
    
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &self.metrics
    }
    
    fn step_type(&self) -> StepType {
        StepType::Stage
    }
    
    fn handle(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type == "MarketTick" {
            if let Some(volume) = event.payload.get("volume").and_then(|v| v.as_f64()) {
                // Detect high volume (potential whale activity)
                let is_high_volume = volume > 5000000.0;
                event.payload["high_volume"] = json!(is_high_volume);
                
                if is_high_volume {
                    println!("🐋 Whale alert! {} volume: ${:.0} (tick #{})", 
                        event.payload.get("coin").and_then(|v| v.as_str()).unwrap_or("???"),
                        volume,
                        event.payload.get("tick").and_then(|v| v.as_u64()).unwrap_or(0)
                    );
                }
                
                // Track utilization based on volume processing
                self.metrics.start_work();
                // Simulate some work
                std::thread::sleep(std::time::Duration::from_micros(10));
                self.metrics.end_work();
            }
        }
        vec![event]
    }
}

/// Aggregates market statistics
struct MarketAggregator {
    stats: Arc<tokio::sync::Mutex<MarketStats>>,
    metrics: <SAAFE as Taxonomy>::Metrics,
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
            metrics: SAAFE::create_metrics("MarketAggregator"),
        }, stats)
    }
}

impl Step for MarketAggregator {
    type Taxonomy = SAAFE;
    
    fn taxonomy(&self) -> &Self::Taxonomy {
        &SAAFE
    }
    
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &self.metrics
    }
    
    fn step_type(&self) -> StepType {
        StepType::Sink
    }
    
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
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
        vec![] // Sinks don't emit
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("🪙 FlowState RS - Crypto Market Simulator");
    println!("=========================================");
    println!("📊 Simulating cryptocurrency market...");
    println!("");
    
    let (aggregator, stats) = MarketAggregator::new();
    
    // Run the flow
    let handle = flow! {
        name: "crypto_market",
        flow_taxonomy: GoldenSignals,
        ("market" => CryptoMarketSource::new(100), RED)  // 100 market events
        |> ("analyzer" => PriceAnalyzer::new(), GoldenSignals)
        |> ("detector" => VolumeDetector::new(), USE) 
        |> ("aggregator" => aggregator, SAAFE)
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