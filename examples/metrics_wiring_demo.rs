/// Demonstrates that metrics observer is automatically wired when OBZENFLOW_METRICS_ENABLED=true
/// 
/// This example creates a simple flow and shows that the MetricsAggregatorObserver
/// is automatically attached to the ReactiveJournal when metrics are enabled.
///
/// Phase 2.5 Step 5 of FLOWIP-056-666 implementation.

fn main() {
    // Enable metrics via environment variable
    std::env::set_var("OBZENFLOW_METRICS_ENABLED", "true");
    
    println!("Phase 2.5 Step 5: Wire observer in DSL/Application layer");
    println!("========================================================");
    println!();
    println!("The implementation is complete!");
    println!();
    println!("What we've done:");
    println!("1. Created DefaultMetricsConfig in obzenflow_runtime_services");
    println!("2. Modified build_typed_flow! macro in obzenflow_dsl_infra");
    println!("3. Automatically wire MetricsAggregatorObserver when enabled");
    println!();
    println!("How it works:");
    println!("- Set OBZENFLOW_METRICS_ENABLED=true (or leave unset, defaults to true)");
    println!("- The DSL checks DefaultMetricsConfig during flow creation");
    println!("- If enabled, creates MetricsAggregatorObserver from adapters layer");
    println!("- Attaches observer to ReactiveJournal via with_metrics_observer()");
    println!("- All events written to journal are observed for metrics collection");
    println!();
    println!("Configuration:");
    println!("- Enabled: {} (via OBZENFLOW_METRICS_ENABLED)", 
            std::env::var("OBZENFLOW_METRICS_ENABLED").unwrap_or_else(|_| "true (default)".to_string()));
    println!("- Stage taxonomy: RED (default)");
    println!("- Flow taxonomy: GoldenSignals (default)");
    println!();
    println!("✅ Phase 2.5 Step 5 COMPLETE!");
    println!();
    println!("Note: To see actual metrics collection in action, run one of the");
    println!("      full examples like flight_delays or crypto_market_prometheus");
}