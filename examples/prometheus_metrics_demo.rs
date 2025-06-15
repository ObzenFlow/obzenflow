//! Simple Prometheus Metrics Demo
//! 
//! This example demonstrates our new FLOWIP-004 compliant Prometheus exporter
//! by creating various metrics, updating them over time, and serving them
//! via HTTP for Grafana to scrape.
//!
//! Run with: cargo run --example prometheus_metrics_demo --features metrics-prometheus
//! Then visit: http://localhost:3030/metrics

use flowstate_rs::monitoring::metrics::{
    RateMetric, ErrorMetric, DurationMetric, 
    SaturationMetric, UtilizationMetric, AnomalyMetric,
    NewMetric as Metric,
};
use flowstate_rs::monitoring::exporters::{PrometheusExporter, MetricExporter};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, interval};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🎯 FlowState RS - Prometheus Metrics Demo");
    println!("=========================================");
    println!("📊 Creating FLOWIP-004 compliant metrics...");
    
    // Create our FLOWIP-004 compliant metrics
    let rate_metric = Arc::new(RateMetric::new("api_requests"));
    let error_metric = Arc::new(ErrorMetric::new("api_errors"));  
    let duration_metric = Arc::new(DurationMetric::new("api_duration"));
    let utilization_metric = Arc::new(UtilizationMetric::new("cpu_utilization"));
    let saturation_metric = Arc::new(SaturationMetric::new("queue_saturation"));
    let anomaly_metric = Arc::new(AnomalyMetric::new("system_anomalies"));
    
    // Create the Prometheus exporter
    let exporter = Arc::new(PrometheusExporter::new());
    
    // Start HTTP server for metrics
    start_metrics_server(exporter.clone(), vec![
        rate_metric.clone(),
        error_metric.clone(),
        duration_metric.clone(),
        utilization_metric.clone(),
        saturation_metric.clone(),
        anomaly_metric.clone(),
    ]).await;
    
    println!("🌐 Metrics available at: http://localhost:3030/metrics");
    println!("📈 Starting metric simulation...");
    println!("💡 Try importing these metrics into Grafana!");
    println!("");
    
    // Simulate realistic application behavior
    simulate_application_metrics(
        rate_metric,
        error_metric, 
        duration_metric,
        utilization_metric,
        saturation_metric,
        anomaly_metric,
    ).await;
    
    Ok(())
}

async fn simulate_application_metrics(
    rate_metric: Arc<RateMetric>,
    error_metric: Arc<ErrorMetric>,
    duration_metric: Arc<DurationMetric>,
    utilization_metric: Arc<UtilizationMetric>,
    saturation_metric: Arc<SaturationMetric>,
    anomaly_metric: Arc<AnomalyMetric>,
) {
    let mut tick_interval = interval(Duration::from_millis(100));
    let mut tick = 0u64;
    let simulation_duration = Duration::from_secs(300); // 5 minutes
    let start_time = std::time::Instant::now();
    
    println!("🚀 Running 5-minute simulation...");
    
    while start_time.elapsed() < simulation_duration {
        tick_interval.tick().await;
        tick += 1;
        
        // Simulate request rate (10-50 requests per tick)
        let requests_this_tick = 10 + (tick % 40);
        
        // 5% error rate, but spike every 200 ticks
        let error_rate = if tick % 200 < 10 { 0.20 } else { 0.05 };
        
        for _ in 0..requests_this_tick {
            rate_metric.record_event();
            
            if fastrand::f64() < error_rate {
                error_metric.record_error();
            }
            
            // Simulate request duration (20-200ms)
            let base_duration = 50 + (tick % 150) as u64;
            let jitter = fastrand::u64(0..20);
            let duration = Duration::from_millis(base_duration + jitter);
            duration_metric.record_duration(duration);
        }
        
        // Simulate CPU utilization (oscillating pattern)
        let cpu_base = 0.3;
        let cpu_wave = 0.4 * ((tick as f64 * 0.02).sin() + 1.0) / 2.0;
        let cpu_noise = (fastrand::f64() - 0.5) * 0.1;
        let cpu_utilization = (cpu_base + cpu_wave + cpu_noise).clamp(0.0, 1.0);
        utilization_metric.set_utilization(cpu_utilization);
        
        // Simulate queue saturation (trending upward with spikes)
        let base_saturation = (tick as f64 / 3000.0).min(0.8);
        let spike = if tick % 150 < 20 { 0.3 } else { 0.0 };
        let saturation = (base_saturation + spike).clamp(0.0, 1.0);
        saturation_metric.set_ratio(saturation);
        
        // Detect anomalies (when things get weird)
        if error_rate > 0.1 || saturation > 0.9 || cpu_utilization > 0.95 {
            anomaly_metric.record_anomaly();
        }
        
        // Progress update every 10 seconds
        if tick % 100 == 0 {
            let elapsed = start_time.elapsed();
            let remaining = simulation_duration.saturating_sub(elapsed);
            let progress = (elapsed.as_secs_f64() / simulation_duration.as_secs_f64()) * 100.0;
            
            println!(
                "⏱️  Progress: {:.1}% | Requests: {} | Errors: {} | CPU: {:.1}% | Queue: {:.1}% ({:.0}s remaining)",
                progress,
                rate_metric.total_events(),
                error_metric.total_errors(),
                cpu_utilization * 100.0,
                saturation * 100.0,
                remaining.as_secs_f64()
            );
        }
    }
    
    // Final statistics
    println!("\n✅ Simulation completed!");
    println!("📊 Final Metrics:");
    println!("   📈 Total Requests: {}", rate_metric.total_events());
    println!("   ❌ Total Errors: {}", error_metric.total_errors());
    println!("   📉 Error Rate: {:.2}%", 
        (error_metric.total_errors() as f64 / rate_metric.total_events() as f64) * 100.0
    );
    println!("   ⚠️  Total Anomalies: {}", anomaly_metric.anomaly_count());
    println!("   🖥️  Final CPU: {:.1}%", utilization_metric.current_utilization() * 100.0);
    println!("   📦 Final Queue: {:.1}%", saturation_metric.current_ratio() * 100.0);
    println!("");
    println!("🎯 Metrics will continue to be served at http://localhost:3030/metrics");
    println!("   You can now:");
    println!("   • Import into Grafana");
    println!("   • Create dashboards");
    println!("   • Set up alerts");
    println!("   • Press Ctrl+C to exit");
    
    // Keep server running
    loop {
        sleep(Duration::from_secs(1)).await;
    }
}

async fn start_metrics_server(
    exporter: Arc<PrometheusExporter>,
    metrics: Vec<Arc<dyn Metric>>,
) {
    use warp::Filter;
    
    let metrics_for_endpoint = Arc::new(metrics);
    let exporter_for_endpoint = exporter.clone();
    
    let metrics_route = warp::path("metrics")
        .and(warp::get())
        .map(move || {
            let mut output = String::new();
            
            // Export all metrics
            for metric in metrics_for_endpoint.iter() {
                match exporter_for_endpoint.export(metric.as_ref()) {
                    Ok(formatted) => {
                        output.push_str(&formatted);
                        output.push('\n');
                    }
                    Err(e) => {
                        eprintln!("⚠️  Failed to export metric {}: {}", metric.name(), e);
                    }
                }
            }
            
            // Add some helpful comments
            output.push_str("# FlowState RS Demo Metrics\n");
            output.push_str("# These metrics demonstrate FLOWIP-004 compliant architecture\n");
            output.push_str("# - Zero locks (all atomic operations)\n");
            output.push_str("# - Clean primitive layer (Counter/Gauge/Histogram)\n");
            output.push_str("# - Domain metrics wrap primitives\n");
            output.push_str("# - Exporter handles format conversion\n");
            
            warp::reply::with_header(
                output,
                "Content-Type",
                "text/plain; version=0.0.4; charset=utf-8",
            )
        });
    
    // Health check endpoint
    let health = warp::path("health")
        .and(warp::get())
        .map(|| {
            warp::reply::json(&serde_json::json!({
                "status": "healthy",
                "service": "flowstate-rs-metrics",
                "architecture": "FLOWIP-004",
                "features": ["prometheus", "lock-free", "atomic-primitives"]
            }))
        });
    
    // Root endpoint with instructions
    let root = warp::path::end()
        .map(|| {
            warp::reply::html(
                r#"
                <!DOCTYPE html>
                <html>
                <head><title>FlowState RS Metrics</title></head>
                <body>
                    <h1>🎯 FlowState RS Metrics Server</h1>
                    <h2>Available Endpoints:</h2>
                    <ul>
                        <li><a href="/metrics">/metrics</a> - Prometheus metrics</li>
                        <li><a href="/health">/health</a> - Health check</li>
                    </ul>
                    <h2>Architecture:</h2>
                    <p>This server demonstrates FLOWIP-004 compliant metrics:</p>
                    <ul>
                        <li>✅ Zero locks (all atomic operations)</li>
                        <li>✅ Clean primitive layer (Counter/Gauge/Histogram)</li>
                        <li>✅ Domain metrics wrap primitives</li>
                        <li>✅ Exporter handles format conversion</li>
                    </ul>
                    <p>Perfect for Grafana dashboards and monitoring!</p>
                </body>
                </html>
                "#
            )
        });
    
    let routes = root.or(metrics_route).or(health);
    
    // Start server in background
    tokio::spawn(async move {
        warp::serve(routes)
            .run(([127, 0, 0, 1], 3030))
            .await;
    });
    
    // Give server time to start
    sleep(Duration::from_millis(100)).await;
    println!("🌐 HTTP server started on http://localhost:3030");
}