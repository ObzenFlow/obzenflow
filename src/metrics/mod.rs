// src/metrics/mod.rs
use crate::step::{MetricsSnapshot, Result};
use async_trait::async_trait;
use std::sync::Arc;

// For feature-gated dependencies
#[cfg(feature = "metrics-prometheus")]
use prometheus::{Registry, Counter, HistogramOpts, HistogramVec};

#[cfg(feature = "metrics-statsd")]
use statsd::Client as StatsdClient;

/// Trait for exporting metrics to external systems
#[async_trait]
pub trait MetricsExporter: Send + Sync {
    async fn export(&self, snapshot: &MetricsSnapshot) -> Result<()>;
    async fn export_batch(&self, snapshots: &[MetricsSnapshot]) -> Result<()> {
        for snapshot in snapshots {
            self.export(snapshot).await?;
        }
        Ok(())
    }
}

/// Prometheus exporter
#[cfg(feature = "metrics-prometheus")]
pub struct PrometheusExporter {
    registry: Registry,
    processed_counter: Counter,
    emitted_counter: Counter,
    error_counter: Counter,
    processing_time: HistogramVec,
}

#[cfg(feature = "metrics-prometheus")]
impl PrometheusExporter {
    pub fn new(registry: Registry) -> Result<Self> {
        let processed_counter = Counter::new(
            "flowstate_events_processed_total",
            "Total events processed"
        )?;

        let emitted_counter = Counter::new(
            "flowstate_events_emitted_total",
            "Total events emitted"
        )?;

        let error_counter = Counter::new(
            "flowstate_errors_total",
            "Total errors"
        )?;

        let processing_time = HistogramVec::new(
            HistogramOpts::new(
                "flowstate_processing_duration_seconds",
                "Processing time in seconds"
            ).buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]),
            &["stage"]
        )?;

        registry.register(Box::new(processed_counter.clone()))?;
        registry.register(Box::new(emitted_counter.clone()))?;
        registry.register(Box::new(error_counter.clone()))?;
        registry.register(Box::new(processing_time.clone()))?;

        Ok(Self {
            registry,
            processed_counter,
            emitted_counter,
            error_counter,
            processing_time,
        })
    }

    pub fn registry(&self) -> &Registry {
        &self.registry
    }
}

#[cfg(feature = "metrics-prometheus")]
#[async_trait]
impl MetricsExporter for PrometheusExporter {
    async fn export(&self, snapshot: &MetricsSnapshot) -> Result<()> {
        self.processed_counter.inc_by(snapshot.processed as f64);
        self.emitted_counter.inc_by(snapshot.emitted as f64);
        self.error_counter.inc_by(snapshot.errors as f64);

        self.processing_time
            .with_label_values(&[&snapshot.name])
            .observe(snapshot.p95_latency_us as f64 / 1_000_000.0);

        Ok(())
    }
}

/// StatsD exporter
#[cfg(feature = "metrics-statsd")]
pub struct StatsdExporter {
    client: StatsdClient,
    prefix: String,
}

#[cfg(feature = "metrics-statsd")]
impl StatsdExporter {
    pub fn new(host: &str, prefix: &str) -> Result<Self> {
        let client = StatsdClient::new(host, 8125)?;
        Ok(Self {
            client,
            prefix: prefix.to_string(),
        })
    }
}

#[cfg(feature = "metrics-statsd")]
#[async_trait]
impl MetricsExporter for StatsdExporter {
    async fn export(&self, snapshot: &MetricsSnapshot) -> Result<()> {
        let prefix = format!("{}.{}", self.prefix, snapshot.name);

        self.client.count(&format!("{}.processed", prefix), snapshot.processed as i64);
        self.client.count(&format!("{}.emitted", prefix), snapshot.emitted as i64);
        self.client.count(&format!("{}.errors", prefix), snapshot.errors as i64);

        self.client.timer(&format!("{}.latency.p50", prefix), snapshot.p50_latency_us as i64);
        self.client.timer(&format!("{}.latency.p95", prefix), snapshot.p95_latency_us as i64);
        self.client.timer(&format!("{}.latency.p99", prefix), snapshot.p99_latency_us as i64);

        Ok(())
    }
}

/// Console exporter for development
pub struct ConsoleExporter {
    pretty: bool,
}

impl ConsoleExporter {
    pub fn new(pretty: bool) -> Self {
        Self { pretty }
    }
}

#[async_trait]
impl MetricsExporter for ConsoleExporter {
    async fn export(&self, snapshot: &MetricsSnapshot) -> Result<()> {
        if self.pretty {
            println!("┌─ Stage: {} ─────────────────────┐", snapshot.name);
            println!("│ Processed: {:>20} │", snapshot.processed);
            println!("│ Emitted:   {:>20} │", snapshot.emitted);
            println!("│ Errors:    {:>20} │", snapshot.errors);
            println!("│ Latency p50: {:>17} μs │", snapshot.p50_latency_us);
            println!("│ Latency p95: {:>17} μs │", snapshot.p95_latency_us);
            println!("│ Latency p99: {:>17} μs │", snapshot.p99_latency_us);
            println!("└────────────────────────────────┘");
        } else {
            println!("{}: processed={} emitted={} errors={} p95={}μs",
                snapshot.name, snapshot.processed, snapshot.emitted,
                snapshot.errors, snapshot.p95_latency_us
            );
        }
        Ok(())
    }
}

/// Enum wrapper to make MetricsExporter object-safe
pub enum MetricsExporterWrapper {
    Console(ConsoleExporter),
    #[cfg(feature = "metrics-prometheus")]
    Prometheus(PrometheusExporter),
    #[cfg(feature = "metrics-statsd")]
    Statsd(StatsdExporter),
}

#[async_trait]
impl MetricsExporter for MetricsExporterWrapper {
    async fn export(&self, snapshot: &MetricsSnapshot) -> Result<()> {
        match self {
            Self::Console(e) => e.export(snapshot).await,
            #[cfg(feature = "metrics-prometheus")]
            Self::Prometheus(e) => e.export(snapshot).await,
            #[cfg(feature = "metrics-statsd")]
            Self::Statsd(e) => e.export(snapshot).await,
        }
    }
}

// Helper to configure metrics export based on environment
pub fn metrics_exporter_from_env() -> Result<Arc<MetricsExporterWrapper>> {
    match std::env::var("METRICS_EXPORT").as_deref() {
        #[cfg(feature = "metrics-prometheus")]
        Ok("prometheus") => {
            let registry = Registry::new();
            Ok(Arc::new(MetricsExporterWrapper::Prometheus(
                PrometheusExporter::new(registry)?
            )))
        }
        #[cfg(feature = "metrics-statsd")]
        Ok("statsd") => {
            let host = std::env::var("STATSD_HOST").unwrap_or_else(|_| "localhost".to_string());
            Ok(Arc::new(MetricsExporterWrapper::Statsd(
                StatsdExporter::new(&host, "flowstate")?
            )))
        }
        _ => {
            Ok(Arc::new(MetricsExporterWrapper::Console(
                ConsoleExporter::new(true)
            )))
        }
    }
}
