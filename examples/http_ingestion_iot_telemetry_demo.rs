//! HTTP Ingestion IoT Telemetry Demo (FLOWIP-084d)
//!
//! Run with:
//! `cargo run -p obzenflow --example http_ingestion_iot_telemetry_demo --features obzenflow_infra/warp-server -- --server --server-port 9090`
//!
//! Then submit telemetry samples:
//! - Valid:
//!   `curl -XPOST http://127.0.0.1:9090/api/telemetry/events -H 'content-type: application/json' -d '{"event_type":"telemetry.sample","data":{"device_id":"sensor-1","temperature_f":72.5,"timestamp":1700000000}}'`
//! - Invalid (missing required fields → 400):
//!   `curl -XPOST http://127.0.0.1:9090/api/telemetry/events -H 'content-type: application/json' -d '{"event_type":"telemetry.sample","data":{"temperature_f":72.5}}'`
//!
//! Rate limiting is applied via pipeline middleware (`rate_limit_with_burst`). When exceeded, the
//! bounded ingestion channel fills and the HTTP endpoint returns 503 with `Retry-After: 1`.

use anyhow::Result;
use obzenflow_adapters::middleware::rate_limit_with_burst;
use obzenflow_core::event::ChainEventContent;
use obzenflow_core::event::schema::TypedPayload;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::ChainEvent;
use obzenflow_dsl_infra::{async_infinite_source, flow, sink};
use obzenflow_infra::application::{FlowApplication, LogLevel};
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::web::endpoints::event_ingestion::{
    create_ingestion_endpoints, IngestionConfig, TypedValidator, ValidationConfig,
};
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::common::handlers::SinkHandler;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use async_trait::async_trait;

#[derive(Debug, Clone)]
struct PrintingSink;

#[async_trait]
impl SinkHandler for PrintingSink {
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        match event.content {
            ChainEventContent::Data { event_type, payload } => {
                println!("event_type={event_type} payload={payload}");
            }
            other => {
                println!("non-data event: {other:?}");
            }
        }

        Ok(DeliveryPayload::success(
            "printer",
            DeliveryMethod::Custom("console:stdout".to_string()),
            Some(1),
        ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TelemetrySample {
    device_id: String,
    temperature_f: f64,
    timestamp: u64,
}

impl TypedPayload for TelemetrySample {
    const EVENT_TYPE: &'static str = "telemetry.sample";
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut config = IngestionConfig::default();
    config.base_path = "/api/telemetry".to_string();
    config.buffer_capacity = 50_000;
    config.validation = Some(ValidationConfig::Single {
        validator: Arc::new(TypedValidator::<TelemetrySample>::new()),
    });

    let (endpoints, rx, state) = create_ingestion_endpoints(config);
    let telemetry = state.telemetry();
    let hook_state = state.clone();

    let http_source = obzenflow_adapters::sources::http::HttpSource::with_telemetry(rx, telemetry);

    FlowApplication::builder()
        .with_log_level(LogLevel::Info)
        .with_web_endpoints(endpoints)
        .with_flow_handle_hook(move |flow_handle| {
            hook_state.watch_pipeline_state(flow_handle.state_receiver())
        })
        .run_async(flow! {
            name: "http_ingestion_iot_telemetry_demo",
            journals: disk_journals(PathBuf::from("target/http-ingestion-iot-telemetry-demo-logs")),
            middleware: [
                // Global throughput control (token bucket with burst).
                rate_limit_with_burst(10_000.0, 20_000.0)
            ],

            stages: {
                events = async_infinite_source!("http" => http_source);

                printer = sink!("printer" => PrintingSink);
            },

            topology: {
                events |> printer;
            }
        })
        .await?;

    Ok(())
}
