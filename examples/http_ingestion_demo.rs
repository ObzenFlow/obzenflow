//! HTTP Ingestion Demo (FLOWIP-084d)
//!
//! Run with:
//! `cargo run -p obzenflow --example http_ingestion_demo --features obzenflow_infra/warp-server -- --server --server-port 9090`
//!
//! Then submit events:
//! - `curl -XPOST http://localhost:9090/api/ingest/events -H 'content-type: application/json' -d '{"event_type":"order.created","data":{"order_id":"1"}}'`
//! - `curl -XPOST http://localhost:9090/api/ingest/batch  -H 'content-type: application/json' -d '{"events":[{"event_type":"order.created","data":{"order_id":"2"}},{"event_type":"order.created","data":{"order_id":"3"}}]}'`

use anyhow::Result;
use obzenflow_dsl_infra::{async_infinite_source, flow, sink};
use obzenflow_infra::application::{FlowApplication, LogLevel};
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::web::endpoints::event_ingestion::{create_ingestion_endpoints, IngestionConfig};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::ChainEventContent;
use obzenflow_core::ChainEvent;
use std::path::PathBuf;
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::common::handlers::SinkHandler;
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

#[tokio::main]
async fn main() -> Result<()> {
    let (endpoints, rx, state) = create_ingestion_endpoints(IngestionConfig::default());
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
            name: "http_ingestion_demo",
            journals: disk_journals(PathBuf::from("target/http-ingestion-demo-logs")),
            middleware: [],

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
