//! HTTP Ingestion Webhook Demo (FLOWIP-084d)
//!
//! Run with:
//! `WEBHOOK_SECRET=dev_secret cargo run -p obzenflow --example http_ingestion_webhook_demo --features obzenflow_infra/warp-server -- --server --server-port 9090`
//!
//! Generate a Stripe-style signature header and POST an event:
//! ```
//! BODY='{"event_type":"stripe.payment_succeeded","data":{"id":"evt_123","amount":2000}}'
//! TS=$(date +%s)
//! SIG=$(python - <<'PY'
//! import hmac, hashlib, os, sys
//! secret = os.environ.get("WEBHOOK_SECRET", "dev_secret").encode()
//! body = os.environ["BODY"].encode()
//! ts = os.environ["TS"].encode()
//! signed = ts + b"." + body
//! print(hmac.new(secret, signed, hashlib.sha256).hexdigest())
//! PY
//! )
//! curl -XPOST http://127.0.0.1:9090/api/webhooks/stripe/events \
//!   -H 'content-type: application/json' \
//!   -H "Stripe-Signature: t=${TS},v1=${SIG}" \
//!   -d "${BODY}"
//! ```

use anyhow::Result;
use obzenflow_core::event::ChainEventContent;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::ChainEvent;
use obzenflow_dsl_infra::{async_infinite_source, flow, sink};
use obzenflow_infra::application::{FlowApplication, LogLevel};
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::web::endpoints::event_ingestion::{
    create_ingestion_endpoints, AuthConfig, IngestionConfig,
};
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::common::handlers::SinkHandler;
use std::path::PathBuf;
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
    let secret = std::env::var("WEBHOOK_SECRET").unwrap_or_else(|_| "dev_secret".to_string());

    let mut config = IngestionConfig::default();
    config.base_path = "/api/webhooks/stripe".to_string();
    config.auth = Some(AuthConfig::WebhookSignature {
        secret,
        header: "stripe-signature".to_string(),
        replay_window_secs: Some(300),
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
            name: "http_ingestion_webhook_demo",
            journals: disk_journals(PathBuf::from("target/http-ingestion-webhook-demo-logs")),
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
