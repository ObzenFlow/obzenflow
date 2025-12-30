//! Payment Gateway Resilience demo
//!
//! This example is used to teach:
//! - Strict transport contracts under a flaky gateway (no circuit breaker).
//! - Breaker-aware contracts with typed fallback and circuit breaker.
//!
//! Two modes are relevant:
//! - `run_example` uses the circuit breaker around `gateway` with
//!   `with_typed_fallback` and `CircuitBreakerContractMode::BreakerAware`.
//!   Under outages, the breaker opens and emits degraded AuthorizedPayment
//!   events; contracts stay green and the pipeline completes successfully.
//! - `run_strict_example_in_tests` builds a strict flow without CB; the same
//!   outage pattern eventually causes a SeqDivergence transport violation on
//!   `gateway → summary`, and the flow is expected to fail in tests.

use super::domain::{AuthorizedPayment, PaymentCommand, TrafficPhase, ValidatedPayment};
use super::sinks::PaymentSummarySink;
use super::sources::{PaymentCommandSource, ScrapedGlitchyPaymentCommandSource};
use anyhow::Result;
use async_trait::async_trait;
use obzenflow_adapters::middleware::circuit_breaker::{HalfOpenPolicy, OpenPolicy};
use obzenflow_adapters::middleware::{rate_limit, CircuitBreakerBuilder};
use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    CircuitBreakerContractMode, TypedPayload,
};
use obzenflow_dsl_infra::{async_transform, flow, sink, source, transform};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::prelude::FlowHandle;
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::common::handlers::{AsyncTransformHandler, TransformHandler};
use serde_json::json;
use std::num::NonZeroU32;

fn env_usize(key: &str) -> Option<usize> {
    std::env::var(key).ok().and_then(|value| value.parse::<usize>().ok())
}

fn env_f64(key: &str) -> Option<f64> {
    std::env::var(key).ok().and_then(|value| value.parse::<f64>().ok())
}

/// Stateless transform that performs cheap local validation.
///
/// This is where we distinguish "bad input" problems from
/// downstream dependency problems. Validation failures are
/// marked with `ProcessingStatus::Error` and still emitted as
/// events so they contribute to `obzenflow_errors_total`.
#[derive(Debug, Clone)]
struct ValidationTransform;

#[async_trait]
impl TransformHandler for ValidationTransform {
    fn process(&self, mut event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        let payload = event.payload();

        let cmd = match serde_json::from_value::<PaymentCommand>(payload.clone()) {
            Ok(cmd) => cmd,
            Err(_) => {
                event = event.mark_as_error(
                    "failed_to_deserialize_payment_command",
                    ErrorKind::Deserialization,
                );
                return Ok(vec![event]);
            }
        };

        let mut validation_error = None;
        if !cmd.card_ok {
            validation_error = Some("card failed local Luhn check".to_string());
        } else if cmd.amount_cents == 0 {
            validation_error = Some("amount must be > 0".to_string());
        }

        if validation_error.is_some() {
            event = event.mark_as_error("payment_validation_failed", ErrorKind::Validation);
        }

        // Replace payload with a typed ValidatedPayment projection while
        // keeping all the flow / processing metadata intact.
        let validated = ValidatedPayment {
            request_id: cmd.request_id,
            customer_id: cmd.customer_id,
            amount_cents: cmd.amount_cents,
            card_ok: cmd.card_ok,
            phase: cmd.phase,
            validation_error,
        };

        let mut out = ChainEventFactory::data_event(
            event.writer_id.clone(),
            ValidatedPayment::EVENT_TYPE,
            json!(validated),
        );
        out.flow_context = event.flow_context.clone();
        out.processing_info = event.processing_info.clone();
        out.causality = event.causality.clone();
        out.correlation_id = event.correlation_id.clone();
        out.correlation_payload = event.correlation_payload.clone();
        out.runtime_context = event.runtime_context.clone();
        out.observability = event.observability.clone();

        Ok(vec![out])
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

/// Transform that simulates talking to an unreliable payment gateway.
///
/// The behaviour is intentionally simple:
/// - Warmup: always succeeds, emits AuthorizedPayment.
/// - Outage: returns `Err(HandlerError::Timeout(..))` so the runtime can
///   convert it into an error-marked event (and middleware can observe the
///   resulting `ErrorKind`).
/// - Recovery: healthy again, succeeds when circuit allows traffic.
#[derive(Debug, Clone)]
struct GatewayTransform;

#[async_trait]
impl AsyncTransformHandler for GatewayTransform {
    async fn process(&self, mut event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        // Simulated remote call latency. We want this stage to exercise async IO
        // semantics without blocking the tokio runtime.
        let latency_ms: u64 = fastrand::u64(50..=200);
        tokio::time::sleep(std::time::Duration::from_millis(latency_ms)).await;

        // If validation has already failed we leave the event alone.
        if matches!(event.processing_info.status, ProcessingStatus::Error { .. }) {
            return Ok(vec![event]);
        }

        let payload = event.payload();
        let validated: ValidatedPayment =
            match serde_json::from_value::<ValidatedPayment>(payload.clone()) {
                Ok(v) => v,
                Err(_) => {
                    event.processing_info.status =
                        ProcessingStatus::error("failed_to_deserialize_validated_payment");
                    return Ok(vec![event]);
                }
            };

        // Simulate different behaviours depending on the scripted phase.
        match validated.phase {
            TrafficPhase::Warmup | TrafficPhase::Recovery => {
                // Healthy gateway: succeed quickly. Overall throughput is
                // shaped by the rate limiter middleware.
                let authorized = AuthorizedPayment {
                    request_id: validated.request_id,
                    customer_id: validated.customer_id,
                    amount_cents: validated.amount_cents,
                    phase: validated.phase,
                    authorization_id: AuthorizedPayment::AUTHORIZATION_ID_DEMO.to_string(),
                };

                let mut out = ChainEventFactory::derived_data_event(
                    event.writer_id.clone(),
                    &event,
                    AuthorizedPayment::EVENT_TYPE,
                    json!(authorized),
                );
                out.flow_context = event.flow_context.clone();
                out.processing_info = event.processing_info.clone();
                out.causality = event.causality.clone();
                out.correlation_id = event.correlation_id.clone();
                out.correlation_payload = event.correlation_payload.clone();
                out.runtime_context = event.runtime_context.clone();
                out.observability = event.observability.clone();

                Ok(vec![out])
            }
            TrafficPhase::Outage => {
                // Simulated remote outage: the gateway call "times out".
                //
                // Returning `Err(Timeout)` exercises FLOWIP-086e's per-record
                // error mapping + routing while still allowing middleware (CB)
                // to classify infra failures.
                Err(HandlerError::Timeout("gateway_timeout_simulated".to_string()))
            }
        }
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

async fn build_flow() -> Result<FlowHandle> {
    flow! {
        name: "payment_gateway_resilience_demo",
        journals: disk_journals(std::path::PathBuf::from("target/payment-gateway-logs")),

        // Gentle flow-level rate limit to keep logs and metrics readable.
        // At 1.0 events/sec you can easily watch RED metrics and
        // circuit breaker gauges change over time.
        middleware: [
            rate_limit(1.0)
        ],

        stages: {
            // Source: scripted stream of payment commands across three phases.
            payments = source!("payments" => PaymentCommandSource::new(), [
                // Source-side circuit breaker: for real-world sources (MQTT, HTTP scrape, etc.)
                // this prevents hot loops when the upstream feed is unhealthy.
                CircuitBreakerBuilder::new(2)
                    .cooldown(std::time::Duration::from_secs(2))
                    .open_policy(OpenPolicy::Skip)
                    .half_open_policy(HalfOpenPolicy::new(
                        NonZeroU32::new(1).expect("permitted_probes must be non-zero"),
                        OpenPolicy::Skip,
                    ))
                    .build()
            ]);

            // Local validation: cheap checks that do NOT involve external IO.
            // Validation failures are tagged as errors and still emitted.
            validated = transform!("validation" => ValidationTransform);

            // Gateway stage: where we "talk" to the unreliable dependency.
            //
            // The circuit breaker wraps this stage and watches for events
            // tagged with ProcessingStatus::Error to decide when to open. With
            // ErrorKind wired in via FLOWIP-082h, gateway timeouts marked with
            // ErrorKind::Timeout are treated as breaker failures by default,
            // while local validation problems (ErrorKind::Validation) do not
            // open the circuit.
            //
            // In 051b‑part‑3 we also demonstrate rate-based failure detection
            // and explicit Open/HalfOpen policies:
            // - Rate-based over the last 5 calls with a 60% failure threshold.
            // - Slow-call contribution for gateway calls that take too long.
            // - Explicit Open/HalfOpen policies that still match the original
            //   semantics (emit fallback while Open; single-probe HalfOpen).
            gateway = async_transform!("gateway" => GatewayTransform, [
                CircuitBreakerBuilder::new(3)
                    .cooldown(std::time::Duration::from_secs(5))
                    // Rate-based failure mode: open when >= 60% of the last
                    // 5 gateway calls fail, with at least 5 observations.
                    .rate_based_over_last_n_calls(5, 0.6)
                    // Additionally count very slow calls toward opening the
                    // breaker when at least half of recent calls are slow.
                    .slow_call(std::time::Duration::from_millis(250), 0.5)
                    .with_typed_fallback::<ValidatedPayment, AuthorizedPayment, _>(|validated| AuthorizedPayment {
                        request_id: validated.request_id.clone(),
                        customer_id: validated.customer_id.clone(),
                        amount_cents: validated.amount_cents,
                        phase: validated.phase.clone(),
                        authorization_id: AuthorizedPayment::AUTHORIZATION_ID_FALLBACK_CB_OPEN.to_string(),
                    })
                    // Make the Open/HalfOpen behaviour explicit; these match
                    // the original defaults but are configurable knobs for
                    // more advanced scenarios.
                    .open_policy(OpenPolicy::EmitFallback)
                    .half_open_policy(HalfOpenPolicy::new(
                        NonZeroU32::new(1).expect("permitted_probes must be non-zero"),
                        OpenPolicy::EmitFallback,
                    ))
                    .with_contract_mode(CircuitBreakerContractMode::BreakerAware)
                    .build()
            ]);

            // Single sink that prints a concise summary at the end.
            summary = sink!("summary" => PaymentSummarySink::new());
        },

        topology: {
            payments |> validated;
            validated |> gateway;
            gateway |> summary;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create payment gateway flow: {:?}", e))
}

async fn build_glitchy_flow(
    total_events: usize,
    rate_limit_events_per_sec: f64,
    warmup_events: usize,
    outage_events: usize,
    recovery_events: usize,
    summary_progress_every: usize,
) -> Result<FlowHandle> {
    flow! {
        name: "payment_gateway_resilience_glitchy_demo",
        journals: disk_journals(std::path::PathBuf::from("target/payment-gateway-logs-glitchy")),

        // Faster rate so the flow runs in ~minutes while still letting you observe
        // breaker open/close cycles in `/metrics` over multiple glitch windows.
        middleware: [
            rate_limit(rate_limit_events_per_sec)
        ],

        stages: {
            payments = source!("payments" => ScrapedGlitchyPaymentCommandSource::with_cycle(
                total_events,
                warmup_events,
                outage_events,
                recovery_events,
            ), [
                // Source-side circuit breaker: models "semi-reliable upstream feed".
                // When the feed glitches, the breaker opens to avoid hammering, then
                // allows occasional HalfOpen probes until recovery.
                CircuitBreakerBuilder::new(2)
                    .cooldown(std::time::Duration::from_secs(2))
                    .open_policy(OpenPolicy::Skip)
                    .half_open_policy(HalfOpenPolicy::new(
                        NonZeroU32::new(1).expect("permitted_probes must be non-zero"),
                        OpenPolicy::Skip,
                    ))
                    .build()
            ]);
            validated = transform!("validation" => ValidationTransform);
            gateway = async_transform!("gateway" => GatewayTransform, [
                CircuitBreakerBuilder::new(3)
                    .cooldown(std::time::Duration::from_secs(5))
                    .rate_based_over_last_n_calls(5, 0.6)
                    .slow_call(std::time::Duration::from_millis(250), 0.5)
                    .with_typed_fallback::<ValidatedPayment, AuthorizedPayment, _>(|validated| AuthorizedPayment {
                        request_id: validated.request_id.clone(),
                        customer_id: validated.customer_id.clone(),
                        amount_cents: validated.amount_cents,
                        phase: validated.phase.clone(),
                        authorization_id: AuthorizedPayment::AUTHORIZATION_ID_FALLBACK_CB_OPEN.to_string(),
                    })
                    .open_policy(OpenPolicy::EmitFallback)
                    .half_open_policy(HalfOpenPolicy::new(
                        NonZeroU32::new(1).expect("permitted_probes must be non-zero"),
                        OpenPolicy::EmitFallback,
                    ))
                    .with_contract_mode(CircuitBreakerContractMode::BreakerAware)
                    .build()
            ]);
            summary = sink!("summary" => PaymentSummarySink::new_compact(summary_progress_every));
        },

        topology: {
            payments |> validated;
            validated |> gateway;
            gateway |> summary;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create glitchy payment gateway flow: {:?}", e))
}

pub fn run_example() -> Result<()> {
    println!("💳 FlowState RS - Payment Gateway Resilience Demo");
    println!("{}", "=".repeat(60));
    println!("This example shows how circuit breakers and rate limits");
    println!("work together to protect an unreliable dependency.");
    println!("");
    println!("Highlights:");
    println!("  • Local validation errors still show up in obzenflow_errors_total");
    println!("  • The payments source simulates a semi-reliable upstream feed (MQTT/IOT style)");
    println!("      - When it glitches, the source circuit breaker opens and you can watch:");
    println!("        obzenflow_circuit_breaker_*{{stage=\"payments\",...}}");
    println!("  • Gateway outages open the circuit breaker and increase:");
    println!("      - obzenflow_circuit_breaker_state");
    println!("      - obzenflow_circuit_breaker_rejection_rate");
    println!("      - obzenflow_circuit_breaker_consecutive_failures");
    println!("  • Once open, the breaker stops hammering the gateway.");
    println!("{}", "=".repeat(60));

    // Use Prometheus exporter by default so it is trivial to inspect
    // the circuit_breaker_* gauges for this example.
    std::env::set_var("OBZENFLOW_METRICS_EXPORTER", "prometheus");

    let server_mode_requested = std::env::args().any(|arg| arg == "--server");

    // Optional high-volume mode (similar to prometheus_100k_demo):
    // - Set PAYMENT_GATEWAY_TOTAL_EVENTS=100000 (or any N) to switch the source
    //   to a glitchy generator that periodically enters Outage.
    // - Or set PAYMENT_GATEWAY_DURATION_SECS=300 to run for ~N seconds using a
    //   derived total event count (based on PAYMENT_GATEWAY_RATE_LIMIT).
    // - When running with `--server` and no env vars are provided, we default
    //   to a ~5-minute glitchy run so you have time to scrape /metrics.
    //
    // Example (longer, server-friendly run):
    //   PAYMENT_GATEWAY_DURATION_SECS=300 PAYMENT_GATEWAY_RATE_LIMIT=200 \
    //     cargo run --example payment_gateway_resilience --features obzenflow_infra/warp-server -- --server
    //
    // Example (very high volume):
    //   PAYMENT_GATEWAY_TOTAL_EVENTS=100000 PAYMENT_GATEWAY_RATE_LIMIT=1000 \
    //     cargo run --example payment_gateway_resilience --features obzenflow_infra/warp-server -- --server
    let mut total_events = env_usize("PAYMENT_GATEWAY_TOTAL_EVENTS");
    let duration_secs = env_usize("PAYMENT_GATEWAY_DURATION_SECS");
    let mut rate_limit_events_per_sec = env_f64("PAYMENT_GATEWAY_RATE_LIMIT");
    let mut glitchy_reason: Option<&'static str> = None;

    match (total_events, duration_secs, server_mode_requested) {
        (Some(_), _, _) => {
            glitchy_reason = Some("PAYMENT_GATEWAY_TOTAL_EVENTS");
            rate_limit_events_per_sec.get_or_insert(1000.0);
        }
        (None, Some(duration_secs), _) => {
            glitchy_reason = Some("PAYMENT_GATEWAY_DURATION_SECS");
            let rate = rate_limit_events_per_sec.unwrap_or(200.0);
            rate_limit_events_per_sec = Some(rate);
            total_events = Some(((duration_secs as f64) * rate).round().max(1.0) as usize);
        }
        (None, None, true) => {
            glitchy_reason = Some("--server default");
            rate_limit_events_per_sec.get_or_insert(200.0);
            total_events = Some(60_000);
        }
        (None, None, false) => {}
    }

    let rate_limit_events_per_sec = rate_limit_events_per_sec.unwrap_or(1.0).max(0.1);

    let warmup_events = env_usize("PAYMENT_GATEWAY_WARMUP_EVENTS").unwrap_or(8_000);
    let outage_events = env_usize("PAYMENT_GATEWAY_OUTAGE_EVENTS").unwrap_or(1_000);
    let recovery_events = env_usize("PAYMENT_GATEWAY_RECOVERY_EVENTS").unwrap_or(1_000);
    let summary_progress_every = env_usize("PAYMENT_GATEWAY_PROGRESS_EVERY").unwrap_or(5_000);

    if let Some(total_events) = total_events {
        println!("\n🔁 High-volume glitchy mode enabled");
        if let Some(reason) = glitchy_reason {
            println!("   enabled_by:     {reason}");
        }
        println!("   total_events:   {total_events}");
        println!("   rate_limit:     {rate_limit_events_per_sec} events/sec");
        println!("   cycle:          warmup={warmup_events}, outage={outage_events}, recovery={recovery_events}");
        println!("   flow_name:      payment_gateway_resilience_glitchy_demo");
        println!("   journal_dir:    target/payment-gateway-logs-glitchy");
        println!("   progress_log:   every {summary_progress_every} events");
    }

    FlowApplication::builder()
        .with_console_subscriber()
        .with_log_level(obzenflow_infra::application::LogLevel::Info)
        .run_blocking(async move {
            match total_events {
                Some(total) => {
                    build_glitchy_flow(
                        total,
                        rate_limit_events_per_sec,
                        warmup_events,
                        outage_events,
                        recovery_events,
                        summary_progress_every,
                    )
                    .await
                }
                None => build_flow().await,
            }
        })
        .map_err(|e| anyhow::anyhow!("Application failed: {:?}", e))?;

    println!("\n✅ Payment gateway resilience demo completed!");
    println!("💡 Next step: scrape /metrics for obzenflow_circuit_breaker_*");
    println!("    and obzenflow_errors_total to see the full story.");

    Ok(())
}

/// Test-friendly runner so we can execute the flow from integration tests
/// without invoking the CLI argument parser.
pub fn run_example_in_tests() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to create runtime: {:?}", e))?;

    runtime.block_on(async {
        let handle = build_flow().await?;
        handle
            .run()
            .await
            .map_err(|e| anyhow::anyhow!("Flow execution failed: {:?}", e))
    })
}

/// Strict-mode variant of the flow used for tests.
///
/// This version omits the circuit breaker entirely so that downstream
/// contracts see the raw effects of a flaky gateway. Under the scripted
/// outage pattern, the `gateway → summary` edge should eventually fail
/// with a SeqDivergence transport violation.
async fn build_strict_flow() -> Result<FlowHandle> {
    flow! {
        name: "payment_gateway_resilience_strict_demo",
        journals: disk_journals(std::path::PathBuf::from("target/payment-gateway-logs-strict")),

        middleware: [
            rate_limit(1.0)
        ],

        stages: {
            payments = source!("payments_strict" => PaymentCommandSource::new());
            validated = transform!("validation_strict" => ValidationTransform);
            gateway = async_transform!("gateway_strict" => GatewayTransform);
            summary = sink!("summary_strict" => PaymentSummarySink::new());
        },

        topology: {
            payments |> validated;
            validated |> gateway;
            gateway |> summary;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create strict payment gateway flow: {:?}", e))
}

/// Test-only runner for the strict flow; expected to fail with a contract violation.
pub fn run_strict_example_in_tests() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to create runtime: {:?}", e))?;

    runtime.block_on(async {
        let handle = build_strict_flow().await?;
        handle
            .run()
            .await
            .map_err(|e| anyhow::anyhow!("Flow execution failed (strict mode): {:?}", e))
    })
}
