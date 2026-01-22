use super::domain::{AuthorizedPayment, TrafficPhase, ValidatedPayment};
use async_trait::async_trait;
use obzenflow_core::{
    event::{
        chain_event::ChainEvent,
        payloads::delivery_payload::{DeliveryMethod, DeliveryPayload},
    },
    TypedPayload,
};
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::common::handlers::SinkHandler;

/// Sink that prints a concise summary of how many payments
/// succeeded, failed validation, or never even reached the
/// gateway because the circuit was open.
///
/// We deliberately keep this sink simple and focused on the
/// teaching storyline – it is where you "read off" what the
/// circuit breaker protected you from.
#[derive(Clone, Debug)]
pub struct PaymentSummarySink {
    total_seen: usize,
    authorized_success: usize,
    authorized_degraded: usize,
    gateway_timeouts: usize,
    validation_errors: usize,
    other_errors: usize,
    verbose_events: bool,
    progress_every: usize,
    last_phase: Option<TrafficPhase>,
}

impl Default for PaymentSummarySink {
    fn default() -> Self {
        Self {
            total_seen: 0,
            authorized_success: 0,
            authorized_degraded: 0,
            gateway_timeouts: 0,
            validation_errors: 0,
            other_errors: 0,
            verbose_events: true,
            progress_every: 1,
            last_phase: None,
        }
    }
}

impl PaymentSummarySink {
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(not(test))]
    pub fn new_compact(progress_every: usize) -> Self {
        Self {
            verbose_events: false,
            progress_every: progress_every.max(1),
            ..Self::default()
        }
    }

    fn maybe_log_phase_change(&mut self, phase: Option<&TrafficPhase>) {
        let Some(phase) = phase else { return };
        if self.last_phase.as_ref() == Some(phase) {
            return;
        }
        self.last_phase = Some(phase.clone());

        println!(
            "\n🔄 Traffic phase: {}",
            self.last_phase
                .as_ref()
                .map(TrafficPhase::as_str)
                .unwrap_or("unknown")
        );
    }

    fn maybe_log_progress(&self) {
        if self.verbose_events || self.total_seen == 0 {
            return;
        }

        if !self.total_seen.is_multiple_of(self.progress_every) {
            return;
        }

        println!(
            "📈 Progress: seen={} ok={} degraded={} timeouts={} validation={} other_errors={}",
            self.total_seen,
            self.authorized_success,
            self.authorized_degraded,
            self.gateway_timeouts,
            self.validation_errors,
            self.other_errors
        );
    }
}

#[async_trait]
impl SinkHandler for PaymentSummarySink {
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        // Ignore lifecycle/observability/control events for the high-level story.
        // We only want to count domain data events that represent payments.
        if !event.is_data() {
            return Ok(DeliveryPayload::success(
                "payment_summary",
                DeliveryMethod::Custom("InMemory".to_string()),
                None,
            ));
        }

        self.total_seen += 1;

        let authorized = AuthorizedPayment::from_event(&event);
        let validated = if authorized.is_none() {
            ValidatedPayment::from_event(&event)
        } else {
            None
        };

        let phase = authorized
            .as_ref()
            .map(|a| &a.phase)
            .or_else(|| validated.as_ref().map(|v| &v.phase));
        self.maybe_log_phase_change(phase);

        // First, treat explicit validation failures as such, regardless of how
        // they are represented downstream (ValidatedPayment or even a fallback
        // AuthorizedPayment that preserved the validation error status).
        let is_validation_failure = matches!(
            event.processing_info.status,
            obzenflow_core::event::status::processing_status::ProcessingStatus::Error {
                kind: Some(obzenflow_core::event::status::processing_status::ErrorKind::Validation),
                ..
            }
        );

        if is_validation_failure {
            self.validation_errors += 1;

            if self.verbose_events {
                if let Some(validated) = validated {
                    println!(
                        "⚠️  Dropped locally invalid payment {} (error: {})",
                        validated.request_id,
                        validated
                            .validation_error
                            .as_deref()
                            .unwrap_or("unknown validation error")
                    );
                } else if let Some(authorized) = authorized.as_ref() {
                    println!(
                        "⚠️  Locally invalid payment {} reached sink as degraded authorization (status: payment_validation_failed)",
                        authorized.request_id
                    );
                } else {
                    println!(
                        "⚠️  Locally invalid payment reached sink with status=payment_validation_failed (unknown payload type)"
                    );
                }
            }
        } else if matches!(
            event.processing_info.status,
            obzenflow_core::event::status::processing_status::ProcessingStatus::Error {
                kind: Some(obzenflow_core::event::status::processing_status::ErrorKind::Timeout),
                ..
            }
        ) {
            self.gateway_timeouts += 1;

            if self.verbose_events {
                let request_id = validated
                    .as_ref()
                    .map(|v| v.request_id.as_str())
                    .or_else(|| authorized.as_ref().map(|a| a.request_id.as_str()))
                    .unwrap_or("<unknown>");
                println!("⏳ Gateway timeout for request {request_id} (simulated)");
            }
        } else if let Some(authorized) = authorized {
            let is_degraded =
                authorized.authorization_id == AuthorizedPayment::AUTHORIZATION_ID_FALLBACK_CB_OPEN;
            if is_degraded {
                self.authorized_degraded += 1;
                if self.verbose_events {
                    println!(
                        "🟡 Degraded authorization {} for customer {} (CB open, phase: {:?}, amount: ${:.2})",
                        authorized.request_id,
                        authorized.customer_id,
                        authorized.phase,
                        authorized.amount_cents as f64 / 100.0
                    );
                }
            } else {
                self.authorized_success += 1;
                if self.verbose_events {
                    println!(
                        "✅ Authorized payment {} for customer {} (phase: {:?}, amount: ${:.2})",
                        authorized.request_id,
                        authorized.customer_id,
                        authorized.phase,
                        authorized.amount_cents as f64 / 100.0
                    );
                }
            }
        } else if matches!(
            event.processing_info.status,
            obzenflow_core::event::status::processing_status::ProcessingStatus::Error { .. }
        ) {
            self.other_errors += 1;

            if self.verbose_events {
                println!(
                    "❌ Payment event reached sink with unexpected error status: {:?}",
                    event.processing_info.status
                );
            }
        }

        self.maybe_log_progress();

        Ok(DeliveryPayload::success(
            "payment_summary",
            DeliveryMethod::Custom("InMemory".to_string()),
            None,
        ))
    }

    /// During drain we print the high-level story the example teaches.
    async fn drain(&mut self) -> Result<Option<DeliveryPayload>, HandlerError> {
        println!("\n============================================================");
        println!("📊 PAYMENT GATEWAY RESILIENCE SUMMARY");
        println!("============================================================");
        println!("Events that reached sink: {}", self.total_seen);
        println!("  • Authorized (ok):       {}", self.authorized_success);
        println!("  • Authorized (degraded): {}", self.authorized_degraded);
        println!("  • Gateway timeouts:      {}", self.gateway_timeouts);
        println!("  • Validation failures:   {}", self.validation_errors);
        println!("  • Other errors:          {}", self.other_errors);
        println!("  • Gateway rejections:    (see obzenflow_circuit_breaker_* metrics)");
        println!("============================================================\n");
        println!("Key observation:");
        println!("  - Locally invalid payments still count as errors_total;");
        println!("  - Remote outages open the circuit breaker, which protects the gateway");
        println!(
            "    and shows up in obzenflow_circuit_breaker_* metrics instead of spamming errors."
        );

        Ok(None)
    }
}
