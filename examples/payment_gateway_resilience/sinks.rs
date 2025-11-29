use super::domain::{AuthorizedPayment, ValidatedPayment};
use async_trait::async_trait;
use obzenflow_core::{
    event::{
        chain_event::ChainEvent,
        payloads::delivery_payload::{DeliveryMethod, DeliveryPayload},
    },
    TypedPayload,
};
use obzenflow_runtime_services::stages::common::handlers::SinkHandler;

/// Sink that prints a concise summary of how many payments
/// succeeded, failed validation, or never even reached the
/// gateway because the circuit was open.
///
/// We deliberately keep this sink simple and focused on the
/// teaching storyline – it is where you "read off" what the
/// circuit breaker protected you from.
#[derive(Clone, Debug, Default)]
pub struct PaymentSummarySink {
    total_seen: usize,
    authorized: usize,
    validation_errors: usize,
}

impl PaymentSummarySink {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl SinkHandler for PaymentSummarySink {
    async fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<DeliveryPayload> {
        self.total_seen += 1;

        if let Some(authorized) = AuthorizedPayment::from_event(&event) {
            self.authorized += 1;
            println!(
                "✅ Authorized payment {} for customer {} (phase: {:?}, amount: ${:.2})",
                authorized.request_id,
                authorized.customer_id,
                authorized.phase,
                authorized.amount_cents as f64 / 100.0
            );
        } else if let Some(validated) = ValidatedPayment::from_event(&event) {
            if validated.validation_error.is_some() {
                self.validation_errors += 1;
                println!(
                    "⚠️  Dropped locally invalid payment {} (error: {})",
                    validated.request_id,
                    validated
                        .validation_error
                        .as_deref()
                        .unwrap_or("unknown validation error")
                );
            }
        }

        Ok(DeliveryPayload::success(
            "payment_summary",
            DeliveryMethod::Custom("InMemory".to_string()),
            None,
        ))
    }

    /// During drain we print the high-level story the example teaches.
    async fn drain(&mut self) -> obzenflow_core::Result<Option<DeliveryPayload>> {
        println!("\n============================================================");
        println!("📊 PAYMENT GATEWAY RESILIENCE SUMMARY");
        println!("============================================================");
        println!("Events that reached sink: {}", self.total_seen);
        println!("  • Authorized payments:   {}", self.authorized);
        println!("  • Validation failures:   {}", self.validation_errors);
        println!("  • Gateway rejections:    (see circuit_breaker_* metrics)");
        println!("============================================================\n");
        println!("Key observation:");
        println!(
            "  - Locally invalid payments still count as errors_total;"
        );
        println!(
            "  - Remote outages open the circuit breaker, which protects the gateway"
        );
        println!(
            "    and shows up in obzenflow_circuit_breaker_* metrics instead of spamming errors."
        );

        Ok(None)
    }
}

