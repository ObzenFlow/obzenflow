// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The tutorial sink: prints a running commentary and a final summary.
//!
//! This is where you "read off" what the flow did: which payments were
//! authorized, which were degraded because the breaker was open, which timed
//! out at the gateway, and which never reached the gateway because local
//! validation rejected them.

use super::domain::{AuthorizedPayment, TrafficPhase, ValidatedPayment};
use async_trait::async_trait;
use obzenflow_core::{
    event::{
        chain_event::ChainEvent,
        payloads::delivery_payload::{DeliveryMethod, DeliveryPayload},
        status::processing_status::{ErrorKind, ProcessingStatus},
    },
    TypedPayload,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::SinkHandler;

/// Sink that counts payment outcomes and narrates the run.
#[derive(Clone, Debug, Default)]
pub struct PaymentSummarySink {
    total_seen: usize,
    authorized_success: usize,
    authorized_degraded: usize,
    gateway_timeouts: usize,
    validation_errors: usize,
    other_errors: usize,
    last_phase: Option<TrafficPhase>,
}

impl PaymentSummarySink {
    pub fn new() -> Self {
        Self::default()
    }

    fn maybe_log_phase_change(&mut self, phase: Option<&TrafficPhase>) {
        let Some(phase) = phase else { return };
        if self.last_phase.as_ref() == Some(phase) {
            return;
        }
        self.last_phase = Some(phase.clone());
        println!("\n🔄 Traffic phase: {phase:?}");
    }
}

#[async_trait]
impl SinkHandler for PaymentSummarySink {
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        // Ignore lifecycle/observability/control events for the high-level
        // story; we only count domain data events that represent payments.
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

        // Treat explicit validation failures first, however they are
        // represented downstream.
        let is_validation_failure = matches!(
            event.processing_info.status,
            ProcessingStatus::Error {
                kind: Some(ErrorKind::Validation),
                ..
            }
        );

        if is_validation_failure {
            self.validation_errors += 1;
            let request_id = validated
                .as_ref()
                .map(|v| v.request_id.as_str())
                .or_else(|| authorized.as_ref().map(|a| a.request_id.as_str()))
                .unwrap_or("<unknown>");
            let reason = validated
                .as_ref()
                .and_then(|v| v.validation_error.as_deref())
                .unwrap_or("payment_validation_failed");
            println!("⚠️  Dropped locally invalid payment {request_id} (error: {reason})");
        } else if matches!(
            event.processing_info.status,
            ProcessingStatus::Error {
                kind: Some(ErrorKind::Timeout),
                ..
            }
        ) {
            self.gateway_timeouts += 1;
            let request_id = validated
                .as_ref()
                .map(|v| v.request_id.as_str())
                .or_else(|| authorized.as_ref().map(|a| a.request_id.as_str()))
                .unwrap_or("<unknown>");
            println!("⏳ Gateway timeout for request {request_id} (simulated)");
        } else if let Some(authorized) = authorized {
            let is_degraded =
                authorized.authorization_id == AuthorizedPayment::AUTHORIZATION_ID_FALLBACK_CB_OPEN;
            if is_degraded {
                self.authorized_degraded += 1;
                println!(
                    "🟡 Degraded authorization {} for customer {} (breaker open, phase: {:?}, amount: ${:.2})",
                    authorized.request_id,
                    authorized.customer_id,
                    authorized.phase,
                    authorized.amount_cents as f64 / 100.0
                );
            } else {
                self.authorized_success += 1;
                println!(
                    "✅ Authorized payment {} for customer {} (phase: {:?}, amount: ${:.2})",
                    authorized.request_id,
                    authorized.customer_id,
                    authorized.phase,
                    authorized.amount_cents as f64 / 100.0
                );
            }
        } else if matches!(event.processing_info.status, ProcessingStatus::Error { .. }) {
            self.other_errors += 1;
            println!(
                "❌ Payment event reached sink with unexpected error status: {:?}",
                event.processing_info.status
            );
        }

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
        println!("  - Locally invalid payments still count toward errors_total;");
        println!("  - Remote outages open the circuit breaker, which protects the gateway");
        println!(
            "    and shows up in obzenflow_circuit_breaker_* metrics instead of spamming errors."
        );

        Ok(None)
    }
}
