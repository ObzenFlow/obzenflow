// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The heart of the example: local validation and the gateway call modelled as
//! a replay-suppressed effect.
//!
//! The gateway call is the one place this flow touches the outside world, so it
//! is expressed as an [`Effect`] (a value the stage returns) rather than inline
//! I/O. The runtime executes the effect once, journals the captured
//! authorization, and on replay returns that recorded value without calling the
//! gateway again. That is the durable-execution property the tutorial teaches.
//!
//! Each payment-gateway example is self-contained; the high-volume variant keeps
//! its own copy of this logic and only swaps the source and the flow wiring.

use super::domain::{AuthorizedPayment, PaymentCommand, TrafficPhase, ValidatedPayment};
use async_trait::async_trait;
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    TypedPayload,
};
use obzenflow_runtime::effects::{
    Effect, EffectContext, EffectError, EffectSafety, Effects, IdempotencyKey,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{EffectfulTransformHandler, TransformHandler};
use serde::{Deserialize, Serialize};
use serde_json::json;

/// Stateless transform that performs cheap, local validation.
///
/// This is where we separate "bad input" from "the dependency is unhealthy".
/// Validation failures are deterministic, so they belong on the pure transform
/// side: we mark them with `ProcessingStatus::Error` and still emit them, so
/// they contribute to `obzenflow_errors_total` and never reach the gateway.
#[derive(Debug, Clone)]
pub struct ValidationTransform;

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

        // Replace the payload with a typed `ValidatedPayment` projection while
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
            event.writer_id,
            ValidatedPayment::EVENT_TYPE,
            json!(validated),
        );
        out.flow_context = event.flow_context.clone();
        out.processing_info = event.processing_info.clone();
        out.causality = event.causality.clone();
        out.correlation = event.correlation.clone();
        out.runtime_context = event.runtime_context.clone();
        out.observability = event.observability.clone();

        Ok(vec![out])
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

/// The captured response from the gateway.
///
/// This is the value the effect produces. The runtime journals it on the live
/// run, so replay can reconstruct the rest of the flow from this recorded value
/// without re-calling the gateway.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayAuthorization {
    pub authorization_id: String,
}

impl TypedPayload for GatewayAuthorization {
    const EVENT_TYPE: &'static str = "payment.gateway_authorization";
}

/// The gateway call, expressed as data.
///
/// Returning this from a stage (instead of calling the gateway inline) is what
/// lets the runtime own execution: run it once, record the outcome, and suppress
/// it on replay. A real implementation would issue an HTTP request inside
/// [`Effect::execute`]; here we simulate latency and a scripted outage so the
/// behaviour is deterministic and easy to follow.
#[derive(Debug, Clone)]
pub struct AuthorizePayment {
    pub validated: ValidatedPayment,
}

#[async_trait]
impl Effect for AuthorizePayment {
    const EFFECT_TYPE: &'static str = "payment.authorize";
    const SCHEMA_VERSION: u32 = 1;
    // A charge is never idempotent on its own, so the effect must carry a key
    // the gateway can dedupe on. The runtime enforces this before any I/O.
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentRequiresKey;

    type Output = GatewayAuthorization;

    fn label(&self) -> &str {
        "authorize_payment"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({
            "request_id": self.validated.request_id,
            "customer_id": self.validated.customer_id,
            "amount_cents": self.validated.amount_cents,
            "phase": self.validated.phase,
        })
    }

    async fn execute(&self, ctx: &mut EffectContext) -> Result<Self::Output, EffectError> {
        // Latency comes from the deterministic, seeded RNG on the effect context
        // rather than a wall clock, so a replay reconstructs the same timing.
        let latency_ms: u64 = ctx.rng("gateway_latency").u64(50..=200);
        ctx.sleep(std::time::Duration::from_millis(latency_ms))
            .await;

        match self.validated.phase {
            TrafficPhase::Warmup | TrafficPhase::Recovery => Ok(GatewayAuthorization {
                authorization_id: AuthorizedPayment::AUTHORIZATION_ID_DEMO.to_string(),
            }),
            TrafficPhase::Outage => Err(EffectError::Execution(
                "gateway_timeout_simulated".to_string(),
            )),
        }
    }

    fn idempotency_key(&self) -> Option<IdempotencyKey> {
        Some(IdempotencyKey(format!(
            "payment-authorize:{}",
            self.validated.request_id
        )))
    }
}

/// Effectful transform that turns a `ValidatedPayment` into an `AuthorizedPayment`
/// by performing the gateway effect.
///
/// The handler body stays small and deterministic: it performs one effect and
/// folds the recorded outcome into a typed output. Everything replay-sensitive
/// lives inside the effect.
#[derive(Debug, Clone)]
pub struct GatewayTransform;

#[async_trait]
impl EffectfulTransformHandler for GatewayTransform {
    type Input = ValidatedPayment;
    type Output = AuthorizedPayment;

    async fn process(
        &self,
        validated: ValidatedPayment,
        fx: &mut Effects,
    ) -> Result<AuthorizedPayment, HandlerError> {
        if validated.validation_error.is_some() {
            return Err(HandlerError::Validation(
                "payment_validation_failed".to_string(),
            ));
        }

        let authorization = fx
            .perform(AuthorizePayment {
                validated: validated.clone(),
            })
            .await
            .map_err(effect_error_to_handler_error)?;

        Ok(AuthorizedPayment {
            request_id: validated.request_id,
            customer_id: validated.customer_id,
            amount_cents: validated.amount_cents,
            phase: validated.phase,
            authorization_id: authorization.authorization_id,
        })
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "payment-gateway-v1"
    }
}

/// Map an effect failure onto the stage error rail.
///
/// A simulated gateway outage becomes a `Timeout`, which the circuit breaker
/// treats as a failure; anything else is surfaced as a generic handler error.
fn effect_error_to_handler_error(err: EffectError) -> HandlerError {
    match err {
        EffectError::Execution(_) | EffectError::RecordedFailure { .. } => {
            HandlerError::Timeout(err.to_string())
        }
        other => HandlerError::Other(other.to_string()),
    }
}
