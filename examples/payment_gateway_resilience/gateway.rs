// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Gateway authorization for the payment-gateway resilience flow.
//!
//! The gateway call is the one place this flow touches the outside world, so it
//! is expressed as an [`Effect`] (a value the stage returns) rather than inline
//! I/O. The runtime executes the effect once, journals the named outcome fact
//! that happened (`payment.authorized.v1` or `payment.declined.v1`), and on
//! replay reconstructs that recorded outcome without calling the gateway
//! again. That is the durable-execution property the tutorial teaches.
//!
//! Each payment-gateway example is self-contained; the high-volume variant keeps
//! its own copy of this logic and only swaps the source and the flow wiring.

use super::domain::{
    GatewayPaymentFallback, GatewayPaymentRejected, OrderCancellationReason, OrderCancelled,
    PaymentAuthorizationUnavailable, PaymentAuthorized, PaymentDeclineReason, PaymentDeclined,
    PaymentMethodState, TrafficPhase, ValidatedOrder,
};
use async_trait::async_trait;
use obzenflow_adapters::effects::{CircuitBreakerOutcome, GuardedEffectExt};
use obzenflow_adapters::middleware::circuit_breaker::FailureClassification;
use obzenflow_core::{event::chain_event::ChainEvent, EffectOutcomeFacts, TypedPayload};
use obzenflow_runtime::effects::{
    Effect, EffectContext, EffectError, EffectSafety, Effects, IdempotencyKey,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// The gateway authorization command, expressed as an effect.
///
/// Returning this command from a stage (instead of calling the gateway inline) is
/// what lets the runtime own execution: run it once, record the outcome, and
/// suppress it on replay. A real implementation would issue an HTTP request
/// inside [`Effect::execute`]; here we simulate latency and a scripted outage so
/// the behaviour is deterministic and easy to follow.
#[derive(Debug, Clone)]
pub struct AuthorizePayment {
    pub order: ValidatedOrder,
    retry_proof: Option<Arc<GatewayRetryProof>>,
}

/// Shared deterministic backend script used by the circuit-breaker retry
/// acceptance profile. Effect clones share this counter, so physical calls
/// observe the sequence timeout, timeout, success under one logical effect
/// cursor.
#[derive(Debug, Default)]
pub struct GatewayRetryProof {
    calls: AtomicUsize,
    panic_on_call: bool,
}

impl GatewayRetryProof {
    pub fn new(panic_on_call: bool) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            panic_on_call,
        }
    }

    #[cfg(test)]
    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn record_call(&self) -> usize {
        assert!(
            !self.panic_on_call,
            "strict replay attempted a live payment gateway call"
        );
        self.calls.fetch_add(1, Ordering::SeqCst) + 1
    }
}

/// Closed set of successful gateway authorization outcomes (FLOWIP-120m).
///
/// The variants are the facts the journal records (`payment.authorized.v1`,
/// `payment.declined.v1`); the derive writes the marshalling. The carrier
/// itself is transient `fx.perform` machinery the handler matches
/// exhaustively. There is no persisted gateway-decision wrapper.
#[derive(Debug, Clone, EffectOutcomeFacts)]
pub enum AuthorizePaymentOutcome {
    Authorized(PaymentAuthorized),
    Declined(PaymentDeclined),
}

#[async_trait]
impl Effect for AuthorizePayment {
    const EFFECT_TYPE: &'static str = "payment.authorize";
    const SCHEMA_VERSION: u32 = 1;
    // A charge is never idempotent on its own, so the effect must carry a key
    // the gateway can dedupe on. The runtime enforces this before any I/O.
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentRequiresKey;

    type Outcome = AuthorizePaymentOutcome;

    fn label(&self) -> &str {
        "authorize_payment"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({
            "order_id": self.order.order_id,
            "customer_id": self.order.customer_id,
            "amount_cents": self.order.amount_cents,
            "payment_method_state": self.order.payment_method_state,
            "phase": self.order.phase,
        })
    }

    async fn execute(&self, ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        if let Some(proof) = &self.retry_proof {
            let call = proof.record_call();
            if call <= 2 {
                return Err(EffectError::Timeout(
                    "gateway_timeout_simulated".to_string(),
                ));
            }
        } else {
            // Latency comes from the deterministic, seeded RNG on the effect context
            // rather than a wall clock, so a replay reconstructs the same timing.
            let latency_ms: u64 = ctx.rng("gateway_latency").u64(50..=200);
            ctx.sleep(std::time::Duration::from_millis(latency_ms))
                .await;

            if matches!(self.order.phase, TrafficPhase::Outage) {
                // A degraded gateway hangs before timing out, so the authorization
                // latency sample for an outage attempt is large (and the outcome is a
                // failure). The hang is drawn from the same seeded RNG as the base
                // latency, so a replay reconstructs the identical slow sample. The
                // latency indicator records this raw `value_ms`; whether it counts as
                // "slow" against an objective is a read-side question (FLOWIP-115l),
                // not baked into the wide event.
                let timeout_ms: u64 = ctx.rng("gateway_outage_hang").u64(2_000..=3_000);
                ctx.sleep(std::time::Duration::from_millis(timeout_ms))
                    .await;
                return Err(EffectError::Timeout(
                    "gateway_timeout_simulated".to_string(),
                ));
            }
        }

        let order = &self.order;
        match order.payment_method_state {
            PaymentMethodState::Valid => {
                Ok(AuthorizePaymentOutcome::Authorized(PaymentAuthorized {
                    order_id: order.order_id.clone(),
                    customer_id: order.customer_id.clone(),
                    amount_cents: order.amount_cents,
                    phase: order.phase.clone(),
                    authorization_id: PaymentAuthorized::AUTHORIZATION_ID_DEMO.to_string(),
                }))
            }
            PaymentMethodState::InsufficientFunds => {
                Ok(AuthorizePaymentOutcome::Declined(PaymentDeclined {
                    order_id: order.order_id.clone(),
                    customer_id: order.customer_id.clone(),
                    amount_cents: order.amount_cents,
                    phase: order.phase.clone(),
                    reason: PaymentDeclineReason::InsufficientFunds,
                }))
            }
            PaymentMethodState::AddressMismatch => {
                Ok(AuthorizePaymentOutcome::Declined(PaymentDeclined {
                    order_id: order.order_id.clone(),
                    customer_id: order.customer_id.clone(),
                    amount_cents: order.amount_cents,
                    phase: order.phase.clone(),
                    reason: PaymentDeclineReason::AddressMismatch,
                }))
            }
            PaymentMethodState::InvalidNumber => Err(EffectError::Validation(
                "invalid_payment_method_reached_gateway".to_string(),
            )),
        }
    }

    fn idempotency_key(&self) -> Option<IdempotencyKey> {
        Some(IdempotencyKey(format!(
            "payment-authorize:{}",
            self.order.order_id
        )))
    }
}

/// Effectful transform that turns a `ValidatedOrder` into named gateway outcome facts.
///
/// The handler body stays small and deterministic. The named payment facts
/// (`PaymentAuthorized`, `PaymentDeclined`) ARE the recorded effect outcome
/// group (FLOWIP-120m): `fx.perform` records whichever fact happened and
/// returns the transient carrier, so the handler never re-emits them. The
/// handler emits only derived consequences (`OrderCancelled`) and the
/// non-performance fact (`PaymentAuthorizationUnavailable`).
#[derive(Debug, Clone, Default)]
pub struct GatewayTransform {
    retry_proof: Option<Arc<GatewayRetryProof>>,
}

impl GatewayTransform {
    pub fn with_retry_proof(retry_proof: Arc<GatewayRetryProof>) -> Self {
        Self {
            retry_proof: Some(retry_proof),
        }
    }
}

#[async_trait]
impl EffectfulTransformHandler for GatewayTransform {
    type Input = ValidatedOrder;

    async fn process(&self, order: ValidatedOrder, fx: &mut Effects) -> Result<(), HandlerError> {
        if matches!(
            order.payment_method_state,
            PaymentMethodState::InvalidNumber
        ) {
            return Err(HandlerError::Validation(
                "invalid_payment_method_reached_gateway".to_string(),
            ));
        }

        // The guarded wrapper (FLOWIP-120h) lifts the effect outcome into an
        // explicit branch type: the breaker's fallback and rejection are
        // distinct named facts the carrier reconstructs by event type, live
        // and replay alike, rather than a variant of the gateway's own
        // decision distinguished by a reason string.
        let outcome = fx
            .perform(
                AuthorizePayment {
                    order: order.clone(),
                    retry_proof: self.retry_proof.clone(),
                }
                .guarded::<GatewayPaymentFallback, GatewayPaymentRejected>(),
            )
            .await;

        match outcome {
            Ok(CircuitBreakerOutcome::Primary(AuthorizePaymentOutcome::Authorized(_))) => {
                // The PaymentAuthorized outcome fact is already recorded by
                // fx.perform; authorization has no derived consequence here.
            }
            Ok(CircuitBreakerOutcome::Primary(AuthorizePaymentOutcome::Declined(declined))) => {
                // Fact first, consequence second: the recorded PaymentDeclined
                // outcome fact is the gateway's decision, and the derived
                // lifecycle consequence is that the order is cancelled.
                fx.emit(OrderCancelled {
                    order_id: declined.order_id,
                    customer_id: declined.customer_id,
                    amount_cents: declined.amount_cents,
                    phase: declined.phase,
                    reason: OrderCancellationReason::PaymentDeclined {
                        reason: declined.reason,
                    },
                })
                .await
                .map_err(|e| HandlerError::Other(e.to_string()))?;
            }
            Ok(CircuitBreakerOutcome::Fallback(fallback)) => {
                // Breaker-synthesized: the gateway was never called, so no
                // payment decision exists and the order goes to manual review.
                emit_authorization_unavailable(order, fallback.reason, fx).await?;
            }
            Ok(CircuitBreakerOutcome::Rejected(rejected)) => {
                emit_authorization_unavailable(order, rejected.reason, fx).await?;
            }
            Err(err) => {
                // Genuine gateway failure (e.g. the scripted outage before the
                // breaker opens): recorded under the effect cursor, so replay
                // reproduces the same failure; the operational consequence is
                // manual review, never a manufactured order fate.
                emit_authorization_unavailable(order, authorization_unavailable_reason(err), fx)
                    .await?;
            }
        }
        Ok(())
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "payment-gateway-v1"
    }
}

/// Map a remote effect failure onto an operational reason string.
///
/// A simulated gateway outage is infrastructure behavior, not a local business
/// invalid-order outcome and not a material gateway decline. The framework's
/// `semantic_reason` projection is identical for a live failure and the
/// `RecordedFailure` replay rehydrates it to, so there is no live-versus-replay
/// handling here (FLOWIP-120i).
fn authorization_unavailable_reason(err: EffectError) -> String {
    err.semantic_reason().into_owned()
}

/// Tell the circuit breaker which scripted inputs represent dependency failure.
///
/// Error-marked outputs cover genuine gateway failures; the input-phase check
/// covers the scripted outage. Breaker-synthesized fallback facts never reach
/// `post_handle` (the boundary short-circuits), so degraded outputs do not
/// re-count as failures. The classification is health authority plus recovery
/// veto only: returning `TransientFailure` cannot make an
/// ineligible raw failure retry.
pub fn classify_simulated_gateway_unavailability(
    event: &ChainEvent,
    outputs: &[ChainEvent],
) -> FailureClassification {
    let failed = outputs.iter().any(|output| {
        matches!(
            output.processing_info.status,
            obzenflow_core::event::status::processing_status::ProcessingStatus::Error { .. }
        )
    }) || matches!(
        ValidatedOrder::from_event(event).map(|order| order.phase),
        Some(TrafficPhase::Outage)
    );
    if failed {
        FailureClassification::TransientFailure
    } else {
        FailureClassification::Success
    }
}

/// Deliberately no cancellation here: unavailability means no payment decision
/// was reached, and manufacturing an order fate out of a gateway outage would
/// be a fake outcome (the FLOWIP-120m non-performance doctrine). These orders
/// go to manual review.
async fn emit_authorization_unavailable(
    order: ValidatedOrder,
    reason: String,
    fx: &mut Effects,
) -> Result<(), HandlerError> {
    fx.emit(PaymentAuthorizationUnavailable {
        order_id: order.order_id,
        customer_id: order.customer_id,
        amount_cents: order.amount_cents,
        phase: order.phase,
        reason,
    })
    .await
    .map_err(|e| HandlerError::Other(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::{authorization_unavailable_reason, EffectError, GatewayRetryProof};
    use obzenflow_runtime::effects::RetryDisposition;

    /// FLOWIP-120i: the recorded payload carries the semantic reason, never
    /// the Display wrapper, so live and replayed failures project to the same
    /// domain reason with no normalisation in example code.
    #[test]
    fn replayed_execution_failure_preserves_live_domain_reason() {
        let live = EffectError::Timeout("gateway_timeout_simulated".to_string());
        let live_reason = authorization_unavailable_reason(live);

        let replay_reason = authorization_unavailable_reason(EffectError::RecordedFailure {
            error_type: "timeout".into(),
            error_message: "gateway_timeout_simulated".to_string(),
            retry: RetryDisposition::Retryable,
            cause: None,
        });

        assert_eq!(live_reason, "gateway_timeout_simulated");
        assert_eq!(replay_reason, live_reason);
    }

    #[test]
    fn retry_proof_counts_physical_calls() {
        let proof = GatewayRetryProof::new(false);
        assert_eq!(proof.record_call(), 1);
        assert_eq!(proof.record_call(), 2);
        assert_eq!(proof.calls(), 2);
    }

    #[test]
    #[should_panic(expected = "strict replay attempted a live payment gateway call")]
    fn retry_proof_panics_on_replay_call() {
        GatewayRetryProof::new(true).record_call();
    }
}
