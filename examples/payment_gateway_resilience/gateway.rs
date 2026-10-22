// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Gateway authorization for the payment-gateway resilience flow.
//!
//! The gateway call is the one place this flow touches the outside world, so it
//! is expressed as a request ([`AuthorizeRequest`]) the handler passes to
//! `fx.call`, rather than inline I/O. The runtime owns one logical effect
//! invocation, applies its configured retry policy to live physical calls,
//! journals the one terminal outcome fact that happened (`payment.authorized`
//! or `payment.declined`), and on replay reconstructs that recorded outcome
//! without calling the gateway again. That is the durable-execution property
//! the tutorial teaches.
//!
//! Each payment-gateway example is self-contained; the high-volume variant keeps
//! its own copy of this logic and only swaps the source and the flow wiring.

use super::domain::{
    PaymentAuthorizationUnavailable, PaymentAuthorized, PaymentDeclineReason, PaymentDeclined,
    PaymentMethodState, TrafficPhase, ValidatedOrder,
};
use async_trait::async_trait;
use obzenflow::effects::{
    Call, CallRequest, EffectContext, EffectError, EffectHandler, Effects, IdempotencyKey, Keyed,
    RequiresKey, System,
};
use obzenflow::error::HandlerError;
use obzenflow::schema::EffectOutcomeFacts;
use obzenflow::stages::transforms::EffectfulTransformHandler;
use serde::Serialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// The payment gateway an authorization request calls.
#[obzenflow::port]
#[async_trait]
pub trait PaymentGatewayPort: Send + Sync {
    async fn authorize(
        &self,
        request: &AuthorizeRequest,
        ctx: &EffectContext,
    ) -> Result<AuthorizePaymentOutcome, EffectError>;
}

/// The tutorial's gateway. A real implementation would issue an HTTP request
/// inside [`PaymentGatewayPort::authorize`]; here we simulate latency and a
/// scripted outage so the behaviour is deterministic and easy to follow.
pub struct ScriptedGateway;

#[async_trait]
impl PaymentGatewayPort for ScriptedGateway {
    async fn authorize(
        &self,
        request: &AuthorizeRequest,
        ctx: &EffectContext,
    ) -> Result<AuthorizePaymentOutcome, EffectError> {
        // Latency comes from the deterministic, seeded RNG on the effect context
        // rather than a wall clock, so a replay reconstructs the same timing.
        let latency_ms: u64 = ctx.rng("gateway_latency").u64(50..=200);
        ctx.sleep(Duration::from_millis(latency_ms)).await;

        if matches!(request.phase, TrafficPhase::Outage) {
            // A degraded gateway hangs before timing out, so the enclosing
            // authorization attempt is visibly slow (and the outcome is a failure).
            // The hang is drawn from the same seeded RNG as the base latency.
            let timeout_ms: u64 = ctx.rng("gateway_outage_hang").u64(2_000..=3_000);
            ctx.sleep(Duration::from_millis(timeout_ms)).await;
            return Err(EffectError::Timeout(
                "gateway_timeout_simulated".to_string(),
            ));
        }

        match request.payment_method_state {
            PaymentMethodState::Valid => {
                Ok(AuthorizePaymentOutcome::Authorized(PaymentAuthorized {
                    order_id: request.order_id.clone(),
                    customer_id: request.customer_id.clone(),
                    amount_cents: request.amount_cents,
                    phase: request.phase.clone(),
                    authorization_id: PaymentAuthorized::AUTHORIZATION_ID_DEMO.to_string(),
                }))
            }
            PaymentMethodState::InsufficientFunds => Ok(AuthorizePaymentOutcome::Declined(
                request.declined(PaymentDeclineReason::InsufficientFunds),
            )),
            PaymentMethodState::AddressMismatch => Ok(AuthorizePaymentOutcome::Declined(
                request.declined(PaymentDeclineReason::AddressMismatch),
            )),
            PaymentMethodState::InvalidNumber => Err(EffectError::Validation(
                "invalid_payment_method_reached_gateway".to_string(),
            )),
        }
    }
}

/// The gateway authorization request.
///
/// Passing this request to `fx.call` (instead of calling the gateway inline) is
/// what lets the runtime own execution: run it once, record the outcome, and
/// suppress it on replay.
#[derive(Debug, Clone, Serialize)]
pub struct AuthorizeRequest {
    order_id: String,
    customer_id: String,
    amount_cents: u64,
    payment_method_state: PaymentMethodState,
    phase: TrafficPhase,
}

impl AuthorizeRequest {
    /// The request that authorizes `order` at the gateway.
    pub fn for_order(order: &ValidatedOrder) -> Self {
        Self {
            order_id: order.order_id.clone(),
            customer_id: order.customer_id.clone(),
            amount_cents: order.amount_cents,
            payment_method_state: order.payment_method_state.clone(),
            phase: order.phase.clone(),
        }
    }

    fn declined(&self, reason: PaymentDeclineReason) -> PaymentDeclined {
        PaymentDeclined {
            order_id: self.order_id.clone(),
            customer_id: self.customer_id.clone(),
            amount_cents: self.amount_cents,
            phase: self.phase.clone(),
            reason,
        }
    }
}

/// The gateway authorization effect, which the stage's effect set names.
pub type AuthorizePayment = Call<AuthorizeRequest>;

/// Closed set of successful gateway authorization outcomes (FLOWIP-120m).
///
/// The variants are the facts the journal records (`payment.authorized`,
/// `payment.declined`); the derive writes the marshalling. The carrier
/// itself is transient `fx.call` machinery the handler matches
/// exhaustively. There is no persisted gateway-decision wrapper.
#[derive(Debug, Clone, EffectOutcomeFacts)]
#[effect_outcome(schema = obzenflow::schema)]
pub enum AuthorizePaymentOutcome {
    Authorized(PaymentAuthorized),
    Declined(PaymentDeclined),
}

#[async_trait]
impl CallRequest for AuthorizeRequest {
    const EFFECT_TYPE: &'static str = "payment.authorize";
    // A charge is never idempotent on its own, so the request must carry a key
    // the gateway can dedupe on. It does not compile without its `Keyed` impl.
    type Safety = RequiresKey;
    type Port = dyn PaymentGatewayPort;
    type Outcome = AuthorizePaymentOutcome;

    async fn call(
        &self,
        gateway: &dyn PaymentGatewayPort,
        ctx: &mut EffectContext,
    ) -> Result<AuthorizePaymentOutcome, EffectError> {
        gateway.authorize(self, ctx).await
    }
}

impl Keyed for AuthorizeRequest {
    fn idempotency_key(&self) -> IdempotencyKey {
        IdempotencyKey(format!("payment-authorize:{}", self.order_id))
    }
}

/// Effectful transform that turns a `ValidatedOrder` into named gateway outcome facts.
///
/// The named payment facts (`PaymentAuthorized`, `PaymentDeclined`) ARE the
/// recorded effect outcome group (FLOWIP-120m): `fx.call` records whichever
/// fact happened and returns the transient carrier, so the handler never
/// re-emits them. The handler emits only the non-performance fact
/// (`PaymentAuthorizationUnavailable`).
#[derive(Debug, Clone, EffectHandler)]
pub struct GatewayTransform {
    #[system]
    gateway: System<dyn PaymentGatewayPort>,
    first_recovery_pause: Option<Duration>,
    recovery_pause_claimed: Arc<AtomicBool>,
}

type GatewayOutput = obzenflow::schema::stage_fact_set![
    PaymentAuthorized,
    PaymentDeclined,
    PaymentAuthorizationUnavailable
];
type GatewayAllowedEffects = obzenflow::effects::effect_set![AuthorizePayment];

impl GatewayTransform {
    /// A gateway transform whose authorizations `gateway` answers.
    pub fn new(gateway: System<dyn PaymentGatewayPort>) -> Self {
        Self {
            gateway,
            first_recovery_pause: None,
            recovery_pause_claimed: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Pace the first live recovery authorization after the scripted outage.
    ///
    /// A real order stream naturally has time between an outage and later
    /// recovery traffic. This finite demo has every input ready immediately,
    /// so its normal assembly adds that quiet period explicitly before breaker
    /// admission. Replay skips the wall-clock wait and reconstructs the
    /// recorded effect outcome as usual.
    pub fn with_first_recovery_pause(mut self, pause: Duration) -> Self {
        assert!(
            !pause.is_zero(),
            "the scripted recovery pause must be greater than zero"
        );
        self.first_recovery_pause = Some(pause);
        self
    }

    async fn pace_first_live_recovery(&self, order: &ValidatedOrder, is_replaying: bool) {
        if is_replaying || !matches!(order.phase, TrafficPhase::Recovery) {
            return;
        }

        let Some(pause) = self.first_recovery_pause else {
            return;
        };
        if !self.recovery_pause_claimed.swap(true, Ordering::SeqCst) {
            tokio::time::sleep(pause).await;
        }
    }
}

#[async_trait]
impl EffectfulTransformHandler for GatewayTransform {
    type Input = ValidatedOrder;
    type Output = GatewayOutput;
    type AllowedEffects = GatewayAllowedEffects;

    async fn process(
        &self,
        order: ValidatedOrder,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow::effects::StageCompletion<Self::Output>, HandlerError> {
        if matches!(
            order.payment_method_state,
            PaymentMethodState::InvalidNumber
        ) {
            return Err(HandlerError::Validation(
                "invalid_payment_method_reached_gateway".to_string(),
            ));
        }

        self.pace_first_live_recovery(&order, fx.is_replaying())
            .await;

        // The configured resilience unit owns retry and breaker admission. If
        // recovery exhausts or the breaker prevents an attempt, `fx.call`
        // returns the one recorded terminal error; resilience never
        // synthesizes a business fact to represent non-execution.
        let outcome = fx.call(AuthorizeRequest::for_order(&order)).await;

        match outcome {
            // fx.call already recorded the gateway's decision as its named
            // outcome fact, which reaches its own delivery; a decline is a
            // payment outcome, not an order cancellation.
            Ok(AuthorizePaymentOutcome::Authorized(_)) => {}
            Ok(AuthorizePaymentOutcome::Declined(_)) => {}
            Err(err) => {
                // Gateway failure or breaker prevention: recorded under the
                // effect cursor either way, so replay reproduces it; no payment
                // decision exists and the order goes to manual review, never a
                // manufactured order fate.
                emit_authorization_unavailable(order, authorization_unavailable_reason(err), fx)
                    .await?;
            }
        }
        Ok(fx.complete()?)
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "payment-gateway-v2"
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

/// Deliberately not a decline: unavailability means no payment decision was
/// reached, and manufacturing one out of a gateway outage would be a fake
/// outcome (the FLOWIP-120m non-performance doctrine). These orders go to
/// manual review.
async fn emit_authorization_unavailable(
    order: ValidatedOrder,
    reason: String,
    fx: &mut Effects<GatewayOutput, GatewayAllowedEffects>,
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
    use super::{authorization_unavailable_reason, EffectError};
    use obzenflow::effects::RetryDisposition;

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
            detail: None,
        });

        assert_eq!(live_reason, "gateway_timeout_simulated");
        assert_eq!(replay_reason, live_reason);
    }
}
