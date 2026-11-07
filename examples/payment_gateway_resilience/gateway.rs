// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::{
    PaymentAuthorizationUnavailable, PaymentAuthorized, PaymentDeclineReason, PaymentDeclined,
    PaymentMethodState, TrafficPhase, ValidatedOrder,
};
use async_trait::async_trait;
use obzenflow::effects::simulated::{CallLog, CallLogError};
use obzenflow::effects::{
    EffectContext, EffectError, Effects, IdempotencyKey, Keyed, Port, StageCompletion,
};
use obzenflow::error::HandlerError;
use obzenflow::schema::EffectOutcomeFacts;
use obzenflow::stages::transforms::EffectfulTransformHandler;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[obzenflow::port]
#[async_trait]
pub trait PaymentGatewayPort: Send + Sync {
    #[effect(type = "payment.authorize", exactly_once)]
    async fn authorize(
        &self,
        request: &AuthorizeRequest,
        key: &IdempotencyKey,
        ctx: &EffectContext,
    ) -> Result<AuthorizePaymentOutcome, EffectError>;
}

#[derive(Debug, Clone, Serialize)]
pub struct AuthorizeRequest {
    order_id: String,
    customer_id: String,
    amount_cents: u64,
    payment_method_state: PaymentMethodState,
    phase: TrafficPhase,
}

impl AuthorizeRequest {
    pub fn for_order(order: &ValidatedOrder) -> Self {
        Self {
            order_id: order.order_id.clone(),
            customer_id: order.customer_id.clone(),
            amount_cents: order.amount_cents,
            payment_method_state: order.payment_method_state.clone(),
            phase: order.phase.clone(),
        }
    }

    fn authorized(&self, authorization_id: String) -> PaymentAuthorized {
        PaymentAuthorized {
            order_id: self.order_id.clone(),
            customer_id: self.customer_id.clone(),
            amount_cents: self.amount_cents,
            phase: self.phase.clone(),
            authorization_id,
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

impl Keyed for AuthorizeRequest {
    fn idempotency_key(&self) -> IdempotencyKey {
        IdempotencyKey(format!("payment-authorize:{}", self.order_id))
    }
}

#[derive(Debug, Clone, EffectOutcomeFacts)]
pub enum AuthorizePaymentOutcome {
    Authorized(PaymentAuthorized),
    Declined(PaymentDeclined),
}

#[obzenflow::command]
#[derive(Debug, Clone)]
pub struct GatewayTransform {
    #[port]
    payment_gateway_port: Port<dyn PaymentGatewayPort>,
    first_recovery_pause: Option<Duration>,
    recovery_pause_claimed: Arc<AtomicBool>,
}

type GatewayOutput = obzenflow::schema::stage_fact_set![
    PaymentAuthorized,
    PaymentDeclined,
    PaymentAuthorizationUnavailable
];

impl GatewayTransform {
    pub fn new(payment_gateway_port: Port<dyn PaymentGatewayPort>) -> Self {
        Self {
            payment_gateway_port,
            first_recovery_pause: None,
            recovery_pause_claimed: Arc::default(),
        }
    }

    pub fn with_first_recovery_pause(mut self, pause: Duration) -> Self {
        assert!(
            !pause.is_zero(),
            "the scripted recovery pause must be greater than zero"
        );
        self.first_recovery_pause = Some(pause);
        self
    }

    async fn pause_before_first_recovery(&self, order: &ValidatedOrder) {
        let Some(pause) = self.first_recovery_pause else {
            return;
        };
        if order.phase == TrafficPhase::Recovery
            && !self.recovery_pause_claimed.swap(true, Ordering::SeqCst)
        {
            tokio::time::sleep(pause).await;
        }
    }
}

#[async_trait]
impl EffectfulTransformHandler for GatewayTransform {
    type Input = ValidatedOrder;
    type Output = GatewayOutput;

    async fn process(
        &self,
        order: ValidatedOrder,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        if !fx.is_replaying() {
            self.pause_before_first_recovery(&order).await;
        }
        let request = AuthorizeRequest::for_order(&order);
        if let Err(error) = fx.call(request).await {
            fx.emit(order.authorization_unavailable(&error)).await?;
        }
        Ok(fx.complete()?)
    }

    fn stage_logic_version(&self) -> &str {
        "payment-gateway-v2"
    }
}

impl ValidatedOrder {
    fn authorization_unavailable(self, error: &EffectError) -> PaymentAuthorizationUnavailable {
        PaymentAuthorizationUnavailable {
            order_id: self.order_id,
            customer_id: self.customer_id,
            amount_cents: self.amount_cents,
            phase: self.phase,
            reason: error.semantic_reason().into_owned(),
        }
    }
}

pub struct ScriptedGateway {
    calls: CallLog<GatewayResponse>,
}

impl ScriptedGateway {
    pub fn open(path: &Path) -> Result<Self, CallLogError> {
        Ok(Self {
            calls: CallLog::open(path)?,
        })
    }
}

#[obzenflow::system("scripted-gateway")]
#[async_trait]
impl PaymentGatewayPort for ScriptedGateway {
    async fn authorize(
        &self,
        request: &AuthorizeRequest,
        key: &IdempotencyKey,
        ctx: &EffectContext,
    ) -> Result<AuthorizePaymentOutcome, EffectError> {
        let latency_ms: u64 = ctx.rng("gateway_latency").u64(50..=200);
        ctx.sleep(Duration::from_millis(latency_ms)).await;
        if request.phase == TrafficPhase::Outage {
            let hang_ms: u64 = ctx.rng("gateway_outage_hang").u64(2_000..=3_000);
            ctx.sleep(Duration::from_millis(hang_ms)).await;
        }
        let response = self
            .calls
            .try_answer(key, ctx.flow_id(), |_| decide(request))
            .map_err(|error| {
                EffectError::unavailable("payment_gateway", "ledger_write_failed", error)
            })??;
        Ok(match response {
            GatewayResponse::Charged { authorization_id } => {
                AuthorizePaymentOutcome::Authorized(request.authorized(authorization_id))
            }
            GatewayResponse::Declined { reason } => {
                AuthorizePaymentOutcome::Declined(request.declined(reason))
            }
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "authorization", rename_all = "snake_case")]
pub enum GatewayResponse {
    Charged { authorization_id: String },
    Declined { reason: PaymentDeclineReason },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
enum GatewayFailure {
    TimedOut,
    InvalidPaymentMethod,
}

impl From<GatewayFailure> for EffectError {
    fn from(failure: GatewayFailure) -> Self {
        match failure {
            GatewayFailure::TimedOut => Self::Timeout("gateway_timeout_simulated".to_string()),
            GatewayFailure::InvalidPaymentMethod => {
                Self::Validation("invalid_payment_method_reached_gateway".to_string())
            }
        }
    }
}

fn decide(request: &AuthorizeRequest) -> Result<GatewayResponse, GatewayFailure> {
    if request.phase == TrafficPhase::Outage {
        return Err(GatewayFailure::TimedOut);
    }
    match request.payment_method_state {
        PaymentMethodState::Valid => Ok(GatewayResponse::Charged {
            authorization_id: PaymentAuthorized::AUTHORIZATION_ID_DEMO.to_owned(),
        }),
        PaymentMethodState::InsufficientFunds => Ok(GatewayResponse::Declined {
            reason: PaymentDeclineReason::InsufficientFunds,
        }),
        PaymentMethodState::AddressMismatch => Ok(GatewayResponse::Declined {
            reason: PaymentDeclineReason::AddressMismatch,
        }),
        PaymentMethodState::InvalidNumber => Err(GatewayFailure::InvalidPaymentMethod),
    }
}
