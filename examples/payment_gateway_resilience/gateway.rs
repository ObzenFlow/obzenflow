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
    EffectContext, EffectError, EffectPortResolutionError, Effects, IdempotencyKey, Keyed, Port,
    StageCompletion,
};
use obzenflow::error::HandlerError;
use obzenflow::schema::EffectOutcomeFacts;
use obzenflow::stages::transforms::EffectfulTransformHandler;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

const SCRIPTED_OUTAGE_TIMEOUTS: u32 = 4;

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
pub struct AuthorizePayment {
    #[port]
    payment_gateway_port: Port<dyn PaymentGatewayPort>,
    first_recovery_pause: Option<Duration>,
    recovery_pause_claimed: Arc<AtomicBool>,
}

type AuthorizePaymentOutput = obzenflow::schema::stage_fact_set![
    PaymentAuthorized,
    PaymentDeclined,
    PaymentAuthorizationUnavailable
];

impl AuthorizePayment {
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
impl EffectfulTransformHandler for AuthorizePayment {
    type Input = ValidatedOrder;
    type Output = AuthorizePaymentOutput;

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

pub struct SimulatedProcessor {
    calls: CallLog<ProcessorResponse>,
    outage_timeouts_left: AtomicU32,
}

impl SimulatedProcessor {
    pub fn open(path: &Path) -> Result<Self, CallLogError> {
        Ok(Self {
            calls: CallLog::open(path)?,
            outage_timeouts_left: AtomicU32::new(SCRIPTED_OUTAGE_TIMEOUTS),
        })
    }

    fn outage_times_out(&self, request: &AuthorizeRequest) -> bool {
        request.phase == TrafficPhase::Outage
            && self
                .outage_timeouts_left
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                    left.checked_sub(1)
                })
                .is_ok()
    }
}

#[obzenflow::system("simulated-processor")]
#[async_trait]
impl PaymentGatewayPort for SimulatedProcessor {
    async fn authorize(
        &self,
        request: &AuthorizeRequest,
        key: &IdempotencyKey,
        ctx: &EffectContext,
    ) -> Result<AuthorizePaymentOutcome, EffectError> {
        let latency_ms: u64 = ctx.rng("processor_latency").u64(50..=200);
        ctx.sleep(Duration::from_millis(latency_ms)).await;
        let times_out = self.outage_times_out(request);
        if times_out {
            let hang_ms: u64 = ctx.rng("processor_outage_hang").u64(2_000..=3_000);
            ctx.sleep(Duration::from_millis(hang_ms)).await;
        }
        let response = self
            .calls
            .try_answer(key, ctx.flow_id(), |_| {
                if times_out {
                    Err(ProcessorFailure::TimedOut)
                } else {
                    decide(request)
                }
            })
            .map_err(|error| {
                EffectError::unavailable("payment_processor", "ledger_write_failed", error)
            })??;
        Ok(match response {
            ProcessorResponse::Charged { authorization_id } => {
                AuthorizePaymentOutcome::Authorized(request.authorized(authorization_id))
            }
            ProcessorResponse::Declined { reason } => {
                AuthorizePaymentOutcome::Declined(request.declined(reason))
            }
        })
    }
}

pub fn simulated_processor(calls: &Path) -> Port<dyn PaymentGatewayPort> {
    let calls = calls.to_path_buf();
    Port::deferred(move || SimulatedProcessor::open(&calls).map_err(processor_unavailable))
}

fn processor_unavailable(error: CallLogError) -> EffectPortResolutionError {
    tracing::error!(%error, "the simulated processor could not open its call log");
    EffectPortResolutionError::ClientConstructionFailed
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "authorization", rename_all = "snake_case")]
pub enum ProcessorResponse {
    Charged { authorization_id: String },
    Declined { reason: PaymentDeclineReason },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
enum ProcessorFailure {
    TimedOut,
    InvalidPaymentMethod,
}

impl From<ProcessorFailure> for EffectError {
    fn from(failure: ProcessorFailure) -> Self {
        match failure {
            ProcessorFailure::TimedOut => Self::Timeout("processor_timeout_simulated".to_string()),
            ProcessorFailure::InvalidPaymentMethod => {
                Self::Validation("invalid_payment_method_reached_processor".to_string())
            }
        }
    }
}

fn decide(request: &AuthorizeRequest) -> Result<ProcessorResponse, ProcessorFailure> {
    match request.payment_method_state {
        PaymentMethodState::Valid => Ok(ProcessorResponse::Charged {
            authorization_id: PaymentAuthorized::AUTHORIZATION_ID_DEMO.to_owned(),
        }),
        PaymentMethodState::InsufficientFunds => Ok(ProcessorResponse::Declined {
            reason: PaymentDeclineReason::InsufficientFunds,
        }),
        PaymentMethodState::AddressMismatch => Ok(ProcessorResponse::Declined {
            reason: PaymentDeclineReason::AddressMismatch,
        }),
        PaymentMethodState::InvalidNumber => Err(ProcessorFailure::InvalidPaymentMethod),
    }
}
