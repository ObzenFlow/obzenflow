// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Test-only runtime witness for FLOWIP-115n payment-gateway guarantees.
//!
//! Scripted failures, physical-call counters, policy variants, and cooldown
//! pauses belong here rather than in the user-facing resilience example.

pub use crate::payment_domain::ValidatedOrder;
use crate::payment_domain::{
    OrderChannel, PaymentAuthorizationUnavailable, PaymentAuthorized, PaymentMethodState,
    TrafficPhase,
};
use async_trait::async_trait;
use obzenflow::typed::sources as typed_sources;
use obzenflow_adapters::middleware::{CircuitBreaker, EffectResilience, RateLimiter, Retry};
use obzenflow_dsl::{effectful_transform, flow, sink, source};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::{
    Effect, EffectContext, EffectError, EffectSafety, Effects, IdempotencyKey,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
use obzenflow_runtime::stages::sink::SinkTyped;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// The two configuration-faithful policies admitted as release evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleasePolicy {
    BreakerOnly,
    BreakerRecovery,
}

#[derive(Debug, Clone, Copy)]
enum GatewayBehaviour {
    FailFirst(usize),
    Healthy,
    AlwaysFail,
}

#[derive(Debug, Clone, Copy)]
struct InvocationPause {
    invocation: usize,
    duration: Duration,
}

/// Shared deterministic dependency used to count actual physical calls.
#[derive(Debug)]
pub struct ScriptedGateway {
    calls: AtomicUsize,
    invocations: AtomicUsize,
    panic_on_live_call: bool,
    behaviour: GatewayBehaviour,
    pause_before_invocation: Option<InvocationPause>,
}

impl ScriptedGateway {
    pub fn fail_first(count: usize) -> Self {
        Self::with_behaviour(GatewayBehaviour::FailFirst(count))
    }

    pub fn healthy() -> Self {
        Self::with_behaviour(GatewayBehaviour::Healthy)
    }

    pub fn always_fail() -> Self {
        Self::with_behaviour(GatewayBehaviour::AlwaysFail)
    }

    fn with_behaviour(behaviour: GatewayBehaviour) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            invocations: AtomicUsize::new(0),
            panic_on_live_call: false,
            behaviour,
            pause_before_invocation: None,
        }
    }

    /// Make any physical call fail loudly, as required by strict-replay cases.
    pub fn panic_on_live_call(mut self) -> Self {
        self.panic_on_live_call = true;
        self
    }

    /// Delay one logical invocation before breaker admission.
    pub fn pause_before_invocation(mut self, invocation: usize, duration: Duration) -> Self {
        assert!(invocation > 0, "invocation ordinals are one-based");
        self.pause_before_invocation = Some(InvocationPause {
            invocation,
            duration,
        });
        self
    }

    async fn prepare_invocation(&self) {
        let invocation = self.invocations.fetch_add(1, Ordering::SeqCst) + 1;
        if self.panic_on_live_call {
            return;
        }
        if let Some(pause) = self.pause_before_invocation {
            if pause.invocation == invocation {
                tokio::time::sleep(pause.duration).await;
            }
        }
    }

    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn record_call(&self) -> usize {
        assert!(
            !self.panic_on_live_call,
            "strict replay attempted a live payment gateway call"
        );
        self.calls.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn should_timeout(&self, call: usize) -> bool {
        match self.behaviour {
            GatewayBehaviour::FailFirst(count) => call <= count,
            GatewayBehaviour::Healthy => false,
            GatewayBehaviour::AlwaysFail => true,
        }
    }
}

#[derive(Debug, Clone)]
struct ScriptedAuthorizePayment {
    order: ValidatedOrder,
    gateway: Arc<ScriptedGateway>,
}

#[async_trait]
impl Effect for ScriptedAuthorizePayment {
    const EFFECT_TYPE: &'static str = "payment.authorize";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentRequiresKey;
    type BindingMode = obzenflow_runtime::effects::Portless;

    type Outcome = PaymentAuthorized;
    type OutcomeSemantics = obzenflow_runtime::effects::DomainFacts;

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

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        let call = self.gateway.record_call();
        if self.gateway.should_timeout(call) {
            return Err(EffectError::Timeout(
                "gateway_timeout_simulated".to_string(),
            ));
        }

        Ok(PaymentAuthorized {
            order_id: self.order.order_id.clone(),
            customer_id: self.order.customer_id.clone(),
            amount_cents: self.order.amount_cents,
            phase: self.order.phase.clone(),
            authorization_id: PaymentAuthorized::AUTHORIZATION_ID_DEMO.to_string(),
        })
    }

    fn idempotency_key(&self) -> Option<IdempotencyKey> {
        Some(IdempotencyKey(format!(
            "payment-authorize:{}",
            self.order.order_id
        )))
    }
}

type GatewayOutput =
    obzenflow_core::stage_fact_set![PaymentAuthorized, PaymentAuthorizationUnavailable];
type GatewayEffects = obzenflow_runtime::effect_set![ScriptedAuthorizePayment];

#[derive(Debug, Clone)]
struct ScriptedGatewayTransform {
    gateway: Arc<ScriptedGateway>,
}

#[async_trait]
impl EffectfulTransformHandler for ScriptedGatewayTransform {
    type Input = ValidatedOrder;
    type Output = GatewayOutput;
    type AllowedEffects = GatewayEffects;

    async fn process(
        &self,
        order: ValidatedOrder,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
        self.gateway.prepare_invocation().await;

        if let Err(error) = fx
            .perform(ScriptedAuthorizePayment {
                order: order.clone(),
                gateway: self.gateway.clone(),
            })
            .await
        {
            fx.emit(PaymentAuthorizationUnavailable {
                order_id: order.order_id,
                customer_id: order.customer_id,
                amount_cents: order.amount_cents,
                phase: order.phase,
                reason: error.semantic_reason().into_owned(),
            })
            .await
            .map_err(|emit_error| HandlerError::Other(emit_error.to_string()))?;
        }

        Ok(fx.complete()?)
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "payment-gateway-v1"
    }
}

fn canonical_recovery() -> Retry {
    Retry::fixed(Duration::from_millis(250))
        .max_attempts(3)
        .attempt_start_window(Duration::from_secs(30))
}

fn discard<T>(
) -> impl FnMut(T, obzenflow_runtime::stages::sink::DeliveryContext) -> std::future::Ready<()>
       + Send
       + Sync
       + Clone
where
    T: Clone + Send + Sync + 'static,
{
    move |_payload: T, _delivery| std::future::ready(())
}

/// Build the focused, configuration-faithful integration-test flow.
pub fn build_flow(
    policy: ReleasePolicy,
    orders: Vec<ValidatedOrder>,
    gateway: Arc<ScriptedGateway>,
    journal_root: std::path::PathBuf,
) -> obzenflow_dsl::FlowDefinition {
    obzenflow_dsl::FlowDefinition::materialize(move |_runtime_config| {
        let gateway_breaker = CircuitBreaker::builder()
            .count_window(5)
            .minimum_calls(5)
            .failure_rate_threshold(0.6)
            .slow_call_duration(Duration::from_millis(250))
            .slow_call_rate_threshold(0.5)
            .open_for(Duration::from_secs(5))
            .probes(1)
            .build()
            .expect("gateway circuit-breaker configuration must be valid");
        let gateway_limiter =
            RateLimiter::per_second(1.0).expect("gateway rate-limiter configuration must be valid");

        let resilience = EffectResilience::with_breaker(gateway_breaker)
            .rate_limit_each_attempt(gateway_limiter);
        let resilience = match policy {
            ReleasePolicy::BreakerOnly => resilience,
            ReleasePolicy::BreakerRecovery => resilience.retry(canonical_recovery()),
        };
        let gateway_resilience = resilience
            .build()
            .expect("gateway resilience witness configuration must be valid");

        let order_feed = typed_sources::finite(orders);
        let authorize_payment = ScriptedGatewayTransform { gateway };
        let record_authorized =
            SinkTyped::with_delivery(discard::<PaymentAuthorized>()).idempotent();
        let record_unavailable =
            SinkTyped::with_delivery(discard::<PaymentAuthorizationUnavailable>()).idempotent();

        Ok(flow! {
            name: "payment_gateway_resilience_demo",
            journals: disk_journals(journal_root),

            stages: {
                orders = source!(ValidatedOrder => order_feed);
                authorize_payment = effectful_transform!(
                    ValidatedOrder -> {
                        PaymentAuthorized,
                        PaymentAuthorizationUnavailable
                    } uses ScriptedAuthorizePayment with gateway_resilience => authorize_payment,
                    observers: []
                );
                paid_orders = sink!(PaymentAuthorized => record_authorized);
                manual_review = sink!(PaymentAuthorizationUnavailable => record_unavailable);
            },

            topology: {
                orders |> authorize_payment;
                authorize_payment |> paid_orders;
                authorize_payment |> manual_review;
            }
        })
    })
}

fn orders(prefix: &str, count: usize) -> Vec<ValidatedOrder> {
    (0..count)
        .map(|index| ValidatedOrder {
            order_id: format!("{prefix}-order-{index}"),
            customer_id: format!("{prefix}-customer-{index}"),
            channel: OrderChannel::Web,
            amount_cents: 10_00,
            payment_method_state: PaymentMethodState::Valid,
            phase: TrafficPhase::Warmup,
        })
        .collect()
}

pub fn retry_order() -> ValidatedOrder {
    orders("retry-witness", 1)
        .into_iter()
        .next()
        .expect("one retry witness order")
}

pub fn healthy_orders() -> Vec<ValidatedOrder> {
    orders("healthy-witness", 5)
}

pub fn open_rejection_orders() -> Vec<ValidatedOrder> {
    orders("open-witness", 6)
}

pub fn half_open_recovery_orders() -> Vec<ValidatedOrder> {
    orders("half-open-witness", 7)
}
