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
use obzenflow::stages::sources;
use obzenflow_adapters::middleware::{
    circuit_breaker, rate_limit, retry, MiddlewareFactory, Retry,
};
use obzenflow_dsl::{effectful_transform, flow, sink, source};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::{
    Call, CallRequest, EffectContext, EffectError, Effects, IdempotencyKey, Keyed, RequiresKey,
    System,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
use obzenflow_runtime::stages::sink::SinkWriteFailure;
use serde::Serialize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Configuration-faithful policies for coordinated and independent controls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleasePolicy {
    BreakerOnly,
    BreakerRecovery,
    RetryOnly,
    RetryWithLimiter,
    RetryWithLimiterPermuted,
    RetryWithLimiterSplit,
}

impl ReleasePolicy {
    pub fn has_breaker(self) -> bool {
        matches!(self, Self::BreakerOnly | Self::BreakerRecovery)
    }

    pub fn has_limiter(self) -> bool {
        self != Self::RetryOnly
    }

    pub fn has_retry(self) -> bool {
        self != Self::BreakerOnly
    }
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
    calls: Arc<AtomicUsize>,
    invocations: Arc<AtomicUsize>,
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
            calls: Arc::new(AtomicUsize::new(0)),
            invocations: Arc::new(AtomicUsize::new(0)),
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

    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    pub fn invocations(&self) -> usize {
        self.invocations.load(Ordering::SeqCst)
    }

    fn port(&self) -> GatewayPort {
        GatewayPort {
            calls: self.calls.clone(),
            panic_on_live_call: self.panic_on_live_call,
            behaviour: self.behaviour,
        }
    }

    fn pacing(&self) -> InvocationPacing {
        InvocationPacing {
            invocations: self.invocations.clone(),
            panic_on_live_call: self.panic_on_live_call,
            pause_before_invocation: self.pause_before_invocation,
        }
    }
}

/// Counts logical invocations and delays one before breaker admission.
#[derive(Debug, Clone)]
struct InvocationPacing {
    invocations: Arc<AtomicUsize>,
    panic_on_live_call: bool,
    pause_before_invocation: Option<InvocationPause>,
}

impl InvocationPacing {
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
}

/// The scripted gateway's port, which counts the physical calls that reach it.
#[obzenflow::port]
#[derive(Debug)]
struct GatewayPort {
    calls: Arc<AtomicUsize>,
    panic_on_live_call: bool,
    behaviour: GatewayBehaviour,
}

impl GatewayPort {
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

#[derive(Debug, Clone, Serialize)]
struct ScriptedAuthorize {
    order_id: String,
    customer_id: String,
    amount_cents: u64,
    payment_method_state: PaymentMethodState,
    phase: TrafficPhase,
}

impl ScriptedAuthorize {
    fn for_order(order: &ValidatedOrder) -> Self {
        Self {
            order_id: order.order_id.clone(),
            customer_id: order.customer_id.clone(),
            amount_cents: order.amount_cents,
            payment_method_state: order.payment_method_state.clone(),
            phase: order.phase.clone(),
        }
    }
}

#[async_trait]
impl CallRequest for ScriptedAuthorize {
    const EFFECT_TYPE: &'static str = "payment.authorize";
    type Safety = RequiresKey;
    type Port = GatewayPort;
    type Outcome = PaymentAuthorized;

    async fn call(
        &self,
        gateway: &GatewayPort,
        _ctx: &mut EffectContext,
    ) -> Result<PaymentAuthorized, EffectError> {
        let call = gateway.record_call();
        if gateway.should_timeout(call) {
            return Err(EffectError::Timeout(
                "gateway_timeout_simulated".to_string(),
            ));
        }

        Ok(PaymentAuthorized {
            order_id: self.order_id.clone(),
            customer_id: self.customer_id.clone(),
            amount_cents: self.amount_cents,
            phase: self.phase.clone(),
            authorization_id: PaymentAuthorized::AUTHORIZATION_ID_DEMO.to_string(),
        })
    }
}

impl Keyed for ScriptedAuthorize {
    fn idempotency_key(&self) -> IdempotencyKey {
        IdempotencyKey(format!("payment-authorize:{}", self.order_id))
    }
}

type ScriptedAuthorizePayment = Call<ScriptedAuthorize>;

type GatewayOutput =
    obzenflow_core::stage_fact_set![PaymentAuthorized, PaymentAuthorizationUnavailable];
type GatewayEffects = obzenflow_runtime::effect_set![ScriptedAuthorizePayment];

#[derive(Debug, Clone, obzenflow::effects::EffectHandler)]
struct ScriptedGatewayTransform {
    #[system]
    gateway: System<GatewayPort>,
    pacing: InvocationPacing,
}

impl ScriptedGatewayTransform {
    fn new(gateway: &ScriptedGateway) -> Self {
        Self {
            gateway: System::eager("payment-gateway", Arc::new(gateway.port()))
                .expect("a valid system name"),
            pacing: gateway.pacing(),
        }
    }
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
        self.pacing.prepare_invocation().await;

        if let Err(error) = fx.call(ScriptedAuthorize::for_order(&order)).await {
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
    retry()
        .fixed_delay(Duration::from_millis(250))
        .max_attempts(3)
        .attempt_start_window(Duration::from_secs(30))
}

#[derive(Clone)]
struct Discard<T: obzenflow_core::TypedPayload + Clone + Send + Sync + 'static> {
    input: std::marker::PhantomData<fn() -> T>,
}
#[async_trait::async_trait]
impl<T: obzenflow_core::TypedPayload + Clone + Send + Sync + 'static>
    obzenflow_runtime::stages::sink::InlineSink for Discard<T>
{
    type Input = T;
    fn describe(&self) -> obzenflow_runtime::stages::sink::SinkDescription {
        obzenflow_runtime::stages::sink::SinkDescription::method(
            obzenflow_core::event::payloads::delivery_payload::DeliveryMethod::Noop,
        )
        .with_redelivery_safety(obzenflow_runtime::effects::SinkRedeliverySafety::SafeToRepeat)
    }
    async fn write(&mut self, input: T) -> Result<(), SinkWriteFailure> {
        let _ = input;
        Ok(())
    }
}

/// Build the focused, configuration-faithful integration-test flow.
pub fn build_flow(
    policy: ReleasePolicy,
    orders: Vec<ValidatedOrder>,
    gateway: Arc<ScriptedGateway>,
    journal_root: std::path::PathBuf,
) -> obzenflow_dsl::FlowDefinition {
    obzenflow_dsl::FlowDefinition::materialize(move |_runtime_config| {
        let gateway_breaker = circuit_breaker()
            .count_window(5)
            .minimum_calls(5)
            .failure_rate_threshold(0.6)
            .slow_call_duration(Duration::from_millis(250))
            .slow_call_rate_threshold(0.5)
            .open_for(Duration::from_secs(5))
            .probes(1);
        let gateway_limiter = rate_limit(1.0);

        let order_feed = sources::ValuesSource::new(orders);
        let authorize_payment_handler = ScriptedGatewayTransform::new(&gateway);
        let authorize_payment_stage = match policy {
            ReleasePolicy::BreakerOnly => effectful_transform!(
                ValidatedOrder -> { PaymentAuthorized, PaymentAuthorizationUnavailable }
                with { gateway_breaker, gateway_limiter }
                => authorize_payment_handler
            ),
            ReleasePolicy::BreakerRecovery => effectful_transform!(
                ValidatedOrder -> { PaymentAuthorized, PaymentAuthorizationUnavailable }
                with { gateway_breaker, gateway_limiter, canonical_recovery() }
                => authorize_payment_handler
            ),
            ReleasePolicy::RetryOnly => effectful_transform!(
                ValidatedOrder -> { PaymentAuthorized, PaymentAuthorizationUnavailable }
                with canonical_recovery()
                => authorize_payment_handler
            ),
            ReleasePolicy::RetryWithLimiter => effectful_transform!(
                ValidatedOrder -> { PaymentAuthorized, PaymentAuthorizationUnavailable }
                with { gateway_limiter, canonical_recovery() }
                => authorize_payment_handler
            ),
            ReleasePolicy::RetryWithLimiterPermuted => effectful_transform!(
                ValidatedOrder -> { PaymentAuthorized, PaymentAuthorizationUnavailable }
                with { canonical_recovery(), gateway_limiter }
                => authorize_payment_handler
            ),
            ReleasePolicy::RetryWithLimiterSplit => {
                let effect_recovery = canonical_recovery().for_effect::<ScriptedAuthorizePayment>();
                effectful_transform!(
                    ValidatedOrder -> { PaymentAuthorized, PaymentAuthorizationUnavailable }
                    with { effect_recovery, gateway_limiter }
                    => authorize_payment_handler
                )
            }
        };
        let record_authorized = Discard::<PaymentAuthorized> {
            input: std::marker::PhantomData,
        };
        let record_unavailable = Discard::<PaymentAuthorizationUnavailable> {
            input: std::marker::PhantomData,
        };

        Ok(flow! {
            name: "payment_gateway_resilience_demo",
            journals: disk_journals(journal_root),

            stages: {
                orders = source!(ValidatedOrder => order_feed);
                authorize_payment = authorize_payment_stage;
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

impl<T: obzenflow_core::TypedPayload + Clone + Send + Sync + 'static> std::fmt::Debug
    for Discard<T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Discard")
    }
}
