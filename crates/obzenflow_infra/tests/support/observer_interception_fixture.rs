// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Private deterministic fixture for observer non-interference verification.

use async_trait::async_trait;
use obzenflow_adapters::middleware::{
    effect_observer, sink_delivery_observer, stage_lifecycle_observer, SinkDeliveryObserver,
};
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{effectful_transform, flow, sink, source, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::{EffectError, Effects, StageCompletion, System};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    EffectfulTransformHandler, InlineSink, SinkDescription, SinkWriteFailure,
    TypedFiniteSourceHandler,
};
use obzenflow_runtime::stages::observer::{
    EffectObserver, EffectObserverContext, ObserverResult, SinkDeliveryObserverContext,
    StageLifecycleObserver, StageLifecycleObserverContext,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub(crate) const ORDER_COUNT: usize = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ObserverTreatment {
    WithoutObservers,
    Observers,
    PanickingObserver,
}

impl ObserverTreatment {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::WithoutObservers => "without-observers",
            Self::Observers => "observers",
            Self::PanickingObserver => "panicking-observer",
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct Probe {
    source_polls: Arc<AtomicUsize>,
    effect_calls: Arc<AtomicUsize>,
    sink_writes: Arc<AtomicUsize>,
    effect_callbacks: Arc<AtomicUsize>,
    delivery_callbacks: Arc<AtomicUsize>,
    delivered_callbacks: Arc<AtomicUsize>,
    lifecycle_callbacks: Arc<AtomicUsize>,
    panicking_callbacks: Arc<AtomicUsize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProbeSnapshot {
    pub(crate) source_polls: usize,
    pub(crate) effect_calls: usize,
    pub(crate) sink_writes: usize,
    pub(crate) effect_callbacks: usize,
    pub(crate) delivery_callbacks: usize,
    pub(crate) delivered_callbacks: usize,
    pub(crate) lifecycle_callbacks: usize,
    pub(crate) panicking_callbacks: usize,
}

impl Probe {
    pub(crate) fn snapshot(&self) -> ProbeSnapshot {
        ProbeSnapshot {
            source_polls: self.source_polls.load(Ordering::SeqCst),
            effect_calls: self.effect_calls.load(Ordering::SeqCst),
            sink_writes: self.sink_writes.load(Ordering::SeqCst),
            effect_callbacks: self.effect_callbacks.load(Ordering::SeqCst),
            delivery_callbacks: self.delivery_callbacks.load(Ordering::SeqCst),
            delivered_callbacks: self.delivered_callbacks.load(Ordering::SeqCst),
            lifecycle_callbacks: self.lifecycle_callbacks.load(Ordering::SeqCst),
            panicking_callbacks: self.panicking_callbacks.load(Ordering::SeqCst),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct OrderAccepted {
    pub(crate) order_id: u64,
}

impl TypedPayload for OrderAccepted {
    const EVENT_TYPE: &'static str = "order.accepted";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ShippingAuthorised {
    pub(crate) order_id: u64,
    pub(crate) authorisation_id: String,
}

impl TypedPayload for ShippingAuthorised {
    const EVENT_TYPE: &'static str = "shipping.authorised";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ShippingReady {
    pub(crate) order_id: u64,
}

impl TypedPayload for ShippingReady {
    const EVENT_TYPE: &'static str = "shipping.ready";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Clone, Debug)]
struct OrderSource {
    next: usize,
    polls: Arc<AtomicUsize>,
}

impl TypedFiniteSourceHandler for OrderSource {
    type Output = OrderAccepted;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        if self.next == ORDER_COUNT {
            return Ok(None);
        }
        let order_id = 1001 + self.next as u64;
        self.next += 1;
        Ok(Some(vec![OrderAccepted { order_id }]))
    }
}

struct ShippingDesk {
    calls: Arc<AtomicUsize>,
}

#[derive(Clone, Debug, Serialize)]
struct AuthoriseShippingRequest {
    order_id: u64,
}

#[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
impl ShippingDesk {
    #[effect(type = "shipping.authorise", exactly_once)]
    fn authorise(
        &self,
        request: &AuthoriseShippingRequest,
    ) -> Result<ShippingAuthorised, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(ShippingAuthorised {
            order_id: request.order_id,
            authorisation_id: format!("shipping-auth-{}", request.order_id),
        })
    }
}

#[derive(Clone, Debug, obzenflow_runtime::effects::EffectHandler)]
#[effect_handler(effects = obzenflow_runtime::effects)]
struct AuthoriseShipping {
    #[system]
    desk: System<ShippingDesk>,
}

#[async_trait]
impl EffectfulTransformHandler for AuthoriseShipping {
    type Input = OrderAccepted;
    type Output = obzenflow_core::stage_fact_set![ShippingAuthorised, ShippingReady];
    type AllowedEffects = AuthoriseShippingRequest;

    async fn process(
        &self,
        order: OrderAccepted,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        let authorised = fx
            .call(AuthoriseShippingRequest {
                order_id: order.order_id,
            })
            .await?;
        fx.emit(ShippingReady {
            order_id: authorised.order_id,
        })
        .await?;
        Ok(fx.complete()?)
    }

    fn stage_logic_version(&self) -> &str {
        "observer-interception-authorise-shipping-v1"
    }
}

#[derive(Clone, Debug)]
struct ShippingHandoff {
    writes: Arc<AtomicUsize>,
}

#[async_trait]
impl InlineSink for ShippingHandoff {
    type Input = ShippingReady;

    fn describe(&self) -> SinkDescription {
        SinkDescription::method(DeliveryMethod::Custom("shipping-handoff".to_string()))
    }

    async fn write(&mut self, order: ShippingReady) -> Result<(), SinkWriteFailure> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        tracing::info!(order_id = order.order_id, "shipping accepted order");
        Ok(())
    }
}

struct EffectProbeObserver {
    calls: Arc<AtomicUsize>,
}

impl EffectObserver for EffectProbeObserver {
    fn after_effect(&self, ctx: &EffectObserverContext<'_>) {
        assert_eq!(ctx.effect_type(), "shipping.authorise");
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}

struct DeliveryProbeObserver {
    calls: Arc<AtomicUsize>,
    delivered: Arc<AtomicUsize>,
}

impl SinkDeliveryObserver for DeliveryProbeObserver {
    type Input = ShippingReady;

    fn on_delivered(&self, order: &ShippingReady) -> ObserverResult {
        self.delivered.fetch_add(1, Ordering::SeqCst);
        tracing::info!(order_id = order.order_id, "shipping delivery committed");
        Ok(())
    }

    fn on_attempt(&self, ctx: &SinkDeliveryObserverContext<'_>) -> ObserverResult {
        self.calls.fetch_add(1, Ordering::SeqCst);
        tracing::info!(
            flow_id = %ctx.flow_id(),
            stage = ctx.stage_name(),
            stage_input_position = ?ctx.stage_input_position(),
            outcome = ?ctx.outcome(),
            "sink delivery classified"
        );
        Ok(())
    }
}

struct LifecycleProbeObserver {
    calls: Arc<AtomicUsize>,
}

impl StageLifecycleObserver for LifecycleProbeObserver {
    fn on_stage_lifecycle(&self, ctx: &StageLifecycleObserverContext<'_>) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        tracing::info!(
            flow_id = %ctx.flow_id(),
            stage = ctx.stage_name(),
            phase = ?ctx.phase(),
            "sink lifecycle classified"
        );
    }
}

struct PanickingDeliveryObserver {
    calls: Arc<AtomicUsize>,
}

impl SinkDeliveryObserver for PanickingDeliveryObserver {
    type Input = ShippingReady;

    fn on_attempt(&self, _ctx: &SinkDeliveryObserverContext<'_>) -> ObserverResult {
        self.calls.fetch_add(1, Ordering::SeqCst);
        panic!("intentional observer panic; the runtime will quarantine this attachment");
    }
}

pub(crate) fn build_flow(
    journal_root: PathBuf,
    treatment: ObserverTreatment,
    probe: Probe,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let orders = OrderSource {
            next: 0,
            polls: probe.source_polls.clone(),
        };
        let shipping_handoff = ShippingHandoff {
            writes: probe.sink_writes.clone(),
        };
        let desk = ShippingDesk {
            calls: probe.effect_calls.clone(),
        };
        let authorise_shipping = AuthoriseShipping {
            desk: System::eager("shipping-desk", Arc::new(desk))?,
        };
        let authorised = match treatment {
            ObserverTreatment::WithoutObservers => effectful_transform!(
                OrderAccepted -> { ShippingAuthorised, ShippingReady } => authorise_shipping),
            ObserverTreatment::Observers | ObserverTreatment::PanickingObserver => {
                effectful_transform!(
                    OrderAccepted -> { ShippingAuthorised, ShippingReady } with { effect_observer(
                        "effect-probe",
                        EffectProbeObserver {
                            calls: probe.effect_callbacks.clone(),
                        }
                    ) } => authorise_shipping)
            }
        };
        let delivered = match treatment {
            ObserverTreatment::WithoutObservers => sink!(
                ShippingReady => shipping_handoff.idempotent()
            ),
            ObserverTreatment::Observers => sink!(
                ShippingReady with { stage_lifecycle_observer(
                        "lifecycle-probe",
                        LifecycleProbeObserver {
                            calls: probe.lifecycle_callbacks.clone(),
                        }
                    ),
                    sink_delivery_observer(
                        "delivery-probe",
                        DeliveryProbeObserver {
                            calls: probe.delivery_callbacks.clone(),
                            delivered: probe.delivered_callbacks.clone(),
                        }
                    ) } => shipping_handoff.idempotent()),
            ObserverTreatment::PanickingObserver => sink!(
                ShippingReady with { stage_lifecycle_observer(
                        "lifecycle-probe",
                        LifecycleProbeObserver {
                            calls: probe.lifecycle_callbacks.clone(),
                        }
                    ),
                    sink_delivery_observer(
                        "panicking-delivery-probe",
                        PanickingDeliveryObserver {
                            calls: probe.panicking_callbacks.clone(),
                        }
                    ),
                    sink_delivery_observer(
                        "delivery-probe",
                        DeliveryProbeObserver {
                            calls: probe.delivery_callbacks.clone(),
                            delivered: probe.delivered_callbacks.clone(),
                        }
                    ) } => shipping_handoff.idempotent()),
        };

        Ok(flow! {
            name: "observer_interception_non_interference_fixture",
            journals: disk_journals(journal_root),

            stages: {
                accepted = source!(OrderAccepted => orders);
                authorised = authorised;
                delivered = delivered;
            },

            topology: {
                accepted |> authorised;
                authorised |> delivered;
            }
        })
    })
}
