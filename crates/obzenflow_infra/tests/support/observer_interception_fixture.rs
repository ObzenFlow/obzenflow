// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Private deterministic fixture for observer non-interference verification.

use async_trait::async_trait;
use obzenflow_adapters::middleware::{sink_delivery_observer, stage_lifecycle_observer};
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, SinkDescription, SinkTerminalOutcome, SinkWriteContext, SinkWriteReport,
    TypedFiniteSourceHandler,
};
use obzenflow_runtime::stages::observer::{
    SinkDeliveryObserver, SinkDeliveryObserverContext, StageLifecycleObserver,
    StageLifecycleObserverContext,
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
    sink_writes: Arc<AtomicUsize>,
    delivery_callbacks: Arc<AtomicUsize>,
    lifecycle_callbacks: Arc<AtomicUsize>,
    panicking_callbacks: Arc<AtomicUsize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProbeSnapshot {
    pub(crate) source_polls: usize,
    pub(crate) sink_writes: usize,
    pub(crate) delivery_callbacks: usize,
    pub(crate) lifecycle_callbacks: usize,
    pub(crate) panicking_callbacks: usize,
}

impl Probe {
    pub(crate) fn snapshot(&self) -> ProbeSnapshot {
        ProbeSnapshot {
            source_polls: self.source_polls.load(Ordering::SeqCst),
            sink_writes: self.sink_writes.load(Ordering::SeqCst),
            delivery_callbacks: self.delivery_callbacks.load(Ordering::SeqCst),
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

#[derive(Clone, Debug)]
struct ShippingHandoff {
    writes: Arc<AtomicUsize>,
}

#[async_trait]
impl InlineSink for ShippingHandoff {
    type Input = OrderAccepted;

    fn describe(&self) -> SinkDescription {
        SinkDescription::unspecified()
    }

    async fn write(
        &mut self,
        order: OrderAccepted,
        _context: SinkWriteContext,
    ) -> Result<SinkWriteReport, HandlerError> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        tracing::info!(order_id = order.order_id, "shipping accepted order");
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            DeliveryMethod::Custom("shipping-handoff".to_string()),
            None,
        )))
    }
}

struct DeliveryProbeObserver {
    calls: Arc<AtomicUsize>,
}

impl SinkDeliveryObserver for DeliveryProbeObserver {
    fn after_sink_delivery(&self, ctx: &SinkDeliveryObserverContext<'_>) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        tracing::info!(
            flow_id = %ctx.flow_id(),
            stage = ctx.stage_name(),
            stage_input_position = ?ctx.stage_input_position(),
            outcome = ?ctx.outcome(),
            "sink delivery classified"
        );
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
    fn after_sink_delivery(&self, _ctx: &SinkDeliveryObserverContext<'_>) {
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
        let delivered = match treatment {
            ObserverTreatment::WithoutObservers => sink!(
                OrderAccepted => shipping_handoff,
                delivery: idempotent
            ),
            ObserverTreatment::Observers => sink!(
                OrderAccepted => shipping_handoff,
                delivery: idempotent,
                observers: [
                    stage_lifecycle_observer(
                        "lifecycle-probe",
                        LifecycleProbeObserver {
                            calls: probe.lifecycle_callbacks.clone(),
                        }
                    ),
                    sink_delivery_observer(
                        "delivery-probe",
                        DeliveryProbeObserver {
                            calls: probe.delivery_callbacks.clone(),
                        }
                    )
                ]
            ),
            ObserverTreatment::PanickingObserver => sink!(
                OrderAccepted => shipping_handoff,
                delivery: idempotent,
                observers: [
                    stage_lifecycle_observer(
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
                        }
                    )
                ]
            ),
        };

        Ok(flow! {
            name: "observer_interception_non_interference_fixture",
            journals: disk_journals(journal_root),

            stages: {
                accepted = source!(OrderAccepted => orders);
                delivered = delivered;
            },

            topology: {
                accepted |> delivered;
            }
        })
    })
}
