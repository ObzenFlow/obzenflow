// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Safe observer interception demo.
//!
//! The observer receives a framework-owned sink-delivery classification and
//! uses ordinary Rust `tracing` for an application diagnostic. It cannot
//! replace the input, change settlement, publish a framework fact, or fail the
//! delivery through its observer return type. The panic treatment also shows
//! per-attachment quarantine: delivery and the following observer continue.
//!
//! ```text
//! cargo run -p obzenflow --example observer_interception_points -- --mode control
//! cargo run -p obzenflow --example observer_interception_points -- --mode trace
//! cargo run -p obzenflow --example observer_interception_points -- --mode panic
//! ```

use anyhow::{bail, Result};
use obzenflow::typed::{sinks, sources};
use obzenflow_adapters::middleware::sink_delivery_observer;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::observer::{SinkDeliveryObserver, SinkDeliveryObserverContext};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Copy)]
enum Mode {
    Control,
    Trace,
    Panic,
}

impl Mode {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "control" => Ok(Self::Control),
            "trace" => Ok(Self::Trace),
            "panic" => Ok(Self::Panic),
            other => bail!("unknown mode {other:?}; expected control, trace, or panic"),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Control => "control",
            Self::Trace => "trace",
            Self::Panic => "panic",
        }
    }
}

fn requested_mode() -> Result<Mode> {
    let mut args = std::env::args().skip(1);
    let mut mode = Mode::Trace;
    while let Some(arg) = args.next() {
        if arg == "--mode" {
            let value = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("--mode needs a value"))?;
            mode = Mode::parse(&value)?;
        } else if let Some(value) = arg.strip_prefix("--mode=") {
            mode = Mode::parse(value)?;
        } else {
            bail!("unknown argument {arg:?}; use --mode control|trace|panic");
        }
    }
    Ok(mode)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderAccepted {
    order_id: u64,
}

impl TypedPayload for OrderAccepted {
    const EVENT_TYPE: &'static str = "order.accepted";
    const SCHEMA_VERSION: u32 = 1;
}

struct DeliveryTrace;

impl SinkDeliveryObserver for DeliveryTrace {
    fn after_sink_delivery(&self, ctx: &SinkDeliveryObserverContext<'_>) {
        tracing::info!(
            stage = ctx.stage_name(),
            stage_input_position = ?ctx.stage_input_position(),
            outcome = ?ctx.outcome(),
            "sink delivery classified"
        );
    }
}

struct PanickingDeliveryTrace;

impl SinkDeliveryObserver for PanickingDeliveryTrace {
    fn after_sink_delivery(&self, _ctx: &SinkDeliveryObserverContext<'_>) {
        panic!("intentional observer panic; the runtime will quarantine this attachment");
    }
}

fn main() -> Result<()> {
    let mode = requested_mode()?;
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "info");
    }

    FlowApplication::builder()
        // `--mode` belongs to this example, not the framework CLI.
        .with_cli_args(["obzenflow"])
        .run_blocking(FlowDefinition::materialize(move |_runtime_config| {
            let orders = sources::finite(vec![
                OrderAccepted { order_id: 1001 },
                OrderAccepted { order_id: 1002 },
            ]);
            let shipping_handoff = sinks::console::<OrderAccepted, _>(|order: &OrderAccepted| {
                format!("shipping accepted order {}", order.order_id)
            });
            let delivered = match mode {
                Mode::Control => sink!(
                    OrderAccepted => shipping_handoff,
                    delivery: idempotent
                ),
                Mode::Trace => sink!(
                    OrderAccepted => shipping_handoff,
                    delivery: idempotent,
                    observers: [sink_delivery_observer("delivery-trace", DeliveryTrace)]
                ),
                Mode::Panic => sink!(
                    OrderAccepted => shipping_handoff,
                    delivery: idempotent,
                    observers: [
                        sink_delivery_observer("panicking-trace", PanickingDeliveryTrace),
                        sink_delivery_observer("delivery-trace", DeliveryTrace)
                    ]
                ),
            };

            Ok(flow! {
                name: "observer_interception_points",
                journals: disk_journals(PathBuf::from(format!(
                    "target/observer-interception-points-{}",
                    mode.label()
                ))),

                stages: {
                    accepted = source!(OrderAccepted => orders);
                    delivered = delivered;
                },

                topology: {
                    accepted |> delivered;
                }
            })
        }))?;

    Ok(())
}
