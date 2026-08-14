// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Safe observer interception demo.
//!
//! The sink-delivery observer receives a framework-owned classification and
//! the lifecycle observer receives framework-owned stage phases. Both use
//! ordinary Rust `tracing` for application diagnostics. Neither can replace
//! input, change settlement, publish a framework fact, or fail the operation
//! through its return type. The panic treatment also shows per-attachment
//! quarantine: delivery and the following observer continue.
//!
//! ```text
//! cargo run -p obzenflow --example observer_interception_points -- --mode control
//! cargo run -p obzenflow --example observer_interception_points -- --mode trace
//! cargo run -p obzenflow --example observer_interception_points -- --mode panic
//! ```

#[path = "observer_interception_points/support.rs"]
mod support;

use anyhow::{bail, Result};
use obzenflow_infra::application::FlowApplication;
use std::path::PathBuf;
use support::{build_flow, Mode, Probe};

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

fn main() -> Result<()> {
    let mode = requested_mode()?;
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "info");
    }

    let probe = Probe::default();
    FlowApplication::builder()
        // `--mode` belongs to this example, not the framework CLI.
        .with_cli_args(["obzenflow"])
        .run_blocking(build_flow(
            PathBuf::from(format!(
                "target/observer-interception-points-{}",
                mode.label()
            )),
            mode,
            probe.clone(),
        ))?;

    let snapshot = probe.snapshot();
    tracing::info!(
        mode = mode.label(),
        source_polls = snapshot.source_polls,
        sink_writes = snapshot.sink_writes,
        delivery_callbacks = snapshot.delivery_callbacks,
        lifecycle_callbacks = snapshot.lifecycle_callbacks,
        panicking_callbacks = snapshot.panicking_callbacks,
        "observer interception example complete"
    );
    Ok(())
}
