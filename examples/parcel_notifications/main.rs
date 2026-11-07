// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Parcel notifications, with every middleware setting inspectable in Studio. Start
//! studiod with the Studio bundle, then run from the repository root:
//!
//! ```text
//! cargo run -p obzenflow --example parcel_notifications --features studio -- \
//!     --config examples/parcel_notifications/obzenflow.studio.toml
//! ```
//!
//! Open http://127.0.0.1:7010, choose parcel_notifications, and inspect each stage's
//! middleware before pressing Start. The run stays up afterwards for inspection; stop it
//! with Ctrl-C.

mod domain;
mod flow;
mod handlers;
mod services;

use obzenflow::application::{Banner, FlowApplication, LogLevel, Presentation};

fn main() -> std::process::ExitCode {
    let presentation = Presentation::new(
        Banner::new("Parcel Notifications")
            .description(
                "Carrier updates become customer emails and texts. Code sets each stage's \
                 middleware, the operator's config refines it, and Studio shows every \
                 setting with where its value came from.",
            )
            .bullets(
                "What to look for in Studio",
                [
                    "carrier_updates: the limiter's rate comes from the config file, its burst from the code",
                    "locate_parcel: a breaker that opens on failures or slow calls, a fixed retry, and an automatic burst",
                    "notify_customer: one limiter per provider, so the canvas shows 2 attachments",
                    "notify_customer: the config switches the email breaker to consecutive failures and its retry to exponential backoff, so the settings they no longer use show as inactive",
                ],
            )
            .section(
                "Usage",
                "Studio: cargo run -p obzenflow --example parcel_notifications --features studio -- --config examples/parcel_notifications/obzenflow.studio.toml\nConsole: cargo run -p obzenflow --example parcel_notifications",
            ),
    );

    let result = FlowApplication::builder()
        .with_log_level(LogLevel::Info)
        .with_presentation(presentation)
        .run_blocking(flow::build_flow());

    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err}");
            std::process::ExitCode::from(err.process_exit_code())
        }
    }
}
