// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[path = "../support/replay_testkit"]
mod replay_testkit {
    mod journal;
    mod unsynced_application;
    mod unsynced_candidate;
    mod unsynced_config;
    mod unsynced_context;
    pub use self::{
        journal::*, unsynced_application::*, unsynced_candidate::*, unsynced_context::*,
    };
}

#[path = "../support/exported_jsonl"]
mod exported_jsonl {
    mod chain;
    #[cfg(all(feature = "web-host", feature = "prometheus"))]
    mod commitment;
    #[cfg(all(feature = "web-host", feature = "prometheus"))]
    mod without_observations;
    pub use self::chain::*;
    #[cfg(all(feature = "web-host", feature = "prometheus"))]
    pub use self::{commitment::*, without_observations::*};
}

#[path = "../../examples/payment_gateway_resilience/support.rs"]
pub mod payment_gateway;

#[path = "../../examples/csv_demo_support_sla/support.rs"]
pub mod csv_demo;

pub mod examples;
