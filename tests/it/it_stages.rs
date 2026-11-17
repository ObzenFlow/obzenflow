// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[path = "../support/replay_testkit"]
mod replay_testkit {
    mod discard;
    mod eof_clock;
    mod failures;
    mod journal;
    mod manifest;
    mod ordered_journal;
    mod run_dir;
    mod stage_journal;
    mod stage_writer;
    mod unsynced_application;
    mod unsynced_candidate;
    mod unsynced_config;
    mod unsynced_context;
    pub use self::{
        discard::*, eof_clock::*, failures::*, journal::*, manifest::*, ordered_journal::*,
        run_dir::*, stage_journal::*, unsynced_application::*, unsynced_context::*,
    };
}

#[path = "../../examples/csv_demo_support_sla/support.rs"]
pub mod csv_demo;

mod stages;
