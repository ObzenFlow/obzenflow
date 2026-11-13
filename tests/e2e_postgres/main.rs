// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[path = "../support/postgres"]
mod postgres_support {
    mod connection;
    mod pool;
    pub use self::{connection::*, pool::*};
}

#[path = "../support/replay_testkit"]
mod replay_testkit {
    mod unsynced_application;
    mod unsynced_config;
    pub use self::unsynced_application::*;
}

mod inventory_consumer;
mod payments;
