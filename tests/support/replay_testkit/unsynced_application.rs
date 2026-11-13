// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::unsynced_config::unsynced_config;
use obzenflow_infra::application::{FlowApplication, FlowApplicationBuilder};

pub fn unsynced_application() -> FlowApplicationBuilder {
    FlowApplication::builder().with_config_file(unsynced_config())
}
