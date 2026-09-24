// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Portable discovery/control data for clients of a hosted application.
pub use obzenflow_infra::web::endpoints::flow_control::{
    FlowControlAction, FlowControlRequest, FlowControlResponse, FlowControlStatus,
};
pub use obzenflow_infra::web::run_control::*;
pub use obzenflow_infra::web::RuntimeInstanceId;
