// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Commands are the requests that an effectful stage or a sink performs through a
//! port. A stage writes `perform(request).through(port)`, or declares a
//! [`Command`] that holds its ports. The effect journal records each call.

pub use obzenflow_runtime::commands::{perform, Command, Perform, PerformRequest, StagePort};
