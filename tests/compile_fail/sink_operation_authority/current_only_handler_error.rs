// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::sink::{SinkWriteFailure, SinkWritePhase};

fn main() {
    let _ = SinkWriteFailure::current_only(
        SinkWritePhase::Execute,
        HandlerError::ContractViolation("connector-forged contract violation".into()),
    );
}
