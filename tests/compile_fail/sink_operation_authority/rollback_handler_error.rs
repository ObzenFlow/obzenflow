// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::{StageFatalCode, StageFatalReason};
use obzenflow_runtime::stages::common::handler_error::{HandlerError, StageFatal};
use obzenflow_runtime::stages::sink::{SinkWriteFailure, SinkWritePhase};

fn main() {
    let _ = SinkWriteFailure::confirmed_rollback(
        SinkWritePhase::Commit,
        HandlerError::Fatal(StageFatal::new(
            StageFatalCode::Protocol,
            StageFatalReason::ProtocolInputIntegrity,
            "connector-forged fatal",
        )),
    );
}
