// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Errors shared by application handlers.

pub use obzenflow_core::event::status::processing_status::ErrorKind;
pub use obzenflow_core::event::{StageFatalCode, StageFatalReason};
pub use obzenflow_runtime::stages::common::handler_error::{HandlerError, StageFatal};
