// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Error journal routing logic
//!
//! Determines whether an error-marked event should be routed to the stage's
//! error journal (isolation from downstream transport contracts) or kept in
//! the normal output pipeline.

use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
use obzenflow_core::ChainEvent;

/// Returns `true` if `event` should be written to the error journal instead of
/// the stage's data journal.
///
/// Infrastructure errors (Timeout, Remote, RateLimited, PermanentFailure,
/// Deserialization, Unknown) go to the error journal. Business errors
/// (Validation, Domain) stay in the main pipeline so downstream stages can
/// observe and potentially handle them.
pub(crate) fn route_to_error_journal(event: &ChainEvent) -> bool {
    match &event.processing_info.status {
        ProcessingStatus::Error { kind, .. } => match kind {
            Some(ErrorKind::Timeout)
            | Some(ErrorKind::Remote)
            | Some(ErrorKind::RateLimited)
            | Some(ErrorKind::PermanentFailure)
            | Some(ErrorKind::Deserialization) => true,
            Some(ErrorKind::Validation) | Some(ErrorKind::Domain) => false,
            None | Some(ErrorKind::Unknown) => true,
        },
        _ => false,
    }
}
