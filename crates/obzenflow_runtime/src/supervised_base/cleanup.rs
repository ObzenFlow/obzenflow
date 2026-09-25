// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Internal resource settlement required by handler supervision.

use super::base::Supervisor;

/// Resource settlement after every orderly return from the consuming handler runner.
///
/// This supertrait lives in a crate-private module, like `Supervisor`: cleanup is
/// a runtime obligation, not an application extension point or a task-builder choice.
/// Supervisors without asynchronous resource settlement explicitly use the default.
/// Wrappers must forward settlement to their inner supervisor.
///
/// Implementations consume cleanup eligibility before awaiting. They must not
/// dispatch, poll, author business data or EOF, or manufacture lifecycle success.
/// Panic, forced task abortion and process death do not promise awaited cleanup.
#[async_trait::async_trait]
pub trait HandlerSupervisedCleanup: Supervisor + Sync {
    async fn cleanup_after_run(
        &mut self,
        _context: &Self::Context,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}
