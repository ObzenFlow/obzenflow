// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Framework integration with Runtime-owned stop admission and completion.
//!
//! This is technically public for Infra's cross-crate use, but is not a
//! supported application API. Runtime remains the sole admission and execution
//! outcome authority; observers neither extend deadlines nor select outcomes.

use crate::errors::FlowError;
use crate::pipeline::FlowHandle;
use crate::supervised_base::handle::ExecutionCancellation;

/// Emergency cancellation for an application-owned execution lifetime.
/// This retains cancellation capabilities independently of completion observers
/// and of the FlowHandle's ownership. Drop cannot claim joined completion.
pub struct ExecutionGuard {
    supervisor: Option<ExecutionCancellation>,
}

impl ExecutionGuard {
    pub(crate) fn new(supervisor: ExecutionCancellation) -> Self {
        Self {
            supervisor: Some(supervisor),
        }
    }

    /// Release the fallback after the owner has observed ordinary settlement.
    pub fn disarm(mut self) {
        self.supervisor = None;
    }
}

impl Drop for ExecutionGuard {
    fn drop(&mut self) {
        if let Some(supervisor) = &self.supervisor {
            supervisor.abort();
            // PipelineContext owns child cancellation when the parent future
            // is destroyed. Aborting children here races a still-running parent
            // poll, which can mistake our cancellation for an independent failure.
        }
    }
}

/// Protect the execution as soon as the application receives its built handle.
/// Dropping an ordinary wait remains independent of this explicit lifetime guard.
pub fn guard_execution(flow: &FlowHandle) -> ExecutionGuard {
    flow.execution_guard()
}

/// Join execution tasks and retained publications. This reports operational
/// errors only; Infra derives lifecycle outcomes from its journal projection.
pub async fn wait(flow: &FlowHandle) -> Result<(), FlowError> {
    flow.wait_for_resources().await
}
