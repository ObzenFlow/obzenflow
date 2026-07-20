// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed evidence returned by a completed effectful stage invocation.

use obzenflow_core::{EventType, StageFactSet};
use std::marker::PhantomData;

/// Evidence that one effectful handler invocation completed after authoring
/// the reported set of already-durable user facts.
///
/// A completion is neither a transaction result nor a persisted event. It
/// contains no domain values and cannot append output, execute an effect, or
/// re-enter a supervisor.
#[must_use = "an effectful handler must return its StageCompletion receipt"]
#[derive(Debug)]
pub struct StageCompletion<Output: StageFactSet> {
    committed_fact_count: usize,
    committed_fact_types: Vec<EventType>,
    _output: PhantomData<fn() -> Output>,
}

impl<Output: StageFactSet> StageCompletion<Output> {
    pub(super) fn new(committed_fact_count: usize, committed_fact_types: Vec<EventType>) -> Self {
        Self {
            committed_fact_count,
            committed_fact_types,
            _output: PhantomData,
        }
    }

    /// Number of user facts durably committed by this invocation.
    #[must_use]
    pub fn committed_fact_count(&self) -> usize {
        self.committed_fact_count
    }

    /// User fact types in durable commit order, including repeated types.
    #[must_use]
    pub fn committed_fact_types(&self) -> &[EventType] {
        &self.committed_fact_types
    }
}
