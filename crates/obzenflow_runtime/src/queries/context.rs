// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::observability::{ObservationRecord, ObservationRecorder};
use std::sync::Arc;

/// The read position and measurement destination for a live query.
#[derive(Clone)]
pub struct QueryContext {
    pub(crate) stage_key: String,
    pub(crate) number: u64,
    pub(crate) recorder: Option<Arc<dyn ObservationRecorder>>,
}

impl QueryContext {
    pub fn stage_key(&self) -> &str {
        &self.stage_key
    }

    pub fn read_number(&self) -> u64 {
        self.number
    }

    pub fn observe(&self, record: ObservationRecord) {
        if let Some(recorder) = &self.recorder {
            recorder.observe(record);
        }
    }

    pub fn for_test(stage_key: &str, number: u64) -> Self {
        Self {
            stage_key: stage_key.to_owned(),
            number,
            recorder: None,
        }
    }
}

impl std::fmt::Debug for QueryContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QueryContext")
            .field("stage_key", &self.stage_key)
            .field("number", &self.number)
            .finish()
    }
}
