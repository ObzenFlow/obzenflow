// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::observability::{ObservationRecord, ObservationSource};
use obzenflow_core::event::EdgeLivenessState;
use obzenflow_core::StageId;
use std::sync::{Arc, Mutex};

/// The existing liveness tests observe the live view while driving the flow.
/// Measurements have no journal history; only the test retains seen states.
#[derive(Default)]
pub struct LivenessTrace {
    pub source: Arc<Mutex<Option<Arc<dyn ObservationSource>>>>,
    pub states: Vec<(StageId, StageId, EdgeLivenessState)>,
}

impl LivenessTrace {
    pub fn capture(&mut self) {
        let source = self.source.lock().unwrap().clone();
        let Some(source) = source else { return };
        for packet in source.snapshot() {
            for record in packet.records {
                if let ObservationRecord::EdgeLiveness {
                    upstream,
                    reader,
                    state,
                    ..
                } = record
                {
                    let value = (upstream, reader, state);
                    if !self.states.contains(&value) {
                        self.states.push(value);
                    }
                }
            }
        }
    }
}
