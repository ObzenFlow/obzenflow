// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Application stop requests and journal observations. Runtime owns deadlines.

use obzenflow_core::event::PipelineStopAdmission;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in super::super) enum StopCommand {
    Graceful,
    Cancel,
}
#[derive(Clone, Copy, Debug)]
pub(in super::super) enum StopReason {
    Graceful,
    Cancel,
    BeforeRun,
}
#[derive(Clone, Copy, Debug)]
pub(in super::super) enum FlowActivity {
    BeforeRun,
    Executing,
    Terminal,
}
#[derive(Clone, Debug)]
pub(in super::super) struct StopInput {
    pub activity: FlowActivity,
    pub admitted: Option<PipelineStopAdmission>,
}
#[derive(Clone, Debug, PartialEq)]
pub(in super::super) struct Settlement {
    pub(super) requested: Option<StopCommand>,
    pub(super) admitted: Option<PipelineStopAdmission>,
}
impl Settlement {
    pub(super) fn begin(
        reason: StopReason,
        input: &StopInput,
        _grace: std::time::Duration,
    ) -> (Self, Option<StopCommand>) {
        let command = match (&input.activity, &input.admitted, reason) {
            (FlowActivity::Terminal, _, _) | (_, Some(PipelineStopAdmission::Cancel { .. }), _) => {
                None
            }
            (_, _, StopReason::Cancel | StopReason::BeforeRun)
            | (FlowActivity::BeforeRun, _, _) => Some(StopCommand::Cancel),
            (_, Some(PipelineStopAdmission::Graceful { .. }), _) => None,
            _ => Some(StopCommand::Graceful),
        };
        (
            Self {
                requested: command,
                admitted: input.admitted.clone(),
            },
            command,
        )
    }
    pub(super) fn observe(&mut self, admission: &Option<PipelineStopAdmission>) {
        if admission.is_some() {
            self.admitted = admission.clone();
        }
    }
}
