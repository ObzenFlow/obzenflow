// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Commands are the requests that an effectful stage or a sink performs through a
//! port. A stage writes `perform(request).through(port)` or holds its ports in a
//! `Command`. The effect journal records each call.

mod command;
mod perform;
mod request;

#[doc(hidden)]
pub use command::{
    assert_command, assert_handler_performs_no_ai_effect, assert_port_answer,
    assert_stateful_handler_performs_no_ai_effect, effectful_stateful_effects,
    effectful_transform_effects, AnswersEffect, DeclareEffects, EveryPortAnswers, FieldEffects,
    FieldsEffects, FindPort, HandlerPerformableEffects, JoinMembers, MarkPortUsed, NarrowedPort,
    PortAfter, PortAnswer, PortAnswersAnEffect, PortEffects, PortFound, PortList, PortMark,
    PortReply, PortServesRequest, StageEffectsError, Unused, Used, WholePort,
};
pub use command::{Command, StagePort};
#[doc(hidden)]
pub use perform::PerformedRequest;
pub use perform::{perform, Perform, PerformRequest};
pub use request::CallRequest;
