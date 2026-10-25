// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::AiEffect;
use crate::stages::common::handler_error::HandlerError;
use obzenflow_core::ai::AiOutcome;

/// The request an AI role prepares for its effect.
pub type AiRequest<R, In> = <<R as AiRole<In>>::Effect as AiEffect>::Request;

/// The reply an AI role interprets for its effect.
pub type AiReply<R, In> = <<R as AiRole<In>>::Effect as AiEffect>::Reply;

/// An AI role, the domain logic around one model call per input.
///
/// `prepare` builds the request and `interpret` turns the input and the call's
/// outcome into the role's output. Both are synchronous, deterministic and free
/// of I/O. Replay recomputes each request, and `prepare` may run more than once
/// for one input. One type may implement several roles, one per input type.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not an AI role for `{In}`",
    label = "this role cannot take `{In}`",
    note = "implement `AiRole<{In}>` for `{Self}`; a map role takes `Chunk<T>`, a reduce role takes `Partials<T, P>` whose `P` is the map role's output"
)]
pub trait AiRole<In>: Send + Sync + 'static {
    /// The AI effect the role's call performs.
    type Effect: AiEffect;
    /// What the role produces for one input.
    type Out;

    fn prepare(&self, input: &In) -> Result<AiRequest<Self, In>, HandlerError>;

    fn interpret(
        &self,
        input: In,
        outcome: AiOutcome<AiReply<Self, In>>,
    ) -> Result<Self::Out, HandlerError>;

    /// The role's logic version, which changes only when its logic changes.
    fn stage_logic_version(&self) -> &str {
        "1"
    }
}
