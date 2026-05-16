// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Authoring-time stage typing trait markers and assertion helpers.
//!
//! These traits do not affect runtime execution semantics. They are used
//! by the DSL macros to validate that a handler's declared type shape
//! matches the contract written at the stage-definition layer.
//!
//! The serialisable data carriers (`TypeHintInfo`, `StageTypingInfo`) live
//! in `obzenflow_topology` as canonical annotations on `StageInfo`; import
//! them from there directly.

/// Source stage typing contract.
pub trait SourceTyping {
    type Output;
}

/// Transform stage typing contract.
pub trait TransformTyping {
    type Input;
    type Output;
}

/// Stateful stage typing contract.
pub trait StatefulTyping {
    type Input;
    type Output;
}

/// Sink stage typing contract.
pub trait SinkTyping {
    type Input;
}

/// Join stage typing contract.
pub trait JoinTyping {
    type Reference;
    type Stream;
    type Output;
}

/// Compile-time assertion helper for source handlers.
pub fn assert_source_output<H, Out>(_: &H)
where
    H: SourceTyping<Output = Out>,
{
}

/// Compile-time assertion helper for transform handlers.
pub fn assert_transform_contract<H, In, Out>(_: &H)
where
    H: TransformTyping<Input = In, Output = Out>,
{
}

/// Compile-time assertion helper for mixed-input transform handlers.
pub fn assert_transform_output<H, Out>(_: &H)
where
    H: TransformTyping<Output = Out>,
{
}

/// Compile-time assertion helper for stateful handlers.
pub fn assert_stateful_contract<H, In, Out>(_: &H)
where
    H: StatefulTyping<Input = In, Output = Out>,
{
}

/// Compile-time assertion helper for mixed-input stateful handlers.
pub fn assert_stateful_output<H, Out>(_: &H)
where
    H: StatefulTyping<Output = Out>,
{
}

/// Compile-time assertion helper for sink handlers.
pub fn assert_sink_input<H, In>(_: &H)
where
    H: SinkTyping<Input = In>,
{
}

/// Compile-time assertion helper for join handlers with exact reference and stream types.
pub fn assert_join_contract<H, Ref, Stream, Out>(_: &H)
where
    H: JoinTyping<Reference = Ref, Stream = Stream, Output = Out>,
{
}

/// Compile-time assertion helper for joins with only output proven.
pub fn assert_join_output<H, Out>(_: &H)
where
    H: JoinTyping<Output = Out>,
{
}

/// Compile-time assertion helper for joins with exact reference type and output.
pub fn assert_join_reference_output<H, Ref, Out>(_: &H)
where
    H: JoinTyping<Reference = Ref, Output = Out>,
{
}

/// Compile-time assertion helper for joins with exact stream type and output.
pub fn assert_join_stream_output<H, Stream, Out>(_: &H)
where
    H: JoinTyping<Stream = Stream, Output = Out>,
{
}
