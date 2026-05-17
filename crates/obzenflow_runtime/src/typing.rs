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

// FLOWIP-114c PR D: the previous mixed-input / partial-typing assertion
// helpers (`assert_transform_output`, `assert_stateful_output`,
// `assert_stateful_contract`, `assert_sink_input`, `assert_join_contract`,
// `assert_join_output`, `assert_join_reference_output`,
// `assert_join_stream_output`) are removed. The DSL no longer has authoring
// surfaces that need to bypass `TransformTyping`/`SinkTyping`/`StatefulTyping`/
// `JoinTyping`. The remaining typed contract is established at the macro
// expansion site by emitting `TypeHint::exact::<T>()` metadata; runtime
// fingerprinting is type-erased through `ChainEvent`.
