// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Generated scalar AI inference leaf.

use super::ai_effect::GeneratedChatEffectRow;
use super::stage_descriptor::{EffectfulTransformDescriptor, StageDescriptor};
use super::typing::{wrap_typed_descriptor, StageTypingMetadata, TypeHint};
use obzenflow_adapters::ai::InferenceHandlerAdapter;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::stages::InferenceHandler;
use std::fmt::Debug;
use std::num::NonZeroU64;

/// Macro-only constructor for the scalar AI leaf.
#[doc(hidden)]
pub fn generated_inference<Input, Out>(
    name: impl Into<String>,
    handler: impl InferenceHandler<Input = Input, Output = Out> + Clone + Debug + 'static,
    effect_row: GeneratedChatEffectRow,
) -> Box<dyn StageDescriptor>
where
    Input: TypedPayload + Clone + Send + Sync + 'static,
    Out: TypedPayload + Clone + Send + Sync + 'static,
{
    let GeneratedChatEffectRow {
        binding: _,
        declarations,
        policy_attachments,
    } = effect_row;
    let direct_bound = NonZeroU64::MIN.saturating_add(2);
    let descriptor = EffectfulTransformDescriptor::generated_for_surface::<Input>(
        "inference!",
        "stage",
        name,
        InferenceHandlerAdapter::new(handler),
        declarations,
        policy_attachments,
        direct_bound,
    );
    wrap_typed_descriptor(
        Box::new(descriptor),
        StageTypingMetadata::transform(
            TypeHint::exact_payload::<Input>(),
            TypeHint::exact_payload::<Out>(),
            false,
            None,
        ),
    )
}
