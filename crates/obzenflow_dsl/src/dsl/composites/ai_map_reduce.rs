// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Generated AI map-reduce protocol descriptor.
//!
//! This module deliberately exposes no programmatic builder. The
//! `ai_map_reduce!` macro is the only public authoring surface and expands to
//! the fixed four-stage FLOWIP-128g protocol.

mod effects;

use self::effects::{GeneratedAiFinaliseHandler, GeneratedAiMapHandler};
use crate::dsl::ai_effect::require_generated_chat_resilience;
use crate::dsl::composition::{CompositeBuildContext, CompositeBuildError, CompositeDescriptor};
use crate::dsl::stage_descriptor::{
    EffectPolicyAttachment, EffectfulTransformDescriptor, StatefulDescriptor, TransformDescriptor,
};
use crate::dsl::typing::{wrap_typed_descriptor, StageTypingMetadata, TypeHint};
use obzenflow_adapters::ai::effects::ChatCompletion;
use obzenflow_adapters::middleware::MiddlewareFactory;
use obzenflow_core::ai::{
    AiFinaliseRole, AiMapReduceChunkFailed, AiMapReduceFinaliseFailed, AiMapReduceJobFailed,
    AiMapReduceMapInput, AiMapReducePlanningFailed, AiMapReducePlanningManifest,
    AiMapReduceReduceInput, AiMapReduceTaggedPartial, AiMapRole, ChatBindingContract,
    ChunkEnvelope, Many,
};
use obzenflow_core::id::CompositeId;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::effects::{Effect, EffectDeclaration};
use obzenflow_runtime::stages::stateful::SeededCollectByInput;
use obzenflow_runtime::stages::transform::strategies::ai_chunking::generated_ai_chunk_handler;
use obzenflow_runtime::stages::transform::ChunkByBudgetTyped;
use std::fmt;
use std::marker::PhantomData;
use std::num::NonZeroU64;

type GeneratedEffectPolicies = (
    Vec<Box<dyn MiddlewareFactory>>,
    Vec<Box<dyn MiddlewareFactory>>,
);
type GeneratedMapReduceTypes<Seed, Item, Partial, Out> = fn() -> (Seed, Item, Partial, Out);

/// Macro-only constructor for the FLOWIP-128g generated protocol.
///
/// The concrete role adapters, effect declarations, collector, and
/// direct-fact bounds are fixed as one generated call graph.
#[doc(hidden)]
pub fn generated_map_reduce<Seed, Item, Partial, Out, MapRole, FinaliseRole>(
    name: impl Into<String>,
    roles: (ChunkByBudgetTyped<Seed, Item>, MapRole, FinaliseRole),
    chat_bindings: (ChatBindingContract, ChatBindingContract),
    policies: GeneratedEffectPolicies,
) -> Box<dyn CompositeDescriptor>
where
    Seed: Clone
        + fmt::Debug
        + serde::Serialize
        + serde::de::DeserializeOwned
        + TypedPayload
        + Send
        + Sync
        + 'static,
    Item: Clone + serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
    Partial: Clone
        + serde::Serialize
        + serde::de::DeserializeOwned
        + TypedPayload
        + Send
        + Sync
        + 'static,
    Out: Clone + TypedPayload + Send + Sync + 'static,
    MapRole: AiMapRole<Item, Partial>,
    FinaliseRole: AiFinaliseRole<Seed, Many<Partial>, Out>,
{
    let (chunker, map_role, finalise_role) = roles;
    let (map_chat_binding, finalise_chat_binding) = chat_bindings;
    let (map_policies, finalise_policies) = policies;
    Box::new(GeneratedAiMapReduceCompositeDescriptor {
        name: name.into(),
        chunker,
        map_role,
        finalise_role,
        map_chat_binding,
        finalise_chat_binding,
        map_policies,
        finalise_policies,
        _types: PhantomData,
    })
}

struct GeneratedAiMapReduceCompositeDescriptor<Seed, Item, Partial, Out, MapRole, FinaliseRole> {
    name: String,
    chunker: ChunkByBudgetTyped<Seed, Item>,
    map_role: MapRole,
    finalise_role: FinaliseRole,
    map_chat_binding: ChatBindingContract,
    finalise_chat_binding: ChatBindingContract,
    map_policies: Vec<Box<dyn MiddlewareFactory>>,
    finalise_policies: Vec<Box<dyn MiddlewareFactory>>,
    _types: PhantomData<GeneratedMapReduceTypes<Seed, Item, Partial, Out>>,
}

impl<Seed, Item, Partial, Out, MapRole, FinaliseRole> CompositeDescriptor
    for GeneratedAiMapReduceCompositeDescriptor<Seed, Item, Partial, Out, MapRole, FinaliseRole>
where
    Seed: Clone
        + fmt::Debug
        + serde::Serialize
        + serde::de::DeserializeOwned
        + TypedPayload
        + Send
        + Sync
        + 'static,
    Item: Clone + serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
    Partial: Clone
        + serde::Serialize
        + serde::de::DeserializeOwned
        + TypedPayload
        + Send
        + Sync
        + 'static,
    Out: Clone + TypedPayload + Send + Sync + 'static,
    MapRole: AiMapRole<Item, Partial>,
    FinaliseRole: AiFinaliseRole<Seed, Many<Partial>, Out>,
{
    fn name(&self) -> &str {
        &self.name
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn kind(&self) -> &'static str {
        "ai_map_reduce"
    }

    fn schema_version(&self) -> u32 {
        2
    }

    fn expand(self: Box<Self>, ctx: &mut CompositeBuildContext) -> Result<(), CompositeBuildError> {
        if !self
            .map_chat_binding
            .shares_construction_origin(&self.finalise_chat_binding)
        {
            return Err(CompositeBuildError::binding_configuration(
                "chat",
                "ai_map_reduce!: map binding `map_chat` and reduce binding `reduce_chat` \
                 must be clones of one ChatBindingContract; equal target metadata does not \
                 prove one estimator/configuration decision",
            ));
        }
        require_generated_chat_resilience(
            "ai_map_reduce!",
            "role",
            "map",
            self.map_policies.iter().map(Box::as_ref),
        )
        .map_err(CompositeBuildError::new)?;
        require_generated_chat_resilience(
            "ai_map_reduce!",
            "role",
            "reduce",
            self.finalise_policies.iter().map(Box::as_ref),
        )
        .map_err(CompositeBuildError::new)?;

        let composite_id = CompositeId::new(format!("ai_map_reduce:{}", self.name));
        let direct_bound = NonZeroU64::MIN.saturating_add(2);

        let chunk_handler = generated_ai_chunk_handler(self.chunker, composite_id.clone());
        let chunk_descriptor = wrap_typed_descriptor(
            Box::new(TransformDescriptor {
                name: "chunk".to_string(),
                handler: chunk_handler,
                middleware: Vec::new(),
                backpressure: None,
            }),
            StageTypingMetadata::transform(
                TypeHint::exact_payload::<Seed>(),
                TypeHint::exact_payload::<AiMapReduceMapInput<ChunkEnvelope<Item>>>(),
                false,
                None,
            )
            .with_additional_output_contract(vec![
                TypeHint::exact_payload::<AiMapReducePlanningManifest>(),
                TypeHint::exact_payload::<AiMapReducePlanningFailed>(),
            ]),
        );

        let map_handler =
            GeneratedAiMapHandler::<Item, Partial, _>::new(self.map_role, self.map_chat_binding);
        let map_descriptor = wrap_typed_descriptor(
            Box::new(EffectfulTransformDescriptor::generated_with_pass_through::<
                AiMapReduceMapInput<ChunkEnvelope<Item>>,
                AiMapReducePlanningManifest,
            >(
                "map",
                map_handler,
                vec![EffectDeclaration::at_least_once::<ChatCompletion>()],
                vec![EffectPolicyAttachment {
                    effect_type: ChatCompletion::EFFECT_TYPE,
                    factories: self.map_policies,
                }],
                direct_bound,
            )),
            StageTypingMetadata::transform(
                TypeHint::exact_payload::<AiMapReduceMapInput<ChunkEnvelope<Item>>>(),
                TypeHint::exact_payload::<AiMapReduceTaggedPartial<Partial>>(),
                false,
                None,
            )
            .with_additional_output_contract(vec![
                TypeHint::exact_payload::<AiMapReducePlanningManifest>(),
                TypeHint::exact_payload::<AiMapReduceChunkFailed>(),
            ]),
        );

        let collector: SeededCollectByInput<Partial, Seed, Many<Partial>> =
            SeededCollectByInput::new(Many::<Partial>::default(), |acc, partial: &Partial| {
                acc.items.push(partial.clone());
            })
            .with_planning_summary(|acc, planning| {
                acc.planning = planning.clone();
            })
            .with_composite_id(composite_id);
        let collect_descriptor = wrap_typed_descriptor(
            Box::new(StatefulDescriptor {
                name: "collect".to_string(),
                handler: collector,
                emit_interval: None,
                middleware: Vec::new(),
                backpressure: None,
            }),
            StageTypingMetadata::stateful(
                TypeHint::exact_payload::<AiMapReduceTaggedPartial<Partial>>(),
                TypeHint::exact_payload::<AiMapReduceReduceInput<Seed, Many<Partial>>>(),
                false,
                None,
            )
            .with_additional_output_contract(vec![TypeHint::exact_payload::<
                AiMapReduceJobFailed,
            >()]),
        );

        let finalise_handler = GeneratedAiFinaliseHandler::<Seed, Many<Partial>, Out, _>::new(
            self.finalise_role,
            self.finalise_chat_binding,
        );
        let finalise_descriptor = wrap_typed_descriptor(
            Box::new(EffectfulTransformDescriptor::generated::<
                AiMapReduceReduceInput<Seed, Many<Partial>>,
            >(
                "finalize",
                finalise_handler,
                vec![EffectDeclaration::at_least_once::<ChatCompletion>()],
                vec![EffectPolicyAttachment {
                    effect_type: ChatCompletion::EFFECT_TYPE,
                    factories: self.finalise_policies,
                }],
                direct_bound,
            )),
            StageTypingMetadata::transform(
                TypeHint::exact_payload::<AiMapReduceReduceInput<Seed, Many<Partial>>>(),
                TypeHint::exact_payload::<Out>(),
                false,
                None,
            )
            .with_additional_output_contract(vec![TypeHint::exact_payload::<
                AiMapReduceFinaliseFailed,
            >()]),
        );

        ctx.member("chunk").descriptor(chunk_descriptor);
        ctx.member("map").descriptor(map_descriptor);
        ctx.member("collect").descriptor(collect_descriptor);
        ctx.member("finalize").descriptor(finalise_descriptor);

        ctx.feed("chunk", "map")
            .lane("data")
            .payload::<AiMapReduceMapInput<ChunkEnvelope<Item>>>()
            .payload::<AiMapReducePlanningManifest>();
        ctx.feed("map", "collect")
            .lane("data")
            .payload::<AiMapReducePlanningManifest>()
            .payload::<AiMapReduceTaggedPartial<Partial>>()
            .payload::<AiMapReduceChunkFailed>();
        ctx.feed("collect", "finalize")
            .lane("data")
            .payload::<AiMapReduceReduceInput<Seed, Many<Partial>>>();

        ctx.boundary()
            .input("in", "chunk")
            .payload::<Seed>()
            .default()
            .output("out", "finalize")
            .payload::<Out>()
            .default();
        Ok(())
    }
}
