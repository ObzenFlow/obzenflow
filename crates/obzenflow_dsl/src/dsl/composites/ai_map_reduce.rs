// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Generated AI map-reduce protocol descriptor.
//!
//! This module deliberately exposes no programmatic builder. The
//! `ai_map_reduce!` macro is the only public authoring surface and expands to
//! the fixed four-stage FLOWIP-128g protocol.

use crate::dsl::composition::{CompositeBuildContext, CompositeBuildError, CompositeDescriptor};
use crate::dsl::stage_descriptor::{
    EffectPolicyAttachment, EffectfulTransformDescriptor, StatefulDescriptor, TransformDescriptor,
};
use crate::dsl::typing::{wrap_typed_descriptor, StageTypingMetadata, TypeHint};
use obzenflow_adapters::ai::effects::{
    ChatCompletion, GeneratedAiFinaliseHandler, GeneratedAiMapHandler,
};
use obzenflow_adapters::ai::GeneratedAiChunkHandler;
use obzenflow_adapters::middleware::MiddlewareFactory;
use obzenflow_core::ai::{
    AiFinaliseRole, AiMapReduceChunkFailed, AiMapReduceFinaliseFailed, AiMapReduceJobFailed,
    AiMapReducePlanningFailed, AiMapReducePlanningManifest, AiMapReduceReduceInput,
    AiMapReduceTaggedPartial, AiMapRole, ChatCompletionCompleted, ChatTarget, ChunkEnvelope, Many,
    ResolvedTokenEstimator,
};
use obzenflow_core::id::CompositeId;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::effects::{Effect, EffectDeclaration};
use obzenflow_runtime::stages::common::handlers::TransformHandler;
use obzenflow_runtime::stages::resources_builder::DirectFactPlan;
use obzenflow_runtime::stages::stateful::{CollectByInput, SeededCollectByInput};
use std::fmt;
use std::marker::PhantomData;
use std::num::NonZeroU64;

/// Macro-only constructor for the FLOWIP-128g generated protocol.
///
/// The concrete role adapters, effect declarations, collector, and
/// direct-fact bounds are fixed as one generated call graph.
#[doc(hidden)]
#[allow(clippy::too_many_arguments)]
pub fn generated_map_reduce<Seed, Item, Partial, Out, Chunker, MapRole, FinaliseRole>(
    name: impl Into<String>,
    chunker: Chunker,
    map_role: MapRole,
    finalise_role: FinaliseRole,
    chat_target: ChatTarget,
    chat_estimator: ResolvedTokenEstimator,
    map_policies: Vec<Box<dyn MiddlewareFactory>>,
    finalise_policies: Vec<Box<dyn MiddlewareFactory>>,
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
    Chunker: TransformHandler + Clone + fmt::Debug + Send + Sync + 'static,
    MapRole: AiMapRole<Item, Partial>,
    FinaliseRole: AiFinaliseRole<Seed, Many<Partial>, Out>,
{
    Box::new(GeneratedAiMapReduceCompositeDescriptor {
        name: name.into(),
        chunker,
        map_role,
        finalise_role,
        chat_target,
        chat_estimator,
        map_policies,
        finalise_policies,
        _types: PhantomData,
    })
}

struct GeneratedAiMapReduceCompositeDescriptor<
    Seed,
    Item,
    Partial,
    Out,
    Chunker,
    MapRole,
    FinaliseRole,
> {
    name: String,
    chunker: Chunker,
    map_role: MapRole,
    finalise_role: FinaliseRole,
    chat_target: ChatTarget,
    chat_estimator: ResolvedTokenEstimator,
    map_policies: Vec<Box<dyn MiddlewareFactory>>,
    finalise_policies: Vec<Box<dyn MiddlewareFactory>>,
    _types: PhantomData<fn() -> (Seed, Item, Partial, Out)>,
}

impl<Seed, Item, Partial, Out, Chunker, MapRole, FinaliseRole> CompositeDescriptor
    for GeneratedAiMapReduceCompositeDescriptor<
        Seed,
        Item,
        Partial,
        Out,
        Chunker,
        MapRole,
        FinaliseRole,
    >
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
    Chunker: TransformHandler + Clone + fmt::Debug + Send + Sync + 'static,
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
        if self.chat_estimator.info().model != self.chat_target.model {
            return Err(CompositeBuildError::new(format!(
                "ai_map_reduce!: `effects.chat_estimator` model '{}' does not match \
                 `effects.chat_target` model '{}'",
                self.chat_estimator.info().model,
                self.chat_target.model
            )));
        }

        let composite_id = CompositeId::new(format!("ai_map_reduce:{}", self.name));
        let direct_bound = NonZeroU64::new(3).expect("generated direct bound is non-zero");

        let chunk_handler = GeneratedAiChunkHandler::<ChunkEnvelope<Item>, _>::new(
            self.chunker,
            composite_id.clone(),
        );
        let chunk_descriptor = wrap_typed_descriptor(
            Box::new(TransformDescriptor {
                name: "chunk".to_string(),
                handler: chunk_handler,
                middleware: Vec::new(),
                backpressure: None,
            }),
            StageTypingMetadata::transform(
                TypeHint::exact_payload::<Seed>(),
                TypeHint::exact_payload::<ChunkEnvelope<Item>>(),
                false,
                None,
            )
            .with_additional_output_contract(vec![
                TypeHint::exact_payload::<AiMapReducePlanningManifest>(),
                TypeHint::exact_payload::<AiMapReducePlanningFailed>(),
            ]),
        );

        let map_handler = GeneratedAiMapHandler::<Item, Partial, _>::new(
            self.map_role,
            self.chat_target.clone(),
            self.chat_estimator.clone(),
            composite_id.clone(),
        );
        let map_descriptor = wrap_typed_descriptor(
            Box::new(EffectfulTransformDescriptor {
                name: "map".to_string(),
                handler: map_handler,
                effects: vec![EffectDeclaration::at_least_once::<ChatCompletion>()],
                middleware: Vec::new(),
                effect_policies: vec![EffectPolicyAttachment {
                    effect_type: ChatCompletion::EFFECT_TYPE,
                    factories: self.map_policies,
                }],
                direct_fact_plan: DirectFactPlan::generated::<ChunkEnvelope<Item>>(direct_bound),
                backpressure: None,
            }),
            StageTypingMetadata::transform(
                TypeHint::exact_payload::<ChunkEnvelope<Item>>(),
                TypeHint::exact_payload::<ChatCompletionCompleted>(),
                false,
                None,
            )
            .with_additional_output_contract(vec![
                TypeHint::exact_payload::<AiMapReducePlanningManifest>(),
                TypeHint::exact_payload::<AiMapReduceTaggedPartial<Partial>>(),
                TypeHint::exact_payload::<AiMapReduceChunkFailed>(),
            ]),
        );

        let collector: SeededCollectByInput<Partial, Seed, Many<Partial>> =
            CollectByInput::new(Many::<Partial>::default(), |acc, partial: &Partial| {
                acc.items.push(partial.clone());
            })
            .with_planning_summary(|acc, planning| {
                acc.planning = planning.clone();
            })
            .with_seed::<Seed>()
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
            self.chat_target,
            self.chat_estimator,
        );
        let finalise_descriptor = wrap_typed_descriptor(
            Box::new(EffectfulTransformDescriptor {
                name: "finalize".to_string(),
                handler: finalise_handler,
                effects: vec![EffectDeclaration::at_least_once::<ChatCompletion>()],
                middleware: Vec::new(),
                effect_policies: vec![EffectPolicyAttachment {
                    effect_type: ChatCompletion::EFFECT_TYPE,
                    factories: self.finalise_policies,
                }],
                direct_fact_plan: DirectFactPlan::generated::<
                    AiMapReduceReduceInput<Seed, Many<Partial>>,
                >(direct_bound),
                backpressure: None,
            }),
            StageTypingMetadata::transform(
                TypeHint::exact_payload::<AiMapReduceReduceInput<Seed, Many<Partial>>>(),
                TypeHint::exact_payload::<Out>(),
                false,
                None,
            )
            .with_additional_output_contract(vec![
                TypeHint::exact_payload::<ChatCompletionCompleted>(),
                TypeHint::exact_payload::<AiMapReduceFinaliseFailed>(),
            ]),
        );

        ctx.member("chunk").descriptor(chunk_descriptor);
        ctx.member("map").descriptor(map_descriptor);
        ctx.member("collect").descriptor(collect_descriptor);
        ctx.member("finalize").descriptor(finalise_descriptor);

        ctx.feed("chunk", "map")
            .lane("data")
            .payload::<ChunkEnvelope<Item>>()
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
