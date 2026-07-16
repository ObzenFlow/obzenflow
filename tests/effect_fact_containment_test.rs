// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-120m: producer-side effect-fact containment is validated at build
//! time for every effectful stage, with or without an `output_middleware:`
//! lane. Before `validate_effect_fact_containment` the check was gated behind
//! that lane's registrations, so an effectful stage without the lane surfaced
//! an undeclared effect fact only at commit time, after live I/O.

use async_trait::async_trait;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload},
    id::StageId,
    TypedPayload, WriterId,
};
use obzenflow_dsl::dsl::stage_descriptor::{EffectfulTransformDescriptor, StageDescriptor};
use obzenflow_dsl::dsl::typing::{wrap_typed_descriptor, StageTypingMetadata, TypeHint};
use obzenflow_dsl::{effectful_transform, flow, sink, source, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::{
    Effect, EffectContext, EffectDeclaration, EffectError, EffectSafety, Effects,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    EffectfulTransformHandler, FiniteSourceHandler, SinkHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::path::PathBuf;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ContainmentInput {
    value: u64,
}

impl TypedPayload for ContainmentInput {
    const EVENT_TYPE: &'static str = "containment.input";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ContainmentOutput {
    value: u64,
}

impl TypedPayload for ContainmentOutput {
    const EVENT_TYPE: &'static str = "containment.output";
}

/// The effect's outcome fact. The failing flow below deliberately leaves it
/// out of the arrow contract.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct ContainmentEffectValue {
    value: u64,
}

impl TypedPayload for ContainmentEffectValue {
    const EVENT_TYPE: &'static str = "containment.effect_value";
}

#[derive(Clone, Debug)]
struct ValueEffect {
    value: u64,
}

#[async_trait]
impl Effect for ValueEffect {
    const EFFECT_TYPE: &'static str = "containment.value";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::Idempotent;

    type Outcome = ContainmentEffectValue;

    fn label(&self) -> &str {
        "containment_value"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({ "value": self.value })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        Ok(ContainmentEffectValue { value: self.value })
    }
}

#[derive(Clone, Debug)]
struct OneShotSource {
    emitted: bool,
    writer_id: WriterId,
}

impl OneShotSource {
    fn new() -> Self {
        Self {
            emitted: false,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for OneShotSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id,
            ContainmentInput::EVENT_TYPE,
            json!(ContainmentInput { value: 1 }),
        )]))
    }
}

#[derive(Clone, Debug)]
struct PerformTransform;

#[async_trait]
impl EffectfulTransformHandler for PerformTransform {
    type Input = ContainmentInput;
    type Output = obzenflow_core::stage_fact_set![ContainmentOutput, ContainmentEffectValue];
    type AllowedEffects = obzenflow_runtime::effect_set![ValueEffect];

    async fn process(
        &self,
        input: ContainmentInput,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
        let effect_value = fx
            .perform(ValueEffect { value: input.value })
            .await
            .map_err(|e| HandlerError::Other(e.to_string()))?;
        fx.emit(ContainmentOutput {
            value: effect_value.value,
        })
        .await
        .map_err(|e| HandlerError::Other(e.to_string()))?;
        Ok(fx.complete()?)
    }

    fn stage_logic_version(&self) -> &str {
        "containment-v1"
    }
}

#[derive(Clone, Debug)]
struct EmitOnlyTransform;

#[async_trait]
impl EffectfulTransformHandler for EmitOnlyTransform {
    type Input = ContainmentInput;
    type Output = ContainmentOutput;
    type AllowedEffects = obzenflow_runtime::effect_set![];

    async fn process(
        &self,
        input: ContainmentInput,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
        fx.emit(ContainmentOutput { value: input.value })
            .await
            .map_err(|e| HandlerError::Other(e.to_string()))?;
        Ok(fx.complete()?)
    }

    fn stage_logic_version(&self) -> &str {
        "containment-emit-only-v1"
    }
}

#[derive(Clone, Debug)]
struct DropSink;

#[async_trait]
impl SinkHandler for DropSink {
    async fn consume(&mut self, _event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        Ok(DeliveryPayload::success(
            DeliveryMethod::Custom("Memory".to_string()),
            None,
        ))
    }
}

fn erased_effectful_descriptor<H>(
    name: &str,
    handler: H,
    effects: Vec<EffectDeclaration>,
    output_type: TypeHint,
    output_contract: Vec<TypeHint>,
) -> Box<dyn StageDescriptor>
where
    H: EffectfulTransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
{
    let descriptor: Box<dyn StageDescriptor> = Box::new(EffectfulTransformDescriptor {
        name: name.to_string(),
        handler,
        effects,
        middleware: Vec::new(),
        effect_policies: Vec::new(),
        synthesized_outcomes: Vec::new(),
        type_shaping_errors: Vec::new(),
        backpressure: None,
    });
    let metadata = StageTypingMetadata::transform(
        TypeHint::exact_payload::<ContainmentInput>(),
        output_type,
        false,
        None,
    )
    .with_output_contract(output_contract);
    wrap_typed_descriptor(descriptor, metadata)
}

fn missing_effect_fact_descriptor() -> Box<dyn StageDescriptor> {
    erased_effectful_descriptor(
        "effectful",
        PerformTransform,
        vec![EffectDeclaration::of::<ValueEffect>()],
        TypeHint::exact_payload::<ContainmentOutput>(),
        vec![TypeHint::exact_payload::<ContainmentOutput>()],
    )
}

/// The effect's `ContainmentEffectValue` outcome fact is missing from the
/// arrow, and no `output_middleware:` lane is configured.
fn undeclared_effect_fact_flow(journal_base: PathBuf) -> FlowDefinition {
    flow! {
        name: "effect_fact_containment_missing",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            inputs = source!(ContainmentInput => OneShotSource::new());
            effectful = missing_effect_fact_descriptor();
            drops = sink!(ContainmentOutput => DropSink);
        },

        topology: {
            inputs |> effectful;
            effectful |> drops;
        }
    }
}

/// `effects: []` stages have no declarations, so containment has nothing to
/// check and the build proceeds.
fn empty_effects_flow(journal_base: PathBuf) -> FlowDefinition {
    flow! {
        name: "effect_fact_containment_empty_effects",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            inputs = source!(ContainmentInput => OneShotSource::new());
            effectful = effectful_transform!(
                ContainmentInput -> { ContainmentOutput } => EmitOnlyTransform,
                effects: [],
                middleware: []);
            drops = sink!(ContainmentOutput => DropSink);
        },

        topology: {
            inputs |> effectful;
            effectful |> drops;
        }
    }
}

#[tokio::test]
async fn undeclared_effect_fact_fails_build_without_output_middleware_lane() {
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let err = FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(undeclared_effect_fact_flow(journal_base))
        .await
        .expect_err("an undeclared effect fact must fail the build before any I/O");

    let message = err.to_string();
    assert!(
        message.contains("unconditional producer-side containment"),
        "expected the FLOWIP-120m containment rejection, got: {message}"
    );
    assert!(
        message.contains("ContainmentEffectValue"),
        "the rejection must name the undeclared fact, got: {message}"
    );
}

/// Two distinct member types colliding on `EVENT_TYPE`: the derive's const
/// member guard rejects this shape at compile time (FLOWIP-120z), so the
/// carrier is hand-written to model an erased or buggy marshalling path,
/// and this remains the flow build's half of the FLOWIP-120m ambiguity
/// rejection.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct ColliderA {
    value: u64,
}

impl TypedPayload for ColliderA {
    const EVENT_TYPE: &'static str = "containment.collide";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ColliderB {
    value: u64,
}

impl TypedPayload for ColliderB {
    const EVENT_TYPE: &'static str = "containment.collide";
}

#[derive(Clone, Debug)]
struct CollidingOutcome {
    primary: ColliderA,
}

impl obzenflow_core::TypedFactSet for CollidingOutcome {
    fn fact_types() -> Vec<obzenflow_core::TypedFactType> {
        // The buggy shape under test: the value-level metadata declares two
        // distinct member types that collide on `EVENT_TYPE`.
        vec![
            obzenflow_core::TypedFactType::of::<ColliderA>(),
            obzenflow_core::TypedFactType::of::<ColliderB>(),
        ]
    }

    fn into_facts(
        self,
    ) -> Result<Vec<obzenflow_core::TypedFact>, obzenflow_core::TypedFactSetError> {
        Ok(vec![obzenflow_core::TypedFact::from_payload(self.primary)?])
    }

    fn try_from_facts(
        _facts: &[obzenflow_core::TypedFact],
    ) -> Result<Self, obzenflow_core::TypedFactSetError> {
        // Reconstruction is ambiguous by construction; the flow-build
        // rejection this fixture exercises exists so this is never reached.
        Err(obzenflow_core::TypedFactSetError::SerializationFailed(
            "colliding carrier reconstruction is ambiguous by construction".to_string(),
        ))
    }
}

impl obzenflow_core::StageFactSet for CollidingOutcome {
    type Members = obzenflow_core::event::schema::WithMember<
        ColliderA,
        obzenflow_core::event::schema::EmptySet,
    >;

    fn member_fact_types() -> Vec<obzenflow_core::TypedFactType> {
        <Self as obzenflow_core::TypedFactSet>::fact_types()
    }
}

#[derive(Clone, Debug)]
struct CollidingEffect;

#[async_trait]
impl Effect for CollidingEffect {
    const EFFECT_TYPE: &'static str = "containment.colliding";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::Idempotent;

    type Outcome = CollidingOutcome;

    fn label(&self) -> &str {
        "colliding"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({})
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        Ok(CollidingOutcome {
            primary: ColliderA { value: 1 },
        })
    }
}

#[derive(Clone, Debug)]
struct CollidingTransform;

#[async_trait]
impl EffectfulTransformHandler for CollidingTransform {
    type Input = ContainmentInput;
    type Output = ColliderA;
    type AllowedEffects = obzenflow_runtime::effect_set![CollidingEffect];

    async fn process(
        &self,
        _input: ContainmentInput,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
        let _ = fx
            .perform(CollidingEffect)
            .await
            .map_err(|e| HandlerError::Other(e.to_string()))?;
        Ok(fx.complete()?)
    }

    fn stage_logic_version(&self) -> &str {
        "containment-collide-v1"
    }
}

fn colliding_effect_descriptor() -> Box<dyn StageDescriptor> {
    erased_effectful_descriptor(
        "effectful",
        CollidingTransform,
        vec![EffectDeclaration::of::<CollidingEffect>()],
        TypeHint::exact_payload::<ColliderA>(),
        vec![
            TypeHint::exact_payload::<ColliderA>(),
            TypeHint::exact_payload::<ColliderB>(),
        ],
    )
}

fn colliding_event_type_flow(journal_base: PathBuf) -> FlowDefinition {
    flow! {
        name: "effect_fact_containment_collision",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            inputs = source!(ContainmentInput => OneShotSource::new());
            effectful = colliding_effect_descriptor();
            drops = sink!(ColliderA => DropSink);
        },

        topology: {
            inputs |> effectful;
            effectful |> drops;
        }
    }
}

#[tokio::test]
async fn duplicate_event_type_across_carrier_members_is_rejected_at_build() {
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let err = FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(colliding_event_type_flow(journal_base))
        .await
        .expect_err("colliding member event types must fail the build");

    let message = err.to_string();
    assert!(
        message.contains("distinct event types"),
        "expected the FLOWIP-120m event-type collision rejection, got: {message}"
    );
}

#[tokio::test]
async fn stages_with_empty_effects_skip_containment() {
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(empty_effects_flow(journal_base))
        .await
        .expect("a stage with no effect declarations must build and run");
}
