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
use obzenflow_dsl::{effectful_transform, flow, sink, source, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::{Effect, EffectContext, EffectError, EffectSafety, Effects};
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

    async fn process(&self, input: ContainmentInput, fx: &mut Effects) -> Result<(), HandlerError> {
        let effect_value = fx
            .perform(ValueEffect { value: input.value })
            .await
            .map_err(|e| HandlerError::Other(e.to_string()))?;
        fx.emit(ContainmentOutput {
            value: effect_value.value,
        })
        .await
        .map_err(|e| HandlerError::Other(e.to_string()))?;
        Ok(())
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

    async fn process(&self, input: ContainmentInput, fx: &mut Effects) -> Result<(), HandlerError> {
        fx.emit(ContainmentOutput { value: input.value })
            .await
            .map_err(|e| HandlerError::Other(e.to_string()))?;
        Ok(())
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

/// The effect's `ContainmentEffectValue` outcome fact is missing from the
/// arrow, and no `output_middleware:` lane is configured.
fn undeclared_effect_fact_flow(journal_base: PathBuf) -> FlowDefinition {
    flow! {
        name: "effect_fact_containment_missing",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            inputs = source!(ContainmentInput => OneShotSource::new());
            effectful = effectful_transform!(
                ContainmentInput -> { ContainmentOutput } => PerformTransform,
                effects: [ValueEffect],
                middleware: []);
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

/// Two distinct member types colliding on `EVENT_TYPE`: a macro compares
/// tokens, not `EVENT_TYPE` values, so this is the flow build's half of the
/// FLOWIP-120m ambiguity rejection.
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

#[derive(Clone, Debug, obzenflow_core::EffectOutcomeFacts)]
enum CollidingOutcome {
    A(ColliderA),
    B(ColliderB),
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
        Ok(CollidingOutcome::A(ColliderA { value: 1 }))
    }
}

#[derive(Clone, Debug)]
struct CollidingTransform;

#[async_trait]
impl EffectfulTransformHandler for CollidingTransform {
    type Input = ContainmentInput;

    async fn process(
        &self,
        _input: ContainmentInput,
        fx: &mut Effects,
    ) -> Result<(), HandlerError> {
        let _ = fx
            .perform(CollidingEffect)
            .await
            .map_err(|e| HandlerError::Other(e.to_string()))?;
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "containment-collide-v1"
    }
}

fn colliding_event_type_flow(journal_base: PathBuf) -> FlowDefinition {
    flow! {
        name: "effect_fact_containment_collision",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            inputs = source!(ContainmentInput => OneShotSource::new());
            effectful = effectful_transform!(
                ContainmentInput -> { ColliderA, ColliderB } => CollidingTransform,
                effects: [CollidingEffect],
                middleware: []);
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
