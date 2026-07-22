// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-010 §6a: the run manifest records the redacted effective config
//! with both provenance axes, so "what configuration was this run executed
//! under" is answerable from the run directory alone.

use obzenflow_adapters::middleware::{rate_limit, CircuitBreaker, EffectResilience};
use obzenflow_core::config::{ConfigSubject, ResolvedForDoc};
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::journal::run_manifest::RunManifest;
use obzenflow_core::{TypedPayload, WriterId};
use obzenflow_dsl::{effectful_transform, flow, sink, source};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::{Effect, EffectContext, EffectError, EffectSafety, Effects};
use obzenflow_runtime::journal::RunSubstrateState;
use obzenflow_runtime::run_context::FlowBuildContext;
use obzenflow_runtime::runtime_config::{
    CandidateSet, ConfigValue, ResolvedRuntimeConfig, ScopedCandidate,
    RATE_LIMITER_BURST_CAPACITY_KEY, RESILIENCE_BREAKER_MINIMUM_CALLS_KEY,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    EffectfulTransformHandler, FiniteSourceHandler, SinkHandler,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Item {
    index: u64,
}

impl TypedPayload for Item {
    const EVENT_TYPE: &'static str = "effective_config_manifest.item";
}

#[derive(Clone, Debug)]
struct OneShotSource {
    emitted: bool,
    writer_id: WriterId,
}

impl FiniteSourceHandler for OneShotSource {
    fn next(
        &mut self,
    ) -> Result<
        Option<Vec<ChainEvent>>,
        obzenflow_runtime::stages::common::handlers::source::traits::SourceError,
    > {
        if self.emitted {
            Ok(None)
        } else {
            self.emitted = true;
            Ok(Some(vec![ChainEventFactory::data_event(
                self.writer_id,
                <Item as TypedPayload>::EVENT_TYPE,
                json!({ "index": 0 }),
            )]))
        }
    }
}

#[derive(Clone, Debug)]
struct NullSink;

#[async_trait]
impl SinkHandler for NullSink {
    async fn consume(&mut self, _event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        Ok(DeliveryPayload::success(
            DeliveryMethod::Custom("Null".to_string()),
            None,
        ))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PaymentEffectFact {
    kind: String,
}

impl TypedPayload for PaymentEffectFact {
    const EVENT_TYPE: &'static str = "effective_config_manifest.payment_effect_fact";
}

#[derive(Clone, Debug)]
struct AuthorizePayment;

#[async_trait]
impl Effect for AuthorizePayment {
    const EFFECT_TYPE: &'static str = "payments.authorize";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::Idempotent;

    type Outcome = PaymentEffectFact;

    fn label(&self) -> &str {
        "authorize-payment"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({})
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        Ok(PaymentEffectFact {
            kind: "authorize".to_string(),
        })
    }
}

#[derive(Clone, Debug)]
struct RefundPayment;

#[async_trait]
impl Effect for RefundPayment {
    const EFFECT_TYPE: &'static str = "payments.refund";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::Idempotent;

    type Outcome = PaymentEffectFact;

    fn label(&self) -> &str {
        "refund-payment"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({})
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        Ok(PaymentEffectFact {
            kind: "refund".to_string(),
        })
    }
}

#[derive(Clone, Debug)]
struct PaymentEffectsHandler;

#[async_trait]
impl EffectfulTransformHandler for PaymentEffectsHandler {
    type Input = Item;
    type Output = obzenflow_core::stage_fact_set![PaymentEffectFact];
    type AllowedEffects = obzenflow_runtime::effect_set![AuthorizePayment, RefundPayment];

    async fn process(
        &self,
        _input: Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
        Ok(fx.complete_empty()?)
    }
}

fn manifest_for(handle: &obzenflow_runtime::prelude::FlowHandle) -> RunManifest {
    let locator = match handle.run_substrate() {
        RunSubstrateState::Durable(locator) => locator.clone(),
        RunSubstrateState::Ephemeral => panic!("disk flow must report Durable"),
    };
    let raw = std::fs::read_to_string(locator.path().join("run_manifest.json"))
        .expect("run_manifest.json should be readable");
    serde_json::from_str(&raw).expect("run_manifest.json should parse")
}

fn build_flow_future(
    base: std::path::PathBuf,
    ctx: FlowBuildContext,
) -> impl std::future::Future<
    Output = Result<obzenflow_runtime::prelude::FlowHandle, obzenflow_dsl::dsl::FlowBuildFailure>,
> {
    flow! {
        name: "effective_config_manifest",
        journals: disk_journals(base),
        middleware: [],

        stages: {
            src = source!(Item => OneShotSource { emitted: false, writer_id: WriterId::from(StageId::new()) });
            snk = sink!(Item => NullSink);
        },

        topology: {
            src |> snk;
        }
    }
    .build(ctx)
}

fn build_rate_limited_flow_future(
    base: std::path::PathBuf,
    ctx: FlowBuildContext,
) -> impl std::future::Future<
    Output = Result<obzenflow_runtime::prelude::FlowHandle, obzenflow_dsl::dsl::FlowBuildFailure>,
> {
    let limiter = rate_limit(10.0);
    flow! {
        name: "effective_config_manifest_with_optional_limiter_burst",
        journals: disk_journals(base),
        middleware: [],

        stages: {
            src = source!(Item => OneShotSource { emitted: false, writer_id: WriterId::from(StageId::new()) });
            snk = sink!(Item => NullSink, middleware: [limiter]);
        },

        topology: {
            src |> snk;
        }
    }
    .build(ctx)
}

fn payment_resilience() -> Box<dyn obzenflow_adapters::middleware::MiddlewareFactory> {
    EffectResilience::with_breaker(
        CircuitBreaker::builder()
            .count_window(10)
            .minimum_calls(5)
            .failure_rate_threshold(0.5)
            .build()
            .expect("manifest proof breaker"),
    )
    .build()
    .expect("manifest proof resilience aggregate")
}

fn build_two_effect_flow_future(
    base: std::path::PathBuf,
    ctx: FlowBuildContext,
) -> impl std::future::Future<
    Output = Result<obzenflow_runtime::prelude::FlowHandle, obzenflow_dsl::dsl::FlowBuildFailure>,
> {
    let authorize_resilience = payment_resilience();
    let refund_resilience = payment_resilience();
    flow! {
        name: "effective_config_manifest_two_effects",
        journals: disk_journals(base),
        middleware: [],

        stages: {
            orders = source!(Item => OneShotSource { emitted: false, writer_id: WriterId::from(StageId::new()) });
            authorize_payment = effectful_transform!(
                Item -> PaymentEffectFact => PaymentEffectsHandler,
                effects: [
                    AuthorizePayment with [authorize_resilience],
                    RefundPayment with [refund_resilience]
                ],
                middleware: []
            );
            output = sink!(PaymentEffectFact => NullSink);
        },

        topology: {
            orders |> authorize_payment;
            authorize_payment |> output;
        }
    }
    .build(ctx)
}

#[tokio::test]
async fn manifest_records_file_sourced_values_with_both_provenance_axes() {
    let dir = tempfile::tempdir().expect("tempdir");

    // A file-sourced global value plus a stage-scoped override for `snk`.
    let mut candidates = CandidateSet::default();
    candidates
        .admit(ScopedCandidate::unqualified(
            "runtime.max_lineage_depth",
            obzenflow_core::config::ConfigScope::Global,
            obzenflow_core::config::ConfigSource::File,
            ConfigValue::U64(7),
        ))
        .expect("global candidate admits");
    candidates
        .admit(ScopedCandidate::unqualified(
            "runtime.max_lineage_depth",
            obzenflow_core::config::ConfigScope::stage("snk"),
            obzenflow_core::config::ConfigSource::File,
            ConfigValue::U64(5),
        ))
        .expect("stage candidate admits");
    let snapshot = Arc::new(ResolvedRuntimeConfig::new(candidates));

    let handle = build_flow_future(dir.path().to_path_buf(), FlowBuildContext::new(snapshot))
        .await
        .expect("flow must build");

    let manifest = manifest_for(&handle);
    let evidence = manifest
        .effective_config
        .expect("manifest must record effective config evidence");
    assert_eq!(evidence.schema_version, 2);

    let lineage: Vec<_> = evidence
        .values
        .iter()
        .filter(|d| d.key_path == "runtime.max_lineage_depth")
        .collect();
    assert_eq!(lineage.len(), 2, "global value plus the stage override");
    assert_eq!(lineage[0].scope, "global");
    assert_eq!(lineage[0].source, "file");
    assert_eq!(lineage[0].value, json!(7));
    assert_eq!(lineage[1].scope, "stage:snk");
    assert_eq!(lineage[1].source, "file");
    assert_eq!(lineage[1].value, json!(5));

    // Defaults for untouched knobs are recorded too, with default provenance.
    let heartbeat = evidence
        .values
        .iter()
        .find(|d| d.key_path == "runtime.heartbeat_interval")
        .expect("defaulted knobs appear in the evidence");
    assert_eq!(heartbeat.source, "default");
    assert_eq!(heartbeat.scope, "global");
}

#[tokio::test]
async fn manifest_records_default_provenance_for_a_hostless_build() {
    let dir = tempfile::tempdir().expect("tempdir");
    let handle = build_flow_future(dir.path().to_path_buf(), FlowBuildContext::for_tests())
        .await
        .expect("flow must build");

    let manifest = manifest_for(&handle);
    let evidence = manifest
        .effective_config
        .expect("manifest must record effective config evidence");
    let lineage = evidence
        .values
        .iter()
        .find(|d| d.key_path == "runtime.max_lineage_depth")
        .expect("lineage depth is a defaulted knob");
    assert_eq!(lineage.value, json!(100));
    assert_eq!(lineage.source, "default");
    assert_eq!(lineage.scope, "global");
    assert_eq!(
        evidence
            .values
            .iter()
            .filter(|d| d.key_path == "runtime.max_lineage_depth")
            .count(),
        1,
        "identical per-stage resolutions collapse to one doc"
    );
}

#[tokio::test]
async fn manifest_records_a_file_supplied_optional_key_for_a_surviving_factory() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut candidates = CandidateSet::default();
    candidates
        .admit(ScopedCandidate::unqualified(
            RATE_LIMITER_BURST_CAPACITY_KEY,
            obzenflow_core::config::ConfigScope::stage("snk"),
            obzenflow_core::config::ConfigSource::File,
            ConfigValue::F64(3.0),
        ))
        .expect("optional burst candidate admits");

    let snapshot = Arc::new(ResolvedRuntimeConfig::new(candidates));
    let handle =
        build_rate_limited_flow_future(dir.path().to_path_buf(), FlowBuildContext::new(snapshot))
            .await
            .expect("the surviving limiter must consume its optional burst key");

    let manifest = manifest_for(&handle);
    let evidence = manifest
        .effective_config
        .expect("manifest must record effective config evidence");
    let burst = evidence
        .values
        .iter()
        .find(|doc| doc.key_path == RATE_LIMITER_BURST_CAPACITY_KEY)
        .expect("the supplied optional value must appear in effective-config evidence");

    assert_eq!(burst.scope, "stage:snk");
    assert_eq!(burst.source, "file");
    assert_eq!(burst.value, json!(3.0));
    assert!(
        burst.resolved_for.is_none(),
        "a stage middleware value resolves for the stage itself"
    );
}

#[tokio::test]
async fn manifest_retains_two_real_effect_rows_for_one_stage_broadcast() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut candidates = CandidateSet::default();
    candidates
        .admit(ScopedCandidate::unqualified(
            RESILIENCE_BREAKER_MINIMUM_CALLS_KEY,
            obzenflow_core::config::ConfigScope::stage("authorize_payment"),
            obzenflow_core::config::ConfigSource::File,
            ConfigValue::U64(8),
        ))
        .expect("stage broadcast candidate admits");

    let snapshot = Arc::new(ResolvedRuntimeConfig::new(candidates));
    let handle =
        build_two_effect_flow_future(dir.path().to_path_buf(), FlowBuildContext::new(snapshot))
            .await
            .expect("both structurally declared effects must consume the stage broadcast");

    let manifest = manifest_for(&handle);
    let evidence = manifest
        .effective_config
        .expect("manifest must record effective config evidence");
    assert_eq!(evidence.schema_version, 2);
    let rows: Vec<_> = evidence
        .values
        .iter()
        .filter(|row| row.key_path == RESILIENCE_BREAKER_MINIMUM_CALLS_KEY)
        .collect();
    assert_eq!(rows.len(), 2, "equal inherited values must not collapse");
    assert!(rows.iter().all(|row| {
        row.scope == "stage:authorize_payment"
            && row.source == "file"
            && row.winner_subject == Some(ConfigSubject::Unqualified)
            && row.value == json!(8)
    }));
    assert_eq!(
        rows.iter()
            .map(|row| row.resolved_for.clone())
            .collect::<Vec<_>>(),
        vec![
            Some(ResolvedForDoc::Effect {
                stage: "authorize_payment".to_string(),
                effect_type: AuthorizePayment::EFFECT_TYPE.to_string(),
            }),
            Some(ResolvedForDoc::Effect {
                stage: "authorize_payment".to_string(),
                effect_type: RefundPayment::EFFECT_TYPE.to_string(),
            }),
        ]
    );
}

#[test]
fn checked_schema_v2_studio_fixture_is_a_complete_run_manifest() {
    let fixture = include_str!("fixtures/effective_config_v2_manifest.json");
    let manifest: RunManifest =
        serde_json::from_str(fixture).expect("Studio fixture must be a valid RunManifest");
    assert_eq!(manifest.manifest_version, "2.0");
    assert_eq!(manifest.journal_format_version, 2);
    let evidence = manifest
        .effective_config
        .expect("Studio fixture must carry effective-config evidence");
    assert_eq!(evidence.schema_version, 2);
    assert_eq!(evidence.values.len(), 2);
    assert_ne!(
        evidence.values[0].resolved_for,
        evidence.values[1].resolved_for
    );
}
