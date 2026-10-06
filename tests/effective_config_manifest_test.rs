// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-010 §6a: the run manifest records the redacted effective config
//! with both provenance axes, so "what configuration was this run executed
//! under" is answerable from the run directory alone.

use obzenflow_adapters::middleware::{circuit_breaker, rate_limit};
use obzenflow_core::config::{ConfigAddress, ConfigSource, ConfigSubject, ResolvedForDoc};
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::journal::archive::manifest::RunManifest;
use obzenflow_core::journal::factory::RunSubstrateState;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{effectful_transform, flow, sink, source, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::{Effect, EffectContext, EffectError, EffectSafety, Effects};
use obzenflow_runtime::run_context::FlowBuildContext;
use obzenflow_runtime::runtime_config::{
    CandidateSet, ConfigValue, ResolvedRuntimeConfig, ScopedCandidate,
    CIRCUIT_BREAKER_CONSECUTIVE_FAILURES_KEY, CIRCUIT_BREAKER_MINIMUM_CALLS_KEY,
    CIRCUIT_BREAKER_MODE_KEY, RATE_LIMITER_BURST_CAPACITY_KEY,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    EffectfulTransformHandler, InlineSink, SinkDescription, SinkWriteFailure,
    TypedFiniteSourceHandler,
};
use obzenflow_topology::{
    CircuitBreakerInfo, CircuitBreakerMode, MiddlewareAttachmentInfo, MiddlewareAuthoredSite,
    MiddlewareDetailsInfo, MiddlewareFamily, MiddlewareInfo, MiddlewareOperation,
    ResolvedSettingInfo, SettingProvenanceInfo, SettingSubject,
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
}

impl TypedFiniteSourceHandler for OneShotSource {
    type Output = Item;

    fn next(
        &mut self,
    ) -> Result<
        Option<Vec<Self::Output>>,
        obzenflow_runtime::stages::common::handlers::source::traits::SourceError,
    > {
        if self.emitted {
            Ok(None)
        } else {
            self.emitted = true;
            Ok(Some(vec![Item { index: 0 }]))
        }
    }
}

#[derive(Debug)]
struct NullSink<T>(std::marker::PhantomData<fn() -> T>);

impl<T> Clone for NullSink<T> {
    fn clone(&self) -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<T> NullSink<T> {
    fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

#[async_trait]
impl<T> InlineSink for NullSink<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    type Input = T;

    fn describe(&self) -> SinkDescription {
        SinkDescription::method(DeliveryMethod::Custom("Null".to_string()))
    }

    async fn write(&mut self, _event: T) -> Result<(), SinkWriteFailure> {
        Ok(())
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
    type BindingMode = obzenflow_runtime::effects::Portless;

    type Outcome = PaymentEffectFact;
    type OutcomeSemantics = obzenflow_runtime::effects::DomainFacts;

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
    type BindingMode = obzenflow_runtime::effects::Portless;

    type Outcome = PaymentEffectFact;
    type OutcomeSemantics = obzenflow_runtime::effects::DomainFacts;

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

fn round_trip_middleware(
    handle: &obzenflow_runtime::prelude::FlowHandle,
    stage_name: &str,
) -> MiddlewareInfo {
    let topology = handle.topology().expect("built flow must expose topology");
    let original = topology
        .stages()
        .find(|stage| stage.name == stage_name)
        .and_then(|stage| stage.middleware.as_ref())
        .expect("the authored stage must retain its middleware");
    let wire = serde_json::to_vec(original).expect("producer information must serialise");
    let decoded: MiddlewareInfo =
        serde_json::from_slice(&wire).expect("producer information must pass checked decoding");
    assert_eq!(
        &decoded, original,
        "every binding key, label, site, operation, setting and provenance field must survive"
    );
    decoded
}

fn assert_setting<T: std::fmt::Debug + PartialEq>(
    actual: &ResolvedSettingInfo<T>,
    value: T,
    source: &str,
    scope: &str,
    winner_subject: SettingSubject,
) {
    assert_eq!(
        actual,
        &ResolvedSettingInfo {
            value,
            provenance: SettingProvenanceInfo {
                source: source.to_string(),
                scope: scope.to_string(),
                winner_subject,
            },
        }
    );
}

fn assert_effect_breaker<'a>(
    attachment: &'a MiddlewareAttachmentInfo,
    effect_type: &str,
) -> &'a CircuitBreakerInfo {
    assert_eq!(attachment.label, "circuit_breaker");
    assert_eq!(attachment.family(), MiddlewareFamily::CircuitBreaker);
    assert_eq!(
        attachment.authored_site,
        MiddlewareAuthoredSite::Effect {
            effect_type: effect_type.to_string(),
        }
    );
    assert_eq!(
        attachment.operation,
        MiddlewareOperation::Effect {
            effect_type: effect_type.to_string(),
        }
    );
    let MiddlewareDetailsInfo::CircuitBreaker(info) = &attachment.details else {
        panic!("the effect must carry typed breaker information");
    };
    let subject = SettingSubject::Effect {
        effect_type: effect_type.to_string(),
    };
    let scope = "stage:authorize_payment";
    assert_setting(info.open_for_ms(), 60_000, "dsl", scope, subject.clone());
    assert_setting(info.probes(), 1, "dsl", scope, subject.clone());
    assert_setting(
        info.rate_limited_counts_as_failure(),
        false,
        "dsl",
        scope,
        subject.clone(),
    );
    assert_setting(
        info.count_window().unwrap(),
        10,
        "dsl",
        scope,
        subject.clone(),
    );
    assert_setting(
        info.failure_rate_threshold().unwrap(),
        0.5,
        "dsl",
        scope,
        subject,
    );
    assert!(info.slow_call_duration_ms().is_none());
    assert!(info.slow_call_rate_threshold().is_none());
    info
}

fn build_flow_future(
    base: std::path::PathBuf,
    ctx: FlowBuildContext,
) -> impl std::future::Future<
    Output = Result<obzenflow_runtime::prelude::FlowHandle, obzenflow_dsl::dsl::FlowBuildFailure>,
> {
    FlowDefinition::materialize(move |_runtime_config| {
        let one_shot_source = OneShotSource { emitted: false };
        let null_sink = NullSink::<Item>::new();

        Ok(flow! {
            name: "effective_config_manifest",
            journals: disk_journals(base),

            stages: {
                src = source!(Item => one_shot_source);
                snk = sink!(Item => null_sink);
            },

            topology: {
                src |> snk;
            }
        })
    })
    .build(ctx)
}

fn build_rate_limited_flow_future(
    base: std::path::PathBuf,
    ctx: FlowBuildContext,
) -> impl std::future::Future<
    Output = Result<obzenflow_runtime::prelude::FlowHandle, obzenflow_dsl::dsl::FlowBuildFailure>,
> {
    FlowDefinition::materialize(move |_runtime_config| {
        let limiter = rate_limit(10.0);
        let one_shot_source = OneShotSource { emitted: false };
        let null_sink = NullSink::<Item>::new();

        Ok(flow! {
            name: "effective_config_manifest_with_optional_limiter_burst",
            journals: disk_journals(base),

            stages: {
                src = source!(Item => one_shot_source);
                snk = sink!(Item => null_sink with {limiter});
            },

            topology: {
                src |> snk;
            }
        })
    })
    .build(ctx)
}

fn payment_resilience() -> obzenflow_adapters::middleware::CircuitBreaker {
    circuit_breaker()
        .count_window(10)
        .minimum_calls(5)
        .failure_rate_threshold(0.5)
}

fn build_two_effect_flow_future(
    base: std::path::PathBuf,
    ctx: FlowBuildContext,
) -> impl std::future::Future<
    Output = Result<obzenflow_runtime::prelude::FlowHandle, obzenflow_dsl::dsl::FlowBuildFailure>,
> {
    FlowDefinition::materialize(move |_runtime_config| {
        let authorize_resilience = payment_resilience();
        let refund_resilience = payment_resilience();
        let one_shot_source = OneShotSource { emitted: false };
        let payment_effects = PaymentEffectsHandler;
        let null_sink = NullSink::<PaymentEffectFact>::new();

        Ok(flow! {
            name: "effective_config_manifest_two_effects",
            journals: disk_journals(base),

            stages: {
                orders = source!(Item => one_shot_source);
                authorize_payment = effectful_transform!(
                    Item -> PaymentEffectFact
                    uses {
                        AuthorizePayment with authorize_resilience,
                        RefundPayment with refund_resilience,
                    }
                    => payment_effects);
                output = sink!(PaymentEffectFact => null_sink);
            },

            topology: {
                orders |> authorize_payment;
                authorize_payment |> output;
            }
        })
    })
    .build(ctx)
}

#[tokio::test]
async fn topology_backpressure_comes_from_resolved_edge_configuration_before_execution() {
    use obzenflow_core::config::ConfigScope;
    use obzenflow_topology::BackpressureInfo;

    for (mode, disable_edge, expected) in [
        ("off", false, None),
        ("track", false, Some(BackpressureInfo::Track)),
        (
            "enforce",
            false,
            Some(BackpressureInfo::Enforce {
                window: std::num::NonZeroU64::new(64).unwrap(),
                stall_timeout_ms: std::num::NonZeroU64::new(30000).unwrap(),
            }),
        ),
        ("enforce", true, None),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let mut candidates = CandidateSet::default();
        for (key, value) in [
            ("runtime.backpressure.mode", ConfigValue::Text(mode.into())),
            ("runtime.backpressure.window", ConfigValue::U64(64)),
            (
                "runtime.backpressure.stall_timeout_ms",
                ConfigValue::U64(30000),
            ),
        ] {
            candidates
                .admit(ScopedCandidate::unqualified(
                    key,
                    ConfigScope::Global,
                    ConfigSource::File,
                    value,
                ))
                .unwrap();
        }
        if disable_edge {
            candidates
                .admit(ScopedCandidate::unqualified(
                    "runtime.backpressure.mode",
                    ConfigScope::edge("src", "snk"),
                    ConfigSource::File,
                    ConfigValue::Text("off".into()),
                ))
                .unwrap();
        }
        let handle = build_flow_future(
            directory.path().to_path_buf(),
            FlowBuildContext::new(Arc::new(ResolvedRuntimeConfig::new(candidates))),
        )
        .await
        .unwrap();
        // No run/start call and no metrics fetch: the built topology already
        // describes the concrete edge policy, including a narrower off override.
        let topology = handle.topology().unwrap();
        assert_eq!(topology.edges().len(), 1);
        assert_eq!(
            topology.edges()[0].backpressure,
            expected,
            "{mode}, override={disable_edge}"
        );
        let decoded: obzenflow_topology::Topology =
            serde_json::from_value(serde_json::to_value(topology.as_ref()).unwrap()).unwrap();
        assert_eq!(decoded.edges()[0].backpressure, expected);
    }
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

    let middleware = round_trip_middleware(&handle, "snk");
    assert_eq!(middleware.attachments.len(), 1);
    let attachment = &middleware.attachments[0];
    assert_eq!(attachment.label, "rate_limiter");
    assert_eq!(attachment.family(), MiddlewareFamily::RateLimiter);
    assert_eq!(
        attachment.authored_site,
        MiddlewareAuthoredSite::Implementation
    );
    assert_eq!(attachment.operation, MiddlewareOperation::SinkDelivery);
    let MiddlewareDetailsInfo::RateLimiter(info) = &attachment.details else {
        panic!("the sink must carry typed limiter information");
    };
    assert_setting(
        info.events_per_second(),
        10.0,
        "dsl",
        "stage:snk",
        SettingSubject::Unqualified,
    );
    assert_setting(
        info.cost_per_attempt(),
        1.0,
        "dsl",
        "stage:snk",
        SettingSubject::Unqualified,
    );
    assert_setting(
        info.burst_capacity()
            .expect("explicit burst remains present"),
        3.0,
        "file",
        "stage:snk",
        SettingSubject::Unqualified,
    );
}

#[tokio::test]
async fn topology_keeps_automatic_burst_absent_without_fabricated_provenance() {
    let dir = tempfile::tempdir().expect("tempdir");
    let handle =
        build_rate_limited_flow_future(dir.path().to_path_buf(), FlowBuildContext::for_tests())
            .await
            .expect("the limiter can calculate its own automatic capacity");
    let middleware = round_trip_middleware(&handle, "snk");
    assert_eq!(middleware.attachments.len(), 1);
    let MiddlewareDetailsInfo::RateLimiter(info) = &middleware.attachments[0].details else {
        panic!("the sink must carry typed limiter information");
    };
    assert!(
        info.burst_capacity().is_none(),
        "an automatically calculated capacity is not a resolved configuration winner"
    );
    let manifest = manifest_for(&handle);
    assert!(manifest
        .effective_config
        .expect("effective configuration must be recorded")
        .values
        .iter()
        .all(|row| row.key_path != RATE_LIMITER_BURST_CAPACITY_KEY));
}

#[tokio::test]
async fn manifest_retains_two_real_effect_rows_for_one_stage_broadcast() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut candidates = CandidateSet::default();
    candidates
        .admit(ScopedCandidate::unqualified(
            CIRCUIT_BREAKER_MINIMUM_CALLS_KEY,
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
        .filter(|row| row.key_path == CIRCUIT_BREAKER_MINIMUM_CALLS_KEY)
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

    let middleware = round_trip_middleware(&handle, "authorize_payment");
    assert_eq!(middleware.attachments.len(), 2);
    assert_ne!(middleware.attachments[0].key, middleware.attachments[1].key);
    let mut effects = Vec::new();
    for attachment in &middleware.attachments {
        let MiddlewareOperation::Effect { effect_type } = &attachment.operation else {
            panic!("each breaker must protect a declared effect");
        };
        effects.push(effect_type.as_str());
        let info = assert_effect_breaker(attachment, effect_type);
        assert_setting(
            info.mode(),
            CircuitBreakerMode::RateBased,
            "dsl",
            "stage:authorize_payment",
            SettingSubject::Effect {
                effect_type: effect_type.clone(),
            },
        );
        assert_setting(
            info.minimum_calls()
                .expect("rate mode requires minimum calls"),
            8,
            "file",
            "stage:authorize_payment",
            SettingSubject::Unqualified,
        );
        assert!(info.consecutive_failures().is_none());
    }
    effects.sort();
    assert_eq!(
        effects,
        [AuthorizePayment::EFFECT_TYPE, RefundPayment::EFFECT_TYPE],
        "equal inherited values retain both concrete effect operations"
    );
}

#[tokio::test]
async fn topology_retains_effect_winners_and_inactive_settings_after_a_mode_override() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut candidates = CandidateSet::default();
    candidates
        .admit(ScopedCandidate::unqualified(
            CIRCUIT_BREAKER_MINIMUM_CALLS_KEY,
            obzenflow_core::config::ConfigScope::stage("authorize_payment"),
            ConfigSource::File,
            ConfigValue::U64(8),
        ))
        .expect("stage broadcast admits");
    for (key, value) in [
        (CIRCUIT_BREAKER_MINIMUM_CALLS_KEY, ConfigValue::U64(6)),
        (
            CIRCUIT_BREAKER_MODE_KEY,
            ConfigValue::Text("consecutive".to_string()),
        ),
        (
            CIRCUIT_BREAKER_CONSECUTIVE_FAILURES_KEY,
            ConfigValue::U64(3),
        ),
    ] {
        candidates
            .admit(ScopedCandidate {
                key_path: key.to_string(),
                address: ConfigAddress::effect("authorize_payment", AuthorizePayment::EFFECT_TYPE),
                source: ConfigSource::File,
                value,
            })
            .expect("exact effect override admits");
    }
    let snapshot = Arc::new(ResolvedRuntimeConfig::new(candidates));
    let handle =
        build_two_effect_flow_future(dir.path().to_path_buf(), FlowBuildContext::new(snapshot))
            .await
            .expect("one effect can select consecutive mode while its sibling remains rate-based");
    let middleware = round_trip_middleware(&handle, "authorize_payment");
    assert_eq!(middleware.attachments.len(), 2);
    assert_ne!(middleware.attachments[0].key, middleware.attachments[1].key);
    let manifest = manifest_for(&handle);
    let evidence = manifest.effective_config.expect("effective configuration");
    for attachment in &middleware.attachments {
        let MiddlewareOperation::Effect { effect_type } = &attachment.operation else {
            panic!("each breaker must protect a declared effect");
        };
        // Count-window and failure-rate rows remain visible even when inactive.
        let info = assert_effect_breaker(attachment, effect_type);
        let exact = effect_type == AuthorizePayment::EFFECT_TYPE;
        assert!(exact || effect_type == RefundPayment::EFFECT_TYPE);
        let effect_subject = SettingSubject::Effect {
            effect_type: effect_type.clone(),
        };
        let minimum = info
            .minimum_calls()
            .expect("inactive settings remain evidence");
        assert_setting(
            minimum,
            if exact { 6 } else { 8 },
            "file",
            "stage:authorize_payment",
            if exact {
                effect_subject.clone()
            } else {
                SettingSubject::Unqualified
            },
        );
        assert_setting(
            info.mode(),
            if exact {
                CircuitBreakerMode::Consecutive
            } else {
                CircuitBreakerMode::RateBased
            },
            if exact { "file" } else { "dsl" },
            "stage:authorize_payment",
            effect_subject.clone(),
        );
        if exact {
            assert_setting(
                info.consecutive_failures()
                    .expect("active consecutive setting"),
                3,
                "file",
                "stage:authorize_payment",
                effect_subject,
            );
        } else {
            assert!(info.consecutive_failures().is_none());
        }
        let minimum_row = evidence
            .values
            .iter()
            .find(|row| {
                row.key_path == CIRCUIT_BREAKER_MINIMUM_CALLS_KEY
                    && row.resolved_for
                        == Some(ResolvedForDoc::Effect {
                            stage: "authorize_payment".to_string(),
                            effect_type: effect_type.clone(),
                        })
            })
            .expect("the topology setting must have corresponding manifest evidence");
        assert_eq!(minimum_row.value, json!(minimum.value));
        assert_eq!(minimum_row.source, minimum.provenance.source);
        assert_eq!(minimum_row.scope, minimum.provenance.scope);
        assert_eq!(
            minimum_row.winner_subject,
            Some(if exact {
                ConfigSubject::Effect {
                    effect_type: effect_type.as_str().into(),
                }
            } else {
                ConfigSubject::Unqualified
            })
        );
    }
}

#[test]
fn checked_schema_v3_studio_fixture_is_a_complete_run_manifest() {
    let fixture = include_str!("fixtures/effective_config_v3_manifest.json");
    let manifest: RunManifest =
        serde_json::from_str(fixture).expect("Studio fixture must be a valid RunManifest");
    assert_eq!(
        manifest.journal_schema_version,
        obzenflow_core::journal::JOURNAL_SCHEMA_VERSION
    );
    // This retained Studio fixture checks structural decoding of the current
    // manifest, including effective-config evidence; it is not a replay archive.
    let evidence = manifest
        .effective_config
        .expect("Studio fixture must carry effective-config evidence");
    assert_eq!(evidence.schema_version, 2);
    assert_eq!(evidence.values.len(), 2);
    assert!(evidence
        .values
        .iter()
        .all(|row| row.key_path == CIRCUIT_BREAKER_MINIMUM_CALLS_KEY));
    assert_ne!(
        evidence.values[0].resolved_for,
        evidence.values[1].resolved_for
    );
}
