// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Source acquisition for the FLOWIP-010 runtime config snapshot: raw file
//! tables, canonical env spellings, and the resume-view CLI flags become a
//! type-validated `CandidateSet`, then the immutable `ResolvedRuntimeConfig`.
//!
//! Scoped entries ride the file source only (§2 first-pass lock; DSL
//! candidates join at flow build); CLI and env candidates are global-only.
//! Startup diagnostics keep the `config error at <path>: ...` currency.

use super::config::{ConfigError, FlowConfig, RawFileStartupConfig};
use crate::env::{env_bool, env_var};
use obzenflow_core::config::{ConfigAddress, ConfigScope, ConfigSource};
use obzenflow_runtime::runtime_config::{
    knob_registry, CandidateSet, ConfigResolveError, ConfigValue, KnobType, ResolvedRuntimeConfig,
    ScopedCandidate,
};

/// Build the startup snapshot (Phase A). The DSL tier and stage/edge
/// resolution join at flow build (`materialize_flow_config`).
pub(crate) fn build_runtime_config_snapshot(
    cli: &FlowConfig,
    file: &RawFileStartupConfig,
) -> Result<ResolvedRuntimeConfig, ConfigError> {
    let mut set = CandidateSet::default();
    admit_file_candidates(&mut set, file)?;
    admit_env_candidates(&mut set)?;
    admit_resume_view_cli(&mut set, cli)?;
    Ok(ResolvedRuntimeConfig::new(set))
}

fn admit(
    set: &mut CandidateSet,
    key_path: &str,
    address: impl Into<ConfigAddress>,
    source: ConfigSource,
    value: ConfigValue,
    at: &str,
) -> Result<(), ConfigError> {
    set.admit(ScopedCandidate {
        key_path: key_path.to_string(),
        address: address.into(),
        source,
        value,
    })
    .map_err(|err| match err {
        ConfigResolveError::InvalidValue { message, .. } => ConfigError::at(at, message),
        other => ConfigError::at(at, other.to_string()),
    })
}

fn u64_value(at: &str, raw: i64) -> Result<ConfigValue, ConfigError> {
    if raw < 0 {
        Err(ConfigError::at(at, format!("must be >= 0, got {raw}")))
    } else {
        Ok(ConfigValue::U64(raw as u64))
    }
}

/// One optional file field -> one candidate, with the dotted file path as
/// the diagnostic address.
macro_rules! file_u64 {
    ($set:expr, $key:expr, $scope:expr, $field:expr, $at:expr) => {
        if let Some(raw) = $field {
            let value = u64_value($at, raw)?;
            admit($set, $key, $scope, ConfigSource::File, value, $at)?;
        }
    };
}

/// One optional text file field -> one candidate; Token knobs validate the
/// allowed set at admission.
macro_rules! file_text {
    ($set:expr, $key:expr, $scope:expr, $field:expr, $at:expr) => {
        if let Some(raw) = $field {
            admit(
                $set,
                $key,
                $scope,
                ConfigSource::File,
                ConfigValue::Text(raw.clone()),
                $at,
            )?;
        }
    };
}

fn admit_file_candidates(
    set: &mut CandidateSet,
    file: &RawFileStartupConfig,
) -> Result<(), ConfigError> {
    // [runtime] global rung.
    let runtime = &file.runtime;
    file_u64!(
        set,
        "runtime.max_lineage_depth",
        ConfigScope::Global,
        runtime.max_lineage_depth,
        "runtime.max_lineage_depth"
    );
    file_u64!(
        set,
        "runtime.cycle_max_iterations",
        ConfigScope::Global,
        runtime.cycle_max_iterations,
        "runtime.cycle_max_iterations"
    );
    file_u64!(
        set,
        "runtime.heartbeat_interval",
        ConfigScope::Global,
        runtime.heartbeat_interval,
        "runtime.heartbeat_interval"
    );
    file_u64!(
        set,
        "runtime.metrics_drain_timeout_ms",
        ConfigScope::Global,
        runtime.metrics_drain_timeout_ms,
        "runtime.metrics_drain_timeout_ms"
    );

    // [runtime.flow].
    file_u64!(
        set,
        "runtime.max_lineage_depth",
        ConfigScope::Flow,
        runtime.flow.max_lineage_depth,
        "runtime.flow.max_lineage_depth"
    );
    file_u64!(
        set,
        "runtime.cycle_max_iterations",
        ConfigScope::Flow,
        runtime.flow.cycle_max_iterations,
        "runtime.flow.cycle_max_iterations"
    );
    file_u64!(
        set,
        "runtime.heartbeat_interval",
        ConfigScope::Flow,
        runtime.flow.heartbeat_interval,
        "runtime.flow.heartbeat_interval"
    );

    // [runtime.stages.<key>] (stage existence is validated at flow build).
    for (stage, entry) in &runtime.stages {
        let scope = ConfigScope::stage(stage.as_str());
        file_u64!(
            set,
            "runtime.max_lineage_depth",
            scope.clone(),
            entry.max_lineage_depth,
            &format!("runtime.stages.{stage}.max_lineage_depth")
        );
        file_u64!(
            set,
            "runtime.heartbeat_interval",
            scope,
            entry.heartbeat_interval,
            &format!("runtime.stages.{stage}.heartbeat_interval")
        );
    }

    // [runtime.backpressure] with the §4c nested edge layout. Mode, window,
    // and stall timeout ride every rung (FLOWIP-115e).
    let backpressure = &runtime.backpressure;
    file_text!(
        set,
        "runtime.backpressure.mode",
        ConfigScope::Global,
        &backpressure.mode,
        "runtime.backpressure.mode"
    );
    file_u64!(
        set,
        "runtime.backpressure.window",
        ConfigScope::Global,
        backpressure.window,
        "runtime.backpressure.window"
    );
    file_u64!(
        set,
        "runtime.backpressure.stall_timeout_ms",
        ConfigScope::Global,
        backpressure.stall_timeout_ms,
        "runtime.backpressure.stall_timeout_ms"
    );
    file_text!(
        set,
        "runtime.backpressure.mode",
        ConfigScope::Flow,
        &backpressure.flow.mode,
        "runtime.backpressure.flow.mode"
    );
    file_u64!(
        set,
        "runtime.backpressure.window",
        ConfigScope::Flow,
        backpressure.flow.window,
        "runtime.backpressure.flow.window"
    );
    file_u64!(
        set,
        "runtime.backpressure.stall_timeout_ms",
        ConfigScope::Flow,
        backpressure.flow.stall_timeout_ms,
        "runtime.backpressure.flow.stall_timeout_ms"
    );
    for (stage, entry) in &backpressure.stages {
        file_text!(
            set,
            "runtime.backpressure.mode",
            ConfigScope::stage(stage.as_str()),
            &entry.mode,
            &format!("runtime.backpressure.stages.{stage}.mode")
        );
        file_u64!(
            set,
            "runtime.backpressure.window",
            ConfigScope::stage(stage.as_str()),
            entry.window,
            &format!("runtime.backpressure.stages.{stage}.window")
        );
        file_u64!(
            set,
            "runtime.backpressure.stall_timeout_ms",
            ConfigScope::stage(stage.as_str()),
            entry.stall_timeout_ms,
            &format!("runtime.backpressure.stages.{stage}.stall_timeout_ms")
        );
        for (downstream, edge) in &entry.edges {
            file_text!(
                set,
                "runtime.backpressure.mode",
                ConfigScope::edge(stage.as_str(), downstream.as_str()),
                &edge.mode,
                &format!("runtime.backpressure.stages.{stage}.edges.{downstream}.mode")
            );
            file_u64!(
                set,
                "runtime.backpressure.window",
                ConfigScope::edge(stage.as_str(), downstream.as_str()),
                edge.window,
                &format!("runtime.backpressure.stages.{stage}.edges.{downstream}.window")
            );
            file_u64!(
                set,
                "runtime.backpressure.stall_timeout_ms",
                ConfigScope::edge(stage.as_str(), downstream.as_str()),
                edge.stall_timeout_ms,
                &format!("runtime.backpressure.stages.{stage}.edges.{downstream}.stall_timeout_ms")
            );
        }
    }

    // [contracts].
    if let Some(mode) = &file.contracts.source_contract_strict_mode {
        admit(
            set,
            "contracts.source_contract_strict_mode",
            ConfigScope::Global,
            ConfigSource::File,
            ConfigValue::Text(mode.clone()),
            "contracts.source_contract_strict_mode",
        )?;
    }

    // [effects] global/flow/stages.
    admit_effects_fields(
        set,
        ConfigScope::Global.into(),
        &file.effects.circuit_breaker,
        &file.effects.rate_limiter,
        &file.effects.resilience,
        "effects",
    )?;
    admit_effects_fields(
        set,
        ConfigScope::Flow.into(),
        &file.effects.flow.circuit_breaker,
        &file.effects.flow.rate_limiter,
        &file.effects.flow.resilience,
        "effects.flow",
    )?;
    for (stage, entry) in &file.effects.stages {
        admit_effects_fields(
            set,
            ConfigScope::stage(stage.as_str()).into(),
            &entry.circuit_breaker,
            &entry.rate_limiter,
            &entry.resilience,
            &format!("effects.stages.{stage}"),
        )?;
        for (effect_type, exact) in &entry.by_type {
            admit_effects_fields(
                set,
                ConfigAddress::effect(stage.as_str(), effect_type.as_str()),
                &exact.circuit_breaker,
                &exact.rate_limiter,
                &exact.resilience,
                &format!("effects.stages.{stage}.by_type.\"{effect_type}\""),
            )?;
        }
    }

    // [ai.models].
    let models = &file.ai.models;
    for (key, field) in [
        ("ai.models.provider", &models.provider),
        ("ai.models.model", &models.model),
        ("ai.models.base_url", &models.base_url),
        ("ai.models.api_key_env", &models.api_key_env),
    ] {
        if let Some(value) = field {
            admit(
                set,
                key,
                ConfigScope::Global,
                ConfigSource::File,
                ConfigValue::Text(value.clone()),
                key,
            )?;
        }
    }

    // The runtime.resume.* view over the existing [replay] table
    // (absorption without moving the parse; provenance stays honest by
    // admitting the same raw inputs 010h resolves).
    let replay = &file.replay;
    for (key, path) in [
        ("runtime.resume.replay_from", &replay.from),
        ("runtime.resume.resume_from", &replay.resume_from),
    ] {
        if let Some(value) = path {
            admit(
                set,
                key,
                ConfigScope::Global,
                ConfigSource::File,
                ConfigValue::Text(value.display().to_string()),
                key,
            )?;
        }
    }
    for (key, flag) in [
        (
            "runtime.resume.allow_incomplete_archive",
            replay.allow_incomplete_archive,
        ),
        (
            "runtime.resume.allow_duplicate_sink_delivery",
            replay.allow_duplicate_sink_delivery,
        ),
        ("runtime.resume.verify", replay.verify),
    ] {
        if let Some(value) = flag {
            admit(
                set,
                key,
                ConfigScope::Global,
                ConfigSource::File,
                ConfigValue::Bool(value),
                key,
            )?;
        }
    }

    Ok(())
}

fn admit_effects_fields(
    set: &mut CandidateSet,
    address: ConfigAddress,
    breaker: &super::config::RawBreakerFields,
    limiter: &super::config::RawLimiterFields,
    resilience: &super::config::RawResilienceFields,
    at_prefix: &str,
) -> Result<(), ConfigError> {
    file_u64!(
        set,
        "effects.circuit_breaker.threshold",
        address.clone(),
        breaker.threshold,
        &format!("{at_prefix}.circuit_breaker.threshold")
    );
    if let Some(rate) = limiter.events_per_second {
        admit(
            set,
            "effects.rate_limiter.events_per_second",
            address.clone(),
            ConfigSource::File,
            ConfigValue::F64(rate),
            &format!("{at_prefix}.rate_limiter.events_per_second"),
        )?;
    }
    if let Some(burst) = limiter.burst_capacity {
        admit(
            set,
            "effects.rate_limiter.burst_capacity",
            address.clone(),
            ConfigSource::File,
            ConfigValue::F64(burst),
            &format!("{at_prefix}.rate_limiter.burst_capacity"),
        )?;
    }

    let rb = &resilience.breaker;
    if let Some(mode) = &rb.mode {
        admit(
            set,
            "effects.resilience.breaker.mode",
            address.clone(),
            ConfigSource::File,
            ConfigValue::Text(mode.clone()),
            &format!("{at_prefix}.resilience.breaker.mode"),
        )?;
    }
    file_u64!(
        set,
        "effects.resilience.breaker.consecutive_failures",
        address.clone(),
        rb.consecutive_failures,
        &format!("{at_prefix}.resilience.breaker.consecutive_failures")
    );
    file_u64!(
        set,
        "effects.resilience.breaker.count_window",
        address.clone(),
        rb.count_window,
        &format!("{at_prefix}.resilience.breaker.count_window")
    );
    file_u64!(
        set,
        "effects.resilience.breaker.minimum_calls",
        address.clone(),
        rb.minimum_calls,
        &format!("{at_prefix}.resilience.breaker.minimum_calls")
    );
    if let Some(value) = rb.failure_rate_threshold {
        admit(
            set,
            "effects.resilience.breaker.failure_rate_threshold",
            address.clone(),
            ConfigSource::File,
            ConfigValue::F64(value),
            &format!("{at_prefix}.resilience.breaker.failure_rate_threshold"),
        )?;
    }
    file_u64!(
        set,
        "effects.resilience.breaker.slow_call_duration_ms",
        address.clone(),
        rb.slow_call_duration_ms,
        &format!("{at_prefix}.resilience.breaker.slow_call_duration_ms")
    );
    if let Some(value) = rb.slow_call_rate_threshold {
        admit(
            set,
            "effects.resilience.breaker.slow_call_rate_threshold",
            address.clone(),
            ConfigSource::File,
            ConfigValue::F64(value),
            &format!("{at_prefix}.resilience.breaker.slow_call_rate_threshold"),
        )?;
    }
    file_u64!(
        set,
        "effects.resilience.breaker.open_for_ms",
        address.clone(),
        rb.open_for_ms,
        &format!("{at_prefix}.resilience.breaker.open_for_ms")
    );
    file_u64!(
        set,
        "effects.resilience.breaker.probes",
        address.clone(),
        rb.probes,
        &format!("{at_prefix}.resilience.breaker.probes")
    );
    if let Some(value) = rb.rate_limited_counts_as_failure {
        admit(
            set,
            "effects.resilience.breaker.rate_limited_counts_as_failure",
            address.clone(),
            ConfigSource::File,
            ConfigValue::Bool(value),
            &format!("{at_prefix}.resilience.breaker.rate_limited_counts_as_failure"),
        )?;
    }

    let retry = &resilience.retry;
    if let Some(kind) = &retry.kind {
        admit(
            set,
            "effects.resilience.retry.kind",
            address.clone(),
            ConfigSource::File,
            ConfigValue::Text(kind.clone()),
            &format!("{at_prefix}.resilience.retry.kind"),
        )?;
    }
    file_u64!(
        set,
        "effects.resilience.retry.fixed_delay_ms",
        address.clone(),
        retry.fixed_delay_ms,
        &format!("{at_prefix}.resilience.retry.fixed_delay_ms")
    );
    file_u64!(
        set,
        "effects.resilience.retry.max_attempts",
        address.clone(),
        retry.max_attempts,
        &format!("{at_prefix}.resilience.retry.max_attempts")
    );
    file_u64!(
        set,
        "effects.resilience.retry.max_backoff_ms",
        address.clone(),
        retry.max_backoff_ms,
        &format!("{at_prefix}.resilience.retry.max_backoff_ms")
    );
    file_u64!(
        set,
        "effects.resilience.retry.attempt_start_window_ms",
        address.clone(),
        retry.attempt_start_window_ms,
        &format!("{at_prefix}.resilience.retry.attempt_start_window_ms")
    );

    let resilience_limiter = &resilience.rate_limiter;
    for (key, value, at) in [
        (
            "effects.resilience.rate_limiter.events_per_second",
            resilience_limiter.events_per_second,
            "events_per_second",
        ),
        (
            "effects.resilience.rate_limiter.burst_capacity",
            resilience_limiter.burst_capacity,
            "burst_capacity",
        ),
        (
            "effects.resilience.rate_limiter.cost_per_attempt",
            resilience_limiter.cost_per_attempt,
            "cost_per_attempt",
        ),
    ] {
        if let Some(value) = value {
            admit(
                set,
                key,
                address.clone(),
                ConfigSource::File,
                ConfigValue::F64(value),
                &format!("{at_prefix}.resilience.rate_limiter.{at}"),
            )?;
        }
    }
    Ok(())
}

/// Env acquisition: the canonical spellings (§2 environment-tier lock) plus
/// the named 010h spellings kept by the runtime.resume view. Global-only.
fn admit_env_candidates(set: &mut CandidateSet) -> Result<(), ConfigError> {
    for spec in knob_registry() {
        let Some(name) = spec.env_name() else {
            continue;
        };
        let value = match spec.value_type {
            KnobType::Bool => env_bool(&name)
                .map_err(|err| ConfigError::at(spec.key_path, err.to_string()))?
                .map(ConfigValue::Bool),
            KnobType::U64 { .. } => env_var::<u64>(&name)
                .map_err(|err| ConfigError::at(spec.key_path, err.to_string()))?
                .map(ConfigValue::U64),
            KnobType::F64 { .. } | KnobType::F64Range { .. } => env_var::<f64>(&name)
                .map_err(|err| ConfigError::at(spec.key_path, err.to_string()))?
                .map(ConfigValue::F64),
            KnobType::Text | KnobType::Token { .. } | KnobType::Path => env_var::<String>(&name)
                .map_err(|err| ConfigError::at(spec.key_path, err.to_string()))?
                .map(ConfigValue::Text),
        };
        if let Some(value) = value {
            admit(
                set,
                spec.key_path,
                ConfigScope::Global,
                ConfigSource::Env,
                value,
                spec.key_path,
            )?;
        }
    }
    Ok(())
}

/// The resume-view CLI flags (the only CLI candidates in the first pass;
/// `--set` is deferred). Positive flags admit only when set, matching the
/// 010h resolve_positive_flag posture.
fn admit_resume_view_cli(set: &mut CandidateSet, cli: &FlowConfig) -> Result<(), ConfigError> {
    for (key, path) in [
        ("runtime.resume.replay_from", &cli.replay_from),
        ("runtime.resume.resume_from", &cli.resume_from),
    ] {
        if let Some(value) = path {
            admit(
                set,
                key,
                ConfigScope::Global,
                ConfigSource::Cli,
                ConfigValue::Text(value.display().to_string()),
                key,
            )?;
        }
    }
    for (key, flag) in [
        (
            "runtime.resume.allow_incomplete_archive",
            cli.allow_incomplete_archive,
        ),
        (
            "runtime.resume.allow_duplicate_sink_delivery",
            cli.allow_duplicate_sink_delivery,
        ),
        ("runtime.resume.verify", cli.verify),
    ] {
        if flag {
            admit(
                set,
                key,
                ConfigScope::Global,
                ConfigSource::Cli,
                ConfigValue::Bool(true),
                key,
            )?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{env_lock, EnvGuard};
    use clap::Parser;
    use obzenflow_core::event::EffectType;
    use obzenflow_core::StageKey;
    use obzenflow_runtime::runtime_config::{
        materialize_flow_config, DslCandidates, FlowResolutionContext,
        RATE_LIMITER_EVENTS_PER_SECOND_KEY,
    };
    use std::collections::{BTreeMap, BTreeSet};

    fn parse_file(toml: &str) -> RawFileStartupConfig {
        toml::from_str(toml).expect("test TOML should parse")
    }

    fn cli(args: &[&str]) -> FlowConfig {
        let mut argv = vec!["test"];
        argv.extend_from_slice(args);
        FlowConfig::parse_from(argv)
    }

    fn snapshot(toml: &str, args: &[&str]) -> ResolvedRuntimeConfig {
        build_runtime_config_snapshot(&cli(args), &parse_file(toml)).expect("snapshot builds")
    }

    #[test]
    fn new_tables_parse_and_resolve_with_file_provenance() {
        let _lock = env_lock();
        let _guard = EnvGuard::new(&["OBZENFLOW_RUNTIME_MAX_LINEAGE_DEPTH"]);
        let snapshot = snapshot(
            r#"
            [runtime]
            max_lineage_depth = 7
            cycle_max_iterations = 12

            [runtime.backpressure.stages.enricher.edges.merger]
            window = 2000

            [contracts]
            source_contract_strict_mode = "warn"

            [effects.stages.fetcher.rate_limiter]
            events_per_second = 10.5

            [ai.models]
            provider = "openai"
            api_key_env = "MY_KEY"
            "#,
            &[],
        );
        let docs = snapshot.global_view();
        let doc = |key: &str| docs.iter().find(|d| d.key_path == key).unwrap();
        assert_eq!(doc("runtime.max_lineage_depth").value, serde_json::json!(7));
        assert_eq!(doc("runtime.max_lineage_depth").source, "file");
        assert_eq!(
            doc("contracts.source_contract_strict_mode").value,
            serde_json::json!("warn")
        );
        assert_eq!(doc("ai.models.provider").value, serde_json::json!("openai"));
        // Scoped entries live in the candidate table for Phase B, not the
        // global view.
        assert!(snapshot
            .candidates()
            .get(
                "runtime.backpressure.window",
                &ConfigScope::edge("enricher", "merger")
            )
            .is_some());
        assert!(snapshot
            .candidates()
            .get(
                "effects.rate_limiter.events_per_second",
                &ConfigScope::stage("fetcher")
            )
            .is_some());
    }

    #[test]
    fn exact_effect_file_entries_parse_at_the_canonical_address_and_resolve() {
        let _lock = env_lock();
        let _guard = EnvGuard::new(&["OBZENFLOW_EFFECTS_RATE_LIMITER_EVENTS_PER_SECOND"]);
        let snapshot = snapshot(
            r#"
            [effects.stages.authorize_payment.rate_limiter]
            events_per_second = 8.0

            [effects.stages.authorize_payment.by_type."payments.authorize".rate_limiter]
            events_per_second = 5.0
            "#,
            &[],
        );
        let key = RATE_LIMITER_EVENTS_PER_SECOND_KEY;
        let stage = StageKey::from("authorize_payment");
        let authorize = EffectType::from("payments.authorize");
        let refund = EffectType::from("payments.refund");

        assert_eq!(
            snapshot
                .candidates()
                .get_at(
                    key,
                    &ConfigAddress::effect(stage.clone(), authorize.clone())
                )
                .and_then(|slots| slots.file.as_ref()),
            Some(&ConfigValue::F64(5.0))
        );
        assert_eq!(
            snapshot
                .candidates()
                .get(key, &ConfigScope::stage(stage.clone()))
                .and_then(|slots| slots.file.as_ref()),
            Some(&ConfigValue::F64(8.0))
        );

        let mut dsl = DslCandidates::default();
        dsl.declare_for_effect(key, stage.clone(), authorize.clone(), ConfigValue::F64(2.0));
        dsl.declare_for_effect(key, stage.clone(), refund.clone(), ConfigValue::F64(3.0));
        let effective = materialize_flow_config(
            &snapshot,
            FlowResolutionContext {
                flow_name: "payment_flow".to_string(),
                stages: BTreeSet::from([stage.clone()]),
                edges: BTreeSet::new(),
                declared_effects: BTreeMap::from([(
                    stage.clone(),
                    BTreeSet::from([authorize.clone(), refund.clone()]),
                )]),
                dsl,
            },
        )
        .unwrap();

        assert_eq!(
            effective
                .effect_value(key, &stage, &authorize)
                .and_then(|resolved| resolved.value.as_f64()),
            Some(5.0),
            "same-scope exact file refines the broadcast file value"
        );
        assert_eq!(
            effective
                .effect_value(key, &stage, &refund)
                .and_then(|resolved| resolved.value.as_f64()),
            Some(8.0),
            "the stage file broadcast beats the exact DSL default"
        );
    }

    #[test]
    fn deny_unknown_fields_rejects_typos_in_every_new_table() {
        for bad in [
            "[runtime]\nmax_lineage_dept = 7",
            "[runtime.flow]\nmetrics_drain_timeout_ms = 1",
            "[runtime.stages.x]\ncycle_max_iterations = 5",
            "[runtime.backpressure]\nwindows = 10",
            "[contracts]\nstrict = true",
            "[effects.stages.x]\nedges = {}",
            "[ai.models]\nprovder = \"ollama\"",
        ] {
            assert!(
                toml::from_str::<RawFileStartupConfig>(bad).is_err(),
                "should reject: {bad}"
            );
        }
    }

    #[test]
    fn effect_subjects_are_rejected_outside_stage_by_type() {
        for bad in [
            "[effects.by_type.\"payments.authorize\".rate_limiter]\nevents_per_second = 1.0",
            "[effects.flow.by_type.\"payments.authorize\".rate_limiter]\nevents_per_second = 1.0",
            "[effects.stages.payments.edges.sink.by_type.\"payments.authorize\".rate_limiter]\nevents_per_second = 1.0",
            "[runtime.stages.payments.by_type.\"payments.authorize\"]\nheartbeat_interval = 1",
        ] {
            assert!(
                toml::from_str::<RawFileStartupConfig>(bad).is_err(),
                "should reject a misplaced effect subject: {bad}"
            );
        }
    }

    #[test]
    fn range_and_token_errors_report_dotted_paths() {
        let err = build_runtime_config_snapshot(
            &cli(&[]),
            &parse_file("[runtime.stages.enricher]\nmax_lineage_depth = -1"),
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "config error at runtime.stages.enricher.max_lineage_depth: must be >= 0, got -1"
        );

        let err = build_runtime_config_snapshot(
            &cli(&[]),
            &parse_file("[runtime]\ncycle_max_iterations = 70000"),
        )
        .unwrap_err();
        assert!(err
            .to_string()
            .contains("config error at runtime.cycle_max_iterations: must be in range 1..=65535"));

        let err = build_runtime_config_snapshot(
            &cli(&[]),
            &parse_file("[contracts]\nsource_contract_strict_mode = \"loud\""),
        )
        .unwrap_err();
        assert!(err.to_string().contains("expected one of abort, warn"));
    }

    #[test]
    fn canonical_env_spellings_feed_global_candidates() {
        let _lock = env_lock();
        let guard = EnvGuard::new(&[
            "OBZENFLOW_RUNTIME_MAX_LINEAGE_DEPTH",
            "OBZENFLOW_MAX_LINEAGE_DEPTH",
        ]);
        guard.set("OBZENFLOW_RUNTIME_MAX_LINEAGE_DEPTH", "42");
        // The killed legacy spelling has no effect (regression for the
        // legacy-kill decision).
        guard.set("OBZENFLOW_MAX_LINEAGE_DEPTH", "9");

        let snapshot = snapshot("", &[]);
        let docs = snapshot.global_view();
        let lineage = docs
            .iter()
            .find(|d| d.key_path == "runtime.max_lineage_depth")
            .unwrap();
        assert_eq!(lineage.value, serde_json::json!(42));
        assert_eq!(lineage.source, "env");
    }

    #[test]
    fn file_beats_env_within_the_global_scope() {
        let _lock = env_lock();
        let guard = EnvGuard::new(&["OBZENFLOW_RUNTIME_HEARTBEAT_INTERVAL"]);
        guard.set("OBZENFLOW_RUNTIME_HEARTBEAT_INTERVAL", "5");
        let snapshot = snapshot("[runtime]\nheartbeat_interval = 250", &[]);
        let docs = snapshot.global_view();
        let heartbeat = docs
            .iter()
            .find(|d| d.key_path == "runtime.heartbeat_interval")
            .unwrap();
        assert_eq!(heartbeat.value, serde_json::json!(250));
        assert_eq!(heartbeat.source, "file");
    }

    #[test]
    fn resume_view_carries_cli_over_file_with_named_env_spellings() {
        let _lock = env_lock();
        let _guard = EnvGuard::new(&["OBZENFLOW_REPLAY_VERIFY"]);
        let snapshot = snapshot(
            "[replay]\nverify = false",
            &["--replay-from", "/tmp/run", "--verify"],
        );
        let docs = snapshot.global_view();
        let verify = docs
            .iter()
            .find(|d| d.key_path == "runtime.resume.verify")
            .unwrap();
        assert_eq!(verify.value, serde_json::json!(true));
        assert_eq!(verify.source, "cli");
        let from = docs
            .iter()
            .find(|d| d.key_path == "runtime.resume.replay_from")
            .unwrap();
        assert_eq!(from.source, "cli");
        assert_eq!(from.value, serde_json::json!("/tmp/run"));
    }

    /// Regression for the legacy-kill decision: the retired scattered
    /// spellings have no effect on the resolved snapshot. Only the
    /// canonical `OBZENFLOW_<KEY_PATH>` names feed the env tier.
    #[test]
    fn killed_legacy_env_spellings_have_no_effect() {
        let _lock = env_lock();
        let guard = EnvGuard::new(&[
            "OBZENFLOW_HEARTBEAT_INTERVAL",
            "OBZENFLOW_METRICS_DRAIN_TIMEOUT_MS",
            "OBZENFLOW_SOURCE_CONTRACT_STRICT_MODE",
            "OBZENFLOW_CYCLE_MAX_ITERATIONS",
            "OBZENFLOW_AI_PROVIDER",
            "OBZENFLOW_RUNTIME_HEARTBEAT_INTERVAL",
            "OBZENFLOW_RUNTIME_METRICS_DRAIN_TIMEOUT_MS",
            "OBZENFLOW_CONTRACTS_SOURCE_CONTRACT_STRICT_MODE",
            "OBZENFLOW_RUNTIME_CYCLE_MAX_ITERATIONS",
            "OBZENFLOW_AI_MODELS_PROVIDER",
        ]);
        guard.set("OBZENFLOW_HEARTBEAT_INTERVAL", "1");
        guard.set("OBZENFLOW_METRICS_DRAIN_TIMEOUT_MS", "1");
        guard.set("OBZENFLOW_SOURCE_CONTRACT_STRICT_MODE", "warn");
        guard.set("OBZENFLOW_CYCLE_MAX_ITERATIONS", "1");
        guard.set("OBZENFLOW_AI_PROVIDER", "openai");

        let snapshot = snapshot("", &[]);
        let docs = snapshot.global_view();
        let expect_default = |key: &str| {
            let doc = docs.iter().find(|d| d.key_path == key).unwrap();
            assert_eq!(
                doc.source, "default",
                "{key} must ignore the killed legacy spelling"
            );
        };
        expect_default("runtime.heartbeat_interval");
        expect_default("runtime.metrics_drain_timeout_ms");
        expect_default("contracts.source_contract_strict_mode");
        expect_default("runtime.cycle_max_iterations");
        expect_default("ai.models.provider");
    }

    /// Source-tree assertion for the legacy kill: the retired spellings may
    /// appear only in the regression tests that assert they are dead.
    #[test]
    fn killed_legacy_env_spellings_absent_from_source() {
        let killed = [
            "OBZENFLOW_HEARTBEAT_INTERVAL",
            "OBZENFLOW_METRICS_DRAIN_TIMEOUT_MS",
            "OBZENFLOW_SOURCE_CONTRACT_STRICT_MODE",
            "OBZENFLOW_CYCLE_MAX_ITERATIONS",
            "OBZENFLOW_AI_PROVIDER",
            "OBZENFLOW_AI_MODEL\"",
            "OBZENFLOW_MAX_LINEAGE_DEPTH",
        ];
        let allowed_files = ["runtime_config_sources.rs", "lineage_propagation_test.rs"];

        let workspace_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .expect("workspace root")
            .to_path_buf();

        let mut offenders = Vec::new();
        let mut stack = vec![
            workspace_root.join("crates"),
            workspace_root.join("src"),
            workspace_root.join("tests"),
            workspace_root.join("examples"),
        ];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    if path.file_name().is_some_and(|n| n == "target") {
                        continue;
                    }
                    stack.push(path);
                } else if path.extension().is_some_and(|e| e == "rs") {
                    let file_name = path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or_default()
                        .to_string();
                    if allowed_files.contains(&file_name.as_str()) {
                        continue;
                    }
                    let Ok(content) = std::fs::read_to_string(&path) else {
                        continue;
                    };
                    for spelling in killed {
                        if content.contains(spelling) {
                            offenders.push(format!("{}: {spelling}", path.display()));
                        }
                    }
                }
            }
        }

        assert!(
            offenders.is_empty(),
            "killed legacy env spellings found in source:\n{}",
            offenders.join("\n")
        );
    }
}
