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
use obzenflow_core::config::{ConfigScope, ConfigSource};
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
    scope: ConfigScope,
    source: ConfigSource,
    value: ConfigValue,
    at: &str,
) -> Result<(), ConfigError> {
    set.admit(ScopedCandidate {
        key_path: key_path.to_string(),
        scope,
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

    // [runtime.backpressure] with the §4c nested edge layout.
    let backpressure = &runtime.backpressure;
    file_u64!(
        set,
        "runtime.backpressure.window",
        ConfigScope::Global,
        backpressure.window,
        "runtime.backpressure.window"
    );
    file_u64!(
        set,
        "runtime.backpressure.window",
        ConfigScope::Flow,
        backpressure.flow.window,
        "runtime.backpressure.flow.window"
    );
    for (stage, entry) in &backpressure.stages {
        file_u64!(
            set,
            "runtime.backpressure.window",
            ConfigScope::stage(stage.as_str()),
            entry.window,
            &format!("runtime.backpressure.stages.{stage}.window")
        );
        for (downstream, edge) in &entry.edges {
            file_u64!(
                set,
                "runtime.backpressure.window",
                ConfigScope::edge(stage.as_str(), downstream.as_str()),
                edge.window,
                &format!("runtime.backpressure.stages.{stage}.edges.{downstream}.window")
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
        ConfigScope::Global,
        &file.effects.circuit_breaker,
        &file.effects.rate_limiter,
        "effects",
    )?;
    admit_effects_fields(
        set,
        ConfigScope::Flow,
        &file.effects.flow.circuit_breaker,
        &file.effects.flow.rate_limiter,
        "effects.flow",
    )?;
    for (stage, entry) in &file.effects.stages {
        admit_effects_fields(
            set,
            ConfigScope::stage(stage.as_str()),
            &entry.circuit_breaker,
            &entry.rate_limiter,
            &format!("effects.stages.{stage}"),
        )?;
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
    scope: ConfigScope,
    breaker: &super::config::RawBreakerFields,
    limiter: &super::config::RawLimiterFields,
    at_prefix: &str,
) -> Result<(), ConfigError> {
    file_u64!(
        set,
        "effects.circuit_breaker.threshold",
        scope.clone(),
        breaker.threshold,
        &format!("{at_prefix}.circuit_breaker.threshold")
    );
    if let Some(rate) = limiter.events_per_second {
        admit(
            set,
            "effects.rate_limiter.events_per_second",
            scope.clone(),
            ConfigSource::File,
            ConfigValue::F64(rate),
            &format!("{at_prefix}.rate_limiter.events_per_second"),
        )?;
    }
    if let Some(burst) = limiter.burst_capacity {
        admit(
            set,
            "effects.rate_limiter.burst_capacity",
            scope,
            ConfigSource::File,
            ConfigValue::F64(burst),
            &format!("{at_prefix}.rate_limiter.burst_capacity"),
        )?;
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
            KnobType::F64 { .. } => env_var::<f64>(&name)
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
}
