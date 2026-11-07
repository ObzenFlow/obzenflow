// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::circuit_breaker::{CircuitBreaker, CircuitBreakerFailureMode, Retry};
use super::rate_limiter::RateLimiter;
use super::resilience::{resolve_breaker, resolve_retry, ControlConfigurationError};
use obzenflow_runtime::runtime_config::{
    ConfigValue, ExactConfigView, Resolved, CIRCUIT_BREAKER_CONSECUTIVE_FAILURES_KEY,
    CIRCUIT_BREAKER_COUNT_WINDOW_KEY, CIRCUIT_BREAKER_FAILURE_RATE_THRESHOLD_KEY,
    CIRCUIT_BREAKER_MINIMUM_CALLS_KEY, CIRCUIT_BREAKER_MODE_KEY, CIRCUIT_BREAKER_OPEN_FOR_MS_KEY,
    CIRCUIT_BREAKER_PROBES_KEY, CIRCUIT_BREAKER_RATE_LIMITED_COUNTS_AS_FAILURE_KEY,
    CIRCUIT_BREAKER_SLOW_CALL_DURATION_MS_KEY, CIRCUIT_BREAKER_SLOW_CALL_RATE_THRESHOLD_KEY,
    RATE_LIMITER_BURST_CAPACITY_KEY, RATE_LIMITER_COST_PER_ATTEMPT_KEY,
    RATE_LIMITER_EVENTS_PER_SECOND_KEY, RETRY_ATTEMPT_START_WINDOW_MS_KEY,
    RETRY_FIXED_DELAY_MS_KEY, RETRY_KIND_KEY, RETRY_MAX_ATTEMPTS_KEY, RETRY_MAX_BACKOFF_MS_KEY,
};
use obzenflow_runtime::stages::common::control_strategies::BackoffStrategy;

/// One consumed setting with the text shown for it. An automatic setting has no resolved value.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq)]
pub enum BuiltinSettingRow<'a> {
    Active {
        setting: &'a Resolved<ConfigValue>,
        display: String,
    },
    Inactive {
        setting: &'a Resolved<ConfigValue>,
        display: String,
    },
    Automatic {
        display: String,
    },
}

/// The rows of one built-in family and the summary shown beside its canvas icon.
#[doc(hidden)]
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BuiltinControlDescription<'a> {
    pub settings: Vec<(&'static str, BuiltinSettingRow<'a>)>,
    pub summary: Option<String>,
}

#[derive(Clone, Copy)]
enum Activity {
    Active,
    Inactive,
}

fn row<'a>(
    view: &ExactConfigView<'a>,
    key: &'static str,
    activity: Activity,
    unit: &str,
) -> Option<(&'static str, BuiltinSettingRow<'a>)> {
    let setting = view.get(key)?;
    let value = match &setting.value {
        ConfigValue::Bool(value) => value.to_string(),
        ConfigValue::U64(value) => value.to_string(),
        ConfigValue::F64(value) => value.to_string(),
        ConfigValue::Text(value) => value.clone(),
    };
    let display = format!("{value}{unit}");
    Some(match activity {
        Activity::Active => (key, BuiltinSettingRow::Active { setting, display }),
        Activity::Inactive => (key, BuiltinSettingRow::Inactive { setting, display }),
    })
}

fn labelled<'a>(
    view: &ExactConfigView<'a>,
    key: &'static str,
    label: &str,
) -> Option<(&'static str, BuiltinSettingRow<'a>)> {
    let setting = view.get(key)?;
    Some((
        key,
        BuiltinSettingRow::Active {
            setting,
            display: label.to_owned(),
        },
    ))
}

pub(in crate::middleware::control) fn describe_limiter<'a>(
    value: &RateLimiter,
    view: &ExactConfigView<'a>,
) -> Result<BuiltinControlDescription<'a>, ControlConfigurationError> {
    let resolved = value.resolved_config(view)?;
    let burst = row(
        view,
        RATE_LIMITER_BURST_CAPACITY_KEY,
        Activity::Active,
        " tokens",
    )
    .unwrap_or((
        RATE_LIMITER_BURST_CAPACITY_KEY,
        BuiltinSettingRow::Automatic {
            display: "Automatic".to_owned(),
        },
    ));
    let settings = [
        row(
            view,
            RATE_LIMITER_EVENTS_PER_SECOND_KEY,
            Activity::Active,
            " events/s",
        ),
        row(
            view,
            RATE_LIMITER_COST_PER_ATTEMPT_KEY,
            Activity::Active,
            " tokens",
        ),
        Some(burst),
    ];
    Ok(BuiltinControlDescription {
        settings: settings.into_iter().flatten().collect(),
        summary: Some(resolved.events_per_second.to_string()),
    })
}

pub(in crate::middleware::control) fn describe_breaker<'a>(
    value: &CircuitBreaker,
    view: &ExactConfigView<'a>,
) -> Result<BuiltinControlDescription<'a>, ControlConfigurationError> {
    let resolved = resolve_breaker(value, view)?;
    let (mode, consecutive, rate_based, summary) = match &resolved.config.failure_mode {
        CircuitBreakerFailureMode::Consecutive { max_failures } => (
            "Consecutive failures",
            Activity::Active,
            Activity::Inactive,
            format!("{max_failures} consecutive failures"),
        ),
        CircuitBreakerFailureMode::RateBased {
            failure_rate_threshold,
            slow_call_rate_threshold,
            ..
        } => {
            let triggers = [
                failure_rate_threshold
                    .map(|threshold| format!("{}% failures", threshold.get() * 100.0)),
                slow_call_rate_threshold
                    .map(|threshold| format!("{}% slow calls", threshold.get() * 100.0)),
            ];
            (
                "Rate based",
                Activity::Inactive,
                Activity::Active,
                triggers
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>()
                    .join("; "),
            )
        }
    };
    let settings = [
        labelled(view, CIRCUIT_BREAKER_MODE_KEY, mode),
        row(
            view,
            CIRCUIT_BREAKER_OPEN_FOR_MS_KEY,
            Activity::Active,
            " ms",
        ),
        row(view, CIRCUIT_BREAKER_PROBES_KEY, Activity::Active, ""),
        row(
            view,
            CIRCUIT_BREAKER_RATE_LIMITED_COUNTS_AS_FAILURE_KEY,
            Activity::Active,
            "",
        ),
        row(
            view,
            CIRCUIT_BREAKER_CONSECUTIVE_FAILURES_KEY,
            consecutive,
            "",
        ),
        row(view, CIRCUIT_BREAKER_COUNT_WINDOW_KEY, rate_based, " calls"),
        row(
            view,
            CIRCUIT_BREAKER_MINIMUM_CALLS_KEY,
            rate_based,
            " calls",
        ),
        row(
            view,
            CIRCUIT_BREAKER_FAILURE_RATE_THRESHOLD_KEY,
            rate_based,
            " ratio",
        ),
        row(
            view,
            CIRCUIT_BREAKER_SLOW_CALL_DURATION_MS_KEY,
            rate_based,
            " ms",
        ),
        row(
            view,
            CIRCUIT_BREAKER_SLOW_CALL_RATE_THRESHOLD_KEY,
            rate_based,
            " ratio",
        ),
    ];
    Ok(BuiltinControlDescription {
        settings: settings.into_iter().flatten().collect(),
        summary: Some(summary),
    })
}

pub(in crate::middleware::control) fn describe_retry<'a>(
    value: &Retry,
    view: &ExactConfigView<'a>,
) -> Result<BuiltinControlDescription<'a>, ControlConfigurationError> {
    let resolved = resolve_retry(value, view)?;
    let (kind, fixed) = match resolved.policy.backoff {
        BackoffStrategy::Fixed { .. } => ("Fixed", Activity::Active),
        BackoffStrategy::Exponential { .. } => ("Exponential", Activity::Inactive),
    };
    let settings = [
        labelled(view, RETRY_KIND_KEY, kind),
        row(view, RETRY_MAX_ATTEMPTS_KEY, Activity::Active, ""),
        row(view, RETRY_MAX_BACKOFF_MS_KEY, Activity::Active, " ms"),
        row(
            view,
            RETRY_ATTEMPT_START_WINDOW_MS_KEY,
            Activity::Active,
            " ms",
        ),
        row(view, RETRY_FIXED_DELAY_MS_KEY, fixed, " ms"),
    ];
    Ok(BuiltinControlDescription {
        settings: settings.into_iter().flatten().collect(),
        summary: None,
    })
}
