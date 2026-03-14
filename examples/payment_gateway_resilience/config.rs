// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

fn env_usize(key: &str) -> Option<usize> {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
}

fn env_f64(key: &str) -> Option<f64> {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
}

fn env_bool(key: &str) -> Option<bool> {
    let raw = std::env::var(key).ok()?;
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "y" | "on" => Some(true),
        "0" | "false" | "no" | "n" | "off" => Some(false),
        _ => None,
    }
}

pub struct DemoConfig {
    pub total_events: Option<usize>,
    pub rate_limit_events_per_sec: f64,
    pub warmup_events: usize,
    pub outage_events: usize,
    pub recovery_events: usize,
    pub summary_progress_every: usize,
    pub use_async_source: bool,
    pub async_poll_timeout: Option<std::time::Duration>,
    pub glitchy_reason: Option<&'static str>,
}

impl DemoConfig {
    pub fn from_env() -> Self {
        let server_mode_requested = std::env::args().any(|arg| arg == "--server");

        let mut total_events = env_usize("PAYMENT_GATEWAY_TOTAL_EVENTS");
        let duration_secs = env_usize("PAYMENT_GATEWAY_DURATION_SECS");
        let mut rate_limit_events_per_sec = env_f64("PAYMENT_GATEWAY_RATE_LIMIT");
        let mut glitchy_reason: Option<&'static str> = None;

        match (total_events, duration_secs, server_mode_requested) {
            (Some(_), _, _) => {
                glitchy_reason = Some("PAYMENT_GATEWAY_TOTAL_EVENTS");
                rate_limit_events_per_sec.get_or_insert(1000.0);
            }
            (None, Some(duration_secs), _) => {
                glitchy_reason = Some("PAYMENT_GATEWAY_DURATION_SECS");
                let rate = rate_limit_events_per_sec.unwrap_or(200.0);
                rate_limit_events_per_sec = Some(rate);
                total_events = Some(((duration_secs as f64) * rate).round().max(1.0) as usize);
            }
            (None, None, true) => {
                glitchy_reason = Some("--server default");
                rate_limit_events_per_sec.get_or_insert(200.0);
                total_events = Some(60_000);
            }
            (None, None, false) => {}
        }

        let rate_limit_events_per_sec = rate_limit_events_per_sec.unwrap_or(1.0).max(0.1);
        let warmup_events = env_usize("PAYMENT_GATEWAY_WARMUP_EVENTS").unwrap_or(8_000);
        let outage_events = env_usize("PAYMENT_GATEWAY_OUTAGE_EVENTS").unwrap_or(1_000);
        let recovery_events = env_usize("PAYMENT_GATEWAY_RECOVERY_EVENTS").unwrap_or(1_000);
        let summary_progress_every = env_usize("PAYMENT_GATEWAY_PROGRESS_EVERY").unwrap_or(5_000);
        let use_async_source = env_bool("PAYMENT_GATEWAY_ASYNC_SOURCE").unwrap_or(false);
        let async_poll_timeout =
            match env_usize("PAYMENT_GATEWAY_ASYNC_POLL_TIMEOUT_SECS").unwrap_or(30) {
                0 => None,
                secs => Some(std::time::Duration::from_secs(secs as u64)),
            };

        Self {
            total_events,
            rate_limit_events_per_sec,
            warmup_events,
            outage_events,
            recovery_events,
            summary_progress_every,
            use_async_source,
            async_poll_timeout,
            glitchy_reason,
        }
    }
}
