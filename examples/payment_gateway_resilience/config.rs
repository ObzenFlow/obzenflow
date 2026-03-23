// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use anyhow::Result;
use obzenflow::env::{env_bool_or, env_var, env_var_or};

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
    pub fn from_env() -> Result<Self> {
        let mut total_events = env_var::<usize>("PAYMENT_GATEWAY_TOTAL_EVENTS")?;
        let duration_secs = env_var::<usize>("PAYMENT_GATEWAY_DURATION_SECS")?;
        let mut rate_limit_events_per_sec = env_var::<f64>("PAYMENT_GATEWAY_RATE_LIMIT")?;
        let mut glitchy_reason: Option<&'static str> = None;

        match (total_events, duration_secs) {
            (Some(_), _) => {
                glitchy_reason = Some("PAYMENT_GATEWAY_TOTAL_EVENTS");
                rate_limit_events_per_sec.get_or_insert(1000.0);
            }
            (None, Some(duration_secs)) => {
                glitchy_reason = Some("PAYMENT_GATEWAY_DURATION_SECS");
                let rate = rate_limit_events_per_sec.unwrap_or(200.0);
                rate_limit_events_per_sec = Some(rate);
                total_events = Some(((duration_secs as f64) * rate).round().max(1.0) as usize);
            }
            (None, None) => {}
        }

        let rate_limit_events_per_sec = rate_limit_events_per_sec.unwrap_or(1.0).max(0.1);
        let warmup_events = env_var_or::<usize>("PAYMENT_GATEWAY_WARMUP_EVENTS", 8_000)?;
        let outage_events = env_var_or::<usize>("PAYMENT_GATEWAY_OUTAGE_EVENTS", 1_000)?;
        let recovery_events = env_var_or::<usize>("PAYMENT_GATEWAY_RECOVERY_EVENTS", 1_000)?;
        let summary_progress_every = env_var_or::<usize>("PAYMENT_GATEWAY_PROGRESS_EVERY", 5_000)?;
        let use_async_source = env_bool_or("PAYMENT_GATEWAY_ASYNC_SOURCE", false)?;
        let async_poll_timeout =
            match env_var_or::<usize>("PAYMENT_GATEWAY_ASYNC_POLL_TIMEOUT_SECS", 30)? {
                0 => None,
                secs => Some(std::time::Duration::from_secs(secs as u64)),
            };

        Ok(Self {
            total_events,
            rate_limit_events_per_sec,
            warmup_events,
            outage_events,
            recovery_events,
            summary_progress_every,
            use_async_source,
            async_poll_timeout,
            glitchy_reason,
        })
    }
}
