// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Surface-neutral rate-limiter admission core (FLOWIP-115d).
//!
//! `RateLimiterCore` owns the token bucket, admission counters, deadline
//! calculation, summary/pulse window state, and snapshot projection for one
//! protected unit. It is the reusable primitive shared by the source, effect,
//! sink-delivery, and ingress adapters.
//!
//! This file is deliberately free of `ChainEvent`, `MiddlewareContext`,
//! `MiddlewareExecutionScope`, source/effect/sink policy traits, ingress
//! boundary traits, HTTP DTOs, journalling types, and protocol response DTOs
//! (FLOWIP-115d AC20). It returns plain data; the adapters turn that data into
//! lifecycle/control facts.

use std::sync::Mutex;
use std::time::Duration;
use tokio::time::Instant;
use tracing::{info, trace};

use super::config::ValidatedRateLimiterConfig;

pub(crate) const ACTIVITY_PULSE_WINDOW_MS: u64 = 1000;
pub(crate) const ACTIVITY_PULSE_WINDOW: Duration = Duration::from_secs(1);

const MODE_ENTER_THRESHOLD_PCT: f64 = 100.0;
const MODE_EXIT_THRESHOLD_PCT: f64 = 80.0;
const MODE_EXIT_HOLD_WINDOWS: u8 = 2;

/// Token bucket rate limiter implementation.
///
/// All time-dependent operations take an explicit `now: Instant` so the
/// non-waiting `try_admit_at` primitive is deterministic under tests and the
/// ingress fast path (FLOWIP-115d). Production callers pass `Instant::now()`.
#[derive(Debug)]
pub(crate) struct TokenBucket {
    /// Maximum tokens (burst capacity)
    pub(crate) capacity: f64,
    /// Current tokens available
    pub(crate) tokens: f64,
    /// Tokens added per second
    refill_rate: f64,
    /// Last time tokens were refilled
    last_refill: Instant,
}

impl TokenBucket {
    fn new(capacity: f64, refill_rate: f64, now: Instant) -> Self {
        Self {
            capacity,
            tokens: capacity, // Start full
            refill_rate,
            last_refill: now,
        }
    }

    /// Try to consume tokens, returns true if successful.
    fn try_consume(&mut self, tokens: f64, now: Instant) -> bool {
        self.refill(now);

        trace!(
            "try_consume: requested={}, available={}, capacity={}",
            tokens,
            self.tokens,
            self.capacity
        );

        if self.tokens >= tokens {
            self.tokens -= tokens;
            trace!("try_consume: SUCCESS, remaining tokens={}", self.tokens);
            true
        } else {
            trace!("try_consume: FAILED, insufficient tokens");
            false
        }
    }

    /// Get time until enough tokens are available.
    fn time_until_available(&mut self, tokens: f64, now: Instant) -> Option<Duration> {
        self.refill(now);

        if self.tokens >= tokens {
            return None; // Already available
        }

        let needed = tokens - self.tokens;
        let seconds_needed = needed / self.refill_rate;
        Some(Duration::from_secs_f64(seconds_needed))
    }

    /// Refill tokens based on elapsed time.
    fn refill(&mut self, now: Instant) {
        let elapsed = now.duration_since(self.last_refill);

        let tokens_to_add = elapsed.as_secs_f64() * self.refill_rate;
        let old_tokens = self.tokens;
        self.tokens = (self.tokens + tokens_to_add).min(self.capacity);
        self.last_refill = now;

        if tokens_to_add > 0.0 {
            trace!(
                "refill: elapsed={:?}, added={}, old={}, new={}",
                elapsed,
                tokens_to_add,
                old_tokens,
                self.tokens
            );
        }
    }

    /// Get current token count (for monitoring).
    fn available_tokens(&mut self, now: Instant) -> f64 {
        self.refill(now);
        self.tokens
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RateLimiterMode {
    Normal,
    Limiting,
}

impl RateLimiterMode {
    fn as_str(&self) -> &'static str {
        match self {
            RateLimiterMode::Normal => "normal",
            RateLimiterMode::Limiting => "limiting",
        }
    }
}

#[derive(Debug)]
pub(crate) struct RateLimiterStats {
    // ---- Cumulative counters (never reset) ----
    pub(crate) events_total: u64,
    pub(crate) delayed_total: u64,
    pub(crate) tokens_consumed_total: f64,
    pub(crate) delay_seconds_total: f64,

    // ---- Window counters (reset on summary emission) ----
    pub(crate) events_window: u64,
    pub(crate) delayed_window: u64,
    pub(crate) tokens_consumed_window: f64,
    pub(crate) last_summary: Instant,

    pub(crate) mode: RateLimiterMode,
    pub(crate) exit_hold_count: u8,

    pub(crate) pulse_window_start: Instant,
    pub(crate) pulse_delayed_events: u64,
    pub(crate) pulse_delay_ms_total: u64,
    pub(crate) pulse_delay_ms_max: u64,
}

impl RateLimiterStats {
    fn new(now: Instant) -> Self {
        Self {
            events_total: 0,
            delayed_total: 0,
            tokens_consumed_total: 0.0,
            delay_seconds_total: 0.0,
            events_window: 0,
            delayed_window: 0,
            tokens_consumed_window: 0.0,
            last_summary: now,
            mode: RateLimiterMode::Normal,
            exit_hold_count: 0,
            pulse_window_start: now,
            pulse_delayed_events: 0,
            pulse_delay_ms_total: 0,
            pulse_delay_ms_max: 0,
        }
    }
}

/// Non-waiting admission decision returned by [`RateLimiterCore::try_admit_at`].
#[derive(Debug, Clone, Copy)]
pub(crate) enum AdmissionDecision {
    /// A token was consumed; the caller must call
    /// [`RateLimiterCore::record_admitted`] to account for it.
    Admitted,
    /// No token was available. `retry_after` is the deadline from `now`. A
    /// waiting-capable surface derives its absolute wake instant as
    /// `now + retry_after`; the fail-fast ingress surface maps this to a
    /// protocol refusal.
    WouldWait { retry_after: Duration },
}

/// Plain-data inputs for the one-shot "delayed" lifecycle fact (FLOWIP-115d).
#[derive(Debug, Clone, Copy)]
pub(crate) struct RateLimitDelayEvent {
    pub(crate) delay_ms: u64,
    pub(crate) current_rate: f64,
    pub(crate) limit_rate: f64,
}

/// Plain-data summary returned by [`RateLimiterCore::take_due_summary`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct RateLimitSummary {
    pub(crate) utilization_percent: f64,
    pub(crate) events_in_window: u64,
    pub(crate) window_size_ms: u64,
    /// `Some((from, to))` stable-label mode transition when hysteresis tripped.
    pub(crate) mode_change: Option<(&'static str, &'static str)>,
}

/// Plain-data activity pulse returned by [`RateLimiterCore::take_due_pulse`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct RateLimitPulse {
    pub(crate) window_ms: u64,
    pub(crate) delayed_events: u64,
    pub(crate) delay_ms_total: u64,
    pub(crate) delay_ms_max: u64,
}

/// Read-only snapshot of the admission counters and bucket state, projected by
/// the control-plane snapshotter into runtime metrics.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RateLimiterCoreSnapshot {
    pub(crate) events_total: u64,
    pub(crate) delayed_total: u64,
    pub(crate) tokens_consumed_total: f64,
    pub(crate) delay_seconds_total: f64,
    pub(crate) bucket_tokens: f64,
    pub(crate) bucket_capacity: f64,
}

/// Surface-neutral token-bucket admission core for one protected unit.
#[derive(Debug)]
pub(crate) struct RateLimiterCore {
    bucket: Mutex<TokenBucket>,
    stats: Mutex<RateLimiterStats>,
    events_per_second: f64,
    cost_per_event: f64,
    limit_rate: f64,
}

impl RateLimiterCore {
    pub(crate) fn new(config: ValidatedRateLimiterConfig) -> Self {
        let now = Instant::now();
        Self {
            bucket: Mutex::new(TokenBucket::new(
                config.burst_capacity,
                config.events_per_second,
                now,
            )),
            stats: Mutex::new(RateLimiterStats::new(now)),
            events_per_second: config.events_per_second,
            cost_per_event: config.cost_per_event,
            limit_rate: config.limit_rate(),
        }
    }

    pub(crate) fn cost_per_event(&self) -> f64 {
        self.cost_per_event
    }

    pub(crate) fn limit_rate(&self) -> f64 {
        self.limit_rate
    }

    /// Non-waiting admission. Consumes one charge of `cost` when a token is
    /// available (returning [`AdmissionDecision::Admitted`]); otherwise returns
    /// the wait deadline without consuming. This is the ingress fast path and a
    /// unit-testable primitive, not the whole waiting contract.
    pub(crate) fn try_admit_at(&self, cost: f64, now: Instant) -> AdmissionDecision {
        let mut bucket = self.bucket.lock().unwrap();
        if bucket.try_consume(cost, now) {
            AdmissionDecision::Admitted
        } else {
            let retry_after = bucket
                .time_until_available(cost, now)
                .unwrap_or(Duration::from_millis(10));
            AdmissionDecision::WouldWait { retry_after }
        }
    }

    /// Account for one admitted charge (FLOWIP-114m: at admission, not output).
    pub(crate) fn record_admitted(&self, cost: f64) {
        if let Ok(mut stats) = self.stats.lock() {
            stats.events_total += 1;
            stats.tokens_consumed_total += cost;
            stats.events_window += 1;
            stats.tokens_consumed_window += cost;
        }
    }

    /// Record that this acquire was delayed for the first time, returning the
    /// current consumption rate used by the one-shot `Delayed` fact. Bumps the
    /// delayed counters exactly once per logical acquire.
    pub(crate) fn note_first_delay(&self) -> f64 {
        if let Ok(mut stats) = self.stats.lock() {
            stats.delayed_total += 1;
            stats.delayed_window += 1;
            stats.pulse_delayed_events += 1;

            let elapsed_s = stats.last_summary.elapsed().as_secs_f64();
            if elapsed_s > 0.0 {
                stats.tokens_consumed_window / elapsed_s / self.cost_per_event
            } else {
                0.0
            }
        } else {
            0.0
        }
    }

    /// Record observed wait time after a sleep on the blocked path.
    pub(crate) fn record_waited(&self, waited: Duration) {
        if let Ok(mut stats) = self.stats.lock() {
            stats.delay_seconds_total += waited.as_secs_f64();
            let waited_ms = waited.as_millis() as u64;
            stats.pulse_delay_ms_total = stats.pulse_delay_ms_total.saturating_add(waited_ms);
            stats.pulse_delay_ms_max = stats.pulse_delay_ms_max.max(waited_ms);
        }
    }

    pub(crate) fn snapshot(&self) -> RateLimiterCoreSnapshot {
        let (events_total, delayed_total, tokens_consumed_total, delay_seconds_total) =
            if let Ok(stats) = self.stats.lock() {
                (
                    stats.events_total,
                    stats.delayed_total,
                    stats.tokens_consumed_total,
                    stats.delay_seconds_total,
                )
            } else {
                (0, 0, 0.0, 0.0)
            };

        let (bucket_tokens, bucket_capacity) = if let Ok(mut bucket) = self.bucket.lock() {
            (bucket.available_tokens(Instant::now()), bucket.capacity)
        } else {
            (0.0, 0.0)
        };

        RateLimiterCoreSnapshot {
            events_total,
            delayed_total,
            tokens_consumed_total,
            delay_seconds_total,
            bucket_tokens,
            bucket_capacity,
        }
    }

    /// Emit an activity pulse if the pulse window has elapsed, resetting the
    /// pulse accumulators. Returns `None` when no pulse is due or there were no
    /// delayed events in the window.
    pub(crate) fn take_due_pulse(&self, now: Instant) -> Option<RateLimitPulse> {
        let mut stats = self.stats.lock().ok()?;
        let elapsed = now.duration_since(stats.pulse_window_start);
        if elapsed < ACTIVITY_PULSE_WINDOW {
            return None;
        }

        let delayed_events = stats.pulse_delayed_events;
        let delay_ms_total = stats.pulse_delay_ms_total;
        let delay_ms_max = stats.pulse_delay_ms_max;

        stats.pulse_delayed_events = 0;
        stats.pulse_delay_ms_total = 0;
        stats.pulse_delay_ms_max = 0;
        stats.pulse_window_start = now;

        if delayed_events == 0 {
            return None;
        }

        Some(RateLimitPulse {
            window_ms: ACTIVITY_PULSE_WINDOW_MS,
            delayed_events,
            delay_ms_total,
            delay_ms_max,
        })
    }

    /// Emit a window summary if the summary window has elapsed, applying mode
    /// hysteresis and resetting the window counters. Returns `None` when no
    /// summary is due.
    pub(crate) fn take_due_summary(&self, now: Instant) -> Option<RateLimitSummary> {
        let mut stats = self.stats.lock().ok()?;

        // Emit a summary every 10 seconds or every 1000 processed events.
        let window_duration = now.duration_since(stats.last_summary);
        let should_emit = window_duration >= Duration::from_secs(10) || stats.events_window >= 1000;
        if !should_emit {
            return None;
        }

        let elapsed_s = window_duration.as_secs_f64();

        let consumption_rate_tokens_per_s = if elapsed_s > 0.0 {
            stats.tokens_consumed_window / elapsed_s
        } else {
            0.0
        };

        let utilization_pct = if self.events_per_second > 0.0 {
            (consumption_rate_tokens_per_s / self.events_per_second) * 100.0
        } else {
            0.0
        };

        info!(
            window_duration_s = window_duration.as_secs(),
            requests_allowed = stats.events_window,
            requests_delayed = stats.delayed_window,
            tokens_consumed = stats.tokens_consumed_window,
            consumption_rate_tokens_per_s,
            utilization_pct = format!("{:.1}%", utilization_pct),
            "Rate limiter summary"
        );

        let events_in_window = stats.events_window;

        let mut mode_change: Option<(&'static str, &'static str)> = None;
        match stats.mode {
            RateLimiterMode::Normal => {
                if utilization_pct >= MODE_ENTER_THRESHOLD_PCT {
                    let from = stats.mode;
                    stats.mode = RateLimiterMode::Limiting;
                    stats.exit_hold_count = 0;
                    mode_change = Some((from.as_str(), stats.mode.as_str()));
                }
            }
            RateLimiterMode::Limiting => {
                if utilization_pct <= MODE_EXIT_THRESHOLD_PCT {
                    let next = stats.exit_hold_count.saturating_add(1);
                    stats.exit_hold_count = next;
                    let required = MODE_EXIT_HOLD_WINDOWS.max(1);
                    if next >= required {
                        let from = stats.mode;
                        stats.mode = RateLimiterMode::Normal;
                        stats.exit_hold_count = 0;
                        mode_change = Some((from.as_str(), stats.mode.as_str()));
                    }
                } else {
                    stats.exit_hold_count = 0;
                }
            }
        }

        let window_size_ms = window_duration.as_millis() as u64;

        // Reset window counters.
        stats.events_window = 0;
        stats.delayed_window = 0;
        stats.tokens_consumed_window = 0.0;
        stats.last_summary = now;

        Some(RateLimitSummary {
            utilization_percent: utilization_pct,
            events_in_window,
            window_size_ms,
            mode_change,
        })
    }

    /// Test-only access to the internal bucket for invariants such as
    /// "no lock held across await" and effective-capacity checks.
    #[cfg(test)]
    pub(crate) fn bucket_for_test(&self) -> &Mutex<TokenBucket> {
        &self.bucket
    }

    /// Test-only access to the internal stats for window/mode assertions.
    #[cfg(test)]
    pub(crate) fn stats_for_test(&self) -> &Mutex<RateLimiterStats> {
        &self.stats
    }
}

/// Shared cancellable async acquisition helper (FLOWIP-114o / FLOWIP-115d).
///
/// Both the effect-boundary and source-boundary paths (and any future
/// waiting-capable surface) delegate here, so the live I/O surfaces share one
/// accounting and lifecycle implementation. It awaits its permit with
/// `tokio::time::sleep` (a cancellable future) and returns once a token has
/// been consumed.
///
/// Invariants: it holds no bucket, stats, or lifecycle lock across `await`; it
/// emits the one-shot `Delayed` fact through `on_first_delay` before the first
/// sleep; and if the future is dropped before admission completes, no token is
/// consumed after the drop and no terminal accounting beyond the already-emitted
/// delay fact occurs.
pub(crate) async fn acquire_admission<F>(core: &RateLimiterCore, mut on_first_delay: F)
where
    F: FnMut(RateLimitDelayEvent),
{
    let cost = core.cost_per_event;
    let mut delayed_this_acquire = false;
    loop {
        match core.try_admit_at(cost, Instant::now()) {
            AdmissionDecision::Admitted => {
                core.record_admitted(cost);
                return;
            }
            AdmissionDecision::WouldWait { retry_after } => {
                if !delayed_this_acquire {
                    delayed_this_acquire = true;
                    let current_rate = core.note_first_delay();
                    on_first_delay(RateLimitDelayEvent {
                        delay_ms: retry_after.as_millis() as u64,
                        current_rate,
                        limit_rate: core.limit_rate,
                    });
                }

                let wait_start = Instant::now();
                tokio::time::sleep(retry_after).await;
                core.record_waited(wait_start.elapsed());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_bucket_basic() {
        let mut bucket = TokenBucket::new(10.0, 5.0, Instant::now()); // 10 capacity, 5/sec refill

        // Should start full
        assert!(bucket.try_consume(5.0, Instant::now()));
        assert!(bucket.try_consume(5.0, Instant::now()));
        assert!(!bucket.try_consume(5.0, Instant::now())); // Should fail

        // Wait a bit and check refill
        std::thread::sleep(Duration::from_millis(200)); // 0.2 sec = 1 token
        assert!(bucket.try_consume(1.0, Instant::now())); // Should succeed
        assert!(!bucket.try_consume(1.0, Instant::now())); // Should fail again
    }

    #[test]
    fn test_token_bucket_time_until_available() {
        let mut bucket = TokenBucket::new(10.0, 2.0, Instant::now()); // 10 capacity, 2/sec refill

        // Consume all tokens
        assert!(bucket.try_consume(10.0, Instant::now()));

        // Should need 2.5 seconds to get 5 tokens
        let wait = bucket.time_until_available(5.0, Instant::now()).unwrap();
        assert!((wait.as_secs_f64() - 2.5).abs() < 0.1);
    }
}
