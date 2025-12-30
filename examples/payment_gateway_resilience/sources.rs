use super::domain::{PaymentCommand, TrafficPhase};
use super::fixtures;
use async_trait::async_trait;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    id::StageId,
    TypedPayload,
    WriterId,
};
use obzenflow_runtime_services::stages::common::handlers::{
    AsyncFiniteSourceHandler, FiniteSourceHandler,
};
use obzenflow_runtime_services::stages::common::handlers::source::traits::SourceError;
use serde_json::json;
use std::time::{Duration, Instant};

fn blocking_sleep(duration: Duration) {
    if duration.is_zero() {
        return;
    }

    // Source handlers are synchronous; block but avoid stalling tokio worker threads.
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| {
                std::thread::sleep(duration);
            });
        }
        _ => std::thread::sleep(duration),
    }
}

#[derive(Clone, Copy, Debug)]
struct XorShift64(u64);

impl XorShift64 {
    fn new(seed: u64) -> Self {
        // xorshift cannot have a zero state.
        let seed = if seed == 0 { 0x9E37_79B9_7F4A_7C15 } else { seed };
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn gen_range_u64(&mut self, range: std::ops::RangeInclusive<u64>) -> u64 {
        let start = *range.start();
        let end = *range.end();
        if start >= end {
            return start;
        }
        let span = end - start + 1;
        start + (self.next_u64() % span)
    }

    fn gen_bool(&mut self, probability: f64) -> bool {
        let p = probability.clamp(0.0, 1.0);
        if p <= 0.0 {
            return false;
        }
        if p >= 1.0 {
            return true;
        }
        let sample = (self.next_u64() as f64) / (u64::MAX as f64);
        sample < p
    }
}

#[derive(Clone, Debug)]
struct ExponentialBackoff {
    base: Duration,
    max: Duration,
    current: Duration,
    next_attempt_at: Option<Instant>,
}

impl ExponentialBackoff {
    fn new(base: Duration, max: Duration) -> Self {
        Self {
            base,
            max,
            current: base,
            next_attempt_at: None,
        }
    }

    fn reset(&mut self) {
        self.current = self.base;
        self.next_attempt_at = None;
    }

    fn sleep_if_needed(&mut self, now: Instant) {
        let Some(next_at) = self.next_attempt_at else {
            return;
        };
        if now >= next_at {
            self.next_attempt_at = None;
            return;
        }
        blocking_sleep(next_at.duration_since(now));
        self.next_attempt_at = None;
    }

    async fn sleep_if_needed_async(&mut self, now: Instant) {
        let Some(next_at) = self.next_attempt_at else {
            return;
        };
        if now >= next_at {
            self.next_attempt_at = None;
            return;
        }

        tokio::time::sleep(next_at.duration_since(now)).await;
        self.next_attempt_at = None;
    }

    fn schedule_next(&mut self, now: Instant, rng: &mut XorShift64) -> Duration {
        // Apply +/- 10% jitter so we don't synchronize with other probes.
        let jitter_pct = if rng.gen_bool(0.5) { 0.9 } else { 1.1 };
        let delay_ms = (self.current.as_millis() as f64 * jitter_pct)
            .round()
            .max(1.0) as u64;
        let delay_ms = delay_ms.min(self.max.as_millis().max(1) as u64);
        let delay = Duration::from_millis(delay_ms);

        self.next_attempt_at = Some(now + delay);

        // Exponential step for the next failure.
        let next_ms = (self.current.as_millis().saturating_mul(2))
            .min(self.max.as_millis())
            .max(self.base.as_millis()) as u64;
        self.current = Duration::from_millis(next_ms);

        delay
    }
}

#[derive(Clone, Debug)]
struct SemiReliableFeed {
    rng: XorShift64,
    offline_until: Option<Instant>,
    next_glitch_at: Option<Instant>,
    backoff: ExponentialBackoff,
}

impl SemiReliableFeed {
    fn new(seed: u64) -> Self {
        let mut rng = XorShift64::new(seed);
        Self {
            rng,
            offline_until: None,
            // Schedule relative to the first poll, not construction time.
            //
            // In `--startup-mode manual` server runs, the flow can be built and then
            // sit idle waiting for Play, which would otherwise "consume" the initial
            // healthy window and make the very first poll glitch immediately.
            next_glitch_at: None,
            backoff: ExponentialBackoff::new(Duration::from_secs(1), Duration::from_secs(15)),
        }
    }

    fn poll(&mut self) -> Result<(), SourceError> {
        let now = Instant::now();
        self.backoff.sleep_if_needed(now);
        let now = Instant::now();

        if self.next_glitch_at.is_none() {
            self.next_glitch_at = Some(now + Duration::from_secs(self.rng.gen_range_u64(5..=20)));
        }

        if let Some(until) = self.offline_until {
            if now < until {
                let remaining = until.duration_since(now);
                let next_delay = self.backoff.schedule_next(now, &mut self.rng);
                let err = if self.rng.gen_bool(0.7) {
                    SourceError::Timeout(format!(
                        "mqtt feed offline (recovery_in={}ms, next_attempt_in={}ms)",
                        remaining.as_millis(),
                        next_delay.as_millis()
                    ))
                } else {
                    SourceError::Transport(format!(
                        "mqtt broker unreachable (recovery_in={}ms, next_attempt_in={}ms)",
                        remaining.as_millis(),
                        next_delay.as_millis()
                    ))
                };
                return Err(err);
            }

            // Recovered.
            self.offline_until = None;
            self.backoff.reset();
            println!("📡 payments feed recovered (resuming scrape)");

            // Schedule the next glitch sometime in the future.
            self.next_glitch_at =
                Some(now + Duration::from_secs(self.rng.gen_range_u64(10..=30)));
        }

        if matches!(self.next_glitch_at, Some(next_glitch_at) if now >= next_glitch_at) {
            let outage_secs = self.rng.gen_range_u64(1..=15);
            let outage = Duration::from_secs(outage_secs);
            self.offline_until = Some(now + outage);
            self.backoff.reset();
            let _ = self.backoff.schedule_next(now, &mut self.rng);

            println!(
                "📡 payments feed glitch: offline for {}s (simulated MQTT outage)",
                outage_secs
            );

            return Err(SourceError::Transport(format!(
                "mqtt feed glitch: offline_for={}s",
                outage_secs
            )));
        }

        // Healthy.
        self.backoff.reset();
        Ok(())
    }

    async fn poll_async(&mut self) -> Result<(), SourceError> {
        let now = Instant::now();
        self.backoff.sleep_if_needed_async(now).await;
        let now = Instant::now();

        if self.next_glitch_at.is_none() {
            self.next_glitch_at = Some(now + Duration::from_secs(self.rng.gen_range_u64(5..=20)));
        }

        if let Some(until) = self.offline_until {
            if now < until {
                let remaining = until.duration_since(now);
                let next_delay = self.backoff.schedule_next(now, &mut self.rng);
                let err = if self.rng.gen_bool(0.7) {
                    SourceError::Timeout(format!(
                        "mqtt feed offline (recovery_in={}ms, next_attempt_in={}ms)",
                        remaining.as_millis(),
                        next_delay.as_millis()
                    ))
                } else {
                    SourceError::Transport(format!(
                        "mqtt broker unreachable (recovery_in={}ms, next_attempt_in={}ms)",
                        remaining.as_millis(),
                        next_delay.as_millis()
                    ))
                };
                return Err(err);
            }

            // Recovered.
            self.offline_until = None;
            self.backoff.reset();
            println!("📡 payments feed recovered (resuming scrape)");

            // Schedule the next glitch sometime in the future.
            self.next_glitch_at =
                Some(now + Duration::from_secs(self.rng.gen_range_u64(10..=30)));
        }

        if matches!(self.next_glitch_at, Some(next_glitch_at) if now >= next_glitch_at) {
            let outage_secs = self.rng.gen_range_u64(1..=15);
            let outage = Duration::from_secs(outage_secs);
            self.offline_until = Some(now + outage);
            self.backoff.reset();
            let _ = self.backoff.schedule_next(now, &mut self.rng);

            println!(
                "📡 payments feed glitch: offline for {}s (simulated MQTT outage)",
                outage_secs
            );

            return Err(SourceError::Transport(format!(
                "mqtt feed glitch: offline_for={}s",
                outage_secs
            )));
        }

        // Healthy.
        self.backoff.reset();
        Ok(())
    }
}

/// Finite source that replays a scripted sequence of payment commands.
///
/// This models an upstream order system sending us payment work. The
/// source is intentionally small and deterministic so you can line up
/// logs, journal events, and Prometheus metrics.
#[derive(Clone, Debug)]
pub struct PaymentCommandSource {
    commands: Vec<PaymentCommand>,
    current_index: usize,
    writer_id: WriterId,
}

impl PaymentCommandSource {
    pub fn new() -> Self {
        Self {
            commands: fixtures::scripted_commands(),
            current_index: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for PaymentCommandSource {
    fn next(
        &mut self,
    ) -> Result<
        Option<Vec<ChainEvent>>,
        obzenflow_runtime_services::stages::common::handlers::source::traits::SourceError,
    > {
        if self.current_index >= self.commands.len() {
            return Ok(None);
        }

        let cmd = &self.commands[self.current_index];
        self.current_index += 1;

        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id.clone(),
            PaymentCommand::EVENT_TYPE,
            json!(cmd),
        )]))
    }
}

/// High-volume source that generates a large number of payment commands with
/// periodic "glitch" windows (Outage) to exercise circuit breaker behaviour.
///
/// Intended for runs similar to `prometheus_100k_demo` where you want:
/// - enough volume to make rates interesting
/// - repeated breaker open/close cycles over time
#[derive(Clone, Debug)]
pub struct GlitchyPaymentCommandSource {
    total_events: usize,
    current_index: usize,
    writer_id: WriterId,
    warmup_events: usize,
    outage_events: usize,
    recovery_events: usize,
}

impl GlitchyPaymentCommandSource {
    /// Create a new glitchy source with a default 10k-event cycle:
    /// - 8k healthy (Warmup)
    /// - 1k outage (Outage)
    /// - 1k healthy (Recovery)
    ///
    /// At 1000 events/sec (like `prometheus_100k_demo`) this yields a ~10s cycle
    /// that lines up well with the circuit breaker's default 5s cooldown.
    pub fn new(total_events: usize) -> Self {
        Self::with_cycle(total_events, 8_000, 1_000, 1_000)
    }

    pub fn with_cycle(
        total_events: usize,
        warmup_events: usize,
        outage_events: usize,
        recovery_events: usize,
    ) -> Self {
        Self {
            total_events,
            current_index: 0,
            writer_id: WriterId::from(StageId::new()),
            warmup_events: warmup_events.max(1),
            outage_events,
            recovery_events,
        }
    }

    fn phase_for_index(&self, index: usize) -> TrafficPhase {
        let cycle_len = self.warmup_events + self.outage_events + self.recovery_events;
        if cycle_len == 0 {
            return TrafficPhase::Warmup;
        }

        let pos = index % cycle_len;
        if pos < self.warmup_events {
            TrafficPhase::Warmup
        } else if pos < self.warmup_events + self.outage_events {
            TrafficPhase::Outage
        } else {
            TrafficPhase::Recovery
        }
    }
}

impl FiniteSourceHandler for GlitchyPaymentCommandSource {
    fn next(
        &mut self,
    ) -> Result<
        Option<Vec<ChainEvent>>,
        obzenflow_runtime_services::stages::common::handlers::source::traits::SourceError,
    > {
        if self.current_index >= self.total_events {
            return Ok(None);
        }

        let idx = self.current_index;
        self.current_index += 1;

        if self.current_index % 10_000 == 0 {
            println!("📊 Generated {} payment commands...", self.current_index);
        }

        let phase = self.phase_for_index(idx);

        // Inject a small number of validation failures so the demo can show:
        // - Validation errors contribute to `obzenflow_errors_total`
        // - Validation errors do NOT open the circuit breaker (ErrorKind::Validation)
        let (card_ok, amount_cents) = match idx % 1_000 {
            0 => (false, 10_00), // bad card
            1 => (true, 0),      // zero amount
            _ => (true, 10_00),
        };

        let cmd = PaymentCommand {
            request_id: format!("cmd-{}", idx),
            customer_id: format!("cust-{}", idx % 10_000),
            amount_cents,
            card_ok,
            phase,
        };

        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id.clone(),
            PaymentCommand::EVENT_TYPE,
            json!(cmd),
        )]))
    }
}

/// High-volume source that simulates scraping from a semi-reliable upstream feed.
///
/// Think: MQTT / IoT ingestion where:
/// - The feed occasionally goes offline for a variable amount of time (1–15s).
/// - We don't need perfect replay; we simply back off and resume once it's back.
/// - A circuit breaker on the source prevents hot-looping and makes outages
///   scrape-visible via `obzenflow_circuit_breaker_*` metrics on the source stage.
#[derive(Clone, Debug)]
pub struct ScrapedGlitchyPaymentCommandSource {
    total_events: usize,
    current_index: usize,
    writer_id: WriterId,
    warmup_events: usize,
    outage_events: usize,
    recovery_events: usize,
    feed: SemiReliableFeed,
}

impl ScrapedGlitchyPaymentCommandSource {
    pub fn with_cycle(
        total_events: usize,
        warmup_events: usize,
        outage_events: usize,
        recovery_events: usize,
    ) -> Self {
        // Stable-ish seed so runs are reproducible enough for demos while
        // still producing "random-ish" outage durations.
        let seed = (total_events as u64)
            .wrapping_mul(31)
            .wrapping_add(warmup_events as u64)
            .wrapping_add(outage_events as u64)
            .wrapping_add(recovery_events as u64);

        Self {
            total_events,
            current_index: 0,
            writer_id: WriterId::from(StageId::new()),
            warmup_events: warmup_events.max(1),
            outage_events,
            recovery_events,
            feed: SemiReliableFeed::new(seed),
        }
    }

    fn phase_for_index(&self, index: usize) -> TrafficPhase {
        let cycle_len = self.warmup_events + self.outage_events + self.recovery_events;
        if cycle_len == 0 {
            return TrafficPhase::Warmup;
        }

        let pos = index % cycle_len;
        if pos < self.warmup_events {
            TrafficPhase::Warmup
        } else if pos < self.warmup_events + self.outage_events {
            TrafficPhase::Outage
        } else {
            TrafficPhase::Recovery
        }
    }
}

impl FiniteSourceHandler for ScrapedGlitchyPaymentCommandSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.current_index >= self.total_events {
            return Ok(None);
        }

        // Simulate scraping/polling an external feed that can glitch.
        // Errors are surfaced as SourceError so the source's circuit breaker
        // can observe them and open, while the middleware wrapper converts
        // them into error-marked lifecycle events (non-fatal to the stage).
        self.feed.poll()?;

        let idx = self.current_index;
        self.current_index += 1;

        if self.current_index % 10_000 == 0 {
            println!("📊 Scraped {} payment commands...", self.current_index);
        }

        let phase = self.phase_for_index(idx);

        let (card_ok, amount_cents) = match idx % 1_000 {
            0 => (false, 10_00), // bad card
            1 => (true, 0),      // zero amount
            _ => (true, 10_00),
        };

        let cmd = PaymentCommand {
            request_id: format!("cmd-{}", idx),
            customer_id: format!("cust-{}", idx % 10_000),
            amount_cents,
            card_ok,
            phase,
        };

        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id.clone(),
            PaymentCommand::EVENT_TYPE,
            json!(cmd),
        )]))
    }
}

/// Async version of ScrapedGlitchyPaymentCommandSource, demonstrating FLOWIP-086f.
///
/// Key difference vs the sync source:
/// - Backoff uses `tokio::time::sleep(...).await` instead of `block_in_place` / `thread::sleep`.
#[derive(Clone, Debug)]
pub struct AsyncScrapedGlitchyPaymentCommandSource {
    total_events: usize,
    current_index: usize,
    writer_id: WriterId,
    warmup_events: usize,
    outage_events: usize,
    recovery_events: usize,
    feed: SemiReliableFeed,
}

impl AsyncScrapedGlitchyPaymentCommandSource {
    pub fn with_cycle(
        total_events: usize,
        warmup_events: usize,
        outage_events: usize,
        recovery_events: usize,
    ) -> Self {
        let seed = (total_events as u64)
            .wrapping_mul(31)
            .wrapping_add(warmup_events as u64)
            .wrapping_add(outage_events as u64)
            .wrapping_add(recovery_events as u64);

        Self {
            total_events,
            current_index: 0,
            writer_id: WriterId::from(StageId::new()),
            warmup_events: warmup_events.max(1),
            outage_events,
            recovery_events,
            feed: SemiReliableFeed::new(seed),
        }
    }

    fn phase_for_index(&self, index: usize) -> TrafficPhase {
        let cycle_len = self.warmup_events + self.outage_events + self.recovery_events;
        if cycle_len == 0 {
            return TrafficPhase::Warmup;
        }

        let pos = index % cycle_len;
        if pos < self.warmup_events {
            TrafficPhase::Warmup
        } else if pos < self.warmup_events + self.outage_events {
            TrafficPhase::Outage
        } else {
            TrafficPhase::Recovery
        }
    }
}

#[async_trait]
impl AsyncFiniteSourceHandler for AsyncScrapedGlitchyPaymentCommandSource {
    async fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.current_index >= self.total_events {
            return Ok(None);
        }

        self.feed.poll_async().await?;

        let idx = self.current_index;
        self.current_index += 1;

        if self.current_index % 10_000 == 0 {
            println!("📊 Scraped {} payment commands...", self.current_index);
        }

        let phase = self.phase_for_index(idx);

        let (card_ok, amount_cents) = match idx % 1_000 {
            0 => (false, 10_00), // bad card
            1 => (true, 0),      // zero amount
            _ => (true, 10_00),
        };

        let cmd = PaymentCommand {
            request_id: format!("cmd-{}", idx),
            customer_id: format!("cust-{}", idx % 10_000),
            amount_cents,
            card_ok,
            phase,
        };

        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id.clone(),
            PaymentCommand::EVENT_TYPE,
            json!(cmd),
        )]))
    }
}
