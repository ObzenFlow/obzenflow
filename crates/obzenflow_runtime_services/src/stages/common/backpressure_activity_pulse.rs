use obzenflow_core::event::payloads::observability_payload::BackpressureEvent;
use obzenflow_core::StageId;
use std::time::{Duration, Instant};

pub(crate) const BACKPRESSURE_ACTIVITY_PULSE_WINDOW_MS: u64 = 1000;
const BACKPRESSURE_ACTIVITY_PULSE_WINDOW: Duration = Duration::from_secs(1);

/// Low-volume, fixed-cadence pulse used as a UI animation driver for backpressure (FLOWIP-086k).
///
/// Mirrors the semantics of `RateLimiterEvent::ActivityPulse` (FLOWIP-059c):
/// - at most one event per second per stage
/// - only emitted when delay activity occurred within the window
#[derive(Debug, Clone)]
pub(crate) struct BackpressureActivityPulse {
    window_start: Instant,
    delayed_events: u64,
    delay_ms_total: u64,
    delay_ms_max: u64,
    last_min_credit: Option<u64>,
    last_limiting_downstream_stage_id: Option<StageId>,
}

impl BackpressureActivityPulse {
    pub(crate) fn new() -> Self {
        Self {
            window_start: Instant::now(),
            delayed_events: 0,
            delay_ms_total: 0,
            delay_ms_max: 0,
            last_min_credit: None,
            last_limiting_downstream_stage_id: None,
        }
    }

    pub(crate) fn record_delay(
        &mut self,
        delay: Duration,
        min_credit: Option<u64>,
        limiting_downstream_stage_id: Option<StageId>,
    ) {
        self.delayed_events = self.delayed_events.saturating_add(1);

        let waited_ms = delay.as_millis() as u64;
        self.delay_ms_total = self.delay_ms_total.saturating_add(waited_ms);
        self.delay_ms_max = self.delay_ms_max.max(waited_ms);

        self.last_min_credit = min_credit;
        self.last_limiting_downstream_stage_id = limiting_downstream_stage_id;
    }

    pub(crate) fn maybe_emit(&mut self) -> Option<BackpressureEvent> {
        let now = Instant::now();
        let elapsed = now.duration_since(self.window_start);
        if elapsed < BACKPRESSURE_ACTIVITY_PULSE_WINDOW {
            return None;
        }

        let delayed_events = self.delayed_events;
        let delay_ms_total = self.delay_ms_total;
        let delay_ms_max = self.delay_ms_max;
        let min_credit = self.last_min_credit;
        let limiting_downstream_stage_id = self.last_limiting_downstream_stage_id;

        self.window_start = now;
        self.delayed_events = 0;
        self.delay_ms_total = 0;
        self.delay_ms_max = 0;

        if delayed_events == 0 {
            return None;
        }

        Some(BackpressureEvent::ActivityPulse {
            window_ms: BACKPRESSURE_ACTIVITY_PULSE_WINDOW_MS,
            delayed_events,
            delay_ms_total,
            delay_ms_max,
            min_credit,
            limiting_downstream_stage_id,
        })
    }
}

impl Default for BackpressureActivityPulse {
    fn default() -> Self {
        Self::new()
    }
}
