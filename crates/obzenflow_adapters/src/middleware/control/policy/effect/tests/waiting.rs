// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::support::*;
use crate::middleware::control::resilience::EffectPlanFactory;
use crate::middleware::control::{
    circuit_breaker, rate_limit, retry, CircuitBreaker, OnRefusal, Retry,
};
use crate::middleware::MiddlewareFactory;
use obzenflow_runtime::backpressure::{BackpressurePlan, BackpressureRegistry};
use obzenflow_runtime::effects::InputHolds;
use obzenflow_runtime::runtime_config::{CIRCUIT_BREAKER_OPEN_FOR_MODE_KEY, RETRY_ON_REFUSAL_KEY};
use tokio::time::Instant;

fn breaker(open_for_ms: u64) -> CircuitBreaker {
    circuit_breaker()
        .consecutive_failures(1)
        .open_for(Duration::from_millis(open_for_ms))
}

fn growing_breaker() -> CircuitBreaker {
    breaker(10)
        .open_for_multiplier(2.0)
        .max_open_for(Duration::from_millis(100))
}

fn quick_retry() -> Retry {
    retry()
        .fixed_delay(Duration::from_millis(1))
        .max_attempts(3)
}

fn plan(breaker: CircuitBreaker, retry: Retry) -> Box<dyn MiddlewareFactory> {
    EffectPlanFactory::with_breaker(breaker)
        .retry(retry)
        .build()
        .expect("waiting resilience aggregate")
}

fn waiting_boundary(breaker: CircuitBreaker, retry: Retry) -> Arc<PerEffectPolicyBoundary> {
    let (attachment, _, _) = materialized_resilience(plan(breaker, retry));
    Arc::new(boundary_with_chain(vec![attachment]))
}

fn timeout() -> EffectError {
    EffectError::Timeout("no reply".to_string())
}

async fn open_breaker(boundary: &PerEffectPolicyBoundary, input_seq: u64) {
    let report = boundary
        .around_repeatable_effect(
            &identity_at(input_seq),
            &data_event(),
            RepeatableEffectOperation::new(|| async {
                Err(EffectError::Permanent("dependency down".to_string()))
            }),
        )
        .await;
    assert!(matches!(
        report.outcome,
        EffectBoundaryOutcome::Executed(Err(EffectError::Permanent(_)))
    ));
}

struct Waited {
    report: EffectBoundaryReport,
    batches: Vec<Vec<ChainEvent>>,
}

async fn call(
    boundary: &PerEffectPolicyBoundary,
    input_seq: u64,
    operation: RepeatableEffectOperation,
) -> Waited {
    let progress = operation.progress();
    let report = boundary
        .around_repeatable_effect(&identity_at(input_seq), &data_event(), operation)
        .await;
    Waited {
        report,
        batches: progress.committed(),
    }
}

impl Waited {
    fn rows(&self) -> impl Iterator<Item = &ChainEvent> {
        self.batches
            .iter()
            .flatten()
            .chain(&self.report.control_events)
    }

    fn recovery<T>(&self, select: impl Fn(&RecoveryFact) -> Option<T>) -> Vec<T> {
        self.rows()
            .filter_map(|event| match &event.payload {
                ChainPayload::Execution(ExecutionPayload::Recovery(fact)) => select(fact),
                _ => None,
            })
            .collect()
    }

    fn delays(&self) -> Vec<u64> {
        self.recovery(|fact| match fact {
            RecoveryFact::RetryScheduled { delay_ms, .. } => Some(*delay_ms),
            _ => None,
        })
    }

    fn scheduled(&self) -> Vec<u32> {
        self.recovery(|fact| match fact {
            RecoveryFact::RetryScheduled { next_attempt, .. } => Some(*next_attempt),
            _ => None,
        })
    }

    fn exhaustions(&self) -> Vec<(u32, RetryStopReason)> {
        self.recovery(|fact| match fact {
            RecoveryFact::RetryExhausted {
                total_attempts,
                reason,
                ..
            } => Some((*total_attempts, *reason)),
            _ => None,
        })
    }

    fn attempts(&self) -> Vec<u32> {
        self.rows()
            .filter_map(|event| match &event.payload {
                ChainPayload::Execution(ExecutionPayload::CircuitBreaker(
                    CircuitBreakerFact::AttemptSettled { attempt, .. },
                )) => Some(*attempt),
                _ => None,
            })
            .collect()
    }

    fn cooldowns(&self) -> Vec<u64> {
        self.rows()
            .filter_map(|event| match &event.payload {
                ChainPayload::Execution(ExecutionPayload::CircuitBreaker(
                    CircuitBreakerFact::Opened { cooldown_ms, .. },
                )) => Some(*cooldown_ms),
                _ => None,
            })
            .collect()
    }

    fn succeeded(&self) -> bool {
        matches!(self.report.outcome, EffectBoundaryOutcome::Executed(Ok(_)))
    }
}

#[tokio::test(start_paused = true)]
async fn call_refused_open_waits_until_reopen() {
    let boundary = waiting_boundary(breaker(10_000), quick_retry());
    open_breaker(&boundary, 1).await;
    tokio::time::advance(Duration::from_millis(4_000)).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();

    let waited = call(
        &boundary,
        2,
        scripted_operation(calls.clone(), |_| Ok(Vec::new())),
    )
    .await;

    assert!(waited.succeeded());
    assert_eq!(started.elapsed(), Duration::from_millis(6_000));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(waited.delays(), [6_000]);
    assert_eq!(waited.batches.len(), 1, "one commit before the one wait");
}

#[tokio::test(start_paused = true)]
async fn refused_call_default_setting_waits() {
    let boundary = waiting_boundary(breaker(50), retry().fixed_delay(Duration::from_millis(1)));
    open_breaker(&boundary, 1).await;

    let waited = call(&boundary, 2, ok_execute()).await;

    assert!(waited.succeeded());
    assert!(waited.exhaustions().is_empty());
}

#[tokio::test(start_paused = true)]
async fn on_refusal_fail_records_rejection() {
    let boundary = waiting_boundary(breaker(50), quick_retry().on_refusal(OnRefusal::Fail));
    open_breaker(&boundary, 1).await;
    let started = Instant::now();

    let waited = call(&boundary, 2, ok_execute()).await;

    assert!(matches!(
        waited.report.outcome,
        EffectBoundaryOutcome::Aborted(ref reason) if reason.cause.code.as_str() == "circuit_open"
    ));
    assert_eq!(started.elapsed(), Duration::ZERO);
    assert!(waited.batches.is_empty());
}

#[tokio::test(start_paused = true)]
async fn opening_call_waits_for_reopen() {
    let boundary = waiting_boundary(breaker(10), quick_retry());
    let calls = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();

    let waited = call(
        &boundary,
        1,
        scripted_operation(calls.clone(), |call| {
            if call == 1 {
                Err(timeout())
            } else {
                Ok(Vec::new())
            }
        }),
    )
    .await;

    assert!(waited.succeeded());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(started.elapsed(), Duration::from_millis(10));
    assert_eq!(waited.delays(), [10]);
    assert_eq!(waited.attempts(), [1, 2]);
}

#[tokio::test(start_paused = true)]
async fn failed_probe_call_waits_for_reopen() {
    let boundary = waiting_boundary(growing_breaker(), quick_retry());
    let calls = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();

    let waited = call(
        &boundary,
        1,
        scripted_operation(calls.clone(), |call| {
            if call < 3 {
                Err(timeout())
            } else {
                Ok(Vec::new())
            }
        }),
    )
    .await;

    assert!(waited.succeeded());
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(started.elapsed(), Duration::from_millis(30));
    assert_eq!(waited.delays(), [10, 20]);
    assert_eq!(waited.cooldowns(), [10, 20]);
}

#[tokio::test(start_paused = true)]
async fn waited_call_attempt_count_restarts() {
    let boundary = waiting_boundary(breaker(10), quick_retry().max_attempts(2));
    let calls = Arc::new(AtomicUsize::new(0));

    let waited = call(
        &boundary,
        1,
        scripted_operation(calls.clone(), |call| {
            if call < 3 {
                Err(timeout())
            } else {
                Ok(Vec::new())
            }
        }),
    )
    .await;

    assert!(waited.succeeded(), "each wait restarts the two attempts");
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert!(waited.exhaustions().is_empty());
}

#[tokio::test(start_paused = true)]
async fn waited_call_window_restarts() {
    let boundary = waiting_boundary(
        breaker(10),
        quick_retry().attempt_start_window(Duration::from_millis(5)),
    );
    let calls = Arc::new(AtomicUsize::new(0));

    let waited = call(
        &boundary,
        1,
        scripted_operation(calls.clone(), |call| {
            if call == 1 {
                Err(timeout())
            } else {
                Ok(Vec::new())
            }
        }),
    )
    .await;

    assert!(
        waited.succeeded(),
        "the 10 ms wait outlasts the first window, and the window restarts"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn attempt_numbers_continue_across_waits() {
    let boundary = waiting_boundary(growing_breaker(), quick_retry());

    let waited = call(
        &boundary,
        1,
        scripted_operation(Arc::new(AtomicUsize::new(0)), |call| {
            if call < 3 {
                Err(timeout())
            } else {
                Ok(Vec::new())
            }
        }),
    )
    .await;

    assert_eq!(waited.attempts(), [1, 2, 3]);
    assert_eq!(waited.scheduled(), [2, 3]);
}

#[tokio::test(start_paused = true)]
async fn reopen_beyond_outage_bound_ends_wait() {
    let boundary = waiting_boundary(
        growing_breaker(),
        quick_retry().max_outage_wait(Duration::from_millis(25)),
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();

    let waited = call(
        &boundary,
        1,
        scripted_operation(calls.clone(), |_| Err(timeout())),
    )
    .await;

    assert!(matches!(
        waited.report.outcome,
        EffectBoundaryOutcome::Executed(Err(EffectError::Timeout(_)))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(started.elapsed(), Duration::from_millis(10));
    assert_eq!(
        waited.exhaustions(),
        [(2, RetryStopReason::OutageWaitLimit)]
    );
}

#[tokio::test(start_paused = true)]
async fn second_call_inherits_outage_deadline() {
    let boundary = waiting_boundary(
        growing_breaker(),
        quick_retry().max_outage_wait(Duration::from_millis(25)),
    );
    open_breaker(&boundary, 1).await;
    tokio::time::advance(Duration::from_millis(5)).await;
    let started = Instant::now();

    let waited = call(
        &boundary,
        2,
        scripted_operation(Arc::new(AtomicUsize::new(0)), |_| Err(timeout())),
    )
    .await;

    assert_eq!(
        started.elapsed(),
        Duration::from_millis(5),
        "the reopen after the failed probe falls past the outage's deadline"
    );
    assert_eq!(
        waited.exhaustions(),
        [(1, RetryStopReason::OutageWaitLimit)]
    );
}

#[tokio::test(start_paused = true)]
async fn refusal_after_outage_bound_returns_rejected() {
    let boundary = waiting_boundary(
        breaker(100),
        quick_retry().max_outage_wait(Duration::from_millis(25)),
    );
    open_breaker(&boundary, 1).await;
    let started = Instant::now();

    let waited = call(&boundary, 2, ok_execute()).await;

    assert!(matches!(
        waited.report.outcome,
        EffectBoundaryOutcome::Aborted(ref reason) if reason.cause.code.as_str() == "circuit_open"
    ));
    assert_eq!(started.elapsed(), Duration::ZERO);
    assert_eq!(
        waited.exhaustions(),
        [(0, RetryStopReason::OutageWaitLimit)]
    );
}

#[tokio::test(start_paused = true)]
async fn rate_limited_probe_waits_retry_after() {
    let boundary = waiting_boundary(breaker(10), quick_retry());
    let calls = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();

    let waited = call(
        &boundary,
        1,
        scripted_operation(calls.clone(), |call| match call {
            1 => Err(timeout()),
            2 => Err(EffectError::RateLimited {
                message: "slow down".to_string(),
                retry_after: Duration::from_millis(50),
            }),
            _ => Ok(Vec::new()),
        }),
    )
    .await;

    assert!(waited.succeeded());
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(started.elapsed(), Duration::from_millis(60));
    assert_eq!(waited.delays(), [10, 50]);
    assert_eq!(
        waited.batches.len(),
        2,
        "the probe's rows commit before its backoff"
    );
}

#[tokio::test(start_paused = true)]
async fn closed_breaker_backoff_commits_one_group() {
    let boundary = waiting_boundary(circuit_breaker().consecutive_failures(5), quick_retry());

    let waited = call(
        &boundary,
        1,
        scripted_operation(Arc::new(AtomicUsize::new(0)), |call| {
            if call < 3 {
                Err(timeout())
            } else {
                Ok(Vec::new())
            }
        }),
    )
    .await;

    assert!(waited.succeeded());
    assert!(waited.batches.is_empty());
    assert_eq!(waited.delays(), [1, 1]);
}

#[tokio::test(start_paused = true)]
async fn waited_call_after_reopening_admitted() {
    let boundary = waiting_boundary(
        circuit_breaker()
            .consecutive_failures(2)
            .open_for(Duration::from_millis(10)),
        retry()
            .fixed_delay(Duration::from_millis(50))
            .max_attempts(3),
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let waiting = {
        let boundary = boundary.clone();
        let calls = calls.clone();
        tokio::spawn(async move {
            call(
                &boundary,
                1,
                scripted_operation(calls, |call| {
                    if call == 1 {
                        Err(timeout())
                    } else {
                        Ok(Vec::new())
                    }
                }),
            )
            .await
        })
    };
    tokio::task::yield_now().await;
    open_breaker(&boundary, 2).await;

    let waited = waiting.await.expect("waiting call");

    assert!(
        waited.succeeded(),
        "the breaker opened during the backoff, and the call takes a fresh epoch"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(waited.attempts(), [1, 2]);
}

#[tokio::test(start_paused = true)]
async fn stale_reservation_after_wait_refused() {
    let factory = EffectPlanFactory::with_breaker(breaker(10))
        .retry(quick_retry())
        .rate_limit_each_attempt(rate_limit(1_000.0).burst_capacity(3.0))
        .build()
        .expect("waiting aggregate with a limiter");
    let (attachment, control, stage_id) = materialized_resilience(factory);
    let target = identity_at(2);
    let gate = FinalAdmissionTestGate::new(target.cursor.clone(), 0);
    attachment
        .effect_resilience_policy()
        .expect("one recovery owner")
        .set_final_admission_test_gate(gate.clone());
    let boundary = Arc::new(boundary_with_chain(vec![attachment]));
    open_breaker(&boundary, 1).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let waiting = {
        let boundary = boundary.clone();
        let calls = calls.clone();
        tokio::spawn(async move {
            boundary
                .around_repeatable_effect(
                    &target,
                    &data_event(),
                    scripted_operation(calls, |_| Ok(Vec::new())),
                )
                .await
        })
    };
    tokio::time::advance(Duration::from_millis(10)).await;
    gate.wait_until_reached().await;
    let before = effect_limiter_events(control.as_ref(), stage_id);

    open_breaker(&boundary, 3).await;
    gate.release();
    tokio::time::advance(Duration::from_millis(10)).await;
    gate.wait_until_reached().await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "no call rides a stale reservation"
    );
    gate.release();

    let report = waiting.await.expect("waiting call");
    assert!(matches!(
        report.outcome,
        EffectBoundaryOutcome::Executed(Ok(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        effect_limiter_events(control.as_ref(), stage_id),
        before + 2
    );
}

#[tokio::test(start_paused = true)]
async fn waiting_call_drop_releases_nothing() {
    let factory = EffectPlanFactory::with_breaker(breaker(10_000))
        .retry(quick_retry())
        .rate_limit_each_attempt(rate_limit(1.0).burst_capacity(1.0))
        .build()
        .expect("waiting aggregate with a limiter");
    let (attachment, control, stage_id) = materialized_resilience(factory);
    let boundary = Arc::new(boundary_with_chain(vec![attachment]));
    open_breaker(&boundary, 1).await;
    let opened_events = effect_limiter_events(control.as_ref(), stage_id);
    let waiting = {
        let boundary = boundary.clone();
        tokio::spawn(async move { call(&boundary, 2, ok_execute()).await })
    };
    tokio::task::yield_now().await;

    waiting.abort();
    assert!(matches!(waiting.await, Err(error) if error.is_cancelled()));
    tokio::time::advance(Duration::from_millis(10_000)).await;
    let started = Instant::now();
    let probe = call(&boundary, 3, ok_execute()).await;

    assert!(probe.succeeded());
    assert_eq!(started.elapsed(), Duration::ZERO);
    assert_eq!(
        effect_limiter_events(control.as_ref(), stage_id),
        opened_events + 1
    );
}

#[tokio::test(start_paused = true)]
async fn call_behind_probe_wakes_on_settlement() {
    for probe_succeeds in [true, false] {
        let (_, _, map_boundary, _, reduce_boundary) =
            composite_members(plan(growing_breaker(), quick_retry()).as_ref());
        open_breaker(&map_boundary, 1).await;
        tokio::time::advance(Duration::from_millis(10)).await;
        let release = Arc::new(tokio::sync::Notify::new());
        let probing = {
            let release = release.clone();
            let map_boundary = map_boundary.clone();
            tokio::spawn(async move {
                call(
                    &map_boundary,
                    2,
                    RepeatableEffectOperation::new(move || {
                        let release = release.clone();
                        async move {
                            release.notified().await;
                            if probe_succeeds {
                                Ok(Vec::new())
                            } else {
                                Err(EffectError::Permanent("still down".to_string()))
                            }
                        }
                    }),
                )
                .await
            })
        };
        tokio::task::yield_now().await;
        let behind = {
            let reduce_boundary = reduce_boundary.clone();
            tokio::spawn(async move { call(&reduce_boundary, 3, ok_execute()).await })
        };
        tokio::task::yield_now().await;
        let settled_at = Instant::now() + Duration::from_millis(3);
        tokio::time::advance(Duration::from_millis(3)).await;
        release.notify_one();
        probing.await.expect("probe");

        let waited = behind.await.expect("call behind the probe");
        assert!(waited.succeeded());
        let reopened = if probe_succeeds { 0 } else { 20 };
        assert_eq!(
            Instant::now().duration_since(settled_at),
            Duration::from_millis(reopened),
            "the call wakes when the probe settles"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn probe_busy_wait_ends_at_deadline() {
    let (_, _, map_boundary, _, reduce_boundary) = composite_members(
        plan(
            breaker(10),
            quick_retry().max_outage_wait(Duration::from_millis(40)),
        )
        .as_ref(),
    );
    open_breaker(&map_boundary, 1).await;
    tokio::time::advance(Duration::from_millis(10)).await;
    let hung = {
        let map_boundary = map_boundary.clone();
        tokio::spawn(async move {
            call(
                &map_boundary,
                2,
                RepeatableEffectOperation::new(std::future::pending),
            )
            .await
        })
    };
    tokio::task::yield_now().await;
    let started = Instant::now();

    let waited = call(&reduce_boundary, 3, ok_execute()).await;

    assert_eq!(started.elapsed(), Duration::from_millis(30));
    assert_eq!(
        waited.exhaustions(),
        [(0, RetryStopReason::OutageWaitLimit)]
    );
    hung.abort();
}

fn enforced_input(stage: &str) -> (BackpressureRegistry, (StageId, StageId)) {
    use obzenflow_runtime::id_conversions::StageIdExt;
    let mut builder = obzenflow_topology::TopologyBuilder::new();
    let upstream = StageId::from_topology_id(builder.add_stage(Some("upstream".to_string())));
    let reader = StageId::from_topology_id(builder.add_stage(Some(stage.to_string())));
    let topology = builder.build_unchecked().expect("topology");
    let plan = BackpressurePlan::disabled().with_edge_enforced(
        upstream,
        reader,
        std::num::NonZeroU64::MIN,
        Duration::from_secs(30),
    );
    (
        BackpressureRegistry::new(&topology, &plan),
        (upstream, reader),
    )
}

fn renewals(registry: &BackpressureRegistry, edge: (StageId, StageId)) -> u64 {
    registry
        .metrics_snapshot()
        .edge_hold_renewals
        .get(&edge)
        .copied()
        .unwrap_or(0)
}

#[tokio::test(start_paused = true)]
async fn backoff_wait_renews_input_hold() {
    let (registry, edge) = enforced_input("stage");
    let boundary = waiting_boundary(
        circuit_breaker().consecutive_failures(5),
        retry().fixed_delay(Duration::from_secs(1)).max_attempts(2),
    );
    let operation = scripted_operation(Arc::new(AtomicUsize::new(0)), |call| {
        if call == 1 {
            Err(timeout())
        } else {
            Ok(Vec::new())
        }
    })
    .with_input_holds(InputHolds::new([registry.reader(edge.0, edge.1)], None));

    let waited = call(&boundary, 1, operation).await;

    assert!(waited.succeeded());
    assert!(
        renewals(&registry, edge) >= 4,
        "a one-second backoff renews at the control cadence"
    );
}

#[tokio::test(start_paused = true)]
async fn sibling_probe_in_flight_pauses_renewal() {
    let (map_registry, map_edge) = enforced_input("map");
    let (reduce_registry, reduce_edge) = enforced_input("reduce");
    let (_, _, map_boundary, _, reduce_boundary) =
        composite_members(plan(breaker(10), quick_retry()).as_ref());
    open_breaker(&map_boundary, 1).await;
    let release = Arc::new(tokio::sync::Notify::new());
    let probing = {
        let map_boundary = map_boundary.clone();
        let release = release.clone();
        let operation = RepeatableEffectOperation::new(move || {
            let release = release.clone();
            async move {
                release.notified().await;
                Ok(Vec::new())
            }
        })
        .with_input_holds(InputHolds::new(
            [map_registry.reader(map_edge.0, map_edge.1)],
            None,
        ));
        tokio::spawn(async move { call(&map_boundary, 2, operation).await })
    };
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(10)).await;
    let behind = {
        let reduce_boundary = reduce_boundary.clone();
        let operation = ok_execute().with_input_holds(InputHolds::new(
            [reduce_registry.reader(reduce_edge.0, reduce_edge.1)],
            None,
        ));
        tokio::spawn(async move { call(&reduce_boundary, 3, operation).await })
    };
    tokio::task::yield_now().await;
    let map_before = renewals(&map_registry, map_edge);
    let reduce_before = renewals(&reduce_registry, reduce_edge);

    for _ in 0..5 {
        tokio::time::advance(Duration::from_secs(1)).await;
    }

    assert!(
        map_before > 0,
        "the prober renewed while it waited for the reopen"
    );
    assert_eq!(
        renewals(&map_registry, map_edge),
        map_before,
        "no renewal while the stage's probe is in flight"
    );
    assert!(renewals(&reduce_registry, reduce_edge) >= reduce_before + 16);
    release.notify_one();
    assert!(probing.await.expect("probe").succeeded());
    assert!(behind.await.expect("call behind the probe").succeeded());
}

#[test]
fn invalid_outage_settings_fail_build() {
    use crate::middleware::control::compose_effect_controls;
    use crate::middleware::MiddlewareAttachmentSite;

    for (retry, setting) in [
        (quick_retry().on_refusal(OnRefusal::Fail), "on_refusal"),
        (
            quick_retry().max_outage_wait(Duration::from_secs(60)),
            "max_outage_wait",
        ),
    ] {
        let factory =
            compose_effect_controls(vec![(MiddlewareAttachmentSite::Effect, Box::new(retry))])
                .expect("a retry alone composes");
        let config = test_stage_config(factory.as_ref());
        let Err(error) = materialize_effect_attachment(
            factory.as_ref(),
            &config,
            &Arc::new(ControlMiddlewareAggregator::new()),
            EffectSafety::Idempotent,
        ) else {
            panic!("an outage setting needs a breaker");
        };
        assert!(
            error.contains(&format!(
                "retry sets {setting}, but no circuit breaker protects this effect; attach a circuit breaker or remove {setting}"
            )),
            "{error}"
        );
    }

    let refused = CandidateSet::default().admit(ScopedCandidate {
        key_path: RETRY_ON_REFUSAL_KEY.to_string(),
        address: ConfigAddress::effect("retrying_breaker_test", "effect.retry"),
        source: ConfigSource::File,
        value: ConfigValue::Text("later".into()),
    });
    assert!(
        matches!(refused, Err(ref error) if error.to_string().contains("\"later\"")),
        "{refused:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn fixed_mode_config_disables_growth() {
    let snapshot = file_effect_snapshot(&[(
        CIRCUIT_BREAKER_OPEN_FOR_MODE_KEY,
        ConfigValue::Text("fixed".into()),
    )]);
    let (attachment, _, _) =
        materialized_resilience_with_snapshot(plan(growing_breaker(), quick_retry()), &snapshot);
    let boundary = boundary_with_chain(vec![attachment]);

    let waited = call(
        &boundary,
        1,
        scripted_operation(Arc::new(AtomicUsize::new(0)), |call| {
            if call < 3 {
                Err(timeout())
            } else {
                Ok(Vec::new())
            }
        }),
    )
    .await;

    assert!(waited.succeeded());
    assert_eq!(waited.cooldowns(), [10, 10]);
}
