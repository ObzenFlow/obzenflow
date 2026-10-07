// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Exclusive elapsed-time accounting for completed supervisor dispatches.
//!
//! One supervisor task owns the clock. Phases include suspension and resume
//! delay, not CPU time. Spawned work does not inherit this task-local ledger.
//! Spans and their retained descendants have no authority over cycle lifetime.

use obzenflow_core::event::payloads::supervisor_descriptor::{SupervisionMode, SupervisorKind};
use obzenflow_core::WriterId;
use std::cell::RefCell;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Phase {
    Read,
    Handler,
    Prepare,
    Publish,
    CreditWait,
    Acknowledge,
    Control,
    Idle,
    Residual,
}

const PHASE_COUNT: usize = 9;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    Completed,
    Error,
    Cancelled,
}

impl Outcome {
    fn name(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Error => "error",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Clone, Copy)]
struct Override {
    token: u64,
    phase: Phase,
}

struct Cycle {
    id: u64,
    state: &'static str,
    started: Instant,
    changed_at: Instant,
    nanoseconds: [u64; PHASE_COUNT],
    business_inputs: u64,
    acknowledgements: u64,
    next_token: u64,
    // This allocation is reused across cycles; guards contain only IDs.
    overrides: Vec<Override>,
}

impl Cycle {
    fn new(id: u64, state: &'static str, now: Instant, overrides: Vec<Override>) -> Self {
        Self {
            id,
            state,
            started: now,
            changed_at: now,
            nanoseconds: [0; PHASE_COUNT],
            business_inputs: 0,
            acknowledgements: 0,
            next_token: 0,
            overrides,
        }
    }

    fn charge(&mut self, now: Instant) {
        let phase = self
            .overrides
            .last()
            .map_or(Phase::Residual, |item| item.phase);
        self.nanoseconds[phase as usize] += nanos(now.duration_since(self.changed_at));
        self.changed_at = now;
    }

    fn enter(&mut self, phase: Phase, now: Instant) -> u64 {
        self.charge(now);
        self.next_token += 1;
        self.overrides.push(Override {
            token: self.next_token,
            phase,
        });
        self.next_token
    }

    fn leave(&mut self, token: u64, now: Instant) {
        if let Some(index) = self.overrides.iter().position(|item| item.token == token) {
            self.charge(now);
            // Dropping a cancelled branch need not follow lexical stack order.
            self.overrides.remove(index);
        }
    }
}

#[derive(Clone, Debug, Default)]
struct Totals {
    dispatch_count: u64,
    business_input_count: u64,
    acknowledgement_count: u64,
    elapsed_ns: u64,
    nanoseconds: [u64; PHASE_COUNT],
    worst_residual_ns: u64,
    worst_residual_elapsed_ns: u64,
    max_residual_ns: u64,
    residual_over_five_percent_count: u64,
    conservation_failures: u64,
    max_conservation_error_ns: u64,
}

impl Totals {
    fn add(&mut self, cycle: &Cycle, now: Instant) {
        let elapsed = nanos(now.duration_since(cycle.started));
        let residual = cycle.nanoseconds[Phase::Residual as usize];
        let sum = cycle
            .nanoseconds
            .iter()
            .map(|value| u128::from(*value))
            .sum::<u128>();
        let error = sum.abs_diff(u128::from(elapsed));
        self.dispatch_count += 1;
        self.business_input_count += cycle.business_inputs;
        self.acknowledgement_count += cycle.acknowledgements;
        self.elapsed_ns += elapsed;
        for (total, duration) in self.nanoseconds.iter_mut().zip(cycle.nanoseconds) {
            *total += duration;
        }
        self.max_residual_ns = self.max_residual_ns.max(residual);
        self.residual_over_five_percent_count +=
            u64::from(u128::from(residual) * 100 > u128::from(elapsed) * 5);
        if self.worst_residual_elapsed_ns == 0
            || u128::from(residual) * u128::from(self.worst_residual_elapsed_ns)
                > u128::from(self.worst_residual_ns) * u128::from(elapsed)
        {
            self.worst_residual_ns = residual;
            self.worst_residual_elapsed_ns = elapsed;
        }
        self.conservation_failures += u64::from(error != 0);
        self.max_conservation_error_ns = self.max_conservation_error_ns.max(
            u64::try_from(error).expect("cycle conservation discrepancy exceeds u64 nanoseconds"),
        );
    }
}

struct StateTotals {
    state: &'static str,
    outcomes: [Totals; 3],
}

struct Aggregate {
    owner: u64,
    supervisor: String,
    kind: SupervisorKind,
    writer_id: WriterId,
    mode: &'static str,
    dispatch: tracing::Dispatch,
    interrupted: bool,
    next_cycle: u64,
    current: Option<Cycle>,
    spare_overrides: Vec<Override>,
    states: Vec<StateTotals>,
}

static NEXT_OWNER: AtomicU64 = AtomicU64::new(1);

tokio::task_local! {
    static AGGREGATE: RefCell<Aggregate>;
}

impl Aggregate {
    fn new(
        supervisor: &str,
        kind: SupervisorKind,
        writer_id: WriterId,
        mode: SupervisionMode,
    ) -> Self {
        Self {
            owner: NEXT_OWNER.fetch_add(1, Ordering::Relaxed),
            supervisor: supervisor.into(),
            kind,
            writer_id,
            mode: match mode {
                SupervisionMode::SelfSupervised => "self_supervised",
                SupervisionMode::HandlerSupervised => "handler_supervised",
            },
            dispatch: tracing::dispatcher::get_default(Clone::clone),
            interrupted: true,
            next_cycle: 0,
            current: None,
            spare_overrides: Vec::new(),
            states: Vec::new(),
        }
    }

    fn finish(&mut self, id: u64, outcome: Outcome, now: Instant) {
        if self.current.as_ref().is_none_or(|cycle| cycle.id != id) {
            return;
        }
        let mut cycle = self.current.take().expect("checked active cycle");
        cycle.charge(now);
        let state = match self.states.iter().position(|row| row.state == cycle.state) {
            Some(index) => index,
            None => {
                self.states.push(StateTotals {
                    state: cycle.state,
                    outcomes: std::array::from_fn(|_| Totals::default()),
                });
                self.states.len() - 1
            }
        };
        self.states[state].outcomes[outcome as usize].add(&cycle, now);
        cycle.overrides.clear();
        self.spare_overrides = cycle.overrides;
    }
}

impl Drop for Aggregate {
    fn drop(&mut self) {
        // The task-local future normally drops the cycle guard in scope first.
        // This fallback also accounts for a cycle if its guard escaped scope.
        if let Some(id) = self.current.as_ref().map(|cycle| cycle.id) {
            self.finish(id, Outcome::Cancelled, Instant::now());
        }
        tracing::dispatcher::with_default(&self.dispatch, || {
            for row in &self.states {
                for outcome in [Outcome::Completed, Outcome::Error, Outcome::Cancelled] {
                    let totals = &row.outcomes[outcome as usize];
                    if totals.dispatch_count == 0 {
                        continue;
                    }
                    let ns = &totals.nanoseconds;
                    tracing::debug!(target: "obzenflow::supervisor_timing",
                        supervisor = %self.supervisor, supervisor_kind = ?self.kind,
                        writer_id = %self.writer_id, supervision_mode = self.mode,
                        state = row.state, outcome = outcome.name(), interrupted = self.interrupted,
                        dispatch_count = totals.dispatch_count,
                        business_input_count = totals.business_input_count,
                        acknowledgement_count = totals.acknowledgement_count,
                        elapsed_ns = totals.elapsed_ns,
                        read_ns = ns[0], handler_ns = ns[1], prepare_ns = ns[2],
                        publish_ns = ns[3], credit_wait_ns = ns[4], acknowledge_ns = ns[5],
                        control_ns = ns[6], idle_ns = ns[7], residual_ns = ns[8],
                        worst_residual_ns = totals.worst_residual_ns,
                        worst_residual_elapsed_ns = totals.worst_residual_elapsed_ns,
                        max_residual_ns = totals.max_residual_ns,
                        residual_over_five_percent_count = totals.residual_over_five_percent_count,
                        conservation_failures = totals.conservation_failures,
                        max_conservation_error_ns = totals.max_conservation_error_ns,
                        "supervisor_cycle_summary");
                }
            }
        });
    }
}

fn nanos(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).expect("supervisor cycle exceeds u64 nanoseconds")
}

/// Install the one supervisor's aggregate; spawned tasks do not inherit it.
pub(crate) async fn scope<F: Future>(
    supervisor: &str,
    kind: SupervisorKind,
    writer_id: WriterId,
    mode: SupervisionMode,
    future: F,
) -> F::Output {
    if !tracing::event_enabled!(target: "obzenflow::supervisor_timing", tracing::Level::DEBUG) {
        return future.await;
    }
    AGGREGATE
        .scope(
            RefCell::new(Aggregate::new(supervisor, kind, writer_id, mode)),
            async move {
                let result = future.await;
                AGGREGATE.with(|aggregate| aggregate.borrow_mut().interrupted = false);
                result
            },
        )
        .await
}

pub(crate) struct CycleGuard {
    owner: u64,
    id: u64,
    outcome: Outcome,
}

impl CycleGuard {
    /// Mark an actual dispatch return; dropping before this remains cancellation.
    pub(crate) fn complete<T, E>(&mut self, result: &Result<T, E>) {
        self.outcome = if result.is_ok() {
            Outcome::Completed
        } else {
            Outcome::Error
        };
    }
}

impl Drop for CycleGuard {
    fn drop(&mut self) {
        if self.owner == 0 {
            return;
        }
        let _ = AGGREGATE.try_with(|aggregate| {
            let mut aggregate = aggregate.borrow_mut();
            if aggregate.owner == self.owner {
                aggregate.finish(self.id, self.outcome, Instant::now());
            }
        });
    }
}

/// Start inside the dispatch body without wrapping or copying its async future.
pub(crate) fn begin(state: &'static str) -> CycleGuard {
    AGGREGATE
        .try_with(|aggregate| {
            let mut aggregate = aggregate.borrow_mut();
            // Helpers may compose measured functions; the enclosing dispatch owns
            // the denominator and must not be counted twice.
            if aggregate.current.is_some() {
                return None;
            }
            aggregate.next_cycle += 1;
            let id = aggregate.next_cycle;
            let overrides = std::mem::take(&mut aggregate.spare_overrides);
            aggregate.current = Some(Cycle::new(id, state, Instant::now(), overrides));
            Some(CycleGuard {
                owner: aggregate.owner,
                id,
                outcome: Outcome::Cancelled,
            })
        })
        .ok()
        .flatten()
        .unwrap_or(CycleGuard {
            owner: 0,
            id: 0,
            outcome: Outcome::Cancelled,
        })
}

/// Convenience for small futures. Large dispatch bodies use `begin` directly.
#[cfg(test)]
pub(crate) async fn measure<T, E>(
    state: &'static str,
    future: impl Future<Output = Result<T, E>>,
) -> Result<T, E> {
    let mut guard = begin(state);
    let result = future.await;
    guard.complete(&result);
    drop(guard);
    result
}

/// A task-owned override, safe across awaits; never enters a tracing span.
#[must_use = "dropping the guard restores the preceding exclusive phase"]
pub(crate) struct PhaseGuard {
    owner: u64,
    cycle: u64,
    token: u64,
}

/// Temporarily replace the exclusive phase, including any awaited suspension.
pub(crate) fn phase(phase: Phase) -> PhaseGuard {
    AGGREGATE
        .try_with(|aggregate| {
            let mut aggregate = aggregate.borrow_mut();
            let owner = aggregate.owner;
            let Some(cycle) = aggregate.current.as_mut() else {
                return PhaseGuard {
                    owner: 0,
                    cycle: 0,
                    token: 0,
                };
            };
            let token = cycle.enter(phase, Instant::now());
            PhaseGuard {
                owner,
                cycle: cycle.id,
                token,
            }
        })
        .unwrap_or(PhaseGuard {
            owner: 0,
            cycle: 0,
            token: 0,
        })
}

impl Drop for PhaseGuard {
    fn drop(&mut self) {
        if self.owner == 0 {
            return;
        }
        let _ = AGGREGATE.try_with(|aggregate| {
            let mut aggregate = aggregate.borrow_mut();
            if aggregate.owner != self.owner {
                return;
            }
            if let Some(cycle) = aggregate
                .current
                .as_mut()
                .filter(|cycle| cycle.id == self.cycle)
            {
                cycle.leave(self.token, Instant::now());
            }
        });
    }
}

pub(crate) fn input_delivered() {
    inputs_delivered(1);
}

pub(crate) fn inputs_delivered(count: u64) {
    let _ = AGGREGATE.try_with(|aggregate| {
        if let Some(cycle) = aggregate.borrow_mut().current.as_mut() {
            cycle.business_inputs += count;
        }
    });
}

pub(crate) fn acknowledgement() {
    acknowledgements(1);
}

pub(crate) fn acknowledgements(count: u64) {
    let _ = AGGREGATE.try_with(|aggregate| {
        if let Some(cycle) = aggregate.borrow_mut().current.as_mut() {
            cycle.acknowledgements += count;
        }
    });
}

/// Charge only actual control polling, not the losing branch's pending lifetime.
pub(super) async fn control_poll<F: Future>(future: F) -> F::Output {
    tokio::pin!(future);
    std::future::poll_fn(|cx| {
        let _phase = phase(Phase::Control);
        future.as_mut().poll(cx)
    })
    .await
}

#[cfg(test)]
mod tests;
