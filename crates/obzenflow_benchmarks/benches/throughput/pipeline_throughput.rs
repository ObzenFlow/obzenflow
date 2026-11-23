// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Completed source -> trivial transforms -> sink, with fixed two-worker runtime,
//! and completed flows whose one effectful transform performs one effect per
//! input. Fixture/build and output checks are excluded; start, publication and
//! drain are timed.
use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use obzenflow_benchmarks::case::{declare, Category};
use obzenflow_benchmarks::numbers::{NextNumber, NumberFeed, Numbers};
use obzenflow_core::config::{ConfigScope, ConfigSource};
use obzenflow_core::journal::{factory::FlowJournalFactory, JournalError};
use obzenflow_core::{FlowId, TypedPayload};
use obzenflow_dsl::{effectful_transform, flow, sink, source, transform, FlowDefinition};
use obzenflow_infra::journal::{disk_journals, memory_journals};
use obzenflow_runtime::effects::Port as InputPort;
use obzenflow_runtime::effects::{perform, EffectError, Effects, Port, StageCompletion};
use obzenflow_runtime::pipeline::FlowHandle;
use obzenflow_runtime::run_context::FlowBuildContext;
use obzenflow_runtime::runtime_config::{
    CandidateSet, ConfigValue, ResolvedRuntimeConfig, ScopedCandidate,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::source::SourceRead;
use obzenflow_runtime::stages::common::handlers::{
    EffectfulTransformHandler, TypedAsyncFiniteSourceHandler, TypedTransformHandler,
};
use serde::{Deserialize, Serialize};
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::runtime::Runtime;

struct Source {
    port: obzenflow_runtime::queries::QueryPort<dyn NumberFeed>,
    origin: Instant,
}

impl Source {
    fn new(inputs: u64, origin: Instant, sparse: bool) -> Self {
        let numbers = if sparse {
            Numbers::paced(inputs, Duration::from_millis(1))
        } else {
            Numbers::counting(inputs, 1)
        };
        Self {
            port: numbers.port(),
            origin,
        }
    }
}

impl TypedAsyncFiniteSourceHandler for Source {
    type Output = Input;

    fn read(&self) -> SourceRead<Input> {
        let origin = self.origin;
        SourceRead::finite(self.port.clone(), NextNumber(0), move |index| {
            Ok(Input {
                index,
                emitted_ns: origin.elapsed().as_nanos() as u64,
            })
        })
    }
}

const INPUTS: u64 = 128;
const EFFECT_INPUTS: [u64; 2] = [2_000, 8_000];
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Input {
    index: u64,
    emitted_ns: u64,
}
impl TypedPayload for Input {
    const EVENT_TYPE: &'static str = "bench.throughput_event";
}
#[derive(Clone, Debug)]
struct Identity;
impl TypedTransformHandler for Identity {
    type Input = Input;
    type Output = Input;
    fn process(&self, event: Input) -> Result<Input, HandlerError> {
        Ok(event)
    }
}
type Delivered = Arc<Mutex<Vec<(u64, Duration)>>>;
struct Recorder {
    origin: Instant,
    delivered: Delivered,
    constrained: bool,
}
#[obzenflow_runtime::effects::port(system = "throughput-recorder")]
impl Recorder {
    #[effect(type = "bench.throughput.record", at_least_once, records = reply)]
    async fn record(&self, event: &Input) -> Result<(), EffectError> {
        if self.constrained {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        self.delivered.lock().unwrap().push((
            event.index,
            self.origin
                .elapsed()
                .checked_sub(Duration::from_nanos(event.emitted_ns))
                .expect("monotonic latency"),
        ));
        Ok(())
    }
}
fn enforced_backpressure(window: u64) -> FlowBuildContext {
    let mut candidates = CandidateSet::default();
    for (key, value) in [
        (
            "runtime.backpressure.mode",
            ConfigValue::Text("enforce".to_owned()),
        ),
        ("runtime.backpressure.window", ConfigValue::U64(window)),
        (
            "runtime.backpressure.stall_timeout_ms",
            ConfigValue::U64(30_000),
        ),
    ] {
        candidates
            .admit(ScopedCandidate::unqualified(
                key,
                ConfigScope::Flow,
                ConfigSource::File,
                value,
            ))
            .unwrap();
    }
    FlowBuildContext::new(Arc::new(ResolvedRuntimeConfig::new(candidates)))
}
async fn build<P, J>(
    journals: P,
    deep: bool,
    sparse: bool,
    constrained: bool,
    origin: Instant,
    delivered: Delivered,
) -> FlowHandle
where
    P: Fn(
            FlowId,
            Option<&std::path::Path>,
            obzenflow_runtime::runtime_config::JournalSync,
        ) -> Result<J, JournalError>
        + Send
        + Sync
        + 'static,
    J: FlowJournalFactory + 'static,
{
    FlowDefinition::materialize(move |_| {
        let source = Source::new(INPUTS, origin, sparse);
        let sink = perform(Input::clone as fn(&Input) -> Input).through(Port::to(Recorder {
            origin,
            delivered,
            constrained,
        }));
        Ok(if deep {
            flow! {
                journals: journals,
                stages: {
                    src = source!(Input => source);
                    a = transform!(Input -> Input => Identity);
                    b = transform!(Input -> Input => Identity);
                    c = transform!(Input -> Input => Identity);
                    snk = sink!(Input => sink);
                },
                topology: { src |> a; a |> b; b |> c; c |> snk; }
            }
        } else {
            flow! {
                journals: journals,
                stages: {
                    src = source!(Input => source);
                    a = transform!(Input -> Input => Identity);
                    snk = sink!(Input => sink);
                },
                topology: { src |> a; a |> snk; }
            }
        })
    })
    .build(enforced_backpressure(if constrained { 2 } else { 64 }))
    .await
    .unwrap()
}
fn bench(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("completed_flow");
    group.throughput(Throughput::Elements(INPUTS));
    for (name, memory, deep, sparse, constrained) in [
        ("disk_shallow_steady", false, false, false, false),
        ("disk_deep_steady", false, true, false, false),
        ("memory_shallow_control", true, false, false, false),
        ("memory_deep_control", true, true, false, false),
        ("disk_constrained_capacity_2", false, false, false, true),
        ("disk_sparse_arrivals", false, false, true, false),
    ] {
        declare(
            &format!("completed_flow/{name}"),
            Category::Flow,
            "128-input completed flow through drain; build excluded",
        );
        let mut reported = false;
        group.bench_function(name, |b| b.iter_custom(|iterations| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iterations {
                let directory = tempfile::tempdir().unwrap();
                let delivered: Delivered = Arc::new(Mutex::new(Vec::with_capacity(INPUTS as usize)));
                let origin = Instant::now();
                let handle = if memory {
                    rt.block_on(build(memory_journals(), deep, sparse, constrained, origin, delivered.clone()))
                } else {
                    rt.block_on(build(disk_journals(directory.path().to_owned()), deep, sparse, constrained, origin, delivered.clone()))
                };
                let started = Instant::now();
                rt.block_on(async {
                    tokio::time::timeout(Duration::from_secs(60), async {
                        handle.start().await.unwrap();
                        handle.wait_for_completion().await.unwrap();
                    }).await.expect("timeout invalidates completed-flow sample");
                });
                elapsed += started.elapsed();
                let rows = delivered.lock().unwrap();
                assert_eq!(rows.len(), INPUTS as usize);
                assert_eq!(rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(), (0..INPUTS).collect::<Vec<_>>(), "missing, duplicate or reordered output");
                if !reported {
                    let mut latencies: Vec<_> = rows.iter().map(|(_, latency)| *latency).collect();
                    latencies.sort();
                    eprintln!("{name}: completed={INPUTS}, first valid sample latency p50={:?}, p99={:?}; not Criterion estimates", latencies[latencies.len()/2], latencies[latencies.len()*99/100]);
                    reported = true;
                }
            }
            elapsed
        }));
    }
    group.finish();
    bench_effect_bookkeeping(c, &rt);
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Settled {
    index: u64,
}
impl TypedPayload for Settled {
    const EVENT_TYPE: &'static str = "bench.effect_settled";
}
struct Ledger;
#[derive(Clone, Debug, Serialize)]
struct Settle {
    index: u64,
}
#[obzenflow_runtime::effects::port(system = "ledger")]
impl Ledger {
    #[effect(type = "bench.settle", exactly_once)]
    fn settle(&self, request: &Settle) -> Result<Settled, EffectError> {
        Ok(Settled {
            index: request.index,
        })
    }
}
#[obzenflow_runtime::effects::command]
#[derive(Clone, Debug)]
struct SettleEach {
    #[port]
    ledger: Port<Ledger>,
}
#[async_trait]
impl EffectfulTransformHandler for SettleEach {
    type Input = Input;
    type Output = Settled;
    async fn process(
        &self,
        input: Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        fx.call(Settle { index: input.index })
            .await
            .map_err(|error| HandlerError::Other(error.to_string()))?;
        Ok(fx.complete()?)
    }
}
struct SettledLog {
    settled: Arc<Mutex<Vec<u64>>>,
}
#[obzenflow_runtime::effects::port(system = "settled-log")]
impl SettledLog {
    #[effect(type = "bench.throughput.settled", at_least_once, records = reply)]
    fn record(&self, event: &Settled) -> Result<(), EffectError> {
        self.settled.lock().unwrap().push(event.index);
        Ok(())
    }
}
async fn build_effectful(
    directory: &Path,
    inputs: u64,
    settled: Arc<Mutex<Vec<u64>>>,
) -> FlowHandle {
    let journals = disk_journals(directory.to_owned());
    FlowDefinition::materialize(move |_| {
        let source = Source::new(inputs, Instant::now(), false);
        let sink = perform(Settled::clone as fn(&Settled) -> Settled)
            .through(Port::to(SettledLog { settled }));
        let settle_each = SettleEach {
            ledger: Port::to(Ledger),
        };
        Ok(flow! {
            journals: journals,
            stages: {
                src = source!(Input => source);
                settle = effectful_transform!(Input -> Settled => settle_each);
                snk = sink!(Settled => sink);
            },
            topology: { src |> settle; settle |> snk; }
        })
    })
    .build(enforced_backpressure(64))
    .await
    .unwrap()
}
fn bench_effect_bookkeeping(c: &mut Criterion, rt: &Runtime) {
    let mut group = c.benchmark_group("effect_bookkeeping");
    for inputs in EFFECT_INPUTS {
        let name = format!("inputs_{inputs}");
        declare(
            &format!("effect_bookkeeping/{name}"),
            Category::Flow,
            &format!("{inputs}-input completed flow through one effectful transform performing one repeatable effect per input on disk journals; build excluded"),
        );
        group.throughput(Throughput::Elements(inputs));
        let mut reported = false;
        group.bench_function(name.as_str(), |b| b.iter_custom(|iterations| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iterations {
                let directory = tempfile::tempdir().unwrap();
                let settled = Arc::new(Mutex::new(Vec::with_capacity(inputs as usize)));
                let handle = rt.block_on(build_effectful(directory.path(), inputs, settled.clone()));
                let started = Instant::now();
                rt.block_on(async {
                    tokio::time::timeout(Duration::from_secs(180), async {
                        handle.start().await.unwrap();
                        handle.wait_for_completion().await.unwrap();
                    }).await.expect("timeout invalidates the effect bookkeeping sample");
                });
                let sample = started.elapsed();
                elapsed += sample;
                let mut rows = settled.lock().unwrap().clone();
                rows.sort_unstable();
                assert_eq!(rows, (0..inputs).collect::<Vec<_>>(), "missing or duplicate settlement");
                if !reported {
                    eprintln!("effect_bookkeeping/{name}: completed={inputs}, first valid sample {sample:?}, {:?} per input; not a Criterion estimate", sample / inputs as u32);
                    reported = true;
                }
            }
            elapsed
        }));
    }
    group.finish();
}
criterion_group! { name = benches; config = Criterion::default().sample_size(10)
.warm_up_time(Duration::from_millis(300)).measurement_time(Duration::from_secs(2)); targets = bench }
criterion_main!(benches);
