// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Completed source -> trivial transforms -> sink, with fixed two-worker runtime.
//! Fixture/build and output checks are excluded; start, publication and drain are timed.
use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use obzenflow_benchmarks::case::{declare, Category};
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::journal::{factory::FlowJournalFactory, JournalError};
use obzenflow_core::{FlowId, TypedPayload};
use obzenflow_dsl::dsl::backpressure_clause::enforced;
use obzenflow_dsl::{async_source, flow, sink, transform, FlowDefinition};
use obzenflow_infra::journal::{disk_journals, memory_journals};
use obzenflow_runtime::pipeline::FlowHandle;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, SinkDescription, SinkWriteFailure, TypedAsyncFiniteSourceHandler,
    TypedTransformHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

const INPUTS: u64 = 128;
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Input {
    index: u64,
    emitted_ns: u64,
}
impl TypedPayload for Input {
    const EVENT_TYPE: &'static str = "bench.throughput_event";
}
#[derive(Clone, Debug)]
struct Source {
    index: u64,
    origin: Instant,
    sparse: bool,
}
#[async_trait]
impl TypedAsyncFiniteSourceHandler for Source {
    type Output = Input;
    async fn next(&mut self) -> Result<Option<Vec<Input>>, SourceError> {
        if self.index == INPUTS {
            return Ok(None);
        }
        if self.sparse {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let event = Input {
            index: self.index,
            emitted_ns: self.origin.elapsed().as_nanos() as u64,
        };
        self.index += 1;
        Ok(Some(vec![event]))
    }
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
#[derive(Clone, Debug)]
struct Sink {
    origin: Instant,
    delivered: Delivered,
    constrained: bool,
}
#[async_trait]
impl InlineSink for Sink {
    type Input = Input;
    fn describe(&self) -> SinkDescription {
        SinkDescription::method(DeliveryMethod::Noop)
    }
    async fn write(&mut self, event: Input) -> Result<(), SinkWriteFailure> {
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
async fn build<P, J>(
    journals: P,
    deep: bool,
    sparse: bool,
    constrained: bool,
    origin: Instant,
    delivered: Delivered,
) -> FlowHandle
where
    P: Fn(FlowId) -> Result<J, JournalError> + Send + Sync + 'static,
    J: FlowJournalFactory + 'static,
{
    FlowDefinition::materialize(move |_| {
        let source = Source {
            index: 0,
            origin,
            sparse,
        };
        let sink = Sink {
            origin,
            delivered,
            constrained,
        };
        let capacity = if constrained { 2 } else { 64 };
        Ok(if deep {
            flow! {
                journals: journals,
                backpressure: enforced(capacity).stall_timeout_ms(30_000),
                stages: {
                    src = async_source!(Input => source);
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
                backpressure: enforced(capacity).stall_timeout_ms(30_000),
                stages: {
                    src = async_source!(Input => source);
                    a = transform!(Input -> Input => Identity);
                    snk = sink!(Input => sink);
                },
                topology: { src |> a; a |> snk; }
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
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
}
criterion_group! { name = benches; config = Criterion::default().sample_size(10)
.warm_up_time(Duration::from_millis(300)).measurement_time(Duration::from_secs(2)); targets = bench }
criterion_main!(benches);
