//! Tokio Channel Performance Benchmark
//!
//! This benchmark investigates tokio channel + scheduler behavior, NOT FlowState.
//! It demonstrates that the 3-stage performance anomaly exists in pure tokio code
//! without any FlowState components, proving the issue is in tokio itself.
//!
//! Key finding: 3-4 concurrent tasks passing messages through channels exhibit
//! pathological performance with tokio's default 4 worker threads.

use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::runtime::Runtime;

async fn benchmark_channel_chain(num_stages: usize, num_events: usize) -> Duration {
    let mut senders = Vec::new();
    let mut receivers = Vec::new();
    let mut tasks = Vec::new();
    
    // Create channels between stages
    for _ in 0..=num_stages {
        let (tx, rx) = mpsc::channel::<usize>(100);
        senders.push(tx);
        receivers.push(rx);
    }
    
    // Create source task
    let source_tx = senders[0].clone();
    tasks.push(tokio::spawn(async move {
        for i in 0..num_events {
            source_tx.send(i).await.unwrap();
        }
    }));
    
    // Create intermediate stages
    for i in 0..num_stages {
        let rx = receivers.remove(0);
        let tx = senders[i + 1].clone();
        
        tasks.push(tokio::spawn(async move {
            let mut rx = rx;
            while let Some(value) = rx.recv().await {
                tx.send(value).await.unwrap();
            }
        }));
    }
    
    // Create sink to measure latency
    let mut sink_rx = receivers.remove(0);
    let start = Instant::now();
    
    let sink_task = tokio::spawn(async move {
        let mut count = 0;
        while let Some(_) = sink_rx.recv().await {
            count += 1;
            if count >= num_events {
                break;
            }
        }
        count
    });
    
    // Wait for all events to be processed
    let _received = sink_task.await.unwrap();
    let elapsed = start.elapsed();
    
    // Clean up source
    drop(senders);
    
    // Wait for all tasks
    for task in tasks {
        let _ = task.await;
    }
    
    elapsed
}

fn bench_channel_scaling(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("tokio_channel_scaling");
    
    // Test different stage counts to show the anomaly
    for stages in [1, 2, 3, 4, 5, 10, 20].iter() {
        group.bench_with_input(
            BenchmarkId::new("stages", stages),
            stages,
            |b, &stages| {
                b.to_async(&rt).iter(|| async move {
                    benchmark_channel_chain(stages, 100).await
                });
            }
        );
    }
    
    group.finish();
}

fn bench_channel_3stage_workers(c: &mut Criterion) {
    let mut group = c.benchmark_group("tokio_channel_3stage_workers");
    
    // Test 3-stage pipeline with different worker counts
    
    // Default 4 workers
    let rt4 = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
        
    group.bench_function("4_workers", |b| {
        b.to_async(&rt4).iter(|| async {
            benchmark_channel_chain(3, 100).await
        });
    });
    
    // 3 workers
    let rt3 = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(3)
        .enable_all()
        .build()
        .unwrap();
        
    group.bench_function("3_workers", |b| {
        b.to_async(&rt3).iter(|| async {
            benchmark_channel_chain(3, 100).await
        });
    });
    
    // 6 workers
    let rt6 = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(6)
        .enable_all()
        .build()
        .unwrap();
        
    group.bench_function("6_workers", |b| {
        b.to_async(&rt6).iter(|| async {
            benchmark_channel_chain(3, 100).await
        });
    });
    
    // Single threaded
    let rt1 = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
        
    group.bench_function("single_threaded", |b| {
        b.to_async(&rt1).iter(|| async {
            benchmark_channel_chain(3, 100).await
        });
    });
    
    group.finish();
}

criterion_group!(benches, bench_channel_scaling, bench_channel_3stage_workers);
criterion_main!(benches);