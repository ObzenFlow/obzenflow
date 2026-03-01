// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! # ObzenFlow Benchmarks
//!
//! This crate contains performance benchmarks for the ObzenFlow event streaming framework.
//!
//! ## Benchmark Categories
//!
//! - **Latency benchmarks**: Measure per-event processing time through pipelines of various depths
//! - **Throughput benchmarks**: Measure sustained event processing rates
//! - **Runtime benchmarks**: Test runtime characteristics like idle CPU usage and threading behavior
//! - **Integration benchmarks**: End-to-end pipeline execution tests
//!
//! ## Running Benchmarks
//!
//! Run all benchmarks:
//! ```bash
//! cargo bench -p obzenflow_benchmarks
//! ```
//!
//! Run specific benchmark category:
//! ```bash
//! cargo bench -p obzenflow_benchmarks latency
//! ```
//!
//! Run individual benchmark:
//! ```bash
//! cargo bench -p obzenflow_benchmarks per_event_latency_3_stage
//! ```

// This is primarily a benchmark crate, but we can expose some common utilities
// that benchmarks might share

/// Re-export commonly used types for benchmarks
pub mod prelude {
    // Core types
    pub use obzenflow_core::ChainEvent as Event;
    pub use obzenflow_core::EventId as Id;
    pub use obzenflow_core::WriterId as Writer;
    pub use obzenflow_core::{ChainEvent, EventId, WriterId};

    // Runtime services
    pub use obzenflow_runtime::prelude::*;

    // DSL
    pub use obzenflow_dsl::prelude::*;

    // Journal
    pub use obzenflow_infra::journal::*;

    // Monitoring
    pub use obzenflow_adapters::monitoring::*;
}

fn set_default_env_var(key: &str, value: &str) {
    if std::env::var_os(key).is_none() {
        std::env::set_var(key, value);
    }
}

fn configure_benchmark_defaults() {
    // Metrics add substantial overhead (extra journal readers, extra parsing) and can
    // push deep disk-journal benchmarks over OS file-descriptor limits. Benchmarks can
    // opt back in by explicitly setting `OBZENFLOW_METRICS_ENABLED=true`.
    set_default_env_var("OBZENFLOW_METRICS_ENABLED", "false");
}

fn bump_nofile_limit() {
    #[cfg(unix)]
    {
        // Best-effort: 100-stage disk benchmarks can exceed macOS's default `ulimit -n 256`.
        // Raise the soft limit up to the hard limit so journal readers/writers can start.
        unsafe {
            let mut current = libc::rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };

            if libc::getrlimit(libc::RLIMIT_NOFILE, &mut current) != 0 {
                return;
            }

            // Keep this comfortably above the ~300 FDs a 100-stage disk pipeline can use.
            let desired: libc::rlim_t = 4096;

            if current.rlim_cur >= desired {
                return;
            }

            let mut updated = current;
            updated.rlim_cur = std::cmp::min(desired, current.rlim_max);

            // If hard limit is lower than desired, raising won't help enough;
            // still attempt to raise to the hard limit and continue either way.
            let _ = libc::setrlimit(libc::RLIMIT_NOFILE, &updated);
        }
    }
}

/// Initialize tracing for benchmark binaries.
///
/// Benchmarks often want runtime diagnostics (FSM state transitions, waits, etc).
/// We install a `tracing_subscriber` once, using `RUST_LOG` if provided and
/// defaulting to `warn` to minimize benchmark overhead.
pub fn init_tracing() {
    use std::sync::OnceLock;

    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        configure_benchmark_defaults();
        bump_nofile_limit();

        let filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn"));

        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(true)
            .with_level(true)
            .try_init();
    });
}

// Any benchmark-specific utilities can be added here
// For example, common benchmark fixtures, measurement helpers, etc.
