// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! # ObzenFlow Benchmarks
//!
//! This crate contains performance benchmarks for the ObzenFlow event streaming framework.
//!
//! ## Benchmark Categories
//!
//! - **Latency benchmarks**: Per-run median event latency through pipelines of various depths
//! - **Throughput benchmarks**: Completed flows and sustained event processing
//! - **Runtime benchmarks**: Process CPU time used by idle and waiting pipelines
//! - **Integration benchmarks**: End-to-end pipeline execution tests
//!
//! ## Running Benchmarks
//!
//! Run all benchmarks:
//! ```bash
//! cargo bench -p obzenflow_benchmarks
//! ```
//!
//! Run individual benchmark:
//! ```bash
//! cargo bench -p obzenflow_benchmarks --bench per_event_latency
//! ```

// This is primarily a benchmark crate, but we can expose some common utilities
// that benchmarks might share

pub mod case;
#[cfg(feature = "components")]
pub mod support;

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

/// CPU time consumed by every thread in this process.
#[cfg(unix)]
pub fn process_cpu_time() -> std::time::Duration {
    let mut now = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `now` is a valid out-pointer for this process-wide clock.
    let status = unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut now) };
    assert_eq!(status, 0, "process CPU clock unavailable");
    std::time::Duration::new(now.tv_sec as u64, now.tv_nsec as u32)
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
