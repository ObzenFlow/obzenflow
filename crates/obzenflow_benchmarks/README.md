# ObzenFlow benchmarks

This unpublished crate holds ObzenFlow's benchmark fixtures and measurements. It
calls only the framework's public APIs and needs no network listeners.

## Suites

| Target | Measures | Feature |
| --- | --- | --- |
| `journal_components` | Public causal and journal-reader operations | `components` |
| `journal_hot_path` | Record, clock, append, read, capture and metrics operations | `journal-benchmarks` |
| `validation_boundaries` | Archive export and admission, replay comparison, metrics refresh and Studio projection | `validation-benchmarks` |
| `per_event_latency` | Median per-event latency through pipelines of 1 to 100 stages | Default |
| `pipeline_throughput` | Completed source, transform and sink flows | Default |
| `pipeline_execution` | Batch completion, metrics and causal-record operations | Default |
| `idle_cpu_usage`, `waiting_for_gun_cpu_usage` | Process CPU time of an idle flow, or of a built flow waiting to start | Default |

Each case declares what it times and in what unit, checks the work it completed
against an independent oracle, and fails its sample on a timeout or incomplete
work instead of reporting a value. Component cases time one public operation,
and only they supply the gated comparisons. Flow cases confirm that an
improvement survives in a running flow.

## Run locally

Run each case once, untimed, to check that it still passes:

```sh
cargo xtask test --lane benchmarks
```

Run the timed comparison and the full suite on one machine:

```sh
cargo xtask test --lane performance
```

Results land in `target/test-runs/<run-id>/performance/`. To compare one target,
save a baseline and compare a later run with it:

```sh
cargo bench --locked -p obzenflow_benchmarks --features components --bench journal_components -- \
  'disk_components/reader_next/' --save-baseline before
cargo bench --locked -p obzenflow_benchmarks --features components --bench journal_components -- \
  'disk_components/reader_next/' --baseline before
```

## In CI

Every pull request and push to `main` runs the `benchmarks` lane as one of the
correctness jobs in `ci.yml`. It builds every target, checks each target's case
declarations against its listing, and runs every case once in Criterion's test
mode. It times nothing, so a broken check fails in the pull request that broke it.

Timed runs happen only in the `Performance` workflow, which you start from
**Actions → Performance → Run workflow**. Choose a branch or tag and,
optionally, a version or commit it contains. No pull request, merge, publish
or release dry run waits on it.

A run plans its work, builds each feature group once, and fans out across
GitHub-hosted runners, one measurement per runner at a time. Qualification jobs
compare the gated cases with a reference, which is the merge base with `main`
for a branch, or the previous commit for a commit on `main`. Measurement jobs
time the full suite. A final job checks that every planned case reported
exactly once and writes the report. With warm caches, a run targets about 15
minutes.

The run's **Summary** page shows the report with a copyable Markdown block, and
the `test-results-performance` artifact keeps the raw Criterion results and
logs. `cargo xtask performance --help` lists the stages for reproducing one job
locally, and `.config/performance-policy.toml` sets the gated cases and their
limits.

## Profile a case

Build with frame pointers in a separate target directory, then run the
executable Cargo prints under `perf` or `samply`:

```sh
cargo bench --locked -p obzenflow_benchmarks --profile profiling --features components \
  --bench journal_components --target-dir target/profiling --no-run \
  --config 'build.rustflags=["-C","force-frame-pointers=yes"]'
samply record target/profiling/profiling/deps/journal_components-<hash> \
  --bench --profile-time 10 --exact 'causal_components/journal_clock_restore/w1_p256'
```

## Add a case

Declare each case next to its registration, with its full Criterion ID,
category and timed boundary:

```rust
use obzenflow_benchmarks::case::{declare, Category};

declare("journal_refresh/append_after_eof", Category::ReadWrite, "64 append, read and EOF-check pairs");
group.bench_function("append_after_eof", |b| { /* ... */ });
```

Keep fixture construction and checks outside timing. A new target also needs a
`[[bench]]` entry and a place in a shard of `.config/performance-shards.toml`.
Before opening a pull request, run:

```sh
cargo xtask test --lane benchmarks
python3 -m unittest discover -s .github/scripts -p 'test_*.py'
```

## Policies

See `LICENSE-MIT`, `LICENSE-APACHE`, `NOTICE`, `SECURITY.md` and `TRADEMARKS.md`.
