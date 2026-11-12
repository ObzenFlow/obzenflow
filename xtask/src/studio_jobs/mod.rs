// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

mod ports;
mod process;
mod state;

use crate::{error, is_help, workspace_root, Result};
use ports::{
    ensure_ports_free, force_release_required_ports, phonebook_is_listening, port_is_reachable,
};
use process::{
    force_kill_process, join_pids, process_exists, terminate_process, wait_until_all_stopped,
    wait_until_stopped,
};
use state::{read_state, remove_state_file, write_state, JobState};
use std::{
    collections::BTreeSet,
    env,
    fs::{self, File},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Duration,
};

const STUDIO_FEATURE: &str = "obzenflow_infra/studio-registration";
const STATE_DIR: &str = "target/studio-jobs";
const STATE_FILE: &str = "target/studio-jobs/state.tsv";
const PHONEBOOK_PORT: u16 = 7010;
const TERMINATE_WAIT: Duration = Duration::from_secs(30);
const FORCE_KILL_WAIT: Duration = Duration::from_secs(5);

const JOBS: &[JobSpec] = &[
    JobSpec {
        job_id: "payment_gateway",
        flow_name: "payment_gateway_resilience_demo",
        example: "payment_gateway_resilience",
        config: "examples/payment_gateway_resilience/obzenflow.studio.toml",
        port: 9090,
        extra_features: &[],
    },
    JobSpec {
        job_id: "csv_support_sla",
        flow_name: "csv_demo_support_sla",
        example: "csv_demo_support_sla",
        config: "examples/csv_demo_support_sla/obzenflow.studio.toml",
        port: 9091,
        extra_features: &[],
    },
    JobSpec {
        job_id: "flight_delays",
        flow_name: "flight_delays",
        example: "flight_delays_simple",
        config: "examples/flight_delays_simple/obzenflow.studio.toml",
        port: 9092,
        extra_features: &[],
    },
    JobSpec {
        job_id: "prometheus",
        flow_name: "prometheus_demo",
        example: "prometheus_demo",
        config: "examples/prometheus_demo/obzenflow.studio.toml",
        port: 9093,
        extra_features: &[],
    },
    // Composite showcase: `digest` renders as one collapsed node
    // in Studio. Mock HN feed and console output are the defaults; the AI
    // provider comes from the checked HN config. `[server] on_terminal =
    // "park"` keeps the run inspectable after the digest completes.
    JobSpec {
        job_id: "hn_ai_digest",
        flow_name: "hn_ai_digest_demo",
        example: "hn_ai_digest_demo",
        config: "examples/hn_ai_digest_demo/obzenflow.studio.toml",
        port: 9094,
        extra_features: &["http", "ai", "postgres"],
    },
];

#[derive(Clone, Copy)]
struct JobSpec {
    job_id: &'static str,
    flow_name: &'static str,
    example: &'static str,
    config: &'static str,
    port: u16,
    /// Example `required-features` beyond the Studio registration feature.
    /// Features unify across the single build invocation, so one job's
    /// extras are additive for the whole example build.
    extra_features: &'static [&'static str],
}

pub(crate) fn run(args: &[String]) -> Result<()> {
    let Some((command, flags)) = args.split_first() else {
        print_help();
        return Ok(());
    };

    match command.as_str() {
        "up" => up(flags),
        "down" => down(flags),
        "status" => status(flags),
        command if is_help(command) => {
            print_help();
            Ok(())
        }
        other => Err(error(format!("unknown studio-jobs command: {other}"))),
    }
}

fn up(flags: &[String]) -> Result<()> {
    let mut force = false;
    for flag in flags {
        match flag.as_str() {
            "--force" => force = true,
            flag if is_help(flag) => {
                print_help();
                return Ok(());
            }
            other => return Err(error(format!("unknown option for studio-jobs up: {other}"))),
        }
    }

    let root = workspace_root()?;
    if force {
        force_stop_jobs_and_release_ports(&root)?;
    } else {
        let existing = read_state(&root)?;
        let live = existing
            .iter()
            .filter(|job| process_exists(job.pid))
            .collect::<Vec<_>>();
        if !live.is_empty() {
            return Err(error(
                "studio jobs already appear to be running; use `cargo xtask studio-jobs down` or `cargo xtask studio-jobs up --force`",
            ));
        }
        if !existing.is_empty() {
            println!("replacing stale {STATE_FILE}");
        }
    }

    ensure_ports_free()?;
    if !phonebook_is_listening() {
        eprintln!(
            "warning: obzenflow-studiod is not listening on 127.0.0.1:{PHONEBOOK_PORT}; jobs will start and registration will retry"
        );
    }

    build_examples(&root)?;
    if force {
        // A detached supervisor or stale `cargo run` parent can recreate a
        // listener while the examples build. Reclaim the authoritative job
        // ports once more immediately before spawning replacements.
        force_release_required_ports()?;
    } else {
        ensure_ports_free()?;
    }
    fs::create_dir_all(root.join(STATE_DIR))?;

    let mut started = Vec::new();
    for spec in JOBS {
        match spawn_job(&root, *spec) {
            Ok(state) => started.push(state),
            Err(error) => {
                stop_started_jobs(&started);
                return Err(error);
            }
        }
    }

    write_state(&root, &started)?;
    print_started_table(&started);
    println!("logs: {STATE_DIR}/*.log");
    println!("stop: cargo xtask studio-jobs down");
    Ok(())
}

fn down(flags: &[String]) -> Result<()> {
    reject_flags("studio-jobs down", flags)?;
    let root = workspace_root()?;
    stop_jobs_from_state(&root)
}

fn status(flags: &[String]) -> Result<()> {
    reject_flags("studio-jobs status", flags)?;
    let root = workspace_root()?;
    let states = read_state(&root)?;
    if states.is_empty() {
        println!("no studio jobs state found");
        return Ok(());
    }

    println!(
        "{:<16} {:<34} {:>5} {:>8} {:<9} {:<9} log",
        "job_id", "flow", "port", "pid", "process", "socket"
    );
    for state in states {
        let process = if process_exists(state.pid) {
            "alive"
        } else {
            "stopped"
        };
        let socket = if port_is_reachable(state.port) {
            "listening"
        } else {
            "closed"
        };
        println!(
            "{:<16} {:<34} {:>5} {:>8} {:<9} {:<9} {}",
            state.job_id,
            state.flow_name,
            state.port,
            state.pid,
            process,
            socket,
            state.log.display()
        );
    }
    Ok(())
}

fn reject_flags(command: &str, flags: &[String]) -> Result<()> {
    if flags.is_empty() {
        return Ok(());
    }
    if flags.len() == 1 && is_help(&flags[0]) {
        print_help();
        return Ok(());
    }
    Err(error(format!("{command} accepts no options")))
}

fn build_examples(root: &Path) -> Result<()> {
    println!("building Studio example binaries");
    let mut features = vec![STUDIO_FEATURE];
    for job in JOBS {
        for feature in job.extra_features {
            if !features.contains(feature) {
                features.push(feature);
            }
        }
    }
    let mut command = Command::new("cargo");
    command.current_dir(root).args([
        "build",
        "-p",
        "obzenflow",
        "--features",
        &features.join(","),
    ]);
    for job in JOBS {
        command.arg("--example").arg(job.example);
    }
    let status = command.status()?;

    if status.success() {
        Ok(())
    } else {
        Err(error(format!("cargo build failed with status {status}")))
    }
}

fn spawn_job(root: &Path, spec: JobSpec) -> Result<JobState> {
    let binary = example_binary_path(root, spec.example);
    if !binary.is_file() {
        return Err(error(format!(
            "example binary not found after build: {}",
            binary.display()
        )));
    }
    if !root.join(spec.config).is_file() {
        return Err(error(format!("Studio config not found: {}", spec.config)));
    }

    let log = PathBuf::from(format!("{STATE_DIR}/{}.log", spec.job_id));
    let log_file = File::create(root.join(&log))?;
    let stderr = log_file.try_clone()?;
    let child = Command::new(&binary)
        .current_dir(root)
        .arg("--config")
        .arg(spec.config)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(stderr))
        .spawn()?;

    let pid = child.id();
    let state = JobState {
        job_id: spec.job_id.to_string(),
        example: spec.example.to_string(),
        flow_name: spec.flow_name.to_string(),
        port: spec.port,
        pid,
        config: PathBuf::from(spec.config),
        log,
    };
    println!(
        "started {:<16} pid={} port={} log={}",
        state.job_id,
        state.pid,
        state.port,
        state.log.display()
    );
    Ok(state)
}

fn stop_jobs_from_state(root: &Path) -> Result<()> {
    let states = read_state(root)?;
    if states.is_empty() {
        println!("no studio jobs state found");
        return Ok(());
    }

    let mut failures = Vec::new();
    for state in &states {
        if !process_exists(state.pid) {
            println!("already stopped {:<16} pid={}", state.job_id, state.pid);
            continue;
        }
        println!("stopping {:<16} pid={}", state.job_id, state.pid);
        match terminate_process(state.pid) {
            Ok(true) => {
                if !wait_until_stopped(state.pid) {
                    failures.push(format!("{} pid {} did not stop", state.job_id, state.pid));
                }
            }
            Ok(false) => failures.push(format!(
                "{} pid {} rejected termination",
                state.job_id, state.pid
            )),
            Err(error) => failures.push(format!("{} pid {}: {error}", state.job_id, state.pid)),
        }
    }

    if failures.is_empty() {
        remove_state_file(root)?;
        println!("logs remain under {STATE_DIR}");
        Ok(())
    } else {
        Err(error(format!(
            "failed to stop all studio jobs: {}",
            failures.join("; ")
        )))
    }
}

/// Force mode is deliberately stronger than `down`: the state file is only a
/// hint, while the configured runtime ports are authoritative. This recovers
/// detached example children whose recorded cargo/parent PID is stale.
fn force_stop_jobs_and_release_ports(root: &Path) -> Result<()> {
    let states = read_state(root)?;
    if states.is_empty() {
        println!("no studio jobs state found; checking required ports for orphan listeners");
    }

    let mut tracked_pids = BTreeSet::new();
    let mut failures = Vec::new();
    for state in &states {
        if !process_exists(state.pid) {
            println!("already stopped {:<16} pid={}", state.job_id, state.pid);
            continue;
        }
        tracked_pids.insert(state.pid);
        println!(
            "force-killing tracked {:<16} pid={}",
            state.job_id, state.pid
        );
        match force_kill_process(state.pid) {
            Ok(true) => {}
            Ok(false) if !process_exists(state.pid) => {}
            Ok(false) => failures.push(format!(
                "{} pid {} rejected SIGKILL",
                state.job_id, state.pid
            )),
            Err(error) => failures.push(format!("{} pid {}: {error}", state.job_id, state.pid)),
        }
    }

    if let Err(error) = force_release_required_ports() {
        failures.push(error.to_string());
    }

    let still_running = wait_until_all_stopped(&tracked_pids, FORCE_KILL_WAIT);
    if !still_running.is_empty() {
        failures.push(format!(
            "tracked PIDs still alive after SIGKILL: {}",
            join_pids(&still_running)
        ));
    }

    if failures.is_empty() {
        remove_state_file(root)?;
        if !states.is_empty() {
            println!("logs remain under {STATE_DIR}");
        }
        Ok(())
    } else {
        Err(error(format!(
            "failed to force-stop all studio jobs: {}",
            failures.join("; ")
        )))
    }
}

fn stop_started_jobs(states: &[JobState]) {
    for state in states {
        if process_exists(state.pid) {
            if let Err(error) = terminate_process(state.pid) {
                eprintln!("could not terminate process {}: {error}", state.pid);
            }
        }
    }
}

fn example_binary_path(root: &Path, example: &str) -> PathBuf {
    let binary = format!("{}{}", example, env::consts::EXE_SUFFIX);
    root.join("target")
        .join("debug")
        .join("examples")
        .join(binary)
}

fn print_started_table(states: &[JobState]) {
    println!(
        "{:<16} {:<34} {:>5} {:>8} log",
        "job_id", "flow", "port", "pid"
    );
    for state in states {
        println!(
            "{:<16} {:<34} {:>5} {:>8} {}",
            state.job_id,
            state.flow_name,
            state.port,
            state.pid,
            state.log.display()
        );
    }
}

fn print_help() {
    println!("usage:");
    println!("  cargo xtask studio-jobs up [--force]");
    println!("  cargo xtask studio-jobs status");
    println!("  cargo xtask studio-jobs down");
    println!();
    println!("up --force SIGKILLs tracked jobs and any listener occupying a configured job port");
    println!("jobs inherit the environment; hn_ai_digest honours HN_* / HN_AI_* vars");
    println!("(mock HN feed by default; AI provider defaults to local Ollama)");
}
