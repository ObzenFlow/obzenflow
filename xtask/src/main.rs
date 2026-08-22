// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    error::Error,
    fmt,
    fs::{self, File},
    io::{self, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

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
    // FLOWIP-128a composite showcase: `digest` renders as one collapsed node
    // in Studio. Mock HN feed by default; the AI provider comes from HN_AI_*
    // env vars (default: local Ollama). `[server] on_terminal = "park"` keeps
    // the run inspectable after the digest completes.
    JobSpec {
        job_id: "hn_ai_digest",
        flow_name: "hn_ai_digest_demo",
        example: "hn_ai_digest_demo",
        config: "examples/hn_ai_digest_demo/obzenflow.studio.toml",
        port: 9094,
        extra_features: &["http-pull", "ai"],
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

#[derive(Clone, Debug)]
struct JobState {
    job_id: String,
    example: String,
    flow_name: String,
    port: u16,
    pid: u32,
    config: PathBuf,
    log: PathBuf,
}

#[derive(Debug)]
struct XtaskError(String);

impl fmt::Display for XtaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for XtaskError {}

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    match args.as_slice() {
        [] => {
            print_help();
            Ok(())
        }
        [arg] if is_help(arg) => {
            print_help();
            Ok(())
        }
        [cmd, rest @ ..] if cmd == "studio-jobs" => run_studio_jobs(rest),
        _ => Err(error(format!("unknown xtask command: {}", args.join(" ")))),
    }
}

fn run_studio_jobs(args: &[String]) -> Result<()> {
    let Some((command, flags)) = args.split_first() else {
        print_studio_jobs_help();
        return Ok(());
    };

    match command.as_str() {
        "up" => studio_jobs_up(flags),
        "down" => studio_jobs_down(flags),
        "status" => studio_jobs_status(flags),
        command if is_help(command) => {
            print_studio_jobs_help();
            Ok(())
        }
        other => Err(error(format!("unknown studio-jobs command: {other}"))),
    }
}

fn studio_jobs_up(flags: &[String]) -> Result<()> {
    let mut force = false;
    for flag in flags {
        match flag.as_str() {
            "--force" => force = true,
            flag if is_help(flag) => {
                print_studio_jobs_help();
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

fn studio_jobs_down(flags: &[String]) -> Result<()> {
    reject_flags("studio-jobs down", flags)?;
    let root = workspace_root()?;
    stop_jobs_from_state(&root)
}

fn studio_jobs_status(flags: &[String]) -> Result<()> {
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
        print_studio_jobs_help();
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
            let _ = terminate_process(state.pid);
        }
    }
}

fn ensure_ports_free() -> Result<()> {
    let occupied = occupied_required_ports()?;

    if occupied.is_empty() {
        Ok(())
    } else {
        Err(error(format!(
            "required runtime ports are already occupied: {}",
            describe_ports(&occupied)
        )))
    }
}

fn occupied_required_ports() -> io::Result<Vec<JobSpec>> {
    JOBS.iter()
        .filter_map(|job| match port_is_available(job.port) {
            Ok(true) => None,
            Ok(false) => Some(Ok(*job)),
            Err(error) => Some(Err(error)),
        })
        .collect()
}

fn describe_ports(jobs: &[JobSpec]) -> String {
    jobs.iter()
        .map(|job| format!("{} ({})", job.port, job.job_id))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Repeatedly discover and SIGKILL listeners on Studio's configured runtime
/// ports until every port can be bound or the bounded cleanup window expires.
/// Repeating matters when a detached parent races by respawning its child.
fn force_release_required_ports() -> Result<()> {
    let deadline = Instant::now() + FORCE_KILL_WAIT;
    let mut announced = BTreeSet::new();

    loop {
        let occupied = occupied_required_ports()?;
        if occupied.is_empty() {
            return Ok(());
        }

        let mut owners = BTreeMap::<u32, Vec<JobSpec>>::new();
        for job in &occupied {
            for pid in listener_pids_for_port(job.port)? {
                owners.entry(pid).or_default().push(*job);
            }
        }

        if owners.is_empty() {
            if Instant::now() >= deadline {
                return Err(error(format!(
                    "required runtime ports remain occupied but no listener PID could be identified: {}",
                    describe_ports(&occupied)
                )));
            }
            thread::sleep(Duration::from_millis(100));
            continue;
        }

        let mut kill_failures = Vec::new();
        for (pid, jobs) in owners {
            if announced.insert(pid) {
                println!(
                    "force-killing orphan listener pid={} on {}",
                    pid,
                    describe_ports(&jobs)
                );
            }
            match force_kill_process(pid) {
                Ok(true) => {}
                Ok(false) if !process_exists(pid) => {}
                Ok(false) => kill_failures.push(format!("pid {pid} rejected SIGKILL")),
                Err(error) => kill_failures.push(format!("pid {pid}: {error}")),
            }
        }
        if !kill_failures.is_empty() {
            return Err(error(format!(
                "failed to SIGKILL Studio port owners: {}",
                kill_failures.join("; ")
            )));
        }

        if Instant::now() >= deadline {
            let occupied = occupied_required_ports()?;
            if occupied.is_empty() {
                return Ok(());
            }
            return Err(error(format!(
                "required runtime ports remain occupied after SIGKILL: {}",
                describe_ports(&occupied)
            )));
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn port_is_available(port: u16) -> io::Result<bool> {
    match TcpListener::bind(("127.0.0.1", port)) {
        Ok(listener) => {
            drop(listener);
            Ok(true)
        }
        Err(error) if error.kind() == io::ErrorKind::AddrInUse => Ok(false),
        Err(error) => Err(error),
    }
}

#[cfg(unix)]
fn listener_pids_for_port(port: u16) -> io::Result<Vec<u32>> {
    let selector = format!("-iTCP:{port}");
    let output = Command::new("lsof")
        .args(["-nP", "-t", &selector, "-sTCP:LISTEN"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "`lsof` is required for `studio-jobs up --force` to identify orphan port owners",
                )
            } else {
                error
            }
        })?;

    // `lsof` uses exit 1 for an empty selection. That is a valid race: the
    // bind probe observed the listener just before it exited.
    if !output.status.success() && output.status.code() != Some(1) {
        return Err(io::Error::other(format!(
            "lsof failed while inspecting port {port}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    parse_pid_lines(&String::from_utf8_lossy(&output.stdout))
}

#[cfg(unix)]
fn parse_pid_lines(output: &str) -> io::Result<Vec<u32>> {
    output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .try_fold(BTreeSet::new(), |mut pids, line| {
            let pid = line.parse::<u32>().map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("lsof returned invalid PID '{line}': {error}"),
                )
            })?;
            pids.insert(pid);
            Ok(pids)
        })
        .map(|pids| pids.into_iter().collect())
}

#[cfg(windows)]
fn listener_pids_for_port(port: u16) -> io::Result<Vec<u32>> {
    let output = Command::new("netstat")
        .args(["-ano", "-p", "tcp"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "netstat failed while inspecting port {port}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }

    let pids = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            if fields.len() < 5
                || !fields[0].eq_ignore_ascii_case("TCP")
                || !fields[3].eq_ignore_ascii_case("LISTENING")
                || fields[1]
                    .rsplit_once(':')
                    .and_then(|(_, value)| value.parse::<u16>().ok())
                    != Some(port)
            {
                return None;
            }
            fields[4].parse::<u32>().ok()
        })
        .collect::<BTreeSet<_>>();
    Ok(pids.into_iter().collect())
}

fn phonebook_is_listening() -> bool {
    port_is_reachable(PHONEBOOK_PORT)
}

fn port_is_reachable(port: u16) -> bool {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    TcpStream::connect_timeout(&addr, Duration::from_millis(150)).is_ok()
}

fn write_state(root: &Path, states: &[JobState]) -> Result<()> {
    fs::create_dir_all(root.join(STATE_DIR))?;
    let mut file = File::create(root.join(STATE_FILE))?;
    writeln!(file, "# obzenflow xtask studio-jobs v1")?;
    for state in states {
        writeln!(
            file,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}",
            state.job_id,
            state.example,
            state.flow_name,
            state.port,
            state.pid,
            state.config.display(),
            state.log.display()
        )?;
    }
    Ok(())
}

fn read_state(root: &Path) -> Result<Vec<JobState>> {
    let path = root.join(STATE_FILE);
    if !path.exists() {
        return Ok(Vec::new());
    }

    let contents = fs::read_to_string(path)?;
    let mut states = Vec::new();
    for (index, line) in contents.lines().enumerate() {
        if line.trim().is_empty() || line.starts_with('#') {
            continue;
        }
        states.push(parse_state_line(line, index + 1)?);
    }
    Ok(states)
}

fn parse_state_line(line: &str, line_number: usize) -> Result<JobState> {
    let fields = line.split('\t').collect::<Vec<_>>();
    if fields.len() != 7 {
        return Err(error(format!(
            "{STATE_FILE}:{line_number}: expected 7 tab-separated fields, got {}",
            fields.len()
        )));
    }

    Ok(JobState {
        job_id: fields[0].to_string(),
        example: fields[1].to_string(),
        flow_name: fields[2].to_string(),
        port: fields[3].parse()?,
        pid: fields[4].parse()?,
        config: PathBuf::from(fields[5]),
        log: PathBuf::from(fields[6]),
    })
}

fn remove_state_file(root: &Path) -> Result<()> {
    let path = root.join(STATE_FILE);
    if path.exists() {
        fs::remove_file(path)?;
    }
    Ok(())
}

fn example_binary_path(root: &Path, example: &str) -> PathBuf {
    let binary = format!("{}{}", example, env::consts::EXE_SUFFIX);
    root.join("target")
        .join("debug")
        .join("examples")
        .join(binary)
}

fn workspace_root() -> Result<PathBuf> {
    let mut dir = env::current_dir()?;
    loop {
        let manifest = dir.join("Cargo.toml");
        if manifest.is_file() {
            let manifest_text = fs::read_to_string(&manifest)?;
            if manifest_text.contains("[workspace]")
                && manifest_text.contains("name = \"obzenflow\"")
            {
                return Ok(dir);
            }
        }
        if !dir.pop() {
            return Err(error("run this xtask from inside the obzenflow workspace"));
        }
    }
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
    println!("usage: cargo xtask studio-jobs <up|down|status>");
}

fn print_studio_jobs_help() {
    println!("usage:");
    println!("  cargo xtask studio-jobs up [--force]");
    println!("  cargo xtask studio-jobs status");
    println!("  cargo xtask studio-jobs down");
    println!();
    println!("up --force SIGKILLs tracked jobs and any listener occupying a configured job port");
    println!("jobs inherit the environment; hn_ai_digest honours HN_* / HN_AI_* vars");
    println!("(mock HN feed by default; AI provider defaults to local Ollama)");
}

fn is_help(arg: &str) -> bool {
    matches!(arg, "-h" | "--help" | "help")
}

fn wait_until_stopped(pid: u32) -> bool {
    let checks = TERMINATE_WAIT.as_millis() / 100;
    for _ in 0..checks {
        if !process_exists(pid) {
            return true;
        }
        thread::sleep(Duration::from_millis(100));
    }
    !process_exists(pid)
}

fn wait_until_all_stopped(pids: &BTreeSet<u32>, timeout: Duration) -> Vec<u32> {
    let deadline = Instant::now() + timeout;
    loop {
        let alive = pids
            .iter()
            .copied()
            .filter(|pid| process_exists(*pid))
            .collect::<Vec<_>>();
        if alive.is_empty() || Instant::now() >= deadline {
            return alive;
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn join_pids(pids: &[u32]) -> String {
    pids.iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(unix)]
fn process_exists(pid: u32) -> bool {
    Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

#[cfg(windows)]
fn process_exists(pid: u32) -> bool {
    let filter = format!("PID eq {pid}");
    Command::new("tasklist")
        .args(["/FI", &filter])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .is_ok_and(|output| {
            output.status.success()
                && String::from_utf8_lossy(&output.stdout).contains(&pid.to_string())
        })
}

#[cfg(unix)]
fn terminate_process(pid: u32) -> io::Result<bool> {
    Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .map(|status| status.success())
}

#[cfg(windows)]
fn terminate_process(pid: u32) -> io::Result<bool> {
    Command::new("taskkill")
        .args(["/PID", &pid.to_string(), "/T"])
        .status()
        .map(|status| status.success())
}

#[cfg(unix)]
fn force_kill_process(pid: u32) -> io::Result<bool> {
    validate_kill_pid(pid)?;
    Command::new("kill")
        .arg("-KILL")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
}

#[cfg(windows)]
fn force_kill_process(pid: u32) -> io::Result<bool> {
    validate_kill_pid(pid)?;
    Command::new("taskkill")
        .args(["/PID", &pid.to_string(), "/T", "/F"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
}

fn validate_kill_pid(pid: u32) -> io::Result<()> {
    if pid <= 1 || pid == std::process::id() {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("refusing to signal protected pid {pid}"),
        ))
    } else {
        Ok(())
    }
}

fn error(message: impl Into<String>) -> Box<dyn Error> {
    Box::new(XtaskError(message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn parses_and_deduplicates_lsof_pid_output() {
        assert_eq!(
            parse_pid_lines("4242\n  17 \n4242\n\n").expect("valid lsof output"),
            vec![17, 4242]
        );
    }

    #[cfg(unix)]
    #[test]
    fn rejects_malformed_lsof_pid_output() {
        let error = parse_pid_lines("4242\nnot-a-pid\n").expect_err("invalid PID must fail");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("not-a-pid"));
    }

    #[test]
    fn refuses_to_signal_protected_pids() {
        assert_eq!(
            validate_kill_pid(0)
                .expect_err("process group must be protected")
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            validate_kill_pid(1)
                .expect_err("init must be protected")
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            validate_kill_pid(std::process::id())
                .expect_err("xtask itself must be protected")
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[cfg(unix)]
    #[test]
    fn force_kill_process_sends_an_uncatchable_signal() {
        let mut child = Command::new("sleep")
            .arg("60")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn disposable child");
        let pid = child.id();

        assert!(force_kill_process(pid).expect("send SIGKILL"));
        let status = child.wait().expect("reap killed child");
        assert!(!status.success());
        assert!(!process_exists(pid));
    }
}
