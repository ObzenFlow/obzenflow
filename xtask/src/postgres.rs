use super::{error, Result};
#[cfg(unix)]
use std::sync::atomic::{AtomicI32, Ordering};
use std::{
    fs::{self, File},
    hash::{Hash, Hasher},
    io::Write,
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};
use uuid::Uuid;

const COMPOSE_FILE: &str = "dev/postgres/compose.yml";
const SESSION_ROOT: &str = "target/postgres-sessions";
const DEVELOPMENT_SESSION: &str = "development";
const STATE_FILE: &str = "state.tsv";
const LOG_FILE: &str = "postgres.log";
const IMAGE: &str = "postgres:17";
const POSTGRES_USER: &str = "obzenflow";
const POSTGRES_DATABASE: &str = "obzenflow";
const POSTGRES_PASSWORD: &str = "obzenflow-secret-083c";
const HEALTH_TIMEOUT: Duration = Duration::from_secs(30);

#[cfg(unix)]
static RECEIVED_SIGNAL: AtomicI32 = AtomicI32::new(0);

#[cfg(unix)]
extern "C" fn record_signal(signal: libc::c_int) {
    RECEIVED_SIGNAL.store(signal, Ordering::Relaxed);
}

#[cfg(unix)]
struct SignalGuard {
    previous_interrupt: libc::sighandler_t,
    previous_terminate: libc::sighandler_t,
}

#[cfg(unix)]
impl SignalGuard {
    fn install() -> Result<Self> {
        RECEIVED_SIGNAL.store(0, Ordering::Relaxed);
        // SAFETY: the handler performs only a lock-free atomic store. The two
        // previous process-global handlers are restored when this scoped test
        // session ends.
        let previous_interrupt =
            unsafe { libc::signal(libc::SIGINT, record_signal as *const () as _) };
        if previous_interrupt == libc::SIG_ERR {
            return Err(error(
                "failed to install PostgreSQL test SIGINT cleanup handler",
            ));
        }
        // SAFETY: see the SIGINT installation above.
        let previous_terminate =
            unsafe { libc::signal(libc::SIGTERM, record_signal as *const () as _) };
        if previous_terminate == libc::SIG_ERR {
            // SAFETY: restoring the handler returned by `signal`.
            unsafe { libc::signal(libc::SIGINT, previous_interrupt) };
            return Err(error(
                "failed to install PostgreSQL test SIGTERM cleanup handler",
            ));
        }
        Ok(Self {
            previous_interrupt,
            previous_terminate,
        })
    }
}

#[cfg(unix)]
impl Drop for SignalGuard {
    fn drop(&mut self) {
        // SAFETY: restoring the handlers returned by the matching calls to
        // `signal` in `SignalGuard::install`.
        unsafe {
            libc::signal(libc::SIGINT, self.previous_interrupt);
            libc::signal(libc::SIGTERM, self.previous_terminate);
        }
    }
}

#[cfg(not(unix))]
struct SignalGuard;

#[cfg(not(unix))]
impl SignalGuard {
    fn install() -> Result<Self> {
        Ok(Self)
    }
}

fn check_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let signal = RECEIVED_SIGNAL.load(Ordering::Relaxed);
        if signal != 0 {
            return Err(error(format!(
                "PostgreSQL proof interrupted by signal {signal}"
            )));
        }
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct ComposeCommand {
    program: String,
    prefix: Vec<String>,
}

#[derive(Clone, Debug)]
struct SessionState {
    project: String,
    run_id: String,
    port: u16,
    mode: SessionMode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SessionMode {
    Development,
    Test,
}

impl SessionMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Development => "development",
            Self::Test => "test",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "development" => Ok(Self::Development),
            "test" => Ok(Self::Test),
            _ => Err(error("invalid PostgreSQL session mode")),
        }
    }
}

pub(super) fn run(args: &[String]) -> Result<()> {
    let Some((command, flags)) = args.split_first() else {
        print_help();
        return Ok(());
    };

    match command.as_str() {
        "up" => up(flags),
        "status" => status(flags),
        "run" => run_child(flags),
        "test" => test(flags),
        "logs" => logs(flags),
        "down" => down(flags),
        "cleanup" => cleanup(flags),
        "help" | "-h" | "--help" => {
            print_help();
            Ok(())
        }
        other => Err(error(format!("unknown postgres command: {other}"))),
    }
}

fn up(flags: &[String]) -> Result<()> {
    reject_flags("postgres up", flags)?;
    let root = super::workspace_root()?;
    let compose = preflight_docker()?;
    let directory = root.join(SESSION_ROOT).join(DEVELOPMENT_SESSION);
    let state_path = directory.join(STATE_FILE);

    if state_path.is_file() {
        let mut state = read_state(&state_path)?;
        if container_health(&root, &compose, &directory, &state)? == "healthy" {
            print_status(&state, "healthy");
            return Ok(());
        }
        if !port_available(state.port) {
            state.port = allocate_port()?;
            write_state(&state_path, &state)?;
        }
        ensure_tls(&directory)?;
        start_session(&root, &compose, &directory, &state)?;
        print_status(&state, "healthy");
        return Ok(());
    }

    fs::create_dir_all(&directory)?;
    let run_id = unique_run_id();
    let state = SessionState {
        project: development_project(&root),
        run_id,
        port: allocate_port()?,
        mode: SessionMode::Development,
    };
    ensure_tls(&directory)?;
    write_state(&state_path, &state)?;
    start_session(&root, &compose, &directory, &state)?;
    print_status(&state, "healthy");
    println!("run a command with: cargo xtask postgres run -- <command> [args...]");
    Ok(())
}

fn status(flags: &[String]) -> Result<()> {
    reject_flags("postgres status", flags)?;
    let root = super::workspace_root()?;
    let compose = preflight_docker()?;
    let state_path = development_state_path(&root);
    if !state_path.is_file() {
        println!("no PostgreSQL development session state found");
        report_stale_test_sessions(&root, Some(&compose))?;
        return Ok(());
    }
    let state = read_state(&state_path)?;
    let directory = state_path
        .parent()
        .ok_or_else(|| error("invalid PostgreSQL state path"))?;
    let health = container_health(&root, &compose, directory, &state)?;
    print_status(&state, &health);
    report_stale_test_sessions(&root, Some(&compose))
}

fn run_child(flags: &[String]) -> Result<()> {
    let Some((delimiter, command)) = flags.split_first() else {
        return Err(error("postgres run requires `-- <command> [args...]`"));
    };
    if delimiter != "--" || command.is_empty() {
        return Err(error("postgres run requires `-- <command> [args...]`"));
    }

    let root = super::workspace_root()?;
    let compose = preflight_docker()?;
    let state_path = development_state_path(&root);
    let state = read_state(&state_path).map_err(|_| {
        error("PostgreSQL development session is unavailable; run `cargo xtask postgres up`")
    })?;
    let directory = state_path
        .parent()
        .ok_or_else(|| error("invalid PostgreSQL state path"))?;
    let health = container_health(&root, &compose, directory, &state)?;
    if health != "healthy" {
        return Err(error(format!(
            "PostgreSQL project '{}' is not healthy; inspect with `cargo xtask postgres logs`",
            state.project
        )));
    }

    let mut child = Command::new(&command[0]);
    child
        .current_dir(&root)
        .args(&command[1..])
        .env("OBZENFLOW_POSTGRES_URL", plaintext_url(state.port));
    let status = child.status()?;
    if status.success() {
        Ok(())
    } else {
        Err(error(format!(
            "PostgreSQL child command failed with status {status}"
        )))
    }
}

fn test(flags: &[String]) -> Result<()> {
    reject_flags("postgres test", flags)?;
    let _signal_guard = SignalGuard::install()?;
    let root = super::workspace_root()?;
    let compose = preflight_docker()?;
    report_stale_test_sessions(&root, Some(&compose))?;

    let run_id = unique_run_id();
    let directory = root.join(SESSION_ROOT).join(&run_id);
    fs::create_dir_all(&directory)?;
    let state = SessionState {
        project: format!("obzenflow-test-{run_id}"),
        run_id,
        port: allocate_port()?,
        mode: SessionMode::Test,
    };
    if let Err(error) = ensure_tls(&directory) {
        let _ = fs::remove_dir_all(&directory);
        return Err(error);
    }
    write_state(&directory.join(STATE_FILE), &state)?;

    let proof_result = check_signal()
        .and_then(|_| start_session(&root, &compose, &directory, &state))
        .and_then(|_| check_signal())
        .and_then(|_| run_test_inventory(&root, &directory, &state))
        .and_then(|_| check_signal());
    let _ = capture_logs(&root, &compose, &directory, &state);
    let cleanup_result = stop_session(&root, &compose, &directory, &state, true);
    let key_cleanup_result = remove_ephemeral_keys(&directory);

    match (proof_result, cleanup_result, key_cleanup_result) {
        (Ok(()), Ok(()), Ok(())) => {
            println!(
                "PostgreSQL proof completed; project={} cleaned",
                state.project
            );
            Ok(())
        }
        (Err(proof), cleanup, key_cleanup) => Err(error(format!(
            "{proof}; cleanup={}; key_cleanup={}; captured logs: {}",
            result_label(cleanup),
            result_label(key_cleanup),
            directory.join(LOG_FILE).display()
        ))),
        (Ok(()), Err(cleanup), key_cleanup) => Err(error(format!(
            "PostgreSQL proof passed but cleanup failed: {cleanup}; key_cleanup={}; project={}",
            result_label(key_cleanup),
            state.project
        ))),
        (Ok(()), Ok(()), Err(key_cleanup)) => Err(error(format!(
            "PostgreSQL proof passed but ephemeral key cleanup failed: {key_cleanup}; project={}",
            state.project
        ))),
    }
}

fn logs(flags: &[String]) -> Result<()> {
    reject_flags("postgres logs", flags)?;
    let root = super::workspace_root()?;
    let compose = preflight_docker()?;
    let state_path = development_state_path(&root);
    let state = read_state(&state_path).map_err(|_| {
        error("PostgreSQL development session is unavailable; run `cargo xtask postgres up`")
    })?;
    let directory = state_path
        .parent()
        .ok_or_else(|| error("invalid PostgreSQL state path"))?;
    let output = compose_output(
        &root,
        &compose,
        directory,
        &state,
        &["logs", "--no-color", "postgres"],
    )?;
    print!("{}", String::from_utf8_lossy(&output.stdout));
    eprint!("{}", String::from_utf8_lossy(&output.stderr));
    if output.status.success() {
        Ok(())
    } else {
        Err(error("failed to read PostgreSQL container logs"))
    }
}

fn down(flags: &[String]) -> Result<()> {
    let volumes = match flags {
        [] => false,
        [flag] if flag == "--volumes" => true,
        [flag] if matches!(flag.as_str(), "-h" | "--help" | "help") => {
            print_help();
            return Ok(());
        }
        _ => return Err(error("postgres down accepts only `--volumes`")),
    };
    let root = super::workspace_root()?;
    let state_path = development_state_path(&root);
    if !state_path.is_file() {
        println!("no PostgreSQL development session state found");
        return Ok(());
    }
    let compose = preflight_docker()?;
    let state = read_state(&state_path)?;
    let directory = state_path
        .parent()
        .ok_or_else(|| error("invalid PostgreSQL state path"))?;
    if volumes {
        println!(
            "removing project '{}' and its PostgreSQL data volume; development data will not be recoverable",
            state.project
        );
    }
    stop_session(&root, &compose, directory, &state, volumes)?;
    if volumes {
        fs::remove_dir_all(directory)?;
    } else {
        println!("PostgreSQL project stopped; its named development volume is retained");
    }
    Ok(())
}

fn cleanup(flags: &[String]) -> Result<()> {
    let [run_id] = flags else {
        return Err(error("postgres cleanup requires one reported test run id"));
    };
    if run_id.is_empty()
        || !run_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err(error("invalid PostgreSQL test run id"));
    }
    let root = super::workspace_root()?;
    let directory = root.join(SESSION_ROOT).join(run_id);
    let state_path = directory.join(STATE_FILE);
    let state = read_state(&state_path)?;
    if state.mode != SessionMode::Test || state.run_id != *run_id {
        return Err(error("refusing to clean a non-test PostgreSQL session"));
    }
    let compose = preflight_docker()?;
    let _ = capture_logs(&root, &compose, &directory, &state);
    stop_session(&root, &compose, &directory, &state, true)?;
    remove_ephemeral_keys(&directory)?;
    fs::remove_file(state_path)?;
    println!("PostgreSQL test project '{}' cleaned", state.project);
    Ok(())
}

fn run_test_inventory(root: &Path, directory: &Path, state: &SessionState) -> Result<()> {
    let tests: &[&[&str]] = &[
        &[
            "cargo",
            "test",
            "--locked",
            "-p",
            "obzenflow_adapters",
            "--features",
            "postgres,test-support",
            "--lib",
        ],
        &[
            "cargo",
            "test",
            "--locked",
            "-p",
            "obzenflow_adapters",
            "--features",
            "postgres,test-support",
            "--test",
            "postgres_sink_driver_test",
        ],
        &[
            "cargo",
            "test",
            "--locked",
            "-p",
            "obzenflow_adapters",
            "--features",
            "postgres,test-support",
            "--test",
            "postgres_sink_conformance_test",
        ],
        &[
            "cargo",
            "test",
            "--locked",
            "-p",
            "obzenflow",
            "--features",
            "postgres,test-support",
            "--test",
            "postgres_sink_application_conformance_test",
        ],
        &[
            "cargo",
            "test",
            "--locked",
            "-p",
            "obzenflow",
            "--features",
            "postgres",
            "--test",
            "postgres_public_consumer_test",
        ],
        &[
            "cargo",
            "test",
            "--locked",
            "-p",
            "obzenflow",
            "--test",
            "postgres_connector_package_boundary_test",
        ],
    ];

    for command in tests {
        check_signal()?;
        let mut child = Command::new(command[0]);
        child
            .current_dir(root)
            .args(&command[1..])
            .env("OBZENFLOW_POSTGRES_TEST_URL", plaintext_url(state.port))
            .env("OBZENFLOW_POSTGRES_TEST_RUN_ID", &state.run_id)
            .env(
                "OBZENFLOW_POSTGRES_TEST_TLS_URL",
                tls_url(state.port, "localhost"),
            )
            .env(
                "OBZENFLOW_POSTGRES_TEST_WRONG_HOST_URL",
                tls_url(state.port, "127.0.0.1"),
            )
            .env(
                "OBZENFLOW_POSTGRES_TEST_CA_CERT",
                directory.join("tls/ca.crt"),
            )
            .env(
                "OBZENFLOW_POSTGRES_TEST_UNTRUSTED_CA_CERT",
                directory.join("tls/untrusted-ca.crt"),
            );
        let status = child.status()?;
        if !status.success() {
            return Err(error(format!(
                "required PostgreSQL test target failed with status {status}"
            )));
        }
        check_signal()?;
    }
    Ok(())
}

fn start_session(
    root: &Path,
    compose: &ComposeCommand,
    directory: &Path,
    state: &SessionState,
) -> Result<()> {
    let output = compose_output(root, compose, directory, state, &["up", "-d"])?;
    if !output.status.success() {
        let _ = capture_logs(root, compose, directory, state);
        return Err(error(format!(
            "PostgreSQL failed to start. project={} image={} port={} captured logs: {}",
            state.project,
            IMAGE,
            state.port,
            directory.join(LOG_FILE).display()
        )));
    }
    wait_healthy(root, compose, directory, state)?;
    verify_server(root, compose, directory, state)
}

fn stop_session(
    root: &Path,
    compose: &ComposeCommand,
    directory: &Path,
    state: &SessionState,
    volumes: bool,
) -> Result<()> {
    let mut args = vec!["down", "--remove-orphans"];
    if volumes {
        args.push("--volumes");
    }
    let output = compose_output(root, compose, directory, state, &args)?;
    if output.status.success() {
        Ok(())
    } else {
        Err(error(format!(
            "failed to stop exact PostgreSQL project '{}'",
            state.project
        )))
    }
}

fn wait_healthy(
    root: &Path,
    compose: &ComposeCommand,
    directory: &Path,
    state: &SessionState,
) -> Result<()> {
    let deadline = Instant::now() + HEALTH_TIMEOUT;
    loop {
        check_signal()?;
        let health = container_health(root, compose, directory, state)?;
        if health == "healthy" {
            return Ok(());
        }
        if health == "unhealthy" || Instant::now() >= deadline {
            let _ = capture_logs(root, compose, directory, state);
            return Err(error(format!(
                "PostgreSQL did not become healthy within 30s. project={} image={} port={} captured logs: {}",
                state.project,
                IMAGE,
                state.port,
                directory.join(LOG_FILE).display()
            )));
        }
        thread::sleep(Duration::from_millis(250));
    }
}

fn verify_server(
    root: &Path,
    compose: &ComposeCommand,
    directory: &Path,
    state: &SessionState,
) -> Result<()> {
    let output = compose_output(
        root,
        compose,
        directory,
        state,
        &[
            "exec",
            "-T",
            "postgres",
            "psql",
            "-U",
            POSTGRES_USER,
            "-d",
            POSTGRES_DATABASE,
            "-Atqc",
            "SHOW server_version_num; SHOW fsync;",
        ],
    )?;
    if !output.status.success() {
        return Err(error("PostgreSQL readiness preflight failed"));
    }
    let lines = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let version = lines
        .first()
        .and_then(|line| line.parse::<u32>().ok())
        .ok_or_else(|| error("PostgreSQL readiness returned an invalid server version"))?;
    if !(170_000..180_000).contains(&version) {
        return Err(error("PostgreSQL proof service must run major version 17"));
    }
    if lines.get(1).map(String::as_str) != Some("on") {
        return Err(error("PostgreSQL proof service must keep fsync enabled"));
    }
    Ok(())
}

fn container_health(
    root: &Path,
    compose: &ComposeCommand,
    directory: &Path,
    state: &SessionState,
) -> Result<String> {
    let output = compose_output(root, compose, directory, state, &["ps", "-q", "postgres"])?;
    if !output.status.success() {
        return Ok("unavailable".to_string());
    }
    let id = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if id.is_empty() {
        return Ok("stopped".to_string());
    }
    let output = Command::new("docker")
        .args(["inspect", "--format", "{{.State.Health.Status}}", &id])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()?;
    if !output.status.success() {
        return Ok("unavailable".to_string());
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn capture_logs(
    root: &Path,
    compose: &ComposeCommand,
    directory: &Path,
    state: &SessionState,
) -> Result<()> {
    let output = compose_output(
        root,
        compose,
        directory,
        state,
        &["logs", "--no-color", "postgres"],
    )?;
    let mut file = File::create(directory.join(LOG_FILE))?;
    file.write_all(&output.stdout)?;
    file.write_all(&output.stderr)?;
    Ok(())
}

fn compose_output(
    root: &Path,
    compose: &ComposeCommand,
    directory: &Path,
    state: &SessionState,
    args: &[&str],
) -> Result<Output> {
    let mut command = Command::new(&compose.program);
    command
        .current_dir(root)
        .args(&compose.prefix)
        .arg("-f")
        .arg(root.join(COMPOSE_FILE))
        .arg("-p")
        .arg(&state.project)
        .args(args)
        .env("OBZENFLOW_POSTGRES_PASSWORD", POSTGRES_PASSWORD)
        .env("OBZENFLOW_POSTGRES_PORT", state.port.to_string())
        .env("OBZENFLOW_POSTGRES_TLS_DIR", directory.join("tls"))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    Ok(command.output()?)
}

fn preflight_docker() -> Result<ComposeCommand> {
    let compose = if command_succeeds("docker", &["compose", "version"]) {
        ComposeCommand {
            program: "docker".to_string(),
            prefix: vec!["compose".to_string()],
        }
    } else if command_succeeds("docker-compose", &["version"]) {
        ComposeCommand {
            program: "docker-compose".to_string(),
            prefix: Vec::new(),
        }
    } else {
        return Err(error(
            "Docker Compose is unavailable; install either `docker compose` or `docker-compose`",
        ));
    };
    let context =
        command_stdout("docker", &["context", "show"]).unwrap_or_else(|| "unknown".to_string());
    let info = Command::new("docker")
        .arg("info")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    if !info.is_ok_and(|status| status.success()) {
        if context == "colima" {
            return Err(error(
                "Docker context 'colima' is selected, but its daemon is unavailable. Start it with: colima start",
            ));
        }
        return Err(error(format!(
            "Docker context '{context}' is selected, but its daemon is unavailable"
        )));
    }

    Ok(compose)
}

fn ensure_tls(directory: &Path) -> Result<()> {
    let tls = directory.join("tls");
    fs::create_dir_all(&tls)?;
    let expected = [
        "ca.crt",
        "ca.key",
        "server.crt",
        "server.key",
        "untrusted-ca.crt",
        "untrusted-ca.key",
    ];
    if expected.iter().all(|name| tls.join(name).is_file()) {
        return Ok(());
    }
    if tls.is_dir() {
        fs::remove_dir_all(&tls)?;
        fs::create_dir_all(&tls)?;
    }
    if !command_succeeds("openssl", &["version"]) {
        return Err(error(
            "OpenSSL is required to generate the PostgreSQL proof certificate",
        ));
    }

    let ca_key = tls.join("ca.key");
    let ca_cert = tls.join("ca.crt");
    let server_key = tls.join("server.key");
    let server_csr = tls.join("server.csr");
    let server_cert = tls.join("server.crt");
    let extensions = tls.join("server.ext");
    let untrusted_ca_key = tls.join("untrusted-ca.key");
    let untrusted_ca_cert = tls.join("untrusted-ca.crt");
    fs::write(
        &extensions,
        "subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n",
    )?;

    run_redacted(
        "openssl",
        &[
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-sha256",
            "-days",
            "7",
            "-nodes",
            "-subj",
            "/CN=ObzenFlow PostgreSQL Test CA",
            "-keyout",
            path_arg(&ca_key)?,
            "-out",
            path_arg(&ca_cert)?,
        ],
    )?;
    run_redacted(
        "openssl",
        &[
            "req",
            "-newkey",
            "rsa:2048",
            "-sha256",
            "-nodes",
            "-subj",
            "/CN=localhost",
            "-keyout",
            path_arg(&server_key)?,
            "-out",
            path_arg(&server_csr)?,
        ],
    )?;
    run_redacted(
        "openssl",
        &[
            "x509",
            "-req",
            "-in",
            path_arg(&server_csr)?,
            "-CA",
            path_arg(&ca_cert)?,
            "-CAkey",
            path_arg(&ca_key)?,
            "-CAcreateserial",
            "-days",
            "7",
            "-sha256",
            "-extfile",
            path_arg(&extensions)?,
            "-out",
            path_arg(&server_cert)?,
        ],
    )?;
    run_redacted(
        "openssl",
        &[
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-sha256",
            "-days",
            "7",
            "-nodes",
            "-subj",
            "/CN=ObzenFlow Untrusted PostgreSQL Test CA",
            "-keyout",
            path_arg(&untrusted_ca_key)?,
            "-out",
            path_arg(&untrusted_ca_cert)?,
        ],
    )?;
    set_private_permissions(&ca_key)?;
    set_private_permissions(&server_key)?;
    set_private_permissions(&untrusted_ca_key)?;
    Ok(())
}

fn remove_ephemeral_keys(directory: &Path) -> Result<()> {
    let tls = directory.join("tls");
    if tls.is_dir() {
        fs::remove_dir_all(tls)?;
    }
    Ok(())
}

#[cfg(unix)]
fn set_private_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_private_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

fn run_redacted(program: &str, args: &[&str]) -> Result<()> {
    let status = Command::new(program)
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|_| error(format!("failed to launch {program}")))?;
    if status.success() {
        Ok(())
    } else {
        Err(error(format!("{program} failed with status {status}")))
    }
}

fn path_arg(path: &Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| error("PostgreSQL session path is not valid Unicode"))
}

fn write_state(path: &Path, state: &SessionState) -> Result<()> {
    let mut file = File::create(path)?;
    writeln!(file, "# obzenflow xtask postgres v1")?;
    writeln!(file, "project\t{}", state.project)?;
    writeln!(file, "run_id\t{}", state.run_id)?;
    writeln!(file, "port\t{}", state.port)?;
    writeln!(file, "mode\t{}", state.mode.as_str())?;
    Ok(())
}

fn read_state(path: &Path) -> Result<SessionState> {
    let contents = fs::read_to_string(path)?;
    let mut project = None;
    let mut run_id = None;
    let mut port = None;
    let mut mode = None;
    for line in contents.lines() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (key, value) = line
            .split_once('\t')
            .ok_or_else(|| error("invalid PostgreSQL session state"))?;
        match key {
            "project" => project = Some(value.to_string()),
            "run_id" => run_id = Some(value.to_string()),
            "port" => port = Some(value.parse::<u16>()?),
            "mode" => mode = Some(SessionMode::parse(value)?),
            _ => return Err(error("invalid PostgreSQL session state field")),
        }
    }
    Ok(SessionState {
        project: project.ok_or_else(|| error("PostgreSQL session project is missing"))?,
        run_id: run_id.ok_or_else(|| error("PostgreSQL session run id is missing"))?,
        port: port.ok_or_else(|| error("PostgreSQL session port is missing"))?,
        mode: mode.ok_or_else(|| error("PostgreSQL session mode is missing"))?,
    })
}

fn report_stale_test_sessions(root: &Path, compose: Option<&ComposeCommand>) -> Result<()> {
    let session_root = root.join(SESSION_ROOT);
    if !session_root.is_dir() {
        return Ok(());
    }
    for entry in fs::read_dir(&session_root)? {
        let entry = entry?;
        if entry.file_name() == DEVELOPMENT_SESSION {
            continue;
        }
        let path = entry.path();
        let state_path = path.join(STATE_FILE);
        if !state_path.is_file() {
            continue;
        }
        let state = read_state(&state_path)?;
        if state.mode != SessionMode::Test {
            continue;
        }
        let health = match compose {
            Some(compose) => container_health(root, compose, &path, &state)?,
            None => "unknown".to_string(),
        };
        if !matches!(health.as_str(), "stopped" | "unavailable") {
            println!(
                "stale PostgreSQL test project detected: project={} run_id={} health={}; exact state: {}",
                state.project,
                state.run_id,
                health,
                state_path.display()
            );
            println!(
                "clean it with: cargo xtask postgres cleanup {}",
                state.run_id
            );
        }
    }
    Ok(())
}

fn development_state_path(root: &Path) -> PathBuf {
    root.join(SESSION_ROOT)
        .join(DEVELOPMENT_SESSION)
        .join(STATE_FILE)
}

fn unique_run_id() -> String {
    Uuid::new_v4().simple().to_string()
}

fn development_project(root: &Path) -> String {
    let canonical = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    canonical.hash(&mut hasher);
    format!("obzenflow-postgres-{:x}", hasher.finish())
}

fn allocate_port() -> Result<u16> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

fn port_available(port: u16) -> bool {
    TcpListener::bind(("127.0.0.1", port)).is_ok()
}

fn plaintext_url(port: u16) -> String {
    format!(
        "postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@localhost:{port}/{POSTGRES_DATABASE}?sslmode=disable"
    )
}

fn tls_url(port: u16, host: &str) -> String {
    format!(
        "postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{host}:{port}/{POSTGRES_DATABASE}?sslmode=verify-full"
    )
}

fn print_status(state: &SessionState, health: &str) {
    println!(
        "project={} image={} health={} host=127.0.0.1 port={}",
        state.project, IMAGE, health, state.port
    );
}

fn reject_flags(command: &str, flags: &[String]) -> Result<()> {
    if flags.is_empty() {
        return Ok(());
    }
    if flags.len() == 1 && matches!(flags[0].as_str(), "-h" | "--help" | "help") {
        print_help();
        return Ok(());
    }
    Err(error(format!("{command} accepts no options")))
}

fn command_succeeds(program: &str, args: &[&str]) -> bool {
    Command::new(program)
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn command_stdout(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn result_label(result: Result<()>) -> String {
    match result {
        Ok(()) => "ok".to_string(),
        Err(error) => error.to_string(),
    }
}

fn print_help() {
    println!("usage:");
    println!("  cargo xtask postgres up");
    println!("  cargo xtask postgres status");
    println!("  cargo xtask postgres run -- <command> [args...]");
    println!("  cargo xtask postgres test");
    println!("  cargo xtask postgres logs");
    println!("  cargo xtask postgres down [--volumes]");
    println!("  cargo xtask postgres cleanup <reported-test-run-id>");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn state_round_trips_without_credentials() {
        let directory = env::temp_dir().join(format!("obzenflow-postgres-{}", unique_run_id()));
        fs::create_dir_all(&directory).expect("create test directory");
        let path = directory.join(STATE_FILE);
        let state = SessionState {
            project: "obzenflow-test-state".to_string(),
            run_id: "run-state".to_string(),
            port: 15432,
            mode: SessionMode::Test,
        };
        write_state(&path, &state).expect("write state");
        let contents = fs::read_to_string(&path).expect("read state");
        assert!(!contents.contains(POSTGRES_PASSWORD));
        assert!(!contents.contains("postgres://"));
        let restored = read_state(&path).expect("restore state");
        assert_eq!(restored.project, state.project);
        assert_eq!(restored.run_id, state.run_id);
        assert_eq!(restored.port, state.port);
        assert_eq!(restored.mode, state.mode);
        fs::remove_dir_all(directory).expect("remove test directory");
    }

    #[test]
    fn allocated_port_is_loopback_bindable() {
        let port = allocate_port().expect("allocate port");
        assert!(port_available(port));
    }
}
