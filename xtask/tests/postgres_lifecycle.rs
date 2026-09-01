// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Black-box lifecycle verification for the public PostgreSQL xtask commands.

use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions, PgSslMode},
    PgPool,
};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};
use uuid::Uuid;

const SESSION_ENV: &str = "OBZENFLOW_POSTGRES_XTASK_PROOF_SESSION";
const TRANSPORT_ENV: &str = "OBZENFLOW_POSTGRES_TRANSPORT";

#[derive(Debug, PartialEq, Eq)]
struct SessionState {
    project: String,
    run_id: String,
    port: u16,
    volume: String,
}

struct CleanupGuard {
    root: PathBuf,
    token: String,
    armed: bool,
}

impl CleanupGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = xtask(&self.root, &self.token, &["down", "--volumes"], &[]);
        }
    }
}

struct StateRestoreGuard {
    original: PathBuf,
    backup: PathBuf,
    armed: bool,
}

impl StateRestoreGuard {
    fn conceal(original: &Path, backup: &Path) -> Self {
        assert!(!backup.exists(), "state backup path is unique");
        fs::rename(original, backup).expect("conceal development state");
        Self {
            original: original.to_path_buf(),
            backup: backup.to_path_buf(),
            armed: true,
        }
    }

    fn restore(&mut self) {
        assert!(
            !self.original.exists(),
            "rejected first-up did not recreate development state"
        );
        fs::rename(&self.backup, &self.original).expect("restore development state");
        self.armed = false;
    }
}

impl Drop for StateRestoreGuard {
    fn drop(&mut self) {
        if self.armed {
            if self.original.exists() {
                let _ = fs::remove_dir_all(&self.original);
            }
            let _ = fs::rename(&self.backup, &self.original);
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires the repository PostgreSQL test environment"]
async fn public_commands_preserve_rows_credentials_and_environment_boundaries() {
    let root = workspace_root();
    let primary_project = required_env("OBZENFLOW_POSTGRES_TEST_PROJECT");
    let primary_run_id = required_env("OBZENFLOW_POSTGRES_TEST_RUN_ID");
    let primary_port = required_env("OBZENFLOW_POSTGRES_TEST_PORT")
        .parse::<u16>()
        .expect("primary port is numeric");
    let primary_pgpass = PathBuf::from(required_env("PGPASSFILE"));
    let primary_secret = pgpass_secret(&primary_pgpass, primary_port);
    let primary = PgPool::connect(&required_env("OBZENFLOW_POSTGRES_TEST_URL"))
        .await
        .expect("connect to the primary PostgreSQL test session");
    assert_eq!(query_one(&primary).await, 1);

    let token = Uuid::new_v4().simple().to_string();
    let directory = root
        .join("target/postgres-sessions")
        .join(format!("persistent-{token}"));
    let state_path = directory.join("state.tsv");
    let raw_path = directory.join("password");
    let pgpass_path = directory.join("pgpass");
    let journal_root = root.join("target/postgres-lifecycle-journals").join(&token);
    let mut cleanup = CleanupGuard {
        root: root.clone(),
        token: token.clone(),
        armed: true,
    };

    let up = xtask(&root, &token, &["up"], &[]);
    let up_text = assert_success("up", up, &[]);
    let initial = read_state(&state_path);
    let raw = fs::read_to_string(&raw_path).expect("read generated raw credential");
    assert!(
        valid_secret(&raw),
        "raw credential is 64 lowercase hex bytes"
    );
    assert_ne!(
        raw, primary_secret,
        "test and development credentials differ"
    );
    assert!(
        !up_text.contains(&raw),
        "up does not print the managed secret"
    );
    assert_private_directory(&directory);
    assert_private_file(&state_path);
    assert_private_file(&raw_path);
    assert_private_file(&pgpass_path);
    assert!(
        !directory.join("tls").exists(),
        "development creates no TLS material"
    );
    assert_eq!(
        fs::read_to_string(&pgpass_path).expect("read generated pgpass"),
        format!("localhost:{}:obzenflow:obzenflow:{raw}\n", initial.port)
    );

    assert_ne!(initial.project, primary_project);
    assert_ne!(initial.run_id, primary_run_id);
    assert_ne!(initial.port, primary_port);
    assert_ne!(pgpass_path, primary_pgpass);

    assert_success("status", xtask(&root, &token, &["status"], &[]), &[&raw]);
    let connection = assert_success(
        "connection",
        xtask(&root, &token, &["connection"], &[]),
        &[&raw],
    );
    for field in [
        "project:",
        "container:",
        "volume:",
        "client host:",
        "port:",
        "database:",
        "schema:",
        "transport:",
        "pgpass:",
        "state:",
    ] {
        assert!(
            connection.contains(field),
            "connection profile omitted {field}"
        );
    }
    assert!(connection.contains("env -u PGPASSWORD"));
    assert!(connection.contains("postgresql://obzenflow@localhost:"));

    let captured = assert_success(
        "run environment capture",
        xtask(
            &root,
            &token,
            &["run", "--", "env"],
            &[
                ("PGPASSWORD", "ambient-wrong-password"),
                ("OBZENFLOW_POSTGRES_TEST_SENTINEL", "must-not-cross"),
                ("OBZENFLOW_POSTGRES_PASSWORD", "legacy-ambient-password"),
                ("OBZENFLOW_POSTGRES_EXAMPLE_SCHEMA", "acceptance-only"),
                (TRANSPORT_ENV, "verified-tls"),
            ],
        ),
        &[&raw],
    );
    let captured = parse_environment(&captured);
    let development_url = captured
        .get("OBZENFLOW_POSTGRES_URL")
        .expect("development URL is supplied");
    assert!(development_url.starts_with(&format!(
        "postgresql://obzenflow@localhost:{}/obzenflow?",
        initial.port
    )));
    assert!(development_url.contains("sslmode=disable"));
    assert!(!development_url.contains(&raw));
    assert_eq!(
        captured.get(TRANSPORT_ENV).map(String::as_str),
        Some("externally-protected-plaintext")
    );
    assert_eq!(
        captured.get("PGPASSFILE"),
        Some(&pgpass_path.display().to_string())
    );
    assert_eq!(
        captured
            .get("OBZENFLOW_POSTGRES_SCHEMA")
            .map(String::as_str),
        Some("obzenflow_example")
    );
    assert!(!captured.contains_key("PGPASSWORD"));
    assert!(!captured.contains_key(SESSION_ENV));
    assert!(!captured.contains_key("OBZENFLOW_POSTGRES_PASSWORD"));
    assert!(!captured.contains_key("OBZENFLOW_POSTGRES_PASSWORD_FILE"));
    assert!(!captured.contains_key("OBZENFLOW_POSTGRES_EXAMPLE_SCHEMA"));
    assert!(
        captured
            .keys()
            .all(|name| !name.starts_with("OBZENFLOW_POSTGRES_TEST_")),
        "development child receives no acceptance-only environment"
    );

    assert_success(
        "payments example",
        xtask(
            &root,
            &token,
            &[
                "run",
                "--",
                "cargo",
                "run",
                "-p",
                "obzenflow",
                "--features",
                "postgres",
                "--example",
                "postgres_sink_payments",
            ],
            &[(
                "OBZENFLOW_JOURNAL_ROOT",
                journal_root.to_str().expect("journal path is Unicode"),
            )],
        ),
        &[&raw],
    );

    let secondary = development_pool(initial.port, &raw).await;
    assert_payment_rows(&secondary).await;
    secondary.close().await;

    let initial_raw = fs::read(&raw_path).expect("snapshot raw credential");
    let initial_pgpass = fs::read(&pgpass_path).expect("snapshot pgpass credential");
    let initial_state = fs::read(&state_path).expect("snapshot development state");
    assert_success("down", xtask(&root, &token, &["down"], &[]), &[&raw]);
    assert!(state_path.is_file(), "normal down retains session state");
    assert_eq!(fs::read(&raw_path).expect("retained raw"), initial_raw);
    assert_eq!(
        fs::read(&pgpass_path).expect("retained pgpass"),
        initial_pgpass
    );

    let project_filter = format!("label=com.docker.compose.project={}", initial.project);
    let containers_before = docker_stdout(&["ps", "--all", "--quiet", "--filter", &project_filter]);
    let networks_before = docker_stdout(&["network", "ls", "--quiet", "--filter", &project_filter]);
    assert!(
        containers_before.trim().is_empty(),
        "normal down removes the development container"
    );
    assert!(
        networks_before.trim().is_empty(),
        "normal down removes the development network"
    );
    let volume_before = docker_stdout(&["volume", "inspect", &initial.volume]);

    let backup = directory
        .parent()
        .expect("development state has a parent")
        .join(format!(".persistent-{token}-a1-backup"));
    let mut state_restore = StateRestoreGuard::conceal(&directory, &backup);
    let rejected = assert_failure(
        "state-less retained-volume up",
        xtask(&root, &token, &["up"], &[]),
        &[&raw],
    );
    for expected in [
        initial.project.as_str(),
        initial.volume.as_str(),
        state_path.to_str().expect("state path is Unicode"),
        "refusing to generate a replacement credential",
    ] {
        assert!(
            rejected.contains(expected),
            "retained-volume refusal omitted {expected}"
        );
    }
    assert!(!rejected.contains("down --volumes"));
    assert!(!directory.exists(), "rejected up creates no local state");
    assert_eq!(
        docker_stdout(&["ps", "--all", "--quiet", "--filter", &project_filter,]),
        containers_before,
        "rejected up creates no development container"
    );
    assert_eq!(
        docker_stdout(&["network", "ls", "--quiet", "--filter", &project_filter]),
        networks_before,
        "rejected up creates no development network"
    );
    assert_eq!(
        docker_stdout(&["volume", "inspect", &initial.volume]),
        volume_before,
        "rejected up does not mutate the retained volume"
    );
    state_restore.restore();

    assert_success("restart", xtask(&root, &token, &["up"], &[]), &[&raw]);
    let restarted = read_state(&state_path);
    assert_eq!(restarted, initial, "normal restart retains exact authority");
    assert_eq!(
        fs::read(&state_path).expect("restarted state"),
        initial_state
    );
    assert_eq!(fs::read(&raw_path).expect("restarted raw"), initial_raw);
    assert_eq!(
        fs::read(&pgpass_path).expect("restarted pgpass"),
        initial_pgpass
    );
    assert!(
        !directory.join("tls").exists(),
        "restart creates no development TLS material"
    );

    let secondary = development_pool(restarted.port, &raw).await;
    assert_payment_rows(&secondary).await;
    secondary.close().await;
    assert_eq!(
        query_one(&primary).await,
        1,
        "the disposable acceptance session remains healthy"
    );

    assert_success("logs", xtask(&root, &token, &["logs"], &[]), &[&raw]);
    assert_success(
        "down --volumes",
        xtask(&root, &token, &["down", "--volumes"], &[]),
        &[&raw],
    );
    assert!(
        !directory.exists(),
        "volume cleanup removes the exact owned session directory"
    );
    if journal_root.exists() {
        fs::remove_dir_all(&journal_root).expect("remove lifecycle journals");
    }
    cleanup.disarm();
    primary.close().await;
}

fn xtask(root: &Path, token: &str, args: &[&str], environment: &[(&str, &str)]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_xtask"));
    command
        .current_dir(root)
        .arg("postgres")
        .args(args)
        .env(SESSION_ENV, token);
    for (name, value) in environment {
        command.env(name, value);
    }
    command.output().expect("launch the public xtask binary")
}

fn assert_success(label: &str, output: Output, forbidden: &[&str]) -> String {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    for text in [&stdout, &stderr] {
        for secret in forbidden {
            assert!(!text.contains(secret), "{label} exposed a managed secret");
        }
        assert!(
            !text.contains("postgres://obzenflow:") && !text.contains("postgresql://obzenflow:"),
            "{label} exposed a password-bearing connection URL"
        );
    }
    assert!(
        output.status.success(),
        "{label} failed with {}; stdout={}; stderr={}",
        output.status,
        stdout.trim(),
        stderr.trim()
    );
    stdout.into_owned()
}

fn assert_failure(label: &str, output: Output, forbidden: &[&str]) -> String {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    for text in [&stdout, &stderr] {
        for secret in forbidden {
            assert!(!text.contains(secret), "{label} exposed a managed secret");
        }
        assert!(
            !text.contains("postgres://obzenflow:") && !text.contains("postgresql://obzenflow:"),
            "{label} exposed a password-bearing connection URL"
        );
    }
    assert!(
        !output.status.success(),
        "{label} unexpectedly succeeded; stdout={}; stderr={}",
        stdout.trim(),
        stderr.trim()
    );
    format!("{stdout}\n{stderr}")
}

fn docker_stdout(args: &[&str]) -> String {
    let output = Command::new("docker")
        .args(args)
        .output()
        .expect("launch Docker evidence query");
    assert!(
        output.status.success(),
        "Docker evidence query failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
    String::from_utf8(output.stdout).expect("Docker evidence is UTF-8")
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask is a workspace member")
        .to_path_buf()
}

fn required_env(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("{name} is required from `cargo xtask postgres test`"))
}

fn valid_secret(secret: &str) -> bool {
    secret.len() == 64
        && secret
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn pgpass_secret(path: &Path, port: u16) -> String {
    fs::read_to_string(path)
        .expect("read outer pgpass")
        .lines()
        .find_map(|line| {
            let fields = line.split(':').collect::<Vec<_>>();
            (fields.len() == 5 && fields[1] == port.to_string()).then(|| fields[4].to_string())
        })
        .expect("outer pgpass contains its exact port")
}

async fn development_pool(port: u16, password: &str) -> PgPool {
    let options = PgConnectOptions::new_without_pgpass()
        .host("localhost")
        .port(port)
        .username("obzenflow")
        .password(password)
        .database("obzenflow")
        .ssl_mode(PgSslMode::Disable);
    PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options)
        .await
        .expect("connect to the isolated development session")
}

async fn query_one(pool: &PgPool) -> i32 {
    sqlx::query_scalar("SELECT 1")
        .fetch_one(pool)
        .await
        .expect("query PostgreSQL readiness")
}

async fn assert_payment_rows(pool: &PgPool) {
    let rows = sqlx::query_as::<_, (i64, String, String, i64)>(
        "SELECT payment_id, order_id, customer_id, amount_cents \
         FROM obzenflow_example.payments ORDER BY payment_id",
    )
    .fetch_all(pool)
    .await
    .expect("inspect development payment rows");
    assert_eq!(
        rows,
        vec![
            (
                1001,
                "order-501".to_string(),
                "customer-71".to_string(),
                12_500,
            ),
            (
                1002,
                "order-502".to_string(),
                "customer-93".to_string(),
                8_750,
            ),
        ]
    );
}

fn parse_environment(stdout: &str) -> BTreeMap<String, String> {
    stdout
        .lines()
        .filter_map(|line| line.split_once('='))
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect()
}

fn read_state(path: &Path) -> SessionState {
    let contents = fs::read_to_string(path).expect("read xtask session state");
    assert_eq!(
        contents.lines().next(),
        Some("# obzenflow xtask postgres v3")
    );
    assert!(!contents.contains("password"));
    let field = |name: &str| {
        contents
            .lines()
            .filter_map(|line| line.split_once('\t'))
            .find_map(|(key, value)| (key == name).then_some(value.to_string()))
            .unwrap_or_else(|| panic!("state field {name} exists"))
    };
    SessionState {
        project: field("project"),
        run_id: field("run_id"),
        port: field("port").parse().expect("state port is numeric"),
        volume: field("volume"),
    }
}

#[cfg(unix)]
fn assert_private_directory(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let metadata = fs::symlink_metadata(path).expect("inspect private directory");
    assert!(metadata.is_dir() && !metadata.file_type().is_symlink());
    assert_eq!(metadata.permissions().mode() & 0o777, 0o700);
}

#[cfg(not(unix))]
fn assert_private_directory(path: &Path) {
    assert!(path.is_dir());
}

#[cfg(unix)]
fn assert_private_file(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let metadata = fs::symlink_metadata(path).expect("inspect private file");
    assert!(metadata.is_file() && !metadata.file_type().is_symlink());
    assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
}

#[cfg(not(unix))]
fn assert_private_file(path: &Path) {
    assert!(path.is_file());
}
