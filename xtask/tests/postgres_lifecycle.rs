// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Black-box lifecycle verification for the public PostgreSQL xtask commands.

use sqlx::PgPool;
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};
use uuid::Uuid;

const SESSION_ENV: &str = "OBZENFLOW_POSTGRES_XTASK_PROOF_SESSION";
const PASSWORD: &str = "obzenflow-secret-083c";

#[derive(Debug, PartialEq, Eq)]
struct SessionState {
    project: String,
    run_id: String,
    port: u16,
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
            let _ = xtask(&self.root, &self.token, &["down", "--volumes"]);
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires the repository PostgreSQL test environment"]
async fn public_commands_preserve_data_and_isolate_concurrent_sessions() {
    let root = workspace_root();
    let primary_project = required_env("OBZENFLOW_POSTGRES_TEST_PROJECT");
    let primary_run_id = required_env("OBZENFLOW_POSTGRES_TEST_RUN_ID");
    let primary_port = required_env("OBZENFLOW_POSTGRES_TEST_PORT")
        .parse::<u16>()
        .expect("primary port is numeric");
    let primary = PgPool::connect(&required_env("OBZENFLOW_POSTGRES_TEST_URL"))
        .await
        .expect("connect to the primary PostgreSQL test session");
    assert_eq!(query_one(&primary).await, 1);

    let token = Uuid::new_v4().simple().to_string();
    let directory = root
        .join("target/postgres-sessions")
        .join(format!("persistent-{token}"));
    let state_path = directory.join("state.tsv");
    let mut cleanup = CleanupGuard {
        root: root.clone(),
        token: token.clone(),
        armed: true,
    };

    assert_success("up", xtask(&root, &token, &["up"]));
    assert_success("status", xtask(&root, &token, &["status"]));
    let initial = read_state(&state_path);
    assert_ne!(initial.project, primary_project);
    assert_ne!(initial.run_id, primary_run_id);
    assert_ne!(initial.port, primary_port);

    let secondary = PgPool::connect(&plaintext_url(initial.port))
        .await
        .expect("connect to the isolated development session");
    sqlx::query(
        "CREATE TABLE public.xtask_lifecycle_marker (id BIGINT PRIMARY KEY, value TEXT NOT NULL)",
    )
    .execute(&secondary)
    .await
    .expect("create retained-volume marker table");
    sqlx::query("INSERT INTO public.xtask_lifecycle_marker VALUES (1, 'retained')")
        .execute(&secondary)
        .await
        .expect("insert retained-volume marker");
    secondary.close().await;

    assert_success("down", xtask(&root, &token, &["down"]));
    assert!(state_path.is_file(), "normal down retains session state");
    assert_success("restart", xtask(&root, &token, &["up"]));
    let restarted = read_state(&state_path);
    assert_eq!(restarted.project, initial.project);
    assert_eq!(restarted.run_id, initial.run_id);

    let secondary = PgPool::connect(&plaintext_url(restarted.port))
        .await
        .expect("reconnect after normal down and up");
    let marker = sqlx::query_scalar::<_, String>(
        "SELECT value FROM public.xtask_lifecycle_marker WHERE id = 1",
    )
    .fetch_one(&secondary)
    .await
    .expect("read retained-volume marker");
    assert_eq!(marker, "retained");
    secondary.close().await;

    assert_eq!(
        query_one(&primary).await,
        1,
        "the primary session remains healthy"
    );
    assert_success("logs", xtask(&root, &token, &["logs"]));
    assert_success(
        "down --volumes",
        xtask(&root, &token, &["down", "--volumes"]),
    );
    assert!(
        !directory.exists(),
        "volume cleanup removes the owned session directory"
    );
    cleanup.disarm();
    primary.close().await;
}

fn xtask(root: &Path, token: &str, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_xtask"))
        .current_dir(root)
        .arg("postgres")
        .args(args)
        .env(SESSION_ENV, token)
        .output()
        .expect("launch the public xtask binary")
}

fn assert_success(label: &str, output: Output) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    for text in [&stdout, &stderr] {
        assert!(
            !text.contains(PASSWORD),
            "{label} exposed the fixture password"
        );
        assert!(
            !text.contains("postgres://obzenflow:"),
            "{label} exposed a connection URL"
        );
    }
    assert!(
        output.status.success(),
        "{label} failed with {}; stdout={}; stderr={}",
        output.status,
        stdout.trim(),
        stderr.trim()
    );
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

fn plaintext_url(port: u16) -> String {
    format!("postgres://obzenflow:{PASSWORD}@localhost:{port}/obzenflow?sslmode=disable")
}

async fn query_one(pool: &PgPool) -> i32 {
    sqlx::query_scalar("SELECT 1")
        .fetch_one(pool)
        .await
        .expect("query PostgreSQL readiness")
}

fn read_state(path: &Path) -> SessionState {
    let contents = fs::read_to_string(path).expect("read xtask session state");
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
    }
}
