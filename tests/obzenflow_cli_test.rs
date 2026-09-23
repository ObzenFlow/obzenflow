// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-095j: the `obzenflow verify` subcommand's OS-level exit codes.
//!
//! The binary is a thin shell over `obzenflow_infra::verify`; this suite
//! asserts the process boundary: `0` certified match with the headline line
//! on stdout, `3` refusal for an unavailable archive. (Divergence and
//! uncertified verdicts are exercised in-process by the other verification
//! suites; the contract mapping itself is unit-tested in `verdict`.)

use obzenflow::application::FlowApplication;
use obzenflow::flow::{flow, sink, source, FlowDefinition};
use obzenflow::journal::disk_journals;
use obzenflow::schema::TypedPayload;
use obzenflow::stages::sinks::{DeliveryContext, SinkTyped};
use obzenflow::stages::sources::{SourceError, TypedFiniteSourceHandler};
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Tick {
    n: u64,
}

impl TypedPayload for Tick {
    const EVENT_TYPE: &'static str = "cli_verify.tick";
}

#[derive(Clone, Debug)]
struct Ticks {
    next: u64,
}

impl Ticks {
    fn new() -> Self {
        Self { next: 1 }
    }
}

impl TypedFiniteSourceHandler for Ticks {
    type Output = Tick;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.next > 3 {
            return Ok(None);
        }
        let n = self.next;
        self.next += 1;
        Ok(Some(vec![Tick { n }]))
    }
}

fn discard<T>() -> impl FnMut(T, DeliveryContext) -> std::future::Ready<()> + Send + Sync + Clone
where
    T: Clone + Send + Sync + 'static,
{
    move |_payload: T, _delivery| std::future::ready(())
}

fn build_flow(journal_base: PathBuf) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let ticks_handler = Ticks::new();
        let out_handler = SinkTyped::with_delivery(discard::<Tick>()).idempotent();

        Ok(flow! {
            name: "cli_verify",
            journals: disk_journals(journal_base),

            stages: {
                ticks = source!(Tick => ticks_handler);
                out = sink!(Tick => out_handler);
            },

            topology: {
                ticks |> out;
            }
        })
    })
}

fn latest_run_dir(base: &Path) -> PathBuf {
    let flows_dir = base.join("flows");
    let mut entries: Vec<PathBuf> = std::fs::read_dir(&flows_dir)
        .expect("flows directory should exist")
        .map(|entry| entry.expect("flow dir entry").path())
        .filter(|path| path.join("run_manifest.json").exists())
        .collect();
    entries.sort();
    entries.pop().expect("run should have produced an archive")
}

#[tokio::test(flavor = "multi_thread")]
async fn cli_verify_exit_codes_follow_the_contract() {
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(journal_base.clone()))
        .await
        .expect("live flow should complete");
    let baseline = latest_run_dir(&journal_base);
    let files_before: std::collections::BTreeMap<_, _> = std::fs::read_dir(&baseline)
        .unwrap()
        .map(Result::unwrap)
        .filter(|entry| entry.file_type().unwrap().is_file())
        .map(|entry| (entry.path(), std::fs::read(entry.path()).unwrap()))
        .collect();
    let mut expected = std::collections::BTreeMap::new();
    let mut snapshot = obzenflow::journal::read::open_disk_run(&baseline)
        .await
        .unwrap();
    while let Some(row) = snapshot.next().await.unwrap() {
        expected.insert(
            (row.journal.id, row.position),
            serde_json::to_value(row).unwrap(),
        );
    }

    // Both views consume the shared projection, including settlement evidence.
    for follow in [false, true] {
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_obzenflow"));
        command
            .arg("show")
            .arg(&baseline)
            .arg("--json")
            .kill_on_drop(true);
        if follow {
            command.arg("--follow");
        }
        let output = tokio::time::timeout(std::time::Duration::from_secs(10), command.output())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(0),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let rows: Vec<obzenflow::journal::read::RunRecord> = String::from_utf8(output.stdout)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert!(!rows.is_empty());
        assert_eq!(
            rows.iter()
                .filter(|r| r.kind == obzenflow::journal::read::RunRecordKind::SourceFact)
                .count(),
            3
        );
        assert!(rows.iter().all(|r| r.version == 1));
        let mut positions = std::collections::BTreeMap::new();
        for row in &rows {
            let next = positions.entry(row.journal.id).or_insert(0);
            assert_eq!(
                row.position.0, *next,
                "no skipped or repeated logical records"
            );
            *next += 1;
            assert_eq!(
                serde_json::to_value(row).unwrap(),
                expected[&(row.journal.id, row.position)]
            );
        }
        if !follow {
            assert_eq!(rows.len(), expected.len());
        }
        if follow {
            assert!(String::from_utf8_lossy(&output.stderr).contains("run_observation_covered"));
        }
    }
    for (file, bytes) in files_before {
        assert_eq!(
            std::fs::read(file).unwrap(),
            bytes,
            "observation cannot change archive evidence"
        );
    }

    let export = Command::new(env!("CARGO_BIN_EXE_obzenflow"))
        .args(["journal", "export-jsonl"])
        .arg(&baseline)
        .output()
        .expect("journal export process");
    assert!(
        export.status.success(),
        "{}",
        String::from_utf8_lossy(&export.stderr)
    );
    let records: Vec<serde_json::Value> = String::from_utf8(export.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).expect("canonical record JSON"))
        .collect();
    assert!(!records.is_empty());
    for record in &records {
        let roots = record.as_object().unwrap();
        assert_eq!(roots.len(), 2);
        assert!(roots.contains_key("envelope") && roots.contains_key("payload"));
        assert!(record["envelope"]["provenance"]["event"]["id"].is_string());
        assert!(record["envelope"]["provenance"]["journal"].is_object());
    }
    let facts: Vec<_> = records
        .iter()
        .filter(|record| record["envelope"]["provenance"]["event"]["event_kind"] == "fact")
        .collect();
    assert_eq!(facts.len(), 3);
    assert!(facts.iter().all(|record| record["payload"]["n"].is_u64()));

    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            baseline.as_os_str().to_os_string(),
        ])
        .run_async(build_flow(journal_base.clone()))
        .await
        .expect("replay flow should complete");
    let candidate = latest_run_dir(&journal_base);

    // Certified match: exit 0 and the headline line on stdout.
    let output = Command::new(env!("CARGO_BIN_EXE_obzenflow"))
        .args(["verify", "--baseline"])
        .arg(&baseline)
        .arg("--candidate")
        .arg(&candidate)
        .output()
        .expect("obzenflow binary should run");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(
        output.status.code(),
        Some(0),
        "certified match exits 0: {stdout}"
    );
    assert!(
        stdout.contains("output matched the original run, 0 differences"),
        "the headline line prints on a certified match: {stdout}"
    );

    // Refusal: a missing baseline exits 3 with the reason on stdout.
    let output = Command::new(env!("CARGO_BIN_EXE_obzenflow"))
        .args(["verify", "--baseline"])
        .arg(temp.path().join("no-such-run"))
        .arg("--candidate")
        .arg(&candidate)
        .output()
        .expect("obzenflow binary should run");
    assert_eq!(output.status.code(), Some(3), "refusals exit 3");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("verification refused"),
        "the refusal names its reason: {stdout}"
    );
}

#[test]
fn cli_version_commands_and_admission_errors_are_public() {
    let output = Command::new(env!("CARGO_BIN_EXE_obzenflow"))
        .arg("--version")
        .output()
        .unwrap();
    let version = String::from_utf8(output.stdout).unwrap();
    assert!(version.contains(env!("CARGO_PKG_VERSION")));
    assert!(version.contains(&format!(
        "journal schema {}",
        obzenflow::journal::JOURNAL_SCHEMA_VERSION
    )));
    let output = Command::new(env!("CARGO_BIN_EXE_obzenflow"))
        .arg("--help")
        .output()
        .unwrap();
    let help = String::from_utf8(output.stdout).unwrap();
    for command in ["show", "start", "journal", "verify"] {
        assert!(help.contains(command));
    }
    assert!(!help.contains("config"));
    let dir = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_obzenflow"))
        .arg("show")
        .arg(dir.path())
        .arg("--json")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(4));
    assert!(output.stdout.is_empty());
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

mod control_client {
    use super::*;
    use serde_json::{json, Value};
    use std::time::Duration;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

    fn target(host: &str) -> Value {
        json!({"runtime_instance_id":host,"pipeline_writer_id":{"type":"System","id":"01H00000000000000000000000"}})
    }

    fn discovery(archive: Value) -> Value {
        let value = json!({"protocol_version":1,"target":target("host-a"),"archive":archive});
        serde_json::from_value::<obzenflow::application::control::CurrentRunDiscovery>(
            value.clone(),
        )
        .expect("valid discovery fixture");
        value
    }

    fn result(host: &str, status: &str) -> Value {
        json!({"protocol_version":1,"target":target(host),"result":{"status":status,"message":"fixture control result","state":"ready_for_run"}})
    }

    // A scripted HTTP peer tests transport failures at the executable boundary.
    // None drops the connection after receiving a request, modelling a lost reply.
    async fn peer(
        replies: Vec<(u16, Option<Value>)>,
    ) -> (String, tokio::task::JoinHandle<Vec<(String, Value)>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let mut requests = vec![];
            let mut replies = replies.into_iter();
            while let Ok(Ok((stream, _))) =
                tokio::time::timeout(Duration::from_secs(1), listener.accept()).await
            {
                let mut reader = BufReader::new(stream);
                let mut first = String::new();
                reader.read_line(&mut first).await.unwrap();
                let mut length = 0;
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).await.unwrap();
                    if line == "\r\n" || line.is_empty() {
                        break;
                    }
                    if let Some((key, value)) = line.split_once(':') {
                        if key.eq_ignore_ascii_case("content-length") {
                            length = value.trim().parse().unwrap();
                        }
                    }
                }
                let mut body = vec![0; length];
                reader.read_exact(&mut body).await.unwrap();
                requests.push((first, serde_json::from_slice(&body).unwrap_or(Value::Null)));
                let (status, reply) = replies
                    .next()
                    .unwrap_or((500, Some(json!({"error":"unexpected extra request"}))));
                if let Some(reply) = reply {
                    let body = serde_json::to_vec(&reply).unwrap();
                    let header = format!("HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
                    let mut stream = reader.into_inner();
                    stream.write_all(header.as_bytes()).await.unwrap();
                    stream.write_all(&body).await.unwrap();
                    stream.shutdown().await.unwrap();
                }
            }
            requests
        });
        (url, task)
    }

    async fn start(url: &str, follow: bool) -> std::process::Output {
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_obzenflow"));
        command
            .args(["start", "--server", url, "--timeout-secs", "2", "--json"])
            .kill_on_drop(true);
        if follow {
            command.arg("--follow");
        }
        tokio::time::timeout(Duration::from_secs(5), command.output())
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn bare_start_accepts_ephemeral_and_sends_only_the_conditional_shape() {
        let (url, requests) = peer(vec![
            (
                200,
                Some(discovery(
                    json!({"kind":"unavailable","reason":"ephemeral"}),
                )),
            ),
            (200, Some(result("host-a", "accepted"))),
        ])
        .await;
        let output = start(&url, false).await;
        assert_eq!(
            output.status.code(),
            Some(0),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].0.starts_with("GET /api/flow/run "));
        assert!(requests[1].0.starts_with("POST /api/flow/control "));
        assert_eq!(requests[1].1["target"], target("host-a"));
        assert_eq!(requests[1].1["control"]["action"], "play");
        assert!(requests[1].1.get("action").is_none());
    }

    #[tokio::test]
    async fn unavailable_follow_or_legacy_server_never_submits_play() {
        for (status, body, follow) in [
            (
                200,
                discovery(json!({"kind":"unavailable","reason":"unreadable"})),
                true,
            ),
            (404, json!({"error":"old server"}), false),
        ] {
            let (url, requests) = peer(vec![(status, Some(body))]).await;
            let output = start(&url, follow).await;
            assert_eq!(output.status.code(), Some(4));
            assert!(output.stdout.is_empty());
            assert_eq!(
                requests.await.unwrap().len(),
                1,
                "no legacy or unconditional Play fallback"
            );
        }
    }

    #[tokio::test]
    async fn stale_target_typed_rejection_and_lost_response_never_retry() {
        for (status, reply, diagnostic) in [
            (
                409,
                Some(result("host-b", "rejected")),
                "run_target_mismatch",
            ),
            (
                200,
                Some(result("host-a", "rejected")),
                "run_control_rejected",
            ),
            (200, None, "uncertain"),
        ] {
            let (url, requests) = peer(vec![
                (
                    200,
                    Some(discovery(
                        json!({"kind":"unavailable","reason":"ephemeral"}),
                    )),
                ),
                (status, reply),
            ])
            .await;
            let output = start(&url, false).await;
            assert_eq!(output.status.code(), Some(4));
            assert!(
                String::from_utf8_lossy(&output.stderr).contains(diagnostic),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(
                requests.await.unwrap().len(),
                2,
                "control is never retried or retargeted"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn advertised_archive_identity_is_checked_before_play() {
        let dir = tempfile::tempdir().unwrap();
        FlowApplication::builder()
            .with_cli_args(["application"])
            .run_async(build_flow(dir.path().to_path_buf()))
            .await
            .unwrap();
        let path = latest_run_dir(dir.path());
        let encoded = obzenflow::application::control::NativeRunPath::encode(&path).unwrap();
        let (url, requests) = peer(vec![(200, Some(discovery(json!({"kind":"local_disk", "flow_id":"flow_01H000000000000000000000000", "path":encoded}))))]).await;
        let output = start(&url, true).await;
        assert_eq!(output.status.code(), Some(4));
        assert!(String::from_utf8_lossy(&output.stderr).contains("identity does not match"));
        assert_eq!(requests.await.unwrap().len(), 1);
    }
}

#[cfg(all(feature = "web-host", unix))]
mod hosted {
    use super::*;
    use obzenflow::application::control::{CurrentRunDiscovery, RunArchive, RUN_DISCOVERY_PATH};
    use obzenflow::journal::read::{open_disk_run, RunOutcome, RunRecord, RunRecordKind, TailRead};
    use std::process::Stdio;
    use std::time::Duration;
    use sysinfo::{Pid, PidExt, ProcessExt, Signal, System, SystemExt};

    const HOST_ROOT: &str = "OBZENFLOW_CLI_TEST_HOST_ROOT";
    const HOST_PORT: &str = "OBZENFLOW_CLI_TEST_HOST_PORT";

    #[test]
    #[ignore = "subprocess fixture launched by independent_lifetimes"]
    fn child_application() {
        let root = PathBuf::from(std::env::var_os(HOST_ROOT).expect("parent fixture root"));
        let port = std::env::var(HOST_PORT).unwrap();
        let definition = FlowDefinition::materialize(move |_| {
            let source = obzenflow::stages::sources::async_finite(|index| async move {
                tokio::time::sleep(Duration::from_millis(30)).await;
                Some(vec![Tick { n: index as u64 }])
            });
            let sink = SinkTyped::with_delivery(discard::<Tick>()).idempotent();
            Ok(flow! {
                name: "cli_process_lifetime", journals: disk_journals(root),
                stages: {
                    ticks = obzenflow::flow::async_source!(Tick => source);
                    out = sink!(Tick => sink);
                },
                topology: { ticks |> out; }
            })
        });
        FlowApplication::builder()
            .with_cli_args([
                "application",
                "--server",
                "--startup-mode",
                "manual",
                "--server-port",
                &port,
            ])
            .run_blocking(definition)
            .expect("application lifecycle");
    }

    fn signal(child: &tokio::process::Child, signal: Signal) {
        let pid = Pid::from_u32(child.id().unwrap());
        let mut system = System::new();
        assert!(system.refresh_process(pid));
        assert_eq!(system.process(pid).unwrap().kill_with(signal), Some(true));
    }

    async fn wait(child: &mut tokio::process::Child) -> std::process::ExitStatus {
        tokio::time::timeout(Duration::from_secs(15), child.wait())
            .await
            .expect("process exits within lifecycle budget")
            .unwrap()
    }

    async fn discover(
        client: &reqwest::Client,
        url: &str,
        child: &mut tokio::process::Child,
    ) -> CurrentRunDiscovery {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                assert!(
                    child.try_wait().unwrap().is_none(),
                    "application exited before admission"
                );
                if let Ok(response) = client
                    .get(format!("{url}{RUN_DISCOVERY_PATH}"))
                    .send()
                    .await
                {
                    assert_eq!(response.headers()["cache-control"], "no-store");
                    if let Ok(discovery) = response.json::<CurrentRunDiscovery>().await {
                        return discovery;
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("host discovery")
    }

    async fn first_fact(path: &Path, viewer: &mut tokio::process::Child) {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                assert!(
                    viewer.try_wait().unwrap().is_none(),
                    "viewer exited before source evidence"
                );
                let text = std::fs::read_to_string(path).unwrap();
                if text
                    .lines()
                    .filter_map(|line| serde_json::from_str::<RunRecord>(line).ok())
                    .any(|r| r.kind == RunRecordKind::SourceFact)
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("follow emits committed source evidence");
    }

    // The application and viewer are independent children of this test. Neither
    // launches the other; these are real OS signals through the production host.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn independent_lifetimes_cover_detach_sigint_sigterm_and_sigkill() {
        for mode in ["detach", "reader_error", "sigint", "sigterm", "sigkill"] {
            let dir = tempfile::tempdir().unwrap();
            let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let port = reservation.local_addr().unwrap().port();
            drop(reservation);
            let url = format!("http://127.0.0.1:{port}");
            let host_log = dir.path().join("host.log");
            let mut application = tokio::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--ignored",
                    "--exact",
                    "hosted::child_application",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(HOST_ROOT, dir.path().join("journals"))
                .env(HOST_PORT, port.to_string())
                .stdout(Stdio::null())
                .stderr(std::fs::File::create(&host_log).unwrap())
                .kill_on_drop(true)
                .spawn()
                .unwrap();
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(2))
                .build()
                .unwrap();
            let discovery = discover(&client, &url, &mut application).await;
            let RunArchive::LocalDisk { flow_id, path } = &discovery.archive else {
                panic!("host must admit its archive: {discovery:?}");
            };
            let path = path.decode().unwrap();
            let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_obzenflow"))
                .arg("show")
                .arg(&path)
                .arg("--json")
                .current_dir(dir.path())
                .output()
                .await
                .unwrap();
            assert_eq!(output.status.code(), Some(0));
            let mut snapshot = open_disk_run(&path).await.unwrap();
            assert_eq!(snapshot.identity().flow_id.to_string(), *flow_id);
            assert_eq!(
                snapshot.identity().pipeline_writer_id,
                discovery.target.pipeline_writer_id
            );
            while let Some(row) = snapshot.next().await.unwrap() {
                assert_ne!(
                    row.kind,
                    RunRecordKind::SourceFact,
                    "manual host must not run before Play"
                );
            }
            assert_eq!(
                client
                    .get(format!("{url}/ready"))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                503
            );
            let rows = dir.path().join("viewer.jsonl");
            let diagnostics = dir.path().join("viewer.stderr");
            let mut viewer = tokio::process::Command::new(env!("CARGO_BIN_EXE_obzenflow"))
                .args(["start", "--server", &url, "--follow", "--json"])
                .stdout(std::fs::File::create(&rows).unwrap())
                .stderr(std::fs::File::create(&diagnostics).unwrap())
                .kill_on_drop(true)
                .spawn()
                .unwrap();
            first_fact(&rows, &mut viewer).await;
            assert_eq!(
                client
                    .get(format!("{url}/ready"))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                200
            );
            let guidance = std::fs::read_to_string(&host_log).unwrap();
            let pid = application.id().unwrap();
            assert!(
                guidance.contains(&format!("kill -TERM {pid}"))
                    && guidance.contains(&format!("kill -KILL {pid}"))
            );
            match mode {
                "detach" => {
                    signal(&viewer, Signal::Interrupt);
                    assert_eq!(wait(&mut viewer).await.code(), Some(0));
                    assert!(application.try_wait().unwrap().is_none());
                    assert_eq!(
                        client
                            .get(format!("{url}/ready"))
                            .send()
                            .await
                            .unwrap()
                            .status(),
                        200
                    );
                    signal(&application, Signal::Term);
                }
                "sigint" => signal(&application, Signal::Interrupt),
                "reader_error" => {
                    let system = path.join("system.log");
                    let hidden = path.join("system.temporarily-unavailable");
                    std::fs::rename(&system, &hidden).unwrap();
                    assert_eq!(wait(&mut viewer).await.code(), Some(4));
                    assert!(std::fs::read_to_string(&diagnostics)
                        .unwrap()
                        .contains("run_observation_failed"));
                    assert!(application.try_wait().unwrap().is_none());
                    assert_eq!(
                        client
                            .get(format!("{url}/ready"))
                            .send()
                            .await
                            .unwrap()
                            .status(),
                        200
                    );
                    std::fs::rename(&hidden, &system).unwrap();
                    signal(&application, Signal::Term);
                }
                "sigterm" => signal(&application, Signal::Term),
                "sigkill" => application.start_kill().unwrap(),
                _ => unreachable!(),
            }
            let _ = wait(&mut application).await;
            let mut tail = open_disk_run(&path).await.unwrap().into_tail();
            while matches!(tail.read_next().await.unwrap(), TailRead::Record(_)) {}
            if mode == "sigkill" {
                assert!(tail.progress().settled_prefix.is_none());
                tokio::time::sleep(Duration::from_millis(200)).await;
                assert!(
                    viewer.try_wait().unwrap().is_none(),
                    "SIGKILL cannot manufacture settlement"
                );
                signal(&viewer, Signal::Interrupt);
            } else {
                assert!(
                    tail.progress().settled_prefix.is_some(),
                    "{mode}: {:?}",
                    tail.progress()
                );
                if mode == "sigint" {
                    assert!(matches!(
                        tail.progress().outcome.as_ref().unwrap().outcome,
                        RunOutcome::Cancelled { .. }
                    ));
                }
            }
            if mode != "detach" && mode != "reader_error" {
                assert_eq!(
                    wait(&mut viewer).await.code(),
                    Some(0),
                    "{mode}: {}",
                    std::fs::read_to_string(&diagnostics).unwrap()
                );
            }
            for line in std::fs::read_to_string(&rows).unwrap().lines() {
                serde_json::from_str::<RunRecord>(line).expect("stdout contains records only");
            }
        }
    }
}
