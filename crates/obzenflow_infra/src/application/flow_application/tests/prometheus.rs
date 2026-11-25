// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use obzenflow_core::event::payloads::execution_payload::{ExecutionPayload, StageLifecycleFact};
use obzenflow_core::event::ChainPayload;
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, StageId};
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Semaphore;

const FIRST: usize = 3;
const SECOND: usize = 2;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct HeldRead(u8);

#[obzenflow_runtime::effects::port]
#[async_trait]
trait HeldInput {
    #[query(type = "flow_application.held.read")]
    async fn read(
        &self,
        from: &HeldRead,
        ctx: &obzenflow_runtime::queries::QueryContext,
    ) -> Result<FiniteRead<IdlePayload, HeldRead>, obzenflow_runtime::queries::QueryError>;
}

#[obzenflow_runtime::effects::system("held")]
struct Held {
    release: Arc<Semaphore>,
}

#[async_trait]
impl HeldInput for Held {
    async fn read(
        &self,
        from: &HeldRead,
        _ctx: &obzenflow_runtime::queries::QueryContext,
    ) -> Result<FiniteRead<IdlePayload, HeldRead>, obzenflow_runtime::queries::QueryError> {
        match from.0 {
            0 => Ok(FiniteRead::Batch {
                records: vec![IdlePayload; FIRST],
                next: HeldRead(1),
            }),
            1 => {
                self.release.acquire().await.unwrap().forget();
                Ok(FiniteRead::Batch {
                    records: vec![IdlePayload; SECOND],
                    next: HeldRead(2),
                })
            }
            _ => Ok(FiniteRead::EndOfInput),
        }
    }
}

struct HeldSource(Port<dyn HeldInput>);

impl TypedFiniteSourceHandler for HeldSource {
    type Output = IdlePayload;

    fn read(&self) -> SourceRead<IdlePayload> {
        SourceRead::finite(self.0.clone(), HeldRead(0), Ok)
    }
}

async fn scrape(address: SocketAddr) -> String {
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    response
}

async fn scrape_until(address: SocketAddr, holds: impl Fn(&str) -> bool) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let response = scrape(address).await;
        if holds(&response) {
            return response;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the scrape never held: {response}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn series(response: &str, name: &str, label: &str) -> Option<u64> {
    let prefix = format!("{name}{{");
    response
        .lines()
        .find(|line| line.starts_with(&prefix) && line.contains(label))
        .and_then(|line| line.rsplit(' ').next()?.parse().ok())
}

async fn completed_totals(stage: StageId, journal: &Arc<dyn Journal<ChainEvent>>) -> (u64, u64) {
    journal
        .read_all_unordered()
        .await
        .unwrap()
        .iter()
        .find_map(|row| match &row.payload {
            ChainPayload::Execution(ExecutionPayload::StageLifecycle(
                StageLifecycleFact::Completed {
                    stage_id,
                    accounting: Some(accounting),
                },
            )) if *stage_id == stage => Some((
                accounting.events_processed_total,
                accounting.events_emitted_total,
            )),
            _ => None,
        })
        .expect("each stage journals its completion")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hosted_metrics_match_journal_totals() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("obzenflow.toml");
    std::fs::write(
        &config,
        r#"
[server]
enabled = true
host = "127.0.0.1"
startup_mode = "manual"
on_terminal = "park"
[metrics]
enabled = true
"#,
    )
    .unwrap();
    let release = Arc::new(Semaphore::new(0));
    let held = release.clone();
    let (bound_tx, bound_rx) = oneshot::channel();
    let (flow_tx, flow_rx) = oneshot::channel();
    let flow_tx = Mutex::new(Some(flow_tx));
    let (signal_tx, signal_rx) = oneshot::channel();
    let application = tokio::spawn(FlowApplication::launch(
        FlowDefinition::materialize(move |_| {
            let source = HeldSource(Port::to(Held {
                release: held.clone(),
            }));
            let sink = noop_sink();
            Ok(flow! {
                name: "hosted_metrics", journals: crate::journal::memory_journals(),
                stages: { src = source!(IdlePayload => source); sink = sink!(IdlePayload => sink); },
                topology: { src |> sink; }
            })
        }),
        LaunchParams {
            test_bound_address: Some(bound_tx),
            cli_args: Some(vec![
                "hosted-metrics".into(),
                "--server-port".into(),
                "0".into(),
                "--config".into(),
                config.into_os_string(),
            ]),
            test_shutdown_signal: Some(signal_rx),
            flow_handle_hooks: vec![Box::new(move |flow| {
                if let Some(sender) = flow_tx.lock().unwrap().take() {
                    assert!(
                        sender.send(flow.clone()).is_ok(),
                        "the test awaits the flow"
                    );
                }
                Ok(tokio::spawn(async {}))
            })],
            ..LaunchParams::default()
        },
    ));
    let address = bound_rx.await.unwrap();
    let flow = flow_rx.await.unwrap();
    flow.wait_for_ready().await.unwrap();
    flow.start().await.unwrap();

    scrape_until(address, |response| {
        series(response, "obzenflow_events_total", "stage=\"sink\"") == Some(FIRST as u64)
    })
    .await;
    assert!(matches!(flow.current_state(), PipelineState::Running));

    release.add_permits(1);
    flow.wait_for_completion().await.unwrap();
    let response = scrape_until(address, |response| {
        response.contains("obzenflow_pipeline_state{state=\"completed\"} 1")
    })
    .await;
    let journals = flow.stage_journals();
    assert_eq!(journals.len(), 2);
    for (stage, journal) in &journals {
        let (processed, emitted) = completed_totals(*stage, journal).await;
        let label = format!("stage_id=\"{stage}\"");
        assert_eq!(
            series(&response, "obzenflow_events_total", &label),
            Some(processed),
            "{stage}: {response}"
        );
        assert_eq!(
            series(&response, "obzenflow_events_emitted_total", &label),
            Some(emitted),
            "{stage}: {response}"
        );
    }

    assert!(signal_tx.send(ShutdownSignal::Sigint).is_ok());
    tokio::time::timeout(Duration::from_secs(10), application)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
