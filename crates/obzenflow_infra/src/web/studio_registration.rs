// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-114d Studio phonebook registration heartbeat.
//!
//! Background presence only: register after the web server answers its own
//! `/health`, renew every `renew_interval_secs` restamping the advisory
//! `phase`, and deregister (fenced by `runtime_instance_id`) when the host's
//! shutdown signal fires. The phonebook is never authoritative for lifecycle;
//! failures here never crash the runtime.

use crate::application::config::{ResolvedStudioConfig, StartupMode};
use crate::http_client::default_http_client;
use obzenflow_core::http_client::{HttpClient, HttpMethod, RequestSpec, Url};
use obzenflow_runtime::pipeline::{PipelinePhase, PipelineState};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;

const SELF_PROBE_ATTEMPTS: u32 = 50;
const SELF_PROBE_INTERVAL: Duration = Duration::from_millis(100);
const FAILURE_LOG_INTERVAL: Duration = Duration::from_secs(60);

pub(crate) struct HeartbeatContext {
    pub(crate) studio: ResolvedStudioConfig,
    pub(crate) runtime_instance_id: String,
    pub(crate) flow_name: String,
    pub(crate) startup_mode: StartupMode,
    /// The runtime's own bind, probed before first registration (RP 5).
    pub(crate) probe_base_url: String,
}

/// Spawn the heartbeat task. Deregistration is exit-tied: it fires when the
/// shutdown signal does, in both on_terminal modes (park keeps renewing with
/// the terminal phase until a signal arrives).
pub(crate) fn spawn_heartbeat(
    ctx: HeartbeatContext,
    state_rx: watch::Receiver<PipelineState>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let client = match default_http_client() {
            Ok(client) => client,
            Err(error) => {
                tracing::error!(%error, "studio registration disabled: no HTTP client");
                return;
            }
        };

        if !self_probe(client.as_ref(), &ctx.probe_base_url, &mut shutdown_rx).await {
            return;
        }

        let register_url = match Url::parse(&format!("{}/register", ctx.studio.phonebook_url)) {
            Ok(url) => url,
            Err(error) => {
                tracing::error!(%error, phonebook_url = %ctx.studio.phonebook_url, "invalid phonebook URL; studio registration disabled");
                return;
            }
        };

        let mut ticker = tokio::time::interval(Duration::from_secs(ctx.studio.renew_interval_secs));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_failure_log: Option<Instant> = None;

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let sent = register_once(client.clone(), &register_url, &ctx, &state_rx).await;
                    // Prompt single retry keeps repopulation within roughly one
                    // renew interval after a phonebook restart; the tick cadence
                    // is the bounded backoff beyond that.
                    let sent = if sent.is_err() {
                        register_once(client.clone(), &register_url, &ctx, &state_rx).await
                    } else {
                        sent
                    };
                    if let Err(error) = sent {
                        let due = last_failure_log
                            .is_none_or(|at| at.elapsed() >= FAILURE_LOG_INTERVAL);
                        if due {
                            tracing::warn!(%error, phonebook = %ctx.studio.phonebook_url, "phonebook registration failing; will keep retrying");
                            last_failure_log = Some(Instant::now());
                        }
                    } else {
                        last_failure_log = None;
                    }
                }
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        deregister(client.as_ref(), &ctx).await;
                        return;
                    }
                }
            }
        }
    })
}

/// Bounded self-probe of the runtime's own `/health` (114a RP 5): register
/// only once the control plane is actually reachable.
async fn self_probe(
    client: &dyn HttpClient,
    probe_base_url: &str,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> bool {
    let url = match Url::parse(&format!("{probe_base_url}/health")) {
        Ok(url) => url,
        Err(error) => {
            tracing::error!(%error, %probe_base_url, "invalid probe URL; studio registration disabled");
            return false;
        }
    };
    for _ in 0..SELF_PROBE_ATTEMPTS {
        if *shutdown_rx.borrow() {
            return false;
        }
        let request = RequestSpec::new(HttpMethod::Get, url.clone());
        if let Ok(response) = client.execute(request).await {
            if response.status == 200 {
                return true;
            }
        }
        tokio::time::sleep(SELF_PROBE_INTERVAL).await;
    }
    tracing::warn!(%probe_base_url, "web server self-probe never succeeded; studio registration disabled");
    false
}

async fn register_once(
    client: Arc<dyn HttpClient>,
    register_url: &Url,
    ctx: &HeartbeatContext,
    state_rx: &watch::Receiver<PipelineState>,
) -> Result<(), String> {
    let phase = PipelinePhase::from(&*state_rx.borrow());
    let payload = serde_json::json!({
        "job_id": ctx.studio.job_id,
        "base_url": ctx.studio.advertise_url,
        "runtime_instance_id": ctx.runtime_instance_id,
        "flow_name": ctx.flow_name,
        "startup_mode": match ctx.startup_mode {
            StartupMode::Auto => "auto",
            StartupMode::Manual => "manual",
        },
        "phase": phase.as_str(),
        "lease_ttl_secs": ctx.studio.lease_ttl_secs,
    });
    let mut request = RequestSpec::new(HttpMethod::Post, register_url.clone());
    request.headers.insert(
        "content-type",
        "application/json".parse().expect("static header value"),
    );
    request.body = Some(payload.to_string().into());
    match client.execute(request).await {
        Ok(response) if (200..300).contains(&response.status) => Ok(()),
        Ok(response) => Err(format!("phonebook returned status {}", response.status)),
        Err(error) => Err(error.to_string()),
    }
}

/// Fenced deregistration, best-effort: a stale instance id is a no-op at the
/// phonebook, so this can never remove a successor's entry.
async fn deregister(client: &dyn HttpClient, ctx: &HeartbeatContext) {
    let mut url = match Url::parse(&format!(
        "{}/register/{}",
        ctx.studio.phonebook_url, ctx.studio.job_id
    )) {
        Ok(url) => url,
        Err(error) => {
            tracing::warn!(%error, "deregistration skipped: invalid URL");
            return;
        }
    };
    url.query_pairs_mut()
        .append_pair("runtime_instance_id", &ctx.runtime_instance_id);
    let request = RequestSpec::new(HttpMethod::Delete, url);
    if let Err(error) = client.execute(request).await {
        tracing::warn!(%error, "phonebook deregistration failed; lease will expire instead");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_runtime::pipeline::PipelineState;
    use std::sync::Mutex;
    use warp::Filter;

    #[derive(Debug, Default)]
    struct StubPhonebook {
        registrations: Mutex<Vec<serde_json::Value>>,
        deletes: Mutex<Vec<String>>,
    }

    /// Serve a stub phonebook (register + fenced delete + health) on an
    /// ephemeral loopback port; health doubles as the runtime self-probe.
    async fn spawn_stub(stub: Arc<StubPhonebook>) -> (String, JoinHandle<()>) {
        let register = {
            let stub = stub.clone();
            warp::path!("register")
                .and(warp::post())
                .and(warp::body::json())
                .map(move |body: serde_json::Value| {
                    stub.registrations.lock().expect("lock").push(body);
                    warp::reply::with_status(warp::reply(), warp::http::StatusCode::NO_CONTENT)
                })
        };
        let deregister = {
            let stub = stub.clone();
            warp::path!("register" / String)
                .and(warp::delete())
                .and(warp::query::raw())
                .map(move |job_id: String, query: String| {
                    stub.deletes
                        .lock()
                        .expect("lock")
                        .push(format!("{job_id}?{query}"));
                    warp::reply::with_status(warp::reply(), warp::http::StatusCode::NO_CONTENT)
                })
        };
        let health = warp::path!("health").map(warp::reply);
        let (addr, server) =
            warp::serve(register.or(deregister).or(health)).bind_ephemeral(([127, 0, 0, 1], 0));
        let handle = tokio::spawn(server);
        (format!("http://{addr}"), handle)
    }

    fn studio(base: &str) -> ResolvedStudioConfig {
        ResolvedStudioConfig {
            phonebook_url: base.to_string(),
            job_id: "demo_job".to_string(),
            advertise_url: "http://127.0.0.1:9090".to_string(),
            lease_ttl_secs: 3,
            renew_interval_secs: 1,
        }
    }

    async fn wait_for<F: Fn() -> bool>(what: &str, check: F) {
        for _ in 0..100 {
            if check() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("timed out waiting for {what}");
    }

    #[tokio::test]
    async fn registers_restamps_phase_and_deregisters_on_shutdown() {
        let stub = Arc::new(StubPhonebook::default());
        let (base, server) = spawn_stub(stub.clone()).await;
        let (state_tx, state_rx) = watch::channel(PipelineState::ReadyForRun);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let task = spawn_heartbeat(
            HeartbeatContext {
                studio: studio(&base),
                runtime_instance_id: "01TESTINSTANCE".to_string(),
                flow_name: "demo_flow".to_string(),
                startup_mode: StartupMode::Manual,
                probe_base_url: base.clone(),
            },
            state_rx,
            shutdown_rx,
        );

        wait_for("first registration", || {
            !stub.registrations.lock().expect("lock").is_empty()
        })
        .await;
        {
            let regs = stub.registrations.lock().expect("lock");
            let first = &regs[0];
            assert_eq!(first["job_id"], "demo_job");
            assert_eq!(first["runtime_instance_id"], "01TESTINSTANCE");
            assert_eq!(first["phase"], "ready_for_run");
            assert_eq!(first["startup_mode"], "manual");
            assert_eq!(first["lease_ttl_secs"], 3);
        }

        state_tx.send(PipelineState::Running).expect("state send");
        wait_for("renewal restamping phase to running", || {
            stub.registrations
                .lock()
                .expect("lock")
                .iter()
                .any(|body| body["phase"] == "running")
        })
        .await;

        shutdown_tx.send(true).expect("shutdown send");
        wait_for("fenced deregistration", || {
            !stub.deletes.lock().expect("lock").is_empty()
        })
        .await;
        assert_eq!(
            stub.deletes.lock().expect("lock")[0],
            "demo_job?runtime_instance_id=01TESTINSTANCE"
        );
        task.await
            .expect("heartbeat task ends after deregistration");
        server.abort();
    }

    #[tokio::test]
    async fn unreachable_phonebook_never_panics_and_keeps_retrying() {
        // Health must answer (self-probe target) while /register 404s, so the
        // renewal loop runs against a phonebook that rejects everything.
        let health_only = warp::path!("health").map(warp::reply);
        let (addr, server) = warp::serve(health_only).bind_ephemeral(([127, 0, 0, 1], 0));
        let server = tokio::spawn(server);
        let base = format!("http://{addr}");

        let (_state_tx, state_rx) = watch::channel(PipelineState::Running);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = spawn_heartbeat(
            HeartbeatContext {
                studio: studio(&base),
                runtime_instance_id: "01TESTINSTANCE".to_string(),
                flow_name: "demo_flow".to_string(),
                startup_mode: StartupMode::Auto,
                probe_base_url: base.clone(),
            },
            state_rx,
            shutdown_rx,
        );

        // Two renewal intervals of failures: the task must still be alive.
        tokio::time::sleep(Duration::from_millis(2500)).await;
        assert!(
            !task.is_finished(),
            "heartbeat must survive registration failures"
        );

        shutdown_tx.send(true).expect("shutdown send");
        task.await.expect("heartbeat ends cleanly");
        server.abort();
    }
}
