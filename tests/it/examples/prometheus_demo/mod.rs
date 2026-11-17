// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[cfg(all(feature = "web-host", feature = "prometheus"))]
#[path = "../../../../examples/prometheus_demo/support.rs"]
mod support;

#[cfg(all(feature = "web-host", feature = "prometheus"))]
mod measurement;

#[cfg(all(feature = "web-host", feature = "prometheus"))]
mod managed_lifecycle_regressions {
    use futures::FutureExt;
    use obzenflow_core::event::{
        PipelineLifecycleEvent as PipelineFact, SystemPayload as SystemFact,
    };
    use obzenflow_infra::application::{ApplicationError, FlowApplication, LogLevel};
    use obzenflow_runtime::__private::lifecycle;
    use obzenflow_runtime::pipeline::FlowHandle;
    use std::sync::{Arc, Mutex};

    use super::support;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn invalid_cors_returns_startup_error_and_stops_the_materialised_flow() {
        for startup in ["auto", "manual"] {
            for from_cli in [false, true] {
                let dir = tempfile::tempdir_in("target").unwrap();
                let config = dir.path().join("invalid-cors.toml");
                let port = 8080;
                std::fs::write(
                    &config,
                    r#"
[journal]
sync = "off"

[server]
enabled = true
host = "127.0.0.1"
port = PROBE_PORT
startup_mode = "STARTUP_MODE"
on_terminal = "exit"
[server.cors]
mode = "allow-list"
allow_origins = ["CORS_ORIGIN"]
[metrics]
enabled = false
"#
                    .replace("PROBE_PORT", &port.to_string())
                    .replace("STARTUP_MODE", startup)
                    .replace(
                        "CORS_ORIGIN",
                        if from_cli {
                            "https://example.com"
                        } else {
                            "not-an-origin"
                        },
                    ),
                )
                .unwrap();
                let mut args = vec!["cors-regression"];
                if from_cli {
                    args.extend(["--cors-allow-origin", "not-an-origin"]);
                }
                let observed = Arc::new(Mutex::new(None::<Arc<FlowHandle>>));
                let hook_observed = observed.clone();
                let app = FlowApplication::builder()
                    .with_config_file(config)
                    .with_cli_args(args)
                    .with_log_level(LogLevel::Error)
                    .with_flow_handle_hook(move |flow| {
                        *hook_observed.lock().unwrap() = Some(flow.clone());
                        tokio::spawn(async {})
                    });
                let outcome = std::panic::AssertUnwindSafe(
                    app.run_async(support::flow_definition(10, dir.path().join("journals"))),
                )
                .catch_unwind()
                .await;
                let flow = observed.lock().unwrap().take();
                let still_running = flow.as_ref().is_some_and(|flow| flow.is_running());
                // Preserve cleanup if this regression reintroduces an admission panic.
                if let Some(flow) = flow.as_ref().filter(|_| still_running) {
                    flow.stop_cancel().await.unwrap();
                    if let Err(error) = lifecycle::wait(flow).await {
                        eprintln!("the cancelled flow ended with {error}");
                    }
                }
                assert!(outcome.is_ok(), "CORS admission panicked; materialised supervisor still running: {still_running}");
                assert!(
                    matches!(outcome, Ok(Err(ApplicationError::ServerStartFailed(_)))),
                    "unexpected result: {outcome:?}"
                );
                assert!(
                    !still_running,
                    "startup failure left the materialised supervisor running"
                );
                let flow = flow.expect("admission follows materialisation");
                let events = flow
                    .system_journal()
                    .unwrap()
                    .read_all_unordered()
                    .await
                    .unwrap();
                assert!(!events.iter().any(|event| matches!(
                    &event.payload,
                    SystemFact::PipelineLifecycle(PipelineFact::Running { .. })
                )));
            }
        }
    }
}
