// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Host-owned development diagnostics. These tasks never control flow execution.

#[cfg(feature = "tokio-console")]
mod console_connections;
#[cfg(test)]
mod tests;

use super::config::ResolvedConsoleConfig;
use super::managed_lifecycle::{abort_and_join, ApplicationTask};
use super::ApplicationError;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

/// Prepared before an owned runtime is created, so its earliest tasks are visible.
pub(super) struct PreparedObservability {
    #[cfg(feature = "tokio-console")]
    console: Option<PreparedConsole>,
}

impl PreparedObservability {
    pub(super) fn install(
        config: &ResolvedConsoleConfig,
        filter: EnvFilter,
    ) -> Result<Self, ApplicationError> {
        if config.enabled {
            #[cfg(not(feature = "tokio-console"))]
            {
                return Err(ApplicationError::FeatureNotEnabled("tokio-console".into()));
            }
            #[cfg(feature = "tokio-console")]
            {
                if !cfg!(tokio_unstable) {
                    return Err(ApplicationError::InvalidConfiguration(
                        "Tokio Console requires RUSTFLAGS=\"--cfg tokio_unstable\" at build time"
                            .into(),
                    ));
                }
                // Console's upstream recorder starts an independently owned IO
                // thread on the process-global layer. It cannot participate in
                // this application's shutdown and join contract.
                if std::env::var_os("TOKIO_CONSOLE_RECORD_PATH").is_some() {
                    return Err(ApplicationError::InvalidConfiguration(
                        "Managed Tokio Console recording is unsupported; unset TOKIO_CONSOLE_RECORD_PATH".into(),
                    ));
                }
                let builder = console_builder()?;
                let listener = bind_console(config.bind)?;
                let (layer, server) = builder.server_addr(listener.local_addr()?).build();
                tracing_subscriber::registry()
                    // Match Console's own spawn() filter without relinquishing
                    // ownership of the aggregation and transport tasks.
                    .with(layer.with_filter(tracing_subscriber::filter::FilterFn::new(
                        |metadata| {
                            if metadata.is_event() {
                                metadata.target().starts_with("runtime")
                                    || metadata.target().starts_with("tokio")
                            } else {
                                metadata.name().starts_with("runtime.")
                                    || metadata.target().starts_with("tokio")
                            }
                        },
                    )))
                    .with(tracing_subscriber::fmt::layer().with_filter(filter))
                    .try_init()
                    .map_err(|error| {
                        ApplicationError::Other(
                            std::io::Error::other(format!(
                                "Tokio Console could not install its tracing subscriber: {error}"
                            ))
                            .into(),
                        )
                    })?;
                return Ok(Self {
                    console: Some(PreparedConsole { listener, server }),
                });
            }
        }

        // An embedding application may already own ordinary tracing.
        let _ = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().with_filter(filter))
            .try_init();
        Ok(Self {
            #[cfg(feature = "tokio-console")]
            console: None,
        })
    }

    pub(super) fn start(self) -> Result<ApplicationDiagnostics, ApplicationError> {
        #[cfg(feature = "tokio-console")]
        if let Some(console) = self.console {
            return console.start();
        }
        Ok(ApplicationDiagnostics {
            tasks: Vec::new(),
            #[cfg(feature = "tokio-console")]
            failure: Default::default(),
            #[cfg(feature = "tokio-console")]
            connections: Default::default(),
            #[cfg(all(test, feature = "tokio-console"))]
            address: None,
        })
    }
}

/// The enclosing launch owns diagnostics through configuration, flow settlement,
/// and post-replay verification, using the host's existing task guards and joins.
#[derive(Default)]
pub(super) struct ApplicationDiagnostics {
    tasks: Vec<ApplicationTask>,
    #[cfg(feature = "tokio-console")]
    failure: std::sync::Arc<std::sync::Mutex<Option<String>>>,
    #[cfg(feature = "tokio-console")]
    connections: std::sync::Arc<console_connections::ConsoleConnections>,
    #[cfg(all(test, feature = "tokio-console"))]
    address: Option<std::net::SocketAddr>,
}

impl ApplicationDiagnostics {
    #[cfg(all(test, feature = "tokio-console"))]
    pub(super) fn address(&self) -> Option<std::net::SocketAddr> {
        self.address
    }

    pub(super) async fn finish(
        mut self,
        result: Result<(), ApplicationError>,
    ) -> Result<(), ApplicationError> {
        // Tonic spawns connection tasks internally. Close their sockets before
        // cancelling the retained tasks so slow clients cannot hold them open.
        #[cfg(feature = "tokio-console")]
        self.connections.close();
        let errors = abort_and_join(std::mem::take(&mut self.tasks)).await;
        for error in errors {
            tracing::warn!(%error, "Diagnostic task failed during setup cleanup");
        }
        #[cfg(feature = "tokio-console")]
        self.connections.wait_closed().await;
        #[cfg(feature = "tokio-console")]
        if let Some(error) = self
            .failure
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            if result.is_err() {
                tracing::error!(%error, "Tokio Console also failed; preserving the application failure");
            } else {
                return Err(ApplicationError::Other(std::io::Error::other(error).into()));
            }
        }
        result
    }
}

impl Drop for ApplicationDiagnostics {
    fn drop(&mut self) {
        #[cfg(feature = "tokio-console")]
        self.connections.close();
        // ApplicationTask's existing Drop requests cancellation of both tasks.
    }
}

#[cfg(feature = "tokio-console")]
struct PreparedConsole {
    listener: std::net::TcpListener,
    server: console_subscriber::Server,
}

#[cfg(feature = "tokio-console")]
fn bind_console(address: std::net::SocketAddr) -> Result<std::net::TcpListener, ApplicationError> {
    let listener = std::net::TcpListener::bind(address).map_err(|error| {
        ApplicationError::ServerStartFailed(format!(
            "Tokio Console could not bind to {address}: {error}"
        ))
    })?;
    listener.set_nonblocking(true)?;
    Ok(listener)
}

#[cfg(feature = "tokio-console")]
fn console_builder() -> Result<console_subscriber::Builder, ApplicationError> {
    // Preserve upstream tuning without with_default_env(): that helper also
    // resolves TOKIO_CONSOLE_BIND and could override or panic behind our resolver.
    fn setting<T: std::str::FromStr>(name: &str) -> Result<Option<T>, ApplicationError>
    where
        T::Err: std::fmt::Display,
    {
        crate::env::env_var(name)
            .map_err(|error| ApplicationError::InvalidConfiguration(error.to_string()))
    }
    let mut builder = console_subscriber::ConsoleLayer::builder();
    if let Some(retention) = setting::<humantime::Duration>("TOKIO_CONSOLE_RETENTION")? {
        builder = builder.retention(retention.into());
    }
    if let Some(interval) = setting::<humantime::Duration>("TOKIO_CONSOLE_PUBLISH_INTERVAL")? {
        if interval.is_zero() {
            return Err(ApplicationError::InvalidConfiguration(
                "TOKIO_CONSOLE_PUBLISH_INTERVAL must be greater than zero".into(),
            ));
        }
        builder = builder.publish_interval(interval.into());
    }
    if let Some(capacity) = setting::<usize>("TOKIO_CONSOLE_BUFFER_CAPACITY")? {
        if capacity == 0 {
            return Err(ApplicationError::InvalidConfiguration(
                "TOKIO_CONSOLE_BUFFER_CAPACITY must be greater than zero".into(),
            ));
        }
        builder = builder.event_buffer_capacity(capacity);
    }
    Ok(builder)
}

#[cfg(feature = "tokio-console")]
impl PreparedConsole {
    fn start(self) -> Result<ApplicationDiagnostics, ApplicationError> {
        use futures::{FutureExt, StreamExt};
        use std::sync::{Arc, Mutex};
        use tracing::instrument::WithSubscriber;

        let address = self.listener.local_addr()?;
        let listener = tokio::net::TcpListener::from_std(self.listener)?;
        let console_subscriber::ServerParts {
            instrument_server,
            aggregator,
            ..
        } = self.server.into_parts();
        let failure = Arc::new(Mutex::new(None));
        let connections = Arc::new(console_connections::ConsoleConnections::default());
        let mut diagnostics = ApplicationDiagnostics {
            tasks: Vec::with_capacity(2),
            failure: failure.clone(),
            connections: connections.clone(),
            #[cfg(test)]
            address: Some(address),
        };
        // Register the silent dispatcher. Dispatch::none() is unregistered:
        // when it first encounters a Tokio callsite, tracing's single-dispatch
        // fast path can cache "never" for that callsite process-wide. This is
        // especially visible when attaching inside an existing runtime.
        let observer_dispatch =
            tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
        let spawn =
            |name: &'static str, work: futures::future::BoxFuture<'static, Result<(), String>>| {
                let failure = failure.clone();
                let work = std::panic::AssertUnwindSafe(work)
                    .catch_unwind()
                    .with_subscriber(observer_dispatch.clone());
                // Match Console's default self-trace exclusion. The spawn must
                // also be untraced: waking an instrumented aggregator from a
                // Console layer callback recursively emits waker telemetry and
                // corrupts the surrounding per-layer filter evaluation.
                ApplicationTask(tracing::dispatcher::with_default(
                    &observer_dispatch,
                    || {
                        tokio::spawn(async move {
                            let outcome = work.await;
                            let error = match outcome {
                                Ok(Ok(())) => format!("Tokio Console {name} stopped unexpectedly"),
                                Ok(Err(error)) => format!("Tokio Console {name} failed: {error}"),
                                Err(_) => format!("Tokio Console {name} panicked"),
                            };
                            // The work's scoped dispatcher has exited. Diagnostic
                            // failures remain visible through application tracing.
                            tracing::error!(%error);
                            failure
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .get_or_insert(error);
                        })
                    },
                ))
            };
        diagnostics.tasks.push(spawn(
            "aggregator",
            async move {
                aggregator.run().await;
                Ok(())
            }
            .boxed(),
        ));
        diagnostics.tasks.push(spawn(
            "server",
            async move {
                tonic::transport::Server::builder()
                    .add_service(instrument_server)
                    // Enable Tonic's connection shutdown watchers in addition to
                    // the enclosing owner's forced socket closure.
                    .serve_with_incoming_shutdown(
                        tokio_stream::wrappers::TcpListenerStream::new(listener).map(
                            move |stream| stream.and_then(|stream| connections.register(stream)),
                        ),
                        std::future::pending::<()>(),
                    )
                    .await
                    .map_err(|error| error.to_string())
            }
            .boxed(),
        ));
        eprintln!("Tokio Console listening at http://{address}; connect with tokio-console to inspect task telemetry");
        Ok(diagnostics)
    }
}
