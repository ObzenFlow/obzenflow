// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The concrete host owns every framework task, including Hyper's HTTP/2 executor.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};
use tower_service::Service;
use warp::{filters::BoxedFilter, Reply};

use super::host_error::ManagedWebHostError;

const CLOSE_GRACE: Duration = Duration::from_secs(5);

#[derive(Clone, Default)]
pub(super) struct HostTasks(Arc<Mutex<TaskSet>>);

#[derive(Default)]
struct TaskSet {
    tasks: JoinSet<()>,
    aborting: bool,
}

impl HostTasks {
    pub(super) fn spawn(&self, task: impl Future<Output = ()> + Send + 'static) {
        let mut set = self.0.lock().expect("host task set poisoned");
        if !set.aborting {
            set.tasks.spawn(task);
        }
    }

    fn abort_all(&self) {
        let mut set = self.0.lock().expect("host task set poisoned");
        set.aborting = true;
        set.tasks.abort_all();
    }

    async fn join_next(&self) -> Option<Result<(), tokio::task::JoinError>> {
        futures::future::poll_fn(|cx| {
            self.0
                .lock()
                .expect("host task set poisoned")
                .tasks
                .poll_join_next(cx)
        })
        .await
    }

    fn is_empty(&self) -> bool {
        self.0
            .lock()
            .expect("host task set poisoned")
            .tasks
            .is_empty()
    }

    pub(super) async fn drain(&self) {
        while let Some(result) = self.join_next().await {
            if let Err(error) = result {
                if error.is_panic() {
                    tracing::error!(%error, "Managed web connection task panicked");
                }
            }
        }
    }
}

impl<F> hyper::rt::Executor<F> for HostTasks
where
    F: Future + Send + 'static,
    F::Output: Send,
{
    fn execute(&self, future: F) {
        self.spawn(async move {
            let _ = future.await;
        });
    }
}

/// Already bound and serving. Dropping the application aborts all owned tasks;
/// an ordinary exit additionally joins them before returning to its caller.
pub(crate) struct ManagedWebHost {
    address: SocketAddr,
    serving: Option<JoinHandle<Result<(), ManagedWebHostError>>>,
    tasks: HostTasks,
    shutdown: watch::Sender<bool>,
}

impl ManagedWebHost {
    pub(super) async fn bind(
        address: SocketAddr,
        routes: BoxedFilter<(Box<dyn Reply>,)>,
        tasks: HostTasks,
        shutdown: watch::Sender<bool>,
    ) -> Result<Self, ManagedWebHostError> {
        let listener =
            TcpListener::bind(address)
                .await
                .map_err(|error| ManagedWebHostError::BindFailed {
                    address: address.to_string(),
                    source: Some(Box::new(error)),
                })?;
        let address = listener.local_addr().map_err(ManagedWebHostError::Accept)?;
        let mut closing = shutdown.subscribe();
        let connection_tasks = tasks.clone();
        let serving = tokio::spawn(async move {
            let service = warp::service(routes);
            loop {
                tokio::select! {
                    biased;
                    _ = wait_for_close(&mut closing) => return Ok(()),
                    result = connection_tasks.join_next(), if !connection_tasks.is_empty() => {
                        if let Some(Err(error)) = result {
                            if error.is_panic() {
                                tracing::error!(%error, "Managed web connection task panicked");
                            }
                        }
                    }
                    result = listener.accept() => {
                        let (socket, _) = result.map_err(ManagedWebHostError::Accept)?;
                        let service = service.clone();
                        let executor = connection_tasks.clone();
                        let mut connection_close = closing.clone();
                        let request_close = closing.clone();
                        connection_tasks.spawn(async move {
                            let service = hyper::service::service_fn(move |request| {
                                let mut service = service.clone();
                                let closing = *request_close.borrow();
                                async move {
                                    if closing {
                                        return Ok(warp::reply::with_status(
                                            "Service Unavailable", warp::http::StatusCode::SERVICE_UNAVAILABLE,
                                        ).into_response());
                                    }
                                    service.call(request).await
                                }
                            });
                            let builder = hyper_util::server::conn::auto::Builder::new(executor);
                            let connection = builder.serve_connection(hyper_util::rt::TokioIo::new(socket), service);
                            tokio::pin!(connection);
                            let result = tokio::select! {
                                biased;
                                _ = wait_for_close(&mut connection_close) => {
                                    connection.as_mut().graceful_shutdown();
                                    connection.await
                                }
                                result = &mut connection => result,
                            };
                            if let Err(error) = result {
                                tracing::debug!(%error, "Managed web connection closed with an I/O error");
                            }
                        });
                    }

                }
            }
        });
        Ok(Self {
            address,
            serving: Some(serving),
            tasks,
            shutdown,
        })
    }

    #[cfg(test)]
    pub(crate) async fn replace_serving_for_test(
        &mut self,
        future: futures::future::BoxFuture<'static, Result<(), ManagedWebHostError>>,
        complete_before_run: bool,
    ) {
        let previous = self.serving.take().unwrap();
        previous.abort();
        let _ = previous.await;
        self.serving = Some(tokio::spawn(future));
        if complete_before_run {
            while !self.serving.as_ref().unwrap().is_finished() {
                tokio::task::yield_now().await;
            }
        }
    }

    pub(crate) fn address(&self) -> SocketAddr {
        self.address
    }

    /// Any completion observed before the coordinator closes the host is a fault.
    pub(crate) async fn failure(&mut self) -> ManagedWebHostError {
        let Some(serving) = self.serving.as_mut() else {
            return std::future::pending().await;
        };
        let result = serving.await;
        self.serving.take();
        match result {
            Ok(Err(error)) => error,
            Ok(Ok(())) => ManagedWebHostError::PrematureCompletion,
            Err(error) => ManagedWebHostError::Task(error),
        }
    }

    fn begin_close(&self) {
        self.shutdown.send_if_modified(|closing| {
            let changed = !*closing;
            *closing = true;
            changed
        });
    }

    pub(crate) async fn close(mut self) -> Result<(), ManagedWebHostError> {
        let premature = self.serving.as_ref().is_some_and(JoinHandle::is_finished);
        self.begin_close();
        let mut serve_result = None;
        // One deadline covers both accepting and the entire connection subtree.
        let expired = tokio::time::timeout(CLOSE_GRACE, async {
            if let Some(serving) = self.serving.as_mut() {
                serve_result = Some(
                    serving
                        .await
                        .map_err(ManagedWebHostError::Task)
                        .and_then(|result| result),
                );
                self.serving.take();
            }
            self.tasks.drain().await;
        })
        .await
        .is_err();
        if let Some(serving) = self.serving.as_mut() {
            serving.abort();
            if let Err(error) = serving.await {
                if !error.is_cancelled() && serve_result.is_none() {
                    serve_result = Some(Err(ManagedWebHostError::Task(error)));
                }
            }
            self.serving.take();
        }
        self.tasks.abort_all();
        self.tasks.drain().await;
        serve_result.unwrap_or(Ok(()))?;
        if premature {
            return Err(ManagedWebHostError::PrematureCompletion);
        }
        if expired {
            Err(ManagedWebHostError::CloseTimeout)
        } else {
            Ok(())
        }
    }
}

impl Drop for ManagedWebHost {
    fn drop(&mut self) {
        self.begin_close();
        if let Some(serving) = &self.serving {
            serving.abort();
        }
        self.tasks.abort_all();
    }
}

async fn wait_for_close(closing: &mut watch::Receiver<bool>) {
    while !*closing.borrow_and_update() {
        if closing.changed().await.is_err() {
            break;
        }
    }
}

#[cfg(test)]
#[path = "managed_host_tests.rs"]
mod tests;
