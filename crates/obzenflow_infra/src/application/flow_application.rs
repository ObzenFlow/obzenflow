// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Spring Boot-style application framework for ObzenFlow
//!
//! Provides automatic lifecycle management for flows including:
//! - CLI argument parsing
//! - Runtime creation and configuration
//! - Observability setup (tracing, console-subscriber)
//! - HTTP server management
//! - Graceful shutdown handling

use super::web_surface::label_endpoint;
use super::{
    ApplicationError, FlowConfig, Presentation, RunPresentationOutcome, WebSurfaceAttachment,
    WebSurfaceWiringContext,
};
use crate::application::config::{CorsModeArg, OnTerminalArg, ResolvedStartupConfig, StartupMode};
use crate::web::endpoints::event_ingestion::{HttpIngress, IngressHandle};
#[cfg(feature = "warp-server")]
use crate::web::surface_metrics::{HttpSurfaceMetricsCollector, HttpSurfaceMetricsEmitter};
use obzenflow_core::metrics::{InfraMetricsSnapshot, MetricsExporter};
use obzenflow_core::web::{CorsConfig, CorsMode, HttpEndpoint, ServerConfig};
use obzenflow_core::TypedPayload;
use obzenflow_dsl::FlowDefinition;
use obzenflow_runtime::bootstrap::{install_bootstrap_config, try_install_bootstrap_config};
use obzenflow_runtime::journal::CurrentRunLocator;
use obzenflow_runtime::prelude::FlowHandle;
use obzenflow_runtime::stages::LivenessSnapshots;
use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

type FlowHandleHook =
    Box<dyn Fn(&Arc<FlowHandle>) -> Result<JoinHandle<()>, ApplicationError> + Send + Sync>;

#[derive(Default)]
struct LaunchParams {
    builder_config_file: Option<PathBuf>,
    enable_autodiscovery: bool,
    web_surfaces: Vec<WebSurfaceAttachment>,
    extra_endpoints: Vec<Box<dyn HttpEndpoint>>,
    flow_handle_hooks: Vec<FlowHandleHook>,
    presentation: Option<Presentation>,
    cli_args: Option<Vec<OsString>>,
    #[cfg(test)]
    test_shutdown_signal: Option<tokio::sync::oneshot::Receiver<ShutdownSignal>>,
}

impl LaunchParams {
    fn autodiscovery_enabled() -> Self {
        Self {
            enable_autodiscovery: true,
            ..Self::default()
        }
    }
}

#[cfg(all(test, feature = "warp-server"))]
mod tests {
    use super::*;
    use crate::journal::disk_journals;
    use async_trait::async_trait;
    use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
    use obzenflow_core::ChainEvent;
    use obzenflow_dsl::{flow, infinite_source, sink};
    use obzenflow_runtime::pipeline::PipelineState;
    use obzenflow_runtime::stages::common::handler_error::HandlerError;
    use obzenflow_runtime::stages::common::handlers::{InfiniteSourceHandler, SinkHandler};
    use obzenflow_runtime::stages::SourceError;
    use std::net::TcpListener;
    use std::sync::Mutex;
    use tokio::sync::oneshot;

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    struct IdlePayload;

    impl TypedPayload for IdlePayload {
        const EVENT_TYPE: &'static str = "flow_application.idle";
    }

    #[derive(Clone, Debug)]
    struct IdleInfiniteSource;

    impl InfiniteSourceHandler for IdleInfiniteSource {
        fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError> {
            Ok(Vec::new())
        }
    }

    #[derive(Clone, Debug)]
    struct NoopSink;

    #[async_trait]
    impl SinkHandler for NoopSink {
        async fn consume(&mut self, _event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
            Ok(DeliveryPayload::success(
                DeliveryMethod::Custom("test".to_string()),
                None,
            ))
        }
    }

    fn available_local_port() -> u16 {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind ephemeral port");
        listener.local_addr().expect("local addr").port()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_auto_mode_tolerates_application_start_and_supervisor_auto_run() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let journal_dir = tempdir.path().join("journals");
        std::fs::create_dir_all(&journal_dir).expect("create journal root");
        let config_path = tempdir.path().join("obzenflow.toml");
        let port = available_local_port();
        std::fs::write(
            &config_path,
            format!(
                r#"
[server]
enabled = true
host = "127.0.0.1"
port = {port}
startup_mode = "auto"

[runtime]
shutdown_timeout_secs = 2

[metrics]
enabled = false
"#
            ),
        )
        .expect("write test config");

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let shutdown_tx = Arc::new(Mutex::new(Some(shutdown_tx)));
        let (running_tx, running_rx) = oneshot::channel();
        let running_tx = Arc::new(Mutex::new(Some(running_tx)));

        let hook_shutdown = Arc::clone(&shutdown_tx);
        let hook_running = Arc::clone(&running_tx);
        let hook = move |flow_handle: &Arc<FlowHandle>| {
            let flow_handle = Arc::clone(flow_handle);
            let hook_shutdown = Arc::clone(&hook_shutdown);
            let hook_running = Arc::clone(&hook_running);
            tokio::spawn(async move {
                let mut states = flow_handle.state_receiver();
                loop {
                    if matches!(*states.borrow(), PipelineState::Running) {
                        break;
                    }
                    if states.changed().await.is_err() {
                        return;
                    }
                }

                if let Some(tx) = hook_running.lock().expect("running lock poisoned").take() {
                    let _ = tx.send(());
                }
                if let Some(tx) = hook_shutdown.lock().expect("shutdown lock poisoned").take() {
                    let _ = tx.send(ShutdownSignal::Sigint);
                }
            })
        };

        let result = tokio::time::timeout(
            Duration::from_secs(10),
            FlowApplication::launch(
                flow! {
                    name: "server_auto_double_run_regression",
                    journals: disk_journals(journal_dir),
                    middleware: [],

                    stages: {
                        src = infinite_source!(IdlePayload => IdleInfiniteSource);
                        sink = sink!(IdlePayload => NoopSink);
                    },

                    topology: {
                        src |> sink;
                    }
                },
                LaunchParams {
                    enable_autodiscovery: false,
                    flow_handle_hooks: vec![Box::new(move |flow_handle| Ok(hook(flow_handle)))],
                    cli_args: Some(vec![
                        OsString::from("obzenflow"),
                        OsString::from("--config"),
                        config_path.into_os_string(),
                    ]),
                    test_shutdown_signal: Some(shutdown_rx),
                    ..LaunchParams::default()
                },
            ),
        )
        .await
        .expect("FlowApplication should not hang in server auto mode");

        result.expect("server auto mode should shut down cleanly");
        running_rx.await.expect("flow should reach Running");
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ShutdownSignal {
    Sigint,
    Sigterm,
}

fn resolve_startup_config(
    builder_config_file: Option<PathBuf>,
    enable_autodiscovery: bool,
    cli_args: Option<Vec<OsString>>,
) -> Result<ResolvedStartupConfig, ApplicationError> {
    let result = if let Some(cli_args) = cli_args {
        FlowConfig::parse_and_resolve_from(cli_args, builder_config_file, enable_autodiscovery)
    } else {
        FlowConfig::parse_and_resolve(builder_config_file, enable_autodiscovery)
    };

    result.map_err(|err| ApplicationError::InvalidConfiguration(err.to_string()))
}

/// Configuration for log level filtering
#[derive(Debug, Clone)]
pub enum LogLevel {
    /// Show all logs (trace, debug, info, warn, error)
    Trace,
    /// Show debug and above (debug, info, warn, error)
    Debug,
    /// Show info and above (info, warn, error) - default
    Info,
    /// Show warnings and errors only
    Warn,
    /// Show errors only
    Error,
    /// Custom filter string (e.g., "info,obzenflow=debug")
    Custom(String),
}

impl LogLevel {
    fn as_filter_string(&self) -> String {
        match self {
            LogLevel::Trace => "trace".to_string(),
            LogLevel::Debug => "debug".to_string(),
            LogLevel::Info => "info".to_string(),
            LogLevel::Warn => "warn".to_string(),
            LogLevel::Error => "error".to_string(),
            LogLevel::Custom(s) => s.clone(),
        }
    }
}

/// Builder for advanced FlowApplication configuration.
///
/// **Prefer `FlowApplication::run()` with `#[tokio::main]` for most use cases.** The builder
/// is only needed when you require web endpoints, flow handle hooks, or console-subscriber
/// integration. If you only need a log level, set the `RUST_LOG` environment variable instead.
///
/// # Example
/// ```ignore
/// fn main() -> Result<(), Box<dyn std::error::Error>> {
///     FlowApplication::builder()
///         .with_web_endpoint(my_endpoint)
///         .run_blocking(flow! {
///             name: "my_flow",
///             // ... flow definition
///         })?;
///     Ok(())
/// }
/// ```
#[derive(Default)]
pub struct FlowApplicationBuilder {
    console_subscriber: bool,
    console_bind: Option<String>,
    log_level: Option<LogLevel>,
    config_file: Option<PathBuf>,
    web_surfaces: Vec<WebSurfaceAttachment>,
    web_endpoints: Vec<Box<dyn HttpEndpoint>>,
    flow_handle_hooks: Vec<FlowHandleHook>,
    presentation: Option<Presentation>,
    cli_args: Option<Vec<OsString>>,
}

impl FlowApplicationBuilder {
    /// Enable tokio-console-subscriber for runtime introspection
    ///
    /// This allows you to connect with `tokio-console` CLI tool to inspect
    /// tasks, async operations, and resource usage in real-time.
    ///
    /// This method is always available, but only takes effect when the `console`
    /// feature is enabled at compile time. This allows user code to be written
    /// once without #[cfg] attributes.
    ///
    /// # Example
    /// ```ignore
    /// // This works whether or not 'console' feature is enabled!
    /// FlowApplication::builder()
    ///     .with_console_subscriber()  // No-op if feature disabled
    ///     .run_blocking(flow! { /* ... */ })
    /// ```
    pub fn with_console_subscriber(mut self) -> Self {
        self.console_subscriber = true;
        self
    }

    /// Set the bind address for console-subscriber
    ///
    /// Default is "127.0.0.1:6669"
    pub fn with_console_bind(mut self, bind: impl Into<String>) -> Self {
        self.console_bind = Some(bind.into());
        self
    }

    /// Set the log level for tracing output
    ///
    /// This filters which log levels are displayed. If not set, defaults to Info.
    /// Can be overridden by the `RUST_LOG` environment variable.
    pub fn with_log_level(mut self, level: LogLevel) -> Self {
        self.log_level = Some(level);
        self
    }

    /// Use an explicit startup config file for builder-driven runs.
    ///
    /// When not set, builder-driven runs also participate in `obzenflow.toml`
    /// autodiscovery from the current working directory. CLI `--config <path>`
    /// still wins when both are present.
    pub fn with_config_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.config_file = Some(path.into());
        self
    }

    /// Use explicit application argv for config parsing instead of process argv.
    ///
    /// This is useful for embedded callers and tests where process argv belongs
    /// to a host/test harness. The first item should be the binary name, matching
    /// normal CLI parsing conventions.
    pub fn with_cli_args<I, T>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString>,
    {
        self.cli_args = Some(args.into_iter().map(Into::into).collect());
        self
    }

    /// Add multiple HTTP endpoints to be hosted by FlowApplication when running with `--server`.
    ///
    /// This appends to any endpoints already registered via `with_web_endpoint(...)`.
    pub fn with_web_endpoints(mut self, mut endpoints: Vec<Box<dyn HttpEndpoint>>) -> Self {
        self.web_endpoints.append(&mut endpoints);
        self
    }

    /// Register a managed web surface to be hosted by FlowApplication when running with `--server`.
    ///
    /// This is the preferred extension point for HTTP-facing capabilities that should share
    /// `FlowApplication` lifecycle, readiness wiring, and shutdown behaviour by default.
    pub fn with_web_surface(mut self, surface: WebSurfaceAttachment) -> Self {
        self.web_surfaces.push(surface);
        self
    }

    /// Register the optional framework-owned HTTP ingress adaptor.
    pub fn with_http_ingress<T>(mut self, ingress: HttpIngress<T>) -> Self
    where
        T: TypedPayload + Send + Sync + 'static,
    {
        let (surface, handle) = ingress.into_surface_and_handle();
        self.web_surfaces.push(surface);
        self = self.with_ingress_handle(handle);
        self
    }

    /// Wire a developer-owned ingress handle to the built flow.
    pub fn with_ingress_handle<T>(mut self, handle: IngressHandle<T>) -> Self
    where
        T: TypedPayload + Send + Sync + 'static,
    {
        self.flow_handle_hooks.push(Box::new(move |flow_handle| {
            handle.bind_flow_handle(flow_handle)
        }));
        self
    }

    /// Add a single HTTP endpoint to be hosted by FlowApplication when running with `--server`.
    pub fn with_web_endpoint(mut self, endpoint: Box<dyn HttpEndpoint>) -> Self {
        self.web_endpoints.push(endpoint);
        self
    }

    /// Register a hook that runs after the flow is built (but before the server starts).
    ///
    /// This is useful for wiring FlowHandle state into other subsystems (e.g. ingestion readiness).
    pub fn with_flow_handle_hook<F>(mut self, hook: F) -> Self
    where
        F: Fn(&Arc<FlowHandle>) -> JoinHandle<()> + Send + Sync + 'static,
    {
        self.flow_handle_hooks
            .push(Box::new(move |flow_handle| Ok(hook(flow_handle))));
        self
    }

    pub fn with_presentation(mut self, presentation: Presentation) -> Self {
        self.presentation = Some(presentation);
        self
    }

    /// Run the flow in a blocking context (without #[tokio::main])
    ///
    /// This builds the tokio runtime, initializes observability, and runs the flow.
    /// Use this when you have a plain `fn main()` and want FlowApplication to
    /// manage the entire runtime lifecycle.
    pub fn run_blocking(self, flow: FlowDefinition) -> Result<(), ApplicationError> {
        // Build tokio runtime so we have a handle for console-subscriber
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| ApplicationError::RuntimeCreationFailed(e.to_string()))?;

        // Initialize tracing/console-subscriber using the runtime handle
        self.init_observability(Some(runtime.handle()));

        let FlowApplicationBuilder {
            config_file,
            web_surfaces,
            web_endpoints,
            flow_handle_hooks,
            presentation,
            cli_args,
            ..
        } = self;

        // Run the flow in the runtime
        runtime.block_on(FlowApplication::launch(
            flow,
            LaunchParams {
                builder_config_file: config_file,
                enable_autodiscovery: true,
                web_surfaces,
                extra_endpoints: web_endpoints,
                flow_handle_hooks,
                presentation,
                cli_args,
                #[cfg(test)]
                test_shutdown_signal: None,
            },
        ))
    }

    /// Run the flow in an existing async context (with #[tokio::main])
    ///
    /// Use this when you already have a tokio runtime (e.g., from #[tokio::main])
    /// and just want FlowApplication to handle observability setup.
    pub async fn run_async(self, flow: FlowDefinition) -> Result<(), ApplicationError> {
        // Initialize tracing/console-subscriber with the current runtime handle
        self.init_observability(Some(&tokio::runtime::Handle::current()));

        let FlowApplicationBuilder {
            config_file,
            web_surfaces,
            web_endpoints,
            flow_handle_hooks,
            presentation,
            cli_args,
            ..
        } = self;

        // Run the flow
        FlowApplication::launch(
            flow,
            LaunchParams {
                builder_config_file: config_file,
                enable_autodiscovery: true,
                web_surfaces,
                extra_endpoints: web_endpoints,
                flow_handle_hooks,
                presentation,
                cli_args,
                #[cfg(test)]
                test_shutdown_signal: None,
            },
        )
        .await
    }

    /// Initialize observability (tracing + console-subscriber)
    fn init_observability(&self, _runtime_handle: Option<&tokio::runtime::Handle>) {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;
        use tracing_subscriber::EnvFilter;

        // Determine log level (RUST_LOG env var takes precedence)
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            let level = self
                .log_level
                .as_ref()
                .unwrap_or(&LogLevel::Info)
                .as_filter_string();
            EnvFilter::new(level)
        });

        #[cfg(feature = "console")]
        if self.console_subscriber {
            // Set bind address for console-subscriber (honor existing env override)
            let bind = std::env::var("TOKIO_CONSOLE_BIND")
                .ok()
                .or_else(|| self.console_bind.clone())
                .unwrap_or_else(|| "127.0.0.1:6669".to_string());
            let addr: std::net::SocketAddr = bind.parse().unwrap_or_else(|err| {
                let fallback = "127.0.0.1:6669";
                eprintln!(
                    "❌ Invalid TOKIO_CONSOLE_BIND '{bind}': {err}. Falling back to {fallback}",
                );
                fallback.parse().expect("fallback address should parse")
            });
            // Ensure downstream tooling that relies on the env var still sees the effective address
            std::env::set_var("TOKIO_CONSOLE_BIND", &bind);
            eprintln!("ℹ️  tokio-console attempting to bind to {addr}");

            let builder = console_subscriber::ConsoleLayer::builder()
                .with_default_env()
                .server_addr(addr);
            let (console_layer, server) = builder.build();

            // Spawn console server with error logging so bind failures are visible instead of silent
            let bind_for_log = bind.clone();
            let spawn_server = async move {
                if let Err(err) = server.serve().await {
                    eprintln!("❌ tokio-console failed to bind on {bind_for_log}: {err}");
                }
            };
            // Small self-connect probe to surface connectivity issues early
            let addr_for_probe = addr;
            let spawn_probe = async move {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                match tokio::net::TcpStream::connect(addr_for_probe).await {
                    Ok(_) => eprintln!("✅ tokio-console TCP probe successful on {addr_for_probe}"),
                    Err(err) => {
                        eprintln!("❌ tokio-console TCP probe failed on {addr_for_probe}: {err}")
                    }
                }
            };

            if let Some(handle) = _runtime_handle {
                handle.spawn(spawn_server);
                handle.spawn(spawn_probe);
            } else {
                // Fallback: attempt to spawn on whatever runtime is available
                tokio::spawn(spawn_server);
                tokio::spawn(spawn_probe);
            }

            tracing_subscriber::registry()
                .with(console_layer)
                .with(tracing_subscriber::fmt::layer())
                .with(filter)
                .try_init()
                .ok();

            eprintln!("🚦 tokio-console enabled on {bind}");
            eprintln!("   Connect with: tokio-console http://{bind}");
            if !cfg!(tokio_unstable) {
                eprintln!("⚠️  Built without `--cfg tokio_unstable`; console may show limited data. Run with RUSTFLAGS=\"--cfg tokio_unstable\" for full instrumentation.");
            }
            return;
        }

        #[cfg(not(feature = "console"))]
        if self.console_subscriber {
            eprintln!("⚠️  Console subscriber requested but 'console' feature not enabled");
            eprintln!("   Recompile with --features obzenflow_infra/console");
        }

        // Standard tracing setup (no console-subscriber)
        let _ = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(filter)
            .try_init();
    }
}

/// The main application framework for running ObzenFlow flows
///
/// This provides a Spring Boot-style experience where users just call
/// `FlowApplication::run()` with their flow and the framework handles everything:
/// - CLI parsing (--server, --server-port)
/// - Server startup if requested
/// - Flow execution
/// - Graceful shutdown
///
/// # Example with #[tokio::main]
/// ```ignore
/// use obzenflow_infra::application::FlowApplication;
/// use obzenflow_dsl::flow;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     FlowApplication::run(flow! {
///         name: "my_flow",
///         // ... flow definition
///     }).await?;
///     Ok(())
/// }
/// ```
///
/// # Example with builder (console-subscriber, no #[tokio::main])
/// ```ignore
/// use obzenflow_infra::application::{FlowApplication, LogLevel};
///
/// fn main() -> Result<(), Box<dyn std::error::Error>> {
///     FlowApplication::builder()
///         .with_console_subscriber()
///         .with_log_level(LogLevel::Info)
///         .run_blocking(flow! {
///             name: "my_flow",
///             // ... flow definition
///         })?;
///     Ok(())
/// }
/// ```
pub struct FlowApplication;

impl FlowApplication {
    /// Create a builder for advanced configuration
    ///
    /// Use this when you need:
    /// - Console-subscriber integration
    /// - Custom log levels
    /// - Runtime creation without #[tokio::main]
    ///
    /// # Example
    /// ```ignore
    /// FlowApplication::builder()
    ///     .with_console_subscriber()
    ///     .with_log_level(LogLevel::Info)
    ///     .run_blocking(flow! { /* flow */ })
    /// ```
    pub fn builder() -> FlowApplicationBuilder {
        FlowApplicationBuilder::default()
    }

    /// Run a flow with automatic lifecycle management
    ///
    /// This is the only public method users need to call. It:
    /// 1. Parses CLI arguments automatically
    /// 2. Builds the flow from the provided future
    /// 3. Starts HTTP server if --server flag is present
    /// 4. Runs the flow to completion
    /// 5. Manages server lifecycle after flow completes
    ///
    /// # Arguments
    /// * `flow` - A flow definition produced by `flow!`
    ///
    /// # Returns
    /// * `Ok(())` if flow completes successfully
    /// * `Err(ApplicationError)` if flow fails or cannot start
    pub async fn run(flow: FlowDefinition) -> Result<(), ApplicationError> {
        Self::run_with_web_endpoints(flow, Vec::new()).await
    }

    pub async fn run_with_presentation(
        flow: FlowDefinition,
        presentation: Presentation,
    ) -> Result<(), ApplicationError> {
        Self::launch(
            flow,
            LaunchParams {
                presentation: Some(presentation),
                ..LaunchParams::autodiscovery_enabled()
            },
        )
        .await
    }

    /// Run a flow and host additional web endpoints when `--server` is enabled.
    pub async fn run_with_web_endpoints(
        flow: FlowDefinition,
        extra_endpoints: Vec<Box<dyn HttpEndpoint>>,
    ) -> Result<(), ApplicationError> {
        Self::run_with_web_endpoints_and_hooks(flow, extra_endpoints, Vec::new()).await
    }

    pub async fn run_with_web_endpoints_and_hooks(
        flow: FlowDefinition,
        extra_endpoints: Vec<Box<dyn HttpEndpoint>>,
        flow_handle_hooks: Vec<FlowHandleHook>,
    ) -> Result<(), ApplicationError> {
        Self::launch(
            flow,
            LaunchParams {
                extra_endpoints,
                flow_handle_hooks,
                ..LaunchParams::autodiscovery_enabled()
            },
        )
        .await
    }

    async fn launch(flow: FlowDefinition, params: LaunchParams) -> Result<(), ApplicationError> {
        let LaunchParams {
            builder_config_file,
            enable_autodiscovery,
            web_surfaces,
            extra_endpoints,
            flow_handle_hooks,
            presentation,
            cli_args,
            #[cfg(test)]
            test_shutdown_signal,
        } = params;

        // Best-effort tracing initialization when the builder isn't used.
        // This ensures examples like char_transform still emit logs without
        // requiring callers to wire tracing explicitly.
        {
            use tracing_subscriber::prelude::*;
            // Try env filter first; fall back to info if unset.
            let filter = tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
            let _ = tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer())
                .with(filter)
                .try_init();
        }

        let config = resolve_startup_config(builder_config_file, enable_autodiscovery, cli_args)?;

        // FLOWIP-120i: resolve the run mode once, for banner and footer copy.
        let run_mode = match &config.replay {
            Some(replay) => match replay.verb {
                obzenflow_runtime::bootstrap::ReplayVerb::Replay => {
                    super::run_mode::RunMode::replay_from_archive(replay.from.clone())
                }
                obzenflow_runtime::bootstrap::ReplayVerb::Resume => {
                    super::run_mode::RunMode::resume_from_archive(replay.from.clone())
                }
            },
            None => super::run_mode::RunMode::Live,
        };

        // FLOWIP-095j pre-flight: verification re-reads the source archive
        // after completion, so it must remain present through the run.
        if config.replay.as_ref().is_some_and(|replay| replay.verify) {
            if let Some(replay) = &config.replay {
                tracing::info!(
                    archive = %replay.from.display(),
                    "--verify: the source archive must remain present through completion"
                );
            }
        }

        let presentation_enabled = presentation.is_some();

        let grace_timeout = config.runtime.shutdown_timeout;
        #[cfg(feature = "warp-server")]
        let surface_metrics_interval = config.runtime.surface_metrics_interval;

        // Background tasks spawned by FlowHandle hooks and/or web surface wiring closures.
        // These must not be allowed to outlive FlowApplication, even on early-return paths.
        let mut managed_tasks: Vec<JoinHandle<()>> = Vec::new();
        #[cfg(feature = "warp-server")]
        let mut surface_metrics_emitter: Option<HttpSurfaceMetricsEmitter> = None;

        if let Some(presentation) = &presentation {
            let rendered = presentation.render_banner(&run_mode);
            for warning in rendered.warnings {
                tracing::warn!("{warning}");
            }
            print!("{}", rendered.text);
        }

        let (result, flow_name, run_state, stopped) = 'run: {
            if (!extra_endpoints.is_empty() || !web_surfaces.is_empty()) && !config.server.enabled {
                break 'run (
                    Err(ApplicationError::InvalidConfiguration(
                        "Web endpoints or surfaces were configured, but FlowApplication is not running with --server"
                            .to_string(),
                    )),
                    None,
                    None,
                    false,
                );
            }

            let control_plane_auth = config.server.control_plane_auth.clone();

            // FLOWIP-120u: the host opens the replay/resume input archive and the
            // bootstrap snapshot carries it; the build consumes it without the
            // factory opening anything. A bad archive fails here, before any
            // journal is created.
            let bootstrap_config = {
                let mut bootstrap_config = config.bootstrap_config();
                if let Some(replay) = bootstrap_config.replay.clone() {
                    let archive = crate::journal::disk::replay_archive::DiskReplayArchive::open(
                        replay.archive_path,
                        replay.allow_incomplete_archive,
                    )
                    .await
                    .map_err(|e| {
                        ApplicationError::InvalidConfiguration(format!(
                            "Failed to open replay archive: {e}"
                        ))
                    })?;
                    bootstrap_config.replay_archive = Some(Arc::new(archive));
                }
                bootstrap_config
            };

            let _bootstrap_guard = if cfg!(debug_assertions) {
                // In debug/test builds, allow Rust's parallel test runner to serialize installs
                // rather than failing unrelated tests with an overlapping-run error.
                install_bootstrap_config(bootstrap_config)
            } else {
                try_install_bootstrap_config(bootstrap_config).map_err(|_| {
                    ApplicationError::InvalidConfiguration(
                        "Overlapping FlowApplication runs in the same process are not supported"
                            .to_string(),
                    )
                })?
            };

            // Note: Logging/console-subscriber should be initialized in main() before
            // the tokio runtime is created for console_subscriber to work properly

            tracing::info!("🚀 Starting FlowApplication");

            // Build the flow (this executes the flow! macro) against the
            // explicit per-run context (FLOWIP-010 §7): the host owns the
            // resolved snapshot; the build consumes it as an input.
            let build_context = obzenflow_runtime::run_context::FlowBuildContext::new(
                config.runtime_config.clone(),
            );
            let flow_handle = match flow.build(build_context).await {
                Ok(handle) => handle,
                Err(failure) => {
                    // FLOWIP-120u F2: the failure carrier holds substrate state; None
                    // means the build failed before substrate selection, so no run
                    // directory exists to point at.
                    break 'run (
                        Err(ApplicationError::FlowBuildFailed(failure.error.to_string())),
                        None,
                        failure.run,
                        false,
                    );
                }
            };

            // The selected run substrate (FLOWIP-120u): durable with its locator,
            // or ephemeral with none. An ephemeral resume never reaches here;
            // the build refuses it (F13).
            let run_state = Some(flow_handle.run_substrate().clone());

            let print_replay_hint = |locator: &CurrentRunLocator| {
                println!("FlowApplication complete!");
                println!("To replay, add: --replay-from {locator}");
                println!("(Source config env vars are ignored during replay)");
            };

            let flow_handle = Arc::new(flow_handle);
            let flow_name = flow_handle.flow_name().to_string();

            for hook in &flow_handle_hooks {
                match hook(&flow_handle) {
                    Ok(task) => managed_tasks.push(task),
                    Err(err) => break 'run (Err(err), Some(flow_name.clone()), run_state, false),
                }
            }

            if let Some(exporter) = flow_handle.metrics_exporter() {
                let liveness_snapshots = flow_handle.liveness_snapshots();
                Self::publish_infra_snapshot(&exporter, liveness_snapshots.as_ref());
                managed_tasks.push(Self::spawn_infra_metrics_collector(
                    exporter,
                    liveness_snapshots,
                    config.runtime.surface_metrics_interval,
                ));
            }

            #[cfg(feature = "warp-server")]
            let surface_metrics_collector = {
                let system_journal = flow_handle.system_journal();
                let surface_metrics_collector = if config.server.enabled
                    && !web_surfaces.is_empty()
                    && system_journal.is_some()
                {
                    Some(Arc::new(HttpSurfaceMetricsCollector::new()))
                } else {
                    None
                };

                if let (Some(collector), Some(system_journal)) =
                    (surface_metrics_collector.clone(), system_journal)
                {
                    let emitter = HttpSurfaceMetricsEmitter::new(collector, system_journal);
                    managed_tasks.push(emitter.spawn_periodic(surface_metrics_interval));
                    surface_metrics_emitter = Some(emitter);
                }

                surface_metrics_collector
            };

            let mut all_extra_endpoints = extra_endpoints;
            for surface in web_surfaces {
                let (surface_name, endpoints, wiring, ingress_slot) = surface.into_parts();
                // FLOWIP-115d (AC41): a registered hosted ingress surface whose
                // source half was never placed in flow topology has an unfilled
                // binding slot. Fail startup before serving endpoints rather than
                // silently running without its configured ingress identity.
                if let Some(slot) = ingress_slot {
                    if !slot.is_filled() {
                        break 'run (
                            Err(ApplicationError::FlowBuildFailed(format!(
                                "hosted ingress surface '{surface_name}' (ingress key '{}') was \
                                 registered but its source half was not placed in the flow \
                                 topology; place the http_ingress source in flow!",
                                slot.ingress_key()
                            ))),
                            Some(flow_name.clone()),
                            run_state,
                            false,
                        );
                    }
                }
                for endpoint in endpoints {
                    all_extra_endpoints.push(label_endpoint(&surface_name, endpoint));
                }
                if let Some(wiring) = wiring {
                    match wiring(WebSurfaceWiringContext {
                        pipeline_state: flow_handle.state_receiver(),
                        // FLOWIP-115d: hand hosted ingress surfaces the host system
                        // journal so they can append refusal facts. A surface with
                        // refusal recording enabled fails startup here if it is None.
                        system_journal: flow_handle.system_journal(),
                    }) {
                        Ok(wired) => managed_tasks.extend(wired.tasks),
                        Err(err) => {
                            break 'run (Err(err), Some(flow_name.clone()), run_state, false)
                        }
                    }
                }
                tracing::debug!(surface = %surface_name, "Web surface attached");
            }

            // FLOWIP-114d: per-process incarnation identity (stamped into the
            // SSE bootstrap event and every registration) and the terminal-
            // gated listener-close signal (gap 8).
            let runtime_instance_id = ulid::Ulid::new().to_string();
            let (server_shutdown_tx, server_shutdown_rx) = tokio::sync::watch::channel(false);

            // Start server if --server flag present
            let server_handle = if config.server.enabled {
                let cors_mode = match config.server.cors_mode {
                    CorsModeArg::AllowAnyOrigin => CorsMode::AllowAnyOrigin,
                    CorsModeArg::AllowList => {
                        CorsMode::AllowList(config.server.cors_allow_origin.clone())
                    }
                    CorsModeArg::SameOrigin => CorsMode::SameOrigin,
                };
                let mut server_config =
                    ServerConfig::new(config.server.host.clone(), config.server.port);
                server_config.cors = Some(CorsConfig { mode: cors_mode });
                server_config.max_body_size = Some(config.server.max_body_size_bytes);
                server_config.request_timeout_secs = Some(config.server.request_timeout_secs);
                server_config.control_plane_auth = control_plane_auth;

                let start_result = {
                    #[cfg(feature = "warp-server")]
                    {
                        Self::start_server(
                            &flow_handle,
                            server_config,
                            all_extra_endpoints,
                            surface_metrics_collector,
                            config.runtime_config.clone(),
                            runtime_instance_id.clone(),
                            server_shutdown_rx.clone(),
                        )
                        .await
                    }

                    #[cfg(not(feature = "warp-server"))]
                    {
                        Self::start_server(
                            &flow_handle,
                            server_config,
                            all_extra_endpoints,
                            runtime_instance_id.clone(),
                            server_shutdown_rx.clone(),
                        )
                        .await
                    }
                };

                match start_result {
                    Ok(server_handle) => server_handle,
                    Err(err) => {
                        break 'run (Err(err), Some(flow_name), run_state, false);
                    }
                }
            } else {
                None
            };

            // FLOWIP-114d: registration heartbeat as a managed task.
            // Deregistration is exit-tied through the shutdown signal in both
            // on_terminal modes; park keeps renewing with the terminal phase.
            #[cfg(feature = "studio-registration")]
            if let Some(studio) = config.studio.clone() {
                if server_handle.is_some() {
                    managed_tasks.push(crate::web::studio_registration::spawn_heartbeat(
                        crate::web::studio_registration::HeartbeatContext {
                            studio,
                            runtime_instance_id: runtime_instance_id.clone(),
                            flow_name: flow_name.clone(),
                            startup_mode: config.server.startup_mode,
                            probe_base_url: format!(
                                "http://{}:{}",
                                config.server.host, config.server.port
                            ),
                        },
                        flow_handle.state_receiver(),
                        server_shutdown_rx.clone(),
                    ));
                }
            }

            if config.server.enabled {
                if server_handle.is_none() {
                    match config.server.startup_mode {
                        StartupMode::Manual => {
                            break 'run (
                                Err(ApplicationError::FeatureNotEnabled(
                                    "warp-server".to_string(),
                                )),
                                Some(flow_name),
                                run_state,
                                false,
                            );
                        }
                        StartupMode::Auto => {
                            tracing::warn!("⚠️  Continuing without HTTP server");
                            tracing::info!("▶️  Starting flow execution (no server)");
                            let handle = match Arc::try_unwrap(flow_handle) {
                                Ok(handle) => handle,
                                Err(_) => {
                                    break 'run (
                                        Err(ApplicationError::FlowExecutionFailed(
                                            "Failed to unwrap FlowHandle for non-server execution"
                                                .to_string(),
                                        )),
                                        Some(flow_name),
                                        run_state,
                                        false,
                                    );
                                }
                            };
                            let result = handle
                                .run()
                                .await
                                .map_err(|e| ApplicationError::FlowExecutionFailed(e.to_string()));
                            if result.is_ok() && !presentation_enabled {
                                if let Some(locator) = run_state.as_ref().and_then(|s| s.locator())
                                {
                                    print_replay_hint(locator);
                                }
                            }
                            break 'run (result, Some(flow_name), run_state, false);
                        }
                    }
                }

                // Server mode: lifecycle is controlled via HTTP (and optionally startup_mode).
                match config.server.startup_mode {
                    StartupMode::Auto => {
                        tracing::info!("▶️  Starting flow execution (startup_mode=auto)");
                        if let Err(err) = flow_handle
                            .start()
                            .await
                            .map_err(|e| ApplicationError::FlowExecutionFailed(e.to_string()))
                        {
                            break 'run (Err(err), Some(flow_name), run_state, false);
                        }
                    }
                    StartupMode::Manual => {
                        tracing::info!(
                            "⏸️  startup_mode=manual; waiting for Play via /api/flow/control"
                        );
                    }
                }

                if let Some(server_task) = server_handle {
                    tracing::info!(
                        "📊 Server running on http://{}:{}",
                        config.server.host,
                        config.server.port
                    );
                    tracing::info!("⏸️  Press Ctrl+C to cancel; send SIGTERM to graceful-stop...");

                    #[cfg(unix)]
                    let mut sigterm_stream = match tokio::signal::unix::signal(
                        tokio::signal::unix::SignalKind::terminate(),
                    ) {
                        Ok(stream) => stream,
                        Err(err) => {
                            break 'run (
                                Err(ApplicationError::from(err)),
                                Some(flow_name),
                                run_state,
                                false,
                            );
                        }
                    };

                    // FLOWIP-114d gap 9: on_terminal=exit adds a terminal arm to
                    // the wait, so a finished flow closes and exits instead of
                    // parking until a signal.
                    enum ServeOutcome {
                        Signal(ShutdownSignal),
                        FlowTerminal,
                    }

                    let exit_on_terminal = config.server.on_terminal == OnTerminalArg::Exit;
                    let mut terminal_rx = flow_handle.state_receiver();
                    let wait_terminal = async move {
                        loop {
                            if terminal_rx.borrow().is_terminal() {
                                break;
                            }
                            if terminal_rx.changed().await.is_err() {
                                break;
                            }
                        }
                    };

                    // Wait for the first shutdown trigger:
                    // - SIGINT (Ctrl+C) => Cancel
                    // - SIGTERM => GracefulStop(timeout=GRACE)
                    // - terminal pipeline state (on_terminal=exit) => close and exit
                    #[cfg(test)]
                    let outcome = if let Some(test_shutdown_signal) = test_shutdown_signal {
                        ServeOutcome::Signal(
                            test_shutdown_signal.await.unwrap_or(ShutdownSignal::Sigint),
                        )
                    } else {
                        #[cfg(unix)]
                        {
                            tokio::select! {
                                _ = tokio::signal::ctrl_c() => ServeOutcome::Signal(ShutdownSignal::Sigint),
                                _ = sigterm_stream.recv() => ServeOutcome::Signal(ShutdownSignal::Sigterm),
                                _ = wait_terminal, if exit_on_terminal => ServeOutcome::FlowTerminal,
                            }
                        }
                        #[cfg(not(unix))]
                        {
                            tokio::select! {
                                signal = tokio::signal::ctrl_c() => {
                                    if let Err(err) = signal {
                                        break 'run (
                                            Err(ApplicationError::from(err)),
                                            Some(flow_name),
                                            run_state,
                                            false,
                                        );
                                    }
                                    ServeOutcome::Signal(ShutdownSignal::Sigint)
                                }
                                _ = wait_terminal, if exit_on_terminal => ServeOutcome::FlowTerminal,
                            }
                        }
                    };

                    #[cfg(not(test))]
                    let outcome = {
                        #[cfg(unix)]
                        {
                            tokio::select! {
                                _ = tokio::signal::ctrl_c() => ServeOutcome::Signal(ShutdownSignal::Sigint),
                                _ = sigterm_stream.recv() => ServeOutcome::Signal(ShutdownSignal::Sigterm),
                                _ = wait_terminal, if exit_on_terminal => ServeOutcome::FlowTerminal,
                            }
                        }
                        #[cfg(not(unix))]
                        {
                            tokio::select! {
                                signal = tokio::signal::ctrl_c() => {
                                    if let Err(err) = signal {
                                        break 'run (
                                            Err(ApplicationError::from(err)),
                                            Some(flow_name),
                                            run_state,
                                            false,
                                        );
                                    }
                                    ServeOutcome::Signal(ShutdownSignal::Sigint)
                                }
                                _ = wait_terminal, if exit_on_terminal => ServeOutcome::FlowTerminal,
                            }
                        }
                    };

                    // FLOWIP-114d gap 8: the web surface keeps serving through
                    // the drain. Observation stays live; Play is refused by the
                    // FSM guard and push ingress refuses via admission. The
                    // listener closes only after the terminal state.
                    let signal_phase = match outcome {
                        ServeOutcome::Signal(first_signal) => {
                            tracing::info!(
                                ?first_signal,
                                "👋 Shutting down; web surface serves through the drain"
                            );
                            // Best-effort: stop the flow before tearing down the runtime.
                            //
                            // We avoid force-aborting here because it can cancel in-flight disk journal
                            // writes (spawn_blocking), which then surfaces as "Background writer task was
                            // cancelled/panicked" errors inside stage supervisors.
                            match first_signal {
                                ShutdownSignal::Sigint => {
                                    if let Err(e) = flow_handle.stop_cancel().await {
                                        tracing::warn!(
                                            error = %e,
                                            "Failed to request flow cancel during SIGINT shutdown; continuing shutdown"
                                        );
                                    }
                                }
                                ShutdownSignal::Sigterm => {
                                    if let Err(e) = flow_handle.stop_graceful(grace_timeout).await {
                                        tracing::warn!(
                                            error = %e,
                                            "Failed to request flow graceful stop during SIGTERM shutdown; continuing shutdown"
                                        );
                                    }
                                }
                            }
                            Some(first_signal)
                        }
                        ServeOutcome::FlowTerminal => {
                            tracing::info!(
                                "🏁 Flow reached a terminal state (on_terminal=exit); closing"
                            );
                            None
                        }
                    };

                    if let Some(first_signal) = signal_phase {
                        let mut phase = first_signal;
                        let mut phase_deadline = match first_signal {
                            ShutdownSignal::Sigint => Instant::now() + grace_timeout,
                            ShutdownSignal::Sigterm => Instant::now() + grace_timeout,
                        };

                        while flow_handle.is_running() {
                            if Instant::now() >= phase_deadline {
                                if phase == ShutdownSignal::Sigterm {
                                    tracing::warn!(
                                        grace_secs = grace_timeout.as_secs(),
                                        "Graceful stop timeout expired; escalating to cancel"
                                    );
                                    if let Err(e) = flow_handle.stop_cancel_timeout().await {
                                        tracing::warn!(
                                            error = %e,
                                            "Failed to request flow cancel during escalation; continuing shutdown"
                                        );
                                    }
                                    phase = ShutdownSignal::Sigint;
                                    phase_deadline = Instant::now() + grace_timeout;
                                    continue;
                                }

                                tracing::warn!(
                                shutdown_timeout_secs = grace_timeout.as_secs(),
                                "Flow did not terminate within shutdown timeout; exiting anyway"
                            );
                                break;
                            }

                            if phase == ShutdownSignal::Sigterm {
                                #[cfg(unix)]
                                {
                                    tokio::select! {
                                        _ = tokio::time::sleep(Duration::from_millis(50)) => {},
                                        _ = tokio::signal::ctrl_c() => {
                                            tracing::warn!("Second SIGINT during graceful stop; escalating to cancel");
                                            let _ = flow_handle.stop_cancel().await;
                                            phase = ShutdownSignal::Sigint;
                                            phase_deadline = Instant::now() + grace_timeout;
                                        }
                                        _ = sigterm_stream.recv() => {
                                            tracing::warn!("Second SIGTERM during graceful stop; escalating to cancel");
                                            let _ = flow_handle.stop_cancel().await;
                                            phase = ShutdownSignal::Sigint;
                                            phase_deadline = Instant::now() + grace_timeout;
                                        }
                                    }
                                }
                                #[cfg(not(unix))]
                                {
                                    tokio::select! {
                                        _ = tokio::time::sleep(Duration::from_millis(50)) => {},
                                        _ = tokio::signal::ctrl_c() => {
                                            tracing::warn!("Second SIGINT during graceful stop; escalating to cancel");
                                            let _ = flow_handle.stop_cancel().await;
                                            phase = ShutdownSignal::Sigint;
                                            phase_deadline = Instant::now() + grace_timeout;
                                        }
                                    }
                                }
                            } else {
                                tokio::time::sleep(Duration::from_millis(50)).await;
                            }
                        }
                    }

                    // Terminal state reached (or the shutdown timeout expired).
                    // The heartbeat deregisters and SSE producers flush the
                    // terminal event on this signal; the listener close carries
                    // a bounded deadline with abort as the escalation backstop,
                    // mirroring the flow's own graceful-to-cancel ladder.
                    let _ = server_shutdown_tx.send(true);
                    const SERVER_CLOSE_GRACE: Duration = Duration::from_secs(5);
                    let mut server_task = server_task;
                    if tokio::time::timeout(SERVER_CLOSE_GRACE, &mut server_task)
                        .await
                        .is_err()
                    {
                        tracing::warn!(
                            close_grace_secs = SERVER_CLOSE_GRACE.as_secs(),
                            "Web server did not close within the grace period; aborting listener"
                        );
                        server_task.abort();
                        let _ = server_task.await;
                    }

                    // FLOWIP-114d gap 9: truthful exit codes. A flow that
                    // failed on its own must not exit zero; an operator stop
                    // is deliberate and exits zero even when cancel semantics
                    // land the FSM in Failed{user_stop}.
                    if signal_phase.is_none() {
                        let final_state = flow_handle.current_state();
                        if let obzenflow_runtime::pipeline::PipelineState::Failed {
                            reason, ..
                        } = &final_state
                        {
                            break 'run (
                                Err(ApplicationError::FlowExecutionFailed(reason.clone())),
                                Some(flow_name),
                                run_state,
                                true,
                            );
                        }
                    }
                }

                break 'run (Ok(()), Some(flow_name), run_state, true);
            }

            // Non-server mode: preserve existing behaviour (run to completion, no HTTP server)
            tracing::info!("▶️  Starting flow execution (no server)");
            let handle = match Arc::try_unwrap(flow_handle) {
                Ok(handle) => handle,
                Err(_) => {
                    break 'run (
                        Err(ApplicationError::FlowExecutionFailed(
                            "Failed to unwrap FlowHandle for non-server execution".to_string(),
                        )),
                        Some(flow_name),
                        run_state,
                        false,
                    );
                }
            };
            let result = handle
                .run()
                .await
                .map_err(|e| ApplicationError::FlowExecutionFailed(e.to_string()));
            if result.is_ok() && !presentation_enabled {
                if let Some(locator) = run_state.as_ref().and_then(|s| s.locator()) {
                    print_replay_hint(locator);
                }
            }
            break 'run (result, Some(flow_name), run_state, false);
        };

        #[cfg(feature = "warp-server")]
        if let Some(emitter) = &surface_metrics_emitter {
            let _ = tokio::time::timeout(grace_timeout, emitter.flush()).await;
        }

        // Best-effort: ensure any hook/surface background tasks cannot escape `FlowApplication`
        // lifetime, even if we exited early due to a startup failure or "no server" fallback.
        Self::cancel_and_join_tasks(managed_tasks, grace_timeout).await;

        match (result, flow_name, run_state, stopped) {
            (Ok(()), flow_name, run_state, stopped) => {
                let location = run_state.as_ref().and_then(|s| s.locator()).cloned();
                // FLOWIP-095j: keep the candidate run directory before the
                // outcome takes ownership of the location below.
                let verify_candidate_dir = location
                    .as_ref()
                    .map(|locator| locator.path().to_path_buf());
                if let Some(presentation) = &presentation {
                    let flow_name = flow_name.unwrap_or_else(|| "Flow".to_string());
                    let outcome = if stopped {
                        RunPresentationOutcome::Stopped {
                            flow_name,
                            location,
                            run_mode: run_mode.clone(),
                        }
                    } else {
                        RunPresentationOutcome::Completed {
                            flow_name,
                            location,
                            run_mode: run_mode.clone(),
                        }
                    };
                    let rendered_footer_banner = presentation.render_footer_banner();
                    let footer = presentation.render_footer(outcome);
                    if rendered_footer_banner.is_some() || !footer.trim().is_empty() {
                        println!();
                        if let Some(rendered_banner) = rendered_footer_banner {
                            for warning in rendered_banner.warnings {
                                tracing::warn!("{warning}");
                            }
                            print!("{}", rendered_banner.text);
                        }
                        if !footer.trim().is_empty() {
                            println!("{footer}");
                        }
                    }
                }

                // FLOWIP-095j: verify the replay output against the source
                // archive after the run reaches its terminal outcome.
                if config.replay.as_ref().is_some_and(|replay| replay.verify) {
                    if let super::run_mode::RunMode::Replay(ctx) = &run_mode {
                        return Self::run_post_replay_verification(
                            ctx.archive_path.clone(),
                            verify_candidate_dir,
                        )
                        .await;
                    }
                }
                Ok(())
            }
            (Err(err), flow_name, run_state, _) => {
                if let Some(presentation) = &presentation {
                    let rendered_footer_banner = presentation.render_footer_banner();
                    let footer = presentation.render_footer(RunPresentationOutcome::Failed {
                        flow_name,
                        error: err.to_string(),
                        location: run_state.as_ref().and_then(|s| s.locator()).cloned(),
                        run_mode: run_mode.clone(),
                    });
                    if rendered_footer_banner.is_some() || !footer.trim().is_empty() {
                        println!();
                        if let Some(rendered_banner) = rendered_footer_banner {
                            for warning in rendered_banner.warnings {
                                tracing::warn!("{warning}");
                            }
                            print!("{}", rendered_banner.text);
                        }
                        if !footer.trim().is_empty() {
                            println!("{footer}");
                        }
                    }
                }
                Err(err)
            }
        }
    }

    /// FLOWIP-095j: the `--verify` convenience form. Compares the just-written
    /// replay run against the source archive it replayed and maps the verdict
    /// onto the exit-code contract (`Ok(())` is exit 0, the fully certified
    /// match; codes 1/2/3 travel as `ApplicationError::Verification`).
    async fn run_post_replay_verification(
        baseline: PathBuf,
        candidate: Option<PathBuf>,
    ) -> Result<(), ApplicationError> {
        let skip = |summary: String| {
            println!("\n{summary}");
            Err(ApplicationError::Verification {
                exit_code: 3,
                summary,
            })
        };

        let Some(candidate) = candidate else {
            return skip(
                "verification skipped: this run wrote no durable run directory (verification needs disk journals)"
                    .to_string(),
            );
        };
        if std::fs::metadata(&baseline).is_err() {
            return skip(format!(
                "verification skipped: source archive unavailable ({})\nrun later: obzenflow verify --baseline {} --candidate {}",
                baseline.display(),
                baseline.display(),
                candidate.display()
            ));
        }

        let options = crate::verify::VerifyOptions::default();
        let outcome = tokio::task::spawn_blocking(move || {
            crate::verify::verify_run_dirs(&baseline, &candidate, &options)
        })
        .await
        .map_err(|err| ApplicationError::Other(Box::new(err)))?
        .map_err(|err| ApplicationError::Other(Box::new(err)))?;

        let rendered = crate::verify::render_verdict(&outcome);
        println!("\n{rendered}");

        match outcome.exit_code() {
            0 => Ok(()),
            exit_code => Err(ApplicationError::Verification {
                exit_code,
                summary: rendered.lines().next().unwrap_or_default().to_string(),
            }),
        }
    }

    async fn cancel_and_join_tasks(tasks: Vec<JoinHandle<()>>, timeout: Duration) {
        if tasks.is_empty() {
            return;
        }

        for task in &tasks {
            task.abort();
        }

        let _ = tokio::time::timeout(timeout, async move {
            for task in tasks {
                let _ = task.await;
            }
        })
        .await;
    }

    fn build_infra_snapshot(
        liveness_snapshots: Option<&LivenessSnapshots>,
    ) -> InfraMetricsSnapshot {
        let mut snapshot = InfraMetricsSnapshot::default();
        if let Some(liveness_snapshots) = liveness_snapshots {
            liveness_snapshots.with_read(|guard| {
                for (stage_id, stage) in guard.iter() {
                    snapshot
                        .liveness_metrics
                        .stage_handler_blocked_seconds
                        .insert(
                            *stage_id,
                            stage
                                .handler_blocked_ms
                                .map(|ms| ms.0 as f64 / 1000.0)
                                .unwrap_or(0.0),
                        );

                    let activity_state = match &stage.activity {
                        obzenflow_core::event::system_event::StageActivity::Polling => 0.0,
                        obzenflow_core::event::system_event::StageActivity::Processing {
                            ..
                        } => 1.0,
                        obzenflow_core::event::system_event::StageActivity::Draining => 2.0,
                        obzenflow_core::event::system_event::StageActivity::Completed => 3.0,
                        obzenflow_core::event::system_event::StageActivity::WaitingOnQuietInput {
                            ..
                        } => 4.0,
                    };
                    snapshot
                        .liveness_metrics
                        .stage_activity_state
                        .insert(*stage_id, activity_state);

                    for edge in &stage.edges {
                        snapshot
                            .liveness_metrics
                            .edge_idle_seconds
                            .insert((edge.upstream, edge.reader), edge.idle_ms.0 as f64 / 1000.0);
                    }
                }
            });
        }

        snapshot
    }

    fn publish_infra_snapshot(
        exporter: &Arc<dyn MetricsExporter>,
        liveness_snapshots: Option<&LivenessSnapshots>,
    ) {
        let snapshot = Self::build_infra_snapshot(liveness_snapshots);
        if let Err(err) = exporter.update_infra_metrics(snapshot) {
            tracing::warn!("Failed to export infrastructure metrics: {}", err);
        }
    }

    fn spawn_infra_metrics_collector(
        exporter: Arc<dyn MetricsExporter>,
        liveness_snapshots: Option<LivenessSnapshots>,
        interval: Duration,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                let snapshot = Self::build_infra_snapshot(liveness_snapshots.as_ref());
                if let Err(err) = exporter.update_infra_metrics(snapshot) {
                    tracing::warn!("Failed to export infrastructure metrics: {}", err);
                }
            }
        })
    }

    /// Internal: Start the web server with all endpoints
    #[cfg(feature = "warp-server")]
    async fn start_server(
        _flow_handle: &Arc<FlowHandle>,
        _server_config: ServerConfig,
        extra_endpoints: Vec<Box<dyn HttpEndpoint>>,
        surface_metrics: Option<Arc<HttpSurfaceMetricsCollector>>,
        runtime_config: Arc<obzenflow_runtime::runtime_config::ResolvedRuntimeConfig>,
        runtime_instance_id: String,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<Option<JoinHandle<()>>, ApplicationError> {
        use crate::web::start_web_server_with_config;
        use crate::web::web_server::WebServerResources;

        // Every flow has a topology - it's required to run
        let topology = _flow_handle.topology().ok_or_else(|| {
            ApplicationError::ServerStartFailed(
                "Flow missing topology - this should never happen".to_string(),
            )
        })?;
        let metrics = _flow_handle.metrics_exporter();
        let has_metrics = metrics.is_some();
        let addr = _server_config.address();

        // FLOWIP-114b: stage typing, join metadata, middleware, and
        // subgraph annotations live on the canonical `Topology` directly,
        // so only the contract side map needs to be threaded through.
        let contract_attachments = _flow_handle.contract_attachments();

        let handle = start_web_server_with_config(
            WebServerResources {
                topology,
                contract_attachments,
                metrics_exporter: metrics,
                flow_handle: Some(_flow_handle.clone()),
                extra_endpoints,
                surface_metrics,
                runtime_config: Some(runtime_config),
                runtime_instance_id: Some(runtime_instance_id),
                shutdown: Some(shutdown),
            },
            _server_config,
        )
        .await
        .map_err(|e| ApplicationError::ServerStartFailed(e.to_string()))?;

        tracing::info!("📊 Web server started on http://{}", addr);
        tracing::info!("   /api/topology  - Flow structure");
        tracing::info!("   /api/config    - Resolved configuration (read-only)");
        if has_metrics {
            tracing::info!("   /metrics       - Prometheus metrics");
        }
        tracing::info!("   /health        - Health status");
        tracing::info!("   /ready         - Readiness status");

        Ok(Some(handle))
    }

    /// Internal: Start the web server with all endpoints
    #[cfg(not(feature = "warp-server"))]
    async fn start_server(
        _flow_handle: &Arc<FlowHandle>,
        _server_config: ServerConfig,
        extra_endpoints: Vec<Box<dyn HttpEndpoint>>,
        _runtime_instance_id: String,
        _shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<Option<JoinHandle<()>>, ApplicationError> {
        if !extra_endpoints.is_empty() {
            return Err(ApplicationError::FeatureNotEnabled(
                "warp-server".to_string(),
            ));
        }
        tracing::warn!("⚠️  --server flag requires warp-server feature");
        tracing::warn!("   Recompile with --features obzenflow_infra/warp-server");
        tracing::warn!("");
        tracing::warn!("   Continuing without HTTP server...");
        Ok(None)
    }
}
