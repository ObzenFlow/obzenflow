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
use crate::application::config::CorsModeArg;
use crate::application::config::StartupMode;
#[cfg(feature = "warp-server")]
use crate::web::surface_metrics::{HttpSurfaceMetricsCollector, HttpSurfaceMetricsEmitter};
use clap::Parser;
use obzenflow_core::web::{CorsConfig, CorsMode, HttpEndpoint, ServerConfig};
use obzenflow_dsl::FlowDefinition;
use obzenflow_runtime::prelude::FlowHandle;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

type FlowHandleHook = Box<dyn Fn(&Arc<FlowHandle>) -> JoinHandle<()> + Send + Sync>;

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
    web_surfaces: Vec<WebSurfaceAttachment>,
    web_endpoints: Vec<Box<dyn HttpEndpoint>>,
    flow_handle_hooks: Vec<FlowHandleHook>,
    presentation: Option<Presentation>,
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
        self.flow_handle_hooks.push(Box::new(hook));
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

        // Run the flow in the runtime
        runtime.block_on(
            FlowApplication::run_with_web_endpoints_and_hooks_and_presentation(
                flow,
                self.web_surfaces,
                self.web_endpoints,
                self.flow_handle_hooks,
                self.presentation,
            ),
        )
    }

    /// Run the flow in an existing async context (with #[tokio::main])
    ///
    /// Use this when you already have a tokio runtime (e.g., from #[tokio::main])
    /// and just want FlowApplication to handle observability setup.
    pub async fn run_async(self, flow: FlowDefinition) -> Result<(), ApplicationError> {
        // Initialize tracing/console-subscriber with the current runtime handle
        self.init_observability(Some(&tokio::runtime::Handle::current()));

        // Run the flow
        FlowApplication::run_with_web_endpoints_and_hooks_and_presentation(
            flow,
            self.web_surfaces,
            self.web_endpoints,
            self.flow_handle_hooks,
            self.presentation,
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
                .init();

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
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(filter)
            .init();
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
        Self::run_with_web_endpoints_and_hooks_and_presentation(
            flow,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Some(presentation),
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
        Self::run_with_web_endpoints_and_hooks_and_presentation(
            flow,
            Vec::new(),
            extra_endpoints,
            flow_handle_hooks,
            None,
        )
        .await
    }

    pub(crate) async fn run_with_web_endpoints_and_hooks_and_presentation(
        flow: FlowDefinition,
        web_surfaces: Vec<WebSurfaceAttachment>,
        extra_endpoints: Vec<Box<dyn HttpEndpoint>>,
        flow_handle_hooks: Vec<FlowHandleHook>,
        presentation: Option<Presentation>,
    ) -> Result<(), ApplicationError> {
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

        // Parse CLI arguments automatically (like Spring Boot)
        let config = FlowConfig::parse();

        let presentation_enabled = presentation.is_some();

        // Clear any stale run-dir hint from prior FlowApplication runs (OT-17).
        let _ = crate::journal::factory::take_last_run_dir();

        let shutdown_timeout_secs = std::env::var("OBZENFLOW_SHUTDOWN_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(30);
        let grace_timeout = Duration::from_secs(shutdown_timeout_secs);

        #[cfg(feature = "warp-server")]
        let surface_metrics_interval = {
            let surface_metrics_interval_secs =
                std::env::var("OBZENFLOW_SURFACE_METRICS_INTERVAL_SECS")
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(15);
            Duration::from_secs(surface_metrics_interval_secs)
        };

        // Background tasks spawned by FlowHandle hooks and/or web surface wiring closures.
        // These must not be allowed to outlive FlowApplication, even on early-return paths.
        let mut managed_tasks: Vec<JoinHandle<()>> = Vec::new();
        #[cfg(feature = "warp-server")]
        let mut surface_metrics_emitter: Option<HttpSurfaceMetricsEmitter> = None;

        if let Some(presentation) = &presentation {
            let rendered = presentation.banner().render_for_stdout();
            for warning in rendered.warnings {
                tracing::warn!("{warning}");
            }
            print!("{}", rendered.text);
        }

        let (result, flow_name, run_dir, stopped) = 'run: {
            if (!extra_endpoints.is_empty() || !web_surfaces.is_empty()) && !config.server {
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

            if !config.cors_allow_origin.is_empty() && config.cors_mode != CorsModeArg::AllowList {
                break 'run (
                    Err(ApplicationError::InvalidConfiguration(
                        "--cors-allow-origin requires --cors-mode=allow-list".to_string(),
                    )),
                    None,
                    None,
                    false,
                );
            }
            if config.cors_mode == CorsModeArg::AllowList && config.cors_allow_origin.is_empty() {
                break 'run (
                    Err(ApplicationError::InvalidConfiguration(
                        "--cors-mode=allow-list requires at least one --cors-allow-origin"
                            .to_string(),
                    )),
                    None,
                    None,
                    false,
                );
            }

            if config.allow_incomplete_archive && config.replay_from.is_none() {
                break 'run (
                    Err(ApplicationError::InvalidConfiguration(
                        "--allow-incomplete-archive requires --replay-from".to_string(),
                    )),
                    None,
                    None,
                    false,
                );
            }

            if let Some(path) = &config.replay_from {
                if !path.is_dir() {
                    break 'run (
                        Err(ApplicationError::InvalidConfiguration(format!(
                            "--replay-from must be a directory: {}",
                            path.display()
                        ))),
                        None,
                        None,
                        false,
                    );
                }
                let manifest_path = path.join(obzenflow_core::journal::RUN_MANIFEST_FILENAME);
                if !manifest_path.exists() {
                    break 'run (
                        Err(ApplicationError::InvalidConfiguration(format!(
                            "Replay archive is missing {} at {}",
                            obzenflow_core::journal::RUN_MANIFEST_FILENAME,
                            manifest_path.display()
                        ))),
                        None,
                        None,
                        false,
                    );
                }
            }

            // Export replay options to environment so infra can inject a replay archive
            // implementation into runtime services during flow build (FLOWIP-095a).
            match &config.replay_from {
                Some(path) => std::env::set_var("OBZENFLOW_REPLAY_FROM", path),
                None => std::env::remove_var("OBZENFLOW_REPLAY_FROM"),
            }
            if config.allow_incomplete_archive {
                std::env::set_var("OBZENFLOW_ALLOW_INCOMPLETE_ARCHIVE", "1");
            } else {
                std::env::remove_var("OBZENFLOW_ALLOW_INCOMPLETE_ARCHIVE");
            }

            // Export startup mode to environment so lower layers (runtime_services)
            // can adjust behaviour (e.g. disable auto-Run in manual mode).
            // This is primarily used by the pipeline supervisor (FLOWIP-059).
            if config.server {
                match config.startup_mode {
                    StartupMode::Auto => {
                        std::env::set_var("OBZENFLOW_STARTUP_MODE", "auto");
                    }
                    StartupMode::Manual => {
                        std::env::set_var("OBZENFLOW_STARTUP_MODE", "manual");
                    }
                }
            }

            // Note: Logging/console-subscriber should be initialized in main() before
            // the tokio runtime is created for console_subscriber to work properly

            tracing::info!("🚀 Starting FlowApplication");

            // Build the flow (this executes the flow! macro)
            let flow_handle = match flow.await {
                Ok(handle) => handle,
                Err(e) => {
                    let run_dir = crate::journal::factory::take_last_run_dir();
                    break 'run (
                        Err(ApplicationError::FlowBuildFailed(e.to_string())),
                        None,
                        run_dir,
                        false,
                    );
                }
            };

            // If disk journals were used, this is the on-disk run directory path (OT-17).
            let run_dir = crate::journal::factory::take_last_run_dir();

            let print_replay_hint = |run_dir: &std::path::Path| {
                println!("FlowApplication complete!");
                println!("To replay, add: --replay-from {}", run_dir.display());
                println!("(Source config env vars are ignored during replay)");
            };

            let flow_handle = Arc::new(flow_handle);
            let flow_name = flow_handle.flow_name().to_string();

            managed_tasks = flow_handle_hooks
                .iter()
                .map(|hook| hook(&flow_handle))
                .collect();

            #[cfg(feature = "warp-server")]
            let surface_metrics_collector = {
                let system_journal = flow_handle.system_journal();
                let surface_metrics_collector =
                    if config.server && !web_surfaces.is_empty() && system_journal.is_some() {
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
                let (surface_name, endpoints, wiring) = surface.into_parts();
                for endpoint in endpoints {
                    all_extra_endpoints.push(label_endpoint(&surface_name, endpoint));
                }
                if let Some(wiring) = wiring {
                    match wiring(WebSurfaceWiringContext {
                        pipeline_state: flow_handle.state_receiver(),
                    }) {
                        Ok(wired) => managed_tasks.extend(wired.tasks),
                        Err(err) => break 'run (Err(err), Some(flow_name.clone()), run_dir, false),
                    }
                }
                tracing::debug!(surface = %surface_name, "Web surface attached");
            }

            // Start server if --server flag present
            let server_handle = if config.server {
                let cors_mode = match config.cors_mode {
                    CorsModeArg::AllowAnyOrigin => CorsMode::AllowAnyOrigin,
                    CorsModeArg::AllowList => CorsMode::AllowList(config.cors_allow_origin.clone()),
                    CorsModeArg::SameOrigin => CorsMode::SameOrigin,
                };
                let mut server_config =
                    ServerConfig::new(config.server_host.clone(), config.server_port);
                server_config.cors = Some(CorsConfig { mode: cors_mode });

                let start_result = {
                    #[cfg(feature = "warp-server")]
                    {
                        Self::start_server(
                            &flow_handle,
                            server_config,
                            all_extra_endpoints,
                            surface_metrics_collector,
                        )
                        .await
                    }

                    #[cfg(not(feature = "warp-server"))]
                    {
                        Self::start_server(&flow_handle, server_config, all_extra_endpoints).await
                    }
                };

                match start_result {
                    Ok(server_handle) => server_handle,
                    Err(err) => {
                        break 'run (Err(err), Some(flow_name), run_dir, false);
                    }
                }
            } else {
                None
            };

            if config.server {
                if server_handle.is_none() {
                    match config.startup_mode {
                        StartupMode::Manual => {
                            break 'run (
                                Err(ApplicationError::FeatureNotEnabled(
                                    "warp-server".to_string(),
                                )),
                                Some(flow_name),
                                run_dir,
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
                                        run_dir,
                                        false,
                                    );
                                }
                            };
                            let result = handle
                                .run()
                                .await
                                .map_err(|e| ApplicationError::FlowExecutionFailed(e.to_string()));
                            if result.is_ok() && !presentation_enabled {
                                if let Some(run_dir_path) = run_dir.as_deref() {
                                    print_replay_hint(run_dir_path);
                                }
                            }
                            break 'run (result, Some(flow_name), run_dir, false);
                        }
                    }
                }

                // Server mode: lifecycle is controlled via HTTP (and optionally startup_mode).
                match config.startup_mode {
                    StartupMode::Auto => {
                        tracing::info!("▶️  Starting flow execution (startup_mode=auto)");
                        if let Err(err) = flow_handle
                            .start()
                            .await
                            .map_err(|e| ApplicationError::FlowExecutionFailed(e.to_string()))
                        {
                            break 'run (Err(err), Some(flow_name), run_dir, false);
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
                        config.server_host,
                        config.server_port
                    );
                    tracing::info!("⏸️  Press Ctrl+C to cancel; send SIGTERM to graceful-stop...");

                    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
                    enum ShutdownSignal {
                        Sigint,
                        Sigterm,
                    }

                    #[cfg(unix)]
                    let mut sigterm_stream = match tokio::signal::unix::signal(
                        tokio::signal::unix::SignalKind::terminate(),
                    ) {
                        Ok(stream) => stream,
                        Err(err) => {
                            break 'run (
                                Err(ApplicationError::from(err)),
                                Some(flow_name),
                                run_dir,
                                false,
                            );
                        }
                    };

                    // Wait for first shutdown signal:
                    // - SIGINT (Ctrl+C) => Cancel
                    // - SIGTERM => GracefulStop(timeout=GRACE)
                    let first_signal = {
                        #[cfg(unix)]
                        {
                            tokio::select! {
                                _ = tokio::signal::ctrl_c() => ShutdownSignal::Sigint,
                                _ = sigterm_stream.recv() => ShutdownSignal::Sigterm,
                            }
                        }
                        #[cfg(not(unix))]
                        {
                            if let Err(err) = tokio::signal::ctrl_c().await {
                                break 'run (
                                    Err(ApplicationError::from(err)),
                                    Some(flow_name),
                                    run_dir,
                                    false,
                                );
                            }
                            ShutdownSignal::Sigint
                        }
                    };

                    tracing::info!(?first_signal, "👋 Shutting down server");

                    // The web server currently has no explicit shutdown hook; abort its task so we don't
                    // keep servicing requests while the pipeline is draining.
                    server_task.abort();
                    let _ = server_task.await;

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
                                shutdown_timeout_secs,
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

                break 'run (Ok(()), Some(flow_name), run_dir, true);
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
                        run_dir,
                        false,
                    );
                }
            };
            let result = handle
                .run()
                .await
                .map_err(|e| ApplicationError::FlowExecutionFailed(e.to_string()));
            if result.is_ok() && !presentation_enabled {
                if let Some(run_dir_path) = run_dir.as_deref() {
                    print_replay_hint(run_dir_path);
                }
            }
            break 'run (result, Some(flow_name), run_dir, false);
        };

        #[cfg(feature = "warp-server")]
        if let Some(emitter) = &surface_metrics_emitter {
            let _ = tokio::time::timeout(grace_timeout, emitter.flush()).await;
        }

        // Best-effort: ensure any hook/surface background tasks cannot escape `FlowApplication`
        // lifetime, even if we exited early due to a startup failure or "no server" fallback.
        Self::cancel_and_join_tasks(managed_tasks, grace_timeout).await;

        match (result, flow_name, run_dir, stopped) {
            (Ok(()), flow_name, run_dir, stopped) => {
                if let Some(presentation) = &presentation {
                    let flow_name = flow_name.unwrap_or_else(|| "Flow".to_string());
                    let outcome = if stopped {
                        RunPresentationOutcome::Stopped { flow_name, run_dir }
                    } else {
                        RunPresentationOutcome::Completed { flow_name, run_dir }
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
                Ok(())
            }
            (Err(err), flow_name, run_dir, _) => {
                if let Some(presentation) = &presentation {
                    let rendered_footer_banner = presentation.render_footer_banner();
                    let footer = presentation.render_footer(RunPresentationOutcome::Failed {
                        flow_name,
                        error: err.to_string(),
                        run_dir,
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

    /// Internal: Start the web server with all endpoints
    #[cfg(feature = "warp-server")]
    async fn start_server(
        _flow_handle: &Arc<FlowHandle>,
        _server_config: ServerConfig,
        extra_endpoints: Vec<Box<dyn HttpEndpoint>>,
        surface_metrics: Option<Arc<HttpSurfaceMetricsCollector>>,
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

        let flow_name = _flow_handle.flow_name().to_string();
        let middleware_stacks = _flow_handle.middleware_stacks();
        let contract_attachments = _flow_handle.contract_attachments();
        let join_metadata = _flow_handle.join_metadata();
        let subgraph_membership = _flow_handle.subgraph_membership();
        let subgraphs = _flow_handle.subgraphs();

        let handle = start_web_server_with_config(
            WebServerResources {
                topology,
                flow_name,
                middleware_stacks,
                contract_attachments,
                join_metadata,
                subgraph_membership,
                subgraphs,
                metrics_exporter: metrics,
                flow_handle: Some(_flow_handle.clone()),
                extra_endpoints,
                surface_metrics,
            },
            _server_config,
        )
        .await
        .map_err(|e| ApplicationError::ServerStartFailed(e.to_string()))?;

        tracing::info!("📊 Web server started on http://{}", addr);
        tracing::info!("   /api/topology  - Flow structure");
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
