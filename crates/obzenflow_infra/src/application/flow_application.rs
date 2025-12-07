//! Spring Boot-style application framework for ObzenFlow
//!
//! Provides automatic lifecycle management for flows including:
//! - CLI argument parsing
//! - Runtime creation and configuration
//! - Observability setup (tracing, console-subscriber)
//! - HTTP server management
//! - Graceful shutdown handling

use super::{ApplicationError, FlowConfig};
use crate::application::config::StartupMode;
use clap::Parser;
use obzenflow_runtime_services::prelude::FlowHandle;
use std::future::Future;
use std::sync::Arc;
use tokio::task::JoinHandle;

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

/// Builder for configuring and running a FlowApplication
///
/// # Example with console-subscriber
/// ```ignore
/// fn main() -> Result<(), Box<dyn std::error::Error>> {
///     FlowApplication::builder()
///         .with_console_subscriber()
///         .with_log_level(LogLevel::Info)
///         .run_blocking(async {
///             flow! {
///                 name: "my_flow",
///                 // ... flow definition
///             }
///         })?;
///     Ok(())
/// }
/// ```
#[derive(Default)]
pub struct FlowApplicationBuilder {
    console_subscriber: bool,
    console_bind: Option<String>,
    log_level: Option<LogLevel>,
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
    ///     .run_blocking(async { /* ... */ })
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

    /// Run the flow in a blocking context (without #[tokio::main])
    ///
    /// This builds the tokio runtime, initializes observability, and runs the flow.
    /// Use this when you have a plain `fn main()` and want FlowApplication to
    /// manage the entire runtime lifecycle.
    pub fn run_blocking<F, E>(self, flow_future: F) -> Result<(), ApplicationError>
    where
        F: Future<Output = Result<FlowHandle, E>> + Send + 'static,
        E: std::fmt::Display,
    {
        // Build tokio runtime so we have a handle for console-subscriber
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| ApplicationError::RuntimeCreationFailed(e.to_string()))?;

        // Initialize tracing/console-subscriber using the runtime handle
        self.init_observability(Some(runtime.handle()));

        // Run the flow in the runtime
        runtime.block_on(FlowApplication::run(flow_future))
    }

    /// Run the flow in an existing async context (with #[tokio::main])
    ///
    /// Use this when you already have a tokio runtime (e.g., from #[tokio::main])
    /// and just want FlowApplication to handle observability setup.
    pub async fn run_async<F, E>(self, flow_future: F) -> Result<(), ApplicationError>
    where
        F: Future<Output = Result<FlowHandle, E>>,
        E: std::fmt::Display,
    {
        // Initialize tracing/console-subscriber with the current runtime handle
        self.init_observability(Some(&tokio::runtime::Handle::current()));

        // Run the flow
        FlowApplication::run(flow_future).await
    }

    /// Initialize observability (tracing + console-subscriber)
    fn init_observability(&self, runtime_handle: Option<&tokio::runtime::Handle>) {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;
        use tracing_subscriber::EnvFilter;

        // Determine log level (RUST_LOG env var takes precedence)
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            let level = self.log_level.as_ref()
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
                    "❌ Invalid TOKIO_CONSOLE_BIND '{}': {}. Falling back to {}",
                    bind, err, fallback
                );
                fallback.parse().expect("fallback address should parse")
            });
            // Ensure downstream tooling that relies on the env var still sees the effective address
            std::env::set_var("TOKIO_CONSOLE_BIND", &bind);
            eprintln!("ℹ️  tokio-console attempting to bind to {}", addr);

            let builder = console_subscriber::ConsoleLayer::builder()
                .with_default_env()
                .server_addr(addr);
            let (console_layer, server) = builder.build();

            // Spawn console server with error logging so bind failures are visible instead of silent
            let bind_for_log = bind.clone();
            let spawn_server = async move {
                if let Err(err) = server.serve().await {
                    eprintln!("❌ tokio-console failed to bind on {}: {}", bind_for_log, err);
                }
            };
            // Small self-connect probe to surface connectivity issues early
            let addr_for_probe = addr;
            let spawn_probe = async move {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                match tokio::net::TcpStream::connect(addr_for_probe).await {
                    Ok(_) => eprintln!("✅ tokio-console TCP probe successful on {}", addr_for_probe),
                    Err(err) => eprintln!("❌ tokio-console TCP probe failed on {}: {}", addr_for_probe, err),
                }
            };

            if let Some(handle) = runtime_handle {
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

            eprintln!("🚦 tokio-console enabled on {}", bind);
            eprintln!("   Connect with: tokio-console http://{}", bind);
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
/// use obzenflow_dsl_infra::flow;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     FlowApplication::run(async {
///         flow! {
///             name: "my_flow",
///             // ... flow definition
///         }
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
///         .run_blocking(async {
///             flow! {
///                 name: "my_flow",
///                 // ... flow definition
///             }
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
    ///     .run_blocking(async { /* flow */ })
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
    /// * `flow_future` - A future that builds and returns a FlowHandle
    /// 
    /// # Returns
    /// * `Ok(())` if flow completes successfully
    /// * `Err(ApplicationError)` if flow fails or cannot start
    pub async fn run<F, E>(flow_future: F) -> Result<(), ApplicationError>
    where
        F: Future<Output = Result<FlowHandle, E>>,
        E: std::fmt::Display,
    {
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
        let flow_handle = flow_future
            .await
            .map_err(|e| ApplicationError::FlowBuildFailed(e.to_string()))?;
        let flow_handle = Arc::new(flow_handle);
        
        // Start server if --server flag present
        let server_handle = if config.server {
            Self::start_server(&flow_handle, config.server_port).await?
        } else {
            None
        };

        if config.server {
            // Server mode: lifecycle is controlled via HTTP (and optionally startup_mode).
            match config.startup_mode {
                StartupMode::Auto => {
                    tracing::info!("▶️  Starting flow execution (startup_mode=auto)");
                    flow_handle
                        .start()
                        .await
                        .map_err(|e| ApplicationError::FlowExecutionFailed(e.to_string()))?;
                }
                StartupMode::Manual => {
                    tracing::info!("⏸️  startup_mode=manual; waiting for Play via /api/flow/control");
                }
            }

            if let Some(_handle) = server_handle {
                tracing::info!("📊 Server running on port {}", config.server_port);
                tracing::info!("⏸️  Press Ctrl+C to stop server...");

                // Wait for Ctrl+C
                tokio::signal::ctrl_c().await?;
                tracing::info!("👋 Shutting down server");
            }

            Ok(())
        } else {
            // Non-server mode: preserve existing behavior (run to completion, no HTTP server)
            tracing::info!("▶️  Starting flow execution (no server)");
            let handle =
                Arc::try_unwrap(flow_handle).map_err(|_| {
                    ApplicationError::FlowExecutionFailed(
                        "Failed to unwrap FlowHandle for non-server execution".to_string(),
                    )
                })?;
            handle
                .run()
                .await
                .map_err(|e| ApplicationError::FlowExecutionFailed(e.to_string()))
        }
    }
    
            /// Internal: Start the web server with all endpoints
    async fn start_server(
        flow_handle: &Arc<FlowHandle>,
        port: u16
    ) -> Result<Option<JoinHandle<()>>, ApplicationError> {
        #[cfg(feature = "warp-server")]
        {
            use crate::web::start_web_server;
            
            // Every flow has a topology - it's required to run
            let topology = flow_handle.topology()
                .ok_or_else(|| ApplicationError::ServerStartFailed(
                    "Flow missing topology - this should never happen".to_string()
                ))?;
            let metrics = flow_handle.metrics_exporter();
            let has_metrics = metrics.is_some();

	            let flow_name = flow_handle.flow_name().to_string();
	            let middleware_stacks = flow_handle.middleware_stacks();
	            let contract_attachments = flow_handle.contract_attachments();
	
	            let handle = start_web_server(
	                topology,
	                flow_name,
	                middleware_stacks,
	                contract_attachments,
	                metrics,
	                Some(flow_handle.clone()),
	                port,
	            )
            .await
                .map_err(|e| ApplicationError::ServerStartFailed(e.to_string()))?;
            
            tracing::info!("📊 Web server started on http://localhost:{}", port);
            tracing::info!("   /api/topology  - Flow structure");
            if has_metrics {
                tracing::info!("   /metrics       - Prometheus metrics");
            }
            tracing::info!("   /health        - Health status");
            tracing::info!("   /ready         - Readiness status");
            
            Ok(Some(handle))
        }
        
        #[cfg(not(feature = "warp-server"))]
        {
            tracing::warn!("⚠️  --server flag requires warp-server feature");
            tracing::warn!("   Recompile with --features obzenflow_infra/warp-server");
            tracing::warn!("");
            tracing::warn!("   Continuing without HTTP server...");
            // Don't fail, just continue without server
            Ok(None)
        }
    }
}
