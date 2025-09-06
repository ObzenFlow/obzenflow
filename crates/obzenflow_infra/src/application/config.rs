use clap::Parser;

/// Configuration for FlowApplication
/// 
/// This struct is automatically populated from CLI arguments when
/// FlowApplication::run() is called.
#[derive(Parser, Debug, Clone)]
#[command(about = "ObzenFlow Application")]
pub struct FlowConfig {
    /// Start HTTP server for metrics and topology visualization
    #[arg(long)]
    pub server: bool,
    
    /// Port for HTTP server
    #[arg(long, default_value = "9090")]
    pub server_port: u16,
    
    // Future fields will be added here:
    // - debug flag
    // - journal overrides  
    // - checkpoint intervals
    // - distributed mode settings
}