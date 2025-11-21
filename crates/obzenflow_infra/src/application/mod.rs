//! Application framework for ObzenFlow
//! 
//! Provides Spring Boot-style lifecycle management for flows with automatic
//! CLI parsing, server management, and graceful shutdown handling.

mod config;
mod error;
mod flow_application;

pub use config::FlowConfig;
pub use error::ApplicationError;
pub use flow_application::{FlowApplication, FlowApplicationBuilder, LogLevel};