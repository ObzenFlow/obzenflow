// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Core web server abstractions
//!
//! This module defines the abstract traits for web servers and HTTP endpoints,
//! following the same pattern as the journal abstractions.
//! Concrete implementations are provided in obzenflow_infra.

pub mod auth;
pub mod endpoint;
pub mod error;
pub mod managed;
pub mod server;
pub mod surface;
pub mod types;

// Re-export core types
pub use auth::AuthPolicy;
pub use endpoint::{EndpointMetadata, HttpEndpoint, ManagedRouteInfo};
pub use error::WebError;
pub use managed::{ManagedResponse, SseBody, SseFrame};
pub use server::WebServer;
pub use surface::{Route, RouteHandler, RouteKind, RoutePolicy, SurfacePolicy, WebSurface};
pub use types::{CorsConfig, CorsMode, HttpMethod, Request, Response, ServerConfig};
