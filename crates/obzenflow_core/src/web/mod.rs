// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Portable HTTP endpoints, managed surfaces, requests and responses.
//! Listener construction and hosting policy belong to infrastructure.

pub mod auth;
pub mod endpoint;
pub mod error;
pub mod managed;
pub mod surface;
pub mod types;

// Re-export core types
pub use auth::AuthPolicy;
pub use endpoint::{EndpointMetadata, HttpEndpoint, ManagedRouteInfo};
pub use error::EndpointError;
pub use managed::{ManagedResponse, SseBody, SseFrame};
pub use surface::{Route, RouteHandler, RouteKind, RoutePolicy, SurfacePolicy, WebSurface};
pub use types::{HttpMethod, Request, Response};
