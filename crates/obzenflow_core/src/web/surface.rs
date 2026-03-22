// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Managed web-surface contract types (routing, policy declarations).
//!
//! Hosting and enforcement are infrastructure concerns. This module only defines
//! the public contract and route model used by ObzenFlow-owned surfaces.

use crate::web::auth::AuthPolicy;
use crate::web::endpoint::{HttpEndpoint, ManagedRouteInfo};
use crate::web::error::WebError;
use crate::web::managed::ManagedResponse;
use crate::web::types::{HttpMethod, Request};
use async_trait::async_trait;
use std::future::Future;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteKind {
    Unary,
    Sse,
}

#[derive(Debug, Clone, Default)]
pub struct SurfacePolicy {
    pub auth: Option<AuthPolicy>,
    pub max_body_size: Option<usize>,
    pub request_timeout_secs: Option<u64>,
}

#[derive(Debug, Clone, Default)]
pub struct RoutePolicy {
    pub auth: Option<AuthPolicy>,
    pub request_content_type: Option<String>,
    pub response_content_type: Option<String>,
}

#[async_trait]
pub trait RouteHandler: Send + Sync {
    async fn handle(&self, request: Request) -> Result<ManagedResponse, WebError>;
}

#[async_trait]
impl<F, Fut> RouteHandler for F
where
    F: Fn(Request) -> Fut + Send + Sync,
    Fut: Future<Output = Result<ManagedResponse, WebError>> + Send,
{
    async fn handle(&self, request: Request) -> Result<ManagedResponse, WebError> {
        (self)(request).await
    }
}

pub struct Route {
    methods: Vec<HttpMethod>,
    path: String,
    kind: RouteKind,
    handler: Arc<dyn RouteHandler>,
    policy: RoutePolicy,
}

impl Route {
    pub fn get(path: impl Into<String>) -> RouteBuilder {
        RouteBuilder::new(vec![HttpMethod::Get], path.into())
    }

    pub fn post(path: impl Into<String>) -> RouteBuilder {
        RouteBuilder::new(vec![HttpMethod::Post], path.into())
    }

    pub fn put(path: impl Into<String>) -> RouteBuilder {
        RouteBuilder::new(vec![HttpMethod::Put], path.into())
    }

    pub fn patch(path: impl Into<String>) -> RouteBuilder {
        RouteBuilder::new(vec![HttpMethod::Patch], path.into())
    }

    pub fn delete(path: impl Into<String>) -> RouteBuilder {
        RouteBuilder::new(vec![HttpMethod::Delete], path.into())
    }

    pub fn methods(&self) -> &[HttpMethod] {
        &self.methods
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn kind(&self) -> RouteKind {
        self.kind
    }

    pub fn policy(&self) -> &RoutePolicy {
        &self.policy
    }

    fn into_endpoint(
        self,
        full_path: String,
        surface_policy: SurfacePolicy,
    ) -> Box<dyn HttpEndpoint> {
        Box::new(ManagedRouteEndpoint {
            methods: self.methods,
            path: full_path,
            kind: self.kind,
            surface_policy,
            route_policy: self.policy,
            handler: self.handler,
        })
    }
}

pub struct RouteBuilder {
    methods: Vec<HttpMethod>,
    path: String,
    kind: RouteKind,
    policy: RoutePolicy,
}

impl RouteBuilder {
    fn new(methods: Vec<HttpMethod>, path: String) -> Self {
        Self {
            methods,
            path,
            kind: RouteKind::Unary,
            policy: RoutePolicy::default(),
        }
    }

    pub fn sse(mut self) -> Self {
        self.kind = RouteKind::Sse;
        self
    }

    pub fn auth(mut self, policy: AuthPolicy) -> Self {
        self.policy.auth = Some(policy);
        self
    }

    pub fn expect_request_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.policy.request_content_type = Some(content_type.into());
        self
    }

    pub fn expect_response_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.policy.response_content_type = Some(content_type.into());
        self
    }

    pub fn handler<H>(self, handler: H) -> Route
    where
        H: RouteHandler + 'static,
    {
        Route {
            methods: self.methods,
            path: self.path,
            kind: self.kind,
            handler: Arc::new(handler),
            policy: self.policy,
        }
    }
}

pub struct WebSurface {
    name: String,
    base_path: String,
    routes: Vec<Route>,
    policy: SurfacePolicy,
}

impl WebSurface {
    pub fn resource(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            base_path: "/".to_string(),
            routes: Vec::new(),
            policy: SurfacePolicy::default(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn base_path(mut self, base_path: impl Into<String>) -> Self {
        self.base_path = normalize_base_path(&base_path.into());
        self
    }

    pub fn policy(mut self, policy: SurfacePolicy) -> Self {
        self.policy = policy;
        self
    }

    pub fn route(mut self, route: Route) -> Self {
        self.routes.push(route);
        self
    }

    pub fn routes(&self) -> &[Route] {
        &self.routes
    }

    pub fn policy_ref(&self) -> &SurfacePolicy {
        &self.policy
    }

    pub fn into_endpoints(self) -> (String, Vec<Box<dyn HttpEndpoint>>) {
        let base = self.base_path;
        let surface_policy = self.policy;
        let endpoints = self
            .routes
            .into_iter()
            .map(|route| {
                let full_path = join_path(&base, route.path());
                route.into_endpoint(full_path, surface_policy.clone())
            })
            .collect();
        (self.name, endpoints)
    }
}

fn normalize_base_path(base_path: &str) -> String {
    let trimmed = base_path.trim();
    if trimmed.is_empty() {
        return "/".to_string();
    }

    let mut out = if trimmed.starts_with('/') {
        trimmed.to_string()
    } else {
        format!("/{trimmed}")
    };

    while out.len() > 1 && out.ends_with('/') {
        out.pop();
    }

    out
}

fn join_path(base_path: &str, suffix: &str) -> String {
    let base = normalize_base_path(base_path);
    let suffix = suffix.trim();
    if suffix.is_empty() || suffix == "/" {
        return base;
    }

    if suffix.starts_with('/') {
        format!("{base}{suffix}")
    } else {
        format!("{base}/{suffix}")
    }
}

struct ManagedRouteEndpoint {
    methods: Vec<HttpMethod>,
    path: String,
    kind: RouteKind,
    surface_policy: SurfacePolicy,
    route_policy: RoutePolicy,
    handler: Arc<dyn RouteHandler>,
}

#[async_trait]
impl HttpEndpoint for ManagedRouteEndpoint {
    fn path(&self) -> &str {
        &self.path
    }

    fn methods(&self) -> &[HttpMethod] {
        &self.methods
    }

    async fn handle(&self, request: Request) -> Result<ManagedResponse, WebError> {
        // The declared kind is a contract signal; the handler is still responsible for returning
        // the appropriate ManagedResponse variant.
        self.handler.handle(request).await
    }

    fn managed_route(&self) -> Option<ManagedRouteInfo> {
        Some(ManagedRouteInfo {
            kind: self.kind,
            surface_policy: Some(self.surface_policy.clone()),
            route_policy: self.route_policy.clone(),
        })
    }
}
