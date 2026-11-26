// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Hosted and in-process ingress construction and submission.
//!
//! Submission contracts are available without features. Enable `web-host` to
//! serve HTTP ingress through [`super::FlowApplication`].
//!
//! [`super::FlowApplicationBuilder::hosted_ingress`] returns the
//! [`super::HostedIngress`] of the application. Give it to the factory of the flow. In
//! the factory, each call to [`super::HostedIngress::http`] registers an HTTP endpoint
//! and returns its event system. The line `subscribe(decoder).through(Port::to(events))`
//! reads that event system. `FlowApplication` attaches the routes of each endpoint after
//! the build.
//!
//! For an application-owned protocol, create an [`Ingress`] with [`ingress_source`].
//! Clone its handle with [`Ingress::handle`]. Then give the ingress to
//! [`super::FlowApplicationBuilder::ingress`], which returns its event system.
//!
//! A success answer follows the submission's committed row, and a stable
//! request ID turns a retry into the original row instead of another copy.

pub use obzenflow_core::ingress::{
    EdgeShedReason, IngressContext, IngressKey, IngressRefusalKind, IngressRefusalReason,
    IngressTiming,
};
pub use obzenflow_infra::web::endpoints::event_ingestion::{
    ingress_source, IngestionConfig, Ingress, IngressErrorBody, IngressErrorCode, IngressHandle,
    IngressOutcome, SubmissionMemberOutcome, SubmissionMemberResult, SubmissionResponse,
};
pub use obzenflow_infra::web::endpoints::event_ingestion::{
    AuthConfig, AuthError, SchemaValidator, TypedValidator, ValidationConfig, ValidationError,
};
