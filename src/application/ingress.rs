// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Hosted and in-process ingress construction and submission.
//!
//! Submission contracts are available without features. Enable `web-host` to
//! serve HTTP ingress through [`super::FlowApplication`].
//!
//! Use [`super::FlowApplicationBuilder::http_ingress`] to register HTTP ingress
//! and receive its source for the flow. The builder owns hosting and lifecycle
//! wiring. For application-owned protocols, clone an [`Ingress::handle`] and
//! pass the bundle to [`super::FlowApplicationBuilder::ingress`].
//!
//! A success answer follows the submission's committed row, and a stable
//! request ID turns a retry into the original row instead of another copy.

pub use obzenflow_core::ingress::{
    BatchSubmission, EdgeShedReason, EventSubmission, IngressContext, IngressKey,
    IngressRefusalKind, IngressRefusalReason, IngressRequestId, IngressRequestIdentity,
    IngressTiming, InvalidIngressRequestId,
};
pub use obzenflow_infra::web::endpoints::event_ingestion::{
    http_ingress, ingress_source, HttpIngress, HttpIngressAttachment, IngestionConfig, Ingress,
    IngressErrorBody, IngressErrorCode, IngressHandle, IngressOutcome, SubmissionMemberOutcome,
    SubmissionMemberResult, SubmissionResponse,
};
pub use obzenflow_infra::web::endpoints::event_ingestion::{
    AuthConfig, AuthError, SchemaValidator, TypedValidator, ValidationConfig, ValidationError,
};
