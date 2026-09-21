// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Hosted and in-process ingress construction and submission.
//!
//! Submission contracts are available without features. Enable `web-host` to
//! serve HTTP ingress through [`super::FlowApplication`].

pub use obzenflow_core::ingress::{
    BatchSubmission, EdgeShedReason, EventSubmission, IngressContext, IngressKey,
    IngressRefusalKind, IngressRefusalReason, SubmissionIngressContext, SubmissionPayloadKind,
    SubmissionResponse,
};
pub use obzenflow_infra::web::endpoints::event_ingestion::{
    http_ingress, ingress_source, HttpIngress, IngestionConfig, Ingress, IngressHandle,
    IngressSubmitError, IngressSubmitOutcome,
};
pub use obzenflow_infra::web::endpoints::event_ingestion::{
    AuthConfig, AuthError, SchemaValidator, TypedValidator, ValidationConfig, ValidationError,
};
