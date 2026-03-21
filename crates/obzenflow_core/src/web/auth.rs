// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Framework-neutral authentication policy declarations for managed web surfaces.
//!
//! Enforcement lives in infrastructure. This module is only the contract.

use serde::{Deserialize, Serialize};

/// Authentication policy for ObzenFlow-managed HTTP surfaces.
///
/// This is intentionally narrow and oriented around private-network deployments
/// and webhook-style shared-secret patterns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuthPolicy {
    /// No authentication is required.
    None,
    /// Static API key in a request header.
    ApiKey {
        /// Header name to inspect (e.g. `X-Obzenflow-Api-Key`).
        header: String,
        /// Environment variable containing the expected secret value.
        value_env: String,
    },
    /// HMAC-SHA256 signature over request content for webhook-style integrity.
    HmacSha256 {
        /// Environment variable containing the shared secret.
        secret_env: String,
        /// Header containing the signature value.
        signature_header: String,
        /// A short label describing what is signed (e.g. `raw_body`).
        ///
        /// This is descriptive and primarily exists for operator clarity.
        body_hash: String,
        /// Optional header containing an event timestamp for replay protection.
        timestamp_header: Option<String>,
    },
}

