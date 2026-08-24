// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Hosted HTTP ingress source facade.
//!
//! The protocol-neutral queue, typed decode, sealed ingress-context handoff,
//! and cleanup behavior are runtime-owned (FLOWIP-134g). HTTP admission stays
//! in `obzenflow_infra`.

pub use obzenflow_runtime::stages::{HostedIngressSource, IngressDecodeError, IngressDecoder};

#[derive(Debug, Clone)]
pub struct HttpSourceConfig {
    pub max_batch_size: usize,
}

impl Default for HttpSourceConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 1000,
        }
    }
}
