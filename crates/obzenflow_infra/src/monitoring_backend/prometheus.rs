// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Prometheus exporter integration (re-export).
//!
//! `obzenflow_infra` doesn't maintain a separate exporter implementation; the adapters
//! layer provides the canonical metrics exporters used by the runtime.

pub use obzenflow_adapters::monitoring::exporters::PrometheusExporter;
