// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Application metrics aggregation now lives in the runtime layer.
//!
//! `MetricsAggregator` is implemented in `obzenflow_runtime`. The adapters layer
//! keeps exporter implementations, but it is no longer the home of the runtime
//! aggregation FSM itself.
