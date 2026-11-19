// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

/// A no-op destination for replay fixtures that only inspect journals.
pub type Discard<T> = obzenflow_adapters::sinks::DiscardSink<T>;
