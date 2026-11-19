// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

/// Count physical calls. Replay makes none, so a replay counts nothing.
pub type CountDeliveries<T> = obzenflow_adapters::sinks::CountingSink<T>;
