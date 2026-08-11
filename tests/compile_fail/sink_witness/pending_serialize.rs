// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_runtime::stages::sink::PendingSinkInput;

fn requires_serialize<T: serde::Serialize>() {}

fn main() {
    requires_serialize::<PendingSinkInput>();
}
