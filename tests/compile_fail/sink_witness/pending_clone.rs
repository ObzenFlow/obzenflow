// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_runtime::stages::sink::PendingSinkInput;

fn clone_pending(pending: PendingSinkInput) {
    let _ = pending.clone();
}

fn main() {}
