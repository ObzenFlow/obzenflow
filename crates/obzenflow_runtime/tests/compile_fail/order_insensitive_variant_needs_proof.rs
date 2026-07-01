// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

// FLOWIP-095l Gap 12: the realistic hand-forge is to build the `OrderInsensitive` variant
// directly, which needs a `OrderInsensitiveProof`. Its field is private, so the witness
// cannot be constructed outside obzenflow_runtime, and the variant cannot be forged.

use obzenflow_runtime::stages::common::handlers::{InputOrderSemantics, OrderInsensitiveProof};

fn main() {
    let _ = InputOrderSemantics::OrderInsensitive(OrderInsensitiveProof(()));
}
