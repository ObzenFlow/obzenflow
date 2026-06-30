// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

// FLOWIP-095l Gap 12: the witness minter is `pub(crate)`, so an external crate cannot
// call it to forge a `TraceInvariant` barrier proof without the trial.

use obzenflow_runtime::stages::common::handlers::TraceInvarianceProof;

fn main() {
    let _ = TraceInvarianceProof::__minted_by_trace_invariant_attribute();
}
