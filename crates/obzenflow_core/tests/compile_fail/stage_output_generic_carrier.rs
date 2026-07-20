// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

// Stage output carriers are concrete per-stage declarations; a generic
// carrier has no fixed leaf-member set (FLOWIP-120z).

use obzenflow_core::StageOutputFacts;

#[derive(Debug, Clone, StageOutputFacts)]
enum GenericOutcome<T> {
    Present(T),
}

fn main() {}
