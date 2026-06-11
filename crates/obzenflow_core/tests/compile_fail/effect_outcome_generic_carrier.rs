// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::{EffectOutcomeFacts, TypedPayload};

#[derive(Debug, Clone, EffectOutcomeFacts)]
pub enum GenericCarrier<T: TypedPayload> {
    Present(T),
}

fn main() {}
