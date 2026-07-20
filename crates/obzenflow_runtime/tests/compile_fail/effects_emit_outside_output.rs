// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::TypedPayload;
use obzenflow_runtime::effects::Effects;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Declared;
impl TypedPayload for Declared {
    const EVENT_TYPE: &'static str = "compile_fail.effects.emit.declared";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Undeclared;
impl TypedPayload for Undeclared {
    const EVENT_TYPE: &'static str = "compile_fail.effects.emit.undeclared";
}

async fn author_outside_output(
    fx: &mut Effects<Declared, obzenflow_runtime::effect_set![]>,
) {
    fx.emit(Undeclared).await.unwrap();
}

fn main() {}
