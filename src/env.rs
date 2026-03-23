// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Environment parsing facade for ObzenFlow.
//!
//! This module re-exports the typed env helpers from [`obzenflow_infra::env`]
//! so applications can depend on `obzenflow` alone.

pub use obzenflow_infra::env::{
    env_bool, env_bool_or, env_var, env_var_or, env_var_required, EnvParseError,
};
