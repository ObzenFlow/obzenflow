// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Private coordination of FlowApplication resources and Runtime observations.

mod driver;
// Hosted observations remain in the closed vocabulary for non-hosted builds.
#[cfg_attr(not(feature = "warp-server"), allow(dead_code))]
mod machine;
#[cfg(feature = "warp-server")]
mod signals;

pub(super) use driver::{ApplicationLifecycle, ApplicationTask};
