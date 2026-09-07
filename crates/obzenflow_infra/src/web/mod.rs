// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Managed hosting and portable endpoint implementations.

#[cfg(feature = "warp-server")]
pub(crate) mod warp;

#[cfg(feature = "warp-server")]
pub(crate) mod host_config;
#[cfg(feature = "warp-server")]
pub(crate) mod host_error;
#[cfg(feature = "warp-server")]
pub(crate) mod managed_host;

pub(crate) mod endpoint_tags;
pub mod endpoints;
#[cfg(feature = "warp-server")]
pub(crate) mod routing;
pub mod runtime_instance_id;
#[cfg(feature = "warp-server")]
pub(crate) mod surface_metrics;

#[cfg(feature = "warp-server")]
pub(crate) mod web_server;

/// FLOWIP-114d: Studio phonebook registration heartbeat.
#[cfg(feature = "studio-registration")]
pub(crate) mod studio_presence;
#[cfg(feature = "studio-registration")]
pub(crate) mod studio_registration;

pub use runtime_instance_id::RuntimeInstanceId;
