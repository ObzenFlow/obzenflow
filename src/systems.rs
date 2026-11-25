// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! A system is any type that implements a port. A flow chooses the system behind each
//! port with [`Port::to`], [`Port::deferred`] or [`SystemConfig::port`]. The
//! framework's own systems are in [`file`] and [`http`].

pub use obzenflow_runtime::systems::{Port, SystemConfig};

/// The `file` module holds the file system, which reads CSV, TSV and YAML files. A
/// [`CsvFileSystem`](file::CsvFileSystem) or a [`YamlFileSystem`](file::YamlFileSystem)
/// configures the file system for one file.
pub mod file {
    pub use obzenflow_infra::systems::file::{CsvFileSystem, FileSystem, YamlFileSystem};
}

/// The `http` module holds the HTTP API system, which reads JSON from an HTTP API.
/// `HttpApi::json` configures the system with a base URL.
pub mod http {
    pub use obzenflow_infra::systems::http::{HttpApi, JsonEndpoint};
}
