// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Systems and the ports they serve. A system is any type that implements a port.
//! A flow chooses the system behind each port with [`Port::to`], [`Port::deferred`]
//! or [`SystemConfig::port`]. The framework's own systems are in [`file`] and
//! [`http`].

pub use obzenflow_runtime::systems::{Port, SystemConfig};

/// The file system, which reads CSV, TSV and YAML files. A
/// [`CsvFileSystem`](file::CsvFileSystem) or a
/// [`YamlFileSystem`](file::YamlFileSystem) configures it for one file, and its
/// `port` gives the port that a source reads through.
pub mod file {
    pub use obzenflow_infra::systems::file::{CsvFileSystem, FileSystem, YamlFileSystem};
}

/// The HTTP API system, which reads JSON from an HTTP API. `HttpApi::json`
/// configures a base URL.
pub mod http {
    pub use obzenflow_infra::systems::http::{HttpApi, JsonEndpoint};
}
