// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Systems and the ports they serve. A flow chooses the system behind each port
//! with [`Port::to`], [`Port::deferred`] or [`SystemConfig::port`]. The
//! framework's own systems are in [`file`] and [`http`].

pub use obzenflow_runtime::systems::{
    BindingEvidenceError, ConfigurationEvidence, DelimitedHeaders, HttpEndpoint, Port, System,
    SystemBinding, SystemConfig,
};

/// The `file-system` system, which reads CSV, TSV and YAML files. A
/// [`CsvFileSystem`](file::CsvFileSystem) or a
/// [`YamlFileSystem`](file::YamlFileSystem) configures it for one file, and its
/// `port` binds the file to a source.
pub mod file {
    pub use obzenflow_infra::systems::file::{CsvFileSystem, FileSystem, YamlFileSystem};
}

/// The `http-api` system, which reads JSON from an HTTP API. `HttpApi::json`
/// binds a base URL.
pub mod http {
    pub use obzenflow_infra::systems::http::{HttpApi, JsonEndpoint};
}
