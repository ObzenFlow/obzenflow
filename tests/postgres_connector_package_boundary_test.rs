// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use serde_json::Value;
use std::process::Command;

#[test]
fn postgres_stays_in_the_feature_gated_adapter_boundary() {
    let output = Command::new(env!("CARGO"))
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("cargo metadata launches");
    assert!(
        output.status.success(),
        "cargo metadata failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let metadata: Value = serde_json::from_slice(&output.stdout).expect("metadata is JSON");
    let packages = metadata["packages"].as_array().expect("metadata packages");
    assert!(
        packages
            .iter()
            .all(|package| package["name"] != "obzenflow-connector-postgres"),
        "PostgreSQL must not introduce a connector API or implementation crate"
    );
    let adapters = packages
        .iter()
        .find(|package| package["name"] == "obzenflow_adapters")
        .expect("adapters package exists");
    let workspace_dependencies = adapters["dependencies"]
        .as_array()
        .expect("adapter dependencies")
        .iter()
        .filter(|dependency| dependency["kind"].is_null())
        .filter_map(|dependency| dependency["name"].as_str())
        .filter(|name| name.starts_with("obzenflow"))
        .collect::<Vec<_>>();
    assert_eq!(
        workspace_dependencies,
        ["obzenflow_core", "obzenflow_runtime"],
        "adapters must keep only core and runtime as production workspace dependencies"
    );
    let sqlx = adapters["dependencies"]
        .as_array()
        .expect("adapter dependencies")
        .iter()
        .find(|dependency| dependency["name"] == "sqlx")
        .expect("adapters declares SQLx");
    assert_eq!(sqlx["optional"], true, "SQLx must remain optional");
    assert_eq!(
        sqlx["uses_default_features"], false,
        "SQLx default features must remain disabled"
    );
    let sqlx_features = sqlx["features"]
        .as_array()
        .expect("SQLx dependency features")
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();
    assert!(
        sqlx_features.contains(&"runtime-tokio")
            && sqlx_features.contains(&"postgres")
            && sqlx_features.contains(&"tls-rustls-ring-native-roots"),
        "SQLx must use Tokio, PostgreSQL, and Rustls native roots"
    );
    assert!(
        sqlx_features
            .iter()
            .all(|feature| !feature.contains("webpki")),
        "the PostgreSQL feature path must not select embedded WebPKI roots"
    );
    let consumer_source = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/postgres_public_consumer_test.rs"),
    )
    .expect("read public PostgreSQL consumer fixture");
    assert!(
        !consumer_source.lines().any(|line| {
            let code = line.split("//").next().unwrap_or_default();
            code.contains("sqlx::") || code.contains("extern crate sqlx")
        }),
        "the public consumer fixture must not import SQLx"
    );

    for example in [
        "examples/postgres_sink_payments.rs",
        "examples/postgres_sink_inventory.rs",
    ] {
        let source =
            std::fs::read_to_string(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(example))
                .unwrap_or_else(|error| {
                    panic!("read shipped PostgreSQL example {example}: {error}")
                });
        let source = source.to_ascii_uppercase();
        for forbidden in [
            "CREATE SCHEMA",
            "CREATE TABLE",
            "CREATE INDEX",
            "CREATE FUNCTION",
            "CREATE TRIGGER",
            "ALTER TABLE",
            "DROP TABLE",
            "DROP FUNCTION",
            "DROP TRIGGER",
            "TRUNCATE TABLE",
        ] {
            assert!(
                !source.contains(forbidden),
                "shipped PostgreSQL example {example} must not own DDL `{forbidden}`"
            );
        }
    }

    let root = packages
        .iter()
        .find(|package| package["name"] == "obzenflow")
        .expect("root facade exists");
    assert!(
        root["features"]["default"]
            .as_array()
            .is_none_or(|features| {
                features
                    .iter()
                    .all(|feature| feature.as_str() != Some("postgres"))
            }),
        "the default root feature set must not activate PostgreSQL"
    );
    assert_eq!(
        root["features"]["postgres"],
        serde_json::json!(["obzenflow_adapters/postgres"]),
        "the root facade must forward its PostgreSQL feature to adapters"
    );
}
