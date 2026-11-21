// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::collect_rust_sources;
use serde_json::Value;
use std::path::Path;
use std::process::Command;
use syn::visit::Visit;

#[test]
fn postgres_client_confined_to_infra_feature() {
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
    for name in ["obzenflow_core", "obzenflow_runtime", "obzenflow_adapters"] {
        let package = packages
            .iter()
            .find(|package| package["name"] == name)
            .unwrap_or_else(|| panic!("{name} package exists"));
        assert!(
            package["dependencies"]
                .as_array()
                .expect("package dependencies")
                .iter()
                .all(|dependency| dependency["name"] != "sqlx"),
            "{name} must not declare SQLx; the PostgreSQL client lives in infra"
        );
    }
    let infra = packages
        .iter()
        .find(|package| package["name"] == "obzenflow_infra")
        .expect("infra package exists");
    assert_eq!(
        infra["features"]["postgres"],
        serde_json::json!(["dep:sqlx"]),
        "infra's PostgreSQL feature must enable SQLx alone"
    );
    let sqlx = infra["dependencies"]
        .as_array()
        .expect("infra dependencies")
        .iter()
        .find(|dependency| dependency["name"] == "sqlx" && dependency["kind"].is_null())
        .expect("infra declares SQLx");
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
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/postgres_public_consumer.rs"),
    )
    .expect("read public PostgreSQL consumer fixture");
    assert!(
        !consumer_source.lines().any(|line| {
            let code = line.split("//").next().unwrap_or_default();
            code.contains("sqlx::") || code.contains("extern crate sqlx")
        }),
        "the public consumer fixture must not import SQLx"
    );

    let fixture_files = [
        "tests/e2e_postgres/payments/application.rs",
        "tests/e2e_postgres/payments/domain.rs",
        "tests/e2e_postgres/payments/flow.rs",
    ];
    let mut fixture_source = String::new();
    for fixture in fixture_files {
        let source =
            std::fs::read_to_string(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(fixture))
                .unwrap_or_else(|error| {
                    panic!("read PostgreSQL application fixture {fixture}: {error}")
                });
        let upper_source = source.to_ascii_uppercase();
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
                !upper_source.contains(forbidden),
                "PostgreSQL application fixture {fixture} must not own DDL `{forbidden}`"
            );
        }
        fixture_source.push_str(&source);
    }
    for forbidden in ["test-support", "sqlx::", "--inspect", "inspect_destination"] {
        assert!(
            !fixture_source.contains(forbidden),
            "the PostgreSQL application fixture must not contain proof concern `{forbidden}`"
        );
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
        serde_json::json!(["obzenflow_infra/postgres"]),
        "the root facade must forward its PostgreSQL feature to infra"
    );
}

#[test]
fn inner_crates_exclude_postgres_client_types() {
    struct ClientNames<'a>(&'a Path);

    impl Visit<'_> for ClientNames<'_> {
        fn visit_item_mod(&mut self, module: &syn::ItemMod) {
            assert_ne!(
                module.ident,
                "postgres",
                "{} declares a PostgreSQL module",
                self.0.display()
            );
            syn::visit::visit_item_mod(self, module);
        }

        fn visit_ident(&mut self, ident: &syn::Ident) {
            let name = ident.to_string();
            assert!(
                !name.starts_with("Postgres") && name != "postgres_client",
                "{} names PostgreSQL integration type or module {name}",
                self.0.display()
            );
        }
    }

    for name in ["obzenflow_core", "obzenflow_runtime"] {
        let mut sources = Vec::new();
        collect_rust_sources(
            &Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("crates")
                .join(name)
                .join("src"),
            &mut sources,
        );
        for path in sources {
            let source = std::fs::read_to_string(&path).unwrap();
            if source.contains("Postgres") || source.contains("postgres") {
                ClientNames(&path).visit_file(&syn::parse_file(&source).unwrap());
            }
        }
    }
}
