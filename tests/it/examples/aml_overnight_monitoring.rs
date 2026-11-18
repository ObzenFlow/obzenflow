// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[path = "../../../examples/aml_overnight_monitoring/support.rs"]
pub mod support;

use std::path::{Path, PathBuf};

fn example_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/aml_overnight_monitoring")
}

fn shipped_table(file: &str) -> toml::Table {
    std::fs::read_to_string(example_dir().join(file))
        .expect("shipped configuration is readable")
        .parse()
        .expect("shipped configuration parses")
}

#[test]
fn each_variant_configuration_differs_from_the_default_in_one_setting() {
    let default = shipped_table("obzenflow.toml");
    for (file, key, value) in [
        (
            "obzenflow.bad-night.toml",
            "transactions",
            "examples/aml_overnight_monitoring/fixtures/bad-night-transactions.csv",
        ),
        (
            "obzenflow.unreadable-replies.toml",
            "case_system",
            "unreadable_replies",
        ),
    ] {
        let mut variant = shipped_table(file);
        let app = variant
            .get_mut("app")
            .and_then(toml::Value::as_table_mut)
            .expect("[app] table");
        assert_eq!(
            app.insert(key.to_owned(), default["app"][key].clone()),
            Some(toml::Value::String(value.to_owned())),
            "{file}"
        );
        assert_eq!(variant, default, "{file} differs only in app.{key}");
    }
}
