// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-140h: prevent provider ownership from migrating into inner layers.
use std::path::Path;

fn check_inner_sources(dir: &Path) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            check_inner_sources(&path);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            let source = std::fs::read_to_string(&path).unwrap();
            for forbidden in [
                "MetricsExporter",
                "MetricsBootstrap",
                "DefaultMetricsConfig",
                "PrometheusProjection",
                "ConsoleProjection",
                "MetricsReporter",
                "render_metrics",
                "run_with_metrics",
                "edge_liveness_state_gauge_value",
                "stage_activity_gauge_value",
            ] {
                assert!(
                    !source.contains(forbidden),
                    "{} contains provider ownership: {forbidden}",
                    path.display()
                );
            }
        }
    }
}

#[test]
fn provider_selection_and_rendering_stay_outside_core_runtime_and_dsl() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for layer in ["obzenflow_core", "obzenflow_runtime", "obzenflow_dsl"] {
        check_inner_sources(&root.join("crates").join(layer).join("src"));
    }
    for removed in [
        "obzenflow_core/src/metrics/exporter.rs",
        "obzenflow_runtime/src/metrics/config.rs",
        "obzenflow_adapters/src/monitoring/exporters/builder.rs",
        "obzenflow_adapters/src/monitoring/exporters/prometheus_exporter.rs",
        "obzenflow_adapters/src/monitoring/exporters/console_summary.rs",
        "obzenflow_infra/src/monitoring_backend/prometheus.rs",
        "obzenflow_infra/src/monitoring_backend/console.rs",
        "obzenflow_adapters/src/monitoring/projections/console.rs",
        "obzenflow_infra/src/web/metrics_server.rs",
        "obzenflow_infra/src/metrics_reporting/mod.rs",
        "obzenflow_adapters/src/monitoring/aggregator/mod.rs",
        "obzenflow_adapters/src/monitoring/metrics/mod.rs",
        "obzenflow_adapters/src/monitoring/exporters/mod.rs",
    ] {
        assert!(
            !root.join("crates").join(removed).exists(),
            "removed provider facade returned: {removed}"
        );
    }
    let config =
        std::fs::read_to_string(root.join("crates/obzenflow_infra/src/application/config.rs"))
            .unwrap();
    assert!(!config.contains("enum MetricsReporter"));
    assert!(!config.contains("fn parse_metrics_exporter"));
}
