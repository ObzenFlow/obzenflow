// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::path::Path;
use std::process::Command;

#[test]
fn core_web_stays_portable_and_managed_host_stays_private() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let core = root.join("crates/obzenflow_core/src/web");
    assert!(!core.join("server.rs").exists());
    assert!(!root
        .join("crates/obzenflow_infra/src/web/factory.rs")
        .exists());
    let retired = [
        "WebServer",
        "WebServerBuilder",
        "ServerShutdownHandle",
        "WebError",
        "ServerConfig",
        "CorsConfig",
        "CorsMode",
        "TlsConfig",
        "ClientAuth",
    ];
    for entry in core.read_dir().unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|extension| extension == "rs") {
            let source = std::fs::read_to_string(&path).unwrap();
            let tokens: Vec<_> = source
                .split(|ch: char| !(ch.is_alphanumeric() || ch == '_'))
                .collect();
            for name in retired {
                assert!(
                    !tokens.contains(&name),
                    "retired host API {name} returned to {}",
                    path.display()
                );
            }
        }
    }
    let web = std::fs::read_to_string(root.join("crates/obzenflow_infra/src/web/mod.rs")).unwrap();
    assert!(!web.contains("pub mod warp;"));
    for name in [
        "WarpServer",
        "WarpWebHost",
        "WebServerFactory",
        "start_web_server",
        "start_web_server_with_config",
        "ManagedWebHost",
    ] {
        assert!(
            !web.contains(name),
            "private host implementation must not be exported: {name}"
        );
    }
    let host = std::fs::read_to_string(root.join("crates/obzenflow_infra/src/web/managed_host.rs"))
        .unwrap();
    assert!(
        !host.contains("warp::serve("),
        "the managed host must own connection serving"
    );
    assert!(
        host.find("TcpListener::bind(address)").unwrap() < host.find("tokio::spawn(").unwrap(),
        "the fallible socket bind must precede the serving task"
    );
    let adapter =
        std::fs::read_to_string(root.join("crates/obzenflow_infra/src/web/warp/warp_server.rs"))
            .unwrap();
    assert!(
        !adapter.contains("bind_with_graceful_shutdown"),
        "panicking Warp bind APIs must not return"
    );
    let metadata = Command::new(env!("CARGO"))
        .args([
            "metadata",
            "--format-version",
            "1",
            "--no-deps",
            "--locked",
            "--offline",
        ])
        .current_dir(root)
        .output()
        .unwrap();
    assert!(
        metadata.status.success(),
        "{}",
        String::from_utf8_lossy(&metadata.stderr)
    );
    let metadata: serde_json::Value = serde_json::from_slice(&metadata.stdout).unwrap();
    let packages = metadata["packages"].as_array().unwrap();
    let core = packages
        .iter()
        .find(|p| p["name"] == "obzenflow_core")
        .unwrap();
    for dependency in core["dependencies"].as_array().unwrap() {
        assert!(!matches!(
            dependency["name"].as_str(),
            Some("warp" | "hyper" | "hyper-util" | "tower-service")
        ));
    }
    let infra = packages
        .iter()
        .find(|p| p["name"] == "obzenflow_infra")
        .unwrap();
    for dependency in infra["dependencies"].as_array().unwrap() {
        if dependency["kind"].is_null()
            && matches!(
                dependency["name"].as_str(),
                Some("warp" | "hyper" | "hyper-util" | "tower-service")
            )
        {
            assert_eq!(
                dependency["optional"], true,
                "host transport dependencies must remain optional"
            );
        }
    }
}

#[test]
fn package_file_lists_exclude_the_retired_host_spi_and_factory() {
    for (package, retired) in [
        ("obzenflow_core", "src/web/server.rs"),
        ("obzenflow_infra", "src/web/factory.rs"),
    ] {
        let output = Command::new(env!("CARGO"))
            .args([
                "package",
                "-p",
                package,
                "--list",
                "--allow-dirty",
                "--locked",
                "--offline",
            ])
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "package list for {package}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let files = String::from_utf8(output.stdout).unwrap();
        assert!(
            !files.lines().any(|path| path == retired),
            "retired host code must not ship in {package}"
        );
        assert!(files.lines().any(|path| path == "src/web/mod.rs"));
    }
}
