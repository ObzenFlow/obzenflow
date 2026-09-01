// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::{error, Result};
use std::path::Path;
use url::Url;

pub(super) const DEVELOPMENT_COMPOSE_FILE: &str = "dev/postgres/compose.yml";
pub(super) const ACCEPTANCE_COMPOSE_FILE: &str = "xtask/postgres/acceptance-compose.yml";
pub(super) const SESSION_ROOT: &str = "target/postgres-sessions";
pub(super) const DEVELOPMENT_STATE_ROOT: &str = ".obzenflow/postgres";
pub(super) const DEVELOPMENT_SESSION: &str = "development";
pub(super) const SESSION_OVERRIDE_ENV: &str = "OBZENFLOW_POSTGRES_XTASK_PROOF_SESSION";
pub(super) const STATE_FILE: &str = "state.tsv";
pub(super) const RAW_PASSWORD_FILE: &str = "password";
pub(super) const PGPASS_FILE: &str = "pgpass";
pub(super) const LOG_FILE: &str = "postgres.log";
pub(super) const IMAGE: &str = "postgres:17";
pub(super) const POSTGRES_USER: &str = "obzenflow";
pub(super) const POSTGRES_DATABASE: &str = "obzenflow";
pub(super) const DEVELOPMENT_PAYMENT_SCHEMA: &str = "obzenflow_example";
pub(super) const PAYMENT_TEST_SCHEMA_ENV: &str = "OBZENFLOW_POSTGRES_EXAMPLE_SCHEMA";
pub(super) const INVENTORY_TEST_SCHEMA_ENV: &str = "OBZENFLOW_POSTGRES_INVENTORY_SCHEMA";
pub(super) const HN_DIGEST_TEST_SCHEMA_ENV: &str = "OBZENFLOW_POSTGRES_HN_DIGEST_SCHEMA";
pub(super) const POSTGRES_SCHEMA_ENV: &str = "OBZENFLOW_POSTGRES_SCHEMA";
pub(super) const POSTGRES_TRANSPORT_ENV: &str = "OBZENFLOW_POSTGRES_TRANSPORT";
pub(super) const VERIFIED_TLS_TRANSPORT: &str = "verified-tls";
pub(super) const LOOPBACK_TRANSPORT: &str = "externally-protected-plaintext";

pub(super) const POSTGRES_CLIENT_HOST: &str = "localhost";
pub(super) const POSTGRES_BIND_ADDRESS: &str = "127.0.0.1";

pub(super) fn payment_test_schema(run_id: &str) -> String {
    format!("obz083c_example_{run_id}")
}

pub(super) fn inventory_test_schema(run_id: &str) -> String {
    format!("obz083c_inventory_{run_id}")
}

pub(super) fn hn_digest_test_schema(run_id: &str) -> String {
    format!("obz010o_hn_digest_{run_id}")
}

pub(super) fn plaintext_url(port: u16) -> String {
    format!("postgresql://{POSTGRES_USER}@localhost:{port}/{POSTGRES_DATABASE}?sslmode=disable")
}

pub(super) fn tls_url(port: u16, host: &str) -> String {
    format!("postgresql://{POSTGRES_USER}@{host}:{port}/{POSTGRES_DATABASE}?sslmode=verify-full")
}

pub(super) fn verified_tls_url(port: u16, ca_certificate: &Path) -> Result<String> {
    let mut url = Url::parse(&tls_url(port, "localhost"))
        .map_err(|_| error("failed to construct the PostgreSQL TLS URL"))?;
    let ca_certificate = ca_certificate
        .to_str()
        .ok_or_else(|| error("PostgreSQL CA path is not valid Unicode"))?;
    url.query_pairs_mut()
        .append_pair("sslrootcert", ca_certificate);
    Ok(url.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, path::PathBuf};

    #[test]
    fn managed_urls_have_a_user_but_no_password() {
        for value in [
            plaintext_url(15432),
            tls_url(15432, "localhost"),
            verified_tls_url(15432, Path::new("/tmp/ca.crt")).expect("verified URL"),
        ] {
            let url = Url::parse(&value).expect("parse managed URL");
            assert_eq!(url.username(), POSTGRES_USER);
            assert_eq!(url.password(), None);
        }
    }

    #[test]
    fn compose_profiles_separate_plaintext_development_from_tls_acceptance() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("xtask is a workspace member")
            .to_path_buf();
        let development = fs::read_to_string(root.join(DEVELOPMENT_COMPOSE_FILE))
            .expect("read development Compose file");
        assert!(development.contains("POSTGRES_PASSWORD_FILE:"));
        assert!(!development.contains("POSTGRES_PASSWORD:"));
        for forbidden in [
            "OBZENFLOW_POSTGRES_TLS_DIR",
            "server.crt",
            "server.key",
            "ssl=on",
            "entrypoint:",
            "CMD-SHELL",
        ] {
            assert!(
                !development.contains(forbidden),
                "development Compose contains TLS or shell concern {forbidden}"
            );
        }

        let acceptance = fs::read_to_string(root.join(ACCEPTANCE_COMPOSE_FILE))
            .expect("read acceptance Compose file");
        assert!(acceptance.contains("POSTGRES_PASSWORD_FILE:"));
        assert!(!acceptance.contains("POSTGRES_PASSWORD:"));
        for required in [
            "OBZENFLOW_POSTGRES_TLS_DIR",
            "server.crt",
            "server.key",
            "ssl=on",
            "condition: service_completed_successfully",
        ] {
            assert!(
                acceptance.contains(required),
                "acceptance Compose omitted TLS concern {required}"
            );
        }
        assert!(!acceptance.contains("/bin/bash"));
        assert!(!acceptance.contains("CMD-SHELL"));
    }

    #[test]
    fn repository_does_not_reintroduce_the_removed_shared_password() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("xtask is a workspace member")
            .to_path_buf();
        let removed = ["obzenflow", "secret", "083c"].join("-");
        let mut pending = vec![root];
        while let Some(path) = pending.pop() {
            if path.is_dir() {
                let name = path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("");
                if matches!(name, ".git" | ".obzenflow" | "target") {
                    continue;
                }
                pending.extend(
                    fs::read_dir(path)
                        .expect("read repository directory")
                        .map(|entry| entry.expect("read repository entry").path()),
                );
            } else if let Ok(contents) = fs::read_to_string(&path) {
                assert!(
                    !contents.contains(&removed),
                    "removed shared PostgreSQL password appears in {}",
                    path.display()
                );
            }
        }
    }
}
