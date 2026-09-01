// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{
    compose::ServiceEvidence,
    config::{
        hn_digest_test_schema, inventory_test_schema, payment_test_schema, plaintext_url, tls_url,
        verified_tls_url, DEVELOPMENT_PAYMENT_SCHEMA, HN_DIGEST_TEST_SCHEMA_ENV,
        INVENTORY_TEST_SCHEMA_ENV, LOOPBACK_TRANSPORT, PAYMENT_TEST_SCHEMA_ENV,
        POSTGRES_SCHEMA_ENV, POSTGRES_TRANSPORT_ENV, SESSION_OVERRIDE_ENV, VERIFIED_TLS_TRANSPORT,
    },
    credentials,
    state::SessionState,
};
use crate::{error, Result};
use std::{env, ffi::OsStr, path::Path, process::Command};

const INTERNAL_DEVELOPMENT_ENV: &[&str] = &[
    "OBZENFLOW_POSTGRES_CA_CERT",
    "OBZENFLOW_POSTGRES_PASSWORD",
    "OBZENFLOW_POSTGRES_PASSWORD_FILE",
    "OBZENFLOW_POSTGRES_TLS_DIR",
    "OBZENFLOW_POSTGRES_HOST_PORT",
    POSTGRES_TRANSPORT_ENV,
    PAYMENT_TEST_SCHEMA_ENV,
    INVENTORY_TEST_SCHEMA_ENV,
    HN_DIGEST_TEST_SCHEMA_ENV,
    SESSION_OVERRIDE_ENV,
];

const KNOWN_TEST_ENV: &[&str] = &[
    "OBZENFLOW_POSTGRES_TEST_URL",
    "OBZENFLOW_POSTGRES_TEST_RUN_ID",
    "OBZENFLOW_POSTGRES_TEST_PROJECT",
    "OBZENFLOW_POSTGRES_TEST_PORT",
    "OBZENFLOW_POSTGRES_TEST_CONTAINER_ID",
    "OBZENFLOW_POSTGRES_TEST_HEALTH",
    "OBZENFLOW_POSTGRES_TEST_TLS_DIR",
    "OBZENFLOW_POSTGRES_TEST_TLS_URL",
    "OBZENFLOW_POSTGRES_TEST_WRONG_HOST_URL",
    "OBZENFLOW_POSTGRES_TEST_CA_CERT",
    "OBZENFLOW_POSTGRES_TEST_UNTRUSTED_CA_CERT",
];

pub(super) fn configure_development(command: &mut Command, state: &SessionState) -> Result<String> {
    remove_development_only_inputs(command);
    command
        .env("OBZENFLOW_POSTGRES_URL", plaintext_url(state.port))
        .env(POSTGRES_TRANSPORT_ENV, LOOPBACK_TRANSPORT);
    let schema = inherited_schema()?;
    command.env(POSTGRES_SCHEMA_ENV, &schema);
    Ok(schema)
}

pub(super) fn configure_test(
    command: &mut Command,
    directory: &Path,
    state: &SessionState,
    service: &ServiceEvidence,
) -> Result<()> {
    let files = credentials::validate_acceptance(directory, state.port)?;
    let ca_certificate = directory.join("tls/ca.crt");
    command
        .env_remove("PGPASSWORD")
        .env_remove(SESSION_OVERRIDE_ENV)
        .env_remove("OBZENFLOW_POSTGRES_PASSWORD")
        .env_remove("OBZENFLOW_POSTGRES_PASSWORD_FILE")
        .env("PGPASSFILE", files.pgpass)
        .env(
            "OBZENFLOW_POSTGRES_URL",
            verified_tls_url(state.port, &ca_certificate)?,
        )
        .env(POSTGRES_TRANSPORT_ENV, VERIFIED_TLS_TRANSPORT)
        .env(POSTGRES_SCHEMA_ENV, DEVELOPMENT_PAYMENT_SCHEMA)
        .env("OBZENFLOW_POSTGRES_TEST_URL", plaintext_url(state.port))
        .env("OBZENFLOW_POSTGRES_TEST_RUN_ID", &state.run_id)
        .env("OBZENFLOW_POSTGRES_TEST_PROJECT", &state.project)
        .env("OBZENFLOW_POSTGRES_TEST_PORT", state.port.to_string())
        .env(
            "OBZENFLOW_POSTGRES_TEST_CONTAINER_ID",
            &service.container_id,
        )
        .env("OBZENFLOW_POSTGRES_TEST_HEALTH", &service.health)
        .env("OBZENFLOW_POSTGRES_TEST_TLS_DIR", directory.join("tls"))
        .env(PAYMENT_TEST_SCHEMA_ENV, payment_test_schema(&state.run_id))
        .env(
            HN_DIGEST_TEST_SCHEMA_ENV,
            hn_digest_test_schema(&state.run_id),
        )
        .env(
            INVENTORY_TEST_SCHEMA_ENV,
            inventory_test_schema(&state.run_id),
        )
        .env(
            "OBZENFLOW_POSTGRES_TEST_TLS_URL",
            tls_url(state.port, "localhost"),
        )
        .env(
            "OBZENFLOW_POSTGRES_TEST_WRONG_HOST_URL",
            tls_url(state.port, "127.0.0.1"),
        )
        .env("OBZENFLOW_POSTGRES_TEST_CA_CERT", &ca_certificate)
        .env(
            "OBZENFLOW_POSTGRES_TEST_UNTRUSTED_CA_CERT",
            directory.join("tls/untrusted-ca.crt"),
        );
    Ok(())
}

fn remove_development_only_inputs(command: &mut Command) {
    command.env_remove("PGPASSWORD");
    command.env_remove("PGPASSFILE");
    for name in INTERNAL_DEVELOPMENT_ENV.iter().chain(KNOWN_TEST_ENV.iter()) {
        command.env_remove(name);
    }
    for (name, _) in env::vars_os() {
        if test_environment_name(&name) {
            command.env_remove(name);
        }
    }
}

fn inherited_schema() -> Result<String> {
    match env::var(POSTGRES_SCHEMA_ENV) {
        Ok(value) => Ok(value),
        Err(env::VarError::NotPresent) => Ok(DEVELOPMENT_PAYMENT_SCHEMA.to_string()),
        Err(env::VarError::NotUnicode(_)) => {
            Err(error(format!("{POSTGRES_SCHEMA_ENV} is not valid Unicode")))
        }
    }
}

fn test_environment_name(name: &OsStr) -> bool {
    name.to_string_lossy()
        .starts_with("OBZENFLOW_POSTGRES_TEST_")
}

#[cfg(test)]
mod tests {
    use super::super::state::{self, SessionMode};
    use super::*;
    use std::{collections::BTreeMap, ffi::OsString, fs};

    #[test]
    fn development_child_is_password_free_and_clears_ambient_precedence() {
        let root = env::temp_dir().join(format!(
            "obzenflow-development-env-{}",
            state::unique_run_id()
        ));
        let directory = root.join("session");
        state::create_session_directory(&directory).expect("create session");
        let project = "obzenflow-postgres-environment".to_string();
        let state = SessionState {
            project: project.clone(),
            run_id: state::unique_run_id(),
            port: 15432,
            mode: SessionMode::Development,
            volume: Some(state::expected_volume(&project)),
        };
        let mut command = Command::new("env");
        command
            .env("PGPASSWORD", "ambient")
            .env("PGPASSFILE", "/ambient/pgpass")
            .env("OBZENFLOW_POSTGRES_TEST_URL", "ambient")
            .env(POSTGRES_TRANSPORT_ENV, "ambient")
            .env(SESSION_OVERRIDE_ENV, "ambient");
        configure_development(&mut command, &state).expect("configure child");
        let environment = command
            .get_envs()
            .map(|(name, value)| (name.to_owned(), value.map(OsString::from)))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(environment.get(OsStr::new("PGPASSWORD")), Some(&None));
        assert_eq!(environment.get(OsStr::new("PGPASSFILE")), Some(&None));
        assert_eq!(
            environment.get(OsStr::new("OBZENFLOW_POSTGRES_TEST_URL")),
            Some(&None)
        );
        assert_eq!(
            environment.get(OsStr::new(SESSION_OVERRIDE_ENV)),
            Some(&None)
        );
        let url = environment
            .get(OsStr::new("OBZENFLOW_POSTGRES_URL"))
            .and_then(Option::as_ref)
            .expect("managed URL")
            .to_string_lossy();
        assert!(url.starts_with("postgresql://obzenflow@localhost:15432/"));
        assert!(url.contains("sslmode=disable"));
        assert!(!url.contains(":ambient@"));
        assert_eq!(
            environment
                .get(OsStr::new(POSTGRES_TRANSPORT_ENV))
                .and_then(Option::as_ref),
            Some(&OsString::from(LOOPBACK_TRANSPORT))
        );
        fs::remove_dir_all(root).expect("remove environment fixture");
    }

    #[test]
    fn arbitrary_test_prefixed_names_are_recognised() {
        assert!(test_environment_name(OsStr::new(
            "OBZENFLOW_POSTGRES_TEST_SENTINEL"
        )));
        assert!(!test_environment_name(OsStr::new("OBZENFLOW_POSTGRES_URL")));
    }
}
