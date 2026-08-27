// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{
    compose::Compose,
    config::{POSTGRES_DATABASE, POSTGRES_USER},
    state::SessionState,
};
use crate::{error, Result};
use std::{fs, io::Write, path::Path, process::Stdio};

const PAYMENTS: &str = "dev/postgres/fixtures/payments.sql";
const INVENTORY: &str = "dev/postgres/fixtures/inventory.sql";
const HN_DIGEST_SUMMARIES: &str = "dev/postgres/fixtures/hn_digest_summaries.sql";

pub(super) fn provision_development(
    root: &Path,
    compose: &Compose,
    directory: &Path,
    state: &SessionState,
    schema: &str,
) -> Result<()> {
    apply(root, compose, directory, state, schema, PAYMENTS)?;
    apply(root, compose, directory, state, schema, HN_DIGEST_SUMMARIES)?;
    provision_tests(root, compose, directory, state)
}

pub(super) fn provision_tests(
    root: &Path,
    compose: &Compose,
    directory: &Path,
    state: &SessionState,
) -> Result<()> {
    apply(
        root,
        compose,
        directory,
        state,
        &super::config::payment_test_schema(&state.run_id),
        PAYMENTS,
    )?;
    apply(
        root,
        compose,
        directory,
        state,
        &super::config::inventory_test_schema(&state.run_id),
        INVENTORY,
    )?;
    apply(
        root,
        compose,
        directory,
        state,
        &super::config::hn_digest_test_schema(&state.run_id),
        HN_DIGEST_SUMMARIES,
    )
}

fn apply(
    root: &Path,
    compose: &Compose,
    directory: &Path,
    state: &SessionState,
    schema: &str,
    fixture: &str,
) -> Result<()> {
    validate_identifier(schema)?;
    let sql = fs::read(root.join(fixture))?;
    let variable = format!("schema={schema}");
    let mut child = compose
        .command(
            root,
            directory,
            state,
            &[
                "exec",
                "-T",
                "postgres",
                "psql",
                "-U",
                POSTGRES_USER,
                "-d",
                POSTGRES_DATABASE,
                "-v",
                "ON_ERROR_STOP=1",
                "-v",
                &variable,
                "-f",
                "-",
            ],
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    child
        .stdin
        .take()
        .ok_or_else(|| error("failed to open PostgreSQL fixture input"))?
        .write_all(&sql)?;
    let output = child.wait_with_output()?;
    if output.status.success() {
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(error(format!(
            "failed to apply PostgreSQL fixture {fixture}: {}",
            stderr.trim()
        )))
    }
}

fn validate_identifier(value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    if !bytes.is_empty()
        && bytes.len() <= 63
        && (bytes[0].is_ascii_alphabetic() || bytes[0] == b'_')
        && bytes[1..]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'_' || *byte == b'$')
    {
        Ok(())
    } else {
        Err(error(
            "PostgreSQL fixture schema is outside the portable identifier grammar",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_schema_identifiers_are_validated_before_psql() {
        assert!(validate_identifier("obzenflow_example").is_ok());
        assert!(validate_identifier("083c").is_err());
        assert!(validate_identifier("public; DROP SCHEMA public").is_err());
        assert!(validate_identifier(&"a".repeat(64)).is_err());
    }
}
