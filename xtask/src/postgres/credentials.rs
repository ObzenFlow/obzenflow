// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Disposable acceptance-session credentials.
//!
//! The persistent development service deliberately uses PostgreSQL `trust`
//! authentication and never calls this module.

use super::config::{
    ACCEPTANCE_PGPASS_FILE, ACCEPTANCE_RAW_PASSWORD_FILE, POSTGRES_CLIENT_HOST, POSTGRES_DATABASE,
    POSTGRES_USER,
};
use super::managed_fs;
use crate::{error, Result};
use ring::rand::{SecureRandom, SystemRandom};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};

const SECRET_BYTES: usize = 32;
const SECRET_HEX_LENGTH: usize = SECRET_BYTES * 2;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct CredentialFiles {
    pub(super) pgpass: PathBuf,
}

pub(super) fn acceptance_raw_path(directory: &Path) -> PathBuf {
    directory.join(ACCEPTANCE_RAW_PASSWORD_FILE)
}

pub(super) fn acceptance_pgpass_path(directory: &Path) -> PathBuf {
    directory.join(ACCEPTANCE_PGPASS_FILE)
}

pub(super) fn create_acceptance_raw(directory: &Path) -> Result<()> {
    require_private_directory(directory)?;
    let secret = generate_secret()?;
    create_private_file(&acceptance_raw_path(directory), secret.as_bytes())
}

pub(super) fn create_acceptance_pgpass(directory: &Path, port: u16) -> Result<()> {
    let secret = read_raw(directory)?;
    let contents = pgpass_contents(&secret, port, &[POSTGRES_CLIENT_HOST, "127.0.0.1"]);
    create_private_file(&acceptance_pgpass_path(directory), contents.as_bytes())
}

pub(super) fn validate_acceptance(directory: &Path, port: u16) -> Result<CredentialFiles> {
    require_private_directory(directory)?;
    if port == 0 {
        return Err(invalid(
            "PostgreSQL acceptance credentials cannot be validated before port discovery",
        ));
    }
    let secret = read_raw(directory)?;
    let pgpass = acceptance_pgpass_path(directory);
    require_private_regular_file(&pgpass, "pgpass")?;
    let actual = fs::read_to_string(&pgpass)
        .map_err(|_| invalid("PostgreSQL acceptance pgpass credential is unreadable"))?;
    let expected = pgpass_contents(&secret, port, &[POSTGRES_CLIENT_HOST, "127.0.0.1"]);
    if actual != expected {
        return Err(invalid(
            "PostgreSQL acceptance pgpass credential is malformed, endpoint-mismatched, or inconsistent",
        ));
    }
    Ok(CredentialFiles { pgpass })
}

fn read_raw(directory: &Path) -> Result<String> {
    let path = acceptance_raw_path(directory);
    require_private_regular_file(&path, "raw")?;
    let secret = fs::read_to_string(&path)
        .map_err(|_| invalid("PostgreSQL acceptance raw credential is unreadable"))?;
    if valid_secret(&secret) {
        Ok(secret)
    } else {
        Err(invalid("PostgreSQL acceptance raw credential is malformed"))
    }
}

fn generate_secret() -> Result<String> {
    let mut bytes = [0_u8; SECRET_BYTES];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| error("operating-system random credential generation failed"))?;
    let mut secret = String::with_capacity(SECRET_HEX_LENGTH);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        secret.push(char::from(HEX[usize::from(byte >> 4)]));
        secret.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(secret)
}

fn valid_secret(secret: &str) -> bool {
    secret.len() == SECRET_HEX_LENGTH
        && secret
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn pgpass_contents(secret: &str, port: u16, hosts: &[&str]) -> String {
    hosts
        .iter()
        .map(|host| format!("{host}:{port}:{POSTGRES_DATABASE}:{POSTGRES_USER}:{secret}\n"))
        .collect()
}

fn create_private_file(path: &Path, contents: &[u8]) -> Result<()> {
    let mut file = managed_fs::secret_file_create_new(path).map_err(|failure| {
        error(format!(
            "refusing to overwrite PostgreSQL acceptance credential file {}: {failure}",
            path.display()
        ))
    })?;
    file.write_all(contents)?;
    file.sync_all()?;
    require_private_regular_file(path, "new")
}

fn require_private_directory(path: &Path) -> Result<()> {
    managed_fs::require_directory(path, "PostgreSQL acceptance credential directory")
        .map_err(|failure| invalid(failure.to_string()))
}

fn require_private_regular_file(path: &Path, label: &str) -> Result<()> {
    managed_fs::require_secret_file(path, &format!("PostgreSQL acceptance {label} credential"))
        .map_err(|failure| invalid(failure.to_string()))
}

fn invalid(message: impl AsRef<str>) -> Box<dyn std::error::Error> {
    error(message.as_ref())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    fn private_directory(label: &str) -> PathBuf {
        let path = env::temp_dir().join(format!(
            "obzenflow-credentials-{label}-{}",
            super::super::state::unique_run_id()
        ));
        fs::create_dir_all(&path).expect("create credential fixture");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
                .expect("secure credential fixture");
        }
        path
    }

    #[test]
    fn generated_credentials_are_unique_lowercase_hex() {
        let first = generate_secret().expect("generate first secret");
        let second = generate_secret().expect("generate second secret");
        assert!(valid_secret(&first));
        assert!(valid_secret(&second));
        assert_ne!(first, second);
    }

    #[test]
    fn acceptance_pgpass_pair_round_trips_without_overwrite() {
        let directory = private_directory("roundtrip");
        create_acceptance_raw(&directory).expect("create raw credential");
        assert!(create_acceptance_raw(&directory).is_err());
        create_acceptance_pgpass(&directory, 15432).expect("create pgpass");
        let files = validate_acceptance(&directory, 15432).expect("validate pair");
        assert_eq!(files.pgpass, directory.join(ACCEPTANCE_PGPASS_FILE));
        let secret =
            fs::read_to_string(acceptance_raw_path(&directory)).expect("read fixture secret");
        assert_eq!(
            fs::read_to_string(&files.pgpass).expect("read pgpass"),
            format!(
                "localhost:15432:obzenflow:obzenflow:{secret}\n127.0.0.1:15432:obzenflow:obzenflow:{secret}\n"
            )
        );
        assert!(validate_acceptance(&directory, 15433).is_err());
        fs::remove_dir_all(directory).expect("remove fixture");
    }

    #[cfg(unix)]
    #[test]
    fn acceptance_credential_permissions_fail_closed() {
        use std::os::unix::fs::PermissionsExt;

        let directory = private_directory("permissions");
        create_acceptance_raw(&directory).expect("create raw credential");
        create_acceptance_pgpass(&directory, 15432).expect("create pgpass");
        fs::set_permissions(
            acceptance_raw_path(&directory),
            fs::Permissions::from_mode(0o640),
        )
        .expect("weaken fixture permissions");
        assert!(validate_acceptance(&directory, 15432).is_err());
        fs::remove_dir_all(directory).expect("remove fixture");
    }

    #[test]
    fn acceptance_pgpass_has_only_the_two_exercised_hosts() {
        let directory = private_directory("test-hosts");
        create_acceptance_raw(&directory).expect("create raw credential");
        create_acceptance_pgpass(&directory, 25432).expect("create test pgpass");
        validate_acceptance(&directory, 25432).expect("validate test pair");
        let pgpass =
            fs::read_to_string(acceptance_pgpass_path(&directory)).expect("read test pgpass");
        assert_eq!(pgpass.lines().count(), 2);
        assert!(pgpass.starts_with("localhost:25432:obzenflow:obzenflow:"));
        assert!(pgpass.contains("\n127.0.0.1:25432:obzenflow:obzenflow:"));
        fs::remove_dir_all(directory).expect("remove fixture");
    }
}
