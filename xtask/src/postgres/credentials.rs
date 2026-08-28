// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::config::{
    PGPASS_FILE, POSTGRES_CLIENT_HOST, POSTGRES_DATABASE, POSTGRES_USER, RAW_PASSWORD_FILE,
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
    pub(super) raw: PathBuf,
    pub(super) pgpass: PathBuf,
}

pub(super) fn raw_path(directory: &Path) -> PathBuf {
    directory.join(RAW_PASSWORD_FILE)
}

pub(super) fn pgpass_path(directory: &Path) -> PathBuf {
    directory.join(PGPASS_FILE)
}

pub(super) fn create_raw(directory: &Path) -> Result<()> {
    require_private_directory(directory)?;
    let secret = generate_secret()?;
    create_private_file(&raw_path(directory), secret.as_bytes())
}

pub(super) fn create_development_pgpass(directory: &Path, port: u16) -> Result<()> {
    let secret = read_raw(directory)?;
    let contents = pgpass_contents(&secret, port, &[POSTGRES_CLIENT_HOST]);
    create_private_file(&pgpass_path(directory), contents.as_bytes())
}

pub(super) fn create_test_pgpass(directory: &Path, port: u16) -> Result<()> {
    let secret = read_raw(directory)?;
    let contents = pgpass_contents(&secret, port, &[POSTGRES_CLIENT_HOST, "127.0.0.1"]);
    create_private_file(&pgpass_path(directory), contents.as_bytes())
}

pub(super) fn validate_development(directory: &Path, port: u16) -> Result<CredentialFiles> {
    validate(directory, port, &[POSTGRES_CLIENT_HOST])
}

pub(super) fn validate_test(directory: &Path, port: u16) -> Result<CredentialFiles> {
    validate(directory, port, &[POSTGRES_CLIENT_HOST, "127.0.0.1"])
}

fn validate(directory: &Path, port: u16, hosts: &[&str]) -> Result<CredentialFiles> {
    require_private_directory(directory)?;
    if port == 0 {
        return Err(reset_required(
            "PostgreSQL session credentials cannot be validated before port discovery",
        ));
    }
    let secret = read_raw(directory)?;
    let pgpass = pgpass_path(directory);
    require_private_regular_file(&pgpass, "pgpass")?;
    let actual = fs::read_to_string(&pgpass)
        .map_err(|_| reset_required("PostgreSQL pgpass credential is unreadable"))?;
    let expected = pgpass_contents(&secret, port, hosts);
    if actual != expected {
        return Err(reset_required(
            "PostgreSQL pgpass credential is malformed, endpoint-mismatched, or inconsistent",
        ));
    }
    Ok(CredentialFiles {
        raw: raw_path(directory),
        pgpass,
    })
}

fn read_raw(directory: &Path) -> Result<String> {
    let path = raw_path(directory);
    require_private_regular_file(&path, "raw")?;
    let secret = fs::read_to_string(&path)
        .map_err(|_| reset_required("PostgreSQL raw credential is unreadable"))?;
    if valid_secret(&secret) {
        Ok(secret)
    } else {
        Err(reset_required("PostgreSQL raw credential is malformed"))
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
            "refusing to overwrite PostgreSQL credential file {}: {failure}",
            path.display()
        ))
    })?;
    file.write_all(contents)?;
    file.sync_all()?;
    require_private_regular_file(path, "new")
}

fn require_private_directory(path: &Path) -> Result<()> {
    managed_fs::require_directory(path, "PostgreSQL credential directory")
        .map_err(|failure| reset_required(failure.to_string()))
}

fn require_private_regular_file(path: &Path, label: &str) -> Result<()> {
    managed_fs::require_secret_file(path, &format!("PostgreSQL {label} credential"))
        .map_err(|failure| reset_required(failure.to_string()))
}

fn reset_required(message: impl AsRef<str>) -> Box<dyn std::error::Error> {
    error(format!(
        "{}; reset this session with `cargo xtask postgres down --volumes`",
        message.as_ref()
    ))
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
    fn exact_pgpass_pair_round_trips_without_overwrite() {
        let directory = private_directory("roundtrip");
        create_raw(&directory).expect("create raw credential");
        assert!(create_raw(&directory).is_err());
        create_development_pgpass(&directory, 15432).expect("create pgpass");
        let files = validate_development(&directory, 15432).expect("validate pair");
        assert_eq!(files.raw, directory.join(RAW_PASSWORD_FILE));
        assert_eq!(files.pgpass, directory.join(PGPASS_FILE));
        let secret = fs::read_to_string(&files.raw).expect("read fixture secret");
        assert_eq!(
            fs::read_to_string(&files.pgpass).expect("read pgpass"),
            format!("localhost:15432:obzenflow:obzenflow:{secret}\n")
        );
        assert!(validate_development(&directory, 15433).is_err());
        fs::remove_dir_all(directory).expect("remove fixture");
    }

    #[cfg(unix)]
    #[test]
    fn credential_permissions_fail_closed() {
        use std::os::unix::fs::PermissionsExt;

        let directory = private_directory("permissions");
        create_raw(&directory).expect("create raw credential");
        create_development_pgpass(&directory, 15432).expect("create pgpass");
        fs::set_permissions(raw_path(&directory), fs::Permissions::from_mode(0o640))
            .expect("weaken fixture permissions");
        assert!(validate_development(&directory, 15432).is_err());
        fs::remove_dir_all(directory).expect("remove fixture");
    }

    #[test]
    fn test_pgpass_has_only_the_two_exercised_hosts() {
        let directory = private_directory("test-hosts");
        create_raw(&directory).expect("create raw credential");
        create_test_pgpass(&directory, 25432).expect("create test pgpass");
        validate_test(&directory, 25432).expect("validate test pair");
        let pgpass = fs::read_to_string(pgpass_path(&directory)).expect("read test pgpass");
        assert_eq!(pgpass.lines().count(), 2);
        assert!(pgpass.starts_with("localhost:25432:obzenflow:obzenflow:"));
        assert!(pgpass.contains("\n127.0.0.1:25432:obzenflow:obzenflow:"));
        fs::remove_dir_all(directory).expect("remove fixture");
    }
}
