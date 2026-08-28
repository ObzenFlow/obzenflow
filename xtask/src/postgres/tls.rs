// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{
    config::{DEVELOPMENT_CA_FILE, DEVELOPMENT_SESSION, DEVELOPMENT_STATE_ROOT},
    managed_fs,
};
use crate::{error, Result};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};
use uuid::Uuid;

const DEVELOPMENT_DAYS: &str = "3650";
const TEST_DAYS: &str = "7";

pub(super) fn create_development(directory: &Path) -> Result<()> {
    let tls = directory.join("tls");
    if tls.exists() {
        return Err(reset_required(
            "PostgreSQL development TLS state already exists before first-start generation",
        ));
    }
    generate(&tls, DEVELOPMENT_DAYS, false, "Development")
}

pub(super) fn verify_development(directory: &Path) -> Result<()> {
    let tls = directory.join("tls");
    require_private_directory(&tls)?;
    for name in ["ca.crt", "server.crt", "server.key"] {
        require_regular_file(&tls.join(name), name)?;
    }
    require_private_file(&tls.join("server.key"), "server.key")?;
    check_certificate(&tls.join("ca.crt"))?;
    check_certificate(&tls.join("server.crt"))?;
    let status = Command::new("openssl")
        .args([
            "verify",
            "-CAfile",
            path_arg(&tls.join("ca.crt"))?,
            path_arg(&tls.join("server.crt"))?,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|_| reset_required("OpenSSL could not validate development TLS state"))?;
    if status.success() {
        Ok(())
    } else {
        Err(reset_required(
            "PostgreSQL development TLS certificate is invalid or no longer trusted",
        ))
    }
}

pub(super) fn client_ca(root: &Path, directory: &Path) -> Result<PathBuf> {
    let managed = directory.join("tls/ca.crt");
    let checkout_development = root.join(DEVELOPMENT_STATE_ROOT).join(DEVELOPMENT_SESSION);
    if directory != checkout_development {
        return Ok(managed);
    }

    require_regular_file(&managed, "ca.crt")?;
    let published = root.join(DEVELOPMENT_CA_FILE);
    let parent = published
        .parent()
        .ok_or_else(|| error("PostgreSQL client CA path has no parent directory"))?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(".local-ca-{}.tmp", Uuid::new_v4().simple()));
    let publish_result: Result<()> = (|| {
        let mut file = public_file_create_new(&temporary)?;
        file.write_all(&fs::read(&managed)?)?;
        file.sync_all()?;
        set_public_permissions(&temporary)?;
        #[cfg(windows)]
        if published.exists() {
            fs::remove_file(&published)?;
        }
        fs::rename(&temporary, &published)?;
        Ok(())
    })();
    if publish_result.is_err() && temporary.exists() {
        let _ = fs::remove_file(&temporary);
    }
    publish_result?;
    Ok(published)
}

pub(super) fn remove_client_ca(root: &Path, directory: &Path) -> Result<()> {
    let checkout_development = root.join(DEVELOPMENT_STATE_ROOT).join(DEVELOPMENT_SESSION);
    if directory != checkout_development {
        return Ok(());
    }
    match fs::remove_file(root.join(DEVELOPMENT_CA_FILE)) {
        Ok(()) => Ok(()),
        Err(failure) if failure.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(failure) => Err(failure.into()),
    }
}

pub(super) fn create_test(directory: &Path) -> Result<()> {
    let tls = directory.join("tls");
    if tls.exists() {
        return Err(error(
            "PostgreSQL test TLS state already exists before disposable generation",
        ));
    }
    generate(&tls, TEST_DAYS, true, "Test")
}

fn generate(tls: &Path, days: &str, untrusted: bool, label: &str) -> Result<()> {
    if !command_succeeds("openssl", &["version"]) {
        return Err(error(
            "OpenSSL is required to generate the PostgreSQL certificate",
        ));
    }
    create_private_directory(tls)?;

    let ca_key = tls.join("ca.key");
    let ca_cert = tls.join("ca.crt");
    let server_key = tls.join("server.key");
    let server_csr = tls.join("server.csr");
    let server_cert = tls.join("server.crt");
    let extensions = tls.join("server.ext");
    fs::write(
        &extensions,
        "subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n",
    )?;

    let ca_subject = format!("/CN=ObzenFlow PostgreSQL {label} CA");
    run(
        "openssl",
        &[
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-sha256",
            "-days",
            days,
            "-nodes",
            "-subj",
            &ca_subject,
            "-keyout",
            path_arg(&ca_key)?,
            "-out",
            path_arg(&ca_cert)?,
        ],
    )?;
    run(
        "openssl",
        &[
            "req",
            "-newkey",
            "rsa:2048",
            "-sha256",
            "-nodes",
            "-subj",
            "/CN=localhost",
            "-keyout",
            path_arg(&server_key)?,
            "-out",
            path_arg(&server_csr)?,
        ],
    )?;
    run(
        "openssl",
        &[
            "x509",
            "-req",
            "-in",
            path_arg(&server_csr)?,
            "-CA",
            path_arg(&ca_cert)?,
            "-CAkey",
            path_arg(&ca_key)?,
            "-CAcreateserial",
            "-days",
            days,
            "-sha256",
            "-extfile",
            path_arg(&extensions)?,
            "-out",
            path_arg(&server_cert)?,
        ],
    )?;
    if untrusted {
        let untrusted_key = tls.join("untrusted-ca.key");
        let untrusted_cert = tls.join("untrusted-ca.crt");
        run(
            "openssl",
            &[
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-sha256",
                "-days",
                days,
                "-nodes",
                "-subj",
                "/CN=ObzenFlow Untrusted PostgreSQL Test CA",
                "-keyout",
                path_arg(&untrusted_key)?,
                "-out",
                path_arg(&untrusted_cert)?,
            ],
        )?;
        set_private_permissions(&untrusted_key)?;
        fs::remove_file(untrusted_key)?;
    }
    set_private_permissions(&ca_key)?;
    set_private_permissions(&server_key)?;
    for temporary in [ca_key, server_csr, extensions, tls.join("ca.srl")] {
        if temporary.exists() {
            fs::remove_file(temporary)?;
        }
    }
    Ok(())
}

fn check_certificate(path: &Path) -> Result<()> {
    let status = Command::new("openssl")
        .args(["x509", "-checkend", "0", "-noout", "-in", path_arg(path)?])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|_| reset_required("OpenSSL could not inspect development TLS state"))?;
    if status.success() {
        Ok(())
    } else {
        Err(reset_required(
            "PostgreSQL development TLS certificate has expired",
        ))
    }
}

fn create_private_directory(path: &Path) -> Result<()> {
    managed_fs::create_directory(path)
}

fn require_private_directory(path: &Path) -> Result<()> {
    managed_fs::require_directory(path, "PostgreSQL development TLS directory")
        .map_err(|failure| reset_required(failure.to_string()))
}

fn require_regular_file(path: &Path, label: &str) -> Result<()> {
    managed_fs::require_regular_file(path, &format!("PostgreSQL development TLS file {label}"))
        .map_err(|failure| reset_required(failure.to_string()))
}

fn require_private_file(path: &Path, label: &str) -> Result<()> {
    managed_fs::require_secret_file(path, &format!("PostgreSQL development TLS file {label}"))
        .map_err(|failure| reset_required(failure.to_string()))
}

fn command_succeeds(program: &str, args: &[&str]) -> bool {
    Command::new(program)
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn run(program: &str, args: &[&str]) -> Result<()> {
    let status = Command::new(program)
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|_| error(format!("failed to launch {program}")))?;
    if status.success() {
        Ok(())
    } else {
        Err(error(format!("{program} failed with status {status}")))
    }
}

fn path_arg(path: &Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| error("PostgreSQL session path is not valid Unicode"))
}

fn public_file_create_new(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o644);
    }
    Ok(options.open(path)?)
}

#[cfg(unix)]
fn set_public_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o644))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_public_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

fn set_private_permissions(path: &Path) -> Result<()> {
    managed_fs::set_secret_file_permissions(path, "generated PostgreSQL TLS private key")
}

fn reset_required(message: impl AsRef<str>) -> Box<dyn std::error::Error> {
    error(format!(
        "{}; reset this development session with `cargo xtask postgres down --volumes`",
        message.as_ref()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_checkout_development_ca_is_published_and_removed() {
        let root = std::env::temp_dir().join(format!(
            "obzenflow-postgres-client-ca-{}",
            Uuid::new_v4().simple()
        ));
        let development = root.join(DEVELOPMENT_STATE_ROOT).join(DEVELOPMENT_SESSION);
        fs::create_dir_all(development.join("tls")).expect("create development TLS fixture");
        fs::write(development.join("tls/ca.crt"), b"development-ca")
            .expect("write development CA fixture");

        let published = client_ca(&root, &development).expect("publish development CA");
        assert_eq!(published, root.join(DEVELOPMENT_CA_FILE));
        assert_eq!(
            fs::read(&published).expect("read published CA"),
            b"development-ca"
        );

        let isolated = root.join("target/postgres-sessions/persistent-proof");
        fs::create_dir_all(isolated.join("tls")).expect("create isolated TLS fixture");
        fs::write(isolated.join("tls/ca.crt"), b"isolated-ca").expect("write isolated CA fixture");
        assert_eq!(
            client_ca(&root, &isolated).expect("resolve isolated CA"),
            isolated.join("tls/ca.crt")
        );
        assert_eq!(
            fs::read(&published).expect("isolated session leaves published CA unchanged"),
            b"development-ca"
        );

        remove_client_ca(&root, &isolated).expect("ignore isolated CA cleanup");
        assert!(published.is_file());
        remove_client_ca(&root, &development).expect("remove published development CA");
        assert!(!published.exists());
        fs::remove_dir_all(root).expect("remove client CA fixture");
    }
}
