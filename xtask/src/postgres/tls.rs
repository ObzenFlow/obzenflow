// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::{error, Result};
use std::{
    fs,
    path::Path,
    process::{Command, Stdio},
};

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
    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn require_private_directory(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| reset_required("PostgreSQL development TLS directory is missing"))?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(reset_required(
            "PostgreSQL development TLS path is not a regular directory",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o777 != 0o700 {
            return Err(reset_required(
                "PostgreSQL development TLS directory permissions must be 0700",
            ));
        }
    }
    Ok(())
}

fn require_regular_file(path: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path).map_err(|_| {
        reset_required(format!(
            "PostgreSQL development TLS file {label} is missing"
        ))
    })?;
    if metadata.is_file() && !metadata.file_type().is_symlink() {
        Ok(())
    } else {
        Err(reset_required(format!(
            "PostgreSQL development TLS file {label} is not a regular file"
        )))
    }
}

#[cfg(unix)]
fn require_private_file(path: &Path, label: &str) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let metadata = fs::symlink_metadata(path)?;
    if metadata.permissions().mode() & 0o777 == 0o600 {
        Ok(())
    } else {
        Err(reset_required(format!(
            "PostgreSQL development TLS file {label} permissions must be 0600"
        )))
    }
}

#[cfg(not(unix))]
fn require_private_file(_path: &Path, _label: &str) -> Result<()> {
    Ok(())
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

#[cfg(unix)]
fn set_private_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_private_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

fn reset_required(message: impl AsRef<str>) -> Box<dyn std::error::Error> {
    error(format!(
        "{}; reset this development session with `cargo xtask postgres down --volumes`",
        message.as_ref()
    ))
}
