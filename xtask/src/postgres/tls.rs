// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::managed_fs;
use crate::{error, Result};
use std::{
    fs,
    path::Path,
    process::{Command, Stdio},
};

const TEST_DAYS: &str = "7";

pub(super) fn create_test(directory: &Path) -> Result<()> {
    let tls = directory.join("tls");
    if tls.exists() {
        return Err(error(
            "PostgreSQL acceptance TLS state already exists before disposable generation",
        ));
    }
    if !command_succeeds("openssl", &["version"]) {
        return Err(error(
            "OpenSSL is required to generate the PostgreSQL acceptance certificate",
        ));
    }
    managed_fs::create_directory(&tls)?;

    let ca_key = tls.join("ca.key");
    let ca_cert = tls.join("ca.crt");
    let server_key = tls.join("server.key");
    let server_csr = tls.join("server.csr");
    let server_cert = tls.join("server.crt");
    let extensions = tls.join("server.ext");
    let untrusted_key = tls.join("untrusted-ca.key");
    let untrusted_cert = tls.join("untrusted-ca.crt");
    fs::write(
        &extensions,
        "subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n",
    )?;

    run(
        "openssl",
        &[
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-sha256",
            "-days",
            TEST_DAYS,
            "-nodes",
            "-subj",
            "/CN=ObzenFlow PostgreSQL Test CA",
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
            TEST_DAYS,
            "-sha256",
            "-extfile",
            path_arg(&extensions)?,
            "-out",
            path_arg(&server_cert)?,
        ],
    )?;
    run(
        "openssl",
        &[
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-sha256",
            "-days",
            TEST_DAYS,
            "-nodes",
            "-subj",
            "/CN=ObzenFlow Untrusted PostgreSQL Test CA",
            "-keyout",
            path_arg(&untrusted_key)?,
            "-out",
            path_arg(&untrusted_cert)?,
        ],
    )?;

    for private_key in [&ca_key, &server_key, &untrusted_key] {
        managed_fs::set_secret_file_permissions(
            private_key,
            "generated PostgreSQL acceptance TLS private key",
        )?;
    }
    for temporary in [
        ca_key,
        untrusted_key,
        server_csr,
        extensions,
        tls.join("ca.srl"),
    ] {
        if temporary.exists() {
            fs::remove_file(temporary)?;
        }
    }
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
