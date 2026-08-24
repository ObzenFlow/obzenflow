// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

mod acceptance;
mod compose;
mod config;
mod environment;
mod fixtures;
mod state;
mod tls;

use self::{
    compose::Compose,
    config::{DEVELOPMENT_PAYMENT_SCHEMA, IMAGE, STATE_FILE},
    state::{DevelopmentIdentity, SessionMode, SessionState},
};
use super::{error, Result};
use std::{fs, path::Path, process::Command};

pub(super) fn run(args: &[String]) -> Result<()> {
    let Some((command, flags)) = args.split_first() else {
        print_help();
        return Ok(());
    };
    match command.as_str() {
        "up" => up(flags),
        "status" => status(flags),
        "run" => run_child(flags),
        "test" => test(flags),
        "logs" => logs(flags),
        "down" => down(flags),
        "cleanup" => cleanup(flags),
        "help" | "-h" | "--help" => {
            print_help();
            Ok(())
        }
        other => Err(error(format!("unknown postgres command: {other}"))),
    }
}

fn up(flags: &[String]) -> Result<()> {
    if !accept_no_flags("postgres up", flags)? {
        return Ok(());
    }
    let root = super::workspace_root()?;
    let compose = Compose::preflight()?;
    let identity = state::development_identity(&root)?;
    let state_path = identity.directory.join(STATE_FILE);

    let mut session = if state_path.is_file() {
        let session = state::read(&state_path)?;
        state::require_development_authority(&session, &identity)?;
        session
    } else {
        fs::create_dir_all(&identity.directory)?;
        let session = SessionState {
            project: identity.project.clone(),
            run_id: state::unique_run_id(),
            port: 0,
            mode: SessionMode::Development,
        };
        state::write(&state_path, &session)?;
        session
    };

    tls::ensure(&identity.directory)?;
    if compose.health(&root, &identity.directory, &session)? == "healthy" {
        session.port = compose.published_port(&root, &identity.directory, &session)?;
    } else {
        compose.start(&root, &identity.directory, &mut session)?;
    }
    state::write(&state_path, &session)?;
    fixtures::provision_development(
        &root,
        &compose,
        &identity.directory,
        &session,
        DEVELOPMENT_PAYMENT_SCHEMA,
    )?;
    print_status(&session, "healthy");
    println!("run a command with: cargo xtask postgres run -- <command> [args...]");
    Ok(())
}

fn status(flags: &[String]) -> Result<()> {
    if !accept_no_flags("postgres status", flags)? {
        return Ok(());
    }
    let root = super::workspace_root()?;
    let compose = Compose::preflight()?;
    let identity = state::development_identity(&root)?;
    let state_path = identity.directory.join(STATE_FILE);
    if !state_path.is_file() {
        println!("no PostgreSQL development session state found");
        return report_existing_test_sessions(&root, &compose);
    }
    let mut session = state::read(&state_path)?;
    state::require_development_authority(&session, &identity)?;
    let health = compose.health(&root, &identity.directory, &session)?;
    if health == "healthy" {
        session.port = compose.published_port(&root, &identity.directory, &session)?;
        state::write(&state_path, &session)?;
    }
    print_status(&session, &health);
    report_existing_test_sessions(&root, &compose)
}

fn run_child(flags: &[String]) -> Result<()> {
    let Some((delimiter, command)) = flags.split_first() else {
        return Err(error("postgres run requires `-- <command> [args...]`"));
    };
    if delimiter != "--" || command.is_empty() {
        return Err(error("postgres run requires `-- <command> [args...]`"));
    }

    let root = super::workspace_root()?;
    let compose = Compose::preflight()?;
    let identity = state::development_identity(&root)?;
    let state_path = identity.directory.join(STATE_FILE);
    let mut session = state::read(&state_path).map_err(|_| {
        error("PostgreSQL development session is unavailable; run `cargo xtask postgres up`")
    })?;
    state::require_development_authority(&session, &identity)?;
    let health = compose.health(&root, &identity.directory, &session)?;
    if health != "healthy" {
        return Err(error(format!(
            "PostgreSQL project '{}' is not healthy; inspect with `cargo xtask postgres logs`",
            session.project
        )));
    }
    session.port = compose.published_port(&root, &identity.directory, &session)?;
    state::write(&state_path, &session)?;
    let service = compose.service_evidence(&root, &identity.directory, &session)?;

    let mut child = Command::new(&command[0]);
    child.current_dir(&root).args(&command[1..]);
    environment::configure(&mut child, &identity.directory, &session, &service)?;
    let child_status = child.status()?;
    if child_status.success() {
        Ok(())
    } else {
        Err(error(format!(
            "PostgreSQL child command failed with status {child_status}"
        )))
    }
}

fn test(flags: &[String]) -> Result<()> {
    if !accept_no_flags("postgres test", flags)? {
        return Ok(());
    }
    let root = super::workspace_root()?;
    let compose = Compose::preflight()?;
    report_existing_test_sessions(&root, &compose)?;
    acceptance::run(root, compose)
}

fn logs(flags: &[String]) -> Result<()> {
    if !accept_no_flags("postgres logs", flags)? {
        return Ok(());
    }
    let root = super::workspace_root()?;
    let compose = Compose::preflight()?;
    let (identity, session) = development_session(&root)?;
    let output = compose.logs(&root, &identity.directory, &session)?;
    print!("{}", String::from_utf8_lossy(&output.stdout));
    eprint!("{}", String::from_utf8_lossy(&output.stderr));
    if output.status.success() {
        Ok(())
    } else {
        Err(error("failed to read PostgreSQL container logs"))
    }
}

fn down(flags: &[String]) -> Result<()> {
    let volumes = match flags {
        [] => false,
        [flag] if flag == "--volumes" => true,
        [flag] if is_help(flag) => {
            print_help();
            return Ok(());
        }
        _ => return Err(error("postgres down accepts only `--volumes`")),
    };
    let root = super::workspace_root()?;
    let identity = state::development_identity(&root)?;
    let state_path = identity.directory.join(STATE_FILE);
    if !state_path.is_file() {
        println!("no PostgreSQL development session state found");
        return Ok(());
    }
    let compose = Compose::preflight()?;
    let session = state::read(&state_path)?;
    state::require_development_authority(&session, &identity)?;
    if let Some(container_id) = compose.container_id(&root, &identity.directory, &session)? {
        compose.verify_container_authority(&container_id, &identity.project)?;
    }
    if volumes {
        state::require_owned_directory(&root, &identity.directory)?;
        println!(
            "removing project '{}' and its PostgreSQL data volume; development data will not be recoverable",
            session.project
        );
    }
    compose.stop(&root, &identity.directory, &session, volumes)?;
    if volumes {
        state::remove_owned_directory(&root, &identity.directory)?;
    } else {
        println!("PostgreSQL project stopped; its named development volume is retained");
    }
    Ok(())
}

fn cleanup(flags: &[String]) -> Result<()> {
    let [run_id] = flags else {
        return Err(error("postgres cleanup requires one reported test run id"));
    };
    let root = super::workspace_root()?;
    let identity = state::test_identity(&root, run_id)?;
    state::require_owned_directory(&root, &identity.directory)?;
    let session = state::read(&identity.directory.join(STATE_FILE))?;
    state::require_test_authority(&session, &identity)?;
    let compose = Compose::preflight()?;
    if let Some(container_id) = compose.container_id(&root, &identity.directory, &session)? {
        compose.verify_container_authority(&container_id, &identity.project)?;
    }
    println!(
        "removing disposable PostgreSQL test project '{}' and its data volume",
        identity.project
    );
    let _ = compose.capture_logs(
        &root,
        &identity.directory,
        &session,
        &identity.directory.join(config::LOG_FILE),
    );
    compose.stop(&root, &identity.directory, &session, true)?;
    state::remove_owned_directory(&root, &identity.directory)?;
    println!("PostgreSQL test project '{}' cleaned", identity.project);
    Ok(())
}

fn development_session(root: &Path) -> Result<(DevelopmentIdentity, SessionState)> {
    let identity = state::development_identity(root)?;
    let session = state::read(&identity.directory.join(STATE_FILE)).map_err(|_| {
        error("PostgreSQL development session is unavailable; run `cargo xtask postgres up`")
    })?;
    state::require_development_authority(&session, &identity)?;
    Ok((identity, session))
}

fn report_existing_test_sessions(root: &Path, compose: &Compose) -> Result<()> {
    for (directory, session) in state::test_sessions(root)? {
        let health = compose.health(root, &directory, &session)?;
        if matches!(health.as_str(), "stopped" | "unavailable") {
            continue;
        }
        println!(
            "PostgreSQL test container remains present: project={} run_id={} health={}; exact state: {}",
            session.project,
            session.run_id,
            health,
            directory.join(STATE_FILE).display()
        );
        println!(
            "if its owning test process is no longer running, remove exactly this disposable project with: cargo xtask postgres cleanup {}",
            session.run_id
        );
    }
    Ok(())
}

fn print_status(session: &SessionState, health: &str) {
    println!(
        "project={} image={} health={} host=127.0.0.1 port={}",
        session.project, IMAGE, health, session.port
    );
}

fn accept_no_flags(command: &str, flags: &[String]) -> Result<bool> {
    match flags {
        [] => Ok(true),
        [flag] if is_help(flag) => {
            print_help();
            Ok(false)
        }
        _ => Err(error(format!("{command} accepts no options"))),
    }
}

fn is_help(value: &str) -> bool {
    matches!(value, "help" | "-h" | "--help")
}

fn print_help() {
    println!("usage:");
    println!("test suite:");
    println!("  cargo xtask postgres test");
    println!();
    println!("development session:");
    println!("  cargo xtask postgres up");
    println!("  cargo xtask postgres status");
    println!("  cargo xtask postgres run -- <command> [args...]");
    println!("  cargo xtask postgres logs");
    println!("  cargo xtask postgres down [--volumes]");
    println!();
    println!("recovery for an operator-identified abandoned disposable test session:");
    println!("  cargo xtask postgres cleanup <reported-test-run-id>");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proof_has_no_hidden_lifecycle_command() {
        let failure = run(&["prove-persistent".to_string()])
            .expect_err("lifecycle proof belongs to an integration test");
        assert_eq!(
            failure.to_string(),
            "unknown postgres command: prove-persistent"
        );
    }
}
