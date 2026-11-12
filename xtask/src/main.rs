// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

mod postgres;
mod studio_jobs;
mod support;
mod validation;

use std::{env, path::Path};
use support::{error, is_help, workspace_root, Result};

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    match args.as_slice() {
        [] => {
            print_help();
            Ok(())
        }
        [arg] if is_help(arg) => {
            print_help();
            Ok(())
        }
        [cmd, rest @ ..] if cmd == "studio-jobs" => studio_jobs::run(rest),
        [cmd, rest @ ..] if cmd == "postgres" => postgres::run(rest),
        [cmd, rest @ ..] if cmd == "test" => validation::run(&workspace_root()?, rest),
        [cmd, rest @ ..] if cmd == "performance" => {
            validation::run_performance(&workspace_root()?, rest)
        }
        [cmd, artifacts, invocation, source] if cmd == "__postgres-test" => {
            postgres::run_validation(&workspace_root()?, Path::new(artifacts), invocation, source)
        }
        [cmd, capability, artifacts] if cmd == "__test-prerequisite" => {
            validation::prerequisites::child(capability, Path::new(artifacts))
        }
        _ => Err(error(format!("unknown xtask command: {}", args.join(" ")))),
    }
}

fn print_help() {
    println!("usage:");
    println!("  cargo xtask studio-jobs <up|down|status>");
    println!("  cargo xtask postgres <up|status|connection|run|test|logs|down|cleanup>");
    println!("  cargo xtask test [--lane <lane>]");
    println!("  cargo xtask performance <plan|build|qualify|measure|assemble> --run-id <id>");
}
