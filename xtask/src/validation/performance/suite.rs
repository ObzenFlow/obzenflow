// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Full Criterion inventory, sharing compilation with the native comparison.
//! CPU assignments bound concurrency; memory, filesystem and caches remain shared.

use super::*;
use std::process::Command;

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct Target {
    name: String,
    #[serde(default, rename = "required-features")]
    features: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Schedule {
    version: u32,
    groups: Vec<Vec<String>>,
}

pub(super) fn inventory(root: &Path) -> Result<Vec<Target>> {
    #[derive(Deserialize)]
    struct Manifest {
        bench: Vec<Target>,
    }
    let manifest: Manifest = toml::from_str(&fs::read_to_string(
        root.join("crates/obzenflow_benchmarks/Cargo.toml"),
    )?)?;
    schedule(root, &manifest.bench)?;
    Ok(manifest.bench)
}

fn schedule(root: &Path, targets: &[Target]) -> Result<Vec<Vec<String>>> {
    let plan: Schedule = toml::from_str(&fs::read_to_string(
        root.join(".config/performance-suite.toml"),
    )?)?;
    let selected: Vec<_> = plan.groups.iter().flatten().cloned().collect();
    let unique: BTreeSet<_> = selected.iter().cloned().collect();
    if plan.version != 1
        || plan.groups.len() != 2
        || plan.groups.iter().any(Vec::is_empty)
        || unique.len() != selected.len()
        || unique != targets.iter().map(|t| t.name.clone()).collect()
    {
        return Err(error(
            "performance suite queues must cover every Cargo benchmark exactly once",
        ));
    }
    Ok(plan.groups)
}

pub(super) fn build_reference(
    root: &Path,
    source: &Path,
    target: &Path,
    tools: &Policy,
    directory: &Path,
) -> Result<BTreeMap<String, Executable>> {
    build_targets(
        root,
        source,
        target,
        tools,
        directory,
        "reference",
        &[
            Target {
                name: "journal_hot_path".into(),
                features: vec!["journal-benchmarks".into()],
            },
            Target {
                name: "validation_boundaries".into(),
                features: vec!["validation-benchmarks".into()],
            },
        ],
    )
}

pub(super) fn build_candidate(
    root: &Path,
    tools: &Policy,
    directory: &Path,
    targets: &[Target],
) -> Result<BTreeMap<String, Executable>> {
    build_targets(
        root,
        root,
        &root.join("target/validation-candidate"),
        tools,
        directory,
        "candidate",
        targets,
    )
}

fn build_targets(
    root: &Path,
    source: &Path,
    target: &Path,
    tools: &Policy,
    directory: &Path,
    label: &str,
    targets: &[Target],
) -> Result<BTreeMap<String, Executable>> {
    // Batch targets with identical features. Do not unify allocation-census into
    // timed binaries, or silently alter a target's declared feature identity.
    let mut groups: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for spec in targets {
        groups
            .entry(spec.features.join(","))
            .or_default()
            .push(spec.name.clone());
    }
    let mut binaries = BTreeMap::new();
    for (index, (features, targets)) in groups.iter().enumerate() {
        binaries.extend(build(
            root,
            source,
            target,
            tools,
            directory,
            &format!("{label}-build-{index}"),
            (targets, features),
        )?);
    }
    let mut census = build(
        root,
        source,
        target,
        tools,
        directory,
        &format!("{label}-census-build"),
        (
            &["journal_hot_path".into()],
            "journal-benchmarks,allocation-census",
        ),
    )?;
    binaries
        .get_mut("journal_hot_path")
        .ok_or_else(|| error("missing hot-path timing binary"))?
        .census = Some(Box::new(census.remove("journal_hot_path").unwrap()));
    Ok(binaries)
}

// No unpinned concurrent fallback: platforms without Linux affinity execute one
// measurement at a time, and record that distinct environment in the report.
fn cpu_groups() -> Result<Vec<Vec<usize>>> {
    #[cfg(target_os = "linux")]
    {
        let mut mask: libc::cpu_set_t = unsafe { std::mem::zeroed() };
        if unsafe { libc::sched_getaffinity(0, std::mem::size_of_val(&mask), &mut mask) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let cpus: Vec<_> = (0..libc::CPU_SETSIZE as usize)
            .filter(|&cpu| unsafe { libc::CPU_ISSET(cpu, &mask) })
            .collect();
        if cpus.len() >= 4 {
            // Four CPUs even on larger hosts: stable per-target capacity.
            return Ok(vec![cpus[..2].to_vec(), cpus[2..4].to_vec()]);
        }
    }
    Ok(vec![vec![]])
}

fn pin(command: &mut Command, cpus: &[usize]) {
    #[cfg(target_os = "linux")]
    if !cpus.is_empty() {
        use std::os::unix::process::CommandExt;
        let mut mask: libc::cpu_set_t = unsafe { std::mem::zeroed() };
        for &cpu in cpus {
            unsafe {
                libc::CPU_SET(cpu, &mut mask);
            }
        }
        // Runs in the child before exec. Only the affinity syscall is made;
        // descendants inherit the mask, including benchmark-owned subprocesses.
        unsafe {
            command.pre_exec(move || {
                if libc::sched_setaffinity(0, std::mem::size_of_val(&mask), &mask) == 0 {
                    Ok(())
                } else {
                    Err(std::io::Error::last_os_error())
                }
            });
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = (command, cpus);
}

fn measurement_command(
    root: &Path,
    tools: &Policy,
    binary: &Executable,
    home: &Path,
    cpus: &[usize],
) -> Command {
    let mut command = process::command(root, tools, &binary.path);
    command
        .env("CRITERION_HOME", home)
        .env_remove("OBZENFLOW_WORK_CENSUS")
        .env_remove("OBZENFLOW_BENCH_CONTROL")
        .env("RAYON_NUM_THREADS", "2")
        .env("RUST_LOG", "warn");
    pin(&mut command, cpus);
    command
}

fn listed_cases(output: &str) -> Result<BTreeSet<String>> {
    let cases: Vec<_> = output
        .lines()
        .filter_map(|line| line.strip_suffix(": benchmark"))
        .map(str::to_owned)
        .collect();
    let unique: BTreeSet<_> = cases.iter().cloned().collect();
    if unique.is_empty() || unique.len() != cases.len() {
        return Err(error("Criterion listed no cases or duplicate IDs"));
    }
    Ok(unique)
}

fn measure_target(
    root: &Path,
    tools: &Policy,
    directory: &Path,
    spec: &Target,
    binary: &Executable,
    cpus: &[usize],
) -> Result<()> {
    let home = directory.join("criterion");
    let listed = process::execute(
        measurement_command(root, tools, binary, &home, cpus).arg("--list"),
        directory,
        "inventory",
        Duration::from_secs(tools.command_watchdog_seconds),
    )?;
    if !listed.success() {
        return Err(failed("Criterion inventory failed"));
    }
    let expected = listed_cases(&fs::read_to_string(directory.join("inventory.stdout.log"))?)?;
    fs::write(
        directory.join("cases.json"),
        serde_json::to_vec_pretty(&expected)?,
    )?;
    // Criterion retains each target's authored sample sizes and measurement
    // budgets. No shortened CI profile and no retries.
    let status = process::execute(
        measurement_command(root, tools, binary, &home, cpus).arg("--bench"),
        directory,
        "measurement",
        Duration::from_secs(tools.command_watchdog_seconds),
    )?;
    if !status.success() {
        return Err(failed(format!("{}: Criterion failed", spec.name)));
    }
    let mut found = BTreeMap::new();
    collect_samples(&home, "new", &mut found)?;
    if found.keys().cloned().collect::<BTreeSet<_>>() != expected {
        return Err(error(format!(
            "{}: measurements differ from executable's case inventory",
            spec.name
        )));
    }
    Ok(())
}

pub(super) fn run(
    root: &Path,
    tools: &Policy,
    directory: &Path,
    targets: &[Target],
    binaries: &BTreeMap<String, Executable>,
) -> Result<()> {
    let cpus = cpu_groups()?;
    let mut groups = schedule(root, targets)?;
    if cpus.len() == 1 {
        groups = vec![groups.into_iter().flatten().collect()];
    }
    fs::write(
        directory.join("suite-execution.json"),
        serde_json::to_vec_pretty(&json!({
            "mode": if cpus.len() == 2 { "two-cpu-partitions-v1" } else { "serial-unpinned-v1" },
            "cpu_groups": cpus, "target_groups": groups,
            "tokio_default_workers": tools.tokio_workers, "rayon_threads": 2,
            "shared_resources": "memory, caches and filesystem; full-suite observations are not the comparison gate",
        }))?,
    )?;
    let outcomes = std::thread::scope(|scope| {
        let workers: Vec<_> = groups.iter().zip(&cpus).map(|(group, cpus)| scope.spawn(move || {
            group.iter().map(|name| {
                let spec = targets.iter().find(|t| &t.name == name).unwrap();
                let phase = directory.join("suite").join(name);
                let started = Instant::now();
                let result = (|| {
                    fs::create_dir_all(&phase)?;
                    if process::was_interrupted() { return Err(error("interrupted before benchmark execution")); }
                    measure_target(root, tools, &phase, spec, &binaries[name], cpus)
                })();
                let record = json!({"target": name, "features": spec.features, "cpus": cpus,
                    "passed": result.is_ok(), "error": result.as_ref().err().map(ToString::to_string),
                    "elapsed_seconds": started.elapsed().as_secs_f64(), "executable_sha256": binaries[name].sha256});
                fs::write(phase.join("outcome.json"), serde_json::to_vec_pretty(&record).unwrap())
                    .map_err(|e| e.to_string())?;
                result.map_err(|e| e.to_string())
            }).collect::<Vec<std::result::Result<(), String>>>()
        })).collect();
        workers
            .into_iter()
            .flat_map(|worker| worker.join().expect("benchmark thread panicked"))
            .collect::<Vec<_>>()
    });
    let failures: Vec<_> = outcomes
        .into_iter()
        .filter_map(std::result::Result::err)
        .collect();
    if !failures.is_empty() {
        return Err(failed(format!(
            "full Criterion suite: {}",
            failures.join("; ")
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repository_schedule_covers_manifest_without_allocation_instrumentation() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
        let targets = inventory(root).unwrap();
        assert_eq!(targets.len(), 16);
        assert!(targets
            .iter()
            .all(|t| !t.features.iter().any(|f| f == "allocation-census")));
    }

    #[test]
    fn case_inventory_retains_full_ids_and_rejects_empty_or_duplicate_output() {
        assert_eq!(
            listed_cases("diagnostic\nreader/8: benchmark\nreader/32: benchmark\n").unwrap(),
            BTreeSet::from(["reader/8".into(), "reader/32".into()])
        );
        assert!(listed_cases("no measurements").is_err());
        assert!(listed_cases("reader/8: benchmark\nreader/8: benchmark").is_err());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn child_affinity_matches_its_partition() {
        let groups = cpu_groups().unwrap();
        if groups.len() < 2 {
            return;
        }
        assert!(groups[0].iter().all(|cpu| !groups[1].contains(cpu)));
        for cpus in groups {
            let mut command = Command::new("sh");
            command.args([
                "-c",
                "awk '/Cpus_allowed_list/ {print $2}' /proc/self/status",
            ]);
            pin(&mut command, &cpus);
            let output = command.output().unwrap();
            assert!(output.status.success());
            let text = String::from_utf8(output.stdout).unwrap();
            assert!(!text.trim().is_empty());
            let observed: BTreeSet<usize> = text
                .trim()
                .split(',')
                .flat_map(|part| {
                    let mut ends = part.split('-').map(|v| v.parse::<usize>().unwrap());
                    let first = ends.next().unwrap();
                    first..=ends.next().unwrap_or(first)
                })
                .collect();
            assert_eq!(observed, cpus.into_iter().collect());
        }
    }
}
