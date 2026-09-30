// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Finite capacity experiments. Workload decisions and acceptance limits do
//! not live in fixtures; incomplete resource attribution is reported explicitly.

pub mod projection;

/// Mark an invocation incomplete before its first case and retain each
/// completed census. A crash cannot leave a prior completed report in place.
pub fn save_census(cases: &[super::Census], completed_selected_cases: bool) {
    if let Ok(path) = std::env::var("OBZENFLOW_WORK_CENSUS") {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path,serde_json::to_vec_pretty(&serde_json::json!({
            "compiled_manifest_dir":env!("CARGO_MANIFEST_DIR"),"cases":cases,
            "completed_selected_cases":completed_selected_cases,
            "scope":"finite capacity experiments; incomplete resource attribution; no supported limits",
        })).unwrap()).unwrap();
    }
}

pub fn process_usage() -> serde_json::Value {
    // RUSAGE_SELF reads only this benchmark process; no host/process discovery.
    #[cfg(unix)]
    {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        // SAFETY: getrusage writes one initialized rusage on a zero return.
        assert_eq!(
            unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) },
            0
        );
        let usage = unsafe { usage.assume_init() };
        let mut nofile = std::mem::MaybeUninit::<libc::rlimit>::uninit();
        // SAFETY: getrlimit writes one initialized rlimit on a zero return.
        assert_eq!(
            unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, nofile.as_mut_ptr()) },
            0
        );
        let nofile = unsafe { nofile.assume_init() };
        let micros = |time: libc::timeval| time.tv_sec as u64 * 1_000_000 + time.tv_usec as u64;
        #[cfg(target_os = "macos")]
        let peak_rss_bytes = usage.ru_maxrss as u64;
        #[cfg(not(target_os = "macos"))]
        let peak_rss_bytes = usage.ru_maxrss as u64 * 1024;
        serde_json::json!({
            "user_cpu_us": micros(usage.ru_utime), "system_cpu_us": micros(usage.ru_stime),
            "process_lifetime_peak_rss_bytes": peak_rss_bytes,
            "live_requested_rust_heap_bytes": super::allocations::live_requested_bytes(),
            "open_file_soft_limit":nofile.rlim_cur,"open_file_hard_limit":nofile.rlim_max,
            "rss_scope": "whole process since start, including fixture and diagnostic retention",
            "complete_memory_attribution": false,
        })
    }
    #[cfg(not(unix))]
    serde_json::json!({"unavailable": "RUSAGE_SELF requires Unix", "complete_memory_attribution":false})
}
