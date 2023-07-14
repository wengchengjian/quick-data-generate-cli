use rand::{rngs::ThreadRng, Rng};
use std::{
    cell::RefCell,
    process::Output,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::process::Command;

pub async fn get_system_usage() -> (u64, f32) {
    let os = std::env::consts::OS;

    if os.eq_ignore_ascii_case("windows") {
        return (
            get_memory_info_on_windows().await,
            get_cpu_info_on_windows().await,
        );
    } else {
        return (
            get_memory_info_on_linux().await,
            get_cpu_info_on_linux().await,
        );
    }
}

pub fn format_memory(size: u64) -> String {
    let mut bytes = String::new();
    if size >= 1024 * 1024 * 1024 {
        let s = size as f64 / (1024.0 * 1024.0 * 1024.0);
        bytes.push_str(format!("{:.2}GB", s).as_str());
    } else if size >= 1024 * 1024 {
        let s = size as f64 / (1024.0 * 1024.0);
        bytes.push_str(format!("{:.2}MB", s).as_str());
    } else if size >= 1024 {
        let s = size as f64 / (1024.0);
        bytes.push_str(format!("{:.2}KB", s).as_str());
    } else if size < 1024 {
        let s = size as f64;
        bytes.push_str(format!("{:.2}B", s).as_str());
    }
    return bytes.to_string();
}

thread_local! {
    static RNG: RefCell<ThreadRng> = RefCell::new(rand::thread_rng());
}

pub fn generate_ip() -> String {
    let ip1 = RNG.with(|rng| {
        let mut rng = rng.borrow_mut();
        rng.gen_range(0..256)
    });
    let ip2 = RNG.with(|rng| {
        let mut rng = rng.borrow_mut();
        rng.gen_range(0..256)
    });
    let ip3 = RNG.with(|rng| {
        let mut rng = rng.borrow_mut();
        rng.gen_range(0..256)
    });
    let ip4 = RNG.with(|rng| {
        let mut rng = rng.borrow_mut();
        rng.gen_range(0..256)
    });

    return format!("{ip1}.{ip2}.{ip3}.{ip4}");
}

pub fn get_current_date() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
pub async fn get_memory_info_on_linux() -> u64 {
    let output: Output = Command::new("ps")
        .arg("-o")
        .arg("rss=")
        .arg("-p")
        .arg(std::process::id().to_string())
        .output()
        .await
        .expect("failed to execute process");

    let mem: u64 = String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse()
        .expect("failed to parse memory usage");
    return mem;
}

pub async fn get_cpu_info_on_linux() -> f32 {
    let output: Output = Command::new("ps")
        .arg("-p")
        .arg(std::process::id().to_string())
        .arg("-o")
        .arg("%cpu=")
        .output()
        .await
        .expect("failed to execute process");

    let cpu: f32 = String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse()
        .expect("failed to parse memory usage");
    return cpu;
}

use winapi::{
    shared::minwindef::FILETIME,
    um::{
        processthreadsapi::{GetCurrentProcess, GetProcessTimes},
        winnt::LARGE_INTEGER,
    },
};

fn get_performance_frequency() -> i64 {
    let mut frequency = LARGE_INTEGER::default();
    unsafe {
        winapi::um::profileapi::QueryPerformanceFrequency(&mut frequency);
        frequency.QuadPart().clone()
    }
}

fn filetime_to_u64(ft: FILETIME) -> u64 {
    let mut res = (ft.dwHighDateTime as u64) << 32;
    res |= ft.dwLowDateTime as u64;
    res
}

pub fn get_cpu_time() -> (f64, f64) {
    let mut lp_creation_time = FILETIME::default();
    let mut lp_exit_time = FILETIME::default();
    let mut lp_kernel_time = FILETIME::default();
    let mut lp_user_time = FILETIME::default();

    unsafe {
        GetProcessTimes(
            GetCurrentProcess(),
            &mut lp_creation_time,
            &mut lp_exit_time,
            &mut lp_kernel_time,
            &mut lp_user_time,
        );
    }

    let kernel_time = filetime_to_u64(lp_kernel_time);
    let user_time = filetime_to_u64(lp_user_time);
    let system_time = get_elapsed_time();
    let frequency = get_performance_frequency();

    let total_time = (kernel_time + user_time) as f64 / frequency as f64;
    let elapsed_time = system_time as f64 / frequency as f64;

    (total_time as f64, elapsed_time as f64)
}

pub async fn get_cpu_info_on_windows() -> f32 {
    let (total_time, elapsed_time) = get_cpu_time();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (total_time2, elapsed_tim2) = get_cpu_time();
    (((total_time2 - total_time) / (elapsed_tim2 - elapsed_time) * 100.0)
        / num_cpus::get_physical() as f64) as f32
}

fn get_elapsed_time() -> i64 {
    let mut time = LARGE_INTEGER::default();
    unsafe {
        winapi::um::profileapi::QueryPerformanceCounter(&mut time);
        time.QuadPart().clone()
    }
}

use winapi::um::psapi::{GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS};
use winapi::um::winnt::HANDLE;

pub async fn get_memory_info_on_windows() -> u64 {
    unsafe {
        let process_handle: HANDLE = GetCurrentProcess();
        let mut pmc = PROCESS_MEMORY_COUNTERS::default();
        if GetProcessMemoryInfo(
            process_handle,
            &mut pmc,
            std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32,
        ) != 0
        {
            pmc.WorkingSetSize as u64
        } else {
            0
        }
    }
}
