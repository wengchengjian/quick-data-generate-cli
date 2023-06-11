use rand::{rngs::ThreadRng, Rng};
use std::{
    cell::RefCell,
    process::Output,
    time::{SystemTime, UNIX_EPOCH, Duration}, mem,
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

pub fn format_memory(size:u64)-> String {
    let mut bytes = String::new();
        if (size >= 1024 * 1024 * 1024) {
            let s = size as f64 / (1024.0 * 1024.0 * 1024.0);
            bytes.push_str(format!("{:.2}GB",s).as_str());
        }
        else if (size >= 1024 * 1024) {
            let s = size as f64 / (1024.0 * 1024.0);
            bytes.push_str(format!("{:.2}MB",s).as_str());
        }
        else if (size >= 1024) {
            let s = size as f64 / (1024.0);
            bytes.push_str(format!("{:.2}KB",s).as_str());
        }
        else if (size < 1024) {
            let s = size as f64;
            bytes.push_str(format!("{:.2}B",s).as_str());
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



use winapi::{um::{processthreadsapi::{GetCurrentProcess, GetProcessTimes}, winnt::LARGE_INTEGER}, shared::minwindef::FILETIME};


pub async fn get_cpu_info_on_windows() -> f32 {
    let mut creation_time = FILETIME::default();
    let mut exit_time = FILETIME::default();
    let mut kernel_time = FILETIME::default();
    let mut user_time = FILETIME::default();

    unsafe {
        GetProcessTimes(
            GetCurrentProcess(),
            &mut creation_time,
            &mut exit_time,
            &mut kernel_time,
            &mut user_time,
        );
    }

    let kernel_time = filetime_to_large_int(&kernel_time);
    let user_time = filetime_to_large_int(&user_time);

    let mut sys_time = LARGE_INTEGER::default();
    unsafe {
        let success = winapi::um::profileapi::QueryPerformanceCounter(&mut sys_time);
        if success == 0 {
            panic!("QueryPerformanceCounter failed");
        }
    }

    let mut sys_time_freq = LARGE_INTEGER::default();
    unsafe {
        let success = winapi::um::profileapi::QueryPerformanceFrequency(&mut sys_time_freq);
        if success == 0 {
            panic!("QueryPerformanceFrequency failed");
        }
    }
    unsafe {
        let sys_time = sys_time.QuadPart().clone() as f64;
        let sys_time_freq = sys_time_freq.QuadPart().clone() as f64;
    
        let total_cpu_time = (kernel_time + user_time) as f64 / sys_time_freq * 100.0;
        let wall_clock_time = sys_time / sys_time_freq * (100.0) as f64;
    
        (total_cpu_time / wall_clock_time) as f32
    }
    
}

fn filetime_to_large_int(ft: &FILETIME) -> i64 {
    ((ft.dwHighDateTime as i64) << 32) | (ft.dwLowDateTime as i64)
}

use winapi::um::psapi::{GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS};
use winapi::um::winnt::HANDLE;

pub async fn get_memory_info_on_windows() -> u64 {
    unsafe {
        let process_handle: HANDLE = GetCurrentProcess();
        let mut pmc = PROCESS_MEMORY_COUNTERS::default();
        if GetProcessMemoryInfo(process_handle, &mut pmc, std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32) != 0 {
            (pmc.WorkingSetSize as u64)
        } else {
            0
        }
    }
}
