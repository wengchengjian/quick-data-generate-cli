use rand::{rngs::ThreadRng, Rng};
use std::{
    cell::RefCell,
    process::Output,
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::process::Command;

pub async fn get_system_usage() -> (f32, f32) {
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
pub async fn get_memory_info_on_linux() -> f32 {
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
    let mem = mem as f64 / 1024.0;
    return mem as f32;
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

/// 待实现
pub async fn get_cpu_info_on_windows() -> f32 {
    return 0.0;
}

pub async fn get_memory_info_on_windows() -> f32 {
    // let pid = std::process::id().to_string();

    // let cmd: String = format!("for /f \"tokens=5\" %a in ('tasklist /fi \"pid eq {pid}\" ^| findstr /i {pid}') do @echo %a");

    // let output = Command::new("cmd")
    //     .args(&["/C", &cmd])
    //     .output()
    //     .await
    //     .expect("failed to execute process");

    // let mem = String::from_utf8_lossy(&output.stdout);
    // let mem = mem.trim().parse().unwrap();
    let mem = 0.0;
    return mem;
}
