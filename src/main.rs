use std::{
    cell::RefCell,
    env,
    error::Error,
    io::Write,
    process::Output,
    sync::atomic::{AtomicU64, Ordering},
    time::{self, Duration, SystemTime, UNIX_EPOCH},
    vec,
};

use ::time::OffsetDateTime;
use chrono::{DateTime, Local, Utc};
use cli::Cli;
use clickhouse::{Client, Row};
use rand::{rngs::ThreadRng, Rng};
use serde::{Deserialize, Serialize};
use structopt::StructOpt;
use tokio::{process::Command, signal};

static TOTAL: AtomicU64 = AtomicU64::new(0);
static START: AtomicU64 = AtomicU64::new(0);
static COMMIT: AtomicU64 = AtomicU64::new(0);

pub mod cli;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // 初始化日志
    let cli = Cli::from_args();

    let print = cli.print.unwrap_or(5);
    let batch = cli.batch.unwrap_or(50000);
    let host = cli.host.unwrap_or("192.168.180.217".to_string());
    let port = cli.port.unwrap_or(8123);
    let user = cli.user.unwrap_or("default".to_string());
    let password = cli.password.unwrap_or("!default@123".to_string());
    println!("pid:{},process starting... ", std::process::id());

    // debug!(
    //     "host: {}, port: {}, user: {}, password: {}, print: {}, batch: {}",
    //     host, port, user, password, print, batch
    // );
    let url = format!("http://{}:{}", host, port);
    let client = Client::default()
        .with_url(url)
        .with_user(user)
        .with_password(password)
        .with_database("ws_dwd");

    startStatics(print).await?;

    tokio::select! {
        _ = startOutput(client, batch) => {
            println!("\nckd exiting...");
        }
        _ = signal::ctrl_c() => {
            println!("\nctrl-c received, exiting...");
        }
    }
    Ok(())
}

thread_local! {
    static RNG: RefCell<ThreadRng> = RefCell::new(rand::thread_rng());
}

pub async fn get_system_usage() -> (f32, f32) {
    let os = std::env::consts::OS;
    let mut memory_usage: f32 = 0.0;
    let mut cpu_percent: f32 = 0.0;
    if os.eq_ignore_ascii_case("windows") {
        memory_usage = get_memory_info_on_windows().await;
        cpu_percent = get_cpu_info_on_windows().await;
    } else {
        memory_usage = get_memory_info_on_linux().await;
        cpu_percent = get_cpu_info_on_linux().await;
    }

    return (memory_usage, cpu_percent);
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

/// 统计信息
pub async fn startStatics(print: usize) -> Result<(), Box<dyn Error>> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(print as u64)).await;
            let (memory_usage, cpu_percent) = get_system_usage().await;
            let total = TOTAL.load(Ordering::SeqCst);
            if total == 0 {
                continue;
            }
            let commit = COMMIT.load(Ordering::SeqCst);
            let start = START.load(Ordering::SeqCst);
            let now = get_current_date();
            let time = now - start;
            let tps = total / time;

            print!(
                "\rTotal: {}, Commit: {}, TPS: {}, Memory Usage: {:.2} KB, Cpu Usage: {:.2}%",
                total, commit, tps, memory_usage, cpu_percent
            );
            std::io::stdout().flush().unwrap();
        }
    });

    Ok(())
}

pub async fn startOutput(client: Client, batch: usize) -> Result<(), Box<dyn Error>> {
    START.store(get_current_date(), Ordering::SeqCst);

    tokio::spawn(async move {
        loop {
            let mut inserter = client
                .inserter("dwd_standard_ip_session_all")
                .unwrap()
                .with_timeouts(Some(Duration::from_secs(5)), Some(Duration::from_secs(20)))
                .with_max_entries(750_000)
                .with_period(Some(Duration::from_secs(15)));
            for i in 1..batch {
                let src_ip = generate_ip();
                let dst_ip = generate_ip();

                let date = get_current_date().try_into().unwrap();
                let data = IpSessionRow {
                    src_ip,
                    dst_ip,
                    device_id: 303510315976552,
                    customer_id: 300810286461824,
                    source_id: String::from("303510315976552_2"),
                    protocol: 3342,
                    protocol_ip: 700,
                    src_port: vec![80],
                    dst_port: vec![752],
                    send_traffic: 35672,
                    recv_traffic: i as u64,
                    end_time: date,
                    request_time: date,
                    src_ip_type: String::from("inside"),
                    dst_ip_type: String::from("inside"),
                    src_inner_network_ip: 1,
                    src_country_name: String::from("局域网"),
                    src_province_name: String::new(),
                    src_city_name: String::new(),
                    src_ip_longitude_latitude: vec![0.0, 0.0],
                    dst_inner_network_ip: 1,
                    dst_country_name: String::from("局域网"),
                    dst_province_name: String::new(),
                    dst_city_name: String::new(),
                    dst_ip_longitude_latitude: vec![0.0, 0.0],
                    create_time: date,
                };
                inserter.write(&data).await.unwrap();
            }
            inserter.commit().await.unwrap();
            TOTAL.fetch_add(batch as u64, Ordering::SeqCst);
            COMMIT.fetch_add(1, Ordering::SeqCst);
            inserter.end().await.unwrap();

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await?;
    Ok(())
}

#[derive(Row, Serialize)]
struct MyRow {
    num: u32,
    row_name: String,
    time: String,
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
#[derive(Row, Serialize, Debug, Deserialize)]
pub struct IpSessionRow {
    src_ip: String,
    dst_ip: String,
    device_id: u64,
    customer_id: u64,
    source_id: String,
    protocol: u32,
    protocol_ip: u32,
    src_port: Vec<u32>,
    dst_port: Vec<u32>,
    send_traffic: u64,
    recv_traffic: u64,
    end_time: u32,
    request_time: u32,
    src_ip_type: String,
    dst_ip_type: String,
    src_inner_network_ip: u8,
    src_country_name: String,
    src_province_name: String,
    src_city_name: String,
    src_ip_longitude_latitude: Vec<f32>,

    dst_inner_network_ip: u8,
    dst_country_name: String,
    dst_province_name: String,
    dst_city_name: String,
    dst_ip_longitude_latitude: Vec<f32>,
    create_time: u32,
}
