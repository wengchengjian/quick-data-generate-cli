use std::net::{IpAddr, Ipv6Addr};

use chrono::{DateTime, NaiveTime, NaiveDate, NaiveDateTime};

use super::cli::Cli;

/// 单次默认提交数量
pub static DEFAULT_BATCH_SIZE: usize = 5_000;

/// 单次最大提交数量
pub static MAX_BATCH_SIZE: usize = 10_0000;

/// 单次最小提交数量
pub static MIN_BATCH_SIZE: usize = 1;

/// 日志最大打印间隔
pub static MAX_INTERVAL: usize = 60;

/// 日志最大打印间隔
pub static DEFAULT_INTERVAL: usize = 5;

/// 日志最大打印间隔
pub static MIN_INTERVAL: usize = 1;

// 最小任务执行线程数
pub static MIN_THREAD_SIZE: usize = 1;

/// 检查命令行参数，填充默认值
pub fn check_args(cli: &mut Cli) {
    // 检查配置的线程数，建议不超过cpu核心数
    check_thread_num(cli);
    // 检查单次批量提交的数量
    check_batch_size(cli);
    // 检查日志打印间隔
    check_interval(cli);
}

pub fn check_interval(cli: &mut Cli) {
    let interval = cli.interval;

    match interval {
        Some(interval) => {
            if interval < MIN_INTERVAL {
                println!("interval must greater than {MIN_INTERVAL}");
                let _ = cli.interval.insert(interval);
            }

            if interval > MAX_INTERVAL {
                println!("interval must less than {MAX_INTERVAL}");
                let _ = cli.interval.insert(MAX_INTERVAL);
            }
        }
        None => {
            let _ = cli.interval.insert(DEFAULT_INTERVAL);
        }
    }
}

pub fn check_thread_num(cli: &mut Cli) {
    // let num_threads = num_cpus::get();

    let th = cli.concurrency;
    match th {
        Some(val) => {
            if val < MIN_THREAD_SIZE {
                println!("threads must greater than {MIN_THREAD_SIZE}");
                cli.concurrency = Some(MIN_THREAD_SIZE);
            }

            // if val > num_threads {
            //     println!("threads must less than or equal to the number of cores: {num_threads}");
            //     cli.concurrency = Some(num_threads);
            // }
        }
        None => {
            cli.concurrency = Some(MIN_THREAD_SIZE);
        }
    }
}

pub fn check_batch_size(cli: &mut Cli) {
    let batch = cli.batch;
    match batch {
        Some(val) => {
            if val < MIN_BATCH_SIZE {
                println!("batch size must greater than {MIN_BATCH_SIZE}");
                cli.batch = Some(MIN_BATCH_SIZE);
            }

            if val > MAX_BATCH_SIZE {
                println!("batch size must less than {MAX_BATCH_SIZE}");
                cli.batch = Some(MAX_BATCH_SIZE);
            }
        }
        None => {
            cli.batch = Some(DEFAULT_BATCH_SIZE);
        }
    }
}


pub fn is_null(val: &str) -> bool {
    todo!()
}

pub fn is_string(val: &str) -> bool {
    todo!()
}

pub fn is_country(val: &str) -> bool {
    todo!()
}

pub fn is_city(val: &str) -> bool {
    todo!()
}

pub fn is_phone(val: &str) -> bool {
    todo!()
}

pub fn is_password(val: &str) -> bool {
    todo!()
}

pub fn is_username(val: &str) -> bool {
    todo!()
}

pub fn is_ipv4(val: &str) -> bool {
    todo!()
}

pub fn is_email(val: &str) -> bool {
    todo!()
}

pub fn is_ipv6(val: &str) -> bool {
    Ipv6Addr::from(val)
}

pub fn is_timestamp(val: &str) -> bool {
    let mut ok = false;
    for format in DATE_FORMATS {
        ok = DateTime::parse_from_str(val, format).is_ok()
    }

    ok
}

pub fn is_datetime(val: &str) -> bool {
    let mut ok = false;
    for format in DATE_FORMATS {
        ok = DateTime::parse_from_str(val, format).is_ok()
    }

    ok
}

pub static DATE_FORMATS: [&'static str; 1] = ["yyyy-MM-dd HH:mm:ss"];
