use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    str::FromStr,
};

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};
use regex::Regex;

use crate::model::column::DataTypeEnum;

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
    return val.to_lowercase().eq("null");
}

pub fn is_string(_val: &str) -> bool {
    true
}

pub fn is_country(_val: &str) -> bool {
    true
}

pub fn is_city(_val: &str) -> bool {
    true
}

pub fn is_phone(val: &str) -> bool {
    PHONE_REGEX.is_match(val)
}

pub fn is_password(_val: &str) -> bool {
    true
}

pub fn is_username(_val: &str) -> bool {
    true
}

pub fn is_ipv4(val: &str) -> bool {
    Ipv4Addr::from_str(val).is_ok()
}

pub fn is_email(val: &str) -> bool {
    EMAIL_REGEX.is_match(val)
}

pub fn is_ipv6(val: &str) -> bool {
    Ipv6Addr::from_str(val).is_ok()
}

pub fn is_timestamp(val: &str) -> bool {
    let mut ok = false;
    for format in DATE_FORMATS {
        ok = DateTime::parse_from_str(val, format).is_ok()
    }

    ok
}

pub fn is_datetime(val: &str) -> bool {
    for format in DATE_FORMATS {
        match NaiveDateTime::parse_from_str(val, format) {
            Ok(_) => return true,
            Err(_) => continue,
        }
    }
    return false;
}
pub static DATE_FORMATS: [&'static str; 5] = [
    "%Y-%m-%d %H:%M:%S",
    "%y/%m/%d %H:%M",
    "%Y-%m-%d %H:%M:%S%.f",
    "%Y-%m-%dT%H:%M:%S%z",
    "%y/%m/%d %H:%M:%S",
];

lazy_static! {

    pub static ref EMAIL_REGEX: Regex =
        Regex::new(r"[\w!#$%&'*+/=?^_`{|}~-]+(?:\.[\w!#$%&'*+/=?^_`{|}~-]+)*@(?:[\w](?:[\w-]*[\w])?\.)+[\w](?:[\w-]*[\w])?").unwrap();

    pub static ref PHONE_REGEX: Regex = Regex::new(r"^1(3[0-9]|4[01456879]|5[0-35-9]|6[2567]|7[0-8]|8[0-9]|9[0-35-9])\d{8}").unwrap();
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_is_email() {
        let email = "473991883@qq.com";
        assert!(is_email(email))
    }

    #[test]
    fn test_is_ipv4() {
        let ip = "127.0.0.1";
        assert!(is_ipv4(ip))
    }

    #[test]
    fn test_is_ipv6() {
        let ipv6 = "2408:80f0:410c:1d:0:ff:b07a:39af";
        assert!(is_ipv6(ipv6))
    }

    #[test]
    fn test_is_null() {
        let val = "null";
        assert!(is_null(val))
    }

    #[test]
    fn test_is_phone() {
        let val = "15385936181";
        assert!(is_phone(val))
    }

    #[test]
    fn test_is_datetime() {
        let val = "2023-06-12 21:53:00";
        assert!(is_datetime(val))
    }
}
