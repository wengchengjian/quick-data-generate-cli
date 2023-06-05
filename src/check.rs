/// 单次默认提交数量
pub static DEFAULT_BATCH_SIZE: usize = 5_0000;

/// 单次最大提交数量
pub static MAX_BATCH_SIZE: usize = 10_0000;

/// 日志最小打印间隔
pub static MIN_BATCH_SIZE: usize = 1_000;

/// 日志最大打印间隔
pub static MAX_INTERVAL: usize = 60;

/// 日志最大打印间隔
pub static DEFAULT_INTERVAL: usize = 5;

/// 日志最大打印间隔
pub static MIN_INTERVAL: usize = 1;

// 最小任务执行线程数
pub static MIN_THREAD_SIZE: usize = 1;

/// 检查命令行参数，填充默认值
pub fn check_args(cli: &mut super::Cli) {
    // 检查配置的线程数，建议不超过cpu核心数
    check_thread_num(cli);
    // 检查单次批量提交的数量
    check_batch_size(cli);
    // 检查日志打印间隔
    check_interval(cli);
}

pub fn check_interval(cli: &mut super::Cli) {
    let mut interval = cli.interval;
    match interval {
        Some(val) => {
            if val < MIN_INTERVAL {
                println!("interval must greater than {MIN_INTERVAL}");
                interval.insert(MIN_INTERVAL);
            }

            if val > MAX_INTERVAL {
                println!("interval must less than {MAX_INTERVAL}");
                interval.insert(MAX_INTERVAL);
            }
        }
        None => {
            interval.insert(DEFAULT_INTERVAL);
        }
    }
}

pub fn check_thread_num(cli: &mut super::Cli) {
    let mut num_threads = num_cpus::get();

    let mut th = cli.concurrency;
    match th {
        Some(val) => {
            if val < MIN_THREAD_SIZE {
                println!("threads must greater than {MIN_THREAD_SIZE}");
                th.insert(num_threads);
            }

            if val > num_threads {
                println!("threads must less than or equal to the number of cores: {num_threads}");
                th.insert(num_threads);
            }
        }
        None => {
            th.insert(num_threads);
        }
    }
}

pub fn check_batch_size(cli: &mut super::Cli) {
    let mut batch = cli.batch;
    match batch {
        Some(val) => {
            if val < MIN_BATCH_SIZE {
                println!("batch size must greater than {MIN_BATCH_SIZE}");
                batch.insert(MIN_BATCH_SIZE);
            }

            if val > MAX_BATCH_SIZE {
                println!("batch size must less than {MAX_BATCH_SIZE}");
                batch.insert(MAX_BATCH_SIZE);
            }
        }
        None => {
            batch.insert(DEFAULT_BATCH_SIZE);
        }
    }
}
