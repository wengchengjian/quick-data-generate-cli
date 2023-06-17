use crate::util::{format_memory, get_system_usage};
use std::{
    collections::HashMap,
    io::Write,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::{Duration, SystemTime},
};


pub static mut STATICS_LOGGER: Option<StaticsLogger> = None;

#[derive(Debug)]
pub struct StaticsLog {
    name: String,

    total: AtomicU64,

    last_total: AtomicU64,

    commit: AtomicU64,

    start: SystemTime,

    run_time: SystemTime,

    last_run_time: SystemTime,

    completed: bool,
}


#[derive(Debug)]
pub struct StaticsLogFactory {
    logs: HashMap<String, StaticsLog>,
}

impl StaticsLogFactory {
    pub fn new() -> Self {
        Self {
            logs: HashMap::new(),
        }
    }

    pub fn add_total(&mut self, name: &str, total: usize) {
        let log = self
            .logs
            .entry(name.to_string())
            .or_insert(StaticsLog::new(name.to_string()));

        log.add_total(total as u64);
    }

    pub fn add_commit(&mut self, name: &str, commit: usize) {
        let log = self
            .logs
            .entry(name.to_string())
            .or_insert(StaticsLog::new(name.to_string()));

        log.add_commit(commit as u64);
    }

    /// 注册一个日志任务
    pub fn register(&mut self, log: StaticsLog) {
        let key = log.name.clone();
        self.logs.entry(key).or_insert(log);
    }

    /// 注销一个日志任务
    pub fn unregister(&mut self, name: &str) {
        self.logs.remove(name);
    }

    pub fn get(&self, name: &str) -> Option<&StaticsLog> {
        self.logs.get(name)
    }

    pub fn get_mut(&mut self, name: &str) -> Option<&mut StaticsLog> {
        self.logs.get_mut(name)
    }

    /// 记录完成的任务日志
    pub fn complete(&mut self, name: &str) {
        if let Some(log) = self.logs.get_mut(name) {
            log.completed = true;
        }
    }
}

#[derive(Debug)]
pub struct StaticsLogger {
    interval: usize,
    factory: StaticsLogFactory,
    shutdown: AtomicBool,
}

impl StaticsLogger {
    pub fn new(interval: usize) -> Self {
        Self {
            factory: StaticsLogFactory::new(),
            interval,
            shutdown: AtomicBool::new(false),
        }
    }

    pub async fn log() {
        println!("start logging tasks log...");
        loop {
            unsafe {
                let logger = STATICS_LOGGER.as_mut().unwrap();
                if logger.shutdown.load(Ordering::SeqCst) {
                    return;
                }

                for log in logger.factory.logs.values_mut() {
                    log.print_log().await;
                }

                let interval = (*logger).interval;
                drop(logger);

                tokio::time::sleep(Duration::from_secs(interval as u64)).await;
            }
        }
    }

    pub fn interval(&mut self, interval: usize) {
        self.interval = interval;
    }

    pub fn add_total(&mut self, name: &str, total: usize) {
        self.factory.add_total(name, total)
    }

    pub fn add_commit(&mut self, name: &str, commit: usize) {
        self.factory.add_commit(name, commit)
    }

    pub fn close(&self) -> crate::Result<()> {
        self.shutdown.store(true, Ordering::SeqCst);
        Ok(())
    }

    pub fn register(&mut self, log: StaticsLog) {
        self.factory.register(log);
    }

    pub fn unregister(&mut self, name: &str) {
        self.factory.unregister(name);
    }

    /// 记录完成的任务日志
    pub fn complete(&mut self, name: &str) {
        if let Some(log) = self.factory.logs.get_mut(name) {
            log.completed = true;
        }
    }
}

pub async fn incr_log(name: &str, total: usize, commit: usize) {
    unsafe {
        let logger = STATICS_LOGGER.as_mut().unwrap();
        logger.add_total(name, total);
        logger.add_commit(name, commit);
    }
}

pub async fn register(name: &str) {
    unsafe {
        let logger = STATICS_LOGGER.as_mut().unwrap();

        logger.register(StaticsLog::new(name.to_string()));
    }
}

impl StaticsLog {
    pub fn new(name: String) -> Self {
        Self {
            run_time: SystemTime::now(),
            last_run_time: SystemTime::now(),
            last_total: AtomicU64::new(0),
            total: AtomicU64::new(0),
            commit: AtomicU64::new(0),
            start: SystemTime::now(),
            name,
            completed: false,
        }
    }

    pub async fn print_log(&mut self) {
        self.run_time = SystemTime::now();

        let total = self.total() - self.last_total.load(Ordering::SeqCst);
        let name = &self.name;
        let mut time = self
            .run_time
            .duration_since(self.last_run_time)
            .unwrap()
            .as_secs();

        if time == 0 {
            time = 1;
        }

        let tps = match total {
            0 => 0,
            _ => total / time,
        };

        let (memory_usage, cpu_percent) = get_system_usage().await;

        let memory_usage = format_memory(memory_usage);
        println!(
            "\r{}---> Total: {}, Commit: {}, Tps: {}, Spend: {}s, Memory Usage: {}, Cpu Usage: {:.2}%",
            name, self.total(), self.commit(), tps, self.start.elapsed().unwrap().as_secs(), memory_usage, cpu_percent
        );
        self.last_total.store(self.total(), Ordering::SeqCst);
        self.last_run_time = self.run_time.clone();

        std::io::stdout().flush().unwrap();
    }

    pub fn total(&self) -> u64 {
        self.total.load(Ordering::SeqCst)
    }

    pub fn commit(&self) -> u64 {
        self.commit.load(Ordering::SeqCst)
    }

    pub fn start(&self) -> &SystemTime {
        &self.start
    }

    pub fn add_total(&self, val: u64) {
        self.total.fetch_add(val, Ordering::SeqCst);
    }

    pub fn add_commit(&self, val: u64) {
        self.commit.fetch_add(val, Ordering::SeqCst);
    }
}

#[derive(Debug)]
pub struct ChannelStaticsLog {
    pub name: String,

    pub total: usize,

    pub commit: usize,

    pub completed: bool,
}

impl ChannelStaticsLog {
    pub fn new(total: usize, commit: usize) -> Self {
        Self {
            name: "Default".into(),
            total,
            commit,
            completed: false,
        }
    }
}
