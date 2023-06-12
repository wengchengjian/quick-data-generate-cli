use crate::util::{format_memory, get_current_date, get_system_usage};
use std::{
    collections::HashMap,
    io::Write,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::Duration,
};
use tokio::sync::Mutex;

lazy_static! {
    pub static ref STATICS_LOGGER: Mutex<StaticsLogger> = Mutex::new(StaticsLogger::new(0));
}

#[derive(Debug)]
pub struct StaticsLog {
    name: String,

    total: AtomicU64,

    commit: AtomicU64,

    start: AtomicU64,

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
            let logger = STATICS_LOGGER.lock().await;
            if logger.shutdown.load(Ordering::SeqCst) {
                return;
            }

            for log in logger.factory.logs.values() {
                log.print_log().await;
            }

            let interval = logger.interval;
            drop(logger);

            tokio::time::sleep(Duration::from_secs(interval as u64)).await;
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
    let mut logger = STATICS_LOGGER.lock().await;
    logger.add_total(name, total);
    logger.add_commit(name, commit);
}

pub async fn register(name: &str) {
    let mut logger = STATICS_LOGGER.lock().await;
    logger.register(StaticsLog::new(name.to_string()));
}

impl StaticsLog {
    pub fn new(name: String) -> Self {
        Self {
            total: AtomicU64::new(0),
            commit: AtomicU64::new(0),
            start: AtomicU64::new(get_current_date()),
            name,
            completed: false,
        }
    }

    pub async fn print_log(&self) {
        let total = self.total();
        let commit = self.commit();
        let start = self.start();
        let name = &self.name;
        let now = get_current_date();
        let mut time = now - start;
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
            "\r{}---> Total: {}, Commit: {}, TPS: {}, Memory Usage: {}, Cpu Usage: {:.2}%",
            name, total, commit, tps, memory_usage, cpu_percent
        );
        std::io::stdout().flush().unwrap();
    }

    pub fn total(&self) -> u64 {
        self.total.load(Ordering::SeqCst)
    }

    pub fn commit(&self) -> u64 {
        self.commit.load(Ordering::SeqCst)
    }

    pub fn start(&self) -> u64 {
        self.start.load(Ordering::SeqCst)
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
