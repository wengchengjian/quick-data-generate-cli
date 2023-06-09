use std::{
    collections::HashMap,
    io::Write,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use crate::util::{get_current_date, get_system_usage};

#[derive(Debug)]
pub struct StaticsLog {
    name: String,

    total: AtomicU64,

    commit: AtomicU64,

    start: AtomicU64,

    interval: usize,

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

    /// 注册一个日志任务
    pub fn register(&mut self, log: StaticsLog) {
        self.logs.entry(log.name).or_insert(log);
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
}

impl StaticsLogger {
    pub fn new(factory: StaticsLogFactory, interval: usize) -> Self {
        Self { factory, interval }
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

    pub async fn log(&self) {
        loop {
            for log in self.factory.logs.values() {
                log.print_log().await;
                println!();
            }
            tokio::time::sleep(Duration::from_secs(self.interval as u64)).await;
        }
    }
}

impl StaticsLog {
    pub fn new(name: String, interval: usize) -> Self {
        Self {
            total: AtomicU64::new(0),
            commit: AtomicU64::new(0),
            start: AtomicU64::new(get_current_date()),
            name,
            completed: false,
            interval,
        }
    }

    pub async fn print_log(&self) {
        let total = self.total();
        let commit = self.commit();
        let start = self.start();
        let now = get_current_date();
        let time = now - start;
        let tps = match total {
            0 => 0,
            _ => total / time,
        };

        let (memory_usage, cpu_percent) = get_system_usage().await;

        print!(
            "\rTotal: {}, Commit: {}, TPS: {}, Memory Usage: {:.2} KB, Cpu Usage: {:.2}%",
            total, commit, tps, memory_usage, cpu_percent
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
