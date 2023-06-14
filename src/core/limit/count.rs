use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, SystemTime},
};

/// 计数器 限流器
pub struct CounterLimiter {
    pub window_size: Duration,

    pub limit: usize,

    pub count: AtomicUsize,

    pub start: SystemTime,
}

impl CounterLimiter {
    pub fn new(size: Duration, limit: usize) -> Self {
        Self {
            window_size: size,
            limit,
            count: AtomicUsize::new(0),
            start: SystemTime::now(),
        }
    }

    fn try_clear_windows(&mut self) {
        let duration = self.start.elapsed().unwrap();

        if duration >= self.window_size {
            self.count.store(0, Ordering::SeqCst);
            self.start = SystemTime::now();
        }
    }

    pub async fn try_acquire(&mut self) -> bool {
        self.try_clear_windows();

        if self.count.load(Ordering::SeqCst) < self.limit {
            self.count.fetch_add(1, Ordering::SeqCst);
            return true;
        }
        return false;
    }

    pub async fn acquire(&mut self) {
        loop {
            self.try_clear_windows();

            if self.count.load(Ordering::SeqCst) < self.limit {
                self.count.fetch_add(1, Ordering::SeqCst);
                break;
            }

            tokio::time::sleep(self.window_size).await;
        }
    }
}

#[cfg(test)]
mod tests {

    use tokio_test::block_on;

    use super::*;

    #[test]
    fn test_count_limiter() {
        block_on(async {
            let mut counter = CounterLimiter::new(Duration::from_secs(1), 20);

            let mut count = 0;

            for _i in 0..50 {
                if counter.try_acquire().await {
                    count += 1;
                }
            }
            println!("50次请求中通过：{},限流：{}", count, 50 - count);

            tokio::time::sleep(Duration::from_secs(1)).await;

            let start = SystemTime::now();

            count = 0;
            for _i in 0..100 {
                counter.acquire().await;
                count += 1;
            }
            let cosume = start.elapsed().unwrap().as_millis();
            assert!(cosume <= 5000);
            println!("100次请求中花费：{}ms", cosume);
        });
    }
}
