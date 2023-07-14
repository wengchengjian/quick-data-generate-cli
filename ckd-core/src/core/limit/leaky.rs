use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, SystemTime},
};

pub struct LeakyBuketLimiter {
    pub rate: usize,

    pub burst: usize,

    pub start: SystemTime,

    pub water: AtomicUsize,
}

impl LeakyBuketLimiter {
    pub fn new(rate: usize, burst: usize) -> Self {
        Self {
            rate,
            burst,
            start: SystemTime::now(),
            water: AtomicUsize::new(0),
        }
    }

    fn refresh_water(&mut self) {
        let consum = self.start.elapsed().unwrap().as_millis();

        let sub = self.water.load(Ordering::SeqCst) as isize
            - ((consum as usize * self.rate) / 1000) as isize;

        if sub > 0 {
            self.water.store(sub as usize, Ordering::SeqCst);
        } else {
            self.water.store(0, Ordering::SeqCst);
        }
        self.start = SystemTime::now();
    }

    pub async fn try_acquire(&mut self) -> bool {
        self.refresh_water();
        if self.water.load(Ordering::SeqCst) < self.burst {
            self.water.fetch_add(1, Ordering::SeqCst);
            return true;
        }
        return false;
    }

    pub async fn acquire(&mut self) {
        loop {
            self.refresh_water();
            if self.water.load(Ordering::SeqCst) < self.burst {
                self.water.fetch_add(1, Ordering::SeqCst);
                break;
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

#[cfg(test)]
mod tests {

    use tokio_test::block_on;

    use super::*;

    #[test]
    fn test_leaky_burst() {
        block_on(async {
            let mut limiter = LeakyBuketLimiter::new(20, 50);
            let mut count = 0;

            for _i in 0..100 {
                if limiter.try_acquire().await {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    count += 1;
                }
            }
            println!("100次请求中通过：{},限流：{}", count, 100 - count);

            tokio::time::sleep(Duration::from_secs(1)).await;

            let start = SystemTime::now();
            count = 0;
            for _i in 0..100 {
                limiter.acquire().await;
                count += 1;
            }
            let cosume = start.elapsed().unwrap().as_millis();
            assert!(cosume <= 5000);
            println!("100次请求中花费：{}ms", cosume);
        });
    }
}
