use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, SystemTime},
};

#[derive(Debug)]
pub struct TokenBuketLimiter {
    /// 生成令牌的速度
    pub rate: usize,
    /// 通大小
    pub burst: usize,
    /// 开始时间
    pub start: SystemTime,
    /// 当前桶令牌数量
    pub token: AtomicUsize,
}

impl TokenBuketLimiter {
    pub fn new(rate: usize, burst: usize) -> Self {
        Self {
            rate,
            burst,
            start: SystemTime::now(),
            token: AtomicUsize::new(burst),
        }
    }

    fn refresh_token(&mut self) {
        let consume = self.start.elapsed().unwrap().as_millis();

        let add = self.token.load(Ordering::SeqCst) + (consume as usize * self.rate) / 1000;

        if add == 0 {
            return;
        }
        if add >= self.burst {
            self.token.store(self.burst, Ordering::SeqCst);
        } else {
            self.token.store(add, Ordering::SeqCst);
        }
        self.start = SystemTime::now();
    }

    pub async fn try_acquire(&mut self) -> bool {
        self.refresh_token();
        if self.token.load(Ordering::SeqCst) > 0 {
            self.token.fetch_sub(1, Ordering::SeqCst);
            return true;
        }
        return false;
    }

    pub async fn acquire(&mut self) {
        loop {
            self.refresh_token();
            if self.token.load(Ordering::SeqCst) > 0 {
                self.token.fetch_sub(1, Ordering::SeqCst);
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
            let mut limiter = TokenBuketLimiter::new(20, 50);
            let mut count = 0;

            for _i in 0..100 {
                if limiter.try_acquire().await {
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
