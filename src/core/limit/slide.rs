use std::{
    cmp::{max, min},
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, SystemTime},
};

pub struct SlideWindowLimiter {
    /// 窗口大小
    pub window_size: Duration,
    /// 限流大小
    pub limit: usize,
    /// 开始时间
    pub start: SystemTime,
    /// 当前小窗口的索引
    pub index: usize,
    /// 小窗口计数数组    
    pub counters: Vec<AtomicUsize>,
    /// 窗口数量
    pub num: usize,
    ////当前请求总数
    // pub total_count: AtomicUsize,
}

impl SlideWindowLimiter {
    fn new(window_size: Duration, limit: usize, num: usize) -> Self {
        let mut counters = Vec::new();
        for _ in 0..num {
            counters.push(AtomicUsize::new(0));
        }
        Self {
            window_size,
            limit,
            start: SystemTime::now(),
            counters,
            num,
            index: 0,
            // total_count: AtomicUsize::new(0),
        }
    }

    pub async fn try_acquire(&mut self) -> bool {
        // 滑动窗口
        self.slide_window(self.get_slide_window_size());

        let count = self.get_total_count();
        if count >= self.limit {
            return false;
        } else {
            self.counters[self.index].fetch_add(1, Ordering::SeqCst);
            // self.total_count.fetch_add(1, Ordering::SeqCst);
            return true;
        }
    }

    pub async fn acquire(&mut self) {
        loop {
            // 滑动窗口
            self.slide_window(self.get_slide_window_size());

            let count = self.get_total_count();
            if count < self.limit {
                self.counters[self.index].fetch_add(1, Ordering::SeqCst);
                // self.total_count.fetch_add(1, Ordering::SeqCst);
                break;
            }
            // 阻塞一个小窗口的时间
            let min_window_size = (self.window_size.as_millis() as usize / self.num) as u64;
            tokio::time::sleep(Duration::from_millis(min_window_size)).await;
        }
    }
    /// 获取当前的请求总数
    pub fn get_total_count(&self) -> usize {
        self.counters.iter().map(|t| t.load(Ordering::SeqCst)).sum()
        // return self.total_count.load(Ordering::SeqCst);
    }

    /// 获取需要滑动窗口的大小
    fn get_slide_window_size(&self) -> usize {
        return (max(
            self.start
                .elapsed()
                .unwrap()
                .checked_sub(self.window_size)
                .unwrap_or(Duration::from_millis(0))
                .as_millis(),
            0,
        ) as usize
            / (self.window_size.as_millis() as usize / self.num)) as usize;
    }

    /// 滑动几个窗口
    fn slide_window(&mut self, num: usize) {
        if num == 0 {
            return;
        }

        let min_window_num = min(num, self.num);

        for _ in 0..min_window_num {
            // 减去总数
            // self.total_count.fetch_sub(
            //     self.counters[self.index].load(Ordering::SeqCst),
            //     Ordering::SeqCst,
            // );

            self.counters[self.index] = AtomicUsize::new(0);

            self.index = (self.index + 1) % self.num;
        }

        self.start = SystemTime::now();
    }
}

#[cfg(test)]
mod tests {
    use tokio_test::block_on;

    use super::*;

    #[test]
    fn test_slide_window() {
        block_on(async {
            let mut limiter = SlideWindowLimiter::new(Duration::from_secs(1), 20, 10);
            let mut count = 0;

            for _i in 0..50 {
                if limiter.try_acquire().await {
                    count += 1;
                }
            }
            println!("50次请求中通过：{},限流：{}", count, 50 - count);

            tokio::time::sleep(Duration::from_secs(1)).await;

            let start = SystemTime::now();

            count = 0;
            for _i in 0..100 {
                limiter.acquire().await;
                count += 1;
            }
            let cosume = start.elapsed().unwrap().as_millis();
            assert!(cosume <= 5000);
            println!("1000次请求中花费：{}ms", cosume);
        });
    }
}
