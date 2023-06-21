use std::{
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::{
    core::{
        error::{Error, IoError},
        fake::get_fake_data,
        limit::token::TokenBuketLimiter,
        log::incr_log,
    },
    model::column::OutputColumn,
};

pub mod csv;
pub mod kafka;
pub mod mysql;

#[async_trait]
pub trait Exector: Send + Sync {
    fn batch(&self) -> usize;

    fn limiter(&mut self) -> Option<&mut Arc<Mutex<TokenBuketLimiter>>>;

    fn count(&mut self) -> Option<&Arc<AtomicI64>>;

    fn columns(&self) -> &Vec<OutputColumn>;

    fn name(&self) -> &str;

    fn is_multi_handle(&self) -> bool {
        return true;
    }

    async fn handle_batch(&mut self, _v: Vec<serde_json::Value>) -> crate::Result<()> {
        Ok(())
    }

    async fn handle_single(&mut self, _v: &mut serde_json::Value) -> crate::Result<()> {
        Ok(())
    }

    ///1. 获取令牌
    ///2. 检查count是否为0
    ///3. 制造数据
    ///4.
    async fn add_batch(&mut self) -> crate::Result<bool> {
        // let mut watch = StopWatch::new();
        let mut num = 0;
        let mut arr = Vec::with_capacity(self.batch() + self.batch() / 4);
        for _i in 0..self.batch() {
            if !self.is_multi_handle() {
                if let Some(limiter) = self.limiter() {
                    // 获取令牌
                    limiter.lock().await.acquire().await;
                }
            }

            if let Some(count) = self.count() {
                if count.load(Ordering::SeqCst) <= 0 {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    break;
                }
            }
            let mut data = get_fake_data(self.columns());

            if !self.is_multi_handle() {
                self.handle_single(&mut data).await?;
                num += 1;
                if let Some(count) = self.count() {
                    count.fetch_sub(1, Ordering::SeqCst);
                }
                if num % 100 == 0 {
                    incr_log(&self.name(), num, 1).await;
                    num = 0;
                }
            } else {
                arr.push(data);
            }
        }
        if arr.len() == 0 {
            match self.count() {
                Some(count) => match count.load(Ordering::SeqCst) {
                    x if x <= 0 => return Ok(true),
                    _ => {
                        return Err(Error::Io(IoError::DataAccessError("no data".to_string())));
                    }
                },
                None => {
                    return Err(Error::Io(IoError::DataAccessError("no data".to_string())));
                }
            };
        }

        if self.is_multi_handle() {
            if let Some(limiter) = self.limiter() {
                // 获取令牌
                limiter.lock().await.acquire_num(arr.len()).await;
            }

            if let Some(count) = self.count() {
                count.fetch_sub(arr.len() as i64, Ordering::SeqCst);
            }

            num += arr.len();

            self.handle_batch(arr).await?;
        }
        // watch.start("日志记录");
        if num != 0 {
            incr_log(&self.name(), num, 1).await;
        }
        // watch.stop();
        // watch.print_all_task_mils();
        match self.count() {
            Some(count) => match count.load(Ordering::SeqCst) {
                x if x <= 0 => Ok(true),
                _ => Ok(false),
            },
            None => Ok(false),
        }
    }
}
