use std::{
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use tokio::sync::{
    mpsc::{Receiver, Sender},
    Mutex,
};

use crate::{
    core::{
        error::{Error, IoError},
        limit::token::TokenBuketLimiter,
        log::incr_log,
        traits::{Name, TaskDetailStatic},
    },
};

pub mod csv;
pub mod fake;
pub mod kafka;
pub mod mysql;

#[async_trait]
pub trait Exector: Send + Sync + TaskDetailStatic + Name {
    fn next(&self) -> usize {
        0
    }

    fn limiter(&mut self) -> Option<&mut Arc<Mutex<TokenBuketLimiter>>> {
        return None;
    }

    fn is_consumer(&mut self) -> bool {
        return self.sender().is_none() && self.receiver().is_some();
    }

    fn receiver(&mut self) -> Option<&mut Arc<Mutex<Receiver<serde_json::Value>>>> {
        return None;
    }

    fn sender(&mut self) -> Option<&mut Sender<serde_json::Value>> {
        return None;
    }

    fn count_rc(&self) -> Option<Arc<AtomicI64>> {
        return None;
    }

    fn is_multi_handle(&self) -> bool {
        return true;
    }

    async fn handle_batch(&mut self, _v: Vec<serde_json::Value>) -> crate::Result<()> {
        Ok(())
    }

    async fn handle_single(&mut self, _v: &mut serde_json::Value) -> crate::Result<()> {
        Ok(())
    }
    async fn handle_fetch(&mut self) -> crate::Result<Vec<serde_json::Value>> {
        Ok(vec![])
    }

    async fn fetch_batch(&mut self) -> crate::Result<bool> {
        let datas = self.handle_fetch().await?;
        if datas.len() == 0 {
            return Ok(true);
        }
        let datas = datas.clone();

        match self.sender() {
            Some(sender) => {
                for data in datas {
                    sender.send(data).await?;
                }
                Ok(false)
            }
            None => Ok(true),
        }
    }
    ///1. 获取令牌
    ///2. 检查count是否为0
    ///3. 制造数据
    async fn add_batch(&mut self) -> crate::Result<bool> {
        let batch = self.batch().await;

        let mut num = 0;
        let mut arr = Vec::with_capacity(batch + batch / 4);
        for _i in 0..batch {
            if !self.is_multi_handle() {
                if let Some(limiter) = self.limiter() {
                    // 获取令牌
                    limiter.lock().await.acquire().await;
                }
            }

            if let Some(count) = self.count_rc() {
                if count.load(Ordering::SeqCst) <= 0 {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    break;
                }
            }
            match self.receiver() {
                Some(receiver) => {
                    let data = receiver.lock().await.recv().await;

                    if let Some(mut data) = data {
                        if !self.is_multi_handle() {
                            self.handle_single(&mut data).await?;
                            num += 1;
                            if let Some(count) = self.count_rc() {
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
                }
                None => return Ok(true),
            }
        }
        if arr.len() == 0 {
            match self.count_rc() {
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

            if let Some(count) = self.count_rc() {
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
        match self.count_rc() {
            Some(count) => match count.load(Ordering::SeqCst) {
                x if x <= 0 => Ok(true),
                _ => Ok(false),
            },
            None => Ok(false),
        }
    }

    async fn execute(&mut self) -> crate::Result<bool> {
        if self.is_consumer() {
            return self.add_batch().await;
        } else {
            return self.fetch_batch().await;
        }
    }
}
