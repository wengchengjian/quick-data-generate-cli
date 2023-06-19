use std::{
    io::Cursor,
    path::PathBuf,
    sync::{atomic::AtomicI64, Arc},
};

use super::Exector;
use crate::{
    core::{error::Error, limit::token::TokenBuketLimiter, watch::StopWatch},
    model::column::OutputColumn,
};
use async_trait::async_trait;
use bytes::Buf;
use tokio::{fs::File, io::BufWriter, sync::Mutex};
use tokio::{fs::OpenOptions, io::AsyncWriteExt};

#[derive(Debug, Clone)]
pub struct CsvTaskExecutor {
    pub filename: String,
    pub batch: usize,
    pub limiter: Option<Arc<Mutex<TokenBuketLimiter>>>,
    pub count: Option<Arc<AtomicI64>>,
    pub columns: Vec<OutputColumn>,
    pub task_name: String,
}

impl CsvTaskExecutor {
    pub fn new(
        filename: String,
        batch: usize,
        count: Option<Arc<AtomicI64>>,
        columns: Vec<OutputColumn>,
        task_name: String,
        limiter: Option<Arc<Mutex<TokenBuketLimiter>>>,
    ) -> Self {
        Self {
            filename,
            batch,
            columns,
            count,
            task_name,
            limiter,
        }
    }
    pub fn get_columns_name(&self) -> String {
        let mut columns_name = String::new();
        for column in &self.columns {
            columns_name.push_str(&column.name());
            columns_name.push_str(",");
        }
        columns_name.pop();
        columns_name
    }

    fn replace_val(&self, header: String, key: &str, val: &serde_json::Value) -> String {
        return header.replace(key, val.to_string().as_str());
    }
}

#[async_trait]
impl Exector for CsvTaskExecutor {
    fn batch(&self) -> usize {
        return self.batch;
    }
    fn columns(&self) -> &Vec<OutputColumn> {
        return &self.columns;
    }

    fn limiter(&mut self) -> Option<&mut Arc<Mutex<TokenBuketLimiter>>> {
        return self.limiter.as_mut();
    }

    fn count(&mut self) -> Option<&Arc<AtomicI64>> {
        return self.count.as_ref();
    }

    fn name(&self) -> &str {
        return &self.task_name;
    }

    fn is_multi_handle(&self) -> bool {
        return true;
    }

    async fn handle_batch(&mut self, vals: Vec<serde_json::Value>) -> crate::Result<()> {
        let column_names = self.get_columns_name();

        let mut insert_header = String::new();

        for val in vals {
            let mut name_vals = column_names.clone();

            let fake_data = val.as_object().expect("错误的数据类型");
            for (key, val) in fake_data {
                name_vals = self.replace_val(name_vals, key, &val);
            }
            insert_header.push_str(&name_vals);
            insert_header.push('\n');
        }
        //        watch.stop();
        insert_header.pop();
        let path = PathBuf::from(&self.filename);

        let mut option = OpenOptions::new();
        option.write(true).append(true);
        let file = option.open(path).await.unwrap();
        let mut writer = BufWriter::new(file);
        let mut buffer = Cursor::new(insert_header.as_bytes());
        while buffer.has_remaining() {
            writer.write_buf(&mut buffer).await.unwrap();
        }
        writer.flush().await.unwrap();

        Ok(())
    }
}
