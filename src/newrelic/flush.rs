use async_trait::async_trait;
use std::io::Result as IoResult;

#[async_trait]
pub trait Flush: Send + Sync {
    async fn flush(&self) -> IoResult<()>;
}


