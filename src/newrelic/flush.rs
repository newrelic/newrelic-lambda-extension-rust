use async_trait::async_trait;
use std::io::Result as IoResult;

/// A trait for objects that can be flushed.
#[async_trait]
pub trait Flush: Send + Sync {
    /// Flushes any buffered data.
    async fn flush(&self) -> IoResult<()>;
    /// Flushes all data before shutdown.
    async fn final_flush(&self) -> IoResult<()>;
}


