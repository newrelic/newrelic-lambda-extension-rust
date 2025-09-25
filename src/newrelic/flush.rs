use async_trait::async_trait;
use std::io::Result;

/// A trait for objects that can be flushed.
#[async_trait]
pub trait Flush: Send + Sync {
    /// Flushes any buffered data.
    async fn flush(&self) -> Result<()>;
    /// Flushes all data before shutdown.
    #[allow(dead_code)]
    async fn final_flush(&self) -> Result<()>;
}

