use async_trait::async_trait;
use std::io::Result as IoResult;

/// A trait for objects that can be flushed.
#[async_trait]
pub trait Flush: Send + Sync {
    /// Flushes any buffered data.
    async fn flush(&self) -> IoResult<()>;
    /// Flushes all data before shutdown.
    #[allow(dead_code)]
    async fn final_flush(&self) -> IoResult<()>;
}

use std::sync::Arc;
use crate::logs::processor::LogProcessor;
use crate::platform::processor::PlatformProcessor;

pub enum ProcessorType {
    LogProcessor(Arc<LogProcessor>),
    PlatformProcessor(Arc<PlatformProcessor>),
}

impl ProcessorType {
    pub async fn flush(&self) -> IoResult<()> {
        match self {
            ProcessorType::LogProcessor(p) => p.flush().await,
            ProcessorType::PlatformProcessor(p) => p.flush().await,
        }
    }
    pub async fn final_flush(&self) -> IoResult<()> {
        match self {
            ProcessorType::LogProcessor(p) => p.final_flush().await,
            ProcessorType::PlatformProcessor(p) => p.final_flush().await,
        }
    }
}
