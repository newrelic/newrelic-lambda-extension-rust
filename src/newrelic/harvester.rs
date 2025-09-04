use crate::newrelic::flush::Flush;
use std::{io::Result, sync::Arc, time::Duration};
use tracing::info;

/// The Harvester is responsible for periodically flushing data from processors.
pub struct Harvester {
    processors: Vec<Arc<dyn Flush>>,
    interval: Duration,
}

impl Harvester {
    /// Creates a new Harvester.
    pub fn new(processors: Vec<Arc<dyn Flush>>, interval: Duration) -> Self {
        Self {
            processors,
            interval,
        }
    }

    /// Runs the harvester loop, periodically flushing all processors.
    pub async fn run(&self) {
        let mut interval = tokio::time::interval(self.interval);
        tracing::info!("[Harvester] Starting harvester with interval: {:?}", self.interval);
        
        loop {
            interval.tick().await;
            tracing::info!("[Harvester] Flushing all {} processors", self.processors.len());
            for (index, p) in self.processors.iter().enumerate() {
                tracing::debug!("[Harvester] Flushing processor {}", index);
                if let Err(e) = p.flush().await {
                    tracing::error!("[Harvester] Error flushing processor {}: {}", index, e);
                }
            }
            tracing::debug!("[Harvester] Completed flush cycle");
        }
    }

    /// Performs a final flush of all processors.
    pub async fn final_flush(&self) -> Result<()> {
        info!("[Harvester] Performing final flush of all processors");
        for p in &self.processors {
            if let Err(e) = p.final_flush().await {
                tracing::error!("[Harvester] Error in final flush: {}", e);
            }
        }
        Ok(())
    }
}

