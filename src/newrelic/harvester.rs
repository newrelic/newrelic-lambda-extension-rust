use crate::newrelic::flush::Flush;
use crate::logs::processor::LogProcessor;
use crate::platform::processor::PlatformProcessor;
use std::{io::Result, sync::Arc, time::Duration};
use tracing::{debug, error, trace, warn};

/// The Harvester is responsible for periodically flushing data from processors.
pub struct Harvester {
    processors: Vec<Arc<dyn Flush>>,
    log_processor: Arc<LogProcessor>,
    platform_processor: Arc<PlatformProcessor>,
    interval: Duration,
}

impl Harvester {
    /// Creates a new Harvester with concrete processor references for actual sending.
    pub fn new(
        processors: Vec<Arc<dyn Flush>>, 
        interval: Duration, 
        log_processor: Arc<LogProcessor>, 
        platform_processor: Arc<PlatformProcessor>
    ) -> Self {
        Self {
            processors,
            log_processor,
            platform_processor,
            interval,
        }
    }

    /// Runs the harvester loop, periodically flushing all processors.
    pub async fn run(&self) {
        let mut interval = tokio::time::interval(self.interval);
        debug!("Starting harvester with interval: {:?}", self.interval);
        let mut flush_cycle_count = 0;
        
        loop {
            interval.tick().await;
            flush_cycle_count += 1;
            
            // Only log every 10th flush cycle to reduce noise
            if flush_cycle_count % 10 == 1 {
                trace!("Flushing all {} processors + log/platform processors (cycle: {})", self.processors.len(), flush_cycle_count);
            }
            
            let mut error_count = 0;
            
            // Flush generic processors
            for (index, p) in self.processors.iter().enumerate() {
                if let Err(e) = p.flush().await {
                    error!("Error flushing processor {}: {}", index, e);
                    error_count += 1;
                }
            }
            
            // Flush log processor (this will send accumulated logs)
            if let Err(e) = self.log_processor.flush().await {
                error!("Error flushing log processor: {}", e);
                error_count += 1;
            }
            
            // Flush platform processor (this will send accumulated platform events)
            if let Err(e) = self.platform_processor.flush().await {
                error!("Error flushing platform processor: {}", e);
                error_count += 1;
            }
            
            // Only log completion if there were errors or every 10th cycle
            if error_count > 0 {
                warn!("Completed flush cycle {} with {} errors", flush_cycle_count, error_count);
            } else if flush_cycle_count % 10 == 1 {
                debug!("Completed flush cycle {} successfully (flushed logs and platform events)", flush_cycle_count);
            }
        }
    }

    /// Performs a final flush of all processors.
    #[allow(dead_code)]
    pub async fn final_flush(&self) -> Result<()> {
        debug!("Performing final flush of all processors");
        let mut error_count = 0;
        for p in &self.processors {
            if let Err(e) = p.final_flush().await {
                error!("Error in final flush: {}", e);
                error_count += 1;
            }
        }
        if error_count == 0 {
            debug!("Final flush completed successfully");
        } else {
            warn!("Final flush completed with {} errors", error_count);
        }
        Ok(())
    }
}

impl std::fmt::Debug for Harvester {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Harvester")
            .field("processor_count", &self.processors.len())
            .field("interval", &self.interval)
            .finish()
    }
}

