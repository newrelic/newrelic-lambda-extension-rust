use crate::newrelic::flush::Flush;
use crate::logs::processor::LogProcessor;
use crate::platform::processor::PlatformProcessor;
use std::{io::Result, sync::Arc, time::Duration};
use tracing::{debug, error, info, trace, warn};

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

    /// Final safety-net flush before freeze - most work done by request-specific flushing
    /// This catches any remaining telemetry that wasn't sent by the periodic request flushing
    pub async fn flush_before_freeze(&self, request_id: &str, _trace_collection_enabled: bool) -> std::io::Result<()> {
        info!("Harvester: Final safety-net flush before freeze for request {}", request_id);
        
        let mut error_count = 0;
        
        // Send any remaining logs (should be minimal due to request-specific flushing)
        if let Err(e) = self.log_processor.send_and_clear_batch_simple().await {
            error!("Harvester: Failed to send remaining logs before freeze: {}", e);
            error_count += 1;
        } else {
            debug!("Harvester: Successfully sent any remaining logs before freeze for request {}", request_id);
        }
        
        // Send any remaining platform events (should be minimal due to request-specific flushing)
        if let Err(e) = self.platform_processor.send_and_clear_batch_simple().await {
            error!("Harvester: Failed to send remaining platform events before freeze: {}", e);
            error_count += 1;
        } else {
            debug!("Harvester: Successfully sent any remaining platform events before freeze for request {}", request_id);
        }
        
        if error_count > 0 {
            warn!("Harvester: Completed safety-net flush with {} errors for request {}", error_count, request_id);
        } else {
            debug!("Harvester: Safety-net flush completed for request {} (most telemetry already sent by request-specific flushing)", request_id);
        }
        
        Ok(())
    }

    /// Start request-specific periodic flushing after getting request context
    /// This ensures logs are sent with proper request_id association within the invoke cycle
    pub fn start_request_flushing(&self, request_id: String, trace_collection_enabled: bool) -> tokio::task::JoinHandle<()> {
        let log_processor = Arc::clone(&self.log_processor);
        let platform_processor = Arc::clone(&self.platform_processor);
        
        tokio::spawn(async move {
            if trace_collection_enabled {
                info!("Starting request-specific flush loop for {} (trace ID enabled - will wait for agent payload)", request_id);
                
                // Wait for agent payload or reasonable timeout, then start aggressive flushing
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                
                // Aggressive flushing every 50ms to ensure quick delivery
                let mut flush_interval = tokio::time::interval(std::time::Duration::from_millis(50));
                for _cycle in 1..=10 { // Max 10 cycles (500ms total)
                    flush_interval.tick().await;
                    
                    let mut sent_something = false;
                    
                    // Try to send logs
                    if let Ok(()) = log_processor.send_and_clear_batch_simple().await {
                        sent_something = true;
                    }
                    
                    // Try to send platform events  
                    if let Ok(()) = platform_processor.send_and_clear_batch_simple().await {
                        sent_something = true;
                    }
                    
                    if sent_something {
                        debug!("Request {} flush cycle {} completed", request_id, _cycle);
                    }
                }
                
            } else {
                info!("Starting request-specific flush loop for {} (trace ID disabled - immediate sending)", request_id);
                
                // Immediate aggressive flushing every 25ms for fast delivery
                let mut flush_interval = tokio::time::interval(std::time::Duration::from_millis(25));
                for _cycle in 1..=20 { // Max 20 cycles (500ms total)
                    flush_interval.tick().await;
                    
                    let mut sent_something = false;
                    
                    // Try to send logs immediately
                    if let Ok(()) = log_processor.send_and_clear_batch_simple().await {
                        sent_something = true;
                    }
                    
                    // Try to send platform events immediately
                    if let Ok(()) = platform_processor.send_and_clear_batch_simple().await {
                        sent_something = true;
                    }
                    
                    if sent_something {
                        debug!("Request {} immediate flush cycle {} completed", request_id, _cycle);
                    }
                }
            }
            
            info!("Request-specific flush loop completed for {}", request_id);
        })
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

