use crate::newrelic::flush::Flush;
use crate::logs::processor::LogProcessor;
use crate::platform::processor::PlatformProcessor;
use std::{sync::Arc, time::Duration};
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

    /// Runs the harvester loop, periodically flushing ONLY logs (function, extension, platform).
    /// This optimized harvester reduces memory by flushing logs frequently without touching
    /// other processors (agent telemetry is handled per-request in serverless mode).
    pub async fn run(&self) {
        let mut interval = tokio::time::interval(self.interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        debug!("Starting log-only harvester with interval: {:?}", self.interval);
        let mut flush_cycle_count = 0;
        
        loop {
            interval.tick().await;
            flush_cycle_count += 1;
            
            if flush_cycle_count % 10 == 1 {
                trace!("Log harvester cycle {} - flushing function, extension, and platform logs", flush_cycle_count);
            }
            
            let mut error_count = 0;
            
            // Flush function and extension logs (controlled by ENV variables)
            if let Err(e) = self.log_processor.flush().await {
                error!("Error flushing function/extension logs: {}", e);
                error_count += 1;
            }
            
            // Flush platform logs (always enabled, formatted as log lines)
            if let Err(e) = self.platform_processor.flush().await {
                error!("Error flushing platform logs: {}", e);
                error_count += 1;
            }
            
            if error_count > 0 {
                warn!("Log harvester cycle {} completed with {} errors", flush_cycle_count, error_count);
            } else if flush_cycle_count % 10 == 1 {
                debug!("Log harvester cycle {} completed - logs flushed to reduce memory", flush_cycle_count);
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ExtensionConfig;
    use crate::context::InvocationContext;
    use crate::newrelic::client::NewRelicClient;
    use crate::request::ProcessorFactory;
    use std::sync::Mutex;

    fn make_harvester(interval_secs: u64) -> Harvester {
        let config = Arc::new(ExtensionConfig::default());
        let client = Arc::new(NewRelicClient::new_noop());
        let apm_app = Arc::new(tokio::sync::RwLock::new(None));
        let factory = ProcessorFactory::new(client, config.clone(), apm_app);
        let ctx = Arc::new(Mutex::new(InvocationContext::default()));
        let log_processor = factory.create_log_processor(ctx.clone());
        let platform_processor = factory.create_platform_processor(ctx, log_processor.clone());

        Harvester::new(
            vec![],
            Duration::from_secs(interval_secs),
            log_processor,
            platform_processor,
        )
    }

    #[test]
    fn test_harvester_new_construction() {
        let harvester = make_harvester(5);
        let debug_str = format!("{harvester:?}");
        assert!(debug_str.contains("Harvester"));
    }

    #[test]
    fn test_harvester_debug_shows_processor_count() {
        let harvester = make_harvester(5);
        let debug_str = format!("{harvester:?}");
        assert!(debug_str.contains("processor_count: 0"));
    }

    #[test]
    fn test_harvester_debug_shows_interval() {
        let harvester = make_harvester(10);
        let debug_str = format!("{harvester:?}");
        assert!(debug_str.contains("10s") || debug_str.contains("10000"), "Should show 10s interval: {debug_str}");
    }

    #[test]
    fn test_harvester_with_different_intervals() {
        let h1 = make_harvester(1);
        let h5 = make_harvester(5);
        let h30 = make_harvester(30);

        let d1 = format!("{h1:?}");
        let d5 = format!("{h5:?}");
        let d30 = format!("{h30:?}");

        // All should contain "Harvester"
        assert!(d1.contains("Harvester"));
        assert!(d5.contains("Harvester"));
        assert!(d30.contains("Harvester"));
    }
}
