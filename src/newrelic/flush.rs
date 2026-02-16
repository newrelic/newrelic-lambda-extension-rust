use async_trait::async_trait;
use std::io::Result as IoResult;

#[async_trait]
pub trait Flush: Send + Sync {
    async fn flush(&self) -> IoResult<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// Test implementation to verify trait is object-safe and implementable
    struct MockFlusher {
        flush_count: std::sync::atomic::AtomicU32,
    }

    impl MockFlusher {
        fn new() -> Self {
            Self {
                flush_count: std::sync::atomic::AtomicU32::new(0),
            }
        }

        fn get_flush_count(&self) -> u32 {
            self.flush_count.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl Flush for MockFlusher {
        async fn flush(&self) -> IoResult<()> {
            self.flush_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_flush_trait_is_implementable() {
        let flusher = MockFlusher::new();
        let result = flusher.flush().await;
        assert!(result.is_ok());
        assert_eq!(flusher.get_flush_count(), 1);
    }

    #[tokio::test]
    async fn test_flush_trait_is_object_safe() {
        // Verify the trait can be used as a trait object (dyn Flush)
        let flusher: Arc<dyn Flush> = Arc::new(MockFlusher::new());
        let result = flusher.flush().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_flush_trait_multiple_calls() {
        let flusher = MockFlusher::new();
        for _ in 0..5 {
            flusher.flush().await.expect("flush should succeed");
        }
        assert_eq!(flusher.get_flush_count(), 5);
    }

    #[test]
    fn test_flush_trait_send_sync_bounds() {
        // Verify Send + Sync bounds are satisfied (compiles = passes)
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MockFlusher>();
    }

    #[tokio::test]
    async fn test_flush_trait_error_propagation() {
        struct FailingFlusher;

        #[async_trait]
        impl Flush for FailingFlusher {
            async fn flush(&self) -> IoResult<()> {
                Err(std::io::Error::new(std::io::ErrorKind::Other, "flush failed"))
            }
        }

        let flusher = FailingFlusher;
        let result = flusher.flush().await;
        assert!(result.is_err());
        assert_eq!(result.expect_err("should be err").to_string(), "flush failed");
    }
}
