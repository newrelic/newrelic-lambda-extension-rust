//! Shared utility traits and helpers

use std::sync::Mutex;
use tracing::error;

/// Safe mutex operations that won't panic and allow graceful degradation.
/// Use instead of `.lock().unwrap()` in production code to prevent panic cascades
/// when a mutex becomes poisoned (e.g., a thread panicked while holding the lock).
pub trait SafeMutexOps<T> {
    fn safe_lock(&self) -> Option<std::sync::MutexGuard<'_, T>>;
}

impl<T> SafeMutexOps<T> for Mutex<T> {
    fn safe_lock(&self) -> Option<std::sync::MutexGuard<'_, T>> {
        match self.lock() {
            Ok(guard) => Some(guard),
            Err(e) => {
                error!("Mutex poisoned (extension will continue in degraded mode): {}", e);
                None
            }
        }
    }
}
