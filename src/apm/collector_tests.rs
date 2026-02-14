#[cfg(test)]
mod tests {
    use crate::apm::collector::{
        resolve_collector_command, CollectorError,
        CMD_METRICS, CMD_SPAN_EVENTS, CMD_ERROR_EVENTS, CMD_ERROR_DATA,
        CMD_ANALYTIC_EVENTS, CMD_CUSTOM_EVENTS, CMD_LOG_EVENTS, CMD_TRANSACTION_SAMPLES,
    };

    // ========================================================================
    // resolve_collector_command
    // ========================================================================

    #[test]
    fn test_resolve_all_known_commands() {
        let known = [
            CMD_METRICS,
            CMD_SPAN_EVENTS,
            CMD_ERROR_DATA,
            CMD_ANALYTIC_EVENTS,
            CMD_CUSTOM_EVENTS,
            CMD_LOG_EVENTS,
            CMD_TRANSACTION_SAMPLES,
        ];

        for cmd in known {
            let result = resolve_collector_command(cmd);
            assert_eq!(result, Some(cmd), "Expected Some for known command: {}", cmd);
        }
    }

    #[test]
    fn test_resolve_error_events_returns_none() {
        // error_event_data is handled separately via send_error_events
        assert_eq!(resolve_collector_command(CMD_ERROR_EVENTS), None);
    }

    #[test]
    fn test_resolve_unknown_type_returns_none() {
        assert_eq!(resolve_collector_command("completely_unknown"), None);
        assert_eq!(resolve_collector_command(""), None);
        assert_eq!(resolve_collector_command("metric_data_v2"), None);
    }

    // ========================================================================
    // CollectorError Display
    // ========================================================================

    #[test]
    fn test_collector_error_disconnect_display() {
        let err = CollectorError::Disconnect;
        assert_eq!(format!("{}", err), "Collector disconnected (410)");
    }

    #[test]
    fn test_collector_error_restart_display() {
        let err = CollectorError::RestartException;
        assert_eq!(format!("{}", err), "Collector restart exception (401/409)");
    }

    #[test]
    fn test_collector_error_equality() {
        assert_eq!(CollectorError::Disconnect, CollectorError::Disconnect);
        assert_eq!(CollectorError::RestartException, CollectorError::RestartException);
        assert_ne!(CollectorError::Disconnect, CollectorError::RestartException);
    }

    // ========================================================================
    // get_user_agent + constants
    // ========================================================================

    use crate::apm::collector::get_user_agent;

    #[test]
    fn test_get_user_agent_format() {
        let ua = get_user_agent();
        assert!(ua.starts_with("NewRelic-Rust-Lambda-Extension/"));
        assert!(ua.contains(env!("CARGO_PKG_VERSION")));
    }

    #[test]
    fn test_cmd_constants_have_expected_values() {
        assert_eq!(CMD_METRICS, "metric_data");
        assert_eq!(CMD_SPAN_EVENTS, "span_event_data");
        assert_eq!(CMD_ERROR_EVENTS, "error_event_data");
        assert_eq!(CMD_ERROR_DATA, "error_data");
        assert_eq!(CMD_ANALYTIC_EVENTS, "analytic_event_data");
        assert_eq!(CMD_CUSTOM_EVENTS, "custom_event_data");
        assert_eq!(CMD_TRANSACTION_SAMPLES, "transaction_sample_data");
        assert_eq!(CMD_LOG_EVENTS, "log_event_data");
    }

    #[test]
    fn test_collector_error_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<CollectorError>();
    }
}
