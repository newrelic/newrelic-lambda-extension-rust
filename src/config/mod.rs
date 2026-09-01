// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;
use std::sync::OnceLock;
use std::{env, time::Duration};
use tracing::{debug, warn, Event, Subscriber};
use tracing_subscriber::{
    fmt::{self, FmtContext, FormatEvent, FormatFields},
    registry::LookupSpan,
    EnvFilter,
};

pub mod deployment;

use deployment::{DeploymentContext, TelemetryMode};

/// Default per-invocation cap on logs parked while waiting for the trace.id.
const DEFAULT_TRACE_ID_LOG_BUFFER_MAX: usize = 2000;
/// Lower/upper clamp for `NEW_RELIC_TRACE_ID_LOG_BUFFER_MAX` to avoid 0 (drop
/// everything) and pathological values that could exhaust sandbox memory.
const MIN_TRACE_ID_LOG_BUFFER_MAX: usize = 1;
const MAX_TRACE_ID_LOG_BUFFER_MAX: usize = 100_000;

/// Fallback used when `NEW_RELIC_DATA_COLLECTION_TIMEOUT` is set but not a valid
/// duration string. Matches the Go extension's default.
const DEFAULT_DATA_COLLECTION_TIMEOUT: Duration = Duration::from_secs(10);

/// Fallback used when `NEW_RELIC_HTTP_TIMEOUT` is set but not a valid duration string.
const DEFAULT_HTTP_TIMEOUT: Duration = Duration::from_millis(2400);

/// Global configuration for the New Relic Lambda Extension
#[derive(Debug, Clone)]
pub struct ExtensionConfig {
    pub new_relic: NewRelicConfig,
    pub aws: AwsConfig,
    pub extension: ExtensionSettings,
    /// Detected once at startup — single source of truth for the deployment
    /// environment (Normal Lambda vs LMI). All loop-dispatch and mode-aware
    /// branching reads this rather than re-parsing env vars. See
    /// `LMI_SUPPORT.md` §2.
    pub deployment: DeploymentContext,
}

/// New Relic specific configuration
#[derive(Debug, Clone)]
pub struct NewRelicConfig {
    pub extension_enabled: bool,
    pub license_key: Option<String>,
    pub license_key_secret_id: String,
    pub license_key_ssm_parameter_name: String,
    pub lambda_handler: Option<String>,
    pub telemetry_endpoint: String,
    pub log_endpoint: String,
    #[allow(dead_code)]
    pub harvest_interval: Duration,
    pub collect_trace_id: bool,
    /// Max number of logs parked per invocation while waiting for the agent
    /// payload (the `trace.id` source) to arrive. Only used when
    /// `collect_trace_id` is true. Configurable via
    /// `NEW_RELIC_TRACE_ID_LOG_BUFFER_MAX` (default 2000). On overflow, logs are
    /// sent without `trace.id` (we cannot stamp a trace we don't yet have).
    pub trace_id_log_buffer_max: usize,
    pub add_version_detail_tags: bool,
    pub layer_version: Option<String>,
    pub apm_lambda_mode: bool,
    pub apm_blocking_handshake: bool,
    pub apm_handshake_timeout_secs: u64,
    /// Telemetry types the customer has opted to drop (APM mode only) via
    /// `NEW_RELIC_APM_DISABLE_TELEMETRY`. Excluded types are neither sent nor
    /// buffered. See `crate::apm::collector::KNOWN_TELEMETRY_TYPES`.
    pub apm_disabled_telemetry: HashSet<String>,
    pub apm_host: String,
    pub metric_endpoint: String,
    pub otlp_metric_endpoint: String,
    /// `NEW_RELIC_OTLP_METRIC_ENABLED` — feature-gates OTLP metrics forwarding
    /// (protobuf `entity.guid` injection + send). Default: false. While
    /// disabled, any `otlp_payload` entries the agent sends are dropped
    /// without being decoded or forwarded.
    ///
    /// This is the raw env-var value. Forwarding additionally requires
    /// `apm_lambda_mode`, since the send path lives in `ApmApp`; the effective
    /// gate is `crate::apm::collector::is_otlp_metric_enabled()`.
    pub otlp_metric_enabled: bool,
    pub proxy_url: Option<String>,
    /// `NEW_RELIC_EXTENSION_SYNCHRONOUS_FLUSH` — master switch for serverless-mode
    /// (standard/non-APM mode only) immediate delivery of the agent payload. When
    /// `true`, the agent payload is sent to New Relic the instant it's received on the
    /// named pipe (`request::route_payload_to_request_buffer`), as its own background
    /// task — instead of buffering it to pair with `platform.report` and waiting for
    /// the 3+ batch threshold or `SHUTDOWN`. `process_request_concurrently` awaits any
    /// outstanding send for its request (bounded by the invocation's remaining
    /// deadline) before the invocation ends, so delivery completes within the same
    /// invoke without the sandbox freezing mid-flight.
    ///
    /// There is no wait for a late `platform.report`, and report handling is otherwise
    /// unaffected by this flag: the Telemetry API only emits `platform.report` after
    /// every extension has already called `/next` for the invocation, so a fresh
    /// request's own report can never be "already arrived" in practice — waiting for it
    /// would either time out every time or delay the extension's own `/next` call. The
    /// report keeps following its existing pairing/threshold/`SHUTDOWN` path
    /// (`AGENT_BATCH_BUFFER`, `set_pending_report`) exactly as it does when this flag is
    /// off; since the payload no longer sits in `agent_buffer` waiting to pair, a report
    /// that arrives later simply finds nothing to pair with, same as today.
    ///
    /// Default `false` — no behavior change unless explicitly enabled. Distinct from
    /// `NEW_RELIC_APM_BLOCKING_AGENT_PAYLOAD`, which is APM-mode-only.
    pub synchronous_flush: bool,
    /// Total wall-clock budget for retrying a send, set via
    /// `NEW_RELIC_DATA_COLLECTION_TIMEOUT`. `None` (the default, env var unset)
    /// preserves the existing fixed-retry-count behavior unchanged. `Some(d)`
    /// switches to retrying until `d` has elapsed instead of a fixed count.
    pub data_collection_timeout: Option<Duration>,
    /// Per-request timeout, set via `NEW_RELIC_HTTP_TIMEOUT`. Opt-in, same as
    /// data_collection_timeout.
    pub http_timeout: Option<Duration>,
}

/// AWS Lambda specific configuration
#[derive(Debug, Clone)]
pub struct AwsConfig {
    pub runtime_api: String,
    pub function_name: String,
    pub function_version: Option<String>,
    pub account_id: Option<String>,
    pub region: Option<String>,
}

/// Extension specific settings
#[derive(Debug, Clone)]
pub struct ExtensionSettings {
    pub send_function_logs: bool,
    pub send_extension_logs: bool,
    pub send_platform_logs: bool,
    /// Specific `platform.*` event type names to send, e.g. `["platform.start", "platform.report"]`.
    /// Empty means no filter — every platform event type is sent (today's behavior).
    /// Populated only when `NEW_RELIC_EXTENSION_SEND_LOGS` lists `platform.<event>` tokens
    /// without the bare `platform` token; bare `platform` (or `all`) always means "no filter".
    pub platform_log_filter: HashSet<String>,
    pub log_level: String,
    /// NEW_RELIC_EXTENSION_LOGS_ENABLED - Master switch for [NR_EXT] log output
    /// Default: true (logs enabled)
    /// If false, suppresses all [NR_EXT] prefixed logs from appearing in CloudWatch
    pub extension_logs_enabled: bool,
    /// NEW_RELIC_RUNTIME_DONE_GRACE_MS — grace period (ms) added AFTER
    /// `platform.runtimeDone` before the end-of-invocation flush, only when
    /// `log_batch` still has data (i.e. `is_drained()` returns false). Default
    /// 25, clamped to `[0, 2000]`. Read once at startup by `init_config()`.
    pub runtime_done_grace_ms: u64,
    /// NEW_RELIC_EXTENSION_PIPELINE_FLUSH — when true, the extension calls
    /// GET /next immediately after runtimeDone and flushes telemetry in the
    /// background during the freeze/thaw gap. Reduces billed duration but
    /// may lose data on final shutdown if flush doesn't complete in time.
    /// Default: false (safe mode — flush completes before GET /next).
    pub pipeline_flush: bool,
    /// NEW_RELIC_LMI_FLUSH_INTERVAL_MS — heartbeat flush cadence (ms) for the
    /// Lambda Managed Instances (LMI) event loop. LMI runs continuously (no
    /// freeze between invokes), so buffered telemetry is drained on this
    /// interval by a background task rather than at `platform.runtimeDone`.
    /// Default 30_000, floored at 1000. Read ONLY by the LMI loop; ignored on
    /// standard Lambda.
    pub lmi_flush_interval_ms: u64,
}

/// Configuration struct that matches the credentials module expectations
#[derive(Debug, Clone)]
pub struct Configuration {
    pub license_key: String,
    pub license_key_secret_id: String,
    pub license_key_ssm_parameter_name: String,
}

impl From<&ExtensionConfig> for Configuration {
    fn from(config: &ExtensionConfig) -> Self {
        Self {
            license_key: config.new_relic.license_key.clone().unwrap_or_default(),
            license_key_secret_id: config.new_relic.license_key_secret_id.clone(),
            license_key_ssm_parameter_name: config.new_relic.license_key_ssm_parameter_name.clone(),
        }
    }
}

impl Default for ExtensionConfig {
    fn default() -> Self {
        Self {
            new_relic: NewRelicConfig::default(),
            aws: AwsConfig::default(),
            extension: ExtensionSettings::default(),
            // Default for tests / non-Lambda contexts. Production code paths
            // overwrite this in `from_env()` via `deployment::detect()`.
            deployment: DeploymentContext::Normal { mode: TelemetryMode::Serverless },
        }
    }
}

impl Default for NewRelicConfig {
    fn default() -> Self {
        Self {
            extension_enabled: true,
            license_key: None,
            license_key_secret_id: String::new(),
            license_key_ssm_parameter_name: String::new(),
            lambda_handler: None,
            telemetry_endpoint: "https://cloud-collector.newrelic.com/aws/lambda/v1".to_string(),
            log_endpoint: "https://log-api.newrelic.com/log/v1".to_string(),
            harvest_interval: Duration::from_secs(2),
            collect_trace_id: false,
            trace_id_log_buffer_max: DEFAULT_TRACE_ID_LOG_BUFFER_MAX,
            add_version_detail_tags: false,
            layer_version: None,
            apm_lambda_mode: false,
            apm_blocking_handshake: false,
            apm_handshake_timeout_secs: 5,
            apm_disabled_telemetry: HashSet::new(),
            apm_host: "collector.newrelic.com".to_string(),
            metric_endpoint: "https://metric-api.newrelic.com/metric/v1".to_string(),
            otlp_metric_endpoint: "https://collector.newrelic.com/v1/metrics".to_string(),
            otlp_metric_enabled: false,
            proxy_url: None,
            synchronous_flush: false,
            data_collection_timeout: None,
            http_timeout: None,
        }
    }
}

impl Default for AwsConfig {
    fn default() -> Self {
        Self {
            runtime_api: "127.0.0.1:9001".to_string(),
            function_name: "unknown".to_string(),
            function_version: None,
            account_id: None,
            region: None,
        }
    }
}

impl AwsConfig {
    /// Suppress dead_code warning: This function is actually used but the compiler
    /// cannot detect it due to dynamic dispatch/reflection patterns
    #[allow(dead_code)]
    pub fn construct_function_arn(&self) -> Option<String> {
        if self.function_name.is_empty() {
            return None;
        }

        let region = env::var("AWS_REGION")
            .or_else(|_| env::var("AWS_DEFAULT_REGION"))
            .unwrap_or_else(|_| "us-east-1".to_string());

        // Get account ID, return None if not available (don't use placeholder)
        let Some(account_id) = self.account_id.as_ref()
            .and_then(|id| if id.is_empty() { None } else { Some(id.as_str()) })
        else {
            warn!("Cannot construct ARN: account ID not available from registration yet");
            return None;
        };

        Some(format!(
            "arn:aws:lambda:{}:{}:function:{}",
            region, account_id, self.function_name
        ))
    }

    pub fn update_from_registration(&mut self, function_name: String, function_version: String, account_id: Option<String>) {
        self.function_name = function_name;
        self.function_version = Some(function_version);
        self.account_id = account_id;

        if self.region.is_none() {
            self.region = env::var("AWS_REGION").ok();
        }
    }

    pub fn extract_and_update_account_id_from_arn(&mut self, invoked_function_arn: &str) {
        if self.account_id.is_none() || self.account_id.as_ref().map_or(true, |id| id.is_empty() || id == "123456789012") {
            if let Some(extracted_account_id) = Self::extract_account_id_from_arn(invoked_function_arn) {
                debug!("Extracted account ID from ARN: {}", extracted_account_id);
                self.account_id = Some(extracted_account_id);
            }
        }
    }

    /// Extracts the account ID from a Lambda function ARN, which has the form
    /// `arn:<partition>:lambda:<region>:<account-id>:function:<name>`.
    /// Requires the full 7-part shape and an `aws`-prefixed partition (`aws`,
    /// `aws-cn`, `aws-us-gov`) to reject malformed or non-Lambda ARNs.
    fn extract_account_id_from_arn(arn: &str) -> Option<String> {
        let parts: Vec<&str> = arn.split(':').collect();
        if parts.len() >= 7
            && parts[0] == "arn"
            && parts[1].starts_with("aws")
            && parts[2] == "lambda"
            && !parts[3].is_empty()
            && !parts[4].is_empty()
        {
            Some(parts[4].to_string())
        } else {
            None
        }
    }
}

impl Default for ExtensionSettings {
    fn default() -> Self {
        Self {
            send_function_logs: false,
            send_extension_logs: false,
            send_platform_logs: false,
            platform_log_filter: HashSet::new(),
            log_level: "info".to_string(),
            extension_logs_enabled: true,
            runtime_done_grace_ms: 25,
            pipeline_flush: false,
            lmi_flush_interval_ms: 30_000,
        }
    }
}

/// Parse `NEW_RELIC_APM_DISABLE_TELEMETRY` into a set of telemetry types to drop.
///
/// Comma-separated, case-insensitive, whitespace-trimmed. Only the canonical
/// types in [`crate::apm::collector::KNOWN_TELEMETRY_TYPES`] are accepted;
/// unknown tokens are warned about and ignored (fail-soft).
pub(crate) fn parse_disabled_telemetry(raw: &str) -> HashSet<String> {
    let mut set = HashSet::new();
    for token in raw.split(',') {
        let t = token.trim().to_ascii_lowercase();
        if t.is_empty() {
            continue;
        }
        if crate::apm::collector::KNOWN_TELEMETRY_TYPES.contains(&t.as_str()) {
            set.insert(t);
        } else {
            warn!(
                "NEW_RELIC_APM_DISABLE_TELEMETRY: ignoring unknown telemetry type '{}' (valid: {})",
                t,
                crate::apm::collector::KNOWN_TELEMETRY_TYPES.join(", ")
            );
        }
    }
    if !set.is_empty() {
        let mut types: Vec<&str> = set.iter().map(String::as_str).collect();
        types.sort_unstable();
        debug!("APM telemetry disabled for types: {}", types.join(", "));
    }
    set
}

/// Parse a Go-style duration string (`"200ms"`, `"30s"`, `"1m"`, `"2h"`).
/// Bare numbers with no unit are rejected, matching Go's `time.ParseDuration`.
pub(crate) fn parse_duration(raw: &str) -> Option<Duration> {
    let raw = raw.trim();
    let unit_start = raw.find(|c: char| !c.is_ascii_digit() && c != '.')?;
    let (num_part, unit) = raw.split_at(unit_start);
    let value: f64 = num_part.parse().ok()?;
    if value < 0.0 {
        return None;
    }
    let millis = match unit {
        "ms" => value,
        "s" => value * 1_000.0,
        "m" => value * 60_000.0,
        "h" => value * 3_600_000.0,
        _ => return None,
    };
    Some(Duration::from_millis(millis as u64))
}

impl ExtensionConfig {
    /// Whether OTLP metric forwarding will actually run: the customer opted in via
    /// `NEW_RELIC_OTLP_METRIC_ENABLED` **and** APM mode is on. APM mode is required
    /// because the send path lives in `ApmApp::process_agent_payload`, and no `ApmApp`
    /// is constructed in serverless mode — so the env var alone is not sufficient.
    ///
    /// This is the single source of truth mirrored into
    /// `crate::apm::collector::set_otlp_metric_enabled()` at startup.
    pub fn otlp_metric_forwarding_active(&self) -> bool {
        self.new_relic.otlp_metric_enabled && self.new_relic.apm_lambda_mode
    }

    /// Validates the log level and returns a valid level or defaults to "info" with a warning
    fn validate_log_level(raw_level: &str) -> String {
        let normalized = raw_level.to_lowercase();
        match normalized.as_str() {
            "trace" | "debug" | "info" | "warn" | "error" | "all" => normalized,
            _ => {
                eprintln!("[NR_EXT] WARNING: Invalid log level '{}' provided in NEW_RELIC_EXTENSION_LOG_LEVEL. Defaulting to 'info'. Valid values are: trace, debug, info, warn, error, all", raw_level);
                "info".to_string()
            }
        }
    }

    /// Parse "NEW_RELIC_EXTENSION_SEND_LOGS" environment variable
    /// Accepts comma-separated values: platform, extension, function, all, or specific
    /// platform.<event> names (e.g. platform.start, platform.report). Returns
    /// (send_function_logs, send_extension_logs, send_platform_logs, platform_log_filter).
    /// platform_log_filter is empty (no filter, send every platform event type) unless
    /// only platform.<event> tokens were given (no bare "platform"/"all").
    fn parse_send_logs(value: &str) -> (bool, bool, bool, HashSet<String>) {
        let normalized = value.to_lowercase();
        let parts: Vec<&str> = normalized.split(',').map(|s| s.trim()).collect();

        // NEW: Check for empty string
        if normalized.is_empty() {
            eprintln!("NEW_RELIC_EXTENSION_SEND_LOGS is empty. No logs will be sent");
            return (false, false, false, HashSet::new());
        }
        // Check for "all" first
        if parts.contains(&"all") {
           if parts.len() > 1 {
                eprintln!("[NR_EXT] INFO: 'all' specified in SEND_LOGS;defaulting to 'all'");
            }
            return (true, true, true, HashSet::new());
        }

        let send_function = parts.contains(&"function");
        let send_extension = parts.contains(&"extension");
        let bare_platform = parts.contains(&"platform");

        let platform_event_tokens: HashSet<String> = parts.iter()
            .filter(|p| p.starts_with("platform.") && p.len() > "platform.".len())
            .map(|p| p.to_string())
            .collect();

        // Bare "platform" always means "no filter, send everything" — it wins over
        // any specific platform.<event> tokens listed alongside it.
        let send_platform = bare_platform || !platform_event_tokens.is_empty();
        let platform_log_filter = if bare_platform {
            HashSet::new()
        } else {
            platform_event_tokens
        };

        (send_function, send_extension, send_platform, platform_log_filter)
    }

    pub fn from_env() -> Self {
        let send_function_logs_str = env::var("NEW_RELIC_EXTENSION_SEND_FUNCTION_LOGS").unwrap_or_default();
        let send_extension_logs_str = env::var("NEW_RELIC_EXTENSION_SEND_EXTENSION_LOGS").unwrap_or_default();
        let send_platform_logs_str = env::var("NEW_RELIC_EXTENSION_SEND_PLATFORM_LOGS").unwrap_or_default();
        let send_logs_str = env::var("NEW_RELIC_EXTENSION_SEND_LOGS").unwrap_or_default();

        let mut config = Self::default();

        // Load New Relic configuration
        config.new_relic.extension_enabled = env::var("NEW_RELIC_LAMBDA_EXTENSION_ENABLED")
            .unwrap_or_else(|_| "true".to_string())
            .parse()
            .unwrap_or(true);

        config.new_relic.license_key = env::var("NEW_RELIC_LICENSE_KEY").ok();
        config.new_relic.license_key_secret_id = env::var("NEW_RELIC_LICENSE_KEY_SECRET").unwrap_or_default();
        config.new_relic.license_key_ssm_parameter_name = env::var("NEW_RELIC_LICENSE_KEY_SSM_PARAMETER_NAME").unwrap_or_default();
        config.new_relic.lambda_handler = env::var("NEW_RELIC_LAMBDA_HANDLER").ok();
        
        fn parse_bool(s: &str) -> bool {
            matches!(s.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on")
        }
        let collect_trace_id_str = env::var("NEW_RELIC_COLLECT_TRACE_ID").unwrap_or_default();
        config.new_relic.collect_trace_id = parse_bool(&collect_trace_id_str);

        // Per-invocation parking cap for trace.id buffering. Only meaningful when
        // collect_trace_id is on; clamped to a sane range so a typo can't drop all
        // logs (0) or exhaust memory.
        config.new_relic.trace_id_log_buffer_max = env::var("NEW_RELIC_TRACE_ID_LOG_BUFFER_MAX")
            .ok()
            .and_then(|s| s.trim().parse::<usize>().ok())
            .map_or(DEFAULT_TRACE_ID_LOG_BUFFER_MAX, |n| {
                n.clamp(MIN_TRACE_ID_LOG_BUFFER_MAX, MAX_TRACE_ID_LOG_BUFFER_MAX)
            });

        let add_version_detail_tags_str = env::var("NEW_RELIC_ADD_VERSION_DETAIL_TAGS").unwrap_or_default();
        config.new_relic.add_version_detail_tags = parse_bool(&add_version_detail_tags_str);

        config.new_relic.layer_version = env::var("NEW_RELIC_LAYER_VERSION").ok();

        let apm_lambda_mode_str = env::var("NEW_RELIC_APM_LAMBDA_MODE").unwrap_or_default();
        config.new_relic.apm_lambda_mode = parse_bool(&apm_lambda_mode_str);

        // Detect deployment context exactly once. On LMI, APM mode is forced
        // regardless of the user-supplied `NEW_RELIC_APM_LAMBDA_MODE` value —
        // `deployment::detect()` already logs a warning when the user explicitly
        // disabled it. See `LMI_SUPPORT.md` §7.
        config.deployment = deployment::detect();
        if config.deployment.is_lmi() {
            config.new_relic.apm_lambda_mode = true;
        }

        let apm_blocking_handshake_str = env::var("NEW_RELIC_APM_BLOCKING_HANDSHAKE").unwrap_or_default();
        config.new_relic.apm_blocking_handshake = parse_bool(&apm_blocking_handshake_str);

        config.new_relic.apm_handshake_timeout_secs = env::var("NEW_RELIC_APM_HANDSHAKE_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(5)
            .max(1);

        // Comma-separated telemetry types the customer wants dropped (APM mode).
        config.new_relic.apm_disabled_telemetry =
            parse_disabled_telemetry(&env::var("NEW_RELIC_APM_DISABLE_TELEMETRY").unwrap_or_default());

        // OTLP metrics forwarding is opt-in: default false until customers explicitly enable it.
        let otlp_metric_enabled_str = env::var("NEW_RELIC_OTLP_METRIC_ENABLED").unwrap_or_default();
        config.new_relic.otlp_metric_enabled = parse_bool(&otlp_metric_enabled_str);

        config.new_relic.proxy_url = env::var("NEW_RELIC_LAMBDA_EXTENSION_PROXY")
            .ok()
            .filter(|s| !s.is_empty());

        // Opt-in: only set (and thus only change retry behavior) when the customer
        // explicitly provides this env var. Unset means data_collection_timeout stays
        // None and existing fixed-retry-count behavior is untouched.
        config.new_relic.data_collection_timeout = env::var("NEW_RELIC_DATA_COLLECTION_TIMEOUT")
            .ok()
            .map(|raw| {
                parse_duration(&raw).unwrap_or_else(|| {
                    warn!(
                        "Invalid NEW_RELIC_DATA_COLLECTION_TIMEOUT value '{}', defaulting to 10s",
                        raw
                    );
                    DEFAULT_DATA_COLLECTION_TIMEOUT
                })
            });

        // Opt-in, same pattern as data_collection_timeout.
        config.new_relic.http_timeout = env::var("NEW_RELIC_HTTP_TIMEOUT")
            .ok()
            .map(|raw| {
                parse_duration(&raw).unwrap_or_else(|| {
                    warn!(
                        "Invalid NEW_RELIC_HTTP_TIMEOUT value '{}', defaulting to 2400ms",
                        raw
                    );
                    DEFAULT_HTTP_TIMEOUT
                })
            });

        if let Some(ref url) = config.new_relic.proxy_url {
            // Log proxy activation at startup (eprintln ensures visibility before tracing is initialized)
            // Mask credentials: http://user:pass@host -> http://***:***@host
            let masked = if let (Some(scheme_end), Some(at_pos)) = (url.find("://"), url.rfind('@')) {
                format!("{}***:***{}", &url[..scheme_end + 3], &url[at_pos..])
            } else {
                url.clone()
            };
            eprintln!("[NR_EXT] INFO Proxy enabled: {masked}");
        }

        if let Ok(runtime_api) = env::var("AWS_LAMBDA_RUNTIME_API") {
            config.aws.runtime_api = runtime_api;
        }

        // Note: function_name is set from extension registration response, not from env var

        // Parse NEW_RELIC_EXTENSION_SEND_LOGS (takes precedence over individual flags)
        if !send_logs_str.is_empty() {
            let (function, extension, platform, platform_log_filter) = Self::parse_send_logs(&send_logs_str);
            config.extension.send_function_logs = function;
            config.extension.send_extension_logs = extension;
            config.extension.send_platform_logs = platform;
            config.extension.platform_log_filter = platform_log_filter;
        } else {
            // Fall back to individual environment variables for backward compatibility
            config.extension.send_function_logs = parse_bool(&send_function_logs_str);
            config.extension.send_extension_logs = parse_bool(&send_extension_logs_str);
            config.extension.send_platform_logs = parse_bool(&send_platform_logs_str);
        }

        let raw_log_level = env::var("NEW_RELIC_EXTENSION_LOG_LEVEL").unwrap_or_else(|_| "info".to_string());
        config.extension.log_level = Self::validate_log_level(&raw_log_level);

        let extension_logs_enabled_str = env::var("NEW_RELIC_EXTENSION_LOGS_ENABLED").unwrap_or_else(|_| "true".to_string());
        config.extension.extension_logs_enabled = parse_bool(&extension_logs_enabled_str);

        // Parse NEW_RELIC_RUNTIME_DONE_GRACE_MS once at startup. Clamp to [0, 2000].
        // Default 25 ms — matches the Telemetry API buffer flush window.
        config.extension.runtime_done_grace_ms = env::var("NEW_RELIC_RUNTIME_DONE_GRACE_MS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(25)
            .min(2000);

        // NEW_RELIC_EXTENSION_PIPELINE_FLUSH: opt-in to pipeline GET /next pattern.
        // When enabled, the extension calls GET /next immediately after runtimeDone
        // and flushes telemetry in the background, reducing billed duration.
        let pipeline_flush_str = env::var("NEW_RELIC_EXTENSION_PIPELINE_FLUSH").unwrap_or_default();
        config.extension.pipeline_flush = parse_bool(&pipeline_flush_str);

        // NEW_RELIC_LMI_FLUSH_INTERVAL_MS: heartbeat flush cadence for the LMI
        // event loop. Floored at 1000 ms to avoid pathological flush storms.
        // Default 30_000 ms.
        config.extension.lmi_flush_interval_ms = env::var("NEW_RELIC_LMI_FLUSH_INTERVAL_MS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(30_000)
            .max(1000);

        let synchronous_flush_str =
            env::var("NEW_RELIC_EXTENSION_SYNCHRONOUS_FLUSH").unwrap_or_default();
        config.new_relic.synchronous_flush = parse_bool(&synchronous_flush_str);

        // synchronous_flush takes precedence over pipeline_flush on any invocation
        // where an immediate send is exercised (see event_loop.rs
        // execute_standard_mode_event_loop) — the delivery guarantee the customer
        // explicitly opted into must not be silently defeated by the deferred-send
        // optimization. Surface that trade-off once at startup rather than leaving it silent.
        if config.extension.pipeline_flush && config.new_relic.synchronous_flush {
            warn!(
                "NEW_RELIC_EXTENSION_SYNCHRONOUS_FLUSH=true overrides NEW_RELIC_EXTENSION_PIPELINE_FLUSH's \
                 deferred-send behavior on invocations where an immediate send is exercised — you \
                 will not get pipeline_flush's billed-duration savings on those invocations. This is \
                 intentional: the delivery guarantee takes precedence over the throughput optimization."
            );
        }

        config
    }


}

/// A custom log formatter that prepends `[NR_EXT]` and follows the desired format.
/// Conditional output based on NEW_RELIC_EXTENSION_LOGS_ENABLED environment variable.
struct CustomFormatter {
    enabled: bool,
}

impl<S, N> FormatEvent<S, N> for CustomFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: fmt::format::Writer<'_>,
        event: &Event<'_>,
    ) -> std::fmt::Result {
        // If extension logs are disabled, skip formatting (suppresses output)
        if !self.enabled {
            return Ok(());
        }

        write!(writer, "[NR_EXT] ")?;
        let metadata = event.metadata();
        write!(writer, "{} ", metadata.level())?;

        #[cfg(debug_assertions)]
        {
            if let Some(file) = metadata.file() {
                write!(writer, "{}:", file)?;
            }
            if let Some(line) = metadata.line() {
                write!(writer, "{} ", line)?;
            }
        }

        ctx.format_fields(writer.by_ref(), event)?;

        writeln!(writer)
    }
}

/// Parse NR_TAGS environment variable into key-value pairs
/// Format: "key1:value1;key2:value2" (delimiter can be customized via NR_ENV_DELIMITER)
/// 
/// # Example
/// ```
/// std::env::set_var("NR_TAGS", "env:prod;team:backend");
/// let tags = parse_nr_tags();
/// assert_eq!(tags, vec![("env".to_string(), "prod".to_string()), ("team".to_string(), "backend".to_string())]);
/// ```
pub fn parse_nr_tags() -> Vec<(String, String)> {
    let nr_tags = match env::var("NR_TAGS") {
        Ok(tags) if !tags.is_empty() => tags,
        _ => return Vec::new(),
    };

    let delimiter = env::var("NR_ENV_DELIMITER").unwrap_or_else(|_| ";".to_string());

    nr_tags
        .split(&delimiter)
        .filter_map(|tag| {
            let parts: Vec<&str> = tag.split(':').collect();
            if parts.len() == 2 && !parts[0].is_empty() && !parts[1].is_empty() {
                Some((parts[0].trim().to_string(), parts[1].trim().to_string()))
            } else {
                None
            }
        })
        .collect()
}

/// Cached NR_TAGS parsed once at cold start. Use `get_nr_tags()` to access.
static NR_TAGS_CACHE: OnceLock<Vec<(String, String)>> = OnceLock::new();

/// Returns cached NR_TAGS, parsing from environment only on first call (cold start).
/// Subsequent warm-start invocations reuse the cached result with zero allocation.
pub fn get_nr_tags() -> &'static [(String, String)] {
    NR_TAGS_CACHE.get_or_init(parse_nr_tags)
}

/// Maximum length (in Unicode scalar values) allowed for a `NEW_RELIC_LABELS` type or
/// value, per the cross-agent Labels spec (agent-specs/Labels.md). Longer values are
/// truncated with a warning, not rejected.
const MAX_LABEL_LEN: usize = 255;
/// Maximum number of `NEW_RELIC_LABELS` type/value pairs sent per agent run, per the
/// cross-agent Labels spec. Applied to `NEW_RELIC_LABELS`'s own output only — independent
/// of the (uncapped, untouched) `NR_TAGS` output.
const MAX_LABELS: usize = 64;

/// Parse the `NEW_RELIC_LABELS` environment variable into key-value pairs.
///
/// Format: `type1:value1;type2:value2`, per the cross-agent Labels spec
/// (agent-specs/Labels.md). Deliberately independent of `NR_TAGS`/`parse_nr_tags`, which
/// this does not touch or share state with:
/// - The delimiters are fixed (no `NR_ENV_DELIMITER`-style override).
/// - A duplicate label type keeps only the value from its *last* occurrence.
/// - A malformed pair (wrong delimiter count, empty type, or empty value — including a
///   non-leading/trailing empty pair like `foo:bar;;zip:zap`) hard-fails the whole list
///   to empty, with a warning. Purely leading/trailing separators (`;;foo:bar;;`) are
///   stripped and not treated as malformed.
/// - Each type/value longer than 255 chars is truncated, with a warning.
/// - The list is capped at 64 entries, with a warning.
///
/// # Example
/// ```
/// std::env::set_var("NEW_RELIC_LABELS", "env:prod;team:backend");
/// let labels = parse_new_relic_labels();
/// assert_eq!(labels, vec![("env".to_string(), "prod".to_string()), ("team".to_string(), "backend".to_string())]);
/// ```
pub fn parse_new_relic_labels() -> Vec<(String, String)> {
    let raw = match env::var("NEW_RELIC_LABELS") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => return Vec::new(),
    };

    let raw_segments: Vec<&str> = raw.split(';').collect();

    // Strip purely leading/trailing empty segments (stray `;` at either end); a middle
    // empty segment is a malformed pair per spec, not tolerated the same way.
    let Some(start) = raw_segments.iter().position(|s| !s.trim().is_empty()) else {
        // Nothing but separators/whitespace (e.g. ";;;") - no labels configured, not an error.
        return Vec::new();
    };
    let Some(end) = raw_segments.iter().rposition(|s| !s.trim().is_empty()) else {
        return Vec::new();
    };

    let mut pairs: Vec<(String, String)> = Vec::new();

    for segment in &raw_segments[start..=end] {
        let segment = segment.trim();
        if segment.is_empty() {
            warn!(
                "NEW_RELIC_LABELS is malformed (empty label pair between valid entries); discarding all labels"
            );
            return Vec::new();
        }

        let parts: Vec<&str> = segment.split(':').collect();
        let (label_type, label_value) = match parts.as_slice() {
            [t, v] if !t.trim().is_empty() && !v.trim().is_empty() => (t.trim(), v.trim()),
            _ => {
                warn!(
                    "NEW_RELIC_LABELS is malformed at \"{segment}\" (expected exactly one non-empty \"type:value\" pair); discarding all labels"
                );
                return Vec::new();
            }
        };

        let label_type = truncate_label_part(label_type, "label type");
        let label_value = truncate_label_part(label_value, "label value");

        // Duplicate type: last occurrence wins, updating the value in place.
        if let Some(existing) = pairs.iter_mut().find(|(t, _)| *t == label_type) {
            existing.1 = label_value;
        } else {
            pairs.push((label_type, label_value));
        }
    }

    if pairs.len() > MAX_LABELS {
        warn!(
            "NEW_RELIC_LABELS has {} entries, exceeding the {MAX_LABELS}-label limit; truncating",
            pairs.len()
        );
        pairs.truncate(MAX_LABELS);
    }

    pairs
}

/// Truncate a `NEW_RELIC_LABELS` type/value to `MAX_LABEL_LEN` chars, warning if truncated.
fn truncate_label_part(part: &str, kind: &str) -> String {
    if part.chars().count() > MAX_LABEL_LEN {
        let truncated: String = part.chars().take(MAX_LABEL_LEN).collect();
        warn!(
            "NEW_RELIC_LABELS {kind} \"{part}\" exceeds {MAX_LABEL_LEN} characters; truncating to \"{truncated}\""
        );
        truncated
    } else {
        part.to_string()
    }
}

/// Cached `NEW_RELIC_LABELS` parsed once at cold start. Use `get_new_relic_labels()` to access.
static NEW_RELIC_LABELS_CACHE: OnceLock<Vec<(String, String)>> = OnceLock::new();

/// Returns cached `NEW_RELIC_LABELS`, parsing from environment only on first call (cold
/// start). Subsequent warm-start invocations reuse the cached result with zero allocation.
pub fn get_new_relic_labels() -> &'static [(String, String)] {
    NEW_RELIC_LABELS_CACHE.get_or_init(parse_new_relic_labels)
}

/// Global configuration instance
static GLOBAL_CONFIG: OnceLock<ExtensionConfig> = OnceLock::new();

/// Initialize the global configuration and logging
pub fn init_config() -> &'static ExtensionConfig {
    GLOBAL_CONFIG.get_or_init(|| {
        let config = ExtensionConfig::from_env();

        let log_level = if config.extension.log_level.to_lowercase() == "all" {
            "trace".to_string()
        } else {
            config.extension.log_level.clone()
        };

        let filter_directive = format!(
            "newrelic_lambda_extension={},aws_config=info,aws_sdk_lambda=info,aws_smithy_runtime=info,aws_smithy_runtime_api=info,aws_sigv4=info,hyper=info,h2=info,{}",
            log_level,
            log_level
        );

        // Try to create EnvFilter with the configured log level, fallback to "info" if it fails
        let env_filter = match EnvFilter::try_new(&filter_directive) {
            Ok(filter) => filter,
            Err(e) => {
                eprintln!("[NR_EXT] ERROR: Failed to parse log level filter '{}': {}. Falling back to 'info' level.", filter_directive, e);
                let fallback_directive = "newrelic_lambda_extension=info,aws_config=info,aws_sdk_lambda=info,aws_smithy_runtime=info,aws_smithy_runtime_api=info,aws_sigv4=info,hyper=info,h2=info,info";
                EnvFilter::try_new(fallback_directive)
                    .expect("Fallback filter directive should always be valid")
            }
        };

        let subscriber = fmt::Subscriber::builder()
            .with_env_filter(env_filter)
            .event_format(CustomFormatter {
                enabled: config.extension.extension_logs_enabled,
            })
            .finish();

        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");

        debug!("New Relic Lambda Extension v{} started", env!("CARGO_PKG_VERSION"));

        config
    })
}

#[cfg(test)]
mod mod_test;