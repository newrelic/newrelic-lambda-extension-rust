// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

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
    pub add_version_detail_tags: bool,
    pub layer_version: Option<String>,
    pub apm_lambda_mode: bool,
    pub apm_blocking_handshake: bool,
    pub apm_handshake_timeout_secs: u64,
    pub apm_host: String,
    pub metric_endpoint: String,
    pub proxy_url: Option<String>,
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
            add_version_detail_tags: false,
            layer_version: None,
            apm_lambda_mode: false,
            apm_blocking_handshake: false,
            apm_handshake_timeout_secs: 5,
            apm_host: "collector.newrelic.com".to_string(),
            metric_endpoint: "https://metric-api.newrelic.com/metric/v1".to_string(),
            proxy_url: None,
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

    fn extract_account_id_from_arn(arn: &str) -> Option<String> {
        let parts: Vec<&str> = arn.split(':').collect();
        if parts.len() >= 5 && parts[0] == "arn" && parts[2] == "lambda" {
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
            log_level: "info".to_string(),
            extension_logs_enabled: true,
            runtime_done_grace_ms: 25,
            pipeline_flush: false,
            lmi_flush_interval_ms: 30_000,
        }
    }
}

impl ExtensionConfig {
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
    /// Accepts comma-separated values: platform, extension, function, all. Returns (send_function_logs, send_extension_logs, send_platform_logs)
    fn parse_send_logs(value: &str) -> (bool, bool, bool) {
        let normalized = value.to_lowercase();
        let parts: Vec<&str> = normalized.split(',').map(|s| s.trim()).collect();
        
        // NEW: Check for empty string
        if normalized.is_empty() {
            eprintln!("NEW_RELIC_EXTENSION_SEND_LOGS is empty. No logs will be sent");
            return (false, false, false);
        }
        // Check for "all" first
        if parts.contains(&"all") {
           if parts.len() > 1 {
                eprintln!("[NR_EXT] INFO: 'all' specified in SEND_LOGS;defaulting to 'all'");
            }
            return (true, true, true);
        }
        
        let send_function = parts.contains(&"function");
        let send_extension = parts.contains(&"extension");
        let send_platform = parts.contains(&"platform");
        
        (send_function, send_extension, send_platform)
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

        config.new_relic.proxy_url = env::var("NEW_RELIC_LAMBDA_EXTENSION_PROXY")
            .ok()
            .filter(|s| !s.is_empty());

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
            let (function, extension, platform) = Self::parse_send_logs(&send_logs_str);
            config.extension.send_function_logs = function;
            config.extension.send_extension_logs = extension;
            config.extension.send_platform_logs = platform;
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

/// Global configuration instance
static mut GLOBAL_CONFIG: Option<ExtensionConfig> = None;
static CONFIG_INIT: std::sync::Once = std::sync::Once::new();

/// Initialize the global configuration and logging
pub fn init_config() -> &'static ExtensionConfig {
    unsafe {
        CONFIG_INIT.call_once(|| {
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

            GLOBAL_CONFIG = Some(config);
        });

        #[allow(static_mut_refs)]
        {
            GLOBAL_CONFIG.as_ref().unwrap()
        }
    }
}

#[cfg(test)]
mod mod_test;