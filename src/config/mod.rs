use std::{env, time::Duration};
use tracing::{info, Event, Subscriber};
use tracing_subscriber::{
    fmt::{self, FmtContext, FormatEvent, FormatFields},
    registry::LookupSpan,
    EnvFilter,
};

/// Global configuration for the New Relic Lambda Extension
#[derive(Debug, Clone)]
pub struct ExtensionConfig {
    pub new_relic: NewRelicConfig,
    pub aws: AwsConfig,
    pub extension: ExtensionSettings,
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
    pub harvest_interval: Duration,
    pub collect_trace_id: bool,
    pub add_version_detail_tags: bool,
    pub apm_lambda_mode: bool,
    pub apm_host: String,
    pub metric_endpoint: String,
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
    pub log_level: String,
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
            apm_lambda_mode: false,
            apm_host: "collector.newrelic.com".to_string(),
            metric_endpoint: "https://metric-api.newrelic.com/metric/v1".to_string(),
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

        let account_id = self.account_id.as_ref()
            .and_then(|id| if id.is_empty() { None } else { Some(id.as_str()) })
            .unwrap_or("123456789012");

        if account_id == "123456789012" {
            info!("Using placeholder account ID - tagging will use actual ARN from invocation event");
        }

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
}

impl Default for ExtensionSettings {
    fn default() -> Self {
        Self {
            send_function_logs: false,
            send_extension_logs: false,
            log_level: "info".to_string(),
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

    pub fn from_env() -> Self {
        let send_function_logs_str = env::var("NEW_RELIC_EXTENSION_SEND_FUNCTION_LOGS").unwrap_or_default();
        let send_extension_logs_str = env::var("NEW_RELIC_EXTENSION_SEND_EXTENSION_LOGS").unwrap_or_default();

        let mut config = Self::default();

        // Load New Relic configuration
        config.new_relic.extension_enabled = env::var("NEW_RELIC_LAMBDA_EXTENSION_ENABLED")
            .unwrap_or_else(|_| "true".to_string())
            .parse()
            .unwrap_or(true);

        config.new_relic.license_key = env::var("NEW_RELIC_LICENSE_KEY").ok();
        config.new_relic.license_key_secret_id = env::var("NEW_RELIC_LICENSE_KEY_SECRET_ID").unwrap_or_default();
        config.new_relic.license_key_ssm_parameter_name = env::var("NEW_RELIC_LICENSE_KEY_SSM_PARAMETER_NAME").unwrap_or_default();
        config.new_relic.lambda_handler = env::var("NEW_RELIC_LAMBDA_HANDLER").ok();
        
        fn parse_bool(s: &str) -> bool {
            matches!(s.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on")
        }
        let collect_trace_id_str = env::var("NEW_RELIC_COLLECT_TRACE_ID").unwrap_or_default();
        config.new_relic.collect_trace_id = parse_bool(&collect_trace_id_str);

        let add_version_detail_tags_str = env::var("NEW_RELIC_ADD_VERSION_DETAIL_TAGS").unwrap_or_default();
        config.new_relic.add_version_detail_tags = parse_bool(&add_version_detail_tags_str);

        let apm_lambda_mode_str = env::var("NEW_RELIC_APM_LAMBDA_MODE").unwrap_or_default();
        config.new_relic.apm_lambda_mode = parse_bool(&apm_lambda_mode_str);

        let license_key_prefix = config.new_relic.license_key.as_deref().unwrap_or("").get(0..2);

        if let Ok(host) = env::var("NEW_RELIC_HOST") {
            config.new_relic.apm_host = host;
        } else if let Some("eu") = license_key_prefix {
            config.new_relic.apm_host = "collector.eu01.nr-data.net".to_string();
        }

        if let Ok(endpoint) = env::var("NEW_RELIC_METRIC_ENDPOINT") {
            config.new_relic.metric_endpoint = endpoint;
        } else if let Some("eu") = license_key_prefix {
            config.new_relic.metric_endpoint = "https://metric-api.eu.newrelic.com/metric/v1".to_string();
        }

        if let Ok(endpoint) = env::var("NEW_RELIC_TELEMETRY_ENDPOINT") {
            config.new_relic.telemetry_endpoint = endpoint;
        } else if let Some("eu") = license_key_prefix {
            config.new_relic.telemetry_endpoint =
                "https://cloud-collector.eu01.nr-data.net/aws/lambda/v1".to_string();
        }

        if let Ok(endpoint) = env::var("NEW_RELIC_LOG_ENDPOINT") {
            config.new_relic.log_endpoint = endpoint;
        } else if let Some("eu") = license_key_prefix {
            config.new_relic.log_endpoint = "https://log-api.eu.newrelic.com/log/v1".to_string();
        }

        if let Ok(runtime_api) = env::var("AWS_LAMBDA_RUNTIME_API") {
            config.aws.runtime_api = runtime_api;
        }

        config.aws.function_name =
            env::var("AWS_LAMBDA_FUNCTION_NAME").unwrap_or(config.aws.function_name);

        config.extension.send_function_logs = parse_bool(&send_function_logs_str);
        config.extension.send_extension_logs = parse_bool(&send_extension_logs_str);

        let raw_log_level = env::var("NEW_RELIC_EXTENSION_LOG_LEVEL").unwrap_or_else(|_| "info".to_string());
        config.extension.log_level = Self::validate_log_level(&raw_log_level);

        config
    }


}

/// A custom log formatter that prepends `[NR_EXT]` and follows the desired format.
struct CustomFormatter;

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
                "newrelic_lambda_extension={},aws_config=info,aws_sdk_lambda=info,aws_smithy_runtime=info,aws_smithy_runtime_api=info,hyper=info,h2=info,{}",
                log_level,
                log_level
            );

            // Try to create EnvFilter with the configured log level, fallback to "info" if it fails
            let env_filter = match EnvFilter::try_new(&filter_directive) {
                Ok(filter) => filter,
                Err(e) => {
                    eprintln!("[NR_EXT] ERROR: Failed to parse log level filter '{}': {}. Falling back to 'info' level.", filter_directive, e);
                    let fallback_directive = "newrelic_lambda_extension=info,aws_config=info,aws_sdk_lambda=info,aws_smithy_runtime=info,aws_smithy_runtime_api=info,hyper=info,h2=info,info";
                    EnvFilter::try_new(fallback_directive)
                        .expect("Fallback filter directive should always be valid")
                }
            };

            let subscriber = fmt::Subscriber::builder()
                .with_env_filter(env_filter)
                .event_format(CustomFormatter)
                .finish();

            tracing::subscriber::set_global_default(subscriber)
                .expect("setting default subscriber failed");

            info!("New Relic Lambda Extension v{} started", env!("CARGO_PKG_VERSION"));

            GLOBAL_CONFIG = Some(config);
        });

        #[allow(static_mut_refs)]
        {
            GLOBAL_CONFIG.as_ref().unwrap()
        }
    }
}



