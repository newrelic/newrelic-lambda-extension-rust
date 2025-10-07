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
    // New Relic Configuration
    pub new_relic: NewRelicConfig,

    // AWS Lambda Configuration
    pub aws: AwsConfig,

    // Extension Configuration
    pub extension: ExtensionSettings,
}

/// New Relic specific configuration
#[derive(Debug, Clone)]
pub struct NewRelicConfig {
    /// Enable/disable the New Relic Lambda Extension
    pub extension_enabled: bool,

    /// New Relic License Key for authentication
    pub license_key: Option<String>,

    /// AWS Secrets Manager secret ID for license key
    pub license_key_secret_id: String,

    /// AWS SSM Parameter Store parameter name for license key
    pub license_key_ssm_parameter_name: String,

    /// Original Lambda handler (before wrapping)
    pub lambda_handler: Option<String>,

    /// New Relic telemetry endpoint URL
    pub telemetry_endpoint: String,

    /// New Relic log endpoint URL
    pub log_endpoint: String,

    /// The interval at which to send data to New Relic
    pub harvest_interval: Duration,

    /// Enable/disable trace ID collection from agent payloads
    pub collect_trace_id: bool,
}

/// AWS Lambda specific configuration
#[derive(Debug, Clone)]
pub struct AwsConfig {
    /// AWS Lambda Runtime API endpoint
    pub runtime_api: String,

    /// Lambda function name
    pub function_name: String,

    /// Lambda function version (from registration response)
    pub function_version: Option<String>,

    /// AWS account ID (from registration response)  
    pub account_id: Option<String>,

    /// AWS region (extracted from runtime API endpoint or environment)
    pub region: Option<String>,
}

/// Extension specific settings
#[derive(Debug, Clone)]
pub struct ExtensionSettings {
    /// Whether to subscribe to function telemetry/logs (Lambda 'function' type)
    pub send_function_logs: bool,

    /// Whether to subscribe to extension logs (Lambda 'extension' type)
    pub send_extension_logs: bool,

    /// Log level for the extension (info, debug, trace, all, error, warn)
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
            harvest_interval: Duration::from_secs(2), // More frequent flushing
            collect_trace_id: false, // Disabled by default
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
    /// Construct the complete Lambda function ARN using registration details
    /// Format: arn:aws:lambda:region:account-id:function:function-name  
    /// This matches the Go implementation: getLambdaARN()
    pub fn construct_function_arn(&self) -> Option<String> {
        // Get account ID from registration response
        let account_id = self.account_id.as_ref()?.as_str();
        if account_id.is_empty() {
            return None;
        }

        // Get function name from registration response  
        if self.function_name.is_empty() {
            return None;
        }

        // Get region from environment variables (AWS_REGION or AWS_DEFAULT_REGION)
        let region = env::var("AWS_REGION")
            .or_else(|_| env::var("AWS_DEFAULT_REGION"))
            .ok()?;
        
        Some(format!(
            "arn:aws:lambda:{}:{}:function:{}",
            region, account_id, self.function_name
        ))
    }

    /// Update configuration with Lambda registration response details  
    pub fn update_from_registration(&mut self, function_name: String, function_version: String, account_id: Option<String>) {
        self.function_name = function_name;
        self.function_version = Some(function_version);
        self.account_id = account_id;
        
        // Try to extract region from environment if not already set
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
    /// Load configuration from environment variables
    pub fn from_env() -> Self {
        // Read (potential) log forwarding flags early so they can be used in later logic.
        // These are currently not wired into config structs; added per user request.
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
        
        // Parse the trace ID collection flag (accept true/false/1/0/yes/no case-insensitive)
        fn parse_bool(s: &str) -> bool {
            matches!(s.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on")
        }
        let collect_trace_id_str = env::var("NEW_RELIC_COLLECT_TRACE_ID").unwrap_or_default();
        config.new_relic.collect_trace_id = parse_bool(&collect_trace_id_str);

        let license_key_prefix = config.new_relic.license_key.as_deref().unwrap_or("").get(0..2);

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

        // Load AWS Lambda configuration
        if let Ok(runtime_api) = env::var("AWS_LAMBDA_RUNTIME_API") {
            config.aws.runtime_api = runtime_api;
        }

        config.aws.function_name =
            env::var("AWS_LAMBDA_FUNCTION_NAME").unwrap_or(config.aws.function_name);

        // Parse the optional boolean flags (accept true/false/1/0/yes/no case-insensitive)
        config.extension.send_function_logs = parse_bool(&send_function_logs_str);
        config.extension.send_extension_logs = parse_bool(&send_extension_logs_str);

        // Configure logging
        config.extension.log_level = env::var("NEW_RELIC_EXTENSION_LOG_LEVEL").unwrap_or_else(|_| "info".to_string());

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
        // Add the static prefix with proper spacing
        write!(writer, "[NR_EXT] ")?;

        // Add the log level without colons
        let metadata = event.metadata();
        write!(writer, "{} ", metadata.level())?;

        // OPTIMIZATION: Only include file and line numbers in debug builds.
        #[cfg(debug_assertions)]
        {
            if let Some(file) = metadata.file() {
                write!(writer, "{}:", file)?;
            }
            if let Some(line) = metadata.line() {
                write!(writer, "{} ", line)?;
            }
        }

        // Add the message
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

            // Determine log level - support 'all' as alias for 'trace'
            let log_level = if config.extension.log_level.to_lowercase() == "all" {
                "trace".to_string()
            } else {
                config.extension.log_level.clone()
            };

            let env_filter = EnvFilter::new(log_level);

            // Use the custom formatter
            let subscriber = fmt::Subscriber::builder()
                .with_env_filter(env_filter)
                .event_format(CustomFormatter)
                .finish();

            tracing::subscriber::set_global_default(subscriber)
                .expect("setting default subscriber failed");

            // Clean startup - only essential log
            info!("New Relic Lambda Extension started");

            GLOBAL_CONFIG = Some(config);
        });

        #[allow(static_mut_refs)]
        {
            GLOBAL_CONFIG.as_ref().unwrap()
        }
    }
}



