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
}

/// AWS Lambda specific configuration
#[derive(Debug, Clone)]
pub struct AwsConfig {
    /// AWS Lambda Runtime API endpoint
    pub runtime_api: String,

    /// Lambda function name
    pub function_name: String,
}

/// Extension specific settings
#[derive(Debug, Clone)]
pub struct ExtensionSettings {
    /// Extension name
    pub name: String,

    /// Maximum telemetry items per batch
    pub max_batch_items: usize,

    /// Maximum telemetry batch size in bytes
    pub max_batch_size: usize,

    /// Telemetry timeout in milliseconds
    pub telemetry_timeout: u64,
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
        }
    }
}

impl Default for AwsConfig {
    fn default() -> Self {
        Self {
            runtime_api: "127.0.0.1:9001".to_string(),
            function_name: "unknown".to_string(),
        }
    }
}

impl Default for ExtensionSettings {
    fn default() -> Self {
        Self {
            name: "newrelic-lambda-extension".to_string(),
            max_batch_size: 262_144, // 256KB
            max_batch_items: 1000,
            telemetry_timeout: 25, // 25ms for immediate delivery
        }
    }
}

impl ExtensionConfig {
    /// Load configuration from environment variables
    pub fn from_env() -> Self {
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
        // Add the static prefix
        write!(writer, "[NR_EXT]")?;

        // Add the log level
        let metadata = event.metadata();
        write!(writer, ":{}:", metadata.level())?;

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

            // Read log level from NEW_RELIC_EXTENSION_LOG_LEVEL, defaulting to "info"
            let log_level = env::var("NEW_RELIC_EXTENSION_LOG_LEVEL")
                .unwrap_or_else(|_| "info".to_string());
            let env_filter = EnvFilter::new(log_level);

            // Use the custom formatter
            let subscriber = fmt::Subscriber::builder()
                .with_env_filter(env_filter)
                .event_format(CustomFormatter)
                .finish();

            tracing::subscriber::set_global_default(subscriber)
                .expect("setting default subscriber failed");

            info!("[Config] New Relic Lambda Extension configuration loaded");
            info!(
                "[Config] Extension enabled: {}",
                config.new_relic.extension_enabled
            );
            info!(
                "[Config] License key: {}",
                if config.new_relic.license_key.is_some() {
                    "Set"
                } else {
                    "Not set"
                }
            );
            info!("[Config] Telemetry endpoint: {}", config.new_relic.telemetry_endpoint);
            info!("[Config] Log endpoint: {}", config.new_relic.log_endpoint);

            GLOBAL_CONFIG = Some(config);
        });

        GLOBAL_CONFIG.as_ref().unwrap()
    }
}

/// Get the global configuration
pub fn get_config() -> &'static ExtensionConfig {
    unsafe {
        GLOBAL_CONFIG
            .as_ref()
            .unwrap_or_else(|| {
                panic!("Configuration not initialized. Call init_config() first.");
            })
    }
}

