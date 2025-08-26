//! Configuration Module
//! 
//! This module handles all configuration for the New Relic Lambda Extension,
//! including New Relic specific settings and AWS Lambda environment variables.

use std::env;
use std::time::Duration;

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
    
    /// Original Lambda handler (before wrapping)
    pub lambda_handler: Option<String>,
    
    /// New Relic telemetry endpoint URL
    pub telemetry_endpoint: String,
    
    /// New Relic log endpoint URL
    pub log_endpoint: String,
    
    /// New Relic metric endpoint URL
    pub metric_endpoint: String,
    
    /// New Relic host for API requests
    pub host: String,
    
    /// Data collection timeout in milliseconds
    pub data_collection_timeout: Duration,
    
    /// Extension log level
    pub extension_log_level: String,
    
    /// Enable/disable extension logs
    pub extension_logs_enabled: bool,
    
    /// Send function logs to New Relic
    pub send_function_logs: bool,
    
    /// Send extension logs to New Relic
    pub send_extension_logs: bool,
    
    /// Collect trace ID from Lambda context
    pub collect_trace_id: bool,
}

/// AWS Lambda specific configuration
#[derive(Debug, Clone)]
pub struct AwsConfig {
    /// AWS Lambda Runtime API endpoint
    pub runtime_api: String,
    
    /// Lambda function name
    pub function_name: String,
    
    /// Lambda function version
    pub function_version: String,
    
    /// Lambda execution environment
    pub execution_env: String,
    
    /// AWS region
    pub region: Option<String>,
    
    /// Lambda task root directory
    pub task_root: String,
    
    /// Lambda runtime directory
    pub runtime_dir: String,
}

/// Extension specific settings
#[derive(Debug, Clone)]
pub struct ExtensionSettings {
    /// Extension name
    pub name: String,
    
    /// Telemetry server host
    pub telemetry_host: String,
    
    /// Telemetry server port
    pub telemetry_port: u16,
    
    /// Maximum telemetry batch size in bytes
    pub max_batch_size: usize,
    
    /// Maximum telemetry items per batch
    pub max_batch_items: usize,
    
    /// Telemetry timeout in milliseconds
    pub telemetry_timeout: u64,
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
            lambda_handler: None,
            telemetry_endpoint: "https://log-api.newrelic.com/log/v1".to_string(),
            log_endpoint: "https://log-api.newrelic.com/log/v1".to_string(),
            metric_endpoint: "https://metric-api.newrelic.com/metric/v1".to_string(),
            host: "log-api.newrelic.com".to_string(),
            data_collection_timeout: Duration::from_millis(10000),
            extension_log_level: "INFO".to_string(),
            extension_logs_enabled: true,
            send_function_logs: true,
            send_extension_logs: false,
            collect_trace_id: true,
        }
    }
}

impl Default for AwsConfig {
    fn default() -> Self {
        Self {
            runtime_api: "127.0.0.1:9001".to_string(),
            function_name: "unknown".to_string(),
            function_version: "$LATEST".to_string(),
            execution_env: "AWS_Lambda_provided".to_string(),
            region: None,
            task_root: "/var/task".to_string(),
            runtime_dir: "/var/runtime".to_string(),
        }
    }
}

impl Default for ExtensionSettings {
    fn default() -> Self {
        Self {
            name: "newrelic-lambda-extension".to_string(),
            telemetry_host: "sandbox.localdomain".to_string(),
            telemetry_port: 4243,
            max_batch_size: 262144, // 256KB
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
        
        config.new_relic.lambda_handler = env::var("NEW_RELIC_LAMBDA_HANDLER").ok();
        
        if let Ok(endpoint) = env::var("NEW_RELIC_TELEMETRY_ENDPOINT") {
            config.new_relic.telemetry_endpoint = endpoint;
        }
        
        if let Ok(endpoint) = env::var("NEW_RELIC_LOG_ENDPOINT") {
            config.new_relic.log_endpoint = endpoint;
        }
        
        if let Ok(endpoint) = env::var("NEW_RELIC_METRIC_ENDPOINT") {
            config.new_relic.metric_endpoint = endpoint;
        }
        
        if let Ok(host) = env::var("NEW_RELIC_HOST") {
            config.new_relic.host = host;
        }
        
        if let Ok(timeout_str) = env::var("NEW_RELIC_DATA_COLLECTION_TIMEOUT") {
            if let Ok(timeout_ms) = timeout_str.parse::<u64>() {
                config.new_relic.data_collection_timeout = Duration::from_millis(timeout_ms);
            }
        }
        
        if let Ok(log_level) = env::var("NEW_RELIC_EXTENSION_LOG_LEVEL") {
            config.new_relic.extension_log_level = log_level;
        }
        
        config.new_relic.extension_logs_enabled = env::var("NEW_RELIC_EXTENSION_LOGS_ENABLED")
            .unwrap_or_else(|_| "true".to_string())
            .parse()
            .unwrap_or(true);
            
        config.new_relic.send_function_logs = env::var("NEW_RELIC_EXTENSION_SEND_FUNCTION_LOGS")
            .unwrap_or_else(|_| "true".to_string())
            .parse()
            .unwrap_or(true);
            
        config.new_relic.send_extension_logs = env::var("NEW_RELIC_EXTENSION_SEND_EXTENSION_LOGS")
            .unwrap_or_else(|_| "false".to_string())
            .parse()
            .unwrap_or(false);
            
        config.new_relic.collect_trace_id = env::var("NEW_RELIC_COLLECT_TRACE_ID")
            .unwrap_or_else(|_| "true".to_string())
            .parse()
            .unwrap_or(true);
        
        // Load AWS Lambda configuration
        if let Ok(runtime_api) = env::var("AWS_LAMBDA_RUNTIME_API") {
            config.aws.runtime_api = runtime_api;
        }
        
        config.aws.function_name = env::var("AWS_LAMBDA_FUNCTION_NAME")
            .unwrap_or_else(|_| config.aws.function_name);
            
        config.aws.function_version = env::var("AWS_LAMBDA_FUNCTION_VERSION")
            .unwrap_or_else(|_| config.aws.function_version);
            
        config.aws.execution_env = env::var("AWS_EXECUTION_ENV")
            .unwrap_or_else(|_| config.aws.execution_env);
            
        config.aws.region = env::var("AWS_REGION").ok()
            .or_else(|| env::var("AWS_DEFAULT_REGION").ok());
            
        config.aws.task_root = env::var("LAMBDA_TASK_ROOT")
            .unwrap_or_else(|_| config.aws.task_root);
            
        config.aws.runtime_dir = env::var("LAMBDA_RUNTIME_DIR")
            .unwrap_or_else(|_| config.aws.runtime_dir);
        
        config
    }
    
    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        if !self.new_relic.extension_enabled {
            return Ok(()); // Skip validation if extension is disabled
        }
        
        if self.new_relic.license_key.is_none() {
            return Err("NEW_RELIC_LICENSE_KEY is required when extension is enabled".to_string());
        }
        
        if self.new_relic.telemetry_endpoint.is_empty() {
            return Err("NEW_RELIC_TELEMETRY_ENDPOINT cannot be empty".to_string());
        }
        
        if self.new_relic.log_endpoint.is_empty() {
            return Err("NEW_RELIC_LOG_ENDPOINT cannot be empty".to_string());
        }
        
        if self.new_relic.metric_endpoint.is_empty() {
            return Err("NEW_RELIC_METRIC_ENDPOINT cannot be empty".to_string());
        }
        
        Ok(())
    }
    
    /// Get the telemetry destination URI for AWS Lambda
    pub fn telemetry_destination_uri(&self) -> String {
        format!("http://{}:{}/telemetry", self.extension.telemetry_host, self.extension.telemetry_port)
    }
    
    /// Get the telemetry server socket address
    pub fn telemetry_socket_addr(&self) -> std::net::SocketAddr {
        use std::net::SocketAddr;
        SocketAddr::from(([0, 0, 0, 0], self.extension.telemetry_port))
    }
    
    /// Get the AWS Lambda Runtime API URL for telemetry subscription
    pub fn telemetry_subscription_url(&self) -> String {
        format!("http://{}/2022-07-01/telemetry", self.aws.runtime_api)
    }
    
    /// Check if the extension should process function logs
    pub fn should_process_function_logs(&self) -> bool {
        self.new_relic.extension_enabled && self.new_relic.send_function_logs
    }
    
    /// Check if the extension should process extension logs
    pub fn should_process_extension_logs(&self) -> bool {
        self.new_relic.extension_enabled && self.new_relic.send_extension_logs
    }
    
    /// Get New Relic authentication headers
    pub fn new_relic_headers(&self) -> Vec<(&'static str, String)> {
        let mut headers = vec![
            ("Content-Type", "application/json".to_string()),
            ("User-Agent", format!("newrelic-lambda-extension/{}", env!("CARGO_PKG_VERSION"))),
        ];
        
        if let Some(ref license_key) = self.new_relic.license_key {
            headers.push(("X-License-Key", license_key.clone()));
        }
        
        headers
    }
}

/// Global configuration instance
static mut GLOBAL_CONFIG: Option<ExtensionConfig> = None;
static CONFIG_INIT: std::sync::Once = std::sync::Once::new();

/// Initialize the global configuration
pub fn init_config() -> &'static ExtensionConfig {
    unsafe {
        CONFIG_INIT.call_once(|| {
            let config = ExtensionConfig::from_env();
            
            // Log configuration status
            tracing::info!("🔧 [Config] New Relic Lambda Extension configuration loaded");
            tracing::info!("   📊 Extension enabled: {}", config.new_relic.extension_enabled);
            tracing::info!("   🔑 License key: {}", if config.new_relic.license_key.is_some() { "✅ Set" } else { "❌ Not set" });
            tracing::info!("   🎯 Telemetry endpoint: {}", config.new_relic.telemetry_endpoint);
            tracing::info!("   📝 Function logs: {}", config.new_relic.send_function_logs);
            tracing::info!("   🔧 Extension logs: {}", config.new_relic.send_extension_logs);
            tracing::info!("   🌐 AWS Runtime API: {}", config.aws.runtime_api);
            tracing::info!("   🏷️  Function: {}", config.aws.function_name);
            
            // Validate configuration
            if let Err(e) = config.validate() {
                tracing::error!("❌ [Config] Configuration validation failed: {}", e);
            } else {
                tracing::info!("✅ [Config] Configuration validation passed");
            }
            
            GLOBAL_CONFIG = Some(config);
        });
        
        GLOBAL_CONFIG.as_ref().unwrap()
    }
}

/// Get the global configuration
pub fn get_config() -> &'static ExtensionConfig {
    unsafe {
        GLOBAL_CONFIG.as_ref().unwrap_or_else(|| {
            panic!("Configuration not initialized. Call init_config() first.");
        })
    }
}
