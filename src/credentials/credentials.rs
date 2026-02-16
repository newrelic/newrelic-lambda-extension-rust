//! New Relic Lambda Extension Credentials Module
//! 
//! This module provides functionality to fetch the New Relic license key from:
//! - Environment variables (NEW_RELIC_LICENSE_KEY)
//! - AWS Secrets Manager (via NEW_RELIC_LICENSE_KEY_SECRET env var or configuration)
//! - AWS Systems Manager Parameter Store (via NEW_RELIC_LICENSE_KEY_SSM_PARAMETER_NAME env var or configuration)

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_secretsmanager::Client as SecretsManagerClient;
use aws_sdk_ssm::Client as SsmClient;
use serde::{Deserialize, Serialize};
use std::sync::OnceLock;
use tracing::{debug, warn};
use crate::config::Configuration;

/// License key secret structure for JSON parsing
#[derive(Debug, Serialize, Deserialize)]
struct LicenseKeySecret {
    #[serde(rename = "LicenseKey")]
    license_key: String,
}

/// Trait for Secrets Manager operations (for testing/mocking)
#[async_trait]
trait SecretsManagerAPI {
    async fn get_secret_value(&self, secret_id: &str) -> Result<String>;
}

/// Trait for SSM operations (for testing/mocking)
#[async_trait]
trait SsmAPI {
    async fn get_parameter(&self, parameter_name: &str) -> Result<String>;
}

/// Default Secrets Manager implementation
struct DefaultSecretsManager {
    client: SecretsManagerClient,
}

impl DefaultSecretsManager {
    fn new(client: SecretsManagerClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl SecretsManagerAPI for DefaultSecretsManager {
    async fn get_secret_value(&self, secret_id: &str) -> Result<String> {
        let response = self
            .client
            .get_secret_value()
            .secret_id(secret_id)
            .send()
            .await?;

        response
            .secret_string()
            .ok_or_else(|| anyhow!("Secret string not found"))
            .map(|s| s.to_string())
    }
}

/// Default SSM implementation
struct DefaultSsm {
    client: SsmClient,
}

impl DefaultSsm {
    fn new(client: SsmClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl SsmAPI for DefaultSsm {
    async fn get_parameter(&self, parameter_name: &str) -> Result<String> {
        let response = self
            .client
            .get_parameter()
            .name(parameter_name)
            .with_decryption(true)
            .send()
            .await?;

        response
            .parameter()
            .and_then(|p| p.value())
            .ok_or_else(|| anyhow!("Parameter value not found"))
            .map(|s| s.to_string())
    }
}

/// Global AWS configuration and clients
struct AwsClients {
    secrets_manager: DefaultSecretsManager,
    ssm: DefaultSsm,
}

static AWS_CLIENTS: OnceLock<AwsClients> = OnceLock::new();

/// Default secret ID for license key
const DEFAULT_SECRET_ID: &str = "NEW_RELIC_LICENSE_KEY";

/// Initialize AWS clients with maximum performance optimizations
async fn initialize_aws_clients() -> Result<()> {
    use std::time::Duration;

    // Already initialized — idempotent guard
    if AWS_CLIENTS.get().is_some() {
        return Ok(());
    }

    if std::env::var("AWS_LAMBDA_RUNTIME_API").is_err() {
        return Err(anyhow!("Not in AWS Lambda environment, skipping AWS client initialization"));
    }

    let config_future = tokio::spawn(async {
        aws_config::defaults(BehaviorVersion::latest())
            .retry_config(aws_config::retry::RetryConfig::disabled())
            .load()
            .await
    });

    let config = tokio::time::timeout(
        Duration::from_millis(1000),
        config_future
    ).await
    .map_err(|_| anyhow!("AWS config initialization timeout (1s)"))?
    .map_err(|e| anyhow!("AWS config task failed: {}", e))?;

    // Create both AWS clients directly (sync constructors — no async work,
    // so tokio::spawn on current_thread runtime adds overhead without benefit)
    let secrets_manager = SecretsManagerClient::new(&config);
    let ssm = SsmClient::new(&config);

    let clients = AwsClients {
        secrets_manager: DefaultSecretsManager::new(secrets_manager),
        ssm: DefaultSsm::new(ssm),
    };
    
    AWS_CLIENTS.set(clients).map_err(|_| anyhow!("Failed to store AWS clients"))?;
    
    Ok(())
}

/// Get the AWS clients (will initialize if needed)
async fn get_aws_clients() -> Result<&'static AwsClients> {
    if let Some(clients) = AWS_CLIENTS.get() {
        return Ok(clients);
    }
    
    initialize_aws_clients().await?;
    AWS_CLIENTS.get().ok_or_else(|| anyhow!("AWS clients not initialized"))
}

/// Decode license key from JSON string
pub(crate) fn decode_license_key(raw_json: &str) -> Result<String> {
    let secret: LicenseKeySecret = serde_json::from_str(raw_json)?;
    
    if secret.license_key.is_empty() {
        return Err(anyhow!("malformed license key secret; missing LicenseKey attribute"));
    }
    
    Ok(secret.license_key)
}

/// Try to get license key from Secrets Manager
async fn try_license_key_from_secret(secret_id: &str) -> Result<String> {
    let clients = get_aws_clients().await
        .map_err(|_| anyhow!("Secrets Manager client not initialized"))?;

    let secret_string = clients.secrets_manager.get_secret_value(secret_id).await?;
    let license_key = decode_license_key(&secret_string)?;

    Ok(license_key)
}

/// Try to get license key from SSM Parameter Store
async fn try_license_key_from_ssm_parameter(parameter_name: &str) -> Result<String> {
    let clients = get_aws_clients().await
        .map_err(|_| anyhow!("SSM client not initialized"))?;

    let parameter_value = clients.ssm.get_parameter(parameter_name).await?;

    Ok(parameter_value)
}

/// Get New Relic license key from AWS sources only.
/// This function is called only when the license key environment variable is not available.
/// Lookup priority: configured Secrets Manager → configured SSM → default name fallback.
pub async fn get_new_relic_license_key(conf: &Configuration) -> Result<String> {
    if let Err(e) = initialize_aws_clients().await {
        warn!("Failed to initialize AWS clients: {}. Skipping AWS credential sources.", e);
        return Err(anyhow!("Failed to initialize AWS clients"));
    }

    // 1. Try Secrets Manager if configured (env var or config file)
    if !conf.license_key_secret_id.is_empty() {
        debug!("Fetching license key from Secrets Manager: {}", conf.license_key_secret_id);
        return try_license_key_from_secret(&conf.license_key_secret_id).await;
    }

    // 2. Try SSM Parameter Store if configured (env var or config file)
    if !conf.license_key_ssm_parameter_name.is_empty() {
        debug!("Fetching license key from SSM Parameter Store: {}", conf.license_key_ssm_parameter_name);
        return try_license_key_from_ssm_parameter(&conf.license_key_ssm_parameter_name).await;
    }

    // 3. Fallback: try default secret/parameter name "NEW_RELIC_LICENSE_KEY"
    if let Ok(license_key) = try_license_key_from_secret(DEFAULT_SECRET_ID).await {
        return Ok(license_key);
    }

    if let Ok(license_key) = try_license_key_from_ssm_parameter(DEFAULT_SECRET_ID).await {
        return Ok(license_key);
    }

    Err(anyhow!("No license key found from any AWS source"))
}