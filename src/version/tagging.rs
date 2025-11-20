//! Lambda function tagging functionality
//!
//! This module handles tagging the Lambda function with New Relic version information.
//! Tags are applied asynchronously in the background to avoid blocking cold start.

use tracing::{debug, info, warn};
use std::collections::HashMap;

/// Tags the Lambda function with New Relic version information
///
/// This function runs asynchronously and does not block the caller.
/// It requires the Lambda execution role to have `lambda:TagResource` permission.
pub async fn tag_lambda_function_with_versions(
    extension_version: String,
    agent_version: Option<String>,
    layer_version: Option<String>,
    function_arn: String,
) {
    debug!("Starting Lambda function tagging process...");
    info!("Tagging Lambda function: {}", function_arn);

    let mut tags = HashMap::new();
    tags.insert(
        "newrelic.extension.version".to_string(),
        extension_version.clone(),
    );

    if let Some(agent_ver) = agent_version {
        tags.insert("newrelic.agent.version".to_string(), agent_ver);
    }


    if let Some(layer_ver) = layer_version {
        tags.insert("newrelic.layer.version".to_string(), layer_ver);
    }

    info!("Tagging Lambda function with {} version tags", tags.len());
    for (key, value) in &tags {
        debug!("  {}: {}", key, value);
    }

    match apply_tags_to_function(&function_arn, tags).await {
        Ok(_) => {
            info!("Successfully tagged Lambda function with New Relic version information");
        }
        Err(e) => {
            warn!("Failed to tag Lambda function: {}", e);
            warn!("Note: Lambda execution role needs 'lambda:TagResource' permission");
        }
    }
}

/// Apply tags to Lambda function using AWS SDK
async fn apply_tags_to_function(
    function_arn: &str,
    tags: HashMap<String, String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    debug!("Loading AWS config for tagging...");
    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;

    debug!("Creating Lambda client...");
    let lambda_client = aws_sdk_lambda::Client::new(&config);

    debug!("Calling TagResource API...");
    match lambda_client
        .tag_resource()
        .resource(function_arn)
        .set_tags(Some(tags))
        .send()
        .await
    {
        Ok(_) => {
            debug!("TagResource API call successful");
            Ok(())
        }
        Err(e) => {
            warn!("TagResource API call failed: {:?}", e);

            let error_msg = format!("{:?}", e);
            if error_msg.contains("AccessDenied") || error_msg.contains("Unauthorized") {
                warn!("Access denied - check IAM permissions");
            } else if error_msg.contains("ResourceNotFound") {
                warn!("Function ARN not found: {}", function_arn);
            }

            Err(Box::new(e))
        }
    }
}

/// Spawn background task to tag Lambda function
///
/// This function spawns a background task and returns immediately,
/// ensuring it doesn't block the cold start process.
pub fn tag_lambda_function_background(
    extension_version: String,
    agent_version: Option<String>,
    layer_version: Option<String>,
    function_arn: String,
) {
    tokio::spawn(async move {
        tag_lambda_function_with_versions(
            extension_version,
            agent_version,
            layer_version,
            function_arn,
        )
        .await;
    });
    debug!("Lambda function tagging task spawned in background");
}
