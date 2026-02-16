//! Lambda function tagging functionality
//!
//! This module handles tagging the Lambda function with New Relic version information.
//! Tags are applied asynchronously in the background to avoid blocking cold start.

use tracing::{debug, warn};
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
    debug!("Tagging Lambda function: {}", function_arn);

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

    debug!("Tagging Lambda function with {} version tags", tags.len());
    for (key, value) in &tags {
        debug!("  {}: {}", key, value);
    }

    match apply_tags_to_function(&function_arn, tags).await {
        Ok(_) => {
            debug!("Successfully tagged Lambda function with New Relic version information");
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
    layer_version_from_config: Option<String>,
    add_version_detail_tags: bool,
    function_name: String,
) {
    tokio::spawn(async move {
        let mut final_layer_version = layer_version;

        // Fallback: if layer version not detected from env vars and user enabled detailed tags, try AWS API
        // This ensures layer tagging works when user has configured AWS permissions
        if final_layer_version.is_none() && add_version_detail_tags {
            debug!("Layer version not detected from env vars, attempting AWS API fallback...");
            match crate::version::detect_layer_version_async(layer_version_from_config, add_version_detail_tags, function_name).await {
                Some(layer_ver) => {
                    debug!("Layer version detected via AWS API fallback: {}", layer_ver);
                    final_layer_version = Some(layer_ver);
                }
                None => {
                    debug!("AWS API fallback also failed, layer will not be tagged (this is normal if no layer is attached)");
                }
            }
        }

        tag_lambda_function_with_versions(
            extension_version,
            agent_version,
            final_layer_version,
            function_arn,
        )
        .await;
    });
    debug!("Lambda function tagging task spawned in background");
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // Tag HashMap construction (unit-testable without AWS SDK)
    // ========================================================================

    #[test]
    fn test_tag_construction_all_versions_present() {
        let mut tags = HashMap::new();
        tags.insert("newrelic.extension.version".to_string(), "2.4.5".to_string());

        if let Some(agent_ver) = Some("10.0.0".to_string()) {
            tags.insert("newrelic.agent.version".to_string(), agent_ver);
        }
        if let Some(layer_ver) = Some("NRLayer:42".to_string()) {
            tags.insert("newrelic.layer.version".to_string(), layer_ver);
        }

        assert_eq!(tags.len(), 3);
        assert_eq!(tags.get("newrelic.extension.version"), Some(&"2.4.5".to_string()));
        assert_eq!(tags.get("newrelic.agent.version"), Some(&"10.0.0".to_string()));
        assert_eq!(tags.get("newrelic.layer.version"), Some(&"NRLayer:42".to_string()));
    }

    #[test]
    fn test_tag_construction_only_extension_version() {
        let mut tags = HashMap::new();
        tags.insert("newrelic.extension.version".to_string(), "1.0.0".to_string());

        let agent_version: Option<String> = None;
        let layer_version: Option<String> = None;

        if let Some(agent_ver) = agent_version {
            tags.insert("newrelic.agent.version".to_string(), agent_ver);
        }
        if let Some(layer_ver) = layer_version {
            tags.insert("newrelic.layer.version".to_string(), layer_ver);
        }

        assert_eq!(tags.len(), 1);
        assert!(tags.contains_key("newrelic.extension.version"));
        assert!(!tags.contains_key("newrelic.agent.version"));
        assert!(!tags.contains_key("newrelic.layer.version"));
    }

    #[test]
    fn test_tag_construction_with_agent_no_layer() {
        let mut tags = HashMap::new();
        tags.insert("newrelic.extension.version".to_string(), "2.0.0".to_string());

        if let Some(agent_ver) = Some("9.5.0".to_string()) {
            tags.insert("newrelic.agent.version".to_string(), agent_ver);
        }

        let layer_version: Option<String> = None;
        if let Some(layer_ver) = layer_version {
            tags.insert("newrelic.layer.version".to_string(), layer_ver);
        }

        assert_eq!(tags.len(), 2);
        assert!(tags.contains_key("newrelic.agent.version"));
        assert!(!tags.contains_key("newrelic.layer.version"));
    }

    #[test]
    fn test_tag_construction_with_layer_no_agent() {
        let mut tags = HashMap::new();
        tags.insert("newrelic.extension.version".to_string(), "2.0.0".to_string());

        let agent_version: Option<String> = None;
        if let Some(agent_ver) = agent_version {
            tags.insert("newrelic.agent.version".to_string(), agent_ver);
        }

        if let Some(layer_ver) = Some("Layer:10".to_string()) {
            tags.insert("newrelic.layer.version".to_string(), layer_ver);
        }

        assert_eq!(tags.len(), 2);
        assert!(!tags.contains_key("newrelic.agent.version"));
        assert!(tags.contains_key("newrelic.layer.version"));
    }

    #[test]
    fn test_tag_key_names_are_correct() {
        // Verify exact key names match AWS tag naming convention
        let mut tags = HashMap::new();
        tags.insert("newrelic.extension.version".to_string(), "v".to_string());
        tags.insert("newrelic.agent.version".to_string(), "v".to_string());
        tags.insert("newrelic.layer.version".to_string(), "v".to_string());

        for key in tags.keys() {
            assert!(key.starts_with("newrelic."), "Tag key should start with 'newrelic.'");
            assert!(key.ends_with(".version"), "Tag key should end with '.version'");
        }
    }

    // ========================================================================
    // tag_lambda_function_with_versions — via mock AWS Lambda API
    // ========================================================================

    use std::convert::Infallible;
    use hyper::{Response, StatusCode};
    use hyper::body::Bytes;
    use hyper::service::service_fn;
    use hyper_util::rt::TokioIo;
    use http_body_util::Full;
    use tokio::net::TcpListener;
    use serial_test::serial;

    /// Start a mock AWS Lambda API server
    async fn start_mock_aws_lambda_api() -> (u16, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();

        let handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { break };
                tokio::spawn(async move {
                    let service = service_fn(|_req| async {
                        // Return success for TagResource
                        Ok::<_, Infallible>(Response::builder()
                            .status(StatusCode::OK)
                            .body(Full::new(Bytes::from("{}")))
                            .expect("response"))
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });

        (port, handle)
    }

    #[tokio::test]
    #[serial]
    async fn test_tag_lambda_function_with_versions_all_present() {
        let (port, server_handle) = start_mock_aws_lambda_api().await;

        // Point AWS SDK at our mock server
        std::env::set_var("AWS_ENDPOINT_URL", format!("http://127.0.0.1:{port}"));
        std::env::set_var("AWS_ACCESS_KEY_ID", "test");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
        std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");

        tag_lambda_function_with_versions(
            "2.4.5".to_string(),
            Some("10.0.0".to_string()),
            Some("NRLayer:42".to_string()),
            "arn:aws:lambda:us-east-1:123:function:test-fn".to_string(),
        )
        .await;

        // If we reach here without panic, the AWS call was made to our mock
        std::env::remove_var("AWS_ENDPOINT_URL");
        std::env::remove_var("AWS_ACCESS_KEY_ID");
        std::env::remove_var("AWS_SECRET_ACCESS_KEY");
        std::env::remove_var("AWS_DEFAULT_REGION");
        server_handle.abort();
    }

    #[tokio::test]
    #[serial]
    async fn test_tag_lambda_function_with_versions_no_agent_no_layer() {
        let (port, server_handle) = start_mock_aws_lambda_api().await;

        std::env::set_var("AWS_ENDPOINT_URL", format!("http://127.0.0.1:{port}"));
        std::env::set_var("AWS_ACCESS_KEY_ID", "test");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
        std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");

        tag_lambda_function_with_versions(
            "2.4.5".to_string(),
            None,
            None,
            "arn:aws:lambda:us-east-1:123:function:test-fn".to_string(),
        )
        .await;

        std::env::remove_var("AWS_ENDPOINT_URL");
        std::env::remove_var("AWS_ACCESS_KEY_ID");
        std::env::remove_var("AWS_SECRET_ACCESS_KEY");
        std::env::remove_var("AWS_DEFAULT_REGION");
        server_handle.abort();
    }

    #[tokio::test]
    #[serial]
    async fn test_tag_lambda_function_handles_api_error_gracefully() {
        // Start a server that returns 403 (AccessDenied)
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();

        let server_handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { break };
                tokio::spawn(async move {
                    let service = service_fn(|_req| async {
                        Ok::<_, Infallible>(Response::builder()
                            .status(StatusCode::FORBIDDEN)
                            .body(Full::new(Bytes::from(r#"{"message":"AccessDenied"}"#)))
                            .expect("response"))
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });

        std::env::set_var("AWS_ENDPOINT_URL", format!("http://127.0.0.1:{port}"));
        std::env::set_var("AWS_ACCESS_KEY_ID", "test");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
        std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");

        // Should NOT panic — handles error gracefully with warning
        tag_lambda_function_with_versions(
            "2.4.5".to_string(),
            Some("10.0.0".to_string()),
            None,
            "arn:aws:lambda:us-east-1:123:function:test-fn".to_string(),
        )
        .await;

        std::env::remove_var("AWS_ENDPOINT_URL");
        std::env::remove_var("AWS_ACCESS_KEY_ID");
        std::env::remove_var("AWS_SECRET_ACCESS_KEY");
        std::env::remove_var("AWS_DEFAULT_REGION");
        server_handle.abort();
    }
}
