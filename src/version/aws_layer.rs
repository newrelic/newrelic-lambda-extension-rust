//! AWS Lambda API integration to fetch layer information

use tracing::{debug, warn};

/// Fetch layer information from AWS Lambda API
pub async fn fetch_layer_info_from_aws(function_name: String) -> Option<String> {
    debug!("Attempting to fetch layer info from AWS Lambda API...");
    debug!("Function name: {}", function_name);

    let config = match aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await {
        config => {
            debug!("AWS config loaded successfully");
            config
        }
    };

    let lambda_client = aws_sdk_lambda::Client::new(&config);
    debug!("Lambda client created");

    debug!("Sending GetFunctionConfiguration request to AWS Lambda API...");
    match lambda_client
        .get_function_configuration()
        .function_name(&function_name)
        .send()
        .await
    {
        Ok(response) => {
            debug!("✓ Successfully retrieved function configuration from AWS");

            let layers = response.layers();
            debug!("Response contains {} layer(s)", layers.len());

            if !layers.is_empty() {
                debug!("Found {} layers attached to function", layers.len());

                for layer in layers {
                    if let Some(arn) = layer.arn() {
                        debug!("Layer ARN: {}", arn);

                        if arn.to_lowercase().contains("newrelic") {
                            if let Some(layer_info) = parse_layer_arn(arn) {
                                debug!("Detected New Relic layer: {}", layer_info);
                                return Some(layer_info);
                            }
                        }
                    }
                }

                if let Some(first_layer) = layers.first() {
                    if let Some(arn) = first_layer.arn() {
                        if let Some(layer_info) = parse_layer_arn(arn) {
                            debug!("Using first layer: {}", layer_info);
                            return Some(layer_info);
                        }
                    }
                }
            }

            if layers.is_empty() {
                debug!("No layers found on function");
            }
        }
        Err(e) => {
            warn!("Failed to fetch function configuration from AWS: {}", e);
            debug!("Error details: {:?}", e);
        }
    }

    None
}

/// Parse layer ARN to extract name and version
/// Format: arn:aws:lambda:region:account:layer:layer-name:version
fn parse_layer_arn(arn: &str) -> Option<String> {
    let parts: Vec<&str> = arn.split(':').collect();

    if parts.len() >= 8 {
        let layer_name = parts[6];
        let layer_version = parts[7];
        Some(format!("{}:{}", layer_name, layer_version))
    } else {
        debug!("Invalid layer ARN format: {}", arn);
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_layer_arn() {
        let arn = "arn:aws:lambda:us-east-1:123456789012:layer:NewRelicPython313X86:93";
        let result = parse_layer_arn(arn);
        assert_eq!(result, Some("NewRelicPython313X86:93".to_string()));
    }

    #[test]
    fn test_parse_invalid_arn() {
        let arn = "invalid-arn";
        let result = parse_layer_arn(arn);
        assert_eq!(result, None);
    }
}
