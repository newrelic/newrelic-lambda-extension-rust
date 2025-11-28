//! Version detection module for agent, extension, and layer versions

mod aws_layer;
pub mod tagging;

use std::fs;
use std::path::Path;
use std::sync::Arc;
use once_cell::sync::OnceCell;
use tracing::{debug, warn};

/// Global cache for version information (detected once, reused everywhere)
static VERSION_INFO_CACHE: OnceCell<Arc<VersionInfo>> = OnceCell::new();

/// Extension version from Cargo.toml
const EXTENSION_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Agent paths for different runtimes (layer installations)
const LAYER_AGENT_PATH_NODE: &[&str] = &["/opt/nodejs/node_modules/newrelic"];
const LAYER_AGENT_PATHS_PYTHON: &[&str] = &[
    "/opt/python/newrelic",
    "/opt/python/lib/python3.13/site-packages/newrelic",
    "/opt/python/lib/python3.12/site-packages/newrelic",
    "/opt/python/lib/python3.11/site-packages/newrelic",
    "/opt/python/lib/python3.10/site-packages/newrelic",
    "/opt/python/lib/python3.9/site-packages/newrelic",
];
const LAYER_AGENT_PATH_DOTNET: &[&str] = &["/opt/lib/newrelic-dotnet-agent"];
const LAYER_AGENT_PATHS_RUBY: &[&str] = &[
    "/opt/ruby/gems/3.2.0/gems/newrelic_rpm",
    "/opt/ruby/gems/3.3.0/gems/newrelic_rpm",
];

/// Vendor agent paths (customer installed agents)
const VENDOR_AGENT_PATH_NODE: &str = "/var/task/node_modules/newrelic";
const VENDOR_AGENT_PATH_PYTHON: &str = "/var/task/newrelic";
const VENDOR_AGENT_PATH_RUBY: &str = "/var/task/vendor/bundle/ruby/3.3.0/gems/newrelic_rpm";

/// Version information structure
#[derive(Debug, Clone)]
pub struct VersionInfo {
    pub agent_version: Option<String>,
    pub agent_name: Option<String>,
    pub extension_version: String,
    pub layer_version: Option<String>,
}

impl VersionInfo {
    /// Create version info with detected versions
    /// Uses fast synchronous detection (env vars, filesystem) - no AWS API calls
    pub fn detect() -> Self {
        debug!("=== Starting version detection (sync) ===");
        let (agent_version, agent_name) = detect_agent_version();
        let extension_version = EXTENSION_VERSION.to_string();

        // Detect layer version from environment variables (fast, no AWS API calls)
        let layer_version = detect_layer_version_sync();

        debug!("Version detection complete (sync):");
        debug!("  Extension version: {}", extension_version);
        debug!("  Agent version: {:?} ({})", agent_version, agent_name.as_deref().unwrap_or("none"));
        debug!("  Layer version: {:?}", layer_version);

        Self {
            agent_version,
            agent_name,
            extension_version,
            layer_version,
        }
    }

    /// Async version detection including AWS API calls for layer info
    pub async fn detect_async() -> Self {
        debug!("=== Starting async version detection ===");
        let (agent_version, agent_name) = detect_agent_version();
        let extension_version = EXTENSION_VERSION.to_string();
        let layer_version = detect_layer_version_async().await;

        debug!("Version detection complete:");
        debug!("  Extension version: {}", extension_version);
        debug!("  Agent version: {:?} ({})", agent_version, agent_name.as_deref().unwrap_or("none"));
        debug!("  Layer version: {:?}", layer_version);

        let info = Self {
            agent_version,
            agent_name,
            extension_version,
            layer_version,
        };

        let _ = VERSION_INFO_CACHE.set(Arc::new(info.clone()));

        info
    }

    /// Get cached version info or detect if not cached
    pub fn get_or_detect() -> Arc<VersionInfo> {
        VERSION_INFO_CACHE
            .get_or_init(|| {
                debug!("Version info not cached, detecting synchronously...");
                Arc::new(Self::detect())
            })
            .clone()
    }

    /// Get formatted tags as key-value pairs for New Relic attributes
    pub fn as_tags(&self) -> Vec<(String, String)> {
        let mut tags = Vec::new();

        tags.push((
            "nr.extensionVersion".to_string(),
            self.extension_version.clone(),
        ));

        if let (Some(name), Some(version)) = (&self.agent_name, &self.agent_version) {
            tags.push((
                format!("nr.{}AgentVersion", name),
                version.clone(),
            ));
        }

        if let Some(layer_version) = &self.layer_version {
            tags.push((
                "nr.layerVersion".to_string(),
                layer_version.clone(),
            ));
        }

        tags
    }
}

/// Detect agent version from various paths
fn detect_agent_version() -> (Option<String>, Option<String>) {
    debug!("Starting agent version detection...");

    for path in LAYER_AGENT_PATH_NODE {
        debug!("Checking Node.js layer path: {}", path);
        if let Some(version) = read_nodejs_version(path) {
            debug!("✓ Detected Node.js agent version: {} from {}", version, path);
            return (Some(version), Some("Node".to_string()));
        }
    }

    debug!("Checking Node.js vendor path: {}", VENDOR_AGENT_PATH_NODE);
    if let Some(version) = read_nodejs_version(VENDOR_AGENT_PATH_NODE) {
        debug!("✓ Detected Node.js agent version: {} from {}", version, VENDOR_AGENT_PATH_NODE);
        return (Some(version), Some("Node".to_string()));
    }

    for path in LAYER_AGENT_PATHS_PYTHON {
        debug!("Checking Python layer path: {}", path);
        if let Some(version) = read_python_version(path) {
            debug!("✓ Detected Python agent version: {} from {}", version, path);
            return (Some(version), Some("Python".to_string()));
        }
    }

    debug!("Checking Python vendor path: {}", VENDOR_AGENT_PATH_PYTHON);
    if let Some(version) = read_python_version(VENDOR_AGENT_PATH_PYTHON) {
        debug!("✓ Detected Python agent version: {} from {}", version, VENDOR_AGENT_PATH_PYTHON);
        return (Some(version), Some("Python".to_string()));
    }

    for path in LAYER_AGENT_PATHS_RUBY {
        debug!("Checking Ruby layer path: {}", path);
        if let Some(version) = read_ruby_version(path) {
            debug!("✓ Detected Ruby agent version: {} from {}", version, path);
            return (Some(version), Some("Ruby".to_string()));
        }
    }

    debug!("Checking Ruby vendor path: {}", VENDOR_AGENT_PATH_RUBY);
    if let Some(version) = read_ruby_version(VENDOR_AGENT_PATH_RUBY) {
        debug!("✓ Detected Ruby agent version: {} from {}", version, VENDOR_AGENT_PATH_RUBY);
        return (Some(version), Some("Ruby".to_string()));
    }

    for path in LAYER_AGENT_PATH_DOTNET {
        debug!("Checking .NET layer path: {}", path);
        if let Some(version) = read_dotnet_version(path) {
            debug!("✓ Detected .NET agent version: {} from {}", version, path);
            return (Some(version), Some("Dotnet".to_string()));
        }
    }

    warn!("No agent version detected from any known paths");
    (None, None)
}

/// Read Node.js agent version from package.json
fn read_nodejs_version(base_path: &str) -> Option<String> {
    let package_json_path = format!("{}/package.json", base_path);
    if !Path::new(&package_json_path).exists() {
        return None;
    }

    match fs::read_to_string(&package_json_path) {
        Ok(content) => {
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(&content) {
                if let Some(version) = json.get("version").and_then(|v| v.as_str()) {
                    return Some(version.to_string());
                }
            }
            None
        }
        Err(e) => {
            debug!("Failed to read {}: {}", package_json_path, e);
            None
        }
    }
}

/// Read Python agent version from version.py or __init__.py
fn read_python_version(base_path: &str) -> Option<String> {
    debug!("  Checking if path exists: {}", base_path);
    if !Path::new(base_path).exists() {
        debug!("  Path does not exist");
        return None;
    }

    let version_py_path = format!("{}/version.py", base_path);
    debug!("  Trying version.py: {}", version_py_path);
    if Path::new(&version_py_path).exists() {
        debug!("  version.py exists, attempting to read");
        if let Some(version) = extract_python_version_from_file(&version_py_path) {
            debug!("  Found version in version.py: {}", version);
            return Some(version);
        }
    }

    let init_py_path = format!("{}/__init__.py", base_path);
    debug!("  Trying __init__.py: {}", init_py_path);
    if Path::new(&init_py_path).exists() {
        debug!("  __init__.py exists, attempting to read");
        if let Some(version) = extract_python_version_from_file(&init_py_path) {
            debug!("  Found version in __init__.py: {}", version);
            return Some(version);
        }
    }

    let metadata_path = format!("{}-*.dist-info/METADATA", base_path);
    debug!("  Trying METADATA pattern: {}", metadata_path);

    if let Some(parent) = Path::new(base_path).parent() {
        let dist_info_pattern = format!("{}/newrelic-*.dist-info/METADATA", parent.display());
        debug!("  Trying dist-info METADATA: {}", dist_info_pattern);

        if let Ok(entries) = fs::read_dir(parent) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(dir_name) = path.file_name().and_then(|n| n.to_str()) {
                    if dir_name.starts_with("newrelic-") && dir_name.ends_with(".dist-info") {
                        let metadata_file = path.join("METADATA");
                        debug!("  Found dist-info directory, checking: {}", metadata_file.display());
                        if let Some(version) = extract_python_version_from_metadata(&metadata_file) {
                            debug!("  Found version in METADATA: {}", version);
                            return Some(version);
                        }
                    }
                }
            }
        }
    }

    debug!("  No version found for path: {}", base_path);
    None
}

/// Extract version from Python file
fn extract_python_version_from_file(file_path: &str) -> Option<String> {
    match fs::read_to_string(file_path) {
        Ok(content) => {
           
            for line in content.lines() {
                let line = line.trim();

                if (line.starts_with("__version__") || line.starts_with("version"))
                    && line.contains('=') {
                    if let Some(version_part) = line.split('=').nth(1) {
                        let version = version_part
                            .trim()
                            .trim_matches(|c| c == '\'' || c == '"' || c == ' ');
                        if !version.is_empty() && version.chars().any(|c| c.is_ascii_digit()) {
                            return Some(version.to_string());
                        }
                    }
                }
            }
            None
        }
        Err(e) => {
            debug!("Failed to read {}: {}", file_path, e);
            None
        }
    }
}

/// Extract version from Python METADATA file
fn extract_python_version_from_metadata(metadata_path: &std::path::Path) -> Option<String> {
    match fs::read_to_string(metadata_path) {
        Ok(content) => {
            for line in content.lines() {
                let line = line.trim();
                if line.starts_with("Version:") {
                    if let Some(version) = line.strip_prefix("Version:") {
                        let version = version.trim();
                        if !version.is_empty() {
                            return Some(version.to_string());
                        }
                    }
                }
            }
            None
        }
        Err(e) => {
            debug!("Failed to read {:?}: {}", metadata_path, e);
            None
        }
    }
}

/// Read Ruby agent version
fn read_ruby_version(base_path: &str) -> Option<String> {
    if let Some(dir_name) = Path::new(base_path).file_name() {
        if let Some(name) = dir_name.to_str() {
            if let Some(version_part) = name.split('-').nth(1) {
                return Some(version_part.to_string());
            }
        }
    }

    let version_rb_path = format!("{}/lib/new_relic/version.rb", base_path);
    if Path::new(&version_rb_path).exists() {
        if let Some(version) = extract_ruby_version_from_file(&version_rb_path) {
            return Some(version);
        }
    }

    None
}

/// Extract version from Ruby file
fn extract_ruby_version_from_file(file_path: &str) -> Option<String> {
    match fs::read_to_string(file_path) {
        Ok(content) => {
            for line in content.lines() {
                let line = line.trim();
                if line.starts_with("VERSION") && line.contains('=') {
                    if let Some(version_part) = line.split('=').nth(1) {
                        let version = version_part
                            .trim()
                            .trim_matches(|c| c == '\'' || c == '"' || c == ' ');
                        if !version.is_empty() && version.chars().any(|c| c.is_ascii_digit()) {
                            return Some(version.to_string());
                        }
                    }
                }
            }
            None
        }
        Err(e) => {
            debug!("Failed to read {}: {}", file_path, e);
            None
        }
    }
}

/// Read .NET agent version
fn read_dotnet_version(base_path: &str) -> Option<String> {
    let version_file = format!("{}/VERSION", base_path);
    if Path::new(&version_file).exists() {
        if let Ok(content) = fs::read_to_string(&version_file) {
            let version = content.trim();
            if !version.is_empty() {
                return Some(version.to_string());
            }
        }
    }

    None
}

/// Synchronous layer version detection (no AWS API calls)
/// Only checks environment variables and filesystem - fast enough for first invocation
fn detect_layer_version_sync() -> Option<String> {
    debug!("Detecting layer version (sync - no AWS API calls)...");

    // Check env var first (instant)
    if let Ok(layer_version) = std::env::var("NEW_RELIC_LAYER_VERSION") {
        debug!("Layer version from NEW_RELIC_LAYER_VERSION: {}", layer_version);
        return Some(layer_version);
    }

    // Check AWS_LAMBDA_LAYERS env var (instant, set by Lambda runtime)
    match std::env::var("AWS_LAMBDA_LAYERS") {
        Ok(layers) => {
            debug!("AWS_LAMBDA_LAYERS found: {}", layers);
            if let Some(layer_info) = parse_layer_version_from_env(&layers) {
                return Some(layer_info);
            }
        }
        Err(_) => {
            debug!("AWS_LAMBDA_LAYERS environment variable not set (will try filesystem)");
        }
    }

    // Fall back to filesystem detection (also fast, no network)
    debug!("Checking filesystem for layer markers...");
    detect_layer_from_filesystem()
}

/// Async layer version detection using AWS Lambda API
/// This makes AWS API calls - used as fallback when env vars don't have layer info
/// Public so tagging background task can use it as fallback
pub async fn detect_layer_version_async() -> Option<String> {
    debug!("Detecting layer version (async - includes AWS API calls)...");

    // First try the fast sync methods
    if let Some(layer_version) = detect_layer_version_sync() {
        return Some(layer_version);
    }

    // Only if sync detection failed, make AWS API call
    debug!("Sync detection failed, attempting to fetch layer info from AWS Lambda API...");
    match aws_layer::fetch_layer_info_from_aws().await {
        Some(layer_info) => {
            debug!("✓ Successfully fetched layer info from AWS: {}", layer_info);
            Some(layer_info)
        }
        None => {
            debug!("✗ Failed to fetch layer info from AWS Lambda API");
            None
        }
    }
}

/// Detect layer from filesystem when environment variable is not available
fn detect_layer_from_filesystem() -> Option<String> {
    debug!("Attempting to detect layer from filesystem...");

    let layer_markers = vec![
        "/opt/newrelic-layer-version",
        "/opt/extensions/newrelic-lambda-extension",
    ];

    for marker in layer_markers {
        if std::path::Path::new(marker).exists() {
            debug!("Found layer marker: {}", marker);
        }
    }

    if let Ok(env_file) = std::fs::read_to_string("/proc/self/environ") {
        for env_var in env_file.split('\0') {
            if env_var.starts_with("AWS_LAMBDA") || env_var.contains("layer") {
                debug!("Found in environ: {}", env_var);
            }
        }
    }

    debug!("Could not detect layer version from filesystem");
    None
}

/// Parse layer version from AWS_LAMBDA_LAYERS environment variable
fn parse_layer_version_from_env(layers_str: &str) -> Option<String> {
    for layer in layers_str.split(',') {
        let layer = layer.trim();
        if layer.contains("newrelic") || layer.contains("NewRelic") {
            let parts: Vec<&str> = layer.split(':').collect();
            if parts.len() >= 8 {
                let layer_name = parts[6];
                let layer_version = parts[7];
                return Some(format!("{}:{}", layer_name, layer_version));
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_info_creation() {
        let version_info = VersionInfo {
            agent_version: Some("9.5.0".to_string()),
            agent_name: Some("python".to_string()),
            extension_version: "0.1.0".to_string(),
            layer_version: Some("NewRelicPython313X86:93".to_string()),
        };

        let tags = version_info.as_tags();
        assert!(tags.len() >= 2);
    }

    #[test]
    fn test_parse_layer_version() {
        let layers = "arn:aws:lambda:us-east-1:123456789012:layer:NewRelicPython313X86:93";
        let version = parse_layer_version_from_env(layers);
        assert_eq!(version, Some("NewRelicPython313X86:93".to_string()));
    }
}
