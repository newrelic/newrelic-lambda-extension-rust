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

/// Global cache for runtime version from platform.initStart event
static RUNTIME_VERSION_CACHE: OnceCell<String> = OnceCell::new();

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
const LAYER_AGENT_PATHS_JAVA: &[&str] = &[
    "/opt/java/lib",  // Directory to scan for newrelic JARs
    "/opt/lib",       // Alternative directory to scan
];
const LAYER_AGENT_JAR_NAMES: &[&str] = &[
    "newrelic-java-lambda",  // Prefix for layer JAR (e.g., newrelic-java-lambda-2.2.5.jar)
    "newrelic.jar",          // Generic name
];
const LAYER_AGENT_PATH_DOTNET: &[&str] = &["/opt/lib/newrelic-dotnet-agent"];
const LAYER_AGENT_PATHS_RUBY: &[&str] = &[
    "/opt/ruby/gems/3.2.0/gems/newrelic_rpm",
    "/opt/ruby/gems/3.3.0/gems/newrelic_rpm",
];

/// Vendor agent paths (customer installed agents)
const VENDOR_AGENT_PATH_NODE: &str = "/var/task/node_modules/newrelic";
const VENDOR_AGENT_PATH_PYTHON: &str = "/var/task/newrelic";
const VENDOR_AGENT_PATHS_JAVA: &[&str] = &[
    "/var/task/newrelic/newrelic.jar",
    "/var/runtime/lib/newrelic.jar",
];
const VENDOR_AGENT_PATH_RUBY: &str = "/var/task/vendor/bundle/ruby/3.3.0/gems/newrelic_rpm";

/// Version information structure
#[derive(Debug, Clone)]
pub struct VersionInfo {
    pub agent_version: Option<String>,
    pub agent_name: Option<String>,
    pub extension_version: String,
    pub layer_version: Option<String>,
    pub runtime_version: Option<String>,
}

impl VersionInfo {
    /// Create version info with detected versions
    /// Uses fast synchronous detection (env vars, filesystem) - no AWS API calls
    pub fn detect(layer_version_from_config: Option<String>) -> Self {
        debug!("=== Starting version detection (sync) ===");
        let (agent_version, agent_name) = detect_agent_version();
        let extension_version = EXTENSION_VERSION.to_string();

        // Detect layer version from config (fast, no AWS API calls)
        let layer_version = detect_layer_version_sync(layer_version_from_config);

        debug!("Version detection complete (sync):");
        debug!("  Extension version: {}", extension_version);
        debug!("  Agent version: {:?} ({})", agent_version, agent_name.as_deref().unwrap_or("none"));
        debug!("  Layer version: {:?}", layer_version);

        Self {
            agent_version,
            agent_name,
            extension_version,
            layer_version,
            runtime_version: None, // Will be set from platform.initStart event
        }
    }

    /// Get cached version info or detect if not cached
    pub fn get_or_detect(layer_version_from_config: Option<String>) -> Arc<VersionInfo> {
        VERSION_INFO_CACHE
            .get_or_init(|| {
                debug!("Version info not cached, detecting synchronously...");
                Arc::new(Self::detect(layer_version_from_config))
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

    /// Set the runtime version from platform.initStart event (called once during cold start)
    pub fn set_runtime_version(runtime_version: String) {
        if RUNTIME_VERSION_CACHE.set(runtime_version.clone()).is_ok() {
            debug!("Cached runtime version from platform.initStart: {}", runtime_version);
        }
    }

    /// Get the cached runtime version
    fn get_runtime_version() -> Option<String> {
        RUNTIME_VERSION_CACHE.get().cloned()
    }

    /// Format version info line for serverless mode logging
    /// Example: "Version RequestId: abc123 Agent Version: 10.35.0 Extension Version: 0.1.0 Runtime: python3.13 Layer Version: NRTestRustExtensionPythonX86:113"
    /// Optimized: Pre-allocates buffer capacity and avoids redundant string clones
    pub fn format_version_line(&self, request_id: &str) -> String {
        // Priority: 1) cached runtime from platform.initStart, 2) runtime_version field, 3) agent_name, 4) unknown
        let runtime = Self::get_runtime_version()
            .or_else(|| self.runtime_version.clone())
            .or_else(|| self.agent_name.clone())
            .unwrap_or_else(|| "unknown".to_string());
        
        // Pre-allocate estimated capacity (reduces allocations)
        let mut result = String::with_capacity(200);
        result.push_str("Version RequestId: ");
        result.push_str(request_id);
        result.push_str(" Agent Version: ");
        result.push_str(self.agent_version.as_deref().unwrap_or("unknown"));
        result.push_str(" Extension Version: ");
        result.push_str(&self.extension_version);
        result.push_str(" Runtime: ");
        result.push_str(&runtime);
        result.push_str(" Layer Version: ");
        result.push_str(self.layer_version.as_deref().unwrap_or("unknown"));
        result
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

    for path in LAYER_AGENT_PATHS_JAVA {
        debug!("Checking Java layer directory: {}", path);
        if let Some((version, jar_path)) = find_java_agent_in_directory(path) {
            debug!("✓ Detected Java agent version: {} from {}", version, jar_path);
            return (Some(version), Some("Java".to_string()));
        }
    }

    for path in VENDOR_AGENT_PATHS_JAVA {
        debug!("Checking Java vendor path: {}", path);
        if let Some(version) = read_java_version(path) {
            debug!("✓ Detected Java agent version: {} from {}", version, path);
            return (Some(version), Some("Java".to_string()));
        }
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

/// Find New Relic Java agent JAR in a directory
fn find_java_agent_in_directory(dir_path: &str) -> Option<(String, String)> {
    use std::fs;
    
    let dir = Path::new(dir_path);
    if !dir.exists() || !dir.is_dir() {
        debug!("  Directory does not exist or is not a directory: {}", dir_path);
        return None;
    }
    
    // Try to read directory contents
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) => {
            debug!("  Failed to read directory {}: {}", dir_path, e);
            return None;
        }
    };
    
    // Look for JAR files matching New Relic agent patterns
    for entry in entries {
        if let Ok(entry) = entry {
            let file_name = entry.file_name();
            let file_name_str = file_name.to_string_lossy();
            
            // Check if filename matches any known New Relic agent pattern
            for pattern in LAYER_AGENT_JAR_NAMES {
                if file_name_str.starts_with(pattern) && file_name_str.ends_with(".jar") {
                    let jar_path = entry.path();
                    let jar_path_str = jar_path.to_string_lossy().to_string();
                    debug!("  Found potential New Relic JAR: {}", jar_path_str);
                    
                    if let Some(version) = read_java_version(&jar_path_str) {
                        return Some((version, jar_path_str));
                    }
                }
            }
        }
    }
    
    debug!("  No New Relic Java agent JAR found in {}", dir_path);
    None
}

/// Read Java agent version from JAR manifest or filename
fn read_java_version(jar_path: &str) -> Option<String> {
    debug!("  Checking if Java agent JAR exists: {}", jar_path);
    if !Path::new(jar_path).exists() {
        debug!("  Java agent JAR does not exist");
        return None;
    }

    // newrelic-java-lambda-2.2.5.jar -> 2.2.5
    if let Some(filename) = Path::new(jar_path).file_name() {
        let filename_str = filename.to_string_lossy();
        
        // Try newrelic-java-lambda-VERSION.jar pattern
        if let Some(version_part) = filename_str.strip_prefix("newrelic-java-lambda-") {
            if let Some(version) = version_part.strip_suffix(".jar") {
                if !version.is_empty() {
                    debug!("  Extracted version from filename: {}", version);
                    return Some(version.to_string());
                }
            }
        }
    }

    use std::process::Command;
    match Command::new("unzip")
        .args(&["-p", jar_path, "META-INF/MANIFEST.MF"])
        .output()
    {
        Ok(output) => {
            if output.status.success() {
                let manifest = String::from_utf8_lossy(&output.stdout);
                debug!("  Manifest content (first 500 chars): {}", &manifest.chars().take(500).collect::<String>());
                
                for line in manifest.lines() {
                    let line = line.trim();
                    if line.starts_with("Implementation-Version:") || 
                       line.starts_with("Bundle-Version:") ||
                       line.starts_with("Agent-Version:") {
                        if let Some(version) = line.split(':').nth(1) {
                            let version = version.trim();
                            if !version.is_empty() {
                                debug!("  Found Java agent version in manifest: {}", version);
                                return Some(version.to_string());
                            }
                        }
                    }
                }
            }
            debug!("  Could not extract version from JAR manifest");
            None
        }
        Err(e) => {
            debug!("  Failed to run unzip command: {}", e);
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
/// Only checks NEW_RELIC_LAYER_VERSION from config
fn detect_layer_version_sync(layer_version_from_config: Option<String>) -> Option<String> {
    debug!("Detecting layer version (sync - no AWS API calls)...");

    // Check config value (already read from NEW_RELIC_LAYER_VERSION env var)
    if let Some(layer_version) = layer_version_from_config {
        debug!("Layer version from config (NEW_RELIC_LAYER_VERSION): {}", layer_version);
        return Some(layer_version);
    }

    debug!("NEW_RELIC_LAYER_VERSION not set - layer version will be 'unknown'");
    None
}

/// Async layer version detection using AWS Lambda API
/// This makes AWS API calls - only called if user has set NEW_RELIC_ADD_VERSION_DETAIL_TAGS=true
/// or NEW_RELIC_LAYER_VERSION (which indicates they have permissions configured)
/// Public so tagging background task can use it as fallback
pub async fn detect_layer_version_async(layer_version_from_config: Option<String>, add_version_detail_tags: bool, function_name: String) -> Option<String> {
    debug!("Detecting layer version (async - includes AWS API calls)...");

    // First try NEW_RELIC_LAYER_VERSION from config
    if let Some(layer_version) = detect_layer_version_sync(layer_version_from_config) {
        return Some(layer_version);
    }

    // Check if user has indicated they want detailed version tags
    // If set, assume they have AWS permissions configured
    if !add_version_detail_tags {
        debug!("NEW_RELIC_ADD_VERSION_DETAIL_TAGS not set - skipping AWS API call for layer detection");
        return None;
    }

    // User has indicated permissions are configured, make AWS API call
    debug!("User has set version tags config - attempting to fetch layer info from AWS Lambda API...");
    match aws_layer::fetch_layer_info_from_aws(function_name).await {
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

/// Get detected runtime name (nodejs, python, ruby, etc.)
/// Checks AWS_EXECUTION_ENV dynamically to handle late environment variable initialization
pub fn get_runtime_name() -> String {
    detect_runtime_internal()
}

/// Get runtime version. Priority: platform.initStart cache, AWS_EXECUTION_ENV, runtime name
pub fn get_runtime_version() -> String {
    if let Some(cached_version) = RUNTIME_VERSION_CACHE.get() {
        return cached_version.clone();
    }

    if let Ok(env) = std::env::var("AWS_EXECUTION_ENV") {
        if let Some(runtime_version) = env.strip_prefix("AWS_Lambda_") {
            return runtime_version.to_string();
        }
    }

    get_runtime_name().to_string()
}

/// Returns the runtime name without version (e.g., "nodejs", "python")
fn detect_runtime_internal() -> String {
    if let Ok(env) = std::env::var("AWS_EXECUTION_ENV") {
        if let Some(runtime_part) = env.strip_prefix("AWS_Lambda_") {
            if runtime_part.starts_with("nodejs") {
                return "nodejs".to_string();
            } else if runtime_part.starts_with("python") {
                return "python".to_string();
            } else if runtime_part.starts_with("ruby") {
                return "ruby".to_string();
            } else if runtime_part.starts_with("dotnet") {
                return "dotnet".to_string();
            } else if runtime_part.starts_with("java") {
                return "java".to_string();
            } else if runtime_part.starts_with("go") {
                return "go".to_string();
            }
        }
    }

    // Fallback: Check /var/lang/bin for runtime binaries
    if Path::new("/var/lang/bin/node").exists() {
        return "nodejs".to_string();
    }
    if Path::new("/var/lang/bin/python").exists() {
        return "python".to_string();
    }
    if Path::new("/var/lang/bin/ruby").exists() {
        return "ruby".to_string();
    }
    if Path::new("/var/lang/bin/dotnet").exists() {
        return "dotnet".to_string();
    }
    if Path::new("/var/lang/bin/java").exists() {
        return "java".to_string();
    }

    debug!("No specific runtime detected - could be custom/containerized Lambda. Using 'unknown' to avoid incorrect tagging.");
    "unknown".to_string()
}



#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    fn test_version_info_creation() {
        let version_info = VersionInfo {
            agent_version: Some("9.5.0".to_string()),
            agent_name: Some("python".to_string()),
            extension_version: "0.1.0".to_string(),
            layer_version: Some("NewRelicPython313X86:93".to_string()),
            runtime_version: None,
        };

        let tags = version_info.as_tags();
        assert!(tags.len() >= 2);
    }

    // ========================================================================
    // as_tags() — tag generation
    // ========================================================================

    #[test]
    fn test_as_tags_extension_version_always_present() {
        let info = VersionInfo {
            agent_version: None,
            agent_name: None,
            extension_version: "1.2.3".to_string(),
            layer_version: None,
            runtime_version: None,
        };
        let tags = info.as_tags();
        assert!(tags.iter().any(|(k, v)| k == "nr.extensionVersion" && v == "1.2.3"));
    }

    #[test]
    fn test_as_tags_with_agent() {
        let info = VersionInfo {
            agent_version: Some("10.0.0".to_string()),
            agent_name: Some("Node".to_string()),
            extension_version: "1.0.0".to_string(),
            layer_version: None,
            runtime_version: None,
        };
        let tags = info.as_tags();
        assert!(tags.iter().any(|(k, v)| k == "nr.NodeAgentVersion" && v == "10.0.0"));
    }

    #[test]
    fn test_as_tags_without_agent() {
        let info = VersionInfo {
            agent_version: None,
            agent_name: None,
            extension_version: "1.0.0".to_string(),
            layer_version: None,
            runtime_version: None,
        };
        let tags = info.as_tags();
        assert!(!tags.iter().any(|(k, _)| k.contains("AgentVersion")));
    }

    #[test]
    fn test_as_tags_with_layer_version() {
        let info = VersionInfo {
            agent_version: None,
            agent_name: None,
            extension_version: "1.0.0".to_string(),
            layer_version: Some("LayerName:42".to_string()),
            runtime_version: None,
        };
        let tags = info.as_tags();
        assert!(tags.iter().any(|(k, v)| k == "nr.layerVersion" && v == "LayerName:42"));
    }

    #[test]
    fn test_as_tags_without_layer_version() {
        let info = VersionInfo {
            agent_version: None,
            agent_name: None,
            extension_version: "1.0.0".to_string(),
            layer_version: None,
            runtime_version: None,
        };
        let tags = info.as_tags();
        assert!(!tags.iter().any(|(k, _)| k == "nr.layerVersion"));
    }

    #[test]
    fn test_as_tags_all_present() {
        let info = VersionInfo {
            agent_version: Some("9.0.0".to_string()),
            agent_name: Some("Python".to_string()),
            extension_version: "2.0.0".to_string(),
            layer_version: Some("Layer:1".to_string()),
            runtime_version: None,
        };
        let tags = info.as_tags();
        assert_eq!(tags.len(), 3); // extension + agent + layer
    }

    #[test]
    fn test_as_tags_only_extension() {
        let info = VersionInfo {
            agent_version: None,
            agent_name: None,
            extension_version: "1.0.0".to_string(),
            layer_version: None,
            runtime_version: None,
        };
        let tags = info.as_tags();
        assert_eq!(tags.len(), 1);
    }

    #[test]
    fn test_as_tags_agent_name_only_no_version() {
        let info = VersionInfo {
            agent_version: None,
            agent_name: Some("Ruby".to_string()),
            extension_version: "1.0.0".to_string(),
            layer_version: None,
            runtime_version: None,
        };
        let tags = info.as_tags();
        // Requires BOTH agent_name and agent_version — should not produce agent tag
        assert!(!tags.iter().any(|(k, _)| k.contains("AgentVersion")));
    }

    // ========================================================================
    // format_version_line() — output formatting
    // ========================================================================

    #[test]
    fn test_format_version_line_all_known() {
        let info = VersionInfo {
            agent_version: Some("10.0.0".to_string()),
            agent_name: Some("Node".to_string()),
            extension_version: "2.4.5".to_string(),
            layer_version: Some("NRLayer:50".to_string()),
            runtime_version: Some("nodejs20.x".to_string()),
        };
        let line = info.format_version_line("req-abc");
        assert!(line.contains("Agent Version: 10.0.0"));
        assert!(line.contains("Extension Version: 2.4.5"));
        assert!(line.contains("Layer Version: NRLayer:50"));
        assert!(line.contains("RequestId: req-abc"));
    }

    #[test]
    fn test_format_version_line_unknown_agent() {
        let info = VersionInfo {
            agent_version: None,
            agent_name: None,
            extension_version: "1.0.0".to_string(),
            layer_version: None,
            runtime_version: None,
        };
        let line = info.format_version_line("req-1");
        assert!(line.contains("Agent Version: unknown"));
    }

    #[test]
    fn test_format_version_line_unknown_layer() {
        let info = VersionInfo {
            agent_version: Some("1.0".to_string()),
            agent_name: Some("Node".to_string()),
            extension_version: "1.0.0".to_string(),
            layer_version: None,
            runtime_version: None,
        };
        let line = info.format_version_line("req-1");
        assert!(line.contains("Layer Version: unknown"));
    }

    #[test]
    fn test_format_version_line_request_id_included() {
        let info = VersionInfo {
            agent_version: None,
            agent_name: None,
            extension_version: "1.0.0".to_string(),
            layer_version: None,
            runtime_version: None,
        };
        let line = info.format_version_line("my-unique-request-id");
        assert!(line.contains("my-unique-request-id"));
    }

    #[test]
    fn test_format_version_line_runtime_priority_agent_name() {
        // When runtime_version is None but agent_name is Some, runtime falls back to agent_name
        let info = VersionInfo {
            agent_version: Some("1.0".to_string()),
            agent_name: Some("python".to_string()),
            extension_version: "1.0.0".to_string(),
            layer_version: None,
            runtime_version: None,
        };
        let line = info.format_version_line("req");
        assert!(line.contains("Runtime: python"));
    }

    // ========================================================================
    // detect_layer_version_sync — sync detection
    // ========================================================================

    #[test]
    fn test_detect_layer_version_sync_with_config() {
        let result = detect_layer_version_sync(Some("v1".to_string()));
        assert_eq!(result, Some("v1".to_string()));
    }

    #[test]
    fn test_detect_layer_version_sync_without_config() {
        let result = detect_layer_version_sync(None);
        assert_eq!(result, None);
    }

    // ========================================================================
    // detect_runtime_internal / get_runtime_name / get_runtime_version
    // ========================================================================

    #[test]
    #[serial]
    fn test_detect_runtime_internal_nodejs() {
        std::env::set_var("AWS_EXECUTION_ENV", "AWS_Lambda_nodejs20.x");
        let result = detect_runtime_internal();
        std::env::remove_var("AWS_EXECUTION_ENV");
        assert_eq!(result, "nodejs");
    }

    #[test]
    #[serial]
    fn test_detect_runtime_internal_python() {
        std::env::set_var("AWS_EXECUTION_ENV", "AWS_Lambda_python3.13");
        let result = detect_runtime_internal();
        std::env::remove_var("AWS_EXECUTION_ENV");
        assert_eq!(result, "python");
    }

    #[test]
    #[serial]
    fn test_detect_runtime_internal_java() {
        std::env::set_var("AWS_EXECUTION_ENV", "AWS_Lambda_java21");
        let result = detect_runtime_internal();
        std::env::remove_var("AWS_EXECUTION_ENV");
        assert_eq!(result, "java");
    }

    #[test]
    #[serial]
    fn test_detect_runtime_internal_no_env() {
        std::env::remove_var("AWS_EXECUTION_ENV");
        let result = detect_runtime_internal();
        // On macOS/CI there are no /var/lang/bin paths, so should be "unknown"
        assert_eq!(result, "unknown");
    }

    #[test]
    #[serial]
    fn test_get_runtime_version_from_env() {
        std::env::set_var("AWS_EXECUTION_ENV", "AWS_Lambda_python3.13");
        let result = get_runtime_version();
        std::env::remove_var("AWS_EXECUTION_ENV");
        // If RUNTIME_VERSION_CACHE is not set, falls back to env var stripping
        assert!(
            result.contains("python") || result.contains("3.13"),
            "Expected python runtime version, got: {result}"
        );
    }

    // ========================================================================
    // read_nodejs_version — temp file tests
    // ========================================================================

    #[test]
    fn test_read_nodejs_version_valid_package_json() {
        let tmp_dir = std::env::temp_dir().join("nr_test_nodejs");
        let _ = std::fs::create_dir_all(&tmp_dir);
        std::fs::write(
            tmp_dir.join("package.json"),
            r#"{"name": "newrelic", "version": "11.23.0"}"#,
        ).expect("write");

        let result = read_nodejs_version(tmp_dir.to_str().expect("path"));
        assert_eq!(result, Some("11.23.0".to_string()));

        let _ = std::fs::remove_dir_all(&tmp_dir);
    }

    #[test]
    fn test_read_nodejs_version_missing_version_field() {
        let tmp_dir = std::env::temp_dir().join("nr_test_nodejs_no_ver");
        let _ = std::fs::create_dir_all(&tmp_dir);
        std::fs::write(
            tmp_dir.join("package.json"),
            r#"{"name": "newrelic"}"#,
        ).expect("write");

        let result = read_nodejs_version(tmp_dir.to_str().expect("path"));
        assert_eq!(result, None);

        let _ = std::fs::remove_dir_all(&tmp_dir);
    }

    #[test]
    fn test_read_nodejs_version_no_package_json() {
        let result = read_nodejs_version("/tmp/nr_test_nonexistent_path");
        assert_eq!(result, None);
    }

    #[test]
    fn test_read_nodejs_version_invalid_json() {
        let tmp_dir = std::env::temp_dir().join("nr_test_nodejs_bad_json");
        let _ = std::fs::create_dir_all(&tmp_dir);
        std::fs::write(
            tmp_dir.join("package.json"),
            "not valid json at all",
        ).expect("write");

        let result = read_nodejs_version(tmp_dir.to_str().expect("path"));
        assert_eq!(result, None);

        let _ = std::fs::remove_dir_all(&tmp_dir);
    }

    // ========================================================================
    // read_python_version — temp file tests
    // ========================================================================

    #[test]
    fn test_read_python_version_from_version_py() {
        let tmp_dir = std::env::temp_dir().join("nr_test_python");
        let _ = std::fs::create_dir_all(&tmp_dir);
        std::fs::write(
            tmp_dir.join("version.py"),
            "__version__ = '9.14.0'\n",
        ).expect("write");

        let result = read_python_version(tmp_dir.to_str().expect("path"));
        assert_eq!(result, Some("9.14.0".to_string()));

        let _ = std::fs::remove_dir_all(&tmp_dir);
    }

    #[test]
    fn test_read_python_version_from_init_py() {
        let tmp_dir = std::env::temp_dir().join("nr_test_python_init");
        let _ = std::fs::create_dir_all(&tmp_dir);
        std::fs::write(
            tmp_dir.join("__init__.py"),
            "version = \"10.0.0\"\n",
        ).expect("write");

        let result = read_python_version(tmp_dir.to_str().expect("path"));
        assert_eq!(result, Some("10.0.0".to_string()));

        let _ = std::fs::remove_dir_all(&tmp_dir);
    }

    #[test]
    fn test_read_python_version_nonexistent_path() {
        let result = read_python_version("/tmp/nr_test_nonexistent_python_path");
        assert_eq!(result, None);
    }

    // ========================================================================
    // extract_python_version_from_file — line parsing
    // ========================================================================

    #[test]
    fn test_extract_python_version_double_quotes() {
        let tmp_file = std::env::temp_dir().join("nr_test_pyver_dq.py");
        std::fs::write(&tmp_file, "__version__ = \"8.5.0\"\n").expect("write");

        let result = extract_python_version_from_file(tmp_file.to_str().expect("path"));
        assert_eq!(result, Some("8.5.0".to_string()));

        let _ = std::fs::remove_file(&tmp_file);
    }

    #[test]
    fn test_extract_python_version_single_quotes() {
        let tmp_file = std::env::temp_dir().join("nr_test_pyver_sq.py");
        std::fs::write(&tmp_file, "__version__ = '7.0.1'\n").expect("write");

        let result = extract_python_version_from_file(tmp_file.to_str().expect("path"));
        assert_eq!(result, Some("7.0.1".to_string()));

        let _ = std::fs::remove_file(&tmp_file);
    }

    #[test]
    fn test_extract_python_version_no_match() {
        let tmp_file = std::env::temp_dir().join("nr_test_pyver_nomatch.py");
        std::fs::write(&tmp_file, "# This is a comment\nname = 'newrelic'\n").expect("write");

        let result = extract_python_version_from_file(tmp_file.to_str().expect("path"));
        assert_eq!(result, None);

        let _ = std::fs::remove_file(&tmp_file);
    }

    // ========================================================================
    // read_dotnet_version — temp file tests
    // ========================================================================

    #[test]
    fn test_read_dotnet_version_from_file() {
        let tmp_dir = std::env::temp_dir().join("nr_test_dotnet");
        let _ = std::fs::create_dir_all(&tmp_dir);
        std::fs::write(tmp_dir.join("VERSION"), "10.25.0\n").expect("write");

        let result = read_dotnet_version(tmp_dir.to_str().expect("path"));
        assert_eq!(result, Some("10.25.0".to_string()));

        let _ = std::fs::remove_dir_all(&tmp_dir);
    }

    #[test]
    fn test_read_dotnet_version_empty_file() {
        let tmp_dir = std::env::temp_dir().join("nr_test_dotnet_empty");
        let _ = std::fs::create_dir_all(&tmp_dir);
        std::fs::write(tmp_dir.join("VERSION"), "").expect("write");

        let result = read_dotnet_version(tmp_dir.to_str().expect("path"));
        assert_eq!(result, None);

        let _ = std::fs::remove_dir_all(&tmp_dir);
    }

    #[test]
    fn test_read_dotnet_version_nonexistent() {
        let result = read_dotnet_version("/tmp/nr_test_nonexistent_dotnet");
        assert_eq!(result, None);
    }

    // ========================================================================
    // read_ruby_version — directory name parsing
    // ========================================================================

    #[test]
    fn test_read_ruby_version_from_directory_name() {
        // Ruby version from directory: newrelic_rpm-9.16.0
        let result = read_ruby_version("/opt/ruby/gems/3.3.0/gems/newrelic_rpm-9.16.0");
        // Directory doesn't exist but the function first tries to parse the dir name
        // The split-on-dash logic extracts "9.16.0" from "newrelic_rpm-9.16.0"
        assert_eq!(result, Some("9.16.0".to_string()));
    }

    // ========================================================================
    // read_java_version — filename parsing
    // ========================================================================

    #[test]
    fn test_read_java_version_nonexistent_jar() {
        let result = read_java_version("/tmp/nr_test_nonexistent.jar");
        assert_eq!(result, None);
    }

    // ========================================================================
    // detect_runtime for more runtimes
    // ========================================================================

    #[test]
    #[serial]
    fn test_detect_runtime_internal_dotnet() {
        std::env::set_var("AWS_EXECUTION_ENV", "AWS_Lambda_dotnet8");
        let result = detect_runtime_internal();
        std::env::remove_var("AWS_EXECUTION_ENV");
        assert_eq!(result, "dotnet");
    }

    #[test]
    #[serial]
    fn test_detect_runtime_internal_ruby() {
        std::env::set_var("AWS_EXECUTION_ENV", "AWS_Lambda_ruby3.3");
        let result = detect_runtime_internal();
        std::env::remove_var("AWS_EXECUTION_ENV");
        assert_eq!(result, "ruby");
    }

    #[test]
    #[serial]
    fn test_detect_runtime_internal_go() {
        std::env::set_var("AWS_EXECUTION_ENV", "AWS_Lambda_go1.x");
        let result = detect_runtime_internal();
        std::env::remove_var("AWS_EXECUTION_ENV");
        assert_eq!(result, "go");
    }

    #[test]
    #[serial]
    fn test_detect_runtime_internal_custom_no_prefix() {
        // AWS_EXECUTION_ENV without AWS_Lambda_ prefix
        std::env::set_var("AWS_EXECUTION_ENV", "CustomRuntime");
        let result = detect_runtime_internal();
        std::env::remove_var("AWS_EXECUTION_ENV");
        assert_eq!(result, "unknown");
    }
}
