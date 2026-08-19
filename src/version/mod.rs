// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Version detection module for agent, extension, and layer versions

mod aws_layer;
pub mod tagging;

use std::fs;
use std::path::Path;
use std::sync::Arc;
use once_cell::sync::OnceCell;
use tracing::debug;

/// Global cache for version information (detected once, reused everywhere)
static VERSION_INFO_CACHE: OnceCell<Arc<VersionInfo>> = OnceCell::new();

/// Global cache for runtime version from platform.initStart event
static RUNTIME_VERSION_CACHE: OnceCell<String> = OnceCell::new();


/// Extension version from Cargo.toml
const EXTENSION_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Single source of truth for the outbound `User-Agent` advertised to the
/// New Relic APM collector (PreConnect/Connect handshake and telemetry sends).
/// Always tracks the crate version from Cargo.toml — never hardcode it.
pub fn user_agent() -> String {
    format!("NewRelic-Rust-Lambda-Extension/{EXTENSION_VERSION}")
}

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
    "/opt/newrelic",  // NR Java agent layer (newrelic.jar + java-agent-version.txt)
    "/opt/java/lib",  // Directory to scan for newrelic JARs
    "/opt/lib",       // Alternative directory to scan
];
/// Fast-path version file installed by the NR Java agent layer at /opt/newrelic/
const JAVA_AGENT_VERSION_FILE: &str = "/opt/newrelic/java-agent-version.txt";
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

    // Fast path: /opt/newrelic/java-agent-version.txt written by the NR Java agent layer
    debug!("Checking Java version file: {}", JAVA_AGENT_VERSION_FILE);
    if let Ok(content) = fs::read_to_string(JAVA_AGENT_VERSION_FILE) {
        let version = content.trim();
        if !version.is_empty() {
            debug!("✓ Detected Java agent version: {} from {}", version, JAVA_AGENT_VERSION_FILE);
            return (Some(version.to_string()), Some("Java".to_string()));
        }
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

    // Go agent: compiled into the handler binary — no layer files to read.
    // Scan the Go binary's embedded build info for the NR go-agent module version.
    // This runs BEFORE the APM connect so the version reaches the Connect payload.
    if let Some(version) = detect_go_agent_version_from_binary() {
        debug!("✓ Detected Go agent version from binary build info: {}", version);
        return (Some(version), Some("Go".to_string()));
    }

    debug!("No agent version detected from any known paths (expected for Go/custom runtimes)");
    (None, None)
}

/// Scan the Go handler binary for the NR go-agent module version embedded in build info.
///
/// Go encodes module dependency versions in a `go.buildinfo` section as human-readable
/// tab-separated text, e.g.:
///   `dep\tgithub.com/newrelic/go-agent/v3\tv3.39.0\th1:...`
///
/// We search for the byte pattern and extract the semver string. This is done once at
/// startup — before the APM Connect — so the version appears in the Connect payload.
fn detect_go_agent_version_from_binary() -> Option<String> {
    let task_root = std::env::var("LAMBDA_TASK_ROOT")
        .unwrap_or_else(|_| "/var/task".to_string());

    // Candidate binary names for Go Lambda handlers
    let candidates = {
        let mut v = vec![
            format!("{}/bootstrap", task_root),
            format!("{}/handler", task_root),
        ];
        // _HANDLER may be "mypackage.Handler" — the binary name is the part before the dot
        if let Ok(h) = std::env::var("_HANDLER") {
            let bin = h.split('.').next().unwrap_or(&h);
            v.push(format!("{}/{}", task_root, bin));
        }
        v
    };

    // Byte pattern: "github.com/newrelic/go-agent/v3\tv"
    const NR_GO_AGENT_PREFIX: &[u8] = b"github.com/newrelic/go-agent/v3\tv";

    for path in &candidates {
        if !Path::new(path).exists() {
            continue;
        }
        debug!("Scanning Go binary for NR agent version: {}", path);
        let bytes = match fs::read(path) {
            Ok(b) => b,
            Err(e) => {
                debug!("Could not read binary {}: {}", path, e);
                continue;
            }
        };
        if let Some(pos) = bytes.windows(NR_GO_AGENT_PREFIX.len())
            .position(|w| w == NR_GO_AGENT_PREFIX)
        {
            let ver_start = pos + NR_GO_AGENT_PREFIX.len();
            let ver_end = bytes[ver_start..]
                .iter()
                .position(|&b| b == b'\t' || b == b'\n' || b == b'\r' || b == b'\0')
                .map(|e| ver_start + e)
                .unwrap_or((ver_start + 20).min(bytes.len()));
            if let Ok(ver) = std::str::from_utf8(&bytes[ver_start..ver_end]) {
                let ver = ver.trim();
                if !ver.is_empty() && ver.chars().all(|c| c.is_ascii_digit() || c == '.') {
                    return Some(ver.to_string());
                }
            }
        }
    }
    None
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
#[path = "mod_tests.rs"]
mod tests;
