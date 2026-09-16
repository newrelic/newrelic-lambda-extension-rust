// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

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

#[test]
fn user_agent_tracks_cargo_version() {
    let ua = user_agent();
    // Must carry the real crate version, never the old hardcoded placeholder.
    assert_eq!(ua, format!("NewRelic-Rust-Lambda-Extension/{}", env!("CARGO_PKG_VERSION")));
    assert!(ua.contains(env!("CARGO_PKG_VERSION")));
    assert_ne!(ua, "NewRelic-Rust-Lambda-Extension/0.1.0");
}

// ============================================================================
// Filesystem-based agent version detection (read_*_version / extract_*).
// These are pure filesystem parsing — no network — so we exercise them
// against real temp directories rather than mocking anything. Each test uses
// a unique subdirectory of std::env::temp_dir() to avoid colliding with any
// other test, and cleans up after itself.
// ============================================================================

/// Unique-per-test scratch dir under the OS temp dir, removed on drop.
struct ScratchDir(std::path::PathBuf);

impl ScratchDir {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!("nr_ext_version_test_{}_{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("should be able to create scratch dir");
        Self(path)
    }

    fn path(&self) -> &std::path::Path {
        &self.0
    }

    fn as_str(&self) -> &str {
        self.0.to_str().unwrap()
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

// --- read_nodejs_version ---

#[test]
fn read_nodejs_version_finds_version_in_package_json() {
    let dir = ScratchDir::new("nodejs_valid");
    std::fs::write(dir.path().join("package.json"), r#"{"name": "newrelic", "version": "9.5.0"}"#).unwrap();
    assert_eq!(read_nodejs_version(dir.as_str()), Some("9.5.0".to_string()));
}

#[test]
fn read_nodejs_version_missing_package_json_returns_none() {
    let dir = ScratchDir::new("nodejs_missing");
    assert_eq!(read_nodejs_version(dir.as_str()), None);
}

#[test]
fn read_nodejs_version_invalid_json_returns_none() {
    let dir = ScratchDir::new("nodejs_invalid_json");
    std::fs::write(dir.path().join("package.json"), "not valid json{{{").unwrap();
    assert_eq!(read_nodejs_version(dir.as_str()), None);
}

#[test]
fn read_nodejs_version_missing_version_field_returns_none() {
    let dir = ScratchDir::new("nodejs_no_version_field");
    std::fs::write(dir.path().join("package.json"), r#"{"name": "newrelic"}"#).unwrap();
    assert_eq!(read_nodejs_version(dir.as_str()), None);
}

// --- read_python_version / extract_python_version_from_file ---

#[test]
fn read_python_version_finds_version_in_version_py() {
    let dir = ScratchDir::new("python_version_py");
    std::fs::write(dir.path().join("version.py"), "__version__ = '9.5.0'\n").unwrap();
    assert_eq!(read_python_version(dir.as_str()), Some("9.5.0".to_string()));
}

#[test]
fn read_python_version_falls_back_to_init_py() {
    let dir = ScratchDir::new("python_init_py");
    std::fs::write(dir.path().join("__init__.py"), "version = \"1.0.0\"\n").unwrap();
    assert_eq!(read_python_version(dir.as_str()), Some("1.0.0".to_string()));
}

#[test]
fn read_python_version_nonexistent_dir_returns_none() {
    let dir = ScratchDir::new("python_nonexistent");
    let missing = dir.path().join("does-not-exist");
    assert_eq!(read_python_version(missing.to_str().unwrap()), None);
}

#[test]
fn read_python_version_no_markers_returns_none() {
    let dir = ScratchDir::new("python_no_markers");
    assert_eq!(read_python_version(dir.as_str()), None);
}

#[test]
fn read_python_version_falls_back_to_dist_info_metadata() {
    let parent = ScratchDir::new("python_dist_info_parent");
    let base_path = parent.path().join("newrelic");
    std::fs::create_dir_all(&base_path).unwrap();

    let dist_info = parent.path().join("newrelic-9.9.9.dist-info");
    std::fs::create_dir_all(&dist_info).unwrap();
    std::fs::write(dist_info.join("METADATA"), "Metadata-Version: 2.1\nName: newrelic\nVersion: 9.9.9\n").unwrap();

    assert_eq!(read_python_version(base_path.to_str().unwrap()), Some("9.9.9".to_string()));
}

#[test]
fn extract_python_version_from_file_ignores_lines_without_digits() {
    let dir = ScratchDir::new("python_extract_no_digits");
    let file_path = dir.path().join("version.py");
    std::fs::write(&file_path, "__version__ = 'unset'\n").unwrap();
    assert_eq!(extract_python_version_from_file(file_path.to_str().unwrap()), None);
}

#[test]
fn extract_python_version_from_file_missing_file_returns_none() {
    let dir = ScratchDir::new("python_extract_missing");
    let file_path = dir.path().join("nope.py");
    assert_eq!(extract_python_version_from_file(file_path.to_str().unwrap()), None);
}

#[test]
fn extract_python_version_from_metadata_finds_version_line() {
    let dir = ScratchDir::new("python_metadata_direct");
    let metadata_path = dir.path().join("METADATA");
    std::fs::write(&metadata_path, "Name: newrelic\nVersion: 1.2.3\nSummary: agent\n").unwrap();
    assert_eq!(
        extract_python_version_from_metadata(&metadata_path),
        Some("1.2.3".to_string())
    );
}

// --- read_ruby_version / extract_ruby_version_from_file ---

#[test]
fn read_ruby_version_extracts_from_directory_name() {
    let parent = ScratchDir::new("ruby_dirname_parent");
    let gem_dir = parent.path().join("newrelic_rpm-9.5.0");
    std::fs::create_dir_all(&gem_dir).unwrap();
    assert_eq!(read_ruby_version(gem_dir.to_str().unwrap()), Some("9.5.0".to_string()));
}

#[test]
fn read_ruby_version_falls_back_to_version_rb() {
    let dir = ScratchDir::new("ruby_version_rb");
    // No dash in the directory name, so the filename-based fast path can't match.
    let gem_dir = dir.path().join("newrelicrpm");
    std::fs::create_dir_all(gem_dir.join("lib/new_relic")).unwrap();
    std::fs::write(gem_dir.join("lib/new_relic/version.rb"), "  VERSION = '9.6.0'\n").unwrap();
    assert_eq!(read_ruby_version(gem_dir.to_str().unwrap()), Some("9.6.0".to_string()));
}

#[test]
fn read_ruby_version_no_markers_returns_none() {
    let dir = ScratchDir::new("ruby_no_markers");
    let gem_dir = dir.path().join("newrelicrpm");
    std::fs::create_dir_all(&gem_dir).unwrap();
    assert_eq!(read_ruby_version(gem_dir.to_str().unwrap()), None);
}

#[test]
fn extract_ruby_version_from_file_finds_version_constant() {
    let dir = ScratchDir::new("ruby_extract");
    let file_path = dir.path().join("version.rb");
    std::fs::write(&file_path, "module NewRelic\n  VERSION = \"9.7.0\"\nend\n").unwrap();
    assert_eq!(extract_ruby_version_from_file(file_path.to_str().unwrap()), Some("9.7.0".to_string()));
}

// --- read_dotnet_version ---

#[test]
fn read_dotnet_version_finds_version_file() {
    let dir = ScratchDir::new("dotnet_version");
    std::fs::write(dir.path().join("VERSION"), "10.2.0\n").unwrap();
    assert_eq!(read_dotnet_version(dir.as_str()), Some("10.2.0".to_string()));
}

#[test]
fn read_dotnet_version_missing_file_returns_none() {
    let dir = ScratchDir::new("dotnet_missing");
    assert_eq!(read_dotnet_version(dir.as_str()), None);
}

// --- find_java_agent_in_directory / read_java_version ---

#[test]
fn find_java_agent_in_directory_matches_filename_pattern() {
    let dir = ScratchDir::new("java_agent_dir");
    let jar_path = dir.path().join("newrelic-java-lambda-2.2.5.jar");
    std::fs::write(&jar_path, b"fake jar bytes").unwrap();

    let result = find_java_agent_in_directory(dir.as_str());
    assert_eq!(result, Some(("2.2.5".to_string(), jar_path.to_str().unwrap().to_string())));
}

#[test]
fn find_java_agent_in_directory_no_match_returns_none() {
    let dir = ScratchDir::new("java_agent_no_match");
    std::fs::write(dir.path().join("some-other-file.jar"), b"not a match").unwrap();
    assert_eq!(find_java_agent_in_directory(dir.as_str()), None);
}

#[test]
fn find_java_agent_in_directory_nonexistent_dir_returns_none() {
    let dir = ScratchDir::new("java_agent_nonexistent");
    let missing = dir.path().join("does-not-exist");
    assert_eq!(find_java_agent_in_directory(missing.to_str().unwrap()), None);
}

#[test]
fn read_java_version_extracts_from_filename() {
    let dir = ScratchDir::new("java_version_filename");
    let jar_path = dir.path().join("newrelic-java-lambda-3.1.0.jar");
    std::fs::write(&jar_path, b"fake jar bytes").unwrap();
    assert_eq!(read_java_version(jar_path.to_str().unwrap()), Some("3.1.0".to_string()));
}

#[test]
fn read_java_version_missing_jar_returns_none() {
    let dir = ScratchDir::new("java_version_missing");
    let jar_path = dir.path().join("newrelic-java-lambda-1.0.0.jar");
    assert_eq!(read_java_version(jar_path.to_str().unwrap()), None);
}

// --- detect_layer_version_sync ---

#[test]
fn detect_layer_version_sync_returns_config_value_when_set() {
    assert_eq!(
        detect_layer_version_sync(Some("NewRelicPython313X86:93".to_string())),
        Some("NewRelicPython313X86:93".to_string())
    );
}

#[test]
fn detect_layer_version_sync_returns_none_when_unset() {
    assert_eq!(detect_layer_version_sync(None), None);
}

// --- detect_runtime_internal ---
//
// AWS_EXECUTION_ENV is process-global, so these run #[serial] (matching the
// convention already used crate-wide for global/env-dependent tests) and
// always restore the prior value afterward.

#[test]
#[serial]
fn detect_runtime_internal_reads_nodejs_from_execution_env() {
    let prev = std::env::var("AWS_EXECUTION_ENV").ok();
    std::env::set_var("AWS_EXECUTION_ENV", "AWS_Lambda_nodejs20.x");
    assert_eq!(detect_runtime_internal(), "nodejs");
    match prev {
        Some(v) => std::env::set_var("AWS_EXECUTION_ENV", v),
        None => std::env::remove_var("AWS_EXECUTION_ENV"),
    }
}

#[test]
#[serial]
fn detect_runtime_internal_reads_python_from_execution_env() {
    let prev = std::env::var("AWS_EXECUTION_ENV").ok();
    std::env::set_var("AWS_EXECUTION_ENV", "AWS_Lambda_python3.12");
    assert_eq!(detect_runtime_internal(), "python");
    match prev {
        Some(v) => std::env::set_var("AWS_EXECUTION_ENV", v),
        None => std::env::remove_var("AWS_EXECUTION_ENV"),
    }
}

#[test]
#[serial]
fn detect_runtime_internal_reads_java_from_execution_env() {
    let prev = std::env::var("AWS_EXECUTION_ENV").ok();
    std::env::set_var("AWS_EXECUTION_ENV", "AWS_Lambda_java21");
    assert_eq!(detect_runtime_internal(), "java");
    match prev {
        Some(v) => std::env::set_var("AWS_EXECUTION_ENV", v),
        None => std::env::remove_var("AWS_EXECUTION_ENV"),
    }
}

#[test]
#[serial]
fn detect_runtime_internal_unknown_when_unset_and_no_runtime_binaries() {
    let prev = std::env::var("AWS_EXECUTION_ENV").ok();
    std::env::remove_var("AWS_EXECUTION_ENV");
    // No /var/lang/bin/* runtime binaries exist on the test machine, so this
    // must fall all the way through to the "unknown" default.
    assert_eq!(detect_runtime_internal(), "unknown");
    if let Some(v) = prev {
        std::env::set_var("AWS_EXECUTION_ENV", v);
    }
}
