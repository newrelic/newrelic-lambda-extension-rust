#[cfg(test)]
mod tests {
    use crate::platform::processor::normalize_platform_runtime_version;

    #[test]
    fn test_normalize_platform_runtime_version() {
        // Node.js - use .x suffix
        assert_eq!(normalize_platform_runtime_version("nodejs:18.v98"), "nodejs18.x");
        assert_eq!(normalize_platform_runtime_version("nodejs:20.v15"), "nodejs20.x");
        assert_eq!(normalize_platform_runtime_version("nodejs:22.v2"), "nodejs22.x");

        // Python - keep major.minor
        assert_eq!(normalize_platform_runtime_version("python:3.13"), "python3.13");
        assert_eq!(normalize_platform_runtime_version("python:3.12.5"), "python3.12");

        // Ruby - keep major.minor
        assert_eq!(normalize_platform_runtime_version("ruby:3.3"), "ruby3.3");
        assert_eq!(normalize_platform_runtime_version("ruby:3.2.0"), "ruby3.2");

        // Java - keep major only
        assert_eq!(normalize_platform_runtime_version("java:17"), "java17");
        assert_eq!(normalize_platform_runtime_version("java:21"), "java21");

        // .NET - keep major only
        assert_eq!(normalize_platform_runtime_version("dotnet:8"), "dotnet8");
        assert_eq!(normalize_platform_runtime_version("dotnet:6"), "dotnet6");

        // No colon - return as-is
        assert_eq!(normalize_platform_runtime_version("unknown"), "unknown");
        assert_eq!(normalize_platform_runtime_version("go1.x"), "go1.x");
    }
}
