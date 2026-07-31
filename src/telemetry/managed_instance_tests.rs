// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use serde_json::json;

#[test]
fn extracts_both_fields_when_present() {
    // Real LMI shape: instanceMaxMemory is the raw uint64 AWS sends — the
    // observed 2 GiB value is in bytes, captured without conversion.
    let record = json!({
        "initializationType": "lambda-managed-instances",
        "instanceId": "2026/05/27/fn[$LATEST]abc123",
        "instanceMaxMemory": 2147483648u64
    });
    let meta = extract_managed_instance_metadata(&record).expect("metadata should parse");
    assert_eq!(meta.instance_id, "2026/05/27/fn[$LATEST]abc123");
    assert_eq!(meta.instance_max_memory, Some(2147483648));
}

#[test]
fn extracts_only_instance_id_when_max_memory_missing() {
    // instanceId present (always for LMI), instanceMaxMemory absent.
    let record = json!({
        "initializationType": "lambda-managed-instances",
        "instanceId": "2026/05/27/fn[$LATEST]abc123"
    });
    let meta = extract_managed_instance_metadata(&record).expect("metadata should parse");
    assert_eq!(meta.instance_id, "2026/05/27/fn[$LATEST]abc123");
    assert!(meta.instance_max_memory.is_none());
}

#[test]
fn returns_none_when_instance_id_absent() {
    // Standard Lambda case: instanceId not present. Listener should not
    // write to the global static.
    let record = json!({
        "initializationType": "on-demand",
        "runtimeVersion": "python:3.14.v43"
    });
    assert!(extract_managed_instance_metadata(&record).is_none());
}

#[test]
fn returns_none_when_instance_id_empty_string() {
    let record = json!({
        "instanceId": ""
    });
    assert!(extract_managed_instance_metadata(&record).is_none());
}

#[test]
fn ignores_non_integer_max_memory() {
    // A non-integer instanceMaxMemory degrades to absent — instanceId still
    // captured, no panic, no error.
    for bad in [json!("2147483648"), json!(-1), json!(1.5), json!(["x"])] {
        let record = json!({ "instanceId": "id", "instanceMaxMemory": bad });
        let meta = extract_managed_instance_metadata(&record).expect("must extract");
        assert_eq!(meta.instance_id, "id");
        assert!(
            meta.instance_max_memory.is_none(),
            "non-integer instanceMaxMemory {bad} should be treated as absent"
        );
    }
}

#[test]
fn ignores_non_string_instance_id() {
    let record = json!({
        "instanceId": 42,
        "instanceMaxMemory": 2147483648u64
    });
    assert!(extract_managed_instance_metadata(&record).is_none());
}

#[tokio::test]
async fn try_read_returns_none_until_written() {
    // Reset the global to a clean state for this serial test.
    {
        let mut guard = MANAGED_INSTANCE_METADATA.write().await;
        *guard = None;
    }
    assert!(try_read_metadata().is_none());

    let meta = ManagedInstanceMetadata {
        instance_id: "test-id".to_string(),
        instance_max_memory: Some(2147483648),
    };
    {
        let mut guard = MANAGED_INSTANCE_METADATA.write().await;
        *guard = Some(meta.clone());
    }

    let read = try_read_metadata().expect("should read after write");
    assert_eq!(read, meta);

    // Clean up so other tests start fresh.
    let mut guard = MANAGED_INSTANCE_METADATA.write().await;
    *guard = None;
}
