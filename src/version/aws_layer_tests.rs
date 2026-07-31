// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

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
