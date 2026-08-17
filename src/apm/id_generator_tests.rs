// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Tests for ID generation
//! 
//! Unit tests for trace ID, span ID generation and random number generation

#[cfg(test)]
mod tests {
    use crate::apm::id_generator::TraceIDGenerator;

    #[test]
    fn test_trace_id_length() {
        let gen = TraceIDGenerator::new(1453);
        let trace_id = gen.generate_trace_id();
        assert_eq!(trace_id.len(), 32);
        assert!(trace_id.chars().all(|c: char| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_span_id_length() {
        let gen = TraceIDGenerator::new(1453);
        let span_id = gen.generate_span_id();
        assert_eq!(span_id.len(), 16);
        assert!(span_id.chars().all(|c: char| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_deterministic_with_seed() {
        let gen1 = TraceIDGenerator::new(1453);
        let gen2 = TraceIDGenerator::new(1453);
        
        let id1 = gen1.generate_trace_id();
        let id2 = gen2.generate_trace_id();
        
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_float32() {
        let gen = TraceIDGenerator::new(1453);
        let f = gen.float32();
        assert!(f >= 0.0 && f <= 1.0);
    }
}
