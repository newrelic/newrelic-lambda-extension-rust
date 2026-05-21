// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! ID generation for distributed tracing
//! 
//! Generates trace IDs, span IDs, and GUIDs for APM telemetry
//! Based on id_generator.go

use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use std::sync::Mutex;

const TRACE_ID_BYTE_LEN: usize = 16;
const SPAN_ID_BYTE_LEN: usize = 8;
const HEX_TABLE: &[u8; 16] = b"0123456789abcdef";

/// Thread-safe trace ID generator
pub struct TraceIDGenerator {
    rng: Mutex<StdRng>,
}

impl TraceIDGenerator {
    /// Create a new trace ID generator with a seed
    pub fn new(seed: u64) -> Self {
        Self {
            rng: Mutex::new(StdRng::seed_from_u64(seed)),
        }
    }

    /// Generate a random float32
    pub fn float32(&self) -> f32 {
        let mut rng = self.rng.lock().unwrap();
        rng.random()
    }

    /// Generate a 32-character hex trace ID
    pub fn generate_trace_id(&self) -> String {
        self.generate_id(TRACE_ID_BYTE_LEN)
    }

    /// Generate a 16-character hex span ID
    pub fn generate_span_id(&self) -> String {
        self.generate_id(SPAN_ID_BYTE_LEN)
    }

    /// Generate an ID of specified byte length as hex string
    fn generate_id(&self, byte_len: usize) -> String {
        let mut rng = self.rng.lock().unwrap();
        let mut bytes = vec![0u8; byte_len];
        rng.fill(&mut bytes[..]);

        let mut hex = String::with_capacity(byte_len * 2);
        for byte in bytes {
            hex.push(HEX_TABLE[(byte >> 4) as usize] as char);
            hex.push(HEX_TABLE[(byte & 0x0f) as usize] as char);
        }

        hex
    }
}
