// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The New Relic module contains all components for interacting with New Relic APIs,
//! including the client for sending data, payload definitions, and the harvesting logic.

pub mod client;
pub mod payload;
pub mod flush;