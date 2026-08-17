// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

pub mod credentials;

pub use credentials::get_new_relic_license_key;

#[cfg(test)]
mod credentials_test;