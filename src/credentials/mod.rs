pub mod credentials;
#[cfg(test)]
mod credentials_tests;

pub use credentials::get_new_relic_license_key;
