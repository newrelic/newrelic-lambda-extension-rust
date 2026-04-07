pub mod credentials;

pub use credentials::get_new_relic_license_key;

#[cfg(test)]
mod credentials_test;