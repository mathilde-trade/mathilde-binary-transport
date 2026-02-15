pub mod batch;
pub mod codec;
pub mod error;
pub mod schema;

pub use crate::error::{Error, Result};

#[cfg(test)]
mod tests;
