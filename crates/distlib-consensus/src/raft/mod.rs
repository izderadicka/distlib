//! The openraft side of the membership log: how it is parameterised, and where
//! it is stored.

pub mod log_store;
pub mod types;

pub use log_store::LogStore;
pub use types::{NodeAddr, TypeConfig};
