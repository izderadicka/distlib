//! The `distlib` application: the CLI, and the runtime it drives.
//!
//! A library target as well as a binary so that tests — here and in later
//! phases — can start whole nodes in-process rather than as subprocesses.
//! [`main`](../main.rs) is a caller like any other.

pub mod cli;
pub mod commands;
pub mod runtime;

pub use runtime::Runtime;
