//! Accord consensus crate.
//!
//! This crate provides the Accord consensus implementation used by
//! holo_store. The API surface is intentionally small: higher layers supply
//! a `StateMachine` and a `Transport`, then drive proposals through `Group`.

pub mod accord;
