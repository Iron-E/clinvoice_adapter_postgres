//! # Summary
//!
//! This crate provides an implementation of [`clinvoice_adapter`] for a Postgres filesystem.

#![warn(missing_docs)]

mod fmt;

pub mod schema;
pub use schema::PgSchema;
