//! High-performance async OBS client for Hadoop FileSystem.
//!
//! Architecture:
//! - `auth` — V2 HMAC-SHA1 signing and credential chain
//! - `transport` — hyper connection pool with rustls TLS
//! - `store` — ObjectStore trait and OBS implementation
//! - `cache` — StatusCache with TTL and negative caching
//! - `listing` — paginated async bucket listing
//! - `pfs` — Parallel File System operations (rename, truncate, append)
//! - `io` — prefetch read, MPU write, PFS append streams
//! - `fs` — high-level OBSFileSystem logic

pub mod auth;
pub mod cache;
pub mod config;
pub mod error;
pub mod fs;
pub mod io;
pub mod listing;
pub mod pfs;
pub mod store;
pub mod transport;
pub mod types;
pub mod xml;

pub use config::ObsConfig;
pub use error::ObsError;
pub use fs::ObsFileSystem;
pub use types::*;
