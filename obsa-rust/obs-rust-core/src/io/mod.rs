#[cfg(feature = "append-diagnostics")]
pub mod append_diagnostics;
pub(crate) mod append_coordination;
pub mod budget;
pub mod input;
pub mod output;
pub mod journal;
pub mod pfs_append;

pub use input::ObsInputStream;
pub use output::ObsOutputStream;
pub use pfs_append::PfsAppendStream;

/// Cleanup is idempotent only for an explicitly absent multipart upload.
/// Generic 404s (including missing buckets) must preserve the recovery record.
pub(crate) async fn abort_known_upload(
    store: &dyn crate::store::ObjectStore,
    key: &str,
    upload_id: &str,
) -> crate::error::ObsResult<()> {
    match store.abort_multipart_upload(key, upload_id).await {
        Err(error) if error.is_not_found() && error.code() == Some("NoSuchUpload") => Ok(()),
        result => result,
    }
}
