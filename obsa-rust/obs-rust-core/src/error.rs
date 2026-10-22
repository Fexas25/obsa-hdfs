use thiserror::Error;

/// Two-level error discrimination preserving OBS XML error codes.
/// Critical for HEAD elimination — 404/409 codes drive control flow.
#[derive(Debug, Error, Clone)]
pub enum ObsError {
    #[error("BadRequest: {code} — {message} (request_id={request_id})")]
    BadRequest {
        code: String,
        message: String,
        request_id: String,
    },

    #[error("NotFound: {code} — {message} (request_id={request_id})")]
    NotFound {
        code: String,
        message: String,
        request_id: String,
    },

    #[error("MethodNotAllowed: {code} — {message} (request_id={request_id})")]
    MethodNotAllowed {
        code: String,
        message: String,
        request_id: String,
    },

    #[error("Conflict: {code} — {message} (request_id={request_id})")]
    Conflict {
        code: String,
        message: String,
        request_id: String,
    },

    #[error("Http {status}: {code} — {message} (request_id={request_id})")]
    Http {
        status: u16,
        code: String,
        message: String,
        request_id: String,
    },

    #[error("Transport: {0}")]
    Transport(String),

    #[error("RemoteFileChanged: {0}")]
    RemoteFileChanged(String),

    #[error("MissingVersion: {0}")]
    MissingVersion(String),

    #[error("Unsupported: {0}")]
    Unsupported(String),

    #[error("InvalidArgument: {0}")]
    InvalidArgument(String),

    #[error("IsDirectory: {0}")]
    IsDirectory(String),

    #[error("AlreadyExists: {0}")]
    AlreadyExists(String),

    #[error("ParentNotDirectory: {0}")]
    ParentNotDirectory(String),

    #[error("DirectoryNotEmpty: {0}")]
    DirectoryNotEmpty(String),

    #[error("Io: {0}")]
    Io(String),
}

pub type ObsResult<T> = Result<T, ObsError>;

impl ObsError {
    pub fn code(&self) -> Option<&str> {
        match self {
            Self::BadRequest { code, .. }
            | Self::NotFound { code, .. }
            | Self::MethodNotAllowed { code, .. }
            | Self::Conflict { code, .. }
            | Self::Http { code, .. } => Some(code),
            _ => None,
        }
    }

    pub fn request_id(&self) -> Option<&str> {
        match self {
            Self::BadRequest { request_id, .. }
            | Self::NotFound { request_id, .. }
            | Self::MethodNotAllowed { request_id, .. }
            | Self::Conflict { request_id, .. }
            | Self::Http { request_id, .. } => Some(request_id),
            _ => None,
        }
    }

    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound { .. })
    }

    /// Path lookup compatibility: HEAD may report 404 without an XML body.
    /// Explicit bucket/upload/routing failures must never imply a missing path.
    pub fn is_path_not_found(&self) -> bool {
        // FsObjectConflict: ObsStore::head reports a PFS path below a file.
        matches!(self, Self::NotFound { code, .. }
            if code.is_empty() || code == "NoSuchKey" || code == conflict_codes::FS_OBJECT_CONFLICT)
    }

    pub fn is_conflict_with_code(&self, expected: &str) -> bool {
        matches!(
            self,
            Self::Conflict { code, .. } if code.eq_ignore_ascii_case(expected)
        )
    }

    pub fn from_status(status: u16, code: String, message: String, request_id: String) -> Self {
        match status {
            400 => Self::BadRequest {
                code,
                message,
                request_id,
            },
            404 => Self::NotFound {
                code,
                message,
                request_id,
            },
            405 => Self::MethodNotAllowed {
                code,
                message,
                request_id,
            },
            409 => Self::Conflict {
                code,
                message,
                request_id,
            },
            _ => Self::Http {
                status,
                code,
                message,
                request_id,
            },
        }
    }
}

/// Known 409 error codes from OBS PFS.
pub mod conflict_codes {
    pub const DIRECTORY_NOT_EMPTY: &str = "DirectoryNotEmpty";
    pub const TRUNCATE_BIGGER_NOT_ALLOWED: &str = "TruncateBiggerNotAllowed";
    pub const DIRECTORY_ALREADY_EXISTS: &str = "DirectoryAlreadyExists";
    pub const FILE_ALREADY_EXISTS: &str = "FileAlreadyExists";
    pub const PARENT_IS_FILE: &str = "ParentIsFile";
    pub const FS_OBJECT_CONFLICT: &str = "FsObjectConflict";
    pub const SNAPSHOT_READ_ONLY: &str = "SnapshotReadOnly";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conflict_code_matching() {
        let err = ObsError::Conflict {
            code: "DirectoryNotEmpty".into(),
            message: "not empty".into(),
            request_id: "req-1".into(),
        };
        assert!(err.is_conflict_with_code("DirectoryNotEmpty"));
        assert!(!err.is_conflict_with_code("FileAlreadyExists"));
    }
}


#[cfg(test)]
mod path_missing_tests {
    use super::*;
    #[test]
    fn path_missing_classification_keeps_head_compatibility_without_swallowing_service_errors() {
        for code in ["", "NoSuchKey", "NoSuchBucket", "NoSuchUpload", "NoSuchCustomDomain", "Unknown"] {
            let error = ObsError::from_status(404, code.into(), "missing".into(), "request".into());
            assert!(error.is_not_found()); // Existing public status classifier is unchanged.
            assert_eq!(error.is_path_not_found(), code.is_empty() || code == "NoSuchKey");
        }
        assert!(!ObsError::from_status(403, "NoSuchKey".into(), "".into(), "".into()).is_path_not_found());
    }
}
