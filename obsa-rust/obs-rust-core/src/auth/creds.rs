use std::sync::Arc;

/// OBS access credentials.
#[derive(Clone)]
pub struct Credentials {
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("access_key", &"<redacted>")
            .field("secret_key", &"<redacted>")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

impl Credentials {
    pub fn new(access_key: impl Into<String>, secret_key: impl Into<String>) -> Self {
        Self {
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            session_token: None,
        }
    }

    pub fn with_session_token(mut self, token: impl Into<String>) -> Self {
        self.session_token = Some(token.into());
        self
    }
}

/// Credential provider chain (static AK/SK for now; extensible to IAM/STS).
#[derive(Clone)]
pub struct CredentialChain {
    creds: Arc<Credentials>,
}

impl CredentialChain {
    pub fn new(creds: Credentials) -> Self {
        Self {
            creds: Arc::new(creds),
        }
    }

    pub fn get(&self) -> Credentials {
        (*self.creds).clone()
    }
}
