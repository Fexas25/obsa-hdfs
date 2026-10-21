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

/// Shared, replaceable credentials. Clones observe later `update` calls, so a
/// refreshed STS token applies to the next signature, including retries of a
/// request already in flight (each attempt signs afresh).
#[derive(Clone)]
pub struct CredentialChain {
    creds: Arc<parking_lot::RwLock<Arc<Credentials>>>,
}

impl CredentialChain {
    pub fn new(creds: Credentials) -> Self {
        Self {
            creds: Arc::new(parking_lot::RwLock::new(Arc::new(creds))),
        }
    }

    pub fn get(&self) -> Credentials {
        (**self.creds.read()).clone()
    }

    pub fn update(&self, creds: Credentials) {
        *self.creds.write() = Arc::new(creds);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clones_observe_updates() {
        let chain = CredentialChain::new(Credentials::new("AK1", "SK1"));
        let shared = chain.clone();
        chain.update(Credentials::new("AK2", "SK2").with_session_token("T2"));
        let current = shared.get();
        assert_eq!(current.access_key, "AK2");
        assert_eq!(current.secret_key, "SK2");
        assert_eq!(current.session_token.as_deref(), Some("T2"));
    }
}
