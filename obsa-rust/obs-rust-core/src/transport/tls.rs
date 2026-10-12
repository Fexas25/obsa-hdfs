use std::sync::Arc;

use rustls::ClientConfig;
use tokio_rustls::TlsConnector;

/// Build a rustls client config (no OpenSSL dependency).
pub fn build_tls_config() -> Arc<ClientConfig> {
    let mut root_store = rustls::RootCertStore::empty();
    for cert in rustls_native_certs::load_native_certs().certs {
        let _ = root_store.add(cert);
    }

    let config =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .expect("supported TLS versions")
            .with_root_certificates(root_store)
            .with_no_client_auth();

    Arc::new(config)
}

pub fn tls_connector() -> TlsConnector {
    TlsConnector::from(build_tls_config())
}
