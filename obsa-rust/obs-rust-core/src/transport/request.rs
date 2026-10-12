use std::collections::BTreeMap;

use bytes::Bytes;
use http::{Method, Uri};

use crate::config::ObsConfig;
use crate::types::AddressingStyle;

/// Low-level HTTP request descriptor.
#[derive(Clone)]
pub struct HttpRequest {
    pub method: Method,
    pub uri: Uri,
    pub headers: BTreeMap<String, String>,
    pub body: Option<Bytes>,
    pub idempotent: bool,
}

impl std::fmt::Debug for HttpRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // URIs may contain signed query parameters; bodies and arbitrary
        // headers are application data, not safe diagnostic output.
        f.debug_struct("HttpRequest")
            .field("method", &self.method)
            .field("uri", &"<redacted>")
            .field("header_count", &self.headers.len())
            .field("body_len", &self.body.as_ref().map(Bytes::len))
            .field("idempotent", &self.idempotent)
            .finish()
    }
}

/// Fluent request builder for OBS operations.
pub struct RequestBuilder {
    config: ObsConfig,
    method: Method,
    bucket: String,
    key: String,
    sub_resources: Vec<(String, Option<String>)>,
    headers: BTreeMap<String, String>,
    body: Option<Bytes>,
    idempotent: bool,
    hws_fs_headers: BTreeMap<String, String>,
}

impl RequestBuilder {
    pub fn new(config: &ObsConfig, bucket: &str, method: Method) -> Self {
        Self {
            config: config.clone(),
            method,
            bucket: bucket.to_string(),
            key: String::new(),
            sub_resources: Vec::new(),
            headers: BTreeMap::new(),
            body: None,
            idempotent: true,
            hws_fs_headers: BTreeMap::new(),
        }
    }

    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.key = key.into();
        self
    }

    pub fn sub_resource(mut self, name: impl Into<String>, value: Option<String>) -> Self {
        self.sub_resources.push((name.into(), value));
        self
    }

    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(name.into(), value.into());
        self
    }

    pub fn obs_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        let name = name.into();
        debug_assert!(name.to_ascii_lowercase().starts_with("x-obs-"));
        self.headers.insert(name, value.into());
        self
    }

    /// x-hws-fs-* headers are NOT V2-signed.
    pub fn hws_fs_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.hws_fs_headers.insert(name.into(), value.into());
        self
    }

    pub fn body(mut self, body: Bytes) -> Self {
        self.body = Some(body);
        self
    }

    pub fn idempotent(mut self, v: bool) -> Self {
        self.idempotent = v;
        self
    }

    pub fn build_uri(&self) -> Result<Uri, http::Error> {
        let endpoint = self.config.endpoint.trim_end_matches('/');
        let host = endpoint
            .trim_start_matches("https://")
            .trim_start_matches("http://");
        let key = self
            .key
            .trim_start_matches('/')
            .split('/')
            .map(|part| urlencoding::encode(part).into_owned())
            .collect::<Vec<_>>()
            .join("/");
        let mut path = match self.config.addressing_style {
            AddressingStyle::Path => format!("/{}/{key}", self.bucket),
            _ => format!("/{key}"),
        };
        if !self.sub_resources.is_empty() {
            path.push('?');
            path.push_str(
                &self
                    .sub_resources
                    .iter()
                    .map(|(n, v)| match v {
                        Some(value) => {
                            format!("{}={}", urlencoding::encode(n), urlencoding::encode(value))
                        }
                        None => urlencoding::encode(n).into_owned(),
                    })
                    .collect::<Vec<_>>()
                    .join("&"),
            );
        }
        let authority = match self.config.addressing_style {
            AddressingStyle::Vhost => format!("{}.{}", self.bucket, host),
            _ => host.to_string(),
        };
        Uri::builder()
            .scheme(if self.config.secure { "https" } else { "http" })
            .authority(authority.as_str())
            .path_and_query(path)
            .build()
    }

    pub fn all_headers(&self) -> BTreeMap<String, String> {
        let mut all = self.headers.clone();
        // x-hws-fs-* merged at send time (not signed)
        for (k, v) in &self.hws_fs_headers {
            all.insert(k.clone(), v.clone());
        }
        all
    }

    pub fn obs_headers(&self) -> &BTreeMap<String, String> {
        &self.headers
    }

    pub fn sub_resources_ref(&self) -> Vec<(&str, Option<&str>)> {
        self.sub_resources
            .iter()
            .map(|(n, v)| (n.as_str(), v.as_deref()))
            .collect()
    }

    pub fn into_parts(
        self,
    ) -> (
        ObsConfig,
        Method,
        String,
        String,
        Vec<(String, Option<String>)>,
        BTreeMap<String, String>,
        BTreeMap<String, String>,
        Option<Bytes>,
        bool,
    ) {
        (
            self.config,
            self.method,
            self.bucket,
            self.key,
            self.sub_resources,
            self.headers,
            self.hws_fs_headers,
            self.body,
            self.idempotent,
        )
    }
}

#[cfg(test)]
mod debug_tests {
    use super::*;
    #[test]
    fn request_debug_redacts_headers_uri_and_body() {
        let request = HttpRequest {
            method: Method::PUT,
            uri: "https://example.invalid/private-path?token=private-query".parse().unwrap(),
            headers: BTreeMap::from([("aUtHoRiZaTiOn".into(), "private-auth".into()),
                ("x-obs-security-token".into(), "private-token".into()),
                ("x-obs-meta-custom".into(), "private-metadata".into())]),
            body: Some(Bytes::from_static(b"private-body")), idempotent: false,
        };
        for rendered in [format!("{request:?}"), format!("{request:#?}")] {
            assert!(!rendered.contains("private-"), "Debug exposed protected request data");
            assert!(rendered.contains("PUT"));
        }
        assert_eq!(request.body.as_deref(), Some(b"private-body".as_slice()));
        assert_eq!(request.headers["aUtHoRiZaTiOn"], "private-auth");
    }
}
