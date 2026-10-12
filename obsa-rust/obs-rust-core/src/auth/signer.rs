use std::collections::BTreeMap;

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use sha1::Sha1;

use super::Credentials;

type HmacSha1 = Hmac<Sha1>;

/// OBS V2 signature signer.
///
/// StringToSign = METHOD \n Content-MD5 \n Content-Type \n Date \n
///                CanonicalizedAmzHeaders CanonicalizedResource
///
/// Only x-obs-* headers are signed; x-hws-fs-* headers are invisible to signer.
pub struct ObsSigner;

/// Signed HTTP request headers ready to send.
#[derive(Clone, Default)]
pub struct SignedRequest {
    pub headers: BTreeMap<String, String>,
    pub date: String,
    pub authorization: String,
}

impl std::fmt::Debug for SignedRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Header values can carry tokens and arbitrary user metadata. Do not
        // depend on a case-sensitive or exhaustive sensitive-header denylist.
        f.debug_struct("SignedRequest")
            .field("header_count", &self.headers.len())
            .field("date", &self.date)
            .field("authorization", &"<redacted>")
            .finish()
    }
}

impl ObsSigner {
    pub fn sign(
        method: &str,
        content_md5: &str,
        content_type: &str,
        date: &str,
        obs_headers: &BTreeMap<String, String>,
        canonical_resource: &str,
        creds: &Credentials,
    ) -> SignedRequest {
        let mut headers = obs_headers.clone();
        if let Some(token) = &creds.session_token {
            headers.insert("x-obs-security-token".into(), token.clone());
        }
        let canonical_amz = Self::canonicalize_obs_headers(&headers);
        let string_to_sign = format!(
            "{method}\n{content_md5}\n{content_type}\n{date}\n{canonical_amz}{canonical_resource}"
        );

        let mut mac =
            HmacSha1::new_from_slice(creds.secret_key.as_bytes()).expect("HMAC key length");
        mac.update(string_to_sign.as_bytes());
        let signature = BASE64.encode(mac.finalize().into_bytes());

        let authorization = format!("OBS {}:{}", creds.access_key, signature);

        headers.insert("Date".into(), date.into());
        headers.insert("Authorization".into(), authorization.clone());

        SignedRequest {
            headers,
            date: date.into(),
            authorization,
        }
    }

    pub fn rfc7231_date(now: DateTime<Utc>) -> String {
        now.format("%a, %d %b %Y %H:%M:%S GMT").to_string()
    }

    /// Build canonicalized x-obs-* headers (sorted lowercase).
    fn canonicalize_obs_headers(headers: &BTreeMap<String, String>) -> String {
        let mut pairs: Vec<_> = headers
            .iter()
            .filter(|(k, _)| k.to_ascii_lowercase().starts_with("x-obs-"))
            .map(|(k, v)| (k.to_ascii_lowercase(), v.trim().to_string()))
            .collect();
        pairs.sort_by(|a, b| a.0.cmp(&b.0));

        pairs
            .into_iter()
            .map(|(k, v)| format!("{k}:{v}\n"))
            .collect()
    }

    /// Build canonical resource: /bucket/key?subresource1&subresource2
    pub fn canonical_resource(
        bucket: &str,
        key: &str,
        sub_resources: &[(&str, Option<&str>)],
    ) -> String {
        let mut resource = format!("/{bucket}/{key}");
        // OBS V2 excludes ordinary LIST query parameters. See Huawei API
        // obs_04_0010.html; PFS folder is an additional protocol subresource.
        let mut sub_resources: Vec<_> = sub_resources
            .iter()
            .copied()
            .filter(|(name, _)| {
                !matches!(
                    *name,
                    "prefix"
                        | "delimiter"
                        | "marker"
                        | "max-keys"
                        | "encoding-type"
                        | "list-type"
                        | "continuation-token"
                        | "start-after"
                        | "fetch-owner"
                )
            })
            .collect();
        sub_resources.sort_by(|a, b| a.0.cmp(b.0));
        if !sub_resources.is_empty() {
            resource.push('?');
            let parts: Vec<String> = sub_resources
                .iter()
                .map(|(name, value)| match value {
                    Some(v) => format!("{name}={v}"),
                    None => (*name).to_string(),
                })
                .collect();
            resource.push_str(&parts.join("&"));
        }
        resource
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signed_debug_redacts_credentials_and_metadata() {
        let creds = Credentials::new("private-access-marker", "private-secret-marker")
            .with_session_token("private-token-marker");
        let headers = BTreeMap::from([("x-obs-meta-custom".into(), "private-metadata-marker".into())]);
        let signed = ObsSigner::sign("GET", "", "", "Mon, 01 Jan 2024 00:00:00 GMT", &headers, "/bucket/key", &creds);
        for rendered in [format!("{signed:?}"), format!("{signed:#?}")] {
            for secret in [creds.access_key.as_str(), creds.secret_key.as_str(),
                creds.session_token.as_deref().unwrap(), "private-metadata-marker", signed.authorization.as_str()] {
                assert!(!rendered.contains(secret), "Debug exposed protected request data");
            }
            assert!(rendered.contains("SignedRequest"));
        }
        assert_eq!(signed.headers["Authorization"], signed.authorization);
        assert_eq!(signed.headers["x-obs-security-token"], "private-token-marker");
        assert_eq!(signed.headers["x-obs-meta-custom"], "private-metadata-marker");
    }

    #[test]
    fn canonicalize_obs_headers_sorted() {
        let mut headers = BTreeMap::new();
        headers.insert("x-obs-meta-b".into(), "2".into());
        headers.insert("x-obs-meta-a".into(), "1".into());
        headers.insert("x-hws-fs-client".into(), "true".into()); // not signed

        let canon = ObsSigner::canonicalize_obs_headers(&headers);
        assert!(canon.contains("x-obs-meta-a:1\n"));
        assert!(canon.contains("x-obs-meta-b:2\n"));
        assert!(!canon.contains("x-hws-fs"));
    }

    #[test]
    fn sign_produces_authorization_header() {
        let creds = Credentials::new("AK", "SK");
        let signed = ObsSigner::sign(
            "GET",
            "",
            "",
            "Mon, 01 Jan 2024 00:00:00 GMT",
            &BTreeMap::new(),
            "/bucket/key",
            &creds,
        );
        assert!(signed.authorization.starts_with("OBS AK:"));
    }
}
