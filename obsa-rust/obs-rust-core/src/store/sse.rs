//! Server-side encryption headers, matching the OBS Java SDK with OBS
//! (`x-obs-`) headers: `RequestConvertor.transSseKmsHeaders/transSseCHeaders/
//! transSseCSourceHeaders` and `ObsHeaders`. Which requests carry them follows
//! the Java OBSA reference (`SseWrapper` users).

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use md5::{Digest, Md5};

use crate::config::ObsConfig;
use crate::error::{ObsError, ObsResult};

type Headers = Vec<(&'static str, String)>;

#[derive(Clone, Default)]
pub(crate) struct SseHeaders {
    /// Object-creating requests: PUT, initiate MPU, copy destination, PFS modify.
    pub(crate) write: Headers,
    /// Requests that read or extend SSE-C data: GET, HEAD, upload part.
    pub(crate) customer: Headers,
    /// Copy source decryption for SSE-C.
    pub(crate) copy_source: Headers,
}

impl SseHeaders {
    /// Unlike the reference, which silently writes unencrypted data for an
    /// unknown type or an sse-c type without key, misconfiguration is an error.
    /// Messages never include the key.
    pub(crate) fn from_config(config: &ObsConfig) -> ObsResult<Self> {
        let key = config.server_side_encryption_key.as_deref();
        match config.server_side_encryption_type.to_ascii_lowercase().as_str() {
            "" | "none" => Ok(Self::default()),
            "sse-kms" => {
                let mut write = vec![("x-obs-server-side-encryption", "kms".to_string())];
                if let Some(id) = key {
                    http::HeaderValue::from_str(id).map_err(|_| {
                        ObsError::InvalidArgument("invalid sse-kms key id".into())
                    })?;
                    write.push(("x-obs-server-side-encryption-kms-key-id", id.to_string()));
                }
                Ok(Self { write, ..Default::default() })
            }
            "sse-c" => {
                let encoded = key.ok_or_else(|| ObsError::InvalidArgument(
                    "sse-c requires fs.obs.server-side-encryption-key".into()))?;
                let raw = BASE64.decode(encoded).ok().filter(|raw| raw.len() == 32).ok_or_else(|| {
                    ObsError::InvalidArgument(
                        "sse-c key must be a base64-encoded 256-bit AES key".into())
                })?;
                let digest = BASE64.encode(Md5::digest(&raw));
                let customer = vec![
                    ("x-obs-server-side-encryption-customer-algorithm", "AES256".to_string()),
                    ("x-obs-server-side-encryption-customer-key", encoded.to_string()),
                    ("x-obs-server-side-encryption-customer-key-MD5", digest.clone()),
                ];
                let copy_source = vec![
                    ("x-obs-copy-source-server-side-encryption-customer-algorithm", "AES256".to_string()),
                    ("x-obs-copy-source-server-side-encryption-customer-key", encoded.to_string()),
                    ("x-obs-copy-source-server-side-encryption-customer-key-MD5", digest),
                ];
                Ok(Self { write: customer.clone(), customer, copy_source })
            }
            _ => Err(ObsError::InvalidArgument(
                "fs.obs.server-side-encryption-type must be sse-kms, sse-c or none".into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(kind: &str, key: Option<&str>) -> ObsConfig {
        ObsConfig {
            server_side_encryption_type: kind.into(),
            server_side_encryption_key: key.map(str::to_owned),
            ..Default::default()
        }
    }

    #[test]
    fn kms_headers_apply_to_writes_only() {
        let sse = SseHeaders::from_config(&config("SSE-KMS", None)).unwrap();
        assert_eq!(sse.write, vec![("x-obs-server-side-encryption", "kms".to_string())]);
        assert!(sse.customer.is_empty() && sse.copy_source.is_empty());
        let sse = SseHeaders::from_config(&config("sse-kms", Some("key-id"))).unwrap();
        assert_eq!(sse.write[1], ("x-obs-server-side-encryption-kms-key-id", "key-id".to_string()));
    }

    #[test]
    fn customer_key_md5_is_over_the_decoded_key() {
        let key = BASE64.encode([7u8; 32]);
        let sse = SseHeaders::from_config(&config("sse-c", Some(&key))).unwrap();
        let md5 = BASE64.encode(Md5::digest([7u8; 32]));
        assert_eq!(sse.customer[2], ("x-obs-server-side-encryption-customer-key-MD5", md5.clone()));
        assert_eq!(sse.write, sse.customer);
        assert_eq!(sse.copy_source[1].1, key);
        assert_eq!(sse.copy_source[2].1, md5);
    }

    #[test]
    fn misconfiguration_fails_without_echoing_the_key() {
        let secret = BASE64.encode([9u8; 16]); // 128-bit: wrong size
        for (kind, key) in [("sse-c", None), ("sse-c", Some(secret.as_str())),
            ("sse-c", Some("not base64!")), ("aes", Some("x")), ("sse-kms", Some("bad\nid"))] {
            let error = SseHeaders::from_config(&config(kind, key)).err().expect(kind).to_string();
            assert!(!error.contains(&secret), "{error}");
        }
        assert!(SseHeaders::from_config(&config("none", Some("ignored"))).unwrap().write.is_empty());
    }
}
