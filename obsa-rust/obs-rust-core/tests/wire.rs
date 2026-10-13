use obs_rust_core::{
    auth::{CredentialChain, Credentials, ObsSigner},
    store::{ObjectStore, ObsStore},
    types::*,
    ObsConfig,
};
use std::collections::BTreeMap;

#[tokio::test]
async fn http_owned_body_is_not_charged_again_by_input_and_shares_upload_budget() {
    let tuning = ObsConfig { buffer_total_bytes: 4, readahead_chunk_size: 4, readahead_max_chunks: 0, ..Default::default() };
    let (store, server) = server_with_config(vec![
        ("206 Partial Content", "Content-Range: bytes 0-3/4\r\n", "data"),
        ("200 OK", "", "data"),
    ], tuning.clone()).await;
    let store = std::sync::Arc::new(store);
    let mut input = obs_rust_core::io::ObsInputStream::new(store.clone(), &tuning, "key".into(), 4, None);
    let mut bytes = [0; 4];
    input.read(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"data"); // exact budget: HTTP and input must not double charge
    let mut output = obs_rust_core::io::ObsOutputStream::new(store.clone(), &tuning, "target".into(),
        std::sync::Arc::new(tokio::sync::Semaphore::new(1)));
    assert!(output.write(b"x").await.unwrap_err().to_string().contains("shared buffer budget"));
    output.abort().await.unwrap();
    drop(input);
    store.get("key", None, None).await.unwrap();
    assert_eq!(server.await.unwrap().len(), 2);
}

#[tokio::test]
async fn response_disconnect_retries_only_idempotent_requests() {
    for operation in 0..6 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, mut stopped) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();
            loop {
                let (mut socket, _) = tokio::select! {
                    _ = &mut stopped => break,
                    connection = listener.accept() => connection.unwrap(),
                };
                let bytes = tokio::time::timeout(std::time::Duration::from_secs(3), async {
                    let mut bytes = Vec::new();
                    let mut chunk = [0; 4096];
                    loop {
                        let n = socket.read(&mut chunk).await.unwrap();
                        assert!(n > 0, "client disconnected before sending the request");
                        bytes.extend_from_slice(&chunk[..n]);
                        if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                            let headers = String::from_utf8_lossy(&bytes[..end]);
                            let length = headers.lines().find_map(|line| {
                                let (name, value) = line.split_once(':')?;
                                name.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().unwrap())
                            }).unwrap_or(0);
                            if bytes.len() >= end + 4 + length { break; }
                        }
                    }
                    bytes
                }).await.unwrap();
                requests.push(String::from_utf8(bytes).unwrap());
                // Entire request received, but no response: remote commit is uncertain.
                drop(socket);
            }
            requests
        });
        let config = ObsConfig {
            endpoint: address.to_string(), bucket: "bucket".into(), secure: false,
            retry_max_attempts: 2, retry_backoff_ms: 0, request_timeout_ms: 2000,
            ..Default::default()
        };
        let store = ObsStore::new(config.clone(), CredentialChain::new(Credentials::new("AK", "SK")));
        let result = match operation {
            0 => store.initiate_multipart_upload("file").await.map(|_| ()),
            1 => store.complete_multipart_upload("file", "upload", &[CompletedPart {
                part_number: 1, etag: "part-etag".into(),
            }]).await.map(|_| ()),
            2 => store.posix_rename("source", "target").await,
            3 => store.get("file", None, None).await.map(|_| ()),
            _ => {
                let idempotent = operation == 5;
                obs_rust_core::transport::HttpPool::new(config).execute(
                    if idempotent { http::Method::GET } else { http::Method::POST },
                    format!("http://{address}/file").parse().unwrap(), BTreeMap::new(),
                    (!idempotent).then(|| bytes::Bytes::from_static(b"payload")), idempotent,
                ).await.map(|_| ())
            }
        };
        let _ = stop.send(());
        let requests = server.await.unwrap();
        assert!(matches!(result, Err(obs_rust_core::ObsError::Transport(_))), "operation={operation}");
        assert_eq!(requests.len(), if operation == 3 || operation == 5 { 3 } else { 1 },
            "operation={operation}");
        if operation == 1 { assert!(requests[0].contains("CompleteMultipartUpload")); }
        if operation == 4 { assert!(requests[0].ends_with("payload")); }
    }
}

#[tokio::test]
async fn retry_after_delays_signed_and_raw_retryable_requests() {
    use obs_rust_core::transport::HttpPool;
    for raw in [false, true] {
        for status in ["429 Too Many Requests", "503 Service Unavailable"] {
            let (config, server) = mock_endpoint(
                vec![(status, "Retry-After: 1\r\n", ""), ("200 OK", "", "ok")],
                ObsConfig {
                    retry_backoff_ms: 0,
                    ..Default::default()
                },
            )
            .await;
            let start = std::time::Instant::now();
            if raw {
                let uri = format!("http://{}/file", config.endpoint).parse().unwrap();
                let response = HttpPool::new(config)
                    .execute(http::Method::GET, uri, BTreeMap::new(), None, true)
                    .await
                    .unwrap();
                assert_eq!(response.body.as_ref(), b"ok");
            } else {
                let store =
                    ObsStore::new(config, CredentialChain::new(Credentials::new("AK", "SK")));
                assert!(store.get("file", None, None).await.is_ok());
            }
            assert!(start.elapsed() >= std::time::Duration::from_secs(1));
            assert_eq!(server.await.unwrap().len(), 2);
        }
    }
}

#[tokio::test]
async fn rate_limit_does_not_replay_non_idempotent_operations() {
    for initiate in [false, true] {
        let (store, server) =
            server(vec![("429 Too Many Requests", "Retry-After: 60\r\n", "")]).await;
        let result = if initiate {
            store.initiate_multipart_upload("file").await.map(|_| ())
        } else {
            store.posix_rename("source", "target").await
        };
        assert!(matches!(
            result,
            Err(obs_rust_core::ObsError::Http { status: 429, .. })
        ));
        assert_eq!(server.await.unwrap().len(), 1);
    }
}

#[tokio::test]
async fn server_retry_after_is_bounded_by_total_request_deadline() {
    use obs_rust_core::transport::HttpPool;
    for raw in [false, true] {
        let (config, server) = mock_endpoint(
            vec![("429 Too Many Requests", "Retry-After: 60\r\n", "")],
            ObsConfig {
                request_timeout_ms: 300,
                retry_backoff_ms: 0,
                ..Default::default()
            },
        )
        .await;
        let error = if raw {
            let uri = format!("http://{}/file", config.endpoint).parse().unwrap();
            HttpPool::new(config)
                .execute(http::Method::GET, uri, BTreeMap::new(), None, true)
                .await
                .err()
                .unwrap()
        } else {
            ObsStore::new(config, CredentialChain::new(Credentials::new("AK", "SK")))
                .get("file", None, None)
                .await
                .err()
                .unwrap()
        };
        assert!(
            matches!(error, obs_rust_core::ObsError::Transport(ref e) if e == "HTTP request total timeout")
        );
        assert_eq!(server.await.unwrap().len(), 1);
    }
}
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[tokio::test]
async fn headers_and_body_share_deadline_and_timeout_releases_connection_slot() {
    use obs_rust_core::transport::HttpPool;
    use std::time::Duration;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri: http::Uri = format!("http://{}/file", listener.local_addr().unwrap())
        .parse()
        .unwrap();
    let server = tokio::spawn(async move {
        let (mut slow, _) = listener.accept().await.unwrap();
        let mut request = [0; 4096];
        assert!(slow.read(&mut request).await.unwrap() > 0);
        tokio::time::sleep(Duration::from_millis(600)).await;
        slow.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let slow_body = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(600)).await;
            let _ = slow.write_all(b"slow").await;
        });
        let (mut fast, _) = listener.accept().await.unwrap();
        assert!(fast.read(&mut request).await.unwrap() > 0);
        fast.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
            .await
            .unwrap();
        slow_body.await.unwrap();
    });
    let pool = HttpPool::new(ObsConfig {
        connection_maximum: 1,
        connection_timeout_ms: 1000,
        ..Default::default()
    });
    let result = pool
        .execute(http::Method::GET, uri.clone(), BTreeMap::new(), None, false)
        .await;
    assert!(
        matches!(result, Err(obs_rust_core::ObsError::Transport(ref e)) if e == "response body timeout"),
        "{result:?}"
    );
    let response = tokio::time::timeout(
        Duration::from_secs(3),
        pool.execute(http::Method::GET, uri, BTreeMap::new(), None, false),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(response.body.as_ref(), b"ok");
    server.await.unwrap();
}

async fn server(
    responses: Vec<(&'static str, &'static str, &'static str)>,
) -> (ObsStore, tokio::task::JoinHandle<Vec<String>>) {
    server_with_config(
        responses,
        ObsConfig {
            retry_backoff_ms: 1,
            ..Default::default()
        },
    )
    .await
}

async fn server_with_config(
    responses: Vec<(&'static str, &'static str, &'static str)>,
    tuning: ObsConfig,
) -> (ObsStore, tokio::task::JoinHandle<Vec<String>>) {
    let (config, handle) = mock_endpoint(responses, tuning).await;
    let mut store = ObsStore::new(
        config,
        CredentialChain::new(Credentials::new("AK", "SK").with_session_token("token")),
    );
    store.init_bucket_mode().await.unwrap();
    (store, handle)
}

async fn mock_endpoint(
    responses: Vec<(&'static str, &'static str, &'static str)>,
    tuning: ObsConfig,
) -> (ObsConfig, tokio::task::JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        let mut requests = Vec::new();
        for (status, headers, body) in responses {
            let (mut socket, _) =
                tokio::time::timeout(std::time::Duration::from_secs(5), listener.accept())
                    .await
                    .unwrap()
                    .unwrap();
            let mut bytes = Vec::new();
            let mut chunk = [0; 4096];
            loop {
                let n = socket.read(&mut chunk).await.unwrap();
                if n == 0 {
                    break;
                }
                bytes.extend_from_slice(&chunk[..n]);
                if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    let text = String::from_utf8_lossy(&bytes[..end]);
                    let len = text
                        .lines()
                        .find_map(|l| {
                            l.to_lowercase()
                                .strip_prefix("content-length:")
                                .and_then(|v| v.trim().parse::<usize>().ok())
                        })
                        .unwrap_or(0);
                    if bytes.len() >= end + 4 + len {
                        break;
                    }
                }
            }
            requests.push(String::from_utf8(bytes).unwrap());
            let response=format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n{headers}\r\n{body}",body.len());
            socket.write_all(response.as_bytes()).await.unwrap();
        }
        requests
    });
    let config = ObsConfig {
        endpoint: address.to_string(),
        bucket: "bucket".into(),
        secure: false,
        bucket_mode: BucketMode::Pfs,
        ..tuning
    };
    (config, handle)
}

#[tokio::test]
async fn get_replays_complete_hint_without_client_header() {
    let (store, server) = server(vec![("200 OK", "", "data")]).await;
    let hint = InodeHint::new(Some(42), Some("server%2Fencoded".into())).unwrap();
    store.get("key", None, Some(&hint)).await.unwrap();
    let requests = server.await.unwrap();
    let req = requests[0].to_lowercase();
    assert!(!req.contains("x-hws-fs-client:"));
    assert!(req.contains("x-hws-fs-inode-no: 42"));
    assert!(req.contains("x-hws-fs-inode-dentryname: server%2fencoded"));
    assert!(req.contains("x-obs-security-token: token"));
}
#[tokio::test]
async fn list_encodes_query_values_and_parses_pfs_fields() {
    let (store,server)=server(vec![("200 OK","","<ListBucketResult><Contents><Key>a &amp; b/file</Key><Size>1</Size><Mode>100644</Mode><InodeNo>42</InodeNo></Contents></ListBucketResult>")]).await;
    let page = store.list("a & b/", "/", Some("x+y"), 1000).await.unwrap();
    assert_eq!(page.contents[0].inode_no, Some(42));
    let requests = server.await.unwrap();
    assert!(requests[0].starts_with(
        "GET /bucket/?prefix=a%20%26%20b%2F&delimiter=%2F&max-keys=1000&marker=x%2By HTTP/1.1"
    ));
}
#[tokio::test]
async fn list_replays_marker_without_trimming_object_names() {
    let (store, server) = server(vec![
        ("200 OK", "", "<ListBucketResult><Contents><Key> dir/file </Key><Size>1</Size></Contents><CommonPrefixes><Prefix> dir/sub/ </Prefix></CommonPrefixes><IsTruncated>true</IsTruncated><NextMarker> marker &amp; </NextMarker></ListBucketResult>"),
        ("200 OK", "", "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>"),
    ]).await;
    let first = store.list(" dir/", "/", None, 1000).await.unwrap();
    store.list(" dir/", "/", first.next_marker.as_deref(), 1000).await.unwrap();
    let requests = server.await.unwrap();
    assert_eq!(first.contents[0].key, " dir/file ");
    assert_eq!(first.common_prefixes, vec![" dir/sub/ "]);
    assert_eq!(first.next_marker.as_deref(), Some(" marker & "));
    assert!(requests[1].starts_with("GET /bucket/?prefix=%20dir%2F&delimiter=%2F&max-keys=1000&marker=%20marker%20%26%20 HTTP/1.1"));
}

#[tokio::test]
async fn list_replays_marker_assembled_from_text_comments_and_cdata() {
    let (store, server) = server(vec![
        ("200 OK", "", "<ListBucketResult><Contents><Key>dir/<!--split--><![CDATA[file &]]></Key><Size>1<!--split-->2</Size></Contents><IsTruncated>tr<![CDATA[ue]]></IsTruncated><NextMarker> mark<!--split--><![CDATA[ &amp;]]> </NextMarker></ListBucketResult>"),
        ("200 OK", "", "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>"),
    ]).await;
    let first = store.list("dir/", "", None, 1000).await.unwrap();
    store.list("dir/", "", first.next_marker.as_deref(), 1000).await.unwrap();
    let requests = server.await.unwrap();
    assert_eq!(first.contents[0].key, "dir/file &");
    assert_eq!(first.contents[0].size, 12);
    assert!(first.is_truncated);
    assert_eq!(first.next_marker.as_deref(), Some(" mark &amp; "));
    assert!(requests[1].starts_with("GET /bucket/?prefix=dir%2F&delimiter=&max-keys=1000&marker=%20mark%20%26amp%3B%20 HTTP/1.1"));
}

#[tokio::test]
async fn list_rejects_ambiguous_fields_in_success_responses() {
    for body in [
        "<ListBucketResult><Contents><Key>dir/a</Key><Key>dir/b</Key></Contents></ListBucketResult>",
        "<ListBucketResult><IsTruncated>true</IsTruncated><IsTruncated>false</IsTruncated></ListBucketResult>",
        "<ListBucketResult><Contents><Key>dir/<Extension/>a</Key></Contents></ListBucketResult>",
    ] {
        let (store, server) = server(vec![("200 OK", "", body)]).await;
        assert!(matches!(store.list("dir/", "", None, 1000).await,
            Err(obs_rust_core::ObsError::Io(_))));
        assert_eq!(server.await.unwrap().len(), 1);
    }
}

#[tokio::test]
async fn multipart_cleanup_replays_complete_fragmented_upload_id() {
    let (store, server) = server(vec![
        ("200 OK", "", "<InitiateMultipartUploadResult><UploadId>up<!--split--><![CDATA[&amp;]]>load</UploadId></InitiateMultipartUploadResult>"),
        ("204 No Content", "", ""),
    ]).await;
    let id = store.initiate_multipart_upload("file").await.unwrap();
    store.abort_multipart_upload("file", &id).await.unwrap();
    let requests = server.await.unwrap();
    assert_eq!(id, "up&amp;load");
    assert!(requests[1].starts_with("DELETE /bucket/file?uploadId=up%26amp%3Bload HTTP/1.1"));
}

#[tokio::test]
async fn list_rejects_fields_at_wrong_xml_depth() {
    for body in [
        "<ListBucketResult><Contents><Extension><Key>dir/wrong</Key></Extension></Contents></ListBucketResult>",
        "<ListBucketResult><Extension><IsTruncated>false</IsTruncated></Extension></ListBucketResult>",
        "<ListBucketResult><Extension><Contents><Key>dir/wrong</Key></Contents></Extension></ListBucketResult>",
    ] {
        let (store, task) = server(vec![("200 OK", "", body)]).await;
        assert!(matches!(store.list("dir/", "", None, 1000).await, Err(obs_rust_core::ObsError::Io(_))));
        assert_eq!(task.await.unwrap().len(), 1);
    }
}

#[tokio::test]
async fn complete_multipart_is_never_retried() {
    let (store, server) = server(vec![(
        "503 Service Unavailable",
        "",
        "<Error><Code>Unavailable</Code></Error>",
    )])
    .await;
    assert!(store
        .complete_multipart_upload("key", "id", &[])
        .await
        .is_err());
    assert_eq!(server.await.unwrap().len(), 1);
}
#[tokio::test]
async fn complete_multipart_embedded_error_is_not_success() {
    let (store, server) = server(vec![(
        "200 OK",
        "",
        "<Error><Code>InternalError</Code><Message>failed</Message></Error>",
    )])
    .await;
    let error = store
        .complete_multipart_upload("key", "id", &[])
        .await
        .unwrap_err();
    assert_eq!(error.code(), Some("InternalError"));
    server.await.unwrap();
}
#[tokio::test]
async fn retryable_get_retries_server_failure() {
    let (store, server) = server(vec![
        ("503 Service Unavailable", "", ""),
        ("200 OK", "", "ok"),
    ])
    .await;
    store.get("key", None, None).await.unwrap();
    assert_eq!(server.await.unwrap().len(), 2);
}
#[tokio::test]
async fn pfs_delete_preserves_not_found() {
    let (store, server) = server(vec![(
        "404 Not Found",
        "",
        "<Error><Code>NoSuchKey</Code></Error>",
    )])
    .await;
    assert!(store.delete("key/", true).await.unwrap_err().is_not_found());
    let requests = server.await.unwrap();
    assert!(requests[0].to_lowercase().contains("x-obs-hdfs: true"));
}
#[tokio::test]
async fn batch_fallback_reports_individual_failure() {
    let (store, server) = server(vec![
        ("501 Not Implemented", "", ""),
        (
            "403 Forbidden",
            "",
            "<Error><Code>AccessDenied</Code></Error>",
        ),
    ])
    .await;
    assert_eq!(
        store
            .delete_batch(&["key".into()])
            .await
            .unwrap_err()
            .code(),
        Some("AccessDenied")
    );
    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].starts_with("POST "));
    assert!(requests[1].starts_with("DELETE "));
}
#[test]
fn canonical_list_query_excluded_and_subresources_sorted() {
    assert_eq!(
        ObsSigner::canonical_resource(
            "bucket",
            "",
            &[("prefix", Some("a")), ("max-keys", Some("10"))]
        ),
        "/bucket/"
    );
    assert_eq!(
        ObsSigner::canonical_resource(
            "bucket",
            "key",
            &[("uploadId", Some("abc")), ("partNumber", Some("2"))]
        ),
        "/bucket/key?partNumber=2&uploadId=abc"
    );
}
#[test]
fn session_token_is_part_of_signature() {
    let creds = Credentials::new("AK", "SK").with_session_token("token");
    let signed = ObsSigner::sign(
        "GET",
        "",
        "",
        "Mon, 01 Jan 2024 00:00:00 GMT",
        &BTreeMap::new(),
        "/bucket/key",
        &creds,
    );
    let explicit = BTreeMap::from([("x-obs-security-token".into(), "token".into())]);
    let expected = ObsSigner::sign(
        "GET",
        "",
        "",
        "Mon, 01 Jan 2024 00:00:00 GMT",
        &explicit,
        "/bucket/key",
        &Credentials::new("AK", "SK"),
    );
    assert_eq!(signed.authorization, expected.authorization);
    assert_eq!(signed.authorization, "OBS AK:tXU3pCi4NSYtS0VWv/mLG3ASEe8=");
}

#[tokio::test]
async fn pfs_mutations_all_send_hdfs_header() {
    let (store, server) = server(vec![
        ("200 OK", "", ""),
        ("200 OK", "", ""),
        ("200 OK", "", ""),
        ("200 OK", "", ""),
        ("200 OK", "", ""),
        ("200 OK", "", ""),
    ])
    .await;
    store.posix_truncate("file", 1).await.unwrap();
    store
        .posix_append("file", 1, bytes::Bytes::from_static(b"a"))
        .await
        .unwrap();
    store.create_folder("dir").await.unwrap();
    store.posix_rename("dir/", "new dir").await.unwrap();
    store
        .set_object_metadata("file", Some(0o640), Some(1), Some(2))
        .await
        .unwrap();
    let requests = server.await.unwrap();
    for request in requests.iter().filter(|request| !request.starts_with("HEAD ")) {
        assert!(request.to_lowercase().contains("x-obs-hdfs: true"));
    }
    assert!(requests[3].starts_with("POST /bucket/dir/?rename&name=new%20dir "));
}

#[tokio::test]
async fn metadata_replace_preserves_unmodified_fields_and_file_type() {
    for permission in [true, false] {
        let (store, server) = server(vec![
            ("200 OK", "x-obs-meta-mode: 100644\r\nx-obs-meta-uid: 17\r\nx-obs-meta-gid: 18\r\nx-obs-meta-project: retained\r\nx-obs-meta-user: alice\r\n", ""),
            ("200 OK", "", ""),
        ]).await;
        if permission {
            store.set_object_metadata("file", Some(0o600), None, None).await.unwrap();
        } else {
            store.set_object_metadata("file", None, Some(42), Some(43)).await.unwrap();
        }
        let requests = server.await.unwrap();
        assert!(requests[0].starts_with("HEAD /bucket/file "));
        let update = requests[1].to_lowercase();
        assert!(update.starts_with("put /bucket/file?metadata "));
        assert!(update.contains("x-obs-meta-project: retained\r\n"));
        assert!(update.contains("x-obs-meta-user: alice\r\n"));
        assert!(update.contains(if permission { "x-obs-meta-mode: 100600\r\n" } else { "x-obs-meta-mode: 100644\r\n" }));
        assert!(update.contains(if permission { "x-obs-meta-uid: 17\r\n" } else { "x-obs-meta-uid: 42\r\n" }));
        assert!(update.contains(if permission { "x-obs-meta-gid: 18\r\n" } else { "x-obs-meta-gid: 43\r\n" }));
    }
}

#[tokio::test]
async fn metadata_read_failure_does_not_send_replace() {
    let (store, server) = server(vec![("404 Not Found", "", "")]).await;
    assert!(store.set_object_metadata("missing", Some(0o600), None, None).await.is_err());
    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].starts_with("HEAD "));
}
#[tokio::test]
async fn multipart_initiation_preserves_file_metadata() {
    let (store, server) = server(vec![(
        "200 OK",
        "",
        "<InitiateMultipartUploadResult><UploadId>u</UploadId></InitiateMultipartUploadResult>",
    )])
    .await;
    store
        .initiate_multipart_upload_with_headers(
            "file",
            obs_rust_core::store::PutHeaders {
                user_meta: vec![("mode".into(), "100640".into())],
                hdfs_client: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let requests = server.await.unwrap();
    assert!(requests[0]
        .to_lowercase()
        .contains("x-obs-meta-mode: 100640"));
}

#[tokio::test]
async fn conditional_get_sends_etag_and_preserves_range() {
    let (store, task) = server(vec![(
        "206 Partial Content",
        "ETag: \"version-a\"\r\nContent-Range: bytes 0-3/4\r\n",
        "data",
    )])
    .await;
    store
        .get_with_etag("key", Some((0, 3)), None, Some("version-a"))
        .await
        .unwrap();
    let requests = task.await.unwrap();
    let request = requests[0].to_lowercase();
    assert!(request.contains("if-match: \"version-a\"\r\n"));
    assert!(request.contains("range: bytes=0-3\r\n"));
}

#[tokio::test]
async fn conditional_get_412_is_not_retried() {
    let (store, task) = server(vec![(
        "412 Precondition Failed",
        "",
        "<Error><Code>PreconditionFailed</Code></Error>",
    )])
    .await;
    let result = store
        .get_with_etag("key", Some((0, 3)), None, Some("version-a"))
        .await;
    assert!(matches!(
        result,
        Err(obs_rust_core::ObsError::RemoteFileChanged(_))
    ));
    assert_eq!(task.await.unwrap().len(), 1);
}

#[tokio::test]
async fn conditional_get_rejects_ignored_precondition_and_missing_etag() {
    for header in [
        "ETag: version-b\r\nContent-Range: bytes 0-3/4\r\n",
        "Content-Range: bytes 0-3/4\r\n",
    ] {
        let (store, task) = server(vec![("206 Partial Content", header, "data")]).await;
        let result = store
            .get_with_etag("key", Some((0, 3)), None, Some("version-a"))
            .await;
        assert!(matches!(
            result,
            Err(obs_rust_core::ObsError::RemoteFileChanged(_))
                | Err(obs_rust_core::ObsError::MissingVersion(_))
        ));
        assert_eq!(task.await.unwrap().len(), 1);
    }
}

#[tokio::test]
async fn ranged_get_rejects_wrong_status_offsets_and_lengths() {
    for (status, header, body) in [
        ("200 OK", "", "data"),
        ("206 Partial Content", "", "data"),
        (
            "206 Partial Content",
            "Content-Range: bytes 1-4/5\r\n",
            "data",
        ),
        (
            "206 Partial Content",
            "Content-Range: bytes 0-2/4\r\n",
            "dat",
        ),
        (
            "206 Partial Content",
            "Content-Range: bytes 0-3/4\r\n",
            "extra",
        ),
        (
            "206 Partial Content",
            "Content-Range: bytes 0-3/4\r\n",
            "dat",
        ),
        (
            "206 Partial Content",
            "Content-Range: bytes 0-3/0\r\n",
            "data",
        ),
    ] {
        let (store, task) = server(vec![(status, header, body)]).await;
        assert!(matches!(
            store.get("key", Some((0, 3)), None).await,
            Err(obs_rust_core::ObsError::Io(_))
        ));
        assert_eq!(task.await.unwrap().len(), 1);
    }
}
#[tokio::test]
async fn ranged_get_allows_end_clipped_at_eof() {
    let (store, task) = server(vec![(
        "206 Partial Content",
        "Content-Range: bytes 2-3/4\r\n",
        "ta",
    )])
    .await;
    store.get("key", Some((2, 9)), None).await.unwrap();
    assert_eq!(task.await.unwrap().len(), 1);
}

async fn bounded_response(
    raw: &'static str,
    method: http::Method,
    limit: u64,
) -> obs_rust_core::error::ObsResult<obs_rust_core::transport::pool::HttpResponse> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = vec![];
        let mut byte = [0];
        while !request.ends_with(b"\r\n\r\n") {
            socket.read_exact(&mut byte).await.unwrap();
            request.push(byte[0]);
        }
        socket.write_all(raw.as_bytes()).await.unwrap();
    });
    let pool = obs_rust_core::transport::HttpPool::new(ObsConfig {
        http_response_max_bytes: limit,
        connection_timeout_ms: 1000,
        ..Default::default()
    });
    let result = pool
        .execute(
            method,
            format!("http://{address}/file").parse().unwrap(),
            BTreeMap::new(),
            None,
            false,
        )
        .await;
    task.await.unwrap();
    result
}
#[tokio::test]
async fn response_limit_rejects_oversize_before_waiting_for_body() {
    let result = bounded_response(
        "HTTP/1.1 200 OK\r\nContent-Length: 999999999\r\n\r\n",
        http::Method::GET,
        4,
    )
    .await;
    assert!(matches!(result, Err(obs_rust_core::ObsError::Io(_))));
}
#[tokio::test]
async fn response_limit_counts_chunked_bodies() {
    let result = bounded_response(
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n",
        http::Method::GET,
        4,
    )
    .await;
    assert!(matches!(result, Err(obs_rust_core::ObsError::Io(_))));
    let result = bounded_response(
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nab\r\n2\r\ncd\r\n0\r\n\r\n",
        http::Method::GET,
        4,
    )
    .await
    .unwrap();
    assert_eq!(result.body.as_ref(), b"abcd");
}
#[tokio::test]
async fn head_content_length_is_object_size_not_body_size() {
    let response = bounded_response(
        "HTTP/1.1 200 OK\r\nContent-Length: 999999999\r\n\r\n",
        http::Method::HEAD,
        4,
    )
    .await
    .unwrap();
    assert_eq!(response.content_length(), Some(999999999));
    assert!(response.body.is_empty());
}

#[tokio::test]
async fn copy_and_complete_preserve_fragmented_etags() {
    for copy in [false, true] {
        let body = if copy {
            "<CopyObjectResult><ETag>&quot;ab<!--split--><![CDATA[&amp;]]>cd&quot;</ETag></CopyObjectResult>"
        } else {
            "<CompleteMultipartUploadResult><ETag>&quot;ab<!--split--><![CDATA[&amp;]]>cd&quot;</ETag></CompleteMultipartUploadResult>"
        };
        let (store, task) = server(vec![("200 OK", "", body)]).await;
        let meta = if copy { store.copy("source", "target").await }
            else { store.complete_multipart_upload("file", "upload", &[]).await }.unwrap();
        assert_eq!(meta.etag.as_deref(), Some("ab&amp;cd"));
        assert_eq!(task.await.unwrap().len(), 1);
    }
}

#[tokio::test]
async fn copy_and_complete_reject_ambiguous_etags() {
    macro_rules! responses {
        ($root:literal) => { [
            concat!("<", $root, "><ETag>a</ETag><ETag>b</ETag></", $root, ">"),
            concat!("<", $root, "><ETag>a<Extra/>b</ETag></", $root, ">"),
            concat!("<", $root, "><Extra><ETag>a</ETag></Extra></", $root, ">"),
            concat!("<", $root, "><ETag>&quot; &quot;</ETag></", $root, ">"),
        ] };
    }
    for copy in [false, true] {
        for body in if copy { responses!("CopyObjectResult") } else { responses!("CompleteMultipartUploadResult") } {
            let (store, task) = server(vec![("200 OK", "", body)]).await;
            let result = if copy { store.copy("source", "target").await }
                else { store.complete_multipart_upload("file", "upload", &[]).await };
            assert!(matches!(result, Err(obs_rust_core::ObsError::Io(_))));
            assert_eq!(task.await.unwrap().len(), 1);
        }
    }
}

#[tokio::test]
async fn fragmented_error_fields_preserve_pfs_conflict_code() {
    let (store, task) = server(vec![("409 Conflict", "",
        "<Error><Code>Directory<!--split--><![CDATA[NotEmpty]]></Code><Message>not <!--split--><![CDATA[empty]]></Message></Error>")]).await;
    let error = store.delete("dir/", true).await.unwrap_err();
    assert!(error.is_conflict_with_code("DirectoryNotEmpty"));
    assert!(error.to_string().contains("not empty"));
    assert_eq!(task.await.unwrap().len(), 1);
}

#[tokio::test]
async fn cdata_embedded_errors_override_success_etags() {
    for copy in [false, true] {
        let body = if copy {
            "<CopyObjectResult><ETag>looks-successful</ETag><Error><Code><![CDATA[CopyFailed]]></Code><Message><![CDATA[rejected]]></Message></Error></CopyObjectResult>"
        } else {
            "<CompleteMultipartUploadResult><ETag>looks-successful</ETag><Error><Code><![CDATA[CompleteFailed]]></Code><Message><![CDATA[rejected]]></Message></Error></CompleteMultipartUploadResult>"
        };
        let (store, task) = server(vec![("200 OK", "", body)]).await;
        let result = if copy { store.copy("source", "target").await }
            else { store.complete_multipart_upload("file", "upload", &[]).await };
        assert!(matches!(result, Err(obs_rust_core::ObsError::Http { status: 200, ref code, ref message, .. })
            if code == if copy { "CopyFailed" } else { "CompleteFailed" } && message == "rejected"));
        assert_eq!(task.await.unwrap().len(), 1);
    }
}

#[tokio::test]
async fn copy_encodes_source_and_parses_result() {
    let (store, task) = server(vec![(
        "200 OK",
        "",
        "<CopyObjectResult><ETag>&quot;copied&quot;</ETag></CopyObjectResult>",
    )])
    .await;
    let result = store.copy("dir/雪 +%?#", "dest").await.unwrap();
    assert_eq!(result.etag.as_deref(), Some("copied"));
    let requests = task.await.unwrap();
    assert!(requests[0]
        .to_lowercase()
        .contains("x-obs-copy-source: /bucket/dir/%e9%9b%aa%20%2b%25%3f%23\r\n"));
}
#[tokio::test]
async fn copy_rejects_embedded_errors_and_incomplete_success() {
    for body in [
        "<Error><Code>CopyFailed</Code></Error>",
        "",
        "<CopyObjectResult><ETag>x</ETag>",
        "<CopyObjectResult/>",
        "<Other><ETag>x</ETag></Other>",
    ] {
        let (store, task) = server(vec![("200 OK", "", body)]).await;
        assert!(store.copy("source", "target").await.is_err());
        assert_eq!(task.await.unwrap().len(), 1);
    }
}
#[tokio::test]
async fn multipart_complete_rejects_truncated_success_xml() {
    let (store, task) = server(vec![(
        "200 OK",
        "",
        "<CompleteMultipartUploadResult><ETag>x</ETag>",
    )])
    .await;
    assert!(store
        .complete_multipart_upload("file", "upload", &[])
        .await
        .is_err());
    assert_eq!(task.await.unwrap().len(), 1);
}

#[tokio::test]
async fn permanent_server_errors_are_not_retried() {
    for status in [
        "501 Not Implemented",
        "505 HTTP Version Not Supported",
        "507 Insufficient Storage",
    ] {
        let (store, task) = server(vec![(status, "", "")]).await;
        let error = store.get("file", None, None).await.err().unwrap();
        assert!(matches!(
            error,
            obs_rust_core::ObsError::Http {
                status: 501 | 505 | 507,
                ..
            }
        ));
        assert_eq!(task.await.unwrap().len(), 1);
    }
}
#[tokio::test]
async fn initiating_upload_and_renaming_are_not_replayed() {
    let (store, task) = server(vec![("503 Service Unavailable", "", "")]).await;
    assert!(matches!(
        store.initiate_multipart_upload("file").await,
        Err(obs_rust_core::ObsError::Http { status: 503, .. })
    ));
    assert_eq!(task.await.unwrap().len(), 1);
    let (store, task) = server(vec![("503 Service Unavailable", "", "")]).await;
    assert!(matches!(
        store.posix_rename("source", "target").await,
        Err(obs_rust_core::ObsError::Http { status: 503, .. })
    ));
    assert_eq!(task.await.unwrap().len(), 1);
}
#[tokio::test]
async fn batch_delete_rejects_missing_truncated_or_error_results() {
    for body in [
        "",
        "<DeleteResult>",
        "<Other/>",
        "<DeleteResult><Error/></DeleteResult>",
        "<DeleteResult xmlns:o=\"urn:obs\"><o:Error/></DeleteResult>",
        "<DeleteResult><Error><Key>file</Key></Error></DeleteResult>",
        "<DeleteResult><Error><Key>file</Key><Code>AccessDenied</Code></Error></DeleteResult>",
    ] {
        let (store, task) = server(vec![("200 OK", "", body)]).await;
        assert!(
            store.delete_batch(&["file".into()]).await.is_err(),
            "accepted {body}"
        );
        assert_eq!(task.await.unwrap().len(), 1);
    }
    let (store, task) = server(vec![("200 OK", "", "<DeleteResult/>")]).await;
    store.delete_batch(&["file".into()]).await.unwrap();
    assert_eq!(task.await.unwrap().len(), 1);
}

#[tokio::test]
async fn configured_retry_limits_apply_to_signed_and_raw_requests() {
    use obs_rust_core::{config::config_from_hadoop, transport::HttpPool};
    for raw in [false, true] {
        for retries in [0, 3] {
            let props = std::collections::HashMap::from([
                ("fs.obs.retry.max.attempts".into(), retries.to_string()),
                ("fs.obs.retry.backoff.ms".into(), "0".into()),
            ]);
            let tuning = config_from_hadoop(&props, "bucket");
            let responses = if retries == 0 {
                vec![("503 Service Unavailable", "", "")]
            } else {
                vec![
                    ("503 Service Unavailable", "", ""),
                    ("503 Service Unavailable", "", ""),
                    ("503 Service Unavailable", "", ""),
                    ("200 OK", "", "ok"),
                ]
            };
            let (config, server) = mock_endpoint(responses, tuning).await;
            if raw {
                let uri = format!("http://{}/file", config.endpoint).parse().unwrap();
                let result = HttpPool::new(config)
                    .execute(http::Method::GET, uri, BTreeMap::new(), None, true)
                    .await
                    .unwrap();
                assert_eq!(result.status.as_u16(), if retries == 0 { 503 } else { 200 });
            } else {
                let store =
                    ObsStore::new(config, CredentialChain::new(Credentials::new("AK", "SK")));
                let result = store.get("file", None, None).await;
                if retries == 0 {
                    assert!(matches!(
                        result,
                        Err(obs_rust_core::ObsError::Http { status: 503, .. })
                    ));
                } else {
                    assert!(result.is_ok());
                }
            }
            assert_eq!(server.await.unwrap().len(), retries as usize + 1);
        }
    }
}

#[tokio::test]
async fn retry_exhaustion_preserves_last_service_error() {
    use obs_rust_core::transport::HttpPool;
    for raw in [false, true] {
        let body = "<Error><Code>Busy</Code><Message>last failure</Message></Error>";
        let (config, server) = mock_endpoint(
            vec![
                ("503 Service Unavailable", "x-obs-request-id: first\r\n", ""),
                (
                    "503 Service Unavailable",
                    "x-obs-request-id: last\r\n",
                    body,
                ),
            ],
            ObsConfig {
                retry_max_attempts: 1,
                retry_backoff_ms: 0,
                ..Default::default()
            },
        )
        .await;
        if raw {
            let uri = format!("http://{}/file", config.endpoint).parse().unwrap();
            let result = HttpPool::new(config)
                .execute(http::Method::GET, uri, BTreeMap::new(), None, true)
                .await
                .unwrap();
            assert_eq!(result.status.as_u16(), 503);
            assert_eq!(result.request_id, "last");
            assert_eq!(result.body.as_ref(), body.as_bytes());
        } else {
            let store = ObsStore::new(config, CredentialChain::new(Credentials::new("AK", "SK")));
            let error = store.get("file", None, None).await.err().unwrap();
            assert!(
                matches!(error, obs_rust_core::ObsError::Http { status: 503, ref code, ref message, ref request_id }
                if code == "Busy" && message == "last failure" && request_id == "last")
            );
        }
        assert_eq!(server.await.unwrap().len(), 2);
    }
}

#[tokio::test]
async fn total_deadline_interrupts_backoff_without_another_attempt() {
    use obs_rust_core::transport::HttpPool;
    for raw in [false, true] {
        let (config, server) = mock_endpoint(
            vec![("503 Service Unavailable", "", "")],
            ObsConfig {
                request_timeout_ms: 300,
                retry_backoff_ms: 60000,
                retry_max_attempts: 10,
                ..Default::default()
            },
        )
        .await;
        let error = if raw {
            let uri = format!("http://{}/file", config.endpoint).parse().unwrap();
            HttpPool::new(config)
                .execute(http::Method::GET, uri, BTreeMap::new(), None, true)
                .await
                .err()
                .unwrap()
        } else {
            ObsStore::new(config, CredentialChain::new(Credentials::new("AK", "SK")))
                .get("file", None, None)
                .await
                .err()
                .unwrap()
        };
        assert!(
            matches!(error, obs_rust_core::ObsError::Transport(ref e) if e == "HTTP request total timeout")
        );
        assert_eq!(server.await.unwrap().len(), 1);
    }
}

#[tokio::test]
async fn total_deadline_cancels_stalled_body_and_pool_remains_usable() {
    use obs_rust_core::transport::HttpPool;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri: http::Uri = format!("http://{}/file", listener.local_addr().unwrap())
        .parse()
        .unwrap();
    let server = tokio::spawn(async move {
        let (mut slow, _) = listener.accept().await.unwrap();
        let mut request = [0; 4096];
        assert!(slow.read(&mut request).await.unwrap() > 0);
        slow.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        // Keep the first body open until a second request proves slot release.
        let (mut fast, _) = listener.accept().await.unwrap();
        assert!(fast.read(&mut request).await.unwrap() > 0);
        fast.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
            .await
            .unwrap();
        drop(slow);
    });
    let pool = HttpPool::new(ObsConfig {
        connection_maximum: 1,
        connection_timeout_ms: 60000,
        request_timeout_ms: 300,
        retry_max_attempts: 10,
        ..Default::default()
    });
    let error = pool
        .execute(http::Method::GET, uri.clone(), BTreeMap::new(), None, true)
        .await
        .unwrap_err();
    assert!(
        matches!(error, obs_rust_core::ObsError::Transport(ref e) if e == "HTTP request total timeout")
    );
    let response = pool
        .execute(http::Method::GET, uri, BTreeMap::new(), None, true)
        .await
        .unwrap();
    assert_eq!(response.body.as_ref(), b"ok");
    server.await.unwrap();
}


#[tokio::test]
async fn upload_part_etag_rejects_missing_or_empty_response() {
    for headers in ["", "ETag: \r\n", "ETag: \"\"\r\n", "ETag: \"   \"\r\n"] {
        let (store, server) = server(vec![("200 OK", headers, "")]).await;
        let result = store.upload_part("file", "upload", 1, bytes::Bytes::from_static(b"data")).await;
        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].starts_with("PUT "));
        assert!(matches!(result, Err(obs_rust_core::ObsError::Io(ref message)) if message.contains("ETag")),
            "empty part ETag must not be accepted: {result:?}");
    }
}

#[tokio::test]
async fn upload_part_etag_preserves_nonempty_opaque_value() {
    for headers in ["ETag: \"part&<opaque>-2\"\r\n", "eTaG: part&<opaque>-2\r\n"] {
        let (store, server) = server(vec![("200 OK", headers, "")]).await;
        let etag = store.upload_part("file", "upload", 1, bytes::Bytes::from_static(b"data")).await.unwrap();
        assert_eq!(etag, "part&<opaque>-2");
        assert_eq!(server.await.unwrap().len(), 1);
    }
}

#[tokio::test]
async fn upload_part_etag_failure_aborts_stream_without_complete() {
    let config = ObsConfig { multipart_threshold: 1, buffer_total_bytes: 128,
        retry_max_attempts: 0, ..Default::default() };
    let (store, server) = server_with_config(vec![
        ("200 OK", "", "<InitiateMultipartUploadResult><UploadId>upload</UploadId></InitiateMultipartUploadResult>"),
        ("200 OK", "", ""),
        ("204 No Content", "", ""),
        ("200 OK", "", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
    ], config.clone()).await;
    let store = std::sync::Arc::new(store);
    let gate = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
    let mut output = obs_rust_core::io::ObsOutputStream::new(store.clone(), &config, "file".into(), gate.clone());
    output.write(b"data").await.unwrap();
    let result = output.close().await;
    // An exact-budget GET succeeds only after the failed upload releases its buffer.
    let response = store.get("probe", None, None).await.unwrap();
    drop(response);
    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 4);
    assert!(requests[0].starts_with("POST /bucket/file?uploads "));
    assert!(requests[1].starts_with("PUT /bucket/file?partNumber=1&uploadId=upload "));
    assert!(requests[2].starts_with("DELETE /bucket/file?uploadId=upload "), "must abort, not complete");
    assert!(matches!(result, Err(obs_rust_core::ObsError::Io(ref message)) if message.contains("ETag")));
    assert_eq!(gate.available_permits(), 1);
    assert!(requests[3].starts_with("GET /bucket/probe "));
}

#[tokio::test]
async fn put_metadata_uses_uploaded_size_not_response_length() {
    for body in [bytes::Bytes::new(), bytes::Bytes::from_static(b"uploaded data")] {
        let size = body.len() as u64;
        let (store, task) = server(vec![("200 OK", "ETag: \"put-etag\"\r\n", "")]).await;
        let meta = store.put("file", body, obs_rust_core::store::PutHeaders::default()).await.unwrap();
        assert_eq!(meta.content_length, size);
        assert_eq!(meta.etag.as_deref(), Some("put-etag"));
        assert_eq!(task.await.unwrap().len(), 1);
    }
}

#[tokio::test]
async fn complete_metadata_does_not_report_xml_length_as_object_size() {
    let (store, task) = server(vec![("200 OK", "",
        "<CompleteMultipartUploadResult><ETag>final-etag</ETag></CompleteMultipartUploadResult>")]).await;
    let meta = store.complete_multipart_upload("file", "upload", &[]).await.unwrap();
    // The direct API has no part lengths; zero follows the existing copy convention.
    assert_eq!(meta.content_length, 0);
    assert_eq!(meta.etag.as_deref(), Some("final-etag"));
    assert_eq!(task.await.unwrap().len(), 1); // No extra HEAD request.
}

#[tokio::test]
async fn metadata_prefix_is_removed_exactly_once_on_read() {
    let (store, task) = server(vec![("200 OK",
        "x-obs-meta-mode: 100644\r\nX-Obs-Meta-X-Obs-Meta-Mode: custom\r\nx-obs-meta-x-obs-meta-x-obs-meta-tag: deep\r\n", "")]).await;
    let meta = store.head("file").await.unwrap();
    assert_eq!(task.await.unwrap().len(), 1);
    assert_eq!(meta.mode, Some(0o100644));
    assert_eq!(meta.user_meta.len(), 3);
    assert_eq!(meta.user_meta.get("mode").map(String::as_str), Some("100644"));
    assert_eq!(meta.user_meta.get("x-obs-meta-mode").map(String::as_str), Some("custom"));
    assert_eq!(meta.user_meta.get("x-obs-meta-x-obs-meta-tag").map(String::as_str), Some("deep"));
}

#[tokio::test]
async fn metadata_prefix_survives_permission_and_user_metadata_replace() {
    for permission in [true, false] {
        let (store, task) = server(vec![
            ("200 OK", "x-obs-meta-mode: 100644\r\nX-Obs-Meta-X-Obs-Meta-Mode: custom\r\nx-obs-meta-x-obs-meta-x-obs-meta-tag: deep\r\n", ""),
            ("200 OK", "", ""),
        ]).await;
        if permission {
            store.set_object_metadata("file", Some(0o600), None, None).await.unwrap();
        } else {
            store.patch_user_metadata("file", std::collections::HashMap::from([
                ("project".into(), "updated".into()),
            ])).await.unwrap();
        }
        let requests = task.await.unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].starts_with("HEAD /bucket/file "));
        let update = requests[1].to_ascii_lowercase();
        assert!(update.starts_with("put /bucket/file?metadata "));
        assert!(update.contains("x-obs-meta-x-obs-meta-mode: custom\r\n"));
        assert!(update.contains("x-obs-meta-x-obs-meta-x-obs-meta-tag: deep\r\n"));
        assert!(!update.contains("\r\nx-obs-meta-tag:"));
        assert!(update.contains(if permission {
            "x-obs-meta-mode: 100600\r\n"
        } else {
            "x-obs-meta-mode: 100644\r\n"
        }));
        if !permission { assert!(update.contains("x-obs-meta-project: updated\r\n")); }
    }
}

#[tokio::test]
async fn metadata_update_case_replaces_existing_header_once() {
    let (store, task) = server(vec![
        ("200 OK", "x-obs-meta-project: old\r\nx-obs-meta-keep: retained\r\n", ""),
        ("200 OK", "", ""),
    ]).await;
    store.patch_user_metadata("file", std::collections::HashMap::from([
        ("PrOjEcT".into(), "new".into()),
    ])).await.unwrap();
    let requests = task.await.unwrap();
    assert_eq!(requests.len(), 2);
    let update = requests[1].to_ascii_lowercase();
    let values: Vec<_> = update.lines().filter_map(|line| line.strip_prefix("x-obs-meta-project: ")).collect();
    assert_eq!(values, vec!["new"]);
    assert!(update.contains("x-obs-meta-keep: retained\r\n"));
}

#[tokio::test]
async fn metadata_update_case_conflict_is_rejected_before_head() {
    let (store, task) = server(vec![]).await;
    task.await.unwrap(); // Listener closed; validation must succeed without using it.
    for values in [("one", "two"), ("same", "same")] {
        let result = store.patch_user_metadata("file", std::collections::HashMap::from([
            ("Project".into(), values.0.into()), ("project".into(), values.1.into()),
        ])).await;
        assert!(matches!(result, Err(obs_rust_core::ObsError::InvalidArgument(ref message))
            if message.contains("duplicate metadata key")), "{result:?}");
    }
}

#[tokio::test]
async fn metadata_create_case_collisions_fail_before_network() {
    let (store, task) = server(vec![]).await;
    task.await.unwrap();
    for multipart in [false, true] {
        for second in ["Project", "project"] {
            let headers = obs_rust_core::store::PutHeaders {
                user_meta: vec![("Project".into(), "one".into()), (second.into(), "two".into())],
                ..Default::default()
            };
            let result = if multipart {
                store.initiate_multipart_upload_with_headers("file", headers).await.map(|_| ())
            } else {
                store.put("file", bytes::Bytes::from_static(b"data"), headers).await.map(|_| ())
            };
            assert!(matches!(result, Err(obs_rust_core::ObsError::InvalidArgument(ref message))
                if message.contains("duplicate metadata key")), "{result:?}");
        }
    }
}

#[tokio::test]
async fn metadata_create_normalizes_keys_and_preserves_values() {
    for multipart in [false, true] {
        let response = if multipart {
            "<InitiateMultipartUploadResult><UploadId>upload</UploadId></InitiateMultipartUploadResult>"
        } else { "" };
        let (store, task) = server(vec![("200 OK", "", response)]).await;
        let headers = obs_rust_core::store::PutHeaders {
            user_meta: vec![("PrOjEcT".into(), "Mixed Value".into()),
                ("X-Obs-Meta-Tag".into(), "Nested".into())],
            ..Default::default()
        };
        if multipart {
            store.initiate_multipart_upload_with_headers("file", headers).await.unwrap();
        } else {
            store.put("file", bytes::Bytes::from_static(b"data"), headers).await.unwrap();
        }
        let requests = task.await.unwrap();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].contains("x-obs-meta-project: Mixed Value\r\n"));
        assert!(requests[0].contains("x-obs-meta-x-obs-meta-tag: Nested\r\n"));
    }
}

#[tokio::test]
async fn metadata_boundary_directory_patch_targets_resolved_marker() {
    for explicit_marker in [false, true] {
        let mut responses = Vec::new();
        if !explicit_marker { responses.push(("404 Not Found", "", "")); }
        responses.push(("200 OK", "x-obs-meta-user: old\r\nx-obs-meta-keep: retained\r\n", ""));
        responses.push(("200 OK", "", ""));
        let (store, task) = server(responses).await;
        store.patch_user_metadata(if explicit_marker { "dir/" } else { "dir" },
            std::collections::HashMap::from([("user".into(), "new".into())])).await.unwrap();
        let requests = task.await.unwrap();
        assert_eq!(requests.len(), if explicit_marker { 2 } else { 3 });
        if !explicit_marker { assert!(requests[0].starts_with("HEAD /bucket/dir ")); }
        assert!(requests[requests.len()-2].starts_with("HEAD /bucket/dir/ "));
        let update = requests.last().unwrap().to_ascii_lowercase();
        assert!(update.starts_with("put /bucket/dir/?metadata "));
        assert!(update.contains("x-obs-meta-user: new\r\n"));
        assert!(update.contains("x-obs-meta-keep: retained\r\n"));
    }
}

#[tokio::test]
async fn metadata_boundary_head_errors_prevent_replace() {
    for patch in [false, true] {
        for status in ["403 Forbidden", "500 Internal Server Error"] {
            let (store, task) = server_with_config(vec![(status, "x-obs-request-id: head-error\r\n", "")],
                ObsConfig { retry_max_attempts: 0, ..Default::default() }).await;
            let result = if patch {
                store.patch_user_metadata("file", std::collections::HashMap::from([("user".into(), "new".into())])).await
            } else { store.set_object_metadata("file", Some(0o600), None, None).await };
            let error = result.unwrap_err();
            assert_eq!(error.request_id(), Some("head-error"));
            assert!(matches!(error, obs_rust_core::ObsError::Http { status: code, .. }
                if code == if status.starts_with("403") { 403 } else { 500 }));
            let requests = task.await.unwrap();
            assert_eq!(requests.len(), 1);
            assert!(requests[0].starts_with("HEAD /bucket/file "));
        }
    }
}

#[tokio::test]
async fn metadata_boundary_missing_directory_stops_after_fallback() {
    for explicit_marker in [false, true] {
        let mut responses = vec![("404 Not Found", "x-obs-request-id: missing\r\n", "")];
        if !explicit_marker { responses.push(("404 Not Found", "x-obs-request-id: missing\r\n", "")); }
        let (store, task) = server(responses).await;
        let error = store.patch_user_metadata(if explicit_marker { "dir/" } else { "dir" },
            std::collections::HashMap::from([("user".into(), "new".into())])).await.unwrap_err();
        assert!(error.is_not_found());
        assert_eq!(error.request_id(), Some("missing"));
        let requests = task.await.unwrap();
        assert_eq!(requests.len(), if explicit_marker { 1 } else { 2 });
        assert!(requests.iter().all(|request| request.starts_with("HEAD ")));
        assert!(requests.last().unwrap().starts_with("HEAD /bucket/dir/ "));
    }
}

#[tokio::test]
async fn metadata_invalid_fields_are_rejected_before_network() {
    let (store, task) = server(vec![]).await;
    task.await.unwrap();
    for operation in [2, 0, 1] {
        for (name, value, category) in [
            ("bad name", "private-value", "name"),
            ("bad:name", "private-value", "name"),
            ("bad\r\nname", "private-value", "name"),
            ("valid", "private\r\nvalue", "value"),
            ("valid", "private\0value", "value"),
        ] {
            let entries = vec![(name.to_string(), value.to_string())];
            let result = match operation {
                0 => store.put("file", bytes::Bytes::new(), obs_rust_core::store::PutHeaders {
                    user_meta: entries, ..Default::default()
                }).await.map(|_| ()),
                1 => store.initiate_multipart_upload_with_headers("file", obs_rust_core::store::PutHeaders {
                    user_meta: entries, ..Default::default()
                }).await.map(|_| ()),
                _ => store.patch_user_metadata("file", entries.into_iter().collect()).await,
            };
            match result {
                Err(obs_rust_core::ObsError::InvalidArgument(message)) => {
                    assert!(!message.contains("private"));
                }
                other => panic!("operation={operation}, category={category}: {other:?}"),
            }
        }
    }
}

#[tokio::test]
async fn batch_fallback_releases_error_body_before_individual_deletes() {
    let body = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let (store, task) = server_with_config(vec![
        ("501 Not Implemented", "", body),
        ("200 OK", "", body),
        ("200 OK", "", body),
    ], ObsConfig { buffer_total_bytes: 128, retry_max_attempts: 0, ..Default::default() }).await;
    let result = store.delete_batch(&["first".into(), "second".into()]).await;
    assert!(result.is_ok(), "fallback must release the 501 response budget: {result:?}");
    let requests = task.await.unwrap();
    assert_eq!(requests.len(), 3);
    assert!(requests[0].starts_with("POST /bucket/?delete "));
    assert!(requests[1].starts_with("DELETE /bucket/first "));
    assert!(requests[2].starts_with("DELETE /bucket/second "));
}
