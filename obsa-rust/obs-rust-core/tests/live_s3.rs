//! Filesystem flows against a real S3-compatible server or OBS bucket
//! (signature OBS_LIVE_SIGNATURE, default v2). The main flow, in AWS V2 signature
//! mode (fs.obs.signature.type=v2). Skipped unless OBS_LIVE_ENDPOINT,
//! OBS_LIVE_ACCESS_KEY, OBS_LIVE_SECRET_KEY and OBS_LIVE_BUCKET are set; the
//! bucket must exist. Every run works under a fresh prefix and removes it.
//! Credentials come only from the environment and are never printed.
//!
//!   OBS_LIVE_ENDPOINT=127.0.0.1:18080 OBS_LIVE_BUCKET=bucket ... \
//!   cargo test -p obs-rust-core --test live_s3 -- --nocapture
use obs_rust_core::{types::*, ObsConfig, ObsFileSystem};

fn var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

fn live_config() -> Option<ObsConfig> {
    live_config_for(&var("OBS_LIVE_BUCKET")?, BucketMode::Object,
        var("OBS_LIVE_SIGNATURE").unwrap_or_else(|| "v2".into()))
}

fn live_config_for(bucket: &str, bucket_mode: BucketMode, signature_type: String) -> Option<ObsConfig> {
    Some(ObsConfig {
        endpoint: var("OBS_LIVE_ENDPOINT")?,
        access_key: var("OBS_LIVE_ACCESS_KEY")?,
        secret_key: var("OBS_LIVE_SECRET_KEY")?,
        bucket: bucket.to_string(),
        secure: var("OBS_LIVE_SECURE").is_some_and(|v| v == "true"),
        signature_type,
        bucket_mode,
        path_style_access: true,
        addressing_style: AddressingStyle::Path,
        // Multipart uploads need parts of at least 5 MiB on S3 servers.
        multipart_size: 5 * 1024 * 1024,
        multipart_threshold: 5 * 1024 * 1024,
        readahead_chunk_size: 1024 * 1024,
        ..Default::default()
    })
}

async fn write(fs: &ObsFileSystem, path: &str, data: &[u8]) {
    let mut out = fs
        .create(path, CreateOptions { overwrite: true, ..Default::default() })
        .await
        .unwrap_or_else(|e| panic!("create {path}: {e}"));
    out.write(data).await.unwrap();
    out.close().await.unwrap_or_else(|e| panic!("close {path}: {e}"));
}

async fn read_all(fs: &ObsFileSystem, path: &str) -> Vec<u8> {
    let mut input = fs.open(path, OpenOptions::default()).await.unwrap();
    let mut data = vec![0; input.content_length() as usize];
    let mut filled = 0;
    while filled < data.len() {
        let n = input.read(&mut data[filled..]).await.unwrap();
        assert!(n > 0, "unexpected EOF in {path}");
        filled += n;
    }
    data
}

#[tokio::test]
async fn filesystem_flow_against_live_s3_compatible_server() {
    let Some(config) = live_config() else {
        eprintln!("skipped: OBS_LIVE_* not set");
        return;
    };
    let fs = ObsFileSystem::from_config(config).await.expect("connect");
    let root = format!("/obsa-live-{}", std::process::id());
    let names = ["plain", "space name", "plus+percent%", "中文-文件", "tilde~bang!star*"];

    // Small single-PUT objects with names that need percent-encoding.
    assert!(fs.mkdirs(&format!("{root}/dir"), 0o755).await.unwrap());
    for (index, name) in names.iter().enumerate() {
        write(&fs, &format!("{root}/dir/{name}"), format!("data-{index}").as_bytes()).await;
    }
    let mut listed: Vec<String> = fs.list_status(&format!("{root}/dir")).await.unwrap()
        .into_iter().map(|s| s.path.rsplit('/').next().unwrap().to_string()).collect();
    listed.sort();
    let mut expected: Vec<String> = names.iter().map(|s| s.to_string()).collect();
    expected.sort();
    assert_eq!(listed, expected);
    for (index, name) in names.iter().enumerate() {
        let path = format!("{root}/dir/{name}");
        let status = fs.get_file_status(&path).await.unwrap();
        assert!(!status.is_dir);
        assert_eq!(status.length, format!("data-{index}").len() as u64, "{name}");
        assert_eq!(read_all(&fs, &path).await, format!("data-{index}").as_bytes(), "{name}");
    }

    // Multipart upload, ranged sequential, positioned and vectored reads.
    let big: Vec<u8> = (0..12 * 1024 * 1024 + 321).map(|i| (i % 251) as u8).collect();
    let big_path = format!("{root}/big.bin");
    write(&fs, &big_path, &big).await;
    assert_eq!(fs.get_file_status(&big_path).await.unwrap().length, big.len() as u64);
    assert_eq!(read_all(&fs, &big_path).await, big);
    let input = fs.open(&big_path, OpenOptions::default()).await.unwrap();
    let reader = input.positioned_reader();
    let mut slice = vec![0; 3 * 1024 * 1024];
    assert_eq!(reader.read_at(5_000_000, &mut slice).await.unwrap(), slice.len());
    assert_eq!(slice, &big[5_000_000..5_000_000 + slice.len()]);
    let (mut a, mut b) = (vec![0; 1000], vec![0; 2000]);
    let mut ranges: Vec<(u64, &mut [u8])> = vec![(10_000_000, &mut b), (100, &mut a)];
    reader.read_ranges(&mut ranges, 4096, 1 << 20).await.unwrap();
    assert_eq!(a, &big[100..1100]);
    assert_eq!(b, &big[10_000_000..10_002_000]);

    // Object-bucket rename (copy + delete) of a file and of a directory tree.
    let moved = format!("{root}/moved 文件");
    assert!(fs.rename(&format!("{root}/dir/plain"), &moved, false).await.unwrap());
    assert_eq!(read_all(&fs, &moved).await, b"data-0");
    assert!(!fs.exists(&format!("{root}/dir/plain")).await.unwrap());
    assert!(fs.rename(&format!("{root}/dir"), &format!("{root}/dir2"), false).await.unwrap());
    assert_eq!(fs.list_status(&format!("{root}/dir2")).await.unwrap().len(), names.len() - 1);
    assert!(!fs.exists(&format!("{root}/dir")).await.unwrap());

    // Recursive delete (batch delete) and absence afterwards.
    assert!(fs.delete(&root, true).await.unwrap());
    assert!(!fs.exists(&root).await.unwrap());
    eprintln!("live S3 filesystem flow passed");
}


/// The signer must pick up refreshed credentials on the next request: a
/// wrong secret is rejected by the real server, then the correct one works.
#[tokio::test]
async fn refreshed_credentials_sign_the_next_live_request() {
    let Some(config) = live_config() else { return };
    let (access, secret) = (config.access_key.clone(), config.secret_key.clone());
    let fs = ObsFileSystem::from_config(ObsConfig { secret_key: format!("wrong-{secret}"), ..config })
        .await.expect("connect");
    let probe = format!("/obsa-live-creds-{}", std::process::id());
    let error = fs.exists(&probe).await.expect_err("a wrong secret must be rejected");
    assert!(!error.is_path_not_found(), "signature failure must not look like a missing path: {error}");
    fs.update_credentials(&access, &secret, None).unwrap();
    assert!(!fs.exists(&probe).await.expect("refreshed credentials must be used"));
    write(&fs, &format!("{probe}/f"), b"ok").await;
    assert_eq!(read_all(&fs, &format!("{probe}/f")).await, b"ok");
    assert!(fs.delete(&probe, true).await.unwrap());
}

/// Server-side encryption round trip. Enabled by OBS_LIVE_SSE_TYPE (sse-kms or
/// sse-c) and, for sse-c, OBS_LIVE_SSE_KEY (base64 AES-256); the bucket must
/// accept that encryption mode.
#[tokio::test]
async fn server_side_encryption_round_trip_on_live_bucket() {
    let Some(config) = live_config() else { return };
    let Some(kind) = var("OBS_LIVE_SSE_TYPE") else {
        eprintln!("skipped: OBS_LIVE_SSE_TYPE not set");
        return;
    };
    let fs = ObsFileSystem::from_config(ObsConfig {
        server_side_encryption_type: kind,
        server_side_encryption_key: var("OBS_LIVE_SSE_KEY"),
        ..config
    }).await.expect("connect");
    let root = format!("/obsa-live-sse-{}", std::process::id());
    let small = format!("{root}/small");
    write(&fs, &small, b"encrypted").await;
    assert_eq!(read_all(&fs, &small).await, b"encrypted");
    // Multipart (initiate + parts) and copy (rename on object buckets).
    let big: Vec<u8> = (0..6 * 1024 * 1024 + 7).map(|i| (i % 253) as u8).collect();
    write(&fs, &format!("{root}/big"), &big).await;
    assert!(fs.rename(&format!("{root}/big"), &format!("{root}/moved"), false).await.unwrap());
    assert_eq!(read_all(&fs, &format!("{root}/moved")).await, big);
    assert!(fs.delete(&root, true).await.unwrap());
    eprintln!("live SSE round trip passed");
}

/// PFS semantics on a real parallel file system bucket, OBS_LIVE_PFS_BUCKET,
/// in auto bucket mode with OBS-native signatures (OBS_LIVE_PFS_SIGNATURE
/// overrides). AccessLabel checks run only with OBS_LIVE_PFS_ACCESS_LABEL=true.
/// No public PFS emulator exists, so this only runs against Huawei Cloud.
#[tokio::test]
async fn pfs_semantics_on_live_bucket() {
    let Some(bucket) = var("OBS_LIVE_PFS_BUCKET") else {
        eprintln!("skipped: OBS_LIVE_PFS_BUCKET not set");
        return;
    };
    let signature = var("OBS_LIVE_PFS_SIGNATURE").unwrap_or_else(|| "obs".into());
    let Some(config) = live_config_for(&bucket, BucketMode::Auto, signature) else { return };
    let fs = ObsFileSystem::from_config(config).await.expect("connect");
    assert!(fs.is_pfs(), "bucket.mode=auto must detect the PFS bucket");
    let root = format!("/obsa-live-pfs-{}", std::process::id());

    // mkdirs uses the PFS folder request (`?folder`, kept pending this check).
    assert!(fs.mkdirs(&format!("{root}/dir 目录"), 0o755).await.unwrap());
    assert!(fs.get_file_status(&format!("{root}/dir 目录")).await.unwrap().is_dir);

    // Append extends the committed file; truncate shortens it.
    let file = format!("{root}/dir 目录/log");
    write(&fs, &file, b"head-").await;
    let mut append = fs.append_stream(&file, 5).await.unwrap();
    append.write(b"tail").await.unwrap();
    append.close().await.unwrap();
    assert_eq!(read_all(&fs, &file).await, b"head-tail");
    fs.truncate(&file, 4).await.unwrap();
    assert_eq!(fs.get_file_status(&file).await.unwrap().length, 4);
    assert_eq!(read_all(&fs, &file).await, b"head");

    // Directory rename is a single PFS operation.
    assert!(fs.rename(&format!("{root}/dir 目录"), &format!("{root}/renamed"), false).await.unwrap());
    assert_eq!(read_all(&fs, &format!("{root}/renamed/log")).await, b"head");
    assert!(!fs.exists(&format!("{root}/dir 目录")).await.unwrap());

    if var("OBS_LIVE_PFS_ACCESS_LABEL").is_some_and(|v| v == "true") {
        let dir = format!("{root}/renamed");
        fs.set_access_label(&dir, &["obsa-live-role".to_string()]).await.unwrap();
        assert_eq!(fs.get_access_label(&dir).await.unwrap(), Some(vec!["obsa-live-role".to_string()]));
        fs.remove_access_label(&dir).await.unwrap();
    }

    assert!(fs.delete(&root, true).await.unwrap());
    assert!(!fs.exists(&root).await.unwrap());
    eprintln!("live PFS semantics passed");
}

/// Hadoop's testRenamePopulatesFileAncestors, repeated: after renaming a
/// directory tree no ancestor of the moved file may remain at the source.
#[tokio::test]
async fn renamed_tree_leaves_no_source_ancestors() {
    let Some(config) = live_config() else { return };
    let rounds: usize = var("OBS_LIVE_RENAME_ROUNDS").and_then(|v| v.parse().ok()).unwrap_or(3);
    let fs = ObsFileSystem::from_config(config.clone()).await.expect("connect");
    let root = format!("/obsa-live-rename-{}", std::process::id());
    for round in 0..rounds {
        let src = format!("{root}/r{round}/source");
        let dst = format!("{root}/r{round}/dest");
        assert!(fs.mkdirs(&src, 0o755).await.unwrap());
        write(&fs, &format!("{src}/dir1/dir2/dir3/file4"), b"nested").await;
        assert!(fs.rename(&src, &dst, false).await.unwrap());
        for ancestor in ["", "/dir1", "/dir1/dir2", "/dir1/dir2/dir3", "/dir1/dir2/dir3/file4"] {
            let left = format!("{src}{ancestor}");
            if fs.exists(&left).await.unwrap() {
                use obs_rust_core::store::{ObjectStore, ObsStore};
                let mut store = ObsStore::new(config.clone(), obs_rust_core::auth::CredentialChain::new(
                    obs_rust_core::auth::Credentials::new(config.access_key.clone(), config.secret_key.clone())));
                store.init_bucket_mode().await.unwrap();
                let key = left.trim_start_matches('/');
                let flat = store.list(&format!("{}/", src.trim_start_matches('/')), "", None, 1000).await.unwrap();
                let head = store.head(key).await;
                let marker = store.head(&format!("{key}/")).await;
                if head.is_err() && marker.is_err() && flat.contents.is_empty() {
                    // No key exists; only a delimited LIST still reports the prefix.
                    // S3Proxy's jclouds backends do this under load; S3/OBS do not.
                    eprintln!("round {round}: server reports phantom prefix for {left} (no keys exist)");
                    continue;
                }
                panic!("round {round}: {left} survived rename; HEAD {key:?}={:?} HEAD {key:?}/={:?}; objects under source: {:?}",
                    head.map(|m| m.content_length), marker.map(|m| m.content_length),
                    flat.contents.iter().map(|e| &e.key).collect::<Vec<_>>());
            }
        }
        assert_eq!(read_all(&fs, &format!("{dst}/dir1/dir2/dir3/file4")).await, b"nested");
    }
    assert!(fs.delete(&root, true).await.unwrap());
}
