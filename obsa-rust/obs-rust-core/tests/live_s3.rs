//! Filesystem flow against a real S3-compatible server in AWS V2 signature
//! mode (fs.obs.signature.type=v2). Skipped unless OBS_LIVE_ENDPOINT,
//! OBS_LIVE_ACCESS_KEY, OBS_LIVE_SECRET_KEY and OBS_LIVE_BUCKET are set; the
//! bucket must exist. Every run works under a fresh prefix and removes it.
//! Credentials come only from the environment and are never printed.
//!
//!   OBS_LIVE_ENDPOINT=127.0.0.1:18080 OBS_LIVE_BUCKET=bucket ... \
//!   cargo test -p obs-rust-core --test live_s3 -- --nocapture
use obs_rust_core::{types::*, ObsConfig, ObsFileSystem};

fn live_config() -> Option<ObsConfig> {
    let var = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
    Some(ObsConfig {
        endpoint: var("OBS_LIVE_ENDPOINT")?,
        access_key: var("OBS_LIVE_ACCESS_KEY")?,
        secret_key: var("OBS_LIVE_SECRET_KEY")?,
        bucket: var("OBS_LIVE_BUCKET")?,
        secure: var("OBS_LIVE_SECURE").is_some_and(|v| v == "true"),
        signature_type: var("OBS_LIVE_SIGNATURE").unwrap_or_else(|| "v2".into()),
        bucket_mode: BucketMode::Object,
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

