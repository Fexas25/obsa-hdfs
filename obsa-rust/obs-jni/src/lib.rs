//! JNI bridge for org.apache.hadoop.fs.obs.NativeObs. Handles are opaque IDs;
//! panics and Rust errors never unwind across the JVM boundary.
use jni::{
    objects::{JByteArray, JByteBuffer, JClass, JString},
    sys::{jbyteArray, jint, jlong, jstring, JavaVM, JNI_VERSION_1_8},
    JNIEnv,
};
use obs_rust_core::{
    error::ObsResult,
    io::{ObsInputStream, ObsOutputStream, PfsAppendStream},
    types::{CreateOptions, OpenOptions},
    ObsConfig, ObsError, ObsFileSystem,
};
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc, Mutex,
    },
};
use tokio::{runtime::Runtime, sync::Mutex as AsyncMutex};
static RT: Lazy<Result<Runtime, std::io::Error>> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
});
static NEXT: AtomicI64 = AtomicI64::new(1);
static FILESYSTEMS: Lazy<Mutex<HashMap<i64, Arc<ObsFileSystem>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
enum Stream {
    Input(ObsInputStream),
    Output(ObsOutputStream),
    Append(PfsAppendStream),
}
type SharedStream = Arc<AsyncMutex<Option<Stream>>>;
static STREAMS: Lazy<Mutex<HashMap<i64, (i64, SharedStream)>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
type SharedListing = Arc<AsyncMutex<Option<obs_rust_core::listing::StatusCursor>>>;
static LISTINGS: Lazy<Mutex<HashMap<i64, (i64, SharedListing)>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

fn add_listing(owner: i64, path: String, files: Option<bool>) -> ObsResult<i64> {
    let filesystems = FILESYSTEMS.lock().map_err(io)?;
    if !filesystems.contains_key(&owner) {
        return Err(io("filesystem closed while opening listing"));
    }
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    LISTINGS.lock().map_err(io)?.insert(
        id,
        (
            owner,
            Arc::new(AsyncMutex::new(Some(match files {
                Some(recursive) => obs_rust_core::listing::StatusCursor::files(path, recursive),
                None => obs_rust_core::listing::StatusCursor::new(path),
            }))),
        ),
    );
    Ok(id)
}

async fn listing_next(owner: i64, id: i64, fs: &ObsFileSystem) -> ObsResult<Value> {
    let shared = LISTINGS
        .lock()
        .map_err(io)?
        .get(&id)
        .filter(|(fs_id, _)| *fs_id == owner)
        .map(|(_, cursor)| cursor.clone())
        .ok_or_else(|| io("invalid or closed listing handle"))?;
    let mut cursor = shared.lock().await;
    let page = cursor
        .as_mut()
        .ok_or_else(|| io("listing closed"))?
        .next_page(fs)
        .await;
    if page
        .as_ref()
        .map_or(true, |page| page.next_marker.is_none())
    {
        cursor.take();
        LISTINGS.lock().map_err(io)?.remove(&id);
    }
    serde_json::to_value(page?).map_err(io)
}
fn rt() -> ObsResult<&'static Runtime> {
    RT.as_ref().map_err(|e| ObsError::Io(e.to_string()))
}
fn invalid(s: impl Into<String>) -> ObsError {
    ObsError::InvalidArgument(s.into())
}
fn io(e: impl ToString) -> ObsError {
    ObsError::Io(e.to_string())
}
fn string(env: &mut JNIEnv, value: &JString) -> ObsResult<String> {
    env.get_string(value).map(|s| s.into()).map_err(io)
}
fn filesystem(id: i64) -> ObsResult<Arc<ObsFileSystem>> {
    FILESYSTEMS
        .lock()
        .map_err(io)?
        .get(&id)
        .cloned()
        .ok_or_else(|| invalid("invalid or closed filesystem handle"))
}
fn stream(id: i64) -> ObsResult<SharedStream> {
    STREAMS
        .lock()
        .map_err(io)?
        .get(&id)
        .map(|(_, s)| s.clone())
        .ok_or_else(|| invalid("invalid or closed stream handle"))
}
fn add_stream(owner: i64, value: Stream) -> ObsResult<i64> {
    let filesystems = FILESYSTEMS.lock().map_err(io)?;
    if !filesystems.contains_key(&owner) {
        return Err(io("filesystem closed while opening stream"));
    }
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    STREAMS
        .lock()
        .map_err(io)?
        .insert(id, (owner, Arc::new(AsyncMutex::new(Some(value)))));
    Ok(id)
}
fn guard<T: Default>(env: &mut JNIEnv, f: impl FnOnce(&mut JNIEnv) -> ObsResult<T>) -> T {
    let result =
        catch_unwind(AssertUnwindSafe(|| f(env))).unwrap_or_else(|_| Err(io("native panic")));
    match result {
        Ok(value) => value,
        Err(e) => {
            let class = match &e {
                error if error.is_path_not_found() => "java/io/FileNotFoundException",
                ObsError::IsDirectory(_) => "java/io/FileNotFoundException",
                ObsError::InvalidArgument(_) | ObsError::BadRequest { .. } => {
                    "java/lang/IllegalArgumentException"
                }
                ObsError::Unsupported(_) => "java/lang/UnsupportedOperationException",
                _ => "java/io/IOException",
            };
            let _ = env.throw_new(class, e.to_string());
            T::default()
        }
    }
}
#[no_mangle]
pub extern "system" fn JNI_OnLoad(_: *mut JavaVM, _: *mut std::ffi::c_void) -> jint {
    JNI_VERSION_1_8
}
#[no_mangle]
pub extern "system" fn Java_org_apache_hadoop_fs_obs_NativeObs_nativeCreate(
    mut env: JNIEnv,
    _: JClass,
    config: JString,
) -> jlong {
    guard(&mut env, |env| {
        let text = string(env, &config)?;
        let value: Value = serde_json::from_str(&text).map_err(invalid_json)?;
        let config: ObsConfig = if let Some(properties) = value.get("hadoop") {
            let properties: HashMap<String, String> =
                serde_json::from_value(properties.clone()).map_err(invalid_json)?;
            obs_rust_core::config::config_from_hadoop(
                &properties,
                value["bucket"]
                    .as_str()
                    .ok_or_else(|| invalid("bucket required"))?,
            )
        } else {
            serde_json::from_value(value).map_err(invalid_json)?
        };
        let fs = rt()?.block_on(ObsFileSystem::from_config(config))?;
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        FILESYSTEMS.lock().map_err(io)?.insert(id, Arc::new(fs));
        Ok(id)
    })
}
fn invalid_json(e: serde_json::Error) -> ObsError {
    invalid(format!("invalid JSON: {e}"))
}

/// Idempotent memory-only cleanup, also safe after filesystem close.
#[no_mangle]
pub extern "system" fn Java_org_apache_hadoop_fs_obs_NativeObs_nativeReleaseListing(
    _: JNIEnv,
    _: JClass,
    id: jlong,
) {
    if let Ok(mut listings) = LISTINGS.lock() {
        listings.remove(&id);
    }
}
#[no_mangle]
pub extern "system" fn Java_org_apache_hadoop_fs_obs_NativeObs_nativeCall(
    mut env: JNIEnv,
    _: JClass,
    id: jlong,
    op: JString,
    args: JString,
) -> jstring {
    guard(&mut env, |env| {
        let fs = filesystem(id)?;
        let op = string(env, &op)?;
        let args: Value = serde_json::from_str(&string(env, &args)?).map_err(invalid_json)?;
        let path = args.get("path").and_then(Value::as_str).unwrap_or("/");
        let value=rt()?.block_on(async{match op.as_str(){
            "isPfs"=>Ok(json!(fs.is_pfs())),
            "blockSize"=>Ok(json!(fs.default_block_size())),
            "contentSummaryEnabled"=>Ok(json!(fs.content_summary_enabled())),
            "recoverPendingUploads"=>{
                let limit=args.get("limit").and_then(Value::as_u64)
                    .filter(|n| (1..=10_000).contains(n))
                    .ok_or_else(||invalid("recovery limit must be an integer in 1..=10000"))?;
                let report=fs.recover_pending_uploads(limit as usize).await?;
                Ok(json!({"aborted":report.aborted,"active":report.active,"errors":report.errors}))
            },
            "status"=>serde_json::to_value(fs.get_file_status(path).await?).map_err(io),
            "exists"=>Ok(json!(fs.exists(path).await?)),
            "list"=>serde_json::to_value(fs.list_status(path).await?).map_err(io),
            "listPage"=>serde_json::to_value(fs.list_status_page(path,args.get("marker").and_then(Value::as_str)).await?).map_err(io),
            "listCursorOpen"=>Ok(json!(add_listing(id,path.to_owned(), if args["files_only"].as_bool().unwrap_or(false) { Some(args["recursive"].as_bool().unwrap_or(false)) } else { None })?)),
            "listCursorNext"=>listing_next(id,args["cursor"].as_i64().ok_or_else(||invalid("cursor required"))?,&fs).await,
            "listFiles"=>serde_json::to_value(fs.list_files_recursive(path).await?).map_err(io),
            "mkdirs"=>Ok(json!(fs.mkdirs(path,args["permission"].as_u64().unwrap_or(0o755) as u32).await?)),
            "delete"=>Ok(json!(fs.delete(path,args["recursive"].as_bool().unwrap_or(false)).await?)),
            "rename"=>Ok(json!(fs.rename(path,args["destination"].as_str().ok_or_else(||invalid("destination required"))?,args["overwrite"].as_bool().unwrap_or(false)).await?)),
            "truncate"=>{fs.truncate(path,args["length"].as_u64().ok_or_else(||invalid("nonnegative length required"))?).await?;Ok(json!(true))},
            "summary"=>{let s=fs.get_content_summary(path).await?;Ok(json!({"length":s.length,"file_count":s.file_count,"directory_count":s.directory_count,"space_consumed":s.space_consumed}))},
            "open"=>Ok(json!(add_stream(id,Stream::Input(fs.open(path,OpenOptions::default()).await?))?)),
            "create"=>Ok(json!(add_stream(id,Stream::Output(fs.create(path,CreateOptions{overwrite:args["overwrite"].as_bool().unwrap_or(false),create_parent:args["create_parent"].as_bool().unwrap_or(true),permission:args["permission"].as_u64().unwrap_or(0o644) as u32,..Default::default()}).await?))?)),
            "append"=>{let s=fs.get_file_status(path).await?;if s.is_dir{return Err(invalid("cannot append to directory"));}Ok(json!(add_stream(id,Stream::Append(fs.append_stream(path,s.length).await?))?))},
            "setPermission"=>{fs.set_hadoop_permission(path,args["permission"].as_u64().ok_or_else(||invalid("permission required"))? as u32).await?;Ok(Value::Null)},
            "setOwner"=>{fs.set_hadoop_owner(path,args.get("owner").and_then(Value::as_str),args.get("group").and_then(Value::as_str)).await?;Ok(Value::Null)},
            _=>Err(ObsError::Unsupported(format!("native operation: {op}"))),
        }})?;
        Ok(env.new_string(value.to_string()).map_err(io)?.into_raw())
    })
}
#[no_mangle]
pub extern "system" fn Java_org_apache_hadoop_fs_obs_NativeObs_nativeRead(
    mut env: JNIEnv,
    _: JClass,
    id: jlong,
    length: jint,
) -> jbyteArray {
    guard(&mut env, |env| {
        if !(0..=16 * 1024 * 1024).contains(&length) {
            return Err(invalid("read size must be 0..16 MiB"));
        }
        let shared = stream(id)?;
        let mut bytes = vec![0; length as usize];
        let n = rt()?.block_on(async {
            let mut lock = shared.lock().await;
            match lock.as_mut() {
                Some(Stream::Input(s)) => s.read(&mut bytes).await,
                _ => Err(invalid("not an input stream")),
            }
        })?;
        Ok(env
            .byte_array_from_slice(&bytes[..n])
            .map_err(io)?
            .into_raw())
    })
}
/// Read directly into the caller's array, avoiding a temporary Java array and
/// a second Java-to-Java copy on every read. The legacy ABI remains available.
#[no_mangle]
pub extern "system" fn Java_org_apache_hadoop_fs_obs_NativeObs_nativeReadInto(
    mut env: JNIEnv,
    _: JClass,
    id: jlong,
    data: JByteArray,
    offset: jint,
    length: jint,
) -> jint {
    guard(&mut env, |env| {
        let size = env.get_array_length(&data).map_err(io)?;
        if offset < 0 || length < 0 || offset > size - length {
            return Err(invalid("read bounds"));
        }
        let shared = stream(id)?;
        let mut bytes = vec![0; (length as usize).min(16 * 1024 * 1024)];
        let n = rt()?.block_on(async {
            let mut lock = shared.lock().await;
            match lock.as_mut() {
                Some(Stream::Input(s)) => s.read(&mut bytes).await,
                _ => Err(invalid("not an input stream")),
            }
        })?;
        if length == 0 {
            return Ok(0);
        }
        if n == 0 {
            return Ok(-1);
        }
        // SAFETY: i8/u8 share layout and all bit patterns; region setter copies
        // synchronously and does not retain the borrowed native buffer.
        let signed = unsafe { std::slice::from_raw_parts(bytes.as_ptr().cast::<i8>(), n) };
        env.set_byte_array_region(&data, offset, signed)
            .map_err(io)?;
        Ok(n as jint)
    })
}
#[no_mangle]
pub extern "system" fn Java_org_apache_hadoop_fs_obs_NativeObs_nativeReadDirect(
    mut env: JNIEnv,
    _: JClass,
    id: jlong,
    data: JByteBuffer,
    offset: jint,
    length: jint,
) -> jint {
    guard(&mut env, |env| {
        let capacity = env
            .get_direct_buffer_capacity(&data)
            .map_err(|_| invalid("direct buffer required"))?;
        if offset < 0 || length < 0 {
            return Err(invalid("direct read bounds"));
        }
        let start = offset as usize;
        let requested = length as usize;
        if start > capacity || requested > capacity - start {
            return Err(invalid("direct read bounds"));
        }
        if env
            .call_method(&data, "isReadOnly", "()Z", &[])
            .map_err(io)?
            .z()
            .map_err(io)?
        {
            return Err(invalid("read-only direct buffer"));
        }
        let actual = requested.min(16 * 1024 * 1024);
        let address = if actual == 0 {
            std::ptr::null_mut()
        } else {
            env.get_direct_buffer_address(&data).map_err(io)?
        };
        let shared = stream(id)?;
        let n = rt()?.block_on(async {
            let mut lock = shared.lock().await;
            match lock.as_mut() {
                Some(Stream::Input(s)) => {
                    if actual == 0 {
                        return s.read(&mut []).await;
                    }
                    // SAFETY: JNI validates a writable direct buffer, and the checked
                    // span fits its capacity. Its local reference keeps the storage
                    // (including a sliced buffer's attachment) alive until return.
                    // The caller must exclusively own this region during the call.
                    // No JVM heap memory is pinned and no pointer is retained afterward.
                    let bytes = unsafe { std::slice::from_raw_parts_mut(address.add(start), actual) };
                    s.read(bytes).await
                }
                _ => Err(invalid("not an input stream")),
            }
        })?;
        Ok(if n == 0 && requested != 0 { -1 } else { n as jint })
    })
}
#[no_mangle]
pub extern "system" fn Java_org_apache_hadoop_fs_obs_NativeObs_nativeWrite(
    mut env: JNIEnv,
    _: JClass,
    id: jlong,
    data: JByteArray,
    offset: jint,
    length: jint,
) -> jint {
    guard(&mut env, |env| {
        let size = env.get_array_length(&data).map_err(io)?;
        if offset < 0 || length < 0 || offset > size - length {
            return Err(invalid("write bounds"));
        }
        let shared = stream(id)?;
        rt()?.block_on(async {
            let mut lock = shared.lock().await;
            let stream = lock.as_mut().ok_or_else(|| io("stream closed"))?;
            if !matches!(stream, Stream::Output(_) | Stream::Append(_)) {
                return Err(invalid("not an output stream"));
            }
            // Only copy the requested slice, in bounded batches, regardless of
            // the size of the caller's backing array. Never pin JVM memory
            // across network I/O or an await point.
            let mut bytes = vec![0u8; (length as usize).min(1024 * 1024)];
            let mut written = 0;
            while written < length {
                let n = (length - written).min(bytes.len() as jint) as usize;
                // SAFETY: i8/u8 have identical size/alignment and all bit
                // patterns are valid; the exclusive borrow lasts only this call.
                let signed =
                    unsafe { std::slice::from_raw_parts_mut(bytes.as_mut_ptr().cast::<i8>(), n) };
                env.get_byte_array_region(&data, offset + written, signed)
                    .map_err(io)?;
                let count = match stream {
                    Stream::Output(s) => s.write(&bytes[..n]).await?,
                    Stream::Append(s) => s.write(&bytes[..n]).await?,
                    _ => unreachable!(),
                };
                if count != n {
                    return Err(io("short native write"));
                }
                written += n as jint;
            }
            Ok(written)
        })
    })
}
#[no_mangle]
pub extern "system" fn Java_org_apache_hadoop_fs_obs_NativeObs_nativeSkip(
    mut env: JNIEnv,
    _: JClass,
    id: jlong,
    amount: jlong,
) -> jlong {
    guard(&mut env, |_| {
        let shared = stream(id)?;
        rt()?.block_on(async {
            let mut lock = shared.lock().await;
            match lock.as_mut() {
                Some(Stream::Input(s)) => s.skip(amount.max(0) as u64).map(|n| n as jlong),
                _ => Err(invalid("not an input stream")),
            }
        })
    })
}
#[no_mangle]
pub extern "system" fn Java_org_apache_hadoop_fs_obs_NativeObs_nativeSeek(
    mut env: JNIEnv,
    _: JClass,
    id: jlong,
    position: jlong,
) {
    guard(&mut env, |_| {
        if position < 0 {
            return Err(invalid("negative seek"));
        }
        let shared = stream(id)?;
        rt()?.block_on(async {
            let mut lock = shared.lock().await;
            match lock.as_mut() {
                Some(Stream::Input(s)) => s.seek(position as u64),
                _ => Err(invalid("not an input stream")),
            }
        })
    })
}
#[no_mangle]
pub extern "system" fn Java_org_apache_hadoop_fs_obs_NativeObs_nativeFlush(
    mut env: JNIEnv,
    _: JClass,
    id: jlong,
) {
    guard(&mut env, |_| {
        let shared = stream(id)?;
        rt()?.block_on(async {
            let mut lock = shared.lock().await;
            match lock.as_mut() {
                Some(Stream::Output(s)) => s.flush().await,
                Some(Stream::Append(s)) => s.flush().await,
                _ => Err(invalid("not an output stream")),
            }
        })
    })
}
/// Explicitly discard a create stream. Rejected stream kinds keep their handles.
#[no_mangle]
pub extern "system" fn Java_org_apache_hadoop_fs_obs_NativeObs_nativeAbortStream(
    mut env: JNIEnv,
    _: JClass,
    id: jlong,
) {
    guard(&mut env, |_| {
        let shared = stream(id)?;
        rt()?.block_on(async {
            let mut current = shared.lock().await;
            match current.as_ref() {
                Some(Stream::Output(_)) => {},
                Some(_) => return Err(ObsError::Unsupported("abort requires a create stream".into())),
                None => return Err(invalid("stream closed")),
            }
            let Some(Stream::Output(output)) = current.take() else { unreachable!() };
            STREAMS.lock().map_err(io)?.remove(&id);
            drop(current);
            output.abort().await
        })
    })
}
#[no_mangle]
pub extern "system" fn Java_org_apache_hadoop_fs_obs_NativeObs_nativeCloseStream(
    mut env: JNIEnv,
    _: JClass,
    id: jlong,
) {
    guard(&mut env, |_| {
        let entry = STREAMS.lock().map_err(io)?.remove(&id);
        if let Some((_, shared)) = entry {
            rt()?.block_on(async {
                match shared.lock().await.take() {
                    Some(Stream::Output(s)) => s.close().await.map(|_| ()),
                    Some(Stream::Append(s)) => s.close().await,
                    _ => Ok(()),
                }
            })?;
        }
        Ok(())
    })
}
#[no_mangle]
pub extern "system" fn Java_org_apache_hadoop_fs_obs_NativeObs_nativeClose(
    mut env: JNIEnv,
    _: JClass,
    id: jlong,
) {
    guard(&mut env, |_| {
        FILESYSTEMS.lock().map_err(io)?.remove(&id);
        LISTINGS
            .lock()
            .map_err(io)?
            .retain(|_, (owner, _)| *owner != id);
        let shared = {
            let mut streams = STREAMS.lock().map_err(io)?;
            let ids: Vec<_> = streams
                .iter()
                .filter(|(_, v)| v.0 == id)
                .map(|(k, _)| *k)
                .collect();
            ids.into_iter()
                .filter_map(|id| streams.remove(&id).map(|(_, s)| s))
                .collect::<Vec<_>>()
        };
        rt()?.block_on(async {
            for s in shared {
                s.lock().await.take();
            }
        });
        Ok(())
    })
}
