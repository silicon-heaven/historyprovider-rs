use std::collections::BTreeMap;
use std::error::Error;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

use async_compression::tokio::write::GzipEncoder;
use futures::io::BufReader;
use futures::{Stream, StreamExt, TryStreamExt};
use log::{error, info, warn};
use sha1::Digest;
use shvclient::client::MetaMethods;
use shvclient::clientnode::{children_on_path, ConstantNode, METH_LS};
use shvclient::AppState;
use shvproto::RpcValue;
use shvrpc::metamethod::MetaMethod;
use shvrpc::rpcmessage::{RpcError, RpcErrorCode};
use shvrpc::{RpcMessage, RpcMessageMetaTags};
use tokio::fs::DirEntry;
use tokio::io::{AsyncBufRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio_stream::wrappers::ReadDirStream;
use tokio_util::compat::TokioAsyncReadCompatExt;
use tokio_util::io::ReaderStream;

use crate::dirtylog::DirtyLogCommand;
use crate::journalentry::JournalEntry;
use crate::journalrw::{journal_entries_to_rpcvalue, matches_path_pattern, GetLog2Params, GetLog2Since, JournalReaderLog2, Log2Header};
use crate::util::{get_files, is_log2_file};
use crate::{ClientCommandSender, State};

// History site node methods
const METH_GET_LOG: &str = "getLog";

const META_METHOD_GET_LOG: MetaMethod = MetaMethod {
    name: METH_GET_LOG,
    flags: 0,
    access: shvrpc::metamethod::AccessLevel::Read,
    param: "RpcValue",
    result: "RpcValue",
    signals: &[],
    description: "",
};

// Root node methods
const METH_UPTIME: &str = "uptime";
const METH_RELOAD_SITES: &str = "reloadSites";

// File nodes methods
const METH_HASH: &str = "hash";
const METH_SIZE: &str = "size";
pub(crate) const METH_READ: &str = "read";
const METH_READ_COMPRESSED: &str = "readCompressed";
// const METH_WRITE: &str = "write";
// const METH_DELETE: &str = "delete";

const METH_LS_FILES: &str = "lsfiles";

// _shvjournal node methods
const METH_LOG_SIZE_LIMIT: &str = "logSizeLimit";
const METH_TOTAL_LOG_SIZE: &str = "totalLogSize";
const METH_LOG_USAGE: &str = "logUsage";
const METH_SYNC_LOG: &str = "syncLog";
const METH_SYNC_INFO: &str = "syncInfo";
const METH_SANITIZE_LOG: &str = "sanitizeLog";

// _valuecache node methods
const METH_GET: &str = "get";
const METH_GET_CACHE: &str = "getCache";

static DOT_APP_NODE: std::sync::LazyLock<shvclient::appnodes::DotAppNode> = std::sync::LazyLock::new(||
    shvclient::appnodes::DotAppNode::new("historyprovider-rs")
);

type RpcRequestResult = Result<RpcValue, RpcError>;

enum NodeType {
    Root,
    DotApp,
    ValueCache,
    ShvJournal,
    History,
}

impl NodeType {
    fn from_path(path: &str) -> Self {
        match path {
            "" => Self::Root,
            ".app" => Self::DotApp,
            "_valuecache" => Self::ValueCache,
            path if path.starts_with("_shvjournal") => Self::ShvJournal,
            _ => Self::History,
        }
    }
}

fn rpc_error_unknown_method(method: impl AsRef<str>) -> RpcError {
    RpcError::new(
        RpcErrorCode::MethodNotFound,
        format!("Unknown method '{}'", method.as_ref())
    )
}

fn rpc_error_filesystem(err: std::io::Error) -> RpcError {
    RpcError::new(
        RpcErrorCode::MethodCallException,
        format!("Filesystem error: {err}")
    )
}

fn path_contains_parent_dir_references(path: impl AsRef<str>) -> bool {
    path.as_ref().split('/').any(|fragment| fragment == "..")
}

async fn shvjournal_methods_getter(path: impl AsRef<str>, app_state: AppState<State>) -> Option<MetaMethods> {
    let path = path.as_ref();
    if path == "_shvjournal" {
        return Some(MetaMethods::from(&[
                &MetaMethod {
                    name: METH_LS_FILES,
                    flags: 0,
                    access: shvrpc::metamethod::AccessLevel::Read,
                    param: "Map|Null",
                    result: "List",
                    signals: &[],
                    description: "",
                },
                &MetaMethod {
                    name: METH_LOG_SIZE_LIMIT,
                    flags: 0,
                    access: shvrpc::metamethod::AccessLevel::Developer,
                    param: "Null",
                    result: "Int",
                    signals: &[],
                    description: "",
                },
                &MetaMethod {
                    name: METH_TOTAL_LOG_SIZE,
                    flags: 0,
                    access: shvrpc::metamethod::AccessLevel::Developer,
                    param: "Null",
                    result: "Int",
                    signals: &[],
                    description: "Returns: total size occupied by logs.",
                },
                &MetaMethod {
                    name: METH_LOG_USAGE,
                    flags: 0,
                    access: shvrpc::metamethod::AccessLevel::Developer,
                    param: "Null",
                    result: "Decimal",
                    signals: &[],
                    description: "Returns: percentage of space occupied by logs.",
                },
                &MetaMethod {
                    name: METH_SYNC_LOG,
                    flags: 0,
                    access: shvrpc::metamethod::AccessLevel::Write,
                    param: "String|Map",
                    result: "List",
                    signals: &[],
                    description: "syncLog - triggers a manual sync\nAccepts a mandatory string param, only the subtree signified by the string is synced.\nsyncLog also takes a map param in this format: {\n\twaitForFinished: bool // the method waits until the whole operation is finished and only then returns a response\n\tshvPath: string // the subtree to be synced\n}\n\nReturns: a list of all leaf sites that will be synced\n",
                },
                &MetaMethod {
                    name: METH_SYNC_INFO,
                    flags: 0,
                    access: shvrpc::metamethod::AccessLevel::Read,
                    param: "String",
                    result: "Map",
                    signals: &[],
                    description: "syncInfo - returns info about sites' sync status\nOptionally takes a string that filters the sites by prefix.\n\nReturns: a map where they is the path of the site and the value is a map with a status string and a last updated timestamp.\n",
                },
                &MetaMethod {
                    name: METH_SANITIZE_LOG,
                    flags: 0,
                    access: shvrpc::metamethod::AccessLevel::Developer,
                    param: "Null",
                    result: "String",
                    signals: &[],
                    description: "",
                },
                ]));
    }

    assert!(path.starts_with("_shvjournal"));
    if path_contains_parent_dir_references(path) {
        // Reject parent dir references
        return None;
    }
    let path = path.replacen("_shvjournal", &app_state.config.journal_dir, 1);

    // probe the path on the fs
    let path_meta = tokio::fs::metadata(path).await.ok()?;
    if path_meta.is_dir() {
        // TODO: mkfile, mkdir, rmdir
        Some(MetaMethods::from(&[
                &MetaMethod {
                    name: METH_LS_FILES,
                    flags: 0,
                    access: shvrpc::metamethod::AccessLevel::Read,
                    param: "Map|Null",
                    result: "List",
                    signals: &[],
                    description: "",
                },
        ]))
    } else if path_meta.is_file() {
        Some(MetaMethods::from(&[
                &MetaMethod {
                    name: METH_HASH,
                    flags: 0,
                    access: shvrpc::metamethod::AccessLevel::Read,
                    param: "Map|Null",
                    result: "String",
                    signals: &[],
                    description: "",
                },
                &MetaMethod {
                    name: METH_SIZE,
                    flags: 0,
                    access: shvrpc::metamethod::AccessLevel::Browse,
                    param: "",
                    result: "Int",
                    signals: &[],
                    description: "",
                },
                &MetaMethod {
                    name: METH_READ,
                    flags: 0,
                    access: shvrpc::metamethod::AccessLevel::Read,
                    param: "Map",
                    result: "Blob",
                    signals: &[],
                    description: "Parameters\n  offset: file offset to start read, default is 0\n  size: number of bytes to read starting on offset, default is till end of file\n",
                },
                &MetaMethod {
                    name: METH_READ_COMPRESSED,
                    flags: 0,
                    access: shvrpc::metamethod::AccessLevel::Read,
                    param: "Map",
                    result: "Blob",
                    signals: &[],
                    description: "Parameters\n  read() parameters\n  compressionType: gzip (default) | qcompress",
                },
                // &MetaMethod {
                //     name: METH_WRITE,
                //     flags: 0,
                //     access: shvrpc::metamethod::AccessLevel::Write,
                //     param: "String | List",
                //     result: "Bool",
                //     signals: &[],
                //     description: "",
                // },
                // &MetaMethod {
                //     name: METH_DELETE,
                //     flags: 0,
                //     access: shvrpc::metamethod::AccessLevel::Service,
                //     param: "",
                //     result: "Bool",
                //     signals: &[],
                //     description: "",
                // },
                ]))
    } else {
        None
    }
}

async fn root_request_handler(
    rq: RpcMessage,
    _client_cmd_tx: ClientCommandSender,
    app_state: AppState<State>,
) -> RpcRequestResult {

    let method = rq.method().unwrap_or_default();
    const ROOT_PATH: &str = "";
    match method {
        METH_LS => {
            let mut nodes = vec![
                ".app".to_string(),
                "_shvjournal".to_string(),
                "_valuecache".to_string()
            ];
            nodes.append(&mut children_on_path(&app_state.sites_data.read().await.sites_info, ROOT_PATH).unwrap_or_default());
            Ok(nodes.into())
        }
        _ => Ok("Not implemented".into()),
    }
}

async fn valuecache_request_handler(
    rq: RpcMessage,
    _client_cmd_tx: ClientCommandSender,
    _app_state: AppState<State>,
) -> RpcRequestResult {
    let method = rq.method().unwrap_or_default();
    match method {
        METH_LS => Ok(shvproto::List::new().into()),
        METH_GET => Ok("Not implemented".into()),
        METH_GET_CACHE => Ok("Not implemented".into()),
        _ => Err(rpc_error_unknown_method(method)),
    }
}

struct ScopedLog { msg: String }
impl ScopedLog {
    fn new(msg: impl AsRef<str>) -> Self {
        let msg = msg.as_ref();
        log::debug!("init: {msg}");
        Self { msg: msg.into() }
    }
}
impl Drop for ScopedLog {
    fn drop(&mut self) {
        log::debug!("drop: {}", self.msg);
    }
}

pub(crate) enum FileType {
    File,
    Directory,
}

impl std::fmt::Display for FileType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", match self {
            FileType::File => "f",
            FileType::Directory => "d",
        })
    }
}

pub(crate) struct LsFilesEntry {
    pub(crate) name: String,
    pub(crate) ftype: FileType,
    pub(crate) size: i64,
}

impl std::fmt::Display for LsFilesEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{{}, {}, {}}}", self.name, self.ftype, self.size)
    }
}

impl From<LsFilesEntry> for RpcValue {
    fn from(value: LsFilesEntry) -> Self {
        shvproto::make_list!(value.name, value.ftype.to_string(), value.size).into()
    }
}

async fn shvjournal_request_handler(
    rq: RpcMessage,
    app_state: AppState<State>,
) -> RpcRequestResult {

    let method = rq.method().unwrap_or_default();
    let path = rq.shv_path().unwrap_or_default();
    if path == "_shvjournal" {
        match method {
            METH_LS | METH_LS_FILES => { /* handled as a directory */ }
            METH_LOG_SIZE_LIMIT => return Ok("To be implemented".into()),
            METH_TOTAL_LOG_SIZE => return Ok("To be implemented".into()),
            METH_LOG_USAGE => return Ok("To be implemented".into()),
            METH_SYNC_LOG => return Ok("To be implemented".into()),
            METH_SYNC_INFO => return Ok((*app_state.sync_info.sites_sync_info.read().await).to_owned().into()),
            METH_SANITIZE_LOG => return Ok("To be implemented".into()),
            _ => return Err(rpc_error_unknown_method(method)),
        }
    }
    assert!(path.starts_with("_shvjournal"));
    assert!(!path_contains_parent_dir_references(path));
    let path = path.replacen("_shvjournal", &app_state.config.journal_dir, 1);
    let path_meta = tokio::fs::metadata(&path)
        .await
        .map_err(rpc_error_filesystem)?;

    let get_dir_entries = async |path| {
        Ok(ReadDirStream::new(
                tokio::fs::read_dir(path)
                .await
                .map_err(rpc_error_filesystem)?)
        )
    };

    if path_meta.is_dir() {
        match method {
            METH_LS => {
                get_dir_entries(path)
                    .await?
                    .try_filter_map(
                        async |entry| {
                            Ok(entry.file_name().to_str().map(String::from))
                        }
                    )
                    .try_collect::<Vec<_>>()
                    .await
                    .map_err(rpc_error_filesystem)
                    .map(|mut res| { res.sort(); res.into()})
            }
            METH_LS_FILES => {
                get_dir_entries(path)
                    .await?
                    .try_filter_map(
                        async |entry| {
                            let res = async {
                                let meta = entry
                                    .metadata()
                                    .await
                                    .inspect_err(|e| log::error!("Cannot read metadata of file `{}`: {}", entry.path().to_string_lossy(), e))
                                    .ok()?;
                                let name = entry.file_name().to_str().map(String::from)?;
                                let ftype = if meta.is_dir() { FileType::Directory } else if meta.is_file() { FileType::File } else { return None };
                                let size = meta.len() as i64;
                                Some(LsFilesEntry { name, ftype, size })
                            }.await;
                            Ok(res)
                        }
                    )
                    .try_collect::<Vec<_>>()
                    .await
                    .map_err(rpc_error_filesystem)
                    .map(|mut res| {
                        res.sort_by(|a, b| a.name.cmp(&b.name));
                        res.into()
                    })
            }
            _ => Err(rpc_error_unknown_method(method)),
        }
    } else if path_meta.is_file() {
        match method {
            METH_LS => Ok(shvproto::List::new().into()),
            METH_HASH => {
                let _l = ScopedLog::new(format!("hash: {path}"));
                let file = tokio::fs::File::open(&path)
                    .await
                    .map_err(rpc_error_filesystem)?;

                // 64 KiB buffer performs better for large files than the 8 KiB default
                const READER_CAPACITY: usize = 1 << 16;
                let mut file_stream = ReaderStream::with_capacity(file, READER_CAPACITY);
                let mut hasher = sha1::Sha1::new();
                while let Some(res) = file_stream.next().await {
                    let bytes = res.map_err(rpc_error_filesystem)?;
                    hasher.update(&bytes);
                }
                Ok(hex::encode(hasher.finalize()).into())
            }
            METH_SIZE => Ok((path_meta.len() as i64).into()),
            METH_READ => {
                let read_params: ReadParams = rq
                    .param()
                    .unwrap_or_default()
                    .try_into()
                    .map_err(|msg| RpcError::new(RpcErrorCode::InvalidParam, msg))?;
                let offset = read_params.offset.unwrap_or(0);
                let res = read_file(path, offset, read_params.size)
                    .await
                    .map_err(rpc_error_filesystem)?;
                let mut result_meta = shvproto::MetaMap::new();
                result_meta
                    .insert("offset", (offset as i64).into())
                    .insert("size", (res.len() as i64).into());
                Ok(RpcValue::new(res.into(), Some(result_meta)))
            }
            METH_READ_COMPRESSED => {
                let read_params: ReadParams = rq
                    .param()
                    .unwrap_or_default()
                    .try_into()
                    .map_err(|msg| RpcError::new(RpcErrorCode::InvalidParam, msg))?;
                let offset = read_params.offset.unwrap_or(0);
                let (res, bytes_read) = compress_file(path, offset, read_params.size)
                    .await
                    .map_err(rpc_error_filesystem)?;
                let mut result_meta = shvproto::MetaMap::new();
                result_meta
                    .insert("offset", (offset as i64).into())
                    .insert("size", (bytes_read as i64).into());
                Ok(RpcValue::new(res.into(), Some(result_meta)))
            }
            _ => Err(rpc_error_unknown_method(method)),
        }
    } else {
        Err(rpc_error_unknown_method(method))
    }
}

#[derive(Default)]
struct ReadParams {
    offset: Option<u64>,
    size: Option<u64>,
}

impl TryFrom<&RpcValue> for ReadParams {
    type Error = String;

    fn try_from(value: &RpcValue) -> Result<Self, Self::Error> {
        if value.is_null() {
            return Ok(Self::default());
        }

        let map: shvproto::rpcvalue::Map = value.try_into()?;

        let parse_param = |param_name: &str| -> Result<Option<u64>, String> {
            match map.get(param_name) {
                None => Ok(None),
                Some(val) => {
                    i64::try_from(val)
                        .map_err(|e| e.to_string())
                        .and_then(|v| u64::try_from(v)
                            .map_err(|e| e.to_string())
                        )
                        .map_err(|e| format!("Error parsing `{param_name}` parameter: {e}"))
                        .map(Some)
                }
            }
        };

        let offset = parse_param("offset")?;
        let size = parse_param("size")?;

        Ok(Self { offset, size })
    }
}


async fn with_file_reader<F, Fut, R>(
    path: impl AsRef<str>,
    offset: u64,
    size: Option<u64>,
    process: F,
) -> tokio::io::Result<R>
where
    F: FnOnce(Box<dyn AsyncBufRead + Unpin + Send>) -> Fut + Send,
    Fut: Future<Output = tokio::io::Result<R>> + Send,
{
    const MAX_READ_SIZE: u64 = 1 << 20;
    let mut file = tokio::fs::File::open(path.as_ref()).await?;
    file.seek(std::io::SeekFrom::Start(offset)).await?;
    let file_size = file.metadata().await?.len();
    let size = size.unwrap_or(file_size).min(file_size).min(MAX_READ_SIZE);
    let reader = tokio::io::BufReader::new(file).take(size);
    process(Box::new(reader)).await
}

async fn read_file(path: impl AsRef<str>, offset: u64, size: Option<u64>) -> tokio::io::Result<Vec<u8>> {
    with_file_reader(path, offset, size, |mut reader| async move {
        let mut res = Vec::new();
        reader.read_to_end(&mut res).await?;
        Ok(res)
    }).await
}

async fn compress_file(path: impl AsRef<str>, offset: u64, size: Option<u64>) -> tokio::io::Result<(Vec<u8>, u64)> {
    let _l = ScopedLog::new(format!("compress: {}", path.as_ref()));
    with_file_reader(path, offset, size, |mut reader| async move {
        let mut res = Vec::new();
        let mut encoder = GzipEncoder::new(&mut res);
        let bytes_read = tokio::io::copy_buf(&mut reader, &mut encoder).await?;
        encoder.shutdown().await?;
        Ok((res, bytes_read))
    }).await
}

async fn getlog_handler(
    rq: RpcMessage,
    app_state: AppState<State>,
) -> RpcRequestResult {
    let params = GetLog2Params::try_from(rq.param().unwrap_or_default())
        .map_err(|e| RpcError::new(RpcErrorCode::InvalidParam, format!("Wrong getLog parameters: {e}")))?;
    let site_path = rq.shv_path().unwrap_or_default();
    if !app_state.sites_data.read().await.sites_info.contains_key(site_path) {
        return Err(RpcError::new(RpcErrorCode::InvalidParam, format!("Wrong getLog path: {site_path}")));
    }
    let local_journal_path = Path::new(&app_state.config.journal_dir).join(site_path);
    info!("getLog site: {site_path}, params: {params:?}");
    let mut log_files = get_files(&local_journal_path, is_log2_file)
        .await
        .map_err(|err| RpcError::new(RpcErrorCode::InternalError, format!("Cannot read log files: {err}")))?;
    log_files.sort_by_key(|entry| entry.file_name());

    // Skip files with no journal entries
    let log_files = tokio_stream::iter(log_files)
        .filter(|file_entry| {
            let file_path = file_entry.path();
            async move {
                match tokio::fs::File::open(&file_path).await {
                    Ok(file) => {
                        JournalReaderLog2::new(BufReader::new(file.compat()))
                            .next()
                            .await
                            .is_some()
                    }
                    Err(err) => {
                        error!("Cannot open file {file_path} in call to getLog: {err}",
                            file_path = file_path.to_string_lossy()
                        );
                        false
                    }
                }
            }
        })
        .collect::<Vec<_>>()
        .await;

    let file_start_index = {
        if log_files.is_empty() {
            0
        } else {
            match &params.since {
                GetLog2Since::DateTime(date_time) => {
                    let since_ms = date_time.epoch_msec();
                    let file_name_to_msec = |file_name: &str| file_name
                        .strip_suffix(".log2")
                        .and_then(|file| shvproto::DateTime::from_iso_str(file).ok().as_ref().map(shvproto::DateTime::epoch_msec))
                        .unwrap_or(i64::MAX);

                    log_files
                        .iter()
                        .map(DirEntry::file_name)
                        .enumerate()
                        .rev()
                        .find(|(_,file)| file_name_to_msec(&file.to_string_lossy()) < since_ms)
                        .map(|(idx, _)| idx)
                        .unwrap_or(0)
                }
                GetLog2Since::LastEntry => log_files.len() - 1,
                GetLog2Since::None => 0,
            }
        }
    };

    let file_readers = tokio_stream::iter(&log_files[file_start_index..])
        .then(|file_entry| async {
            let file_path = file_entry.path();
            tokio::fs::File::open(&file_path)
                .await
                .map_err(|err| format!("Cannot open file {file_path} in call to getLog: {err}", file_path = file_path.to_string_lossy()))
                .map(|file| JournalReaderLog2::new(BufReader::new(file.compat())))
        })
        .try_collect::<Vec<_>>()
        .await
        .map_err(|err| RpcError::new(RpcErrorCode::InternalError, err))?;


    let (dirtylog_tx, dirtylog_rx) = futures::channel::oneshot::channel();
    app_state.dirtylog_cmd_tx
        .unbounded_send(DirtyLogCommand::Get {
            site: site_path.into(),
            response_tx: dirtylog_tx,
        })
        .map_err(|err| RpcError::new(RpcErrorCode::InternalError, format!("Cannot get dirtylog: {err}")))?;
    let dirtylog = dirtylog_rx
        .await
        .map_err(|err| RpcError::new(RpcErrorCode::InternalError, format!("Cannot get dirtylog: {err}")))?;

    Ok(getlog_impl(file_readers.into_iter().map(|s| Box::pin(s) as _), dirtylog, &params).await)
}

type JournalEntryStream = Pin<Box<dyn Stream<Item = Result<JournalEntry, Box<dyn Error + Send + Sync>>> + Send + Sync>>;

async fn getlog_impl(
    journal_readers: impl IntoIterator<Item = JournalEntryStream>,
    dirty_log: impl IntoIterator<Item = JournalEntry>,
    params: &GetLog2Params
) -> RpcValue
{
    const RECORD_COUNT_LIMIT_MAX: i64 = 100_000;

    enum Since {
        Msec(i64),
        Last,
    }

    fn is_snapshot_entry(entry: &JournalEntry, since: &Since) -> bool {
        match since {
            Since::Msec(since_msec) => entry.epoch_msec <= *since_msec,
            Since::Last => true,
        }
    }

    struct Context<'a> {
        params: &'a GetLog2Params,
        since: Option<Since>,
        snapshot: BTreeMap<String, Arc<JournalEntry>>,
        entries: Vec<Arc<JournalEntry>>,
        record_count: usize,
        record_count_limit: usize,
        record_count_limit_hit: bool,
        last_entry: Option<Arc<JournalEntry>>,
        first_unmatched_entry_msec: Option<i64>,
        until_ms: i64,
    }

    fn process_journal_entry(
        entry: JournalEntry,
        context: &mut Context,
    ) -> bool {
        if context
            .params
            .path_pattern
                .as_ref()
                .is_some_and(|p| !matches_path_pattern(&entry.path, p))
        {
            return true;
        }

        let entry = Arc::new(entry);

        let since_val = context.since.get_or_insert_with(|| determine_since(&entry, &context.params.since));
        let is_snapshot_entry = is_snapshot_entry(&entry, since_val);

        if is_snapshot_entry {
            context.snapshot.insert(entry.path.clone(), Arc::clone(&entry));
        } else if entry.epoch_msec >= context.until_ms {
            context.first_unmatched_entry_msec = Some(entry.epoch_msec);
            return false;
        }

        if !is_snapshot_entry {
            let total = context.snapshot.len() + context.record_count + 1;
            if total > context.record_count_limit {
                context.record_count_limit_hit = true;

                // Record all consecutive entries with the same timestamp
                if context.last_entry.as_ref().is_some_and(|last_entry| last_entry.epoch_msec != entry.epoch_msec) {
                    context.first_unmatched_entry_msec = Some(entry.epoch_msec);
                    return false;
                }
            }

            context.entries.push(Arc::clone(&entry));
            context.record_count += 1;
        }

        context.last_entry = Some(entry);
        true
    }


    fn determine_since(entry: &JournalEntry, params_since: &GetLog2Since) -> Since {
        match params_since {
            GetLog2Since::DateTime(dt) => Since::Msec(dt.epoch_msec().max(entry.epoch_msec)),
            GetLog2Since::LastEntry => Since::Last,
            GetLog2Since::None => Since::Msec(entry.epoch_msec),
        }
    }

    let mut context = Context {
        params,
        since: None,
        snapshot: BTreeMap::new(),
        entries: Vec::new(),
        record_count: 0,
        record_count_limit: params.record_count_limit.clamp(0, RECORD_COUNT_LIMIT_MAX) as _,
        record_count_limit_hit: false,
        last_entry: None,
        first_unmatched_entry_msec: None,
        until_ms: params.until.as_ref().map_or(i64::MAX, shvproto::DateTime::epoch_msec),
    };

    for mut reader in journal_readers {
        while let Some(entry_res) = reader.next().await {
            match entry_res {
                Ok(entry) => {
                    if !process_journal_entry(entry, &mut context) {
                        break;
                    }
                }
                Err(err) => error!("Skipping corrupted journal entry: {err}"),
            }
        }
    }

    for entry in dirty_log {
        // Filter out entries that overlap with the entries from the synced files
        if context.last_entry.as_ref().is_some_and(|last_entry| entry.epoch_msec < last_entry.epoch_msec) {
            continue;
        }

        if !process_journal_entry(entry, &mut context) {
            break;
        }
    }

    let since_ms = match context.since {
        Some(Since::Msec(msec)) => msec,
        Some(Since::Last) => context.last_entry.map_or(0, |entry| entry.epoch_msec),
        None => 0, // No entries
    };

    let with_snapshot = if matches!(params.since, GetLog2Since::None) && params.with_snapshot {
        warn!("Requested snapshot without a valid `since`. No snapshot will be provided.");
        // FIXME: we can provide a snapshot at the first entry datetime
        false
    } else {
        params.with_snapshot
    };

    let snapshot_entries = if with_snapshot {
        context.snapshot.into_values().map(|entry| {
            JournalEntry {
                epoch_msec: since_ms,
                repeat: true,
                ..(*entry).clone()
            }
        })
        .collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    let current_datetime = shvproto::DateTime::now();
    let until_ms = match context.first_unmatched_entry_msec {
        Some(msec) => msec, // set `until` to the first entry that is not included in the log
        None =>
            if let Some(last_entry_msec) = context.entries.last().map(|entry| entry.epoch_msec) {
                // There are other entries than just the snapshot
                if matches!(params.since, GetLog2Since::LastEntry) {
                    // The last entry is explicitly requested, return all we have.
                    // Set `until` to the timestamp of the last entry to indicate that
                    // there might be more data with this timestamp on the next call.
                    last_entry_msec
                } else if last_entry_msec.abs_diff(current_datetime.epoch_msec()) < 1000 {
                    // If the latest entries with the same timestmao are very recent,
                    // remove them same timestamp from the end of the log
                    while context.entries.last().is_some_and(|entry| entry.epoch_msec == last_entry_msec) {
                        context.entries.pop();
                    }
                    // result_entries.retain(|e| e.epoch_msec != last_entry_msec);
                    last_entry_msec
                } else {
                    // Keep the last entries, set `until` just after the last entry timestamp
                    last_entry_msec + 1
                }
            } else {
                // Empty result or only the snapshot, set `until` equal to `since`
                since_ms
            }
    };

    let (mut result, paths_dict) = journal_entries_to_rpcvalue(
        snapshot_entries.iter().chain(context.entries.iter().map(|arc_entry| arc_entry.as_ref())),
        params.with_paths_dict
    );

    result.meta = Some(Box::new(Log2Header {
        record_count: context.record_count as _,
        record_count_limit: context.record_count_limit as _,
        record_count_limit_hit: context.record_count_limit_hit,
        date_time: current_datetime,
        since: shvproto::DateTime::from_epoch_msec(since_ms),
        until: shvproto::DateTime::from_epoch_msec(until_ms),
        with_paths_dict: params.with_paths_dict,
        with_snapshot,
        paths_dict,
        log_params: params.clone(),
        log_version: 2,
    }
    .into()));

    result
}

async fn history_request_handler(
    rq: RpcMessage,
    _client_cmd_tx: ClientCommandSender,
    app_state: AppState<State>,
) -> RpcRequestResult {
    let method = rq.method().unwrap_or_default();
    let path = rq.shv_path().unwrap_or_default();
    let children = children_on_path(&app_state.sites_data.read().await.sites_info, path)
        .unwrap_or_else(|| panic!("Children on path `{path}` should be Some after methods processing"));

    match method {
        METH_LS => Ok(children.into()),
        METH_GET_LOG if children.is_empty() => getlog_handler(rq, app_state).await,
        _ => Err(rpc_error_unknown_method(method)),
    }
}

pub(crate) async fn methods_getter(
    path: String,
    app_state: Option<AppState<State>>
) -> Option<MetaMethods> {
    let app_state = app_state.expect("AppState is Some");
    match NodeType::from_path(&path) {
        NodeType::DotApp => Some(MetaMethods::from(shvclient::appnodes::DOT_APP_METHODS)),
        NodeType::ShvJournal => shvjournal_methods_getter(path, app_state).await,
        NodeType::ValueCache =>
            Some(MetaMethods::from(&[
                    &MetaMethod {
                        name: METH_GET,
                        flags: 0,
                        access: shvrpc::metamethod::AccessLevel::Developer,
                        param: "String",
                        result: "RpcValue",
                        signals: &[],
                        description: "",
                    },
                    &MetaMethod {
                        name: METH_GET_CACHE,
                        flags: 0,
                        access: shvrpc::metamethod::AccessLevel::Developer,
                        param: "Null",
                        result: "Map",
                        signals: &[],
                        description: "",
                    },
            ])),
        NodeType::Root =>
            Some(MetaMethods::from(&[
                    &MetaMethod {
                        name: METH_UPTIME,
                        flags: 0,
                        access: shvrpc::metamethod::AccessLevel::Read,
                        param: "Null",
                        result: "String",
                        signals: &[],
                        description: "",
                    },
                    &MetaMethod {
                        name: METH_RELOAD_SITES,
                        flags: 0,
                        access: shvrpc::metamethod::AccessLevel::Write,
                        param: "Null",
                        result: "Bool",
                        signals: &[],
                        description: "",
                    },
                    // TODO: All root node methods:
                    //
                    // appName
                    // deviceId
                    // deviceType
                    // version
                    // gitCommit
                    // shvVersion
                    // shvGitCommit
                    // uptime
                    // reloadSites (wr) -> Bool
        ])),
        NodeType::History => {
            let children = children_on_path(&app_state.sites_data.read().await.sites_info, path)?;
            if children.is_empty() {
                // `path` is a site path
                Some(MetaMethods::from(&[&META_METHOD_GET_LOG]))
            } else {
                // `path` is a dir in the middle of the tree
                Some(MetaMethods::from(&[]))
            }
        }
    }
    // TODO: put the processed data to a request cache in the app state: path -> children
}

pub(crate) async fn request_handler(
    rq: RpcMessage,
    client_cmd_tx: ClientCommandSender,
    app_state: Option<AppState<State>>,
) {
    let mut resp = rq.prepare_response().unwrap();
    resp.set_result_or_error(request_handler_impl(rq, client_cmd_tx.clone(), app_state).await);
    client_cmd_tx.send_message(resp).unwrap();
}

async fn request_handler_impl(
    rq: RpcMessage,
    client_cmd_tx: ClientCommandSender,
    app_state: Option<AppState<State>>,
) -> RpcRequestResult {
    let app_state = app_state.expect("AppState is Some");
    let path = rq.shv_path().unwrap_or_default();
    let method = rq.method().unwrap_or_default();

    match NodeType::from_path(path) {
        NodeType::Root =>
            root_request_handler(rq, client_cmd_tx, app_state).await,
        NodeType::DotApp => {
            if method == shvclient::clientnode::METH_LS {
                Ok(shvproto::List::new().into())
            } else {
                DOT_APP_NODE
                    .process_request(&rq)
                    .unwrap_or_else(|| Err(rpc_error_unknown_method(method)))
            }
        }
        NodeType::ValueCache =>
            valuecache_request_handler(rq, client_cmd_tx, app_state).await,
        NodeType::ShvJournal =>
            shvjournal_request_handler(rq, app_state).await,
        NodeType::History =>
            history_request_handler(rq, client_cmd_tx, app_state).await,
    }
}

