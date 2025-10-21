use std::collections::BTreeMap;
use std::error::Error;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

use async_compression::tokio::write::GzipEncoder;
use futures::io::{BufReader, BufWriter};
use futures::{Stream, StreamExt, TryStreamExt};
use log::{error, info, warn};
use sha1::Digest;
use shvclient::client::MetaMethods;
use shvclient::clientnode::{children_on_path, ConstantNode, METH_LS};
use shvclient::AppState;
use shvproto::{FromRpcValue, RpcValue, ToRpcValue};
use shvrpc::metamethod::MetaMethod;
use shvrpc::rpcmessage::{RpcError, RpcErrorCode};
use shvrpc::{RpcMessage, RpcMessageMetaTags};
use tokio::fs::DirEntry;
use tokio::io::{AsyncBufRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio_stream::wrappers::ReadDirStream;
use tokio_util::compat::TokioAsyncReadCompatExt;
use tokio_util::io::ReaderStream;

use crate::cleanup::collect_log2_files;
use crate::dirtylog::DirtyLogCommand;
use crate::journalentry::JournalEntry;
use crate::journalrw::{journal_entries_to_rpcvalue, matches_path_pattern, GetLog2Params, GetLog2Since, JournalReaderLog2, JournalWriterLog2, Log2Header, Log2Reader};
use crate::sites::{update_alarms, SubHpInfo};
use crate::util::{get_files, is_log2_file};
use crate::{AlarmWithTimestamp, ClientCommandSender, HpConfig, State, MAX_JOURNAL_DIR_SIZE_DEFAULT};

// History site node methods
const METH_GET_LOG: &str = "getLog";
const METH_ALARM_TABLE: &str = "alarmTable";
const METH_ALARM_LOG: &str = "alarmLog";
const METH_PUSH_LOG: &str = "pushLog";

const META_METHOD_GET_LOG: MetaMethod = MetaMethod {
    name: METH_GET_LOG,
    flags: 0,
    access: shvrpc::metamethod::AccessLevel::Read,
    param: "RpcValue",
    result: "RpcValue",
    signals: &[],
    description: "",
};

const META_METHOD_ALARM_TABLE: MetaMethod = MetaMethod {
    name: METH_ALARM_TABLE,
    flags: 0,
    access: shvrpc::metamethod::AccessLevel::Read,
    param: "RpcValue",
    result: "RpcValue",
    signals: &[("alarmmod", Some("Null"))],
    description: "",
};

const META_METHOD_ALARM_LOG: MetaMethod = MetaMethod {
    name: METH_ALARM_LOG,
    flags: 0,
    access: shvrpc::metamethod::AccessLevel::Read,
    param: "RpcValue",
    result: "RpcValue",
    signals: &[],
    description: "",
};

const META_METHOD_PUSH_LOG: MetaMethod = MetaMethod {
    name: METH_PUSH_LOG,
    flags: 0,
    access: shvrpc::metamethod::AccessLevel::Write,
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

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
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

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
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

fn log_size_limit(config: &HpConfig) -> i64 {
    config.max_journal_dir_size.unwrap_or(MAX_JOURNAL_DIR_SIZE_DEFAULT) as i64
}

async fn total_log_size(config: &HpConfig) -> tokio::io::Result<i64> {
    collect_log2_files(&config.journal_dir)
        .await
        .map(|files| files.into_iter().map(|f| f.size as i64).sum::<i64>())
}

async fn sync_log_request_handler(rq: RpcMessage, app_state: AppState<State>) -> RpcRequestResult {
    let shvproto::Value::String(shv_path) = &rq.param().unwrap_or_default().value else {
        return Err(RpcError::new(RpcErrorCode::InvalidParam, "Expected a string parameter (SHV path)"));
    };

    let (sites_info, sub_hps) = {
        let sites_data = app_state.sites_data.read().await;
        (sites_data.sites_info.clone(), sites_data.sub_hps.clone())
    };

    let sites_to_sync = sites_info
        .iter()
        .filter(|(site, site_info)|
            site.starts_with(shv_path.as_str()) &&
            sub_hps.get(&site_info.sub_hp).is_some_and(|sub_hp| !matches!(sub_hp, SubHpInfo::PushLog))
        )
        .map(|(site, _)| site)
        .collect::<Vec<_>>();

    if shv_path.is_empty() {
        app_state.sync_cmd_tx.send(crate::sync::SyncCommand::SyncAll)
            .map(|_|())
            .map_err(|_| RpcError::new(RpcErrorCode::InternalError, "Cannot send SyncAll command through the channel"))?;
    } else {
        sites_to_sync.iter().try_for_each(|site| app_state.sync_cmd_tx
            .send(crate::sync::SyncCommand::SyncSite(site.to_string()))
            .map(|_|())
            .map_err(|_| RpcError::new(RpcErrorCode::InternalError, format!("Cannot send Sync({site}) command through the channel")))
        )?;
    }

    Ok(sites_to_sync.into())
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
            METH_LOG_SIZE_LIMIT => return Ok(log_size_limit(&app_state.config).into()),
            METH_TOTAL_LOG_SIZE => return total_log_size(&app_state.config)
                .await
                .map(RpcValue::from)
                .map_err(rpc_error_filesystem),
            METH_LOG_USAGE => return total_log_size(&app_state.config)
                .await
                .map(|size| (100. * (size as f64) / (log_size_limit(&app_state.config) as f64)).into())
                .map_err(rpc_error_filesystem),
            METH_SYNC_LOG => return sync_log_request_handler(rq, app_state).await,
            METH_SYNC_INFO => return Ok((*app_state.sync_info.sites_sync_info.read().await).to_owned().into()),
            METH_SANITIZE_LOG => return app_state.sync_cmd_tx
                .send(crate::sync::SyncCommand::Cleanup)
                .map(|_| true.into())
                .map_err(|_| RpcError::new(RpcErrorCode::InternalError, "Cannot send the command through the channel")),
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
    if path_contains_parent_dir_references(&path) {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Path cannot contain parent dir references"));
    }
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

async fn pushlog_handler(
    rq: RpcMessage,
    app_state: AppState<State>,
) -> RpcRequestResult {
    let log_reader = Log2Reader::new(rq.param().map_or_else(RpcValue::null, RpcValue::clone))
        .map_err(|e| RpcError::new(RpcErrorCode::InvalidParam, format!("Cannot parse pushLog parameter: {e}")))?;
    let Log2Header {since, until, ..} = log_reader.header;
    let site_path = rq.shv_path().unwrap_or_default();
    info!("pushLog handler, site: {site_path}, log header: {header}", header = log_reader.header);
    if !app_state.sites_data.read().await.sites_info.contains_key(site_path) {
        return Err(RpcError::new(RpcErrorCode::InvalidParam, format!("Wrong site path for pushLog: {site_path}")));
    }

    let local_journal_path = Path::new(&app_state.config.journal_dir).join(site_path);

    let local_latest_entries = {
        let log_files = get_files(&local_journal_path, is_log2_file)
            .await
            .map(|mut files| { files.sort_by_key(|entry| std::cmp::Reverse(entry.file_name())); files })
            .inspect_err(|err| {error!("Cannot read journal dir entries: {err}"); })
            .unwrap_or_default();

        Box::pin(futures::stream::iter(log_files)
            .map(|file_entry| file_entry.path())
            .then(|file_path|
                async move {
                    match tokio::fs::File::open(&file_path).await {
                        Ok(file) => {
                            let reader = JournalReaderLog2::new(BufReader::new(file.compat()));
                            let (_, latest_entries) = reader
                                .filter_map(async |res| res.ok())
                                .fold((None, Vec::new()), async |(latest_ts, mut latest_entries), entry| {
                                    match latest_ts {
                                        None => (Some(entry.epoch_msec), vec![entry]),
                                        Some(ts) if entry.epoch_msec > ts =>
                                            (Some(entry.epoch_msec), vec![entry]),
                                        Some(ts) if entry.epoch_msec == ts => {
                                            latest_entries.push(entry);
                                            (Some(ts), latest_entries)
                                        }
                                        Some(ts) => (Some(ts), latest_entries),
                                    }
                                })
                                .await;
                            (!latest_entries.is_empty()).then_some(latest_entries)
                        }
                        Err(err) => {
                            error!("Cannot open file {file_path} while getting the last journal entries in pushLog handler: {err}",
                                file_path = file_path.to_string_lossy()
                            );
                            None
                        }
                    }
                })
            .filter_map(async |latest_entries| latest_entries))
            .next()
            .await
            .unwrap_or_default()
    };
    let local_latest_entry_msec = local_latest_entries.first().map_or(0, |entry| entry.epoch_msec);

    let journal_file_path = local_journal_path.join(since.to_iso_string() + ".log2");
    let journal_file = match tokio::fs::OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(&journal_file_path)
        .await {
            Ok(file) => file,
            Err(err) => {
                error!("Cannot open journal file {} for writing: {}", journal_file_path.to_string_lossy(), err);
                // Return Ok to the device assuming that the devices are not smart enough to handle
                // the error in this case. It is not their fault that we cannot create the file.
                // FIXME: if there is a better approach
                return Ok(shvproto::make_map!(
                        "since" => since,
                        "until" => until,
                        "msg" => "success",
                ).into())
            }
        };

    let mut remote_entries_count = 0;
    let mut log_reader_stream = futures::stream::iter(log_reader
        .into_iter()
        .filter_map(|entry_res| {
            let entry = entry_res
                .inspect_err(|err| warn!("Ignoring invalid pushLog entry: {err}"))
                .ok()?;

            remote_entries_count += 1;

            if entry.epoch_msec < local_latest_entry_msec {
                warn!("Ignoring pushLog entry for: {entry_path} with timestamp: {entry_msec}, because a newer one exists with ts: {local_latest_entry_msec}",
                    entry_path = entry.path,
                    entry_msec = entry.epoch_msec,
                );
                return None;
            }
            if entry.epoch_msec == local_latest_entry_msec && local_latest_entries.iter().any(|latest_entry| latest_entry.path == entry.path) {
                warn!("Ignoring pushLog entry for: {entry_path} with timestamp: {entry_msec}, because we already have an entry with this timestamp and path",
                    entry_path = entry.path,
                    entry_msec = entry.epoch_msec,
                );
                return None;
            }
            Some(entry)
        }));

    let mut writer = JournalWriterLog2::new(BufWriter::new(journal_file.compat()));
    let mut written_entries_count = 0;
    while let Some(entry) = log_reader_stream.next().await {
        if let Err(err) = writer.append(&entry).await {
            warn!("Cannot append to journal file {path}: {err}", path = journal_file_path.to_string_lossy());
            break;
        }
        written_entries_count += 1;
    }

    if written_entries_count == 0 {
        tokio::fs::remove_file(&journal_file_path)
            .await
            .unwrap_or_else(|err|
                error!("Cannot remove empty journal file {path}: {err}", path = journal_file_path.to_string_lossy())
            );
    }

    let (since, until) = if remote_entries_count == 0 {
        (RpcValue::null(), RpcValue::null())
    } else {
        (since.into(), until.into())
    };

    info!("pushLog for site: {site_path} done, got {remote_entries_count}, rejected {rejected_count} entries",
        rejected_count = remote_entries_count - written_entries_count
    );

    Ok(shvproto::make_map!(
            "since" => since,
            "until" => until,
            "msg" => "success",
    ).into())
}

pub async fn getlog_handler(
    site_path: &str,
    params: &GetLog2Params,
    app_state: AppState<State>,
) -> Result<GetLogResult, RpcError> {
    if !app_state.sites_data.read().await.sites_info.contains_key(site_path) {
        return Err(RpcError::new(RpcErrorCode::InvalidParam, format!("Wrong getLog path: {site_path}")));
    }
    let local_journal_path = Path::new(&app_state.config.journal_dir).join(site_path);
    info!("getLog handler, site: {site_path}, params: {params}");
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

    Ok(getlog_impl(file_readers.into_iter().map(|s| Box::pin(s) as _), dirtylog, params).await)
}

async fn getlog_handler_rq(
    rq: RpcMessage,
    app_state: AppState<State>,
) -> RpcRequestResult {
    let site_path = rq.shv_path().unwrap_or_default();
    let params = GetLog2Params::try_from(rq.param().unwrap_or_default())
        .map_err(|e| RpcError::new(RpcErrorCode::InvalidParam, format!("Wrong getLog parameters: {e}")))?;
    let getlog_result = getlog_handler(site_path, &params, app_state).await?;
    let chained_entries = getlog_result.snapshot_entries.iter().map(Arc::as_ref).chain(getlog_result.event_entries.iter().map(Arc::as_ref));
    let (mut result, paths_dict) = journal_entries_to_rpcvalue(
        chained_entries,
        params.with_paths_dict
    );

    result.meta = Some(Box::new(Log2Header {
        record_count: getlog_result.record_count,
        record_count_limit: getlog_result.record_count_limit,
        record_count_limit_hit: getlog_result.record_count_limit_hit,
        date_time: getlog_result.date_time,
        since: getlog_result.since,
        until: getlog_result.until,
        with_paths_dict: getlog_result.with_paths_dict,
        with_snapshot: getlog_result.with_snapshot,
        paths_dict,
        log_params: params,
        log_version: 2,
    }.into()));

    Ok(result)
}

async fn alarmtable_handler(
    rq: RpcMessage,
    app_state: AppState<State>,
) -> RpcRequestResult {
    let site_path = rq.shv_path().unwrap_or_default();
    if !app_state.sites_data.read().await.sites_info.contains_key(site_path) {
        return Err(RpcError::new(RpcErrorCode::InvalidParam, format!("Wrong alarmTable path: {site_path}")));
    }
    match app_state.alarms.read().await.get(site_path) {
        Some(alarms_for_site) => Ok(alarms_for_site.clone().into()),
        None => Ok(Vec::<RpcValue>::new().into()),
    }
}

#[derive(FromRpcValue)]
struct AlarmLogParams {
    since: shvproto::DateTime,
    until: shvproto::DateTime,
}

async fn alarmlog_handler(
    rq: RpcMessage,
    app_state: AppState<State>,
) -> RpcRequestResult {
    let params = AlarmLogParams::try_from(rq.param().unwrap_or_default())
        .map_err(|err| RpcError::new(RpcErrorCode::InvalidParam, format!("Wrong alarmLog parameters: {err}")))?;

    let site_path = rq.shv_path().unwrap_or_default();

    let getlog_params = GetLog2Params {
        since: GetLog2Since::DateTime(params.since),
        until: Some(params.until),
        with_snapshot: true,
        ..Default::default()
    };

    #[derive(ToRpcValue)]
    struct AlarmLog {
        snapshot: Vec<AlarmWithTimestamp>,
        events: Vec<AlarmWithTimestamp>,
    }

    let typeinfos = &app_state.sites_data.read().await.typeinfos;
    let Some(Ok(type_info)) = typeinfos.get(site_path) else {
        return Ok(BTreeMap::from([(site_path.to_string(), RpcValue::from(shvproto::List::new()))]).into());
    };

    let mut tmp_alarms = Vec::<AlarmWithTimestamp>::new();

    let log = getlog_handler(site_path, &getlog_params, app_state.clone()).await?;
    for entry in log.snapshot_entries {
        update_alarms(&mut tmp_alarms, type_info, &entry.path, &entry.value, shvproto::DateTime::from_epoch_msec(entry.epoch_msec));
    }

    let snapshot = tmp_alarms.clone();

    let events = log.event_entries
        .into_iter()
        .flat_map(|entry| update_alarms(&mut tmp_alarms, type_info, &entry.path, &entry.value, shvproto::DateTime::from_epoch_msec(entry.epoch_msec)))
        .collect::<Vec<_>>();

    Ok(AlarmLog {
        events,
        snapshot,
    }.into())
}

type JournalEntryStream = Pin<Box<dyn Stream<Item = Result<JournalEntry, Box<dyn Error + Send + Sync>>> + Send + Sync>>;

pub(crate) struct GetLogResult {
    record_count: i64,
    record_count_limit: i64,
    record_count_limit_hit: bool,
    date_time: shvproto::DateTime,
    since: shvproto::DateTime,
    until: shvproto::DateTime,
    with_paths_dict: bool,
    with_snapshot: bool,
    pub(crate) snapshot_entries: Vec<Arc<JournalEntry>>,
    pub(crate) event_entries: Vec<Arc<JournalEntry>>,
}

async fn getlog_impl(
    journal_readers: impl IntoIterator<Item = JournalEntryStream>,
    dirty_log: impl IntoIterator<Item = JournalEntry>,
    params: &GetLog2Params
) -> GetLogResult
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

    'outer: for mut reader in journal_readers {
        while let Some(entry_res) = reader.next().await {
            match entry_res {
                Ok(entry) => {
                    if !process_journal_entry(entry, &mut context) {
                        break 'outer;
                    }
                }
                Err(err) => error!("Skipping corrupted journal entry: {err}"),
            }
        }
    }

    // Only append entries from the dirtylog if they are adjacent to the journal entries or else
    // the resulting `until` will be incorrectly taken from the first dirtylog entry.
    if !context.record_count_limit_hit {
        for entry in dirty_log {
            // Filter out entries that overlap with the entries from the synced files
            if context.last_entry.as_ref().is_some_and(|last_entry| entry.epoch_msec < last_entry.epoch_msec) {
                continue;
            }

            if !process_journal_entry(entry, &mut context) {
                break;
            }
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
        context.snapshot
            .into_values()
            .map(|entry| Arc::new(JournalEntry {
                epoch_msec: since_ms,
                repeat: true,
                ..(*entry).clone()
            }))
            .collect::<Vec<_>>()
    } else {
        // Include all entries from the snapshot if their timestamp >= since
        context.snapshot
            .into_values()
            .filter(|entry| entry.epoch_msec >= since_ms)
            .collect::<Vec<_>>()
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

    let record_count = snapshot_entries.len() as i64 + context.entries.len() as i64;

    GetLogResult {
        record_count,
        snapshot_entries,
        event_entries: context.entries.into_iter().collect(),
        record_count_limit: context.record_count_limit as _,
        record_count_limit_hit: context.record_count_limit_hit,
        date_time: current_datetime,
        since: shvproto::DateTime::from_epoch_msec(since_ms),
        until: shvproto::DateTime::from_epoch_msec(until_ms),
        with_paths_dict: params.with_paths_dict,
        with_snapshot,
    }
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
        METH_GET_LOG => getlog_handler_rq(rq, app_state).await,
        METH_ALARM_TABLE => alarmtable_handler(rq, app_state).await,
        METH_ALARM_LOG => alarmlog_handler(rq, app_state).await,
        METH_PUSH_LOG => pushlog_handler(rq, app_state).await,
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
            let sites_data = app_state.sites_data.read().await;
            let children = children_on_path(&sites_data.sites_info, &path)?;
            if children.is_empty() {
                // `path` is a site path
                let meta_methods = if sites_data.sites_info
                    .get(&path)
                    .and_then(|site_info| sites_data.sub_hps.get(&site_info.sub_hp))
                    .is_some_and(|sub_hp_info| matches!(sub_hp_info, SubHpInfo::PushLog)) {
                        MetaMethods::from(&[&META_METHOD_GET_LOG, &META_METHOD_PUSH_LOG])
                    } else {
                        MetaMethods::from(&[&META_METHOD_GET_LOG, &META_METHOD_ALARM_TABLE, &META_METHOD_ALARM_LOG])
                    };
                Some(meta_methods)
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

#[cfg(test)]
mod tests {
    use std::error::Error;

    use shvproto::{DateTime, RpcValue};

    use crate::{journalrw::{GetLog2Params, GetLog2Since}, tree::{GetLogResult, JournalEntryStream}};

    use super::{getlog_impl, JournalEntry};

    fn ts(ts_str: &str) -> shvproto::DateTime {
        DateTime::from_iso_str(ts_str).unwrap()
    }

    fn since(ts_str: &str)-> crate::journalrw::GetLog2Since {
        crate::journalrw::GetLog2Since::DateTime(ts(ts_str))
    }

    fn make_entry(timestamp: &str, path: &str, value: impl Into<RpcValue>) -> Result<JournalEntry, Box<dyn Error + Send + Sync>> {
        Ok(JournalEntry {
            path: path.to_string(),
            epoch_msec: DateTime::from_iso_str(timestamp).unwrap().epoch_msec(),
            signal: "chng".to_string(),
            short_time: -1,
            access_level: 32,
            source: "".to_string(),
            value: value.into(),
            user_id: None,
            repeat: false,
            provisional: false,
        })
    }

    fn create_reader(entries: Vec<Result<JournalEntry, Box<dyn Error + Send + Sync + 'static>>>) -> crate::tree::JournalEntryStream {
        Box::pin(tokio_stream::iter(entries))
    }

    async fn get_log_entries(readers: Vec<JournalEntryStream>, params: GetLog2Params) -> GetLogResult {
        getlog_impl(readers, [], &params).await
    }

    #[tokio::test]
    async fn getlog() {
        fn data_1() -> Vec<JournalEntryStream> {
            vec![
                create_reader(vec![
                    make_entry("2022-07-07T18:06:15.557Z", "APP_START", true),
                    make_entry("2022-07-07T18:06:15.557Z", "zone1/system/sig/plcDisconnected", false),
                    make_entry("2022-07-07T18:06:15.557Z", "zone1/zone/Zone1/plcDisconnected", false),
                    make_entry("2022-07-07T18:06:15.557Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32),

                    make_entry("2022-07-07T18:06:17.784Z", "zone1/system/sig/plcDisconnected", false),
                    make_entry("2022-07-07T18:06:17.784Z", "zone1/zone/Zone1/plcDisconnected", false),
                    make_entry("2022-07-07T18:06:17.869Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32),
                ]),
                create_reader(vec![
                    make_entry("2022-07-07T18:06:17.872Z", "APP_START", true),
                    make_entry("2022-07-07T18:06:17.872Z", "zone1/system/sig/plcDisconnected", false),
                    make_entry("2022-07-07T18:06:17.872Z", "zone1/zone/Zone1/plcDisconnected", false),
                    make_entry("2022-07-07T18:06:17.872Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32),

                    make_entry("2022-07-07T18:06:17.873Z", "zone1/system/sig/plcDisconnected", false),
                    make_entry("2022-07-07T18:06:17.874Z", "zone1/zone/Zone1/plcDisconnected", false),
                    make_entry("2022-07-07T18:06:17.880Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32),
                ])
            ]
        }

        fn data_2() -> Vec<JournalEntryStream> {
            vec![
                create_reader(vec![
                    make_entry("2022-07-07T18:06:17.784Z", "value1", 0),
                    make_entry("2022-07-07T18:06:17.784Z", "value2", 1),
                    make_entry("2022-07-07T18:06:17.784Z", "value3", 3),
                    make_entry("2022-07-07T18:06:17.800Z", "value3", 200),
                    make_entry("2022-07-07T18:06:17.950Z", "value2", 10),
                ]),
            ]
        }

        fn data_3() -> Vec<JournalEntryStream> {
            vec![]
        }

        fn data_4() -> Vec<JournalEntryStream> {
            vec![
                create_reader(vec![
                    make_entry("2022-07-07T18:06:14.000Z", "value1", 10),
                    make_entry("2022-07-07T18:06:15.557Z", "value2", 20),
                    make_entry("2022-07-07T18:06:16.600Z", "value3", 30),
                    make_entry("2022-07-07T18:06:17.784Z", "value4", 40),
                ])
            ]
        }

        #[derive(Default)]
        struct TestCase {
            name: &'static str,
            params: GetLog2Params,
            expected: Vec<(&'static str, &'static str, RpcValue)>,
            expected_record_count_limit_hit: Option<bool>,
            expected_since: Option<&'static str>,
            expected_until: Option<&'static str>,
        }

        let test_cases = [
            TestCase {
                name: "default params (no snapshot)",
                params: Default::default(),
                expected: vec![
                    ("2022-07-07T18:06:15.557Z", "APP_START", true.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/zone/Zone1/plcDisconnected", false.into()),

                    ("2022-07-07T18:06:17.784Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.784Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.869Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),

                    ("2022-07-07T18:06:17.872Z", "APP_START", true.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),

                    ("2022-07-07T18:06:17.873Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.874Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.880Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                ],
                ..Default::default()
            },
            TestCase {
                name: "since",
                params: GetLog2Params { since: since("2022-07-07T18:06:17.872Z"), ..Default::default() },
                expected: vec![
                    ("2022-07-07T18:06:17.872Z", "APP_START", true.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/zone/Zone1/plcDisconnected", false.into()),

                    ("2022-07-07T18:06:17.873Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.874Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.880Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                ],
                ..Default::default()
            },
            TestCase {
                name: "until",
                params: GetLog2Params { until: Some(ts("2022-07-07T18:06:17.872Z")), ..Default::default() },
                expected: vec![
                    ("2022-07-07T18:06:15.557Z", "APP_START", true.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/zone/Zone1/plcDisconnected", false.into()),

                    ("2022-07-07T18:06:17.784Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.784Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.869Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                ],
                ..Default::default()
            },
            TestCase {
                name: "until after last entry",
                params: GetLog2Params { until: Some(ts("2022-07-07T18:06:17.900")), ..Default::default() },
                expected: vec![
                    ("2022-07-07T18:06:15.557Z", "APP_START", true.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/zone/Zone1/plcDisconnected", false.into()),

                    ("2022-07-07T18:06:17.784Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.784Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.869Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),

                    ("2022-07-07T18:06:17.872Z", "APP_START", true.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),

                    ("2022-07-07T18:06:17.873Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.874Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.880Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                ],
                ..Default::default()
            },
            TestCase {
                name: "until on last entry",
                params: GetLog2Params { until: Some(ts("2022-07-07T18:06:17.880")), ..Default::default() },
                expected: vec![
                    ("2022-07-07T18:06:15.557Z", "APP_START", true.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/zone/Zone1/plcDisconnected", false.into()),

                    ("2022-07-07T18:06:17.784Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.784Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.869Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),

                    ("2022-07-07T18:06:17.872Z", "APP_START", true.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),

                    ("2022-07-07T18:06:17.873Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.874Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                ],
                ..Default::default()
            },
            TestCase {
                name: "exact path",
                params: GetLog2Params { path_pattern: "zone1/pme/TSH1-1/switchRightCounterPermanent".to_string().into(), ..Default::default() },
                expected: vec![
                    ("2022-07-07T18:06:15.557Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                    ("2022-07-07T18:06:17.869Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                    ("2022-07-07T18:06:17.880Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                ],
                ..Default::default()
            },
            TestCase {
                name: "wildcard",
                params: GetLog2Params { path_pattern: "zone1/**".to_string().into(), ..Default::default() },
                expected: vec![
                    ("2022-07-07T18:06:15.557Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.784Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.784Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.869Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                    ("2022-07-07T18:06:17.873Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.874Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.880Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                ],
                ..Default::default()
            },
            TestCase {
                name: "record count higher",
                params: GetLog2Params { record_count_limit: 1000, ..Default::default() },
                expected_record_count_limit_hit: Some(false),
                expected: vec![
                    ("2022-07-07T18:06:15.557Z", "APP_START", true.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/zone/Zone1/plcDisconnected", false.into()),

                    ("2022-07-07T18:06:17.784Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.784Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.869Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),

                    ("2022-07-07T18:06:17.872Z", "APP_START", true.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),

                    ("2022-07-07T18:06:17.873Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.874Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.880Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                ],
                ..Default::default()
            },
            TestCase {
                name: "record count lower",
                params: GetLog2Params { record_count_limit: 7, ..Default::default() },
                expected_record_count_limit_hit: Some(true),
                expected: vec![
                    ("2022-07-07T18:06:15.557Z", "APP_START", true.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/zone/Zone1/plcDisconnected", false.into()),

                    ("2022-07-07T18:06:17.784Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.784Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.869Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                ],
                ..Default::default()
            },
            TestCase {
                name: "records from the same timestamp are always returned",
                params: GetLog2Params { record_count_limit: 5, ..Default::default() },
                expected_record_count_limit_hit: Some(true),
                expected: vec![
                    ("2022-07-07T18:06:15.557Z", "APP_START", true.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.784Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.784Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                ],
                ..Default::default()
            },
        ].into_iter().map(|test_case| (data_1 as fn() -> _, test_case)).chain([
            TestCase {
                name: "snapshot without since",
                params: GetLog2Params { with_snapshot: true, ..Default::default() },
                expected: vec![
                    ("2022-07-07T18:06:17.784Z", "value1", 0.into()),
                    ("2022-07-07T18:06:17.784Z", "value2", 1.into()),
                    ("2022-07-07T18:06:17.784Z", "value3", 3.into()),
                    ("2022-07-07T18:06:17.800Z", "value3", 200.into()),
                    ("2022-07-07T18:06:17.950Z", "value2", 10.into()),
                ],
                ..Default::default()
            },
            TestCase {
                name: "snapshot - one entry between snapshot and since",
                params: GetLog2Params { since: since("2022-07-07T18:06:17.850"), with_snapshot: true, ..Default::default() },
                expected: vec![
                    ("2022-07-07T18:06:17.850Z", "value1", 0.into()),
                    ("2022-07-07T18:06:17.850Z", "value2", 1.into()),
                    ("2022-07-07T18:06:17.850Z", "value3", 200.into()),
                    ("2022-07-07T18:06:17.950Z", "value2", 10.into()),
                ],
                ..Default::default()
            },
            TestCase {
                name: "snapshot - one entry exactly on since",
                params: GetLog2Params { since: since("2022-07-07T18:06:17.800"), with_snapshot: true, ..Default::default() },
                expected: vec![
                    ("2022-07-07T18:06:17.800Z", "value1", 0.into()),
                    ("2022-07-07T18:06:17.800Z", "value2", 1.into()),
                    ("2022-07-07T18:06:17.800Z", "value3", 200.into()),
                    ("2022-07-07T18:06:17.950Z", "value2", 10.into()),
                ],
                ..Default::default()
            },
            TestCase {
                name: "snapshot - with record cound limit smaller than the snapshot",
                params: GetLog2Params { since: since("2022-07-07T18:06:17.800"), with_snapshot: true, record_count_limit: 1, ..Default::default() },
                expected_record_count_limit_hit: Some(true),
                // The whole snapshot should be sent regardless of the small recordCountLimit.
                expected: vec![
                    ("2022-07-07T18:06:17.800Z", "value1", 0.into()),
                    ("2022-07-07T18:06:17.800Z", "value2", 1.into()),
                    ("2022-07-07T18:06:17.800Z", "value3", 200.into()),
                ],
                ..Default::default()
            },
            TestCase {
                name: "snapshot - with since after the last entry",
                params: GetLog2Params { since: since("2022-07-07T18:06:20.850"), with_snapshot: true, ..Default::default() },
                expected: vec![
                    ("2022-07-07T18:06:20.850Z", "value1", 0.into()),
                    ("2022-07-07T18:06:20.850Z", "value2", 10.into()),
                    ("2022-07-07T18:06:20.850Z", "value3", 200.into()),
                ],
                ..Default::default()
            },
            TestCase {
                name: "since last with snapshot",
                params: GetLog2Params { since: GetLog2Since::LastEntry, with_snapshot: true, ..Default::default() },
                expected: vec![
                    ("2022-07-07T18:06:17.950Z", "value1", 0.into()),
                    ("2022-07-07T18:06:17.950Z", "value2", 10.into()),
                    ("2022-07-07T18:06:17.950Z", "value3", 200.into()),
                ],
                expected_since: Some("2022-07-07T18:06:17.950Z"),
                expected_until: Some("2022-07-07T18:06:17.950Z"),
                ..Default::default()
            },
            TestCase {
                name: "since last without snapshot",
                params: GetLog2Params { since: GetLog2Since::LastEntry, with_snapshot: false, ..Default::default() },
                expected: vec![
                    ("2022-07-07T18:06:17.950Z", "value2", 10.into()),
                ],
                expected_since: Some("2022-07-07T18:06:17.950Z"),
                expected_until: Some("2022-07-07T18:06:17.950Z"),
                ..Default::default()
            },
        ].into_iter().map(|test_case| (data_2 as fn() -> _, test_case))).chain([
            ("result since/until - default params", Default::default()),
            ("result since/until - since set", GetLog2Params { since: since("2022-07-07T18:06:15.557Z"), ..Default::default() }),
            ("result since/until - until set", GetLog2Params { until: Some(ts("2022-07-07T18:06:15.557Z")), ..Default::default() }),
            ("result since/until - both since/until set", GetLog2Params { since: since("2022-07-07T18:06:15.557Z"), until: Some(ts("2022-07-07T18:06:20.000Z")), ..Default::default() }),
        ].into_iter().map(|(name, params)| (data_3 as fn() -> _, TestCase {
            name,
            params,
            expected: vec![],
            // For empty dataset, getlog always returns this since and until.
            expected_since: Some("1970-01-01T00:00:00.000Z"),
            expected_until: Some("1970-01-01T00:00:00.000Z"),
            ..Default::default()
        }))).chain([
            TestCase {
                name: "since/until not set",
                params: Default::default(),
                expected: vec![
                    ("2022-07-07T18:06:14.000Z", "value1", 10.into()),
                    ("2022-07-07T18:06:15.557Z", "value2", 20.into()),
                    ("2022-07-07T18:06:16.600Z", "value3", 30.into()),
                    ("2022-07-07T18:06:17.784Z", "value4", 40.into()),
                ],
                expected_since: Some("2022-07-07T18:06:14.000Z"),
                expected_until: Some("2022-07-07T18:06:17.785Z"),
                ..Default::default()
            },
            TestCase {
                name: "since on the first entry",
                params: GetLog2Params{since: since("2022-07-07T18:06:15.557Z"), ..Default::default()},
                expected: vec![
                    ("2022-07-07T18:06:15.557Z", "value2", 20.into()),
                    ("2022-07-07T18:06:16.600Z", "value3", 30.into()),
                    ("2022-07-07T18:06:17.784Z", "value4", 40.into()),
                ],
                expected_since: Some("2022-07-07T18:06:15.557Z"),
                expected_until: Some("2022-07-07T18:06:17.785Z"),
                ..Default::default()
            },
            TestCase {
                name: "since before the first entry",
                params: GetLog2Params{since: since("2022-07-07T18:06:13.000Z"), ..Default::default()},
                expected: vec![
                    ("2022-07-07T18:06:14.000Z", "value1", 10.into()),
                    ("2022-07-07T18:06:15.557Z", "value2", 20.into()),
                    ("2022-07-07T18:06:16.600Z", "value3", 30.into()),
                    ("2022-07-07T18:06:17.784Z", "value4", 40.into()),
                ],
                expected_since: Some("2022-07-07T18:06:14.000Z"),
                expected_until: Some("2022-07-07T18:06:17.785Z"),
                ..Default::default()
            },
            TestCase {
                name: "since after the first entry",
                params: GetLog2Params{since: since("2022-07-07T18:06:15.553Z"), ..Default::default()},
                expected: vec![
                    ("2022-07-07T18:06:15.557Z", "value2", 20.into()),
                    ("2022-07-07T18:06:16.600Z", "value3", 30.into()),
                    ("2022-07-07T18:06:17.784Z", "value4", 40.into()),
                ],
                expected_since: Some("2022-07-07T18:06:15.553Z"),
                expected_until: Some("2022-07-07T18:06:17.785Z"),
                ..Default::default()
            },
            TestCase {
                name: "until set",
                params: GetLog2Params{until: Some(ts("2022-07-07T18:06:18.700")), ..Default::default()},
                expected: vec![
                    ("2022-07-07T18:06:14.000Z", "value1", 10.into()),
                    ("2022-07-07T18:06:15.557Z", "value2", 20.into()),
                    ("2022-07-07T18:06:16.600Z", "value3", 30.into()),
                    ("2022-07-07T18:06:17.784Z", "value4", 40.into()),
                ],
                expected_since: Some("2022-07-07T18:06:14.000Z"),
                expected_until: Some("2022-07-07T18:06:17.785Z"),
                ..Default::default()
            },
            TestCase {
                name: "until set - record count limit hit",
                params: GetLog2Params{record_count_limit: 2, until: Some(ts("2022-07-07T18:06:18.700")), ..Default::default()},
                expected: vec![
                    ("2022-07-07T18:06:14.000Z", "value1", 10.into()),
                    ("2022-07-07T18:06:15.557Z", "value2", 20.into()),
                ],
                expected_since: Some("2022-07-07T18:06:14.000Z"),
                expected_until: Some("2022-07-07T18:06:16.600Z"),
                ..Default::default()
            },
            TestCase {
                name: "since/until set",
                params: GetLog2Params{since: since("2022-07-07T18:06:10.000Z"), until: Some(ts("2022-07-07T18:06:20.000Z")), ..Default::default()},
                expected: vec![
                    ("2022-07-07T18:06:14.000Z", "value1", 10.into()),
                    ("2022-07-07T18:06:15.557Z", "value2", 20.into()),
                    ("2022-07-07T18:06:16.600Z", "value3", 30.into()),
                    ("2022-07-07T18:06:17.784Z", "value4", 40.into()),
                ],
                expected_since: Some("2022-07-07T18:06:14.000Z"),
                expected_until: Some("2022-07-07T18:06:17.785Z"),
                ..Default::default()
            },
            TestCase {
                name: "since/until set - record_count_limit hit",
                params: GetLog2Params{record_count_limit: 2, since: since("2022-07-07T18:06:10.000Z"), until: Some(ts("2022-07-07T18:06:20.000Z")), ..Default::default()},
                expected: vec![
                    ("2022-07-07T18:06:14.000Z", "value1", 10.into()),
                    ("2022-07-07T18:06:15.557Z", "value2", 20.into()),
                ],
                expected_since: Some("2022-07-07T18:06:14.000Z"),
                expected_until: Some("2022-07-07T18:06:16.600Z"),
                ..Default::default()
            }
        ].into_iter().map(|test_case| (data_4 as fn() -> _, test_case)));

        for (data, case) in test_cases {
            let result = get_log_entries(data(), case.params).await;
            let chained_entries = result.snapshot_entries.into_iter().chain(result.event_entries.into_iter()).collect::<Vec<_>>();
            let expected = case.expected.into_iter().map(|(ts, path, val)| (ts.to_string(), path.to_string(), val)).collect::<Vec<_>>();
            assert_eq!(chained_entries.into_iter().map(|entry_val| (DateTime::from_epoch_msec(entry_val.epoch_msec).to_iso_string(), entry_val.path.clone(), entry_val.value.clone())).collect::<Vec<_>>(), expected, "Test case failed: {}", case.name);
            if let Some(expected_record_count_limit_hit) = case.expected_record_count_limit_hit {
                assert_eq!(result.record_count_limit_hit, expected_record_count_limit_hit, "Test case failed: {}", case.name);
            }
            if let Some(expected_since) = case.expected_since {
                assert_eq!(result.since.to_iso_string(), expected_since, "Test case failed: {}", case.name);
            }
            if let Some(expected_until) = case.expected_until {
                assert_eq!(result.until.to_iso_string(), expected_until, "Test case failed: {}", case.name);
            }
        }
    }
}
