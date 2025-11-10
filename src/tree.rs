use std::sync::Arc;

use async_compression::tokio::write::GzipEncoder;
use futures::{StreamExt, TryStreamExt};
use sha1::Digest;
use shvclient::client::MetaMethods;
use shvclient::clientnode::{children_on_path, ConstantNode, METH_LS};
use shvclient::AppState;
use shvproto::RpcValue;
use shvrpc::metamethod::MetaMethod;
use shvrpc::rpcmessage::{RpcError, RpcErrorCode};
use shvrpc::{RpcMessage, RpcMessageMetaTags};
use tokio::io::{AsyncBufRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio_stream::wrappers::ReadDirStream;
use tokio_util::io::ReaderStream;

use crate::alarmlog::{alarmlog_impl, AlarmLogParams};
use crate::cleanup::collect_log2_files;
use crate::getlog::getlog_handler;
use crate::journalrw::{journal_entries_to_rpcvalue, GetLog2Params, Log2Header, Log2Reader};
use crate::pushlog::pushlog_impl;
use crate::sites::SubHpInfo;
use crate::{ClientCommandSender, HpConfig, State, MAX_JOURNAL_DIR_SIZE_DEFAULT};

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
const METH_VERSION: &str = "version";
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
    path: &str,
    method: &str,
    param: &RpcValue,
    app_state: AppState<State>,
) -> RpcRequestResult {

    const ROOT_PATH: &str = "";
    match method {
        METH_VERSION => Ok(env!("CARGO_PKG_VERSION").into()),
        METH_UPTIME => Ok(humantime::format_duration(std::time::Duration::from_secs(app_state.start_time.elapsed().as_secs())).to_string().into()),
        METH_LS => {
            let mut nodes = vec![
                ".app".to_string(),
                "_shvjournal".to_string(),
                "_valuecache".to_string()
            ];
            nodes.append(&mut children_on_path(&app_state.sites_data.read().await.sites_info, ROOT_PATH).unwrap_or_default());
            Ok(nodes.into())
        }
        METH_ALARM_LOG => alarmlog_handler(path, param, app_state).await,
        _ => Ok("Not implemented".into()),
    }
}

async fn valuecache_request_handler(
    method: &str,
) -> RpcRequestResult {
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

async fn sync_log_request_handler(param: &RpcValue, app_state: AppState<State>) -> RpcRequestResult {
    let shvproto::Value::String(shv_path) = &param.value else {
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
    path: &str,
    method: &str,
    param: &RpcValue,
    app_state: AppState<State>,
) -> RpcRequestResult {

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
            METH_SYNC_LOG => return sync_log_request_handler(param, app_state).await,
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
                let read_params: ReadParams = param
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
                let read_params: ReadParams = param
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
    site_path: &str,
    param: RpcValue,
    app_state: AppState<State>,
) -> RpcRequestResult {
    let log_reader = Log2Reader::new(param)
        .map_err(|e| RpcError::new(RpcErrorCode::InvalidParam, format!("Cannot parse pushLog parameter: {e}")))?;

    if !app_state.sites_data.read().await.sites_info.contains_key(site_path) {
        return Err(RpcError::new(RpcErrorCode::InvalidParam, format!("Wrong site path for pushLog: {site_path}")));
    }

    Ok(pushlog_impl(log_reader, site_path, app_state).await.into())
}

async fn getlog_handler_rq(
    site_path: &str,
    param: &RpcValue,
    app_state: AppState<State>,
) -> RpcRequestResult {
    let params = GetLog2Params::try_from(param)
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
    site_path: &str,
    app_state: AppState<State>,
) -> RpcRequestResult {
    if !app_state.sites_data.read().await.sites_info.contains_key(site_path) {
        return Err(RpcError::new(RpcErrorCode::InvalidParam, format!("Wrong alarmTable path: {site_path}")));
    }
    match app_state.alarms.read().await.get(site_path) {
        Some(alarms_for_site) => Ok(alarms_for_site.clone().into()),
        None => Ok(Vec::<RpcValue>::new().into()),
    }
}

async fn alarmlog_handler(
    path: &str,
    param: &RpcValue,
    app_state: AppState<State>,
) -> RpcRequestResult {
    let params = AlarmLogParams::try_from(param)
        .map_err(|err| RpcError::new(RpcErrorCode::InvalidParam, format!("Wrong alarmLog parameters: {err}")))?;

    Ok(alarmlog_impl(path, &params, app_state).await.into())
}

async fn history_request_handler(
    path: &str,
    method: &str,
    param: RpcValue,
    _client_cmd_tx: ClientCommandSender,
    app_state: AppState<State>,
) -> RpcRequestResult {
    let children = children_on_path(&app_state.sites_data.read().await.sites_info, path)
        .unwrap_or_else(|| panic!("Children on path `{path}` should be Some after methods processing"));

    match method {
        METH_LS => Ok(children.into()),
        METH_GET_LOG => getlog_handler_rq(path, &param, app_state).await,
        METH_ALARM_TABLE => alarmtable_handler(path, app_state).await,
        METH_ALARM_LOG => alarmlog_handler(path, &param, app_state).await,
        METH_PUSH_LOG => pushlog_handler(path, param, app_state).await,
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
                        name: METH_VERSION,
                        flags: 0,
                        access: shvrpc::metamethod::AccessLevel::Read,
                        param: "Null",
                        result: "String",
                        signals: &[],
                        description: "",
                    },
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
                    &META_METHOD_ALARM_LOG,
                    // TODO: All root node methods:
                    //
                    // appName
                    // deviceId
                    // deviceType
                    // gitCommit
                    // shvVersion
                    // shvGitCommit
                    // reloadSites (wr) -> Bool
        ])),
        NodeType::History => {
            let sites_data = app_state.sites_data.read().await;
            let children = children_on_path(&sites_data.sites_info, &path)?;
            if children.is_empty() {
                // `path` is a site path
                let is_pushlog = sites_data.sites_info.get(&path)
                    .and_then(|site_info| sites_data.sub_hps.get(&site_info.sub_hp))
                    .is_some_and(|sub_hp_info| matches!(sub_hp_info, SubHpInfo::PushLog));

                if is_pushlog {
                    return Some(MetaMethods::from(&[&META_METHOD_GET_LOG, &META_METHOD_PUSH_LOG]))
                }

                if sites_data.typeinfos.get(&path).is_some_and(Result::is_ok) {
                    Some(MetaMethods::from(&[&META_METHOD_GET_LOG, &META_METHOD_ALARM_TABLE, &META_METHOD_ALARM_LOG]))
                } else {
                    Some(MetaMethods::from(&[&META_METHOD_GET_LOG, &META_METHOD_ALARM_LOG]))
                }
            } else {
                // `path` is a dir in the middle of the tree
                Some(MetaMethods::from(&[&META_METHOD_ALARM_LOG]))
            }
        }
    }
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
    let param = rq.param().map_or_else(RpcValue::null, RpcValue::clone);

    match NodeType::from_path(path) {
        NodeType::Root =>
            root_request_handler(path, method, &param, app_state).await,
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
            valuecache_request_handler(method).await,
        NodeType::ShvJournal =>
            shvjournal_request_handler(path, method, &param, app_state).await,
        NodeType::History =>
            history_request_handler(path, method, param, client_cmd_tx, app_state).await,
    }
}
