use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use async_compression::tokio::write::GzipEncoder;
use futures::{StreamExt, TryStreamExt};
use log::error;
use sha1::Digest;
use shvclient::appnodes::DOT_APP_METHODS;
use shvclient::clientnode::{err_unresolved_request, LsHandlerResult, Method, RequestHandlerResult, UnresolvedRequest};
use shvclient::clientnode::StaticNode;
use shvclient::shvproto::RpcValue;
use shvclient::ClientCommandSender;
use shvrpc::metamethod::{AccessLevel, MetaMethod};
use shvrpc::rpcmessage::{RpcError, RpcErrorCode};
use shvrpc::util::children_on_path;
use shvrpc::{RpcMessage, RpcMessageMetaTags};
use tokio::io::{AsyncBufRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::RwLockReadGuard;
use tokio_stream::wrappers::ReadDirStream;
use tokio_util::io::ReaderStream;

use crate::alarmlog::{alarmlog_impl, AlarmLogParams};
use crate::cleanup::collect_log2_files;
use crate::getlog::getlog_handler;
use crate::journalrw::{journal_entries_to_rpcvalue, GetLog2Params, Log2Header, Log2Reader};
use crate::pushlog::pushlog_impl;
use crate::sites::SubHpInfo;
use crate::sync::log_size_limit;
use crate::{AlarmWithTimestamp, HpConfig, State};

// History site node methods
const METH_GET_LOG: &str = "getLog";
const METH_ALARM_TABLE: &str = "alarmTable";
const METH_STATE_ALARM_TABLE: &str = "stateAlarmTable";
const METH_ALARM_LOG: &str = "alarmLog";
const METH_PUSH_LOG: &str = "pushLog";

const META_METHOD_LS_FILES: MetaMethod = MetaMethod::new_static(METH_LS_FILES, 0, AccessLevel::Read, "Map|Null", "List", &[], "");
const META_METHOD_GET_LOG: MetaMethod = MetaMethod::new_static(METH_GET_LOG, 0, AccessLevel::Read, "RpcValue", "RpcValue", &[], "");
const META_METHOD_ALARM_TABLE: MetaMethod = MetaMethod::new_static(METH_ALARM_TABLE, 0, AccessLevel::Read, "RpcValue", "RpcValue", &[("alarmmod", Some("Null"))], "");
const META_METHOD_STATE_ALARM_TABLE: MetaMethod = MetaMethod::new_static(METH_STATE_ALARM_TABLE, 0, AccessLevel::Read, "RpcValue", "RpcValue", &[("statealarmmod", Some("Null"))], "");
const META_METHOD_ALARM_LOG: MetaMethod = MetaMethod::new_static(METH_ALARM_LOG, 0, AccessLevel::Read, "RpcValue", "RpcValue", &[], "");
const META_METHOD_PUSH_LOG: MetaMethod = MetaMethod::new_static(METH_PUSH_LOG, 0, AccessLevel::Write, "RpcValue", "RpcValue", &[], "");

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

fn rpc_error_filesystem(err: std::io::Error) -> RpcError {
    RpcError::new(
        RpcErrorCode::MethodCallException,
        format!("Filesystem error: {err}")
    )
}

fn path_contains_parent_dir_references(path: impl AsRef<str>) -> bool {
    path.as_ref().split('/').any(|fragment| fragment == "..")
}

async fn shvjournal_request_handler(
    path: String,
    method: Method,
    param: RpcValue,
    app_state: Arc<State>,
) -> RequestHandlerResult
{
    async fn get_journaldir_entries(path: impl AsRef<Path>) -> Result<ReadDirStream, RpcError> {
        Ok(ReadDirStream::new(
                tokio::fs::read_dir(path)
                .await
                .map_err(rpc_error_filesystem)?)
        )
    }

    async fn journaldir_ls_handler(entries: ReadDirStream) -> LsHandlerResult {
        entries
            .try_filter_map(async |entry| {
                    Ok(entry.file_name().to_str().map(String::from))
                }
            )
            .try_collect::<Vec<_>>()
            .await
            .map_err(rpc_error_filesystem)
            .map(|mut res| { res.sort(); res})
    }

    async fn journaldir_lsfiles_handler(entries: ReadDirStream) -> Result<Vec<LsFilesEntry>, RpcError> {
        entries
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
                res
            })
    }

    async fn journalfile_methods_handler(method: &str, path: &str, file_size: u64, param: &RpcValue) -> Result<RpcValue, RpcError> {
        match method {
            METH_HASH => {
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
            METH_SIZE => Ok((file_size as i64).into()),
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
            _ => Err(rpc_error_method_not_found()),
        }
    }


    if path_contains_parent_dir_references(&path) {
        // Reject parent dir references
        return err_unresolved_request()
    }
    let is_shvjournal_root = &path == "_shvjournal";
    let path = path.replacen("_shvjournal", &app_state.config.journal_dir, 1);

    if is_shvjournal_root {
        const METHODS: &[MetaMethod] = &[
            META_METHOD_LS_FILES,
            MetaMethod::new_static(METH_LOG_SIZE_LIMIT, 0, AccessLevel::Developer, "Null", "Int", &[], ""),
            MetaMethod::new_static(METH_TOTAL_LOG_SIZE, 0, AccessLevel::Developer, "Null", "Int", &[], "Returns: total size occupied by logs."),
            MetaMethod::new_static(METH_LOG_USAGE, 0, AccessLevel::Developer, "Null", "Decimal", &[], "Returns: percentage of space occupied by logs."),
            MetaMethod::new_static(METH_SYNC_LOG, 0, AccessLevel::Write, "String|Map", "List", &[], "syncLog - triggers a manual sync\nAccepts a mandatory string param, only the subtree signified by the string is synced.\nsyncLog also takes a map param in this format: {\n\twaitForFinished: bool // the method waits until the whole operation is finished and only then returns a response\n\tshvPath: string // the subtree to be synced\n}\n\nReturns: a list of all leaf sites that will be synced\n"),
            MetaMethod::new_static(METH_SYNC_INFO, 0, AccessLevel::Read, "String", "Map", &[], "syncInfo - returns info about sites' sync status\nOptionally takes a string that filters the sites by prefix.\n\nReturns: a map where they is the path of the site and the value is a map with a status string and a last updated timestamp.\n"),
            MetaMethod::new_static(METH_SANITIZE_LOG, 0, AccessLevel::Developer, "Null", "String", &[], ""),
        ];
        match method {
            Method::Dir(dir) => return dir.resolve(METHODS),
            Method::Ls(ls) => return ls.resolve(METHODS, async || {
                journaldir_ls_handler(get_journaldir_entries(path).await?).await
            }),
            Method::Other(m) => match m.method() {
                METH_LS_FILES => return m.resolve(METHODS, async || {
                    journaldir_lsfiles_handler(get_journaldir_entries(path).await?).await
                }),
                METH_LOG_SIZE_LIMIT => return m.resolve(METHODS, async move || Ok(log_size_limit(&app_state.config))),
                METH_TOTAL_LOG_SIZE => return m.resolve(METHODS, async move || total_log_size(&app_state.config)
                    .await
                    .map(RpcValue::from)
                    .map_err(rpc_error_filesystem)
                ),
                METH_LOG_USAGE => return m.resolve(METHODS, async move || total_log_size(&app_state.config)
                        .await
                        .map(|size| 100. * (size as f64) / (log_size_limit(&app_state.config) as f64))
                        .map_err(rpc_error_filesystem)
                ),
                METH_SYNC_LOG => return m.resolve(METHODS, async move || sync_log_request_handler(&param, app_state).await),
                METH_SYNC_INFO => return m.resolve(METHODS, async move || Ok((*app_state.sync_info.sites_sync_info.read().await).to_owned())),
                METH_SANITIZE_LOG => return m.resolve(METHODS, async move || app_state.sync_cmd_tx
                        .send(crate::sync::SyncCommand::Cleanup)
                        .map(|_| true)
                        .map_err(|_| RpcError::new(RpcErrorCode::InternalError, "Cannot send the command through the channel"))
                ),
                _ => return err_unresolved_request(),
            }
        }
    }

    // Probe the path on the fs. Prevent leaking FS info to unauthorized
    // users, do not return `rpc_error_filesystem`.
    let path_meta = tokio::fs::metadata(&path)
        .await
        .inspect_err(|err| error!("Cannot read FS metadata, path: {path}, error: {err}"))
        .or(Err(UnresolvedRequest))?;

    if path_meta.is_dir() {
        const METHODS: &[MetaMethod] = &[META_METHOD_LS_FILES];
        match method {
            Method::Dir(dir) => dir.resolve(METHODS),
            Method::Ls(ls) => ls.resolve(METHODS, async || {
                journaldir_ls_handler(get_journaldir_entries(path).await?).await
            }),
            Method::Other(m) if m.method() == METH_LS_FILES => m.resolve(METHODS, async || {
                journaldir_lsfiles_handler(get_journaldir_entries(path).await?).await
            }),
            _ => err_unresolved_request(),
        }
    } else if path_meta.is_file() {
        const METHODS: &[MetaMethod] = &[
            MetaMethod::new_static(METH_HASH, 0, AccessLevel::Read, "Map|Null", "String", &[], ""),
            MetaMethod::new_static(METH_SIZE, 0, AccessLevel::Browse, "", "Int", &[], ""),
            MetaMethod::new_static(METH_READ, 0, AccessLevel::Read, "Map", "Blob", &[], "Parameters\n  offset: file offset to start read, default is 0\n  size: number of bytes to read starting on offset, default is till end of file\n"),
            MetaMethod::new_static(METH_READ_COMPRESSED, 0, AccessLevel::Read, "Map", "Blob", &[], "Parameters\n  read() parameters\n  compressionType: gzip (default) | qcompress"),
        ];
        match method {
            Method::Dir(dir) => dir.resolve(METHODS),
            Method::Ls(ls) => ls.resolve(METHODS, ls_empty),
            Method::Other(m) => {
                let method = m.method().to_owned();
                m.resolve(METHODS, async move || {
                    journalfile_methods_handler(&method, &path, path_meta.len(), &param).await
                })
            },
        }
    } else {
        err_unresolved_request()
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

async fn total_log_size(config: &HpConfig) -> tokio::io::Result<i64> {
    collect_log2_files(&config.journal_dir)
        .await
        .map(|files| files.into_iter().map(|f| f.size as i64).sum::<i64>())
}

async fn sync_log_request_handler(param: &RpcValue, app_state: Arc<State>) -> Result<Vec<String>, RpcError> {
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
        .map(|(site, _)| site.to_owned())
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

    Ok(sites_to_sync)
}

#[derive(Default)]
struct ReadParams {
    offset: Option<u64>,
    size: Option<u64>,
}

impl TryFrom<RpcValue> for ReadParams {
    type Error = String;

    fn try_from(value: RpcValue) -> Result<Self, Self::Error> {
        (&value).try_into()
    }
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
    app_state: Arc<State>,
) -> Result<RpcValue, RpcError> {
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
    app_state: Arc<State>,
) -> Result<RpcValue, RpcError> {
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

trait AlarmGetter {
    async fn alarm_getter(f: &State) -> RwLockReadGuard<'_, BTreeMap<String, Vec<AlarmWithTimestamp>>>;
}

struct CommonAlarm;
impl AlarmGetter for CommonAlarm {
    async fn alarm_getter(f: &State) -> RwLockReadGuard<'_, BTreeMap<String, Vec<AlarmWithTimestamp>>> {
        f.alarms.read().await
    }
}

struct StateAlarm;
impl AlarmGetter for StateAlarm {
    async fn alarm_getter(f: &State) -> RwLockReadGuard<'_, BTreeMap<String, Vec<AlarmWithTimestamp>>> {
        f.state_alarms.read().await
    }
}

async fn alarmtable_handler<Getter: AlarmGetter>(
    site_path: &str,
    app_state: Arc<State>,
) -> Result<RpcValue, RpcError> {
    if !app_state.sites_data.read().await.sites_info.contains_key(site_path) {
        return Err(RpcError::new(RpcErrorCode::InvalidParam, format!("Wrong alarmTable path: {site_path}")));
    }

    match Getter::alarm_getter(&app_state).await.get(site_path) {
        Some(alarms_for_site) => Ok(alarms_for_site.clone().into()),
        None => Ok(Vec::<RpcValue>::new().into()),
    }
}

async fn alarmlog_handler(
    path: &str,
    param: &RpcValue,
    app_state: Arc<State>,
) -> Result<RpcValue, RpcError> {
    let params = AlarmLogParams::try_from(param)
        .map_err(|err| RpcError::new(RpcErrorCode::InvalidParam, format!("Wrong alarmLog parameters: {err}")))?;

    Ok(alarmlog_impl(path, &params, app_state).await.into())
}

fn rpc_error_not_implemented() -> RpcError {
    RpcError::new(RpcErrorCode::NotImplemented, "Method is not implemented")
}

fn rpc_error_method_not_found() -> RpcError {
    RpcError::new(RpcErrorCode::MethodNotFound, "Method not found")
}

async fn ls_empty() -> Result<Vec<String>, RpcError> {
    Ok(vec![])
}

pub(crate) async fn request_handler(
    rq: RpcMessage,
    client_cmd_tx: ClientCommandSender,
    app_state: Arc<State>,
) -> RequestHandlerResult
{
    let path = rq.shv_path().map_or_else(String::new, String::from);
    let method = Method::from_request(&rq);
    let param = rq.param().map_or_else(RpcValue::null, RpcValue::clone);

    match NodeType::from_path(&path) {
        NodeType::DotApp => {
            match method {
                Method::Dir(dir) => dir.resolve(DOT_APP_METHODS),
                Method::Ls(ls) => ls.resolve(DOT_APP_METHODS, ls_empty),
                Method::Other(m) => m.resolve_opt(DOT_APP_METHODS, async move ||
                    DOT_APP_NODE.process_request(rq, client_cmd_tx).await
                ),
            }
        }
        NodeType::ShvJournal =>
            shvjournal_request_handler(path, method, param, app_state).await,
        NodeType::ValueCache => {
            const METHODS: &[MetaMethod] = &[
                MetaMethod::new_static(METH_GET, 0, AccessLevel::Developer, "String", "RpcValue", &[], ""),
                MetaMethod::new_static(METH_GET_CACHE, 0, AccessLevel::Developer, "Null", "Map", &[], ""),
            ];
            match method {
                Method::Dir(dir) => dir.resolve(METHODS),
                Method::Ls(ls) => ls.resolve(METHODS, ls_empty),
                Method::Other(m) if m.method() == METH_GET || m.method() == METH_GET_CACHE => m.resolve(METHODS, async || Err::<(), _>(rpc_error_not_implemented())),
                _ => err_unresolved_request(),
            }
        }
        NodeType::Root => {
            const METHODS: &[MetaMethod] = &[
                MetaMethod::new_static(METH_VERSION, 0, AccessLevel::Read, "Null", "String", &[], ""),
                MetaMethod::new_static(METH_UPTIME, 0, AccessLevel::Read, "Null", "String", &[], ""),
                MetaMethod::new_static(METH_RELOAD_SITES, 0, AccessLevel::Write, "Null", "Bool", &[], ""),
                META_METHOD_ALARM_LOG,
                // TODO: All root node methods:
                // appName
                // deviceId
                // deviceType
                // gitCommit
                // shvVersion
                // shvGitCommit
                // reloadSites (wr) -> Bool
            ];
            match method {
                Method::Dir(dir) => dir.resolve(METHODS),
                Method::Ls(ls) => {
                    let ls_handler = async move || {
                        let mut nodes = vec![
                            ".app".to_string(),
                            "_shvjournal".to_string(),
                            "_valuecache".to_string()
                        ];
                        const ROOT_PATH: &str = "";
                        nodes.append(&mut children_on_path(&app_state.sites_data.read().await.sites_info, ROOT_PATH).unwrap_or_default());
                        Ok(nodes)
                    };
                    ls.resolve(METHODS, ls_handler)
                }
                Method::Other(m) => match m.method() {
                    METH_VERSION => m.resolve(METHODS, async || Ok(env!("CARGO_PKG_VERSION"))),
                    METH_UPTIME => m.resolve(METHODS, async move || {
                        Ok(humantime::format_duration(std::time::Duration::from_secs(app_state.start_time.elapsed().as_secs())).to_string())
                    }),
                    METH_ALARM_LOG => m.resolve(METHODS, async move || {
                        let params = AlarmLogParams::try_from(param)
                            .map_err(|err| RpcError::new(RpcErrorCode::InvalidParam, format!("Wrong alarmLog parameters: {err}")))?;
                        Ok(alarmlog_impl(&path, &params, app_state).await)
                    }),
                    _ => err_unresolved_request(),
                }
            }
        }
        NodeType::History => {
            let methods = {
                let sites_data = app_state.sites_data.read().await;
                let is_site_path = children_on_path(&sites_data.sites_info, &path)
                    .ok_or(UnresolvedRequest)?
                    .is_empty();
                if is_site_path {
                    let is_pushlog = sites_data.sites_info.get(&path)
                        .and_then(|site_info| sites_data.sub_hps.get(&site_info.sub_hp))
                        .is_some_and(|sub_hp_info| matches!(sub_hp_info, SubHpInfo::PushLog));

                    if is_pushlog {
                        const METHODS: &[MetaMethod] = &[META_METHOD_GET_LOG, META_METHOD_PUSH_LOG];
                        METHODS
                    } else if sites_data.typeinfos.get(&path).is_some_and(Result::is_ok) {
                        const METHODS: &[MetaMethod] = &[META_METHOD_GET_LOG, META_METHOD_ALARM_TABLE, META_METHOD_STATE_ALARM_TABLE, META_METHOD_ALARM_LOG];
                        METHODS
                    } else {
                        const METHODS: &[MetaMethod] = &[META_METHOD_GET_LOG, META_METHOD_ALARM_LOG];
                        METHODS
                    }
                } else {
                    // `path` is a dir in the middle of the tree
                    const METHODS: &[MetaMethod] = &[META_METHOD_ALARM_LOG];
                    METHODS
                }
            };
            match method {
                Method::Dir(dir) => dir.resolve(methods),
                Method::Ls(ls) => {
                    ls.resolve(methods, async move || {
                        let children = children_on_path(&app_state.sites_data.read().await.sites_info, &path)
                            .unwrap_or_else(|| panic!("Children on path `{path}` should be Some after methods processing"));
                        Ok(children)
                    })
                }
                Method::Other(m) => match m.method() {
                    METH_GET_LOG => m.resolve(methods, async move || { getlog_handler_rq(&path, &param, app_state).await }),
                    METH_ALARM_TABLE => m.resolve(methods, async move || { alarmtable_handler::<CommonAlarm>(&path, app_state).await }),
                    METH_STATE_ALARM_TABLE => m.resolve(methods, async move || { alarmtable_handler::<StateAlarm>(&path, app_state).await }),
                    METH_ALARM_LOG => m.resolve(methods, async move || { alarmlog_handler(&path, &param, app_state).await }),
                    METH_PUSH_LOG => m.resolve(methods, async move || { pushlog_handler(&path, param, app_state).await }),
                    _ => err_unresolved_request(),
                },
            }
        }
    }
}
