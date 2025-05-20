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
use tokio::io::{AsyncBufRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use tokio_stream::wrappers::ReadDirStream;
use tokio_util::io::ReaderStream;

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
const METH_READ: &str = "read";
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
            nodes.append(&mut children_on_path(&*app_state.sites.0.read().await, ROOT_PATH).unwrap_or_default());
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

struct LsFilesEntry {
    name: String,
    ftype: char,
    size: i64,
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
            METH_SYNC_INFO => return Ok("To be implemented".into()),
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
                                let ftype = if meta.is_dir() { 'd' } else if meta.is_file() { 'f' } else { return None };
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
                let size = read_params.size.unwrap_or(0);
                let res = read_file(path, offset, size)
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
                let size = read_params.size.unwrap_or(0);
                let (res, bytes_read) = compress_file(path, offset, size)
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
    size: u64,
    process: F,
) -> tokio::io::Result<R>
where
    F: FnOnce(Box<dyn AsyncBufRead + Unpin + Send>) -> Fut + Send,
    Fut: Future<Output = tokio::io::Result<R>> + Send,
{
    const MAX_READ_SIZE: u64 = 10 * (1 << 20);
    let mut file = tokio::fs::File::open(path.as_ref()).await?;
    file.seek(std::io::SeekFrom::Start(offset)).await?;
    let file_size = file.metadata().await?.len();
    let size = if size == 0 { file_size } else { size }.min(file_size).min(MAX_READ_SIZE);
    let reader = BufReader::new(file).take(size);
    process(Box::new(reader)).await
}

async fn read_file(path: impl AsRef<str>, offset: u64, size: u64) -> tokio::io::Result<Vec<u8>> {
    with_file_reader(path, offset, size, |mut reader| async move {
        let mut res = Vec::new();
        reader.read_to_end(&mut res).await?;
        Ok(res)
    }).await
}

async fn compress_file(path: impl AsRef<str>, offset: u64, size: u64) -> tokio::io::Result<(Vec<u8>, u64)> {
    let _l = ScopedLog::new(format!("compress: {}", path.as_ref()));
    with_file_reader(path, offset, size, |mut reader| async move {
        let mut res = Vec::new();
        let mut encoder = GzipEncoder::new(&mut res);
        let bytes_read = tokio::io::copy_buf(&mut reader, &mut encoder).await?;
        encoder.shutdown().await?;
        Ok((res, bytes_read))
    }).await
}

async fn history_request_handler(
    rq: RpcMessage,
    _client_cmd_tx: ClientCommandSender,
    app_state: AppState<State>,
) -> RpcRequestResult {
    let method = rq.method().unwrap_or_default();
    let path = rq.shv_path().unwrap_or_default();
    let children = children_on_path(&*app_state.sites.0.read().await, path)
        .unwrap_or_else(|| panic!("Children on path `{path}` should be Some after methods processing"));

    match method {
        METH_LS => Ok(children.into()),
        METH_GET_LOG if children.is_empty() => Ok(vec!["getLog: TODO"].into()),
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
            let children = children_on_path(&*app_state.sites.0.read().await, path)?;
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

