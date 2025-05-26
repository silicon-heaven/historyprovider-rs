use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use futures::channel::mpsc::UnboundedReceiver;
use futures::StreamExt;
use shvclient::client::RpcCall;
use shvclient::clientnode::METH_DIR;
use shvclient::{AppState, ClientEventsReceiver};
use shvproto::RpcValue;
use shvrpc::join_path;
use time::format_description::well_known::Iso8601;
use tokio::io::AsyncWriteExt;
use tokio::sync::{RwLock, Semaphore};

use crate::sites::{SitesData, SubHpInfo};
use crate::tree::{FileType, LsFilesEntry, METH_READ};
use crate::{ClientCommandSender, State};

#[derive(Default)]
pub(crate) struct SyncInfo {
    pub(crate) last_synced_at: RwLock<Option<tokio::time::Instant>>,
    pub(crate) sites_sync_info: RwLock<BTreeMap<String, Vec<String>>>,
}

impl SyncInfo {
    pub(crate) async fn reset(&self, site: impl Into<String>) {
        self.sites_sync_info.write().await.insert(site.into(), Vec::new());
    }

    pub(crate) async fn append(&self, site: impl AsRef<str>, msg: impl Into<String>) {
        if let Some(sync_info) = self.sites_sync_info.write().await.get_mut(site.as_ref()) {
            sync_info.push(format!("{} {}", time::OffsetDateTime::now_utc().format(&Iso8601::DATE_TIME_OFFSET).unwrap_or_else(|e| e.to_string()), msg.into()));
        }
    }
}

pub(crate) enum SyncCommand {
    SyncLogs,
}

impl TryFrom<String> for FileType {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "f" => Ok(Self::File),
            "d" => Ok(Self::Directory),
            _ => Err(format!("Invalid FileType `{value}`")),
        }
    }
}

impl TryFrom<&RpcValue> for LsFilesEntry {
    type Error = String;

    fn try_from(value: &RpcValue) -> Result<Self, Self::Error> {
        let shvproto::Value::List(list) = &value.value else {
            return Err("Expected List for LsFiles".to_string());
        };
        let mut list_iter = list.iter().fuse();
        let name = list_iter.next()
            .ok_or("Missing `file_name` field".to_string())
            .and_then(|v| if let shvproto::Value::String(file_name) = &v.value {
                Ok((**file_name).clone())
            } else {
                Err("Invalid type of `file_name` field".to_string())
            })?;
        let ftype = list_iter.next()
            .ok_or("Missing `file_type` field".to_string())
            .and_then(|v|
                if let shvproto::Value::String(file_type) = &v.value {
                    Ok((**file_type).clone())
                } else {
                    Err("Invalid type of `file_type` field".to_string())
                }
            )
            .and_then(FileType::try_from)?;
        let size = list_iter.next()
            .ok_or("Missing `size` field".to_string())
            .and_then(|v| if let shvproto::Value::Int(size) = v.value {
                Ok(size)
            } else {
                Err("Invalid type of `size` field".to_string())
            })?;
        Ok(Self {
            name,
            ftype,
            size,
        })
    }
}

fn to_string(v: impl ToString) -> String {
    v.to_string()
}

macro_rules! log_sync {
    ($app_state:expr, $site:expr, $msg:expr) => {
        log::info!("{}: {}", $site, $msg);
        $app_state.sync_info.append($site, $msg).await;
    };
}

trait SyncLogger: Clone {
    async fn reset(&self);
    async fn log(&self, msg: impl AsRef<str>);
}

#[derive(Clone)]
struct SyncSiteLogger {
    site: String,
    app_state: AppState<State>,
}

impl SyncLogger for SyncSiteLogger {
    async fn reset(&self) {
        self.app_state.sync_info.reset(&self.site).await;
    }

    async fn log(&self, msg: impl AsRef<str>) {
        log_sync!(self.app_state, &self.site, msg.as_ref());
    }
}

async fn sync_site(
    site_path: impl AsRef<str>,
    remote_journal_path: impl AsRef<str>,
    download_chunk_size: i64,
    client_cmd_tx: ClientCommandSender,
    app_state: AppState<State>,
    sync_logger: impl SyncLogger,
) -> Result<(), String>
{
    let (site_path, remote_journal_path) = (site_path.as_ref(), remote_journal_path.as_ref());
    // app_state.sync_info.reset(site_path).await;
    sync_logger.reset().await;

    let local_journal_path = join_path!(&app_state.config.journal_dir, site_path);
    // log_sync!(app_state, site_path, format!("Start syncing from {} to {}, download chunk size: {}", remote_journal_path, local_journal_path, download_chunk_size));
    sync_logger.log(format!("start syncing from {} to {}, download chunk size: {}", remote_journal_path, local_journal_path, download_chunk_size)).await;
    tokio::fs::create_dir_all(&local_journal_path).await.map_err(to_string)?;

    let file_list: Vec<LsFilesEntry> = RpcCall::new(remote_journal_path, "lsfiles")
        .exec(&client_cmd_tx)
        .await
        .map_err(to_string)?;
    log::trace!("  files: [{}]", file_list.iter().map(LsFilesEntry::to_string).collect::<Vec<_>>().join(","));

    let files_to_sync = futures::stream::iter(
            file_list.iter().filter(|f| matches!(f.ftype, FileType::File) && f.name.ends_with(".log2"))
        )
        .filter_map(|remote_file| async {
            let local_file_path = join_path!(&local_journal_path, &remote_file.name);
            match tokio::fs::metadata(&local_file_path).await {
                Ok(local_file) if local_file.is_file() => {
                    let local_size = local_file.len() as i64;
                    let sync_offset = match local_size.cmp(&remote_file.size) {
                        Ordering::Less => {
                            sync_logger.log(
                                format!("{}: will sync (remote size: {}, local size: {})",
                                &remote_file.name,
                                remote_file.size,
                                local_size)
                            ).await;
                            local_size
                        }
                        Ordering::Greater => {
                            sync_logger.log(
                                format!("{}: will be replaced (remote size: {}, local size: {})",
                                &remote_file.name,
                                remote_file.size,
                                local_size)
                            ).await;
                            0
                        }
                        Ordering::Equal => {
                            sync_logger.log(format!("{}: up-to-date", &remote_file.name)).await;
                            return None;
                        }
                    };
                    Some((remote_file.name.clone(), sync_offset, remote_file.size))
                }
                Ok(_not_a_file) => None,
                Err(_err) => {
                    sync_logger.log(
                        format!("{}: will sync (remote size: {}, local size: <not found>)",
                        &remote_file.name,
                        remote_file.size,
                        )
                    ).await;
                    Some((remote_file.name.clone(), 0, remote_file.size))
                }
            }
        })
        .collect::<Vec<_>>()
        .await;

    // Prepare sync directory
    // TODO

    // Sync from the remote to the sync directory
    for (file_name, sync_offset, file_size) in files_to_sync {
        sync_file(
            client_cmd_tx.clone(),
            join_path!(remote_journal_path, &file_name),
            join_path!(local_journal_path, &file_name),
            download_chunk_size,
            sync_offset,
            file_size,
            sync_logger.clone(),
        )
        .await
        .map_err(to_string)?;
    }

    // Move synced files from the sync directory to the journal directory
    // and trim the provisional log
    // TODO

    Ok(())
}

async fn sync_file(
    client_cmd_tx: ClientCommandSender,
    file_path_remote: impl AsRef<str>,
    file_path_local: impl AsRef<str>,
    download_chunk_size: i64,
    sync_offset: i64,
    file_size: i64,
    sync_logger: impl SyncLogger,
) -> Result<(), String>
{
    let file_path_remote = file_path_remote.as_ref();
    let file_path_local = file_path_local.as_ref();

    sync_logger.log(format!(
            "{}: starting to sync, start offset: {}, file size: {}",
            file_path_remote,
            sync_offset,
            file_size,
    )).await;

    let mut local_file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .write(true)
        .open(file_path_local)
        .await
        .map_err(to_string)?;

    enum ReadApi { List, Map }
    let read_api = RpcCall::new(file_path_remote, METH_DIR)
        .param("sha1")
        .exec(&client_cmd_tx)
        .await
        .map(|v: RpcValue| if v.is_imap() { ReadApi::List } else { ReadApi::Map })
        .map_err(|e| format!("Cannot get read param API for {file_path_remote}: {e}"))?;

    let mut sync_offset = sync_offset;
    let mut remaining_bytes = file_size - sync_offset;
    while sync_offset < file_size {
        let sync_size = remaining_bytes.max(0).min(download_chunk_size);

        // log::info!("  downloading chunk, offset: {}, size: {}", sync_offset, sync_size);

        let param: RpcValue = match read_api {
            ReadApi::List => shvproto::make_list!(sync_offset, sync_size).into(),
            ReadApi::Map => shvproto::make_map!(
                "offset" => sync_offset,
                "size" => sync_size,
            ).into(),
        };

        let chunk: shvproto::Blob = RpcCall::new(file_path_remote, METH_READ)
            .param(param)
            .exec(&client_cmd_tx)
            .await
            .map_err(to_string)?;

        local_file.write_all(&chunk)
          .await
          .map_err(to_string)?;

        let chunk_len = chunk.len() as i64;
        sync_offset += chunk_len;
        remaining_bytes -= chunk_len;

        sync_logger.log(format!(
                "{}: got chunk of size: {}, remaining: {} ({:.2})",
                file_path_remote,
                chunk_len,
                remaining_bytes,
                (sync_offset as f64 / file_size as f64) * 100.0,
        )).await;
    }
    sync_logger.log(format!("{}: successfully synced", file_path_remote)).await;
    Ok(())
}

const MAX_SYNC_TASKS_DEFAULT: usize = 8;

pub(crate) async fn sync_task(
    client_cmd_tx: ClientCommandSender,
    _client_evt_rx: ClientEventsReceiver,
    app_state: AppState<State>,
    mut sync_cmd_rx: UnboundedReceiver<SyncCommand>,
)
{

    while let Some(cmd) = sync_cmd_rx.next().await {
        match cmd {
            SyncCommand::SyncLogs => {
                log::info!("Sync logs start");
                let max_sync_tasks = app_state.config.max_sync_tasks.unwrap_or(MAX_SYNC_TASKS_DEFAULT);
                let semaphore = Arc::new(Semaphore::new(max_sync_tasks));
                let mut sync_tasks = vec![];
                let sync_start = tokio::time::Instant::now();
                let SitesData { sites_info, sub_hps } = app_state.sites_data.read().await.clone();
                for (site_path, site_info) in sites_info.iter() {
                    let sub_hp = sub_hps
                        .get(&site_info.sub_hp)
                        .unwrap_or_else(|| panic!("Sub HP for site {site_path} should be set"));
                    let permit = semaphore
                        .clone()
                        .acquire_owned()
                        .await
                        .unwrap_or_else(|e| panic!("Cannot acquire semaphore: {e}"));
                    match sub_hp {
                        SubHpInfo::Normal { sync_path, download_chunk_size } => {
                            let site_suffix = shvrpc::util::strip_prefix_path(site_path, &site_info.sub_hp)
                                .unwrap_or_else(|| panic!("Site {site_path} should be under its sub HP {}", site_info.sub_hp));
                            let remote_journal_path = join_path!("shv", &site_info.sub_hp, sync_path, site_suffix);
                            let client_cmd_tx = client_cmd_tx.clone();
                            let site_path = site_path.clone();
                            let download_chunk_size = *download_chunk_size;
                            let app_state = app_state.clone();
                            let sync_task = tokio::spawn(async move {
                                let sync_logger = SyncSiteLogger {
                                    site: site_path.clone(),
                                    app_state: app_state.clone()
                                };
                                let sync_result = sync_site(
                                    &site_path,
                                    remote_journal_path,
                                    download_chunk_size,
                                    client_cmd_tx,
                                    app_state,
                                    sync_logger.clone()
                                ).await;
                                if let Err(err) = sync_result {
                                    sync_logger.log(format!("site sync error: {err}")).await;
                                }
                                drop(permit);
                            });
                                sync_tasks.push(sync_task);
                        }
                        SubHpInfo::Legacy { getlog_path } => {
                            log::info!("Syncing {site_path} via getLog from {getlog_path}");
                            // TODO
                        }
                        SubHpInfo::PushLog => {
                            // TODO
                        }
                    }
                }
                futures::future::join_all(sync_tasks).await;
                log::info!("Sync logs done in {} s", sync_start.elapsed().as_secs());
            }
        }
    }
}
