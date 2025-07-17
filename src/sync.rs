use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures::io::{BufReader, BufWriter};
use futures::StreamExt;
use shvclient::client::RpcCall;
use shvclient::clientnode::METH_DIR;
use shvclient::{AppState, ClientEventsReceiver};
use shvproto::RpcValue;
use shvrpc::join_path;
use time::format_description::well_known::Iso8601;
use tokio::io::AsyncWriteExt;
use tokio::sync::{RwLock, Semaphore};
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::journalentry::JournalEntry;
use crate::journalrw::{GetLog2Params, JournalReaderLog2, JournalWriterLog2, Log2Reader};
use crate::sites::{SitesData, SubHpInfo};
use crate::tree::{FileType, LsFilesEntry, METH_READ};
use crate::util::{get_files, is_log2_file};
use crate::{ClientCommandSender, State};

#[derive(Default)]
pub(crate) struct SyncInfo {
    pub(crate) last_sync_timestamp: RwLock<Option<tokio::time::Instant>>,
    pub(crate) sites_sync_info: RwLock<BTreeMap<String, Vec<String>>>,
}

impl SyncInfo {
    pub(crate) async fn reset(&self, site: impl Into<String>) {
        self.sites_sync_info.write().await.insert(site.into(), Vec::new());
    }

    pub(crate) async fn append(&self, site: impl AsRef<str>, msg: impl Into<String>) {
        if let Some(sync_info) = self.sites_sync_info.write().await.get_mut(site.as_ref()) {
            sync_info.push(msg.into());
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

trait SyncLogger: Clone {
    fn log(&self, level: log::Level, msg: impl AsRef<str>);
}

enum LogEvent {
    Reset { site: String },
    Append { site: String, message: String },
}

#[derive(Clone)]
struct SyncSiteLogger {
    site: String,
    logger_tx: UnboundedSender<LogEvent>,
}

impl SyncSiteLogger {
    fn new(site: impl Into<String>, logger_tx: UnboundedSender<LogEvent>) -> Self {
        let site = site.into();
        logger_tx
            .unbounded_send(LogEvent::Reset { site: site.clone() })
            .unwrap_or_else(|e| log::error!("Couldn't send Reset log event throught the channel: {e}"));
        Self {
            site,
            logger_tx,
        }
    }
}

impl SyncLogger for SyncSiteLogger {
    fn log(&self, level: log::Level, msg: impl AsRef<str>) {
        let msg = format!("{} {}",
            time::OffsetDateTime::now_utc().format(&Iso8601::DATE_TIME_OFFSET).unwrap_or_else(|e| e.to_string()),
            msg.as_ref()
        );
        let log_msg = format!("{}: {}", &self.site, &msg);
        match level {
            log::Level::Error => log::error!("{log_msg}"),
            log::Level::Warn => log::warn!("{log_msg}"),
            log::Level::Info => log::info!("{log_msg}"),
            log::Level::Debug => log::debug!("{log_msg}"),
            log::Level::Trace => log::trace!("{log_msg}"),
        }
        self.logger_tx
            .unbounded_send(LogEvent::Append { site: self.site.clone(), message: log_msg })
            .unwrap_or_else(|e| log::error!("Couldn't send a message throught the channel: {e}"));
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
    let local_journal_path = Path::new(&app_state.config.journal_dir).join(site_path);
    sync_logger.log(
        log::Level::Info,
        format!("start syncing from {} to {}, download chunk size: {}",
            remote_journal_path,
            local_journal_path.to_string_lossy(),
            download_chunk_size
        )
    );
    tokio::fs::create_dir_all(&local_journal_path)
        .await
        .map_err(|e| format!("Cannot create journal directory at {}: {e}", local_journal_path.to_string_lossy()))?;

    let file_list: Vec<LsFilesEntry> = RpcCall::new(remote_journal_path, "lsfiles")
        .exec(&client_cmd_tx)
        .await
        .map_err(to_string)?;

    let files_to_sync = futures::stream::iter(
            file_list.iter().filter(|f| matches!(f.ftype, FileType::File) && f.name.ends_with(".log2"))
        )
        .filter_map(|remote_file| async {
            let local_file_path = local_journal_path.join(&remote_file.name);
            match tokio::fs::metadata(&local_file_path).await {
                Ok(local_file) if local_file.is_file() => {
                    let local_size = local_file.len() as i64;
                    let sync_offset = match local_size.cmp(&remote_file.size) {
                        Ordering::Less => {
                            sync_logger.log(
                                log::Level::Info,
                                format!("{}: will sync (remote size: {}, local size: {})",
                                &remote_file.name,
                                remote_file.size,
                                local_size)
                            );
                            local_size
                        }
                        Ordering::Greater => {
                            sync_logger.log(
                                log::Level::Info,
                                format!("{}: will be replaced (remote size: {}, local size: {})",
                                &remote_file.name,
                                remote_file.size,
                                local_size)
                            );
                            0
                        }
                        Ordering::Equal => {
                            sync_logger.log(log::Level::Info, format!("{}: up-to-date", &remote_file.name));
                            return None;
                        }
                    };
                    Some((remote_file.name.clone(), sync_offset, remote_file.size))
                }
                Ok(_not_a_file) => None,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    sync_logger.log(
                        log::Level::Info,
                        format!("{}: will sync (remote size: {}, local size: <not found>)",
                        &remote_file.name,
                        remote_file.size,
                        )
                    );
                    Some((remote_file.name.clone(), 0, remote_file.size))
                }
                Err(err) => {
                    sync_logger.log(
                        log::Level::Info,
                        format!("{}: will try to sync (remote size: {}, local size: <I/O error: {}>)",
                        &remote_file.name,
                        remote_file.size,
                        err,
                        )
                    );
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
            local_journal_path.join(&file_name),
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
    file_path_local: impl AsRef<Path>,
    download_chunk_size: i64,
    sync_offset: i64,
    file_size: i64,
    sync_logger: impl SyncLogger,
) -> Result<(), String>
{
    let file_path_remote = file_path_remote.as_ref();
    let file_path_local = file_path_local.as_ref();

    sync_logger.log(log::Level::Info,
        format!("{file_path_remote}: starting to sync, start offset: {sync_offset}, file size: {file_size}",
    ));

    let mut local_file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .write(true)
        .open(file_path_local)
        .await
        .map_err(|e| format!("Cannot open {}: {e}", file_path_local.to_string_lossy()))?;

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
          .map_err(|e| format!("Cannot write to {}: {e}", file_path_local.to_string_lossy()))?;

        let chunk_len = chunk.len() as i64;
        sync_offset += chunk_len;
        remaining_bytes -= chunk_len;

        sync_logger.log(log::Level::Info,
            format!("{}: got chunk of size: {}, remaining: {} ({:.2})",
                file_path_remote,
                chunk_len,
                remaining_bytes,
                (sync_offset as f64 / file_size as f64) * 100.0,
        ));
    }
    sync_logger.log(log::Level::Info, format!("{file_path_remote}: successfully synced"));
    Ok(())
}

async fn sync_site_legacy(
    site_path: impl AsRef<str>,
    getlog_path: impl AsRef<str>,
    client_cmd_tx: ClientCommandSender,
    app_state: AppState<State>,
    sync_logger: impl SyncLogger,
) -> Result<(), String>
{
    let (site_path, getlog_path) = (site_path.as_ref(), getlog_path.as_ref());
    let local_journal_path = Path::new(&app_state.config.journal_dir).join(site_path);
    sync_logger.log(
        log::Level::Info,
        format!("start syncing from {} to {} via getLog", getlog_path, local_journal_path.to_string_lossy())
    );
    tokio::fs::create_dir_all(&local_journal_path)
        .await
        .map_err(|e| format!("Cannot create journal directory at {}: {e}", local_journal_path.to_string_lossy()))?;

    // Get the newest file if any
    let mut log_files = get_files(&local_journal_path, is_log2_file).await?;
    log_files.sort_by_key(|entry| entry.file_name());

    let newest_log = loop {
        match log_files.last() {
            Some(newest_file) => {
                let newest_file_path = newest_file.path();
                let file = tokio::fs::File::open(std::path::Path::new(&newest_file_path))
                    .await
                    .map_err(|err| format!("Cannot open journal file {}: {}", newest_file_path.to_string_lossy(), err))?;
                let entries = JournalReaderLog2::new(BufReader::new(file.compat()));
                let entries = entries
                    .filter_map(|entry| {
                        let entry = entry
                            .inspect_err(|err|
                                sync_logger.log(
                                    log::Level::Warn,
                                    format!("Skipping wrong journal entry in {}: {err}",
                                        newest_file_path.to_string_lossy())
                                )
                            )
                            .ok();
                        async { entry }
                    })
                    .collect::<Vec<_>>()
                    .await;
                if !entries.is_empty() {
                    break Some((newest_file_path, entries));
                }
                // Remove the file if it doesn't contain any valid entries
                tokio::fs::remove_file(&newest_file_path)
                    .await
                    .map_err(|err| format!("Cannot remove empty journal file {}: {}", newest_file_path.to_string_lossy(), err))?;
            }
            None => break None,
        }
    };

    const LOG_FILE_RECORD_COUNT_LIMIT: usize = 100000;
    const GETLOG_SINCE_DAYS_DEFAULT: i64 = 365;
    const RECORD_COUNT_LIMIT: i64 = 10000;

    fn msec_to_log2_filename(msec: i64) -> String {
        shvproto::DateTime::from_epoch_msec(msec)
            .to_chrono_datetime()
            .format("%Y-%m-%dT%H:%M:%S.log2")
            .to_string()
    }

    let (mut getlog_params, mut log_file_path, mut log_file_entries) = match newest_log {
        Some((newest_log_file, newest_log_entries)) => {
            let last_log_entry_msec = newest_log_entries.last().expect("The newest log is not empty").epoch_msec;
            let since = shvproto::DateTime::from_epoch_msec(last_log_entry_msec + 1);
            if newest_log_entries.len() > LOG_FILE_RECORD_COUNT_LIMIT {
                // Start with a new file if it already contains too many records
                sync_logger.log(
                    log::Level::Info,
                    format!("sync will create a new file since {}", since.to_iso_string())
                );
                let params = GetLog2Params {
                    since: Some(since),
                    until: None,
                    path_pattern: None,
                    with_paths_dict: true,
                    with_snapshot: true,
                    record_count_limit: RECORD_COUNT_LIMIT,
                };
                (params, None, Vec::new())
            } else {
                sync_logger.log(
                    log::Level::Info,
                    format!("sync will append to {}", newest_log_file.to_string_lossy())
                );
                let params = GetLog2Params {
                    since: Some(since),
                    until: None,
                    path_pattern: None,
                    with_paths_dict: true,
                    with_snapshot: false,
                    record_count_limit: RECORD_COUNT_LIMIT,
                };
                (params, Some(newest_log_file), newest_log_entries)
            }
        },
        None => {
            let since = shvproto::DateTime::now().add_days(-GETLOG_SINCE_DAYS_DEFAULT);
            sync_logger.log(
                log::Level::Info,
                format!("sync to a new journal directory since {}", since.to_iso_string())
            );
            let params = GetLog2Params {
                since: Some(since),
                until: None,
                path_pattern: None,
                with_paths_dict: true,
                with_snapshot: true,
                record_count_limit: RECORD_COUNT_LIMIT,
            };
            (params, None, Vec::new())
        }
    };

    enum JournalPath {
        Dir(PathBuf),
        File(PathBuf),
    }
    async fn write_journal(journal_path: JournalPath, log_entries: &Vec<JournalEntry>, sync_logger: &impl SyncLogger) -> Result<(), Box<dyn std::error::Error>> {
        let Some(first_entry_msec) = log_entries.first().map(|entry| entry.epoch_msec) else {
            return Ok(());
        };
        let journal_file_path = match journal_path {
            JournalPath::Dir(mut path) => {
                path.push(msec_to_log2_filename(first_entry_msec));
                path
            }
            JournalPath::File(path) => path,
        };
        sync_logger.log(log::Level::Info, format!("Write {} journal entries to {}", log_entries.len(), journal_file_path.to_string_lossy()));
        let journal_file = tokio::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&journal_file_path)
            .await
            .map_err(|err| format!("Cannot open journal file {} for writing: {}", journal_file_path.to_string_lossy(), err))?;
        let mut writer = JournalWriterLog2::new(BufWriter::new(journal_file.compat()));
        for entry in log_entries {
            writer.append(entry)
                .await
                .map_err(|err| format!("Cannot append to journal file {}: {}", journal_file_path.to_string_lossy(), err))?;
        }
        Ok(())
    }

    loop {
        sync_logger.log(
            log::Level::Info,
            format!("Calling getLog, target: {}, since: {:?}, snapshot: {}",
                local_journal_path.to_string_lossy(),
                getlog_params.since.map(|dt| dt.to_iso_string()), getlog_params.with_snapshot)
        );
        let log: RpcValue = RpcCall::new(getlog_path, "getLog")
            .param(getlog_params.clone())
            .timeout(std::time::Duration::from_secs(60))
            .exec(&client_cmd_tx)
            .await
            .map_err(to_string)?;

        let log_rd = Log2Reader::new(log).map_err(to_string)?;
        let mut log_entries = log_rd.filter_map(Result::ok).collect::<Vec<_>>();

        let last_entry_ms = log_entries
            .last()
            .map(|entry| entry.epoch_msec)
            .and_then(|last_entry_ms| {
                // Skip all entries from the last ms, and get them on the next getLog call
                log_entries.retain(|entry| entry.epoch_msec != last_entry_ms);
                (!log_entries.is_empty()).then_some(last_entry_ms)
            });


        let Some(last_entry_ms) = last_entry_ms else {
            write_journal(log_file_path
                .map_or_else(
                    || JournalPath::Dir(local_journal_path.clone()),
                    JournalPath::File
                ),
                &log_file_entries,
                &sync_logger)
                .await
                .map_err(to_string)?;
            // No more data, the sync is finished
            break;
        };

        log_file_entries.append(&mut log_entries);

        getlog_params.since = Some(shvproto::DateTime::from_epoch_msec(last_entry_ms));

        if log_file_entries.len() > LOG_FILE_RECORD_COUNT_LIMIT {
            write_journal(log_file_path
                .map_or_else(|| JournalPath::Dir(local_journal_path.clone()), JournalPath::File),
                &log_file_entries,
                &sync_logger)
                .await
                .map_err(to_string)?;
            // Start a new file
            log_file_entries.clear();
            log_file_path = None;
            getlog_params.with_snapshot = true;
        } else {
            getlog_params.with_snapshot = false;
        }
    }
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

    let (logger_tx, mut logger_rx) = futures::channel::mpsc::unbounded();
    let logger_task = tokio::task::spawn({
        let app_state = app_state.clone();
        async move {
            while let Some(log_event) = logger_rx.next().await {
                match log_event {
                    LogEvent::Reset { site } =>
                        app_state.sync_info.reset(site).await,
                    LogEvent::Append { site, message } =>
                        app_state.sync_info.append(site, message).await,
                }
            }
        }
    });

    while let Some(cmd) = sync_cmd_rx.next().await {
        match cmd {
            SyncCommand::SyncLogs => {
                log::info!("Sync logs start");
                app_state.sync_info.last_sync_timestamp.write().await.replace(tokio::time::Instant::now());
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
                    let client_cmd_tx = client_cmd_tx.clone();
                    let site_path = site_path.clone();
                    let app_state = app_state.clone();
                    let logger_tx = logger_tx.clone();
                    match sub_hp {
                        SubHpInfo::Normal { sync_path, download_chunk_size } => {
                            let site_suffix = shvrpc::util::strip_prefix_path(&site_path, &site_info.sub_hp)
                                .unwrap_or_else(|| panic!("Site {site_path} should be under its sub HP {}", site_info.sub_hp));
                            let remote_journal_path = join_path!("shv", &site_info.sub_hp, sync_path, site_suffix);
                            let download_chunk_size = *download_chunk_size;
                            let sync_task = tokio::spawn(async move {
                                let sync_logger = SyncSiteLogger::new(&site_path, logger_tx);
                                let sync_result = sync_site(
                                    site_path,
                                    remote_journal_path,
                                    download_chunk_size,
                                    client_cmd_tx,
                                    app_state,
                                    sync_logger.clone()
                                ).await;
                                if let Err(err) = sync_result {
                                    sync_logger.log(log::Level::Error, format!("site sync error: {err}"));
                                }
                                sync_logger.log(log::Level::Info, "syncing done");
                                drop(permit);
                            });
                            sync_tasks.push(sync_task);
                        }
                        SubHpInfo::Legacy { getlog_path } => {
                            let site_suffix = shvrpc::util::strip_prefix_path(&site_path, &site_info.sub_hp)
                                .unwrap_or_else(|| panic!("Site {site_path} should be under its sub HP {}", site_info.sub_hp));
                            let remote_getlog_path = join_path!("shv", &site_info.sub_hp, getlog_path, site_suffix);
                            let sync_task = tokio::spawn(async move {
                                let sync_logger = SyncSiteLogger::new(&site_path, logger_tx);
                                let sync_result = sync_site_legacy(
                                    site_path,
                                    remote_getlog_path,
                                    client_cmd_tx,
                                    app_state,
                                    sync_logger.clone()
                                ).await;
                                if let Err(err) = sync_result {
                                    sync_logger.log(log::Level::Error, format!("site sync error: {err}"));
                                }
                                sync_logger.log(log::Level::Info, "syncing done");
                                drop(permit);
                            });
                            sync_tasks.push(sync_task);
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
    logger_task.abort();
}
