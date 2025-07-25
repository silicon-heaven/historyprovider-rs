use std::path::Path;
use std::sync::Arc;

use futures::channel::mpsc::UnboundedReceiver;
use futures::channel::oneshot::Sender as OneshotSender;
use futures::io::BufReader;
use futures::stream::{FuturesUnordered, SelectAll};
use futures::{select, StreamExt};
use log::{debug, error, info, warn};
use shvclient::client::ShvApiVersion;
use shvclient::{AppState, ClientEventsReceiver};
use shvproto::DateTime as ShvDateTime;
use shvrpc::metamethod::AccessLevel;
use shvrpc::{join_path, RpcMessageMetaTags};
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::datachange::DataChange;
use crate::journalentry::JournalEntry;
use crate::journalrw::{JournalReaderLog2, JournalWriterLog2, VALUE_FLAG_PROVISIONAL_BIT, VALUE_FLAG_SPONTANEOUS_BIT};
use crate::util::{get_files, is_log2_file, subscribe, subscription_prefix_path};
use crate::{ClientCommandSender, State, Subscriber};

use shvclient::clientnode::{find_longest_path_prefix, SIG_CHNG};
const SIG_CMDLOG: &str = "cmdlog";

pub(crate) enum DirtyLogCommand {
    // On the client connect
    Subscribe(ShvApiVersion),
    Trim {
        site: String
    },
    Get {
        site: String,
        response_tx: OneshotSender<Vec<JournalEntry>>,
    }
}

pub(crate) async fn dirty_log_task(
    client_cmd_tx: ClientCommandSender,
    _client_evt_rx: ClientEventsReceiver,
    app_state: AppState<State>,
    mut cmd_rx: UnboundedReceiver<DirtyLogCommand>,
)
{
    // TODO:
    // on mntchng:
    //   sync logs from the site (in the scope of an upper module)
    // on client connect:
    //   subscribe mntchng (in the scope of an upper module)
    //   subscribe events
    //   sync logs from all sites (in the scope of an upper module)
    // let mut mntchng_subscribers = SelectAll::<Subscriber>::default(); // Put to the upper module

    let mut site_paths = Arc::default();
    let mut subscribers = SelectAll::<Subscriber>::default();
    loop {
        select! {
            command = cmd_rx.select_next_some() => match command {
                DirtyLogCommand::Subscribe(shv_api_version) => {
                    info!("Subscribing signals on the devices");
                    site_paths = app_state
                        .sites_data
                        .read()
                        .await
                        .sites_info
                        .clone();
                    subscribers = site_paths
                        .keys()
                        .flat_map(|path| {
                            let shv_path = join_path!("shv", path);
                            let sub_chng = subscribe(&client_cmd_tx, subscription_prefix_path(&shv_path, &shv_api_version), SIG_CHNG);
                            let sub_cmdlog = subscribe(&client_cmd_tx, subscription_prefix_path(&shv_path, &shv_api_version), SIG_CMDLOG);
                            [sub_chng, sub_cmdlog]
                        })
                        .collect::<FuturesUnordered<_>>()
                        .collect::<SelectAll<_>>()
                        .await;

                    debug!("Device subscriptions:\n{}", subscribers.iter()
                        .map(Subscriber::path_signal)
                        .map(|(path, signal)| format!("{path}:{signal}"))
                        .collect::<Vec<_>>()
                        .join("\n")
                    );
                }
                DirtyLogCommand::Trim { site } => {
                    info!("Trim dirty log start, site: {site}");
                    // Get the latest entry from the site log and remove all entries up to that
                    // entry from the dirty log
                    let dirty_log_path = Path::new(&app_state.config.journal_dir).join(&site).join("dirtylog");
                    let dirty_log_file = match tokio::fs::File::open(&dirty_log_path).await {
                        Ok(file) => file,
                        Err(err) => {
                            if err.kind() == std::io::ErrorKind::NotFound {
                                info!("Trim dirty log done, no dirty log file for the site");
                            } else {
                                error!("Cannot trim dirty log. Cannot open file {file_path}: {err}", file_path = dirty_log_path.to_string_lossy());
                            }
                            continue;
                        }
                    };

                    let newest_log_path = {
                        let mut log_files = match get_files(&Path::new(&app_state.config.journal_dir).join(&site), is_log2_file).await {
                            Ok(files) => files,
                            Err(err) => {
                                error!("Cannot trim dirty log. Cannot read journal dir entries: {err}");
                                continue;
                            }
                        };
                        log_files.sort_by_key(|entry| entry.file_name());
                        match log_files.last() {
                            Some(newest_log_file) => newest_log_file.path(),
                            None => {
                                info!("Trim dirty log done, no synced files");
                                continue
                            }
                        }
                    };
                    let newest_log_file = match tokio::fs::File::open(&newest_log_path).await {
                        Ok(file) => file,
                        Err(err) => {
                            if err.kind() == std::io::ErrorKind::NotFound {
                                info!("Trim dirty log done, no synced files");
                            } else {
                                error!("Cannot trim dirty log. Cannot open file {file_path}: {err}",
                                    file_path = newest_log_path.to_string_lossy()
                                );
                            }
                            continue;
                        }
                    };

                    // Get the latest entry from the latest log
                    let reader = JournalReaderLog2::new(BufReader::new(newest_log_file.compat()));
                    let Some(latest_entry) = reader.fold(None, |_, entry| { let entry = entry.ok(); async { entry }}).await else {
                        info!("Trim dirty log done, no journal entries in synced files");
                        continue;
                    };

                    // Remove all entries older than the latest entry from the dirty log
                    let reader = JournalReaderLog2::new(BufReader::new(dirty_log_file.compat()));
                    let trimmed_log = reader.filter_map(|entry| {
                        let passed_entry = entry
                            .as_ref()
                            .ok()
                            .filter(|entry| entry.epoch_msec >= latest_entry.epoch_msec)
                            .cloned();
                        async move { passed_entry }
                    })
                    .collect::<Vec<_>>()
                    .await;

                    // Write the dirty log
                    let dirty_log_file = match tokio::fs::OpenOptions::new()
                        .write(true)
                        .open(&dirty_log_path)
                        .await {
                            Ok(file) => file,
                            Err(err) => {
                                error!("Cannot trim dirty log. Cannot open file {file_path} for write: {err}",
                                    file_path = dirty_log_path.to_string_lossy()
                                );
                                continue;
                            }
                        };
                    let mut writer = JournalWriterLog2::new(dirty_log_file.compat());
                    for entry in &trimmed_log {
                        writer.append(entry)
                            .await
                            .unwrap_or_else(|err|
                                error!("Cannot write a journal entry to dirty log {log_file}: {err}",
                                    log_file = dirty_log_path.to_string_lossy()
                                )
                            );
                    }
                    info!("Trim dirty log done");

                }
                DirtyLogCommand::Get { site, response_tx } => {
                    // Load the dirty log and return it in the response channel
                    let dirty_log_path =  Path::new(&app_state.config.journal_dir).join(site).join("dirtylog");
                    let res = match tokio::fs::File::open(&dirty_log_path).await {
                        Ok(file) => {
                            let reader = JournalReaderLog2::new(BufReader::new(file.compat())).enumerate();
                            reader
                                .filter_map(|(entry_no, entry_res)| {
                                    let entry = entry_res.inspect_err(|err|
                                        warn!("Invalid journal entry no. {entry_no} in dirty log at {log_path}: {err}",
                                            log_path = dirty_log_path.to_string_lossy()
                                        )
                                    )
                                    .ok();
                                    async { entry }
                                })
                                .collect::<Vec<_>>()
                                .await
                        }
                        Err(err) => {
                            if err.kind() != std::io::ErrorKind::NotFound {
                                error!("Cannot open {log_path}: {err}", log_path = dirty_log_path.to_string_lossy());
                            }
                            Vec::new()
                        }
                    };
                    response_tx.send(res).unwrap_or_default();
                }
            },
            notification = subscribers.select_next_some() => {
                // Parse the notification
                let msg = match notification.to_rpcmesage() {
                    Ok(msg) => msg,
                    Err(err) => {
                        warn!("Ignoring wrong RpcFrame: {err}");
                        continue;
                    }
                };
                let signal = msg.method().unwrap_or_default().to_string();
                let path = msg.shv_path().unwrap_or_default().to_string();
                let param = msg.param().unwrap_or_default();

                let Some(stripped_path) = path.strip_prefix("shv/") else {
                    continue;
                };
                let Some((site_path, property_path)) = find_longest_path_prefix(&*site_paths, stripped_path) else {
                    continue;
                };
                // Append to the site's dirty log
                let dirty_log_path = Path::new(&app_state.config.journal_dir).join(site_path).join("dirtylog");
                let dirty_log_file = match tokio::fs::OpenOptions::new()
                    .append(true)
                    .open(&dirty_log_path)
                    .await {
                        Ok(file) => file,
                        Err(err) => {
                            error!("Cannot append a notification to dirty log. Cannot open file {file_path} for write: {err}",
                                file_path = dirty_log_path.to_string_lossy()
                            );
                            continue;
                        }
                    };
                let mut writer = JournalWriterLog2::new(dirty_log_file.compat());
                let data_change = DataChange::from(param.clone());
                let entry = JournalEntry {
                    epoch_msec: data_change.date_time.unwrap_or_else(ShvDateTime::now).epoch_msec(),
                    path: property_path.to_string(),
                    signal,
                    source: Default::default(),
                    value: data_change.value,
                    access_level: AccessLevel::Read as _,
                    short_time: data_change.short_time.unwrap_or(-1),
                    user_id: Default::default(),
                    repeat: data_change.value_flags & (1 << VALUE_FLAG_SPONTANEOUS_BIT) == 0,
                    provisional: true, // data_change.value_flags & (1 << VALUE_FLAG_PROVISIONAL_BIT) != 0,
                };
                writer.append(&entry)
                    .await
                    .unwrap_or_else(|err|
                        error!("Cannot append a notification to dirty log {log_file}: {err}",
                            log_file = dirty_log_path.to_string_lossy()
                        )
                    );
                }
            complete => break,
        }
    }
}
