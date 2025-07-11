use std::path::Path;

use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures::io::BufReader;
use futures::stream::{FuturesUnordered, SelectAll};
use futures::{select, StreamExt};
use log::{debug, error, info, warn};
use shvclient::client::ShvApiVersion;
use shvclient::{AppState, ClientEventsReceiver};
use shvrpc::join_path;
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::journalentry::JournalEntry;
use crate::journalrw::{JournalReaderLog2, JournalWriterLog2};
use crate::util::{get_files, is_log2_file, subscribe, subscription_prefix_path};
use crate::{ClientCommandSender, State, Subscriber};

pub(crate) enum ProvisionalLogCommand {
    // On the client connect
    SubscribeNotifications(ShvApiVersion),
    TrimLog {
        site: String
    },
    GetLog {
        site: String,
        response_tx: UnboundedSender<Vec<JournalEntry>>,
    }
}

pub(crate) async fn provisional_log_task(
    client_cmd_tx: ClientCommandSender,
    _client_evt_rx: ClientEventsReceiver,
    app_state: AppState<State>,
    mut cmd_rx: UnboundedReceiver<ProvisionalLogCommand>,
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

    let mut subscribers = SelectAll::<Subscriber>::default();
    loop {
        select! {
            command = cmd_rx.select_next_some() => match command {
                ProvisionalLogCommand::SubscribeNotifications(shv_api_version) => {
                    // Subscribe all(?) signals on the devices
                    info!("Subscribing signals on the devices");
                    let site_paths = app_state
                        .sites_data
                        .read()
                        .await
                        .sites_info
                        .keys()
                        .cloned()
                        .collect::<Vec<_>>();
                    subscribers = site_paths
                        .into_iter()
                        .map(|p| subscribe(&client_cmd_tx, subscription_prefix_path(join_path!("shv", p), &shv_api_version), "*"))
                        .collect::<FuturesUnordered<_>>()
                        .collect::<SelectAll<_>>()
                        .await;

                    debug!("Device subscriptions:\n{}", subscribers.iter()
                        .map(Subscriber::path_signal)
                        .map(|(path, signal)| format!("{}:{}", path, signal))
                        .collect::<Vec<_>>()
                        .join("\n")
                    );
                }
                ProvisionalLogCommand::TrimLog { site } => {
                    info!("Trim provisional log start, site: {site}");
                    // Get the latest entry from the site log and remove all entries up to that
                    // entry from the provisional log
                    let provisional_log_path = Path::new(&app_state.config.journal_dir).join(&site).join("provisionallog");
                    let provisional_log_file = match tokio::fs::File::open(&provisional_log_path).await {
                        Ok(file) => file,
                        Err(err) => {
                            if err.kind() == std::io::ErrorKind::NotFound {
                                info!("Trim provisional log done, no provisional log file for the site");
                            } else {
                                error!("Cannot trim provisional log. Cannot open file {file_path}: {err}", file_path = provisional_log_path.to_string_lossy());
                            }
                            continue;
                        }
                    };

                    let newest_log_path = {
                        let mut log_files = match get_files(&Path::new(&app_state.config.journal_dir).join(&site), is_log2_file).await {
                            Ok(files) => files,
                            Err(err) => {
                                error!("Cannot trim provisional log. Cannot read journal dir entries: {err}");
                                continue;
                            }
                        };
                        log_files.sort_by_key(|entry| entry.file_name());
                        match log_files.last() {
                            Some(newest_log_file) => newest_log_file.path(),
                            None => {
                                info!("Trim provisional log done, no synced files");
                                continue
                            }
                        }
                    };
                    let newest_log_file = match tokio::fs::File::open(&newest_log_path).await {
                        Ok(file) => file,
                        Err(err) => {
                            if err.kind() == std::io::ErrorKind::NotFound {
                                info!("Trim provisional log done, no synced files");
                            } else {
                                error!("Cannot trim provisional log. Cannot open file {file_path}: {err}",
                                    file_path = newest_log_path.to_string_lossy()
                                );
                            }
                            continue;
                        }
                    };

                    // Get the latest entry from the latest log
                    let reader = JournalReaderLog2::new(BufReader::new(newest_log_file.compat()));
                    let Some(latest_entry) = reader.fold(None, |_, entry| async { entry.ok() }).await else {
                        info!("Trim provisional log done, no journal entries in synced files");
                        continue;
                    };

                    // Remove all entries older than the latest entry from the provisional log
                    let reader = JournalReaderLog2::new(BufReader::new(provisional_log_file.compat()));
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

                    // Write the provisional log
                    let provisional_log_file = match tokio::fs::OpenOptions::new()
                        .write(true)
                        .open(&provisional_log_path)
                        .await {
                            Ok(file) => file,
                            Err(err) => {
                                error!("Cannot trim provisional log. Cannot open file {file_path} for write: {err}",
                                    file_path = provisional_log_path.to_string_lossy()
                                );
                                continue;
                            }
                        };
                    let mut writer = JournalWriterLog2::new(provisional_log_file.compat());
                    for entry in &trimmed_log {
                        writer.append(entry)
                            .await
                            .unwrap_or_else(|err|
                                error!("Cannot write a journal entry to provisional log {log_file}: {err}",
                                    log_file = provisional_log_path.to_string_lossy()
                                )
                            );
                    }
                    info!("Trim provisional log done");

                }
                ProvisionalLogCommand::GetLog { site, response_tx } => {
                    // Load the provisional log and return it in the response channel
                    let provisional_log_path =  Path::new(&app_state.config.journal_dir).join(site).join("provisionallog");
                    let res = match tokio::fs::File::open(&provisional_log_path).await {
                        Ok(file) => {
                            let mut reader = JournalReaderLog2::new(BufReader::new(file.compat())).enumerate();
                            reader
                                .filter_map(|(entry_no, entry_res)| {
                                    let entry = entry_res.inspect_err(|err|
                                        warn!("Invalid journal entry no. {entry_no} in provisional log at {log_path}: {err}",
                                            log_path = provisional_log_path.to_string_lossy()
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
                                error!("Cannot open {log_path}: {err}", log_path = provisional_log_path.to_string_lossy());
                            }
                            Vec::new()
                        }
                    };
                    response_tx.unbounded_send(res).unwrap_or_default();
                }
            },
            notification = subscribers.select_next_some() => {
                // Parse the notification
                // Append to the site's provisional log
            }
            complete => break,
        }
    }
}
