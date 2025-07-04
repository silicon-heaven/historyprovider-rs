use std::path::Path;

use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures::io::BufReader;
use futures::stream::{FuturesUnordered, SelectAll};
use futures::{select, FutureExt, StreamExt, TryStreamExt};
use log::{debug, error, info, warn};
use shvclient::client::ShvApiVersion;
use shvclient::{AppState, ClientEventsReceiver};
use shvrpc::join_path;
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::journalentry::JournalEntry;
use crate::journalrw::JournalReaderLog2;
use crate::util::{subscribe, subscription_prefix_path};
use crate::{ClientCommandSender, State, Subscriber};

pub(crate) struct LogContent(Vec<JournalEntry>);

pub(crate) enum ProvisionalLogCommand {
    // On the client connect
    SubscribeNotifications(ShvApiVersion),
    TrimLog {
        site: String
    },
    GetLog {
        site: String,
        response_tx: UnboundedSender<LogContent>,
    }
}

pub(crate) async fn provisional_log_task(
    client_cmd_tx: ClientCommandSender,
    client_evt_rx: ClientEventsReceiver,
    app_state: AppState<State>,
    mut cmd_rx: UnboundedReceiver<ProvisionalLogCommand>,
)
{
    // Provisional log maintenance
    //  - manage subscriptions
    //  - record notifications to provisional logs
    //  - trim provisional log (periodically, or on command when the site is synced)
    //  - provide provisional log files to getLog (via a channel)


    // TODO:
    // on mntchng:
    //   sync logs from the site
    // on client connect:
    //   subscribe mntchng
    //   subscribe events
    //   sync logs from all sites
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
                    // Get the latest entry from the site log and remove all entries up to that
                    // entry from the provisional log
                    let provisional_log_path = Path::new(&app_state.config.journal_dir).join(site).join("provisionallog");
                    let provisional_log_file = match tokio::fs::File::open(&provisional_log_path).await {
                        Ok(file) => file,
                        Err(err) => {
                            if err.kind() != std::io::ErrorKind::NotFound {
                                warn!("Cannot trim provisional log. Cannot open file {file_path}: {err}", file_path = provisional_log_path.to_string_lossy());
                            }
                            continue;
                        }
                    };
                    let latest_log_path = {
                        Path::new(&app_state.config.journal_dir).join(site)
                        // TODO
                    };
                    let latest_log_file = match tokio::fs::File::open(&latest_log_path).await {
                        Ok(file) => file,
                        Err(err) => {
                            if err.kind() != std::io::ErrorKind::NotFound {
                                warn!("Cannot trim provisional log. Cannot open file {file_path}: {err}", file_path = latest_log_path.to_string_lossy());
                            }
                            continue;
                        }
                    };

                    // Get the latest entry from the latest log
                    let reader = JournalReaderLog2::new(BufReader::new(latest_log_file.compat()));
                    let Some(latest_entry) = reader.fold(None, |_, entry| async { entry.ok() }).await else {
                        continue;
                    };

                    // Remove all entries earlier than the latest entry from the provisional log
                    let reader = JournalReaderLog2::new(BufReader::new(provisional_log_file.compat()));
                    let trimmed_log = reader.filter_map(|entry| {
                        let passed_entry = entry
                            .as_ref()
                            .ok()
                            .filter(|entry| entry.epoch_msec > latest_entry.epoch_msec)
                            .cloned();
                        async move { passed_entry }
                    })
                    .collect::<Vec<_>>()
                    .await;

                    // Write the provisional log
                    // TODO

                }
                ProvisionalLogCommand::GetLog { site, response_tx } => {
                    // Load the provisional log and return it in the response channel
                    let provisional_log_path =  Path::new(&app_state.config.journal_dir).join(site).join("provisionallog");
                    let res = match tokio::fs::File::open(&provisional_log_path).await {
                        Ok(file) => {
                            let mut reader = JournalReaderLog2::new(BufReader::new(file.compat())).enumerate();
                            let mut res = Vec::new();
                            while let Some((entry_no, journal_entry_res)) = reader.next().await {
                                match journal_entry_res {
                                    Ok(journal_entry) => res.push(journal_entry),
                                    Err(err) => warn!("Invalid journal entry no. {entry_no} in provisional log at {log_path}: {err}", log_path = provisional_log_path.to_string_lossy()),
                                }
                            }
                            res
                        }
                        Err(err) => {
                            if err.kind() != std::io::ErrorKind::NotFound {
                                error!("Cannot open {log_path}: {err}", log_path = provisional_log_path.to_string_lossy());
                            }
                            Vec::new()
                        }
                    };
                    response_tx.unbounded_send(LogContent(res)).unwrap_or_default();
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
