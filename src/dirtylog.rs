use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::pin::Pin;
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
use crate::journalrw::{JournalReaderLog2, JournalWriterLog2, VALUE_FLAG_SPONTANEOUS_BIT};
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

pub(crate) async fn dirtylog_task(
    client_cmd_tx: ClientCommandSender,
    _client_evt_rx: ClientEventsReceiver,
    app_state: AppState<State>,
    mut cmd_rx: UnboundedReceiver<DirtyLogCommand>,
) {
    let mut site_paths = Arc::default();
    let mut subscribers = SelectAll::<Subscriber>::default();

    // Per site request
    enum Request {
        Get(OneshotSender<Vec<JournalEntry>>),
        Append(JournalEntry),
        Trim,
    }

    async fn process_request(site: String, journal_dir: PathBuf, request: Request) {
        match request {
            Request::Get(response_sender) => {
                // Load the dirty log and return it in the response channel
                let dirty_log_path =  journal_dir.join(site).join("dirtylog");
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
                response_sender.send(res).unwrap_or_default();
            }
            Request::Append(journal_entry) => {
                let journal_site_path = journal_dir.join(site);
                if !journal_site_path.exists() {
                    warn!("Ignoring notification while journal directory {journal_site_path} for the site does not exist",
                        journal_site_path = journal_site_path.to_string_lossy()
                    );
                    return;
                }
                // Append to the site's dirty log
                let dirty_log_path = journal_site_path.join("dirtylog");
                let dirty_log_file = match tokio::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&dirty_log_path)
                    .await {
                        Ok(file) => file,
                        Err(err) => {
                            error!("Cannot append a notification to dirty log. Cannot open file {file_path} for write: {err}",
                                file_path = dirty_log_path.to_string_lossy()
                            );
                            return;
                        }
                    };
                let mut writer = JournalWriterLog2::new(dirty_log_file.compat());
                writer.append(&journal_entry)
                    .await
                    .unwrap_or_else(|err|
                        error!("Cannot append a notification to dirty log {log_file}: {err}",
                            log_file = dirty_log_path.to_string_lossy()
                        )
                    );
            }
            Request::Trim => {
                info!("Trim dirty log start, site: {site}");
                // Get the latest entry from the site log and remove all entries up to that
                // entry from the dirty log
                let dirty_log_path = journal_dir.join(&site).join("dirtylog");
                let dirty_log_file = match tokio::fs::File::open(&dirty_log_path).await {
                    Ok(file) => file,
                    Err(err) => {
                        if err.kind() == std::io::ErrorKind::NotFound {
                            info!("Trim dirty log done, no dirty log file for the site");
                        } else {
                            error!("Cannot trim dirty log. Cannot open file {file_path}: {err}", file_path = dirty_log_path.to_string_lossy());
                        }
                        return;
                    }
                };

                let latest_entry = {
                    let mut log_files = match get_files(journal_dir.join(&site), is_log2_file).await {
                        Ok(files) => files,
                        Err(err) => {
                            error!("Cannot trim dirty log. Cannot read journal dir entries: {err}");
                            return;
                        }
                    };
                    log_files.sort_by_key(|entry| std::cmp::Reverse(entry.file_name()));

                    let latest_entry = Box::pin(futures::stream::iter(log_files)
                        .map(|file_entry| file_entry.path())
                        .then(|file_path|
                            async move {
                                match tokio::fs::File::open(&file_path).await {
                                    Ok(file) => {
                                        let reader = JournalReaderLog2::new(BufReader::new(file.compat()));
                                        reader.fold(None, |_, entry| { let entry = entry.ok(); async { entry }}).await
                                    }
                                    Err(err) => {
                                        error!("Cannot open file {file_path} while getting the last journal entry for trim dirtylog: {err}",
                                            file_path = file_path.to_string_lossy()
                                        );
                                        None
                                    }
                        }
                            })
                        .filter_map(async move |entry| entry))
                        .next()
                        .await;

                    match latest_entry {
                        Some(entry) => entry,
                        None => {
                            info!("Trim dirty log done, no journal entries in synced files");
                            return;
                        }
                    }
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
                    .truncate(true)
                    .open(&dirty_log_path)
                    .await {
                        Ok(file) => file,
                        Err(err) => {
                            error!("Cannot trim dirty log. Cannot open file {file_path} for write: {err}",
                                file_path = dirty_log_path.to_string_lossy()
                            );
                            return;
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
                info!("Trim dirty log done, site: {site}");

            }
        }
    }

    fn schedule_next_request<'a>(
        site: String,
        journal_dir: PathBuf,
        per_site: &mut HashMap<String, VecDeque<Request>>,
        running: &mut HashSet<String>,
        inflight: &mut FuturesUnordered<Pin<Box<dyn futures::Future<Output = String> + Send + 'a>>>,
    ) {
        if running.contains(&site) {
            return;
        }
        if let Some(queue) = per_site.get_mut(&site) {
            if let Some(request) = queue.pop_front() {
                running.insert(site.clone());
                inflight.push(Box::pin(async move {
                    process_request(site.clone(), journal_dir, request).await;
                    site
                }));
            }
        }
    }

    let mut site_requests: HashMap<String, VecDeque<Request>> = HashMap::new();
    let mut site_processing: HashSet<String> = HashSet::new();
    // Global pool of running site tasks (max 1 per site)
    let mut inflight_requests = FuturesUnordered::new();

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
                    site_requests.entry(site.clone()).or_default().push_back(Request::Trim);
                    schedule_next_request(site, Path::new(&app_state.config.journal_dir).into(), &mut site_requests, &mut site_processing, &mut inflight_requests);
                }
                DirtyLogCommand::Get { site, response_tx } => {
                    site_requests.entry(site.clone()).or_default().push_back(Request::Get(response_tx));
                    schedule_next_request(site, Path::new(&app_state.config.journal_dir).into(), &mut site_requests, &mut site_processing, &mut inflight_requests);
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

                let data_change = DataChange::from(param.clone());
                let journal_entry = JournalEntry {
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
                // Schedule next task
                site_requests.entry(site_path.into()).or_default().push_back(Request::Append(journal_entry));
                schedule_next_request(site_path.into(), Path::new(&app_state.config.journal_dir).into(), &mut site_requests, &mut site_processing, &mut inflight_requests);

            }
            site = inflight_requests.next() => {
                if let Some(site) = site {
                    site_processing.remove(&site);
                    schedule_next_request(site, Path::new(&app_state.config.journal_dir).into(), &mut site_requests, &mut site_processing, &mut inflight_requests);
                }
            }
            complete => break,
        }
    }
}
