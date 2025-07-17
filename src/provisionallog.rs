use std::path::Path;
use std::sync::Arc;

use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures::io::BufReader;
use futures::stream::{FuturesUnordered, SelectAll};
use futures::{select, StreamExt};
use log::{debug, error, info, warn};
use shvclient::client::ShvApiVersion;
use shvclient::{AppState, ClientEventsReceiver};
use shvproto::{DateTime, RpcValue};
use shvrpc::metamethod::AccessLevel;
use shvrpc::{join_path, RpcMessageMetaTags};
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::journalentry::JournalEntry;
use crate::journalrw::{JournalReaderLog2, JournalWriterLog2, VALUE_FLAG_PROVISIONAL_BIT, VALUE_FLAG_SPONTANEOUS_BIT};
use crate::util::{get_files, is_log2_file, subscribe, subscription_prefix_path};
use crate::{ClientCommandSender, State, Subscriber};

use shvclient::clientnode::{find_longest_path_prefix, SIG_CHNG};
const SIG_CMDLOG: &str = "cmdlog";

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

    let mut site_paths = Arc::default();
    let mut subscribers = SelectAll::<Subscriber>::default();
    loop {
        select! {
            command = cmd_rx.select_next_some() => match command {
                ProvisionalLogCommand::SubscribeNotifications(shv_api_version) => {
                    info!("Subscribing signals on the devices");
                    site_paths = app_state
                        .sites_data
                        .read()
                        .await
                        .sites_info
                        .clone();
                    subscribers = site_paths
                        .iter()
                        .flat_map(|(path, _)| {
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
                            let reader = JournalReaderLog2::new(BufReader::new(file.compat())).enumerate();
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
                // Append to the site's provisional log
                let provisional_log_path = Path::new(&app_state.config.journal_dir).join(site_path).join("provisionallog");
                let provisional_log_file = match tokio::fs::OpenOptions::new()
                    .append(true)
                    .open(&provisional_log_path)
                    .await {
                        Ok(file) => file,
                        Err(err) => {
                            error!("Cannot append a notification to provisional log. Cannot open file {file_path} for write: {err}",
                                file_path = provisional_log_path.to_string_lossy()
                            );
                            continue;
                        }
                    };
                let mut writer = JournalWriterLog2::new(provisional_log_file.compat());
                let data_change = DataChange::from(param.clone());
                let entry = JournalEntry {
                    epoch_msec: data_change.date_time.unwrap_or_else(|| DateTime::now()).epoch_msec(),
                    path: property_path.to_string(),
                    signal, source: Default::default(),
                    value: data_change.value,
                    access_level: AccessLevel::Read as _,
                    short_time: data_change.short_time.unwrap_or(-1),
                    user_id: Default::default(),
                    repeat: data_change.value_flags & (1 << VALUE_FLAG_SPONTANEOUS_BIT) == 0,
                    provisional: data_change.value_flags & (1 << VALUE_FLAG_PROVISIONAL_BIT) != 0,
                };
                writer.append(&entry)
                    .await
                    .unwrap_or_else(|err|
                        error!("Cannot append a notification to provisional log {log_file}: {err}",
                            log_file = provisional_log_path.to_string_lossy()
                        )
                    );
                }
            complete => break,
        }
    }
}

struct DataChange {
    value: RpcValue,
    date_time: Option<DateTime>,
    short_time: Option<i32>,
    value_flags: u64,
}

enum DataChangeMetaTag {
    DateTime = shvrpc::rpctype::Tag::USER as isize,
    ShortTime,
    ValueFlags,
    SpecialListValue,
}

fn meta_value<I: shvproto::metamap::GetIndex>(rv: &RpcValue, key: I) -> Option<&RpcValue> {
    rv.meta.as_ref().and_then(| meta| meta.get(key))
}

fn is_data_change(rv: &RpcValue) -> bool {
    meta_value(rv, shvrpc::rpctype::Tag::MetaTypeNameSpaceId as usize).unwrap_or_default().as_int() == shvrpc::rpctype::NameSpaceID::Global as i64 &&
        meta_value(rv, shvrpc::rpctype::Tag::MetaTypeId as usize).unwrap_or_default().as_int() == shvrpc::rpctype::GlobalNS::MetaTypeID::ValueChange as i64
}

impl From<RpcValue> for DataChange {
    fn from(value: RpcValue) -> Self {
        if !is_data_change(&value) {
            return DataChange {
                value,
                date_time: None,
                short_time: None,
                value_flags: Default::default(),
            }
        }

        let date_time = meta_value(&value, DataChangeMetaTag::DateTime as usize)
            .and_then(|v| v.to_datetime());
        let short_time = meta_value(&value, DataChangeMetaTag::ShortTime as usize)
            .map(|v| v.as_i32());
        let value_flags = meta_value(&value, DataChangeMetaTag::ValueFlags as usize)
            .map(|v| v.as_u64()).unwrap_or_default();

        let unpacked_value = if let shvproto::Value::List(lst) = &value.value &&
            lst.len() == 1 &&
            lst[0].meta.as_ref().is_some_and(|meta| !meta.is_empty()) &&
            value.meta.as_ref().is_none_or(|meta| meta.get(DataChangeMetaTag::SpecialListValue as usize).is_none())
        {
            lst[0].clone()
        } else {
            RpcValue {
                meta: None,
                value: value.value,
            }
        };
        DataChange {
            value: unpacked_value,
            date_time,
            short_time,
            value_flags,
        }
    }
}

impl From<DataChange> for RpcValue {
    fn from(data_change: DataChange) -> Self {
        let mut res = if data_change.value.meta.as_ref().is_none_or(|meta| meta.is_empty()) {
            let val = data_change.value.value.clone();
            if let shvproto::Value::List(lst) = &val &&
                lst.len() == 1 &&
                lst[0].meta.as_ref().is_some_and(|meta| !meta.is_empty()) {
                    let mut mm = shvproto::MetaMap::new();
                    mm.insert(DataChangeMetaTag::SpecialListValue as usize, true.into());
                    RpcValue::new(val, Some(mm))
            } else {
                RpcValue::new(val, None)
            }
        } else {
            shvproto::make_list!(data_change.value).into()
        };
        let mm = res.meta
            .get_or_insert_default()
            .insert(shvrpc::rpctype::Tag::MetaTypeId as usize, RpcValue::from(shvrpc::rpctype::GlobalNS::MetaTypeID::ValueChange as i64));
        if let Some(date_time) = data_change.date_time {
            mm.insert(DataChangeMetaTag::DateTime as usize, date_time.into());
        }
        if let Some(short_time) = data_change.short_time {
            mm.insert(DataChangeMetaTag::ShortTime as usize, short_time.into());
        }
        if data_change.value_flags != 0 {
            mm.insert(DataChangeMetaTag::ValueFlags as usize, data_change.value_flags.into());
        }
        res
    }
}
