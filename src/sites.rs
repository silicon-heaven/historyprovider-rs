use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures::stream::{FusedStream, FuturesUnordered};
use futures::StreamExt;
use log::{debug, error, info, warn};
use shvclient::clientapi::{CallRpcMethodErrorKind, RpcCall, RpcCallDirExists, RpcCallLsList, Subscriber};
use shvclient::clientnode::{METH_DIR, SIG_CHNG};
use shvclient::{ClientCommandSender, ClientEvent, ClientEventsReceiver};
use shvproto::{DateTime, RpcValue};
use shvrpc::rpcmessage::{RpcError, RpcErrorCode};
use shvrpc::typeinfo::TypeInfo;
use shvrpc::util::find_longest_path_prefix;
use shvrpc::{join_path, RpcMessageMetaTags};
use tokio::sync::Semaphore;
use tokio::time::timeout;
use tokio_stream::StreamMap;

use crate::alarm::{collect_alarms, collect_state_alarms, Alarm};
use crate::getlog::{getlog_handler};
use crate::records::{record_name_is_valid, record_names_from_rpc};
use crate::util::{subscribe, subscription_prefix_path};
use crate::{AlarmWithTimestamp, State};

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) enum SiteOnlineStatus {
    #[default]
    Unknown = 0,
    Offline = 1,
    Online = 2,
}

pub(crate) enum SitesCommand {
    ReloadSites,
}

#[derive(Clone,Default)]
pub(crate) struct SitesData {
    pub(crate) sites_info: Arc<BTreeMap<String, SiteInfo>>,
    pub(crate) sub_hps: Arc<BTreeMap<String, SubHpInfo>>,
    pub(crate) typeinfos: Arc<BTreeMap<String, Result<TypeInfo, String>>>,
}


#[derive(Debug, PartialEq)]
pub(crate) struct SiteInfo {
    pub(crate) name: String,
    pub(crate) site_type: String,
    pub(crate) sub_hp: String,
}

fn collect_sites(
    path_segments: &[&str],
    sites_subtree: &shvproto::Map,
) -> BTreeMap<String, SiteInfo>
{
    if let Some((&"_meta", path_prefix)) = path_segments.split_last() {
        let is_site = sites_subtree.contains_key("HP") ||
            sites_subtree.get("HP3")
            .and_then(|hp3_node| shvproto::Map::try_from(hp3_node).ok())
            .is_some_and(|hp3_node| {
                hp3_node.get("type").is_none_or(|ty| {
                    let ty = ty.as_str();
                    ty == "device" || ty == "records"
                })
            });

        return if is_site {
            let get_or_default = |key: &str, default: &str| sites_subtree
                .get(key)
                .and_then(|val| String::try_from(val).ok())
                .unwrap_or_else(|| default.into());
            BTreeMap::from([(
                    path_prefix.join("/"),
                    SiteInfo {
                        name: get_or_default("name", "<undefined>"),
                        site_type: get_or_default("type", "<undefined>"),
                        sub_hp: Default::default(),
                    },
            )])
        } else {
            BTreeMap::new()
        }
    }

    sites_subtree
        .iter()
        .flat_map(|(key, val)|
            collect_sites(
                &path_segments.iter().copied().chain(std::iter::once(key.as_str())).collect::<Vec<_>>(),
                val.as_map(),
            )
        )
        .collect()
}

#[derive(Debug)]
pub(crate) enum SubHpInfo {
    Normal {
        // SHV path of the log files relative to the sub HP node
        sync_path: String,
        download_chunk_size: i64,
    },
    Legacy {
        // SHV path of the legacy HP relative to the sub HP node
        getlog_path: String,
    },
    Records {
        records: Vec<String>,
    },
    PushLog,
}

const DEFAULT_SYNC_PATH_DEVICE: &str = ".app/history";
const DEFAULT_SYNC_PATH_HP: &str = ".local/history/_shvjournal";
const DOWNLOAD_CHUNK_SIZE_MAX: i64 = 256 << 10;

// FIXME: We only synchronize by getLog from devices, never from
// intermediate legacy HP because of flaws in getLog design.
const LEGACY_SYNC_PATH_DEVICE: &str = "";
const LEGACY_SYNC_PATH_HP: &str = ".local/history";

fn collect_sub_hps(
    path_segments: &[&str],
    sites_subtree: &shvproto::Map,
) -> BTreeMap<String, SubHpInfo>
{
    if !path_segments.is_empty() {
        let sub_hp = sites_subtree
            .get("_meta")
            .and_then(|v| {
                let meta = v.as_map();
                meta
                    .get("HP")
                    .or_else(|| meta.get("HP3"))
                    .map(|hp| {
                        let is_device = meta.contains_key("type");
                        let hp = hp.as_map();
                        if hp.get("pushLog").is_some_and(RpcValue::as_bool) {
                            SubHpInfo::PushLog
                        } else if hp.get("type").is_some_and(|ty| ty.as_str() == "records") {
                            SubHpInfo::Records {
                                records: hp
                                    .get("records")
                                    .map(record_names_from_rpc)
                                    .unwrap_or_default()
                                    .into_iter()
                                    .filter(|record| record_name_is_valid(record))
                                    .collect(),
                            }
                        } else if meta.contains_key("HP") {
                            SubHpInfo::Legacy {
                                getlog_path: if is_device { LEGACY_SYNC_PATH_DEVICE } else { LEGACY_SYNC_PATH_HP }.to_string(),
                            }
                        } else {
                            SubHpInfo::Normal {
                                sync_path: hp
                                    .get("syncPath")
                                    .map(RpcValue::as_str)
                                    .unwrap_or_else(|| if is_device { DEFAULT_SYNC_PATH_DEVICE } else { DEFAULT_SYNC_PATH_HP })
                                    .to_string(),
                                download_chunk_size: hp
                                    .get("readLogChunkLimit")
                                    .map(|v| v.as_int().min(DOWNLOAD_CHUNK_SIZE_MAX))
                                    .unwrap_or(DOWNLOAD_CHUNK_SIZE_MAX),
                            }
                        }
                    })
            });
        if let Some(v) = sub_hp {
            return BTreeMap::from([(path_segments.join("/"), v)]);
        }
    }
    sites_subtree
        .iter()
        .flat_map(|(key, val)|
            collect_sub_hps(
                &path_segments.iter().copied().chain(std::iter::once(key.as_str())).collect::<Vec<_>>(),
                val.as_map(),
            )
        )
        .collect()
}

type AlarmCollector = fn(type_info: &TypeInfo, shv_path: &str, value: &RpcValue) -> Vec<Alarm>;
pub(crate) fn update_alarms(alarms_colector: AlarmCollector, alarms_for_site: &mut Vec<AlarmWithTimestamp>, type_info: &TypeInfo, property_path: &str, value: &RpcValue, timestamp: shvproto::DateTime) -> Vec<AlarmWithTimestamp> {
    let new_alarms = alarms_colector(type_info, property_path, value)
        .into_iter()
        .map(|alarm| AlarmWithTimestamp {
            alarm,
            timestamp,
            stale: false,
        });

    let mut updated_alarms = Vec::new();

    for new_alarm in new_alarms {
        if new_alarm.alarm.is_active {
            if let Some(existing) = alarms_for_site.iter_mut().find(|a| a.alarm.path == new_alarm.alarm.path) {
                if existing.alarm != new_alarm.alarm {
                    *existing = new_alarm.clone();
                    updated_alarms.push(new_alarm);
                }
            } else {
                alarms_for_site.push(new_alarm.clone());
                updated_alarms.push(new_alarm);
            }
        } else {
            let old_len = alarms_for_site.len();
            alarms_for_site.retain(|a| a.alarm.path != new_alarm.alarm.path);
            if alarms_for_site.len() != old_len {
                updated_alarms.push(new_alarm);
            }
        }
    }

    updated_alarms
}

async fn set_online_status(
    site: impl AsRef<str>,
    new_status: SiteOnlineStatus,
    client_commands: &ClientCommandSender,
    app_state: &Arc<State>
)
{
    let site = site.as_ref();
    let mut online_states = app_state.online_states.write().await;
    let Some(online_status) = online_states.get_mut(site) else {
        error!(target: "OnlineStatus", "No onlineStatus for site {site}");
        return
    };
    if *online_status == new_status {
        return;
    }

    debug!(target: "OnlineStatus", "[{site}] Set online status: {new_status:?}");
    *online_status = new_status;

    client_commands
        .send_message(shvrpc::RpcMessage::new_signal(site, "onlinestatuschng").with_param(new_status as i32))
        .unwrap_or_else(|err| log::error!(target: "OnlineStatus", "[{site}] Cannot send 'onlinestatuschng' signal: {err}"));

    const SITE_OFFLINE_ALARM_KEY: &str = "site-offline";

    drop(online_states);
    let mut alarms = app_state.alarms.write().await;
    let Some(site_alarms) = alarms.get_mut(site) else {
        return;
    };

    let offline_alarm_idx = site_alarms
        .iter()
        .position(|alarm_with_ts| alarm_with_ts.alarm.path == SITE_OFFLINE_ALARM_KEY);

    let emit_alarmmod = if new_status == SiteOnlineStatus::Offline  {
        site_alarms.iter_mut().for_each(|alarm_with_ts| alarm_with_ts.stale = true);
        if offline_alarm_idx.is_none() {
            site_alarms.push(AlarmWithTimestamp {
                alarm: Alarm {
                    path: SITE_OFFLINE_ALARM_KEY.into(),
                    is_active: true,
                    description: "Site is offline".into(),
                    label: "Offline".into(),
                    level: 0,
                    severity: crate::alarm::Severity::Error,
                },
                timestamp: DateTime::now(),
                stale: false,
            });
        }
        true
    } else if new_status == SiteOnlineStatus::Online && let Some(idx) = offline_alarm_idx {
        site_alarms.remove(idx);
        true
    } else {
        false
    };

    if emit_alarmmod {
        client_commands
            .send_message(shvrpc::RpcMessage::new_signal(site, "alarmmod"))
            .unwrap_or_else(|err| log::error!(target: "OnlineStatus", "[{site}] Cannot send 'alarmmod' signal: {err}"));
    }
}

fn online_status_worker(
    site: impl Into<String>,
    mut events: UnboundedReceiver<SiteOnlineStatus>,
    client_commands: ClientCommandSender,
    app_state: Arc<State>
) -> impl Future<Output = ()>
{
    const ONLINE_TIMER: Duration = Duration::from_secs(10);
    let site = site.into();

    debug!(target: "OnlineStatus", "[{site}] Worker started");
    async move {
        loop {
            match timeout(ONLINE_TIMER, events.next()).await {
                // Received a message before timeout
                Ok(Some(status)) => {
                    set_online_status(&site, status, &client_commands, &app_state).await;
                }

                // Channel closed
                Ok(None) => {
                    debug!(target: "OnlineStatus", "[{site}] Events channel closed");
                    set_online_status(&site, SiteOnlineStatus::Unknown, &client_commands, &app_state).await;
                    break;
                }

                // Timeout elapsed
                Err(_) => {
                    debug!(target: "OnlineStatus", "[{site}] Timer expired, checking the site status by an RPC call");
                    let dir_result = RpcCallDirExists::new(&join_path!("shv", &site), METH_DIR)
                        .timeout(ONLINE_TIMER)
                        .exec(&client_commands)
                        .await;
                    debug!(target: "OnlineStatus", "[{site}] RPC call result: {dir_result:?}");
                    if dir_result.is_ok() {
                        set_online_status(&site, SiteOnlineStatus::Online, &client_commands, &app_state).await;
                    } else if let Err(err) = dir_result && matches!(err.error(), CallRpcMethodErrorKind::RpcError(RpcError { .. })) {
                        set_online_status(&site, SiteOnlineStatus::Offline, &client_commands, &app_state).await;
                    }
                }
            }
        }
        debug!(target: "OnlineStatus", "[{site}] Worker finished");
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub(crate) struct ParsedNotification {
    pub(crate) site_path: String,
    pub(crate) property_path: String,
    pub(crate) signal: String,
    pub(crate) source: String,
    pub(crate) param: RpcValue,
    pub(crate) user_id: Option<String>,
}

pub(crate) fn parse_notification(msg: &shvrpc::RpcMessage, sites_info: &BTreeMap<String, SiteInfo>) -> Option<ParsedNotification> {
    let path = msg.shv_path().unwrap_or_default();
    let (site_path, property_path) = find_longest_path_prefix(sites_info, path.strip_prefix("shv/")?)?;
    let signal = msg.method().unwrap_or_default().to_string();
    let source = msg.source().unwrap_or_default().to_string();
    let param = msg.param().unwrap_or_default();
    let user_id = msg.user_id().map(String::from);
    Some(ParsedNotification {
        site_path: site_path.to_string(),
        property_path: property_path.to_string(),
        signal,
        source,
        param: param.clone(),
        user_id,
    })
}

struct SitesTaskState {
    mntchng_subscribers: StreamMap<String, Subscriber>,
    subscribers: StreamMap<String, Subscriber>,
    online_status_channels: BTreeMap<String, UnboundedSender<SiteOnlineStatus>>,
    online_status_task: Option<tokio::task::JoinHandle<()>>,
}

async fn init_subscribers(
    old_subscribers: &mut StreamMap<String, Subscriber>,
    client_cmd_tx: &ClientCommandSender,
    subscriptions: impl IntoIterator<Item = (String, String)>,
) -> StreamMap<String, Subscriber> {
    let mut subscribers = StreamMap::new();
    let mut new_sub_futures = vec![];
    for (path, signal) in subscriptions {
        let key = format!("{path}:*:{signal}");

        if let Some(subscriber) = old_subscribers.remove(&key) {
            // Keep existing subscriber
            subscribers.insert(key, subscriber);
        } else {
            // Queue up creation for missing subscriber
            new_sub_futures.push(async move {
                let subscriber = subscribe(client_cmd_tx, &path, &signal).await;
                (key, subscriber)
            });
        }
    }

    for (key, subscriber) in futures::future::join_all(new_sub_futures).await {
        subscribers.insert(key, subscriber);
    }

    subscribers
}

enum PeriodicSyncCommand {
    Enable,
    Disable,
}


async fn reload_sites(
    shv_api_version: shvclient::clientapi::ShvApiVersion,
    client_cmd_tx: &ClientCommandSender,
    app_state: &Arc<State>,
    old_state: &mut SitesTaskState,
) -> Option<SitesTaskState>
{
    let _reload_guard = crate::ReloadGuard::new(&app_state.sites_reload_in_progress);
    log::info!("Getting sites info");

    let (sites_info, sub_hps) = 'sites_get_loop: loop {
        let sites: Result<shvproto::Map, _> = RpcCall::new("sites", "getSites")
            .exec(client_cmd_tx)
            .await;

        match sites {
            Ok(sites) => {
                if sites
                    .get("_meta")
                        .map(RpcValue::as_map)
                        .and_then(|map| map.get("HP3"))
                        .map(RpcValue::as_map)
                        .and_then(|map| map.get("type"))
                        .map(RpcValue::as_str)
                        .is_none_or(|type_str| type_str != "HP3")
                {
                    eprintln!("This site's _meta does NOT include an HP3 node. Refusing to continue. Add an HP3 node to the site's _meta, otherwise this HP instance will not be visible to parent HPs.");
                    std::process::abort();
                }
                let sub_hps = collect_sub_hps(&[], &sites);
                let mut sites_info = collect_sites(&[], &sites);
                for (path, site_info) in &mut sites_info {
                    if let Some((prefix, _)) = find_longest_path_prefix(&sub_hps, path) {
                        site_info.sub_hp = prefix.into();
                    } else {
                        log::error!("Cannot find sub HP for site {path}");
                        site_info.sub_hp = path.clone();
                    }
                }
                break 'sites_get_loop (Arc::new(sites_info), Arc::new(sub_hps));
            }
            Err(err) => {
                match err.error() {
                    CallRpcMethodErrorKind::ConnectionClosed => {
                        log::warn!("Connection closed while getting sites info");
                        return None;
                    }
                    _ => {
                        log::error!("Get sites info error: {err}");
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    };

    log::debug!("Loaded sites:\n{}", sites_info
        .iter()
        .map(|(path, site)| format!(" {path}: {site:?}"))
        .collect::<Vec<_>>()
        .join("\n")
    );

    log::debug!("Loaded sub HPs:\n{}", sub_hps
        .iter()
        .map(|(path, hp)| format!(" {path}: {hp:?}"))
        .collect::<Vec<_>>()
        .join("\n")
    );

    let sites_without_pushlog = sites_info
        .iter()
        .filter(|(_, site)| sub_hps
            .get(&site.sub_hp)
            .is_some_and(|sub_hp| !matches!(sub_hp, SubHpInfo::PushLog))
        );

    // Subscribe mntchng
    let mntchng_subscribers = init_subscribers(
        &mut old_state.mntchng_subscribers,
        client_cmd_tx,
        sites_without_pushlog.clone().map(|(path, _)| (subscription_prefix_path(join_path!("shv", path), &shv_api_version), "mntchng".to_string()))
    ).await;

    log::info!("Loading typeinfo");
    let subscribers = init_subscribers(
        &mut old_state.subscribers,
        client_cmd_tx,
        sites_without_pushlog.clone().flat_map(|(path, _)| {
                let shv_path = join_path!("shv", path);
                const SIG_CMDLOG: &str = "cmdlog";
                [
                    (subscription_prefix_path(&shv_path, &shv_api_version), SIG_CHNG.to_string()),
                    (subscription_prefix_path(&shv_path, &shv_api_version), SIG_CMDLOG.to_string()),
                ]
            })
    ).await;

    let typeinfos = sites_without_pushlog
        .map(|(path, _)| {
            let client_cmd_tx = ClientCommandSender::clone(client_cmd_tx);
            async move {
                let result = 'result: {
                    let files_path = join_path!("sites", &path, "_files");
                    let files = RpcCallLsList::new(&files_path).exec(&client_cmd_tx)
                        .await
                        .unwrap_or_else(|err| panic!("Couldn't discover typeInfo support for {files_path}: {err}"));
                    let Some(type_info_filename) = files
                        .into_iter()
                        .find(|file| file == "typeInfo.cpon" || file == "nodesTree.cpon") else {
                            break 'result Err(format!("No typeInfo.cpon or nodesTree.cpon found for site {path}"));
                        };
                    let type_info_filepath = join_path!(files_path, type_info_filename);
                    let type_info_file: RpcValue = RpcCall::new(&type_info_filepath, "read").exec(&client_cmd_tx)
                        .await
                        .unwrap_or_else(|err| panic!("Retrieving {type_info_filepath} failed: {err}"));
                    RpcValue::from_cpon(String::from_utf8_lossy(type_info_file.as_blob()))
                        .map_err(|err| format!("Couldn't parse typeinfo file as cpon: {err}"))
                        .and_then(|rv| rv.try_into().map_err(|err| format!("Failed to parse typeinfo for {path}: {err}")))
                };

                (path.clone(), result)
            }
        })
        .collect::<FuturesUnordered<_>>()
        .collect::<BTreeMap<_, _>>()
        .await;

    let typeinfos = Arc::new(typeinfos);

    *app_state.sites_data.write().await = SitesData {
        sites_info: sites_info.clone(),
        sub_hps: sub_hps.clone(),
        typeinfos: typeinfos.clone()
    };

    *app_state.online_states.write().await = sites_info.keys().map(|site| (site.clone(), Default::default())).collect();
    let mut online_status_channels = BTreeMap::new();
    let mut online_status_workers = Vec::new();
    for (site, info) in sites_info.iter() {
        if app_state.app_closing.load(std::sync::atomic::Ordering::Relaxed) {
            break;
        }

        if sub_hps.get(&info.sub_hp).is_none_or(|sub_hp| matches!(sub_hp, SubHpInfo::PushLog)) {
            continue
        };
        let (tx, rx) = futures::channel::mpsc::unbounded();
        online_status_channels.insert(site.to_string(), tx);
        online_status_workers.push(online_status_worker(site.clone(), rx, ClientCommandSender::clone(client_cmd_tx), Arc::clone(app_state)));
    }

    let alarms = &mut *app_state.alarms.write().await;
    let state_alarms = &mut *app_state.state_alarms.write().await;

    let online_status_task = Some(tokio::spawn(async move {
        debug!(target: "OnlineStatus", "online status task starts");
        futures::future::join_all(online_status_workers).await;
        debug!(target: "OnlineStatus", "online status task finish");
    }));
    *app_state.online_states.write().await = sites_info.keys().map(|site| (site.clone(), Default::default())).collect();

    let params = Arc::new(shvrpc::journalrw::GetLog2Params {
        since: shvrpc::journalrw::GetLog2Since::LastEntry,
        with_snapshot: true,
        ..Default::default()
    });

    let alarm_load_start = std::time::Instant::now();

    let valid_sites: Vec<_> = sites_info
        .keys()
        .filter_map(|path| typeinfos.get(path).and_then(|type_info| type_info.as_ref().ok()).map(|type_info| (path, type_info)))
        .collect();

    let semaphore = Arc::new(Semaphore::new(app_state.config.max_sync_tasks));

    let results: Vec<_> = valid_sites
        .into_iter()
        .map(|(site_path, type_info)| {
            let params = Arc::clone(&params);
            let app_state = Arc::clone(app_state);
            let semaphore = Arc::clone(&semaphore);
            let site_path = site_path.clone();
            async move {
                let _permit = semaphore
                    .acquire()
                    .await
                    .unwrap_or_else(|e| panic!("Cannot acquire semaphore: {e}"));

                if app_state.app_closing.load(std::sync::atomic::Ordering::Relaxed) {
                    return (site_path, type_info, Err(RpcError::new(RpcErrorCode::InternalError, "App is closing")));
                }

                let log = getlog_handler(&site_path, &params, app_state).await;
                (site_path, type_info, log)
            }
        })
        .collect::<FuturesUnordered<_>>()
        .filter_map(|(site_path, type_info, result)| async move {
            match result {
                Ok(log) => Some((site_path, type_info, log)),
                Err(err) => {
                    if !app_state.app_closing.load(std::sync::atomic::Ordering::Relaxed) {
                        log::error!("couldn't init alarms: getlog failed: {err}");
                    }
                    None
                }
            }
        })
        .collect::<Vec<_>>()
        .await;

    let loaded_count = results.len() as u32;

    for (site_path, type_info, log) in results {
        if app_state.app_closing.load(std::sync::atomic::Ordering::Relaxed) {
            break;
        }

        let chained_entries = log.snapshot_entries.iter().map(Arc::as_ref).chain(log.event_entries.iter().map(Arc::as_ref));
        let impl_update_alarms = |alarm_table: &mut BTreeMap<String, Vec<AlarmWithTimestamp>>, alarm_collector| {
            let alarms_for_site = alarm_table.entry(site_path.to_string()).or_default();

            for entry in chained_entries.clone() {
                update_alarms(alarm_collector, alarms_for_site, type_info, &entry.path, &entry.value, shvproto::DateTime::from_epoch_msec(entry.epoch_msec));
            }
        };

        impl_update_alarms(alarms, collect_alarms);
        impl_update_alarms(state_alarms, collect_state_alarms);
    }

    info!("Alarm init done: {loaded_count} sites in {:?}", alarm_load_start.elapsed());
    *app_state.last_sites_loaded.write().await = Some(std::time::Instant::now());
    Some(SitesTaskState { mntchng_subscribers, subscribers, online_status_channels, online_status_task })
}

pub(crate) async fn sites_task(
    client_cmd_tx: ClientCommandSender,
    client_evt_rx: ClientEventsReceiver,
    app_state: Arc<State>,
    mut sites_cmd_rx: UnboundedReceiver<SitesCommand>,
)
{
    let mut client_evt_rx = std::pin::pin!(client_evt_rx.fuse().peekable());

    let mut state = SitesTaskState {
        mntchng_subscribers: StreamMap::new(),
        subscribers: StreamMap::new(),
        online_status_channels: BTreeMap::new(),
        online_status_task: None,
    };

    let (periodic_sync_tx, mut periodic_sync_rx) = futures::channel::mpsc::unbounded();
    let periodic_sync_task = {
        let app_state = app_state.clone();
        tokio::spawn(async move {
            let periodic_sync_interval = app_state.config.periodic_sync_interval;
            let mut interval: Option<tokio::time::Interval> = None;
            loop {
                tokio::select! {
                    event = periodic_sync_rx.next() => match event {
                        Some(event) => {
                            match event {
                                PeriodicSyncCommand::Enable => {
                                    log::info!("periodic sync enable");
                                    interval = Some(tokio::time::interval(tokio::time::Duration::from_secs(periodic_sync_interval)));
                                }
                                PeriodicSyncCommand::Disable => {
                                    log::info!("periodic sync disable");
                                    interval = None;
                                }
                            }
                        }
                        None => break,
                    },
                    _ = async {
                        if let Some(i) = &mut interval {
                            i.tick().await;
                        }
                    }, if interval.is_some() => {
                        log::info!("periodic sync trigger");
                        app_state
                            .sync_cmd_tx
                            .send(crate::sync::SyncCommand::SyncAll)
                            .map(|_|())
                            .unwrap_or_else(|e| log::error!("Cannot send SyncAll command: {e}"));
                    }
                }
            }
            log::debug!("periodic sync task finished");
        })
    };


    'main_loop: loop {
        periodic_sync_tx
            .unbounded_send(PeriodicSyncCommand::Disable)
            .unwrap_or_else(|e| log::error!("Cannot send periodic sync disable command: {e}"));

        let shv_api_version = loop {
            match client_evt_rx.next().await {
                Some(ClientEvent::Connected(shv_api_version)) => break shv_api_version,
                Some(ClientEvent::ConnectionFailed(_)) | Some(ClientEvent::Disconnected) => {
                    if let Some(online_status_task) = state.online_status_task {
                        state.online_status_channels.clear();
                        if let Err(err) = online_status_task.await {
                            log::error!("Failed to join online_status_task: {err}")
                        };
                    }

                    state = SitesTaskState {
                        mntchng_subscribers: StreamMap::new(),
                        subscribers: StreamMap::new(),
                        online_status_channels: BTreeMap::new(),
                        online_status_task: None,
                    };
                },
                None => break 'main_loop,
            }
        };

        'sites_loop: loop {
            state = match reload_sites(shv_api_version.clone(), &client_cmd_tx, &app_state, &mut state).await {
                Some(s) => s,
                None => continue 'main_loop,
            };

            periodic_sync_tx
                .unbounded_send(PeriodicSyncCommand::Enable)
                .unwrap_or_else(|e| log::error!("Cannot send periodic sync enable command: {e}"));

            loop {
                tokio::select! {
                    _event = client_evt_rx.as_mut().peek() => continue 'main_loop,
                    sites_command = sites_cmd_rx.next(), if !sites_cmd_rx.is_terminated() => match sites_command {
                        Some(SitesCommand::ReloadSites) => {
                            continue 'sites_loop;
                        },
                        None => (),
                    },
                    mntchng_frame = state.mntchng_subscribers.next(), if !state.mntchng_subscribers.is_empty() => {
                        let Some((_, mntchng_frame)) = mntchng_frame else {
                            continue;
                        };
                        let msg = match mntchng_frame.to_rpcmesage() {
                            Ok(msg) => msg,
                            Err(err) => {
                                warn!("Ignoring wrong mntchng RpcFrame: {err}, frame: {mntchng_frame:?}");
                                continue;
                            }
                        };
                        let Some(ParsedNotification { site_path, signal, param, .. }) = parse_notification(&msg, &app_state.sites_data.read().await.sites_info) else {
                            continue
                        };

                        if signal != "mntchng" {
                            continue;
                        }
                        let mounted = param.as_bool();
                        if mounted {
                            log::info!("Site mounted: {site_path}");
                            app_state.sync_cmd_tx
                                .send(crate::sync::SyncCommand::SyncSite(site_path.clone()))
                                .unwrap_or_else(|e| panic!("Cannot send SyncSite({site_path}) command: {e}"));

                            if let Some(tx) = state.online_status_channels.get(&site_path) {
                                tx.unbounded_send(SiteOnlineStatus::Online).ok();
                            }
                        } else {
                            log::info!("Site unmounted: {site_path}");
                            if let Some(tx) = state.online_status_channels.get(&site_path) {
                                tx.unbounded_send(SiteOnlineStatus::Offline).ok();
                            }
                        }
                    }
                    notification_frame = state.subscribers.next(), if !state.subscribers.is_empty() => {
                        let Some((_, notification_frame)) = notification_frame else {
                            continue;
                        };
                        let msg = match notification_frame.to_rpcmesage() {
                            Ok(msg) => msg,
                            Err(err) => {
                                warn!("Ignoring wrong notification RpcFrame: {err}, frame: {notification_frame:?}");
                                continue;
                            }
                        };
                        let Some(parsed_notification) = parse_notification(&msg, &app_state.sites_data.read().await.sites_info) else {
                            continue
                        };
                        if let Some(tx) = state.online_status_channels.get(&parsed_notification.site_path) {
                            tx.unbounded_send(SiteOnlineStatus::Online).ok();
                        }

                        app_state.dirtylog_cmd_tx
                            .unbounded_send(crate::dirtylog::DirtyLogCommand::ProcessNotification(parsed_notification.clone()))
                            .unwrap_or_else(|e| log::error!("Cannot send dirtylog ProcessNotification command: {e}"));

                        let typeinfos = &app_state.sites_data.read().await.typeinfos;
                        let Some(Ok(type_info)) = typeinfos.get(&parsed_notification.site_path) else {
                            continue;
                        };

                        let impl_update_alarms = |alarm_table: &mut BTreeMap<String, Vec<AlarmWithTimestamp>>, alarm_collector, signal_name| {
                            let alarms_for_site = alarm_table.entry(parsed_notification.site_path.clone()).or_default();

                            let updated = update_alarms(alarm_collector, alarms_for_site, type_info, &parsed_notification.property_path, &parsed_notification.param, shvproto::DateTime::now());
                            if !updated.is_empty() {
                                client_cmd_tx.send_message(shvrpc::RpcMessage::new_signal(&parsed_notification.site_path, signal_name))
                                    .unwrap_or_else(|err| log::error!("alarms: Cannot send signal ({err})"));
                            }
                        };

                        impl_update_alarms(&mut *app_state.alarms.write().await, collect_alarms, "alarmmod");
                        impl_update_alarms(&mut *app_state.state_alarms.write().await, collect_state_alarms, "statealarmmod");
                    }
                    else => break 'main_loop,
                }
            }
        }
    }

    drop(periodic_sync_tx);
    log::debug!("waiting for periodic sync task to finish");
    if let Err(err) = periodic_sync_task.await {
        log::error!("Failed to join periodic_sync_task: {err}")
    }

    if let Some(online_status_task) = state.online_status_task {
        state.online_status_channels.clear();
        if let Err(err) = online_status_task.await {
            log::error!("Failed to join online_status_task: {err}")
        };
    }

    log::debug!("sites task finished");
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use async_broadcast::Sender;
    use futures::{channel::mpsc::{UnboundedReceiver, UnboundedSender}, StreamExt};
    use shvclient::{clientapi::ClientCommand, ClientEvent};
    use shvproto::RpcValue;
    use shvrpc::{RpcMessageMetaTags, rpcframe::RpcFrame};

    use crate::{dirtylog::DirtyLogCommand, sites::{SiteInfo, SitesCommand, sites_task}, sync::SyncCommand, util::{DedupReceiver, init_logger, testing::{ExpectCall, ExpectSignal, ExpectSubscription, ExpectUnsubscription, PrettyJoinError, SendSignal, TestStep, run_test}}};

    #[test]
    fn parse_notification() {
        let dummy_siteinfo = || SiteInfo { name: Default::default(), site_type: Default::default(), sub_hp: Default::default() };
        let sites_info = BTreeMap::from([
            ("foo/site1".to_string(), dummy_siteinfo()),
            ("foo/site2".to_string(), dummy_siteinfo()),
        ]);
        assert_eq!(super::parse_notification(&shvrpc::RpcMessage::new_signal("something/site1/some_value_node", "chng").with_param(20), &sites_info), None);
        assert_eq!(super::parse_notification(&shvrpc::RpcMessage::new_signal("shv/bar/site1/some_value_node", "chng").with_param(20), &sites_info), None);
        assert_eq!(
            super::parse_notification(&shvrpc::RpcMessage::new_signal("shv/foo/site1/xyz/node", "chng").with_param(20), &sites_info),
            Some(super::ParsedNotification {
                site_path: "foo/site1".into(),
                property_path: "xyz/node".into(),
                signal: "chng".into(),
                source: String::default(),
                param: 20.into(),
                user_id: None,
            })
        );
        assert_eq!(
            super::parse_notification(shvrpc::RpcMessage::new_signal("shv/foo/site2/none", "chng").set_user_id("user"), &sites_info),
            Some(super::ParsedNotification {
                site_path: "foo/site2".into(),
                property_path: "none".into(),
                signal: "chng".into(),
                source: String::default(),
                param: RpcValue::null(),
                user_id: Some("user".into()),
            })
        );
    }

    #[test]
    fn collect_sites() {
        let sites_tree = shvproto::make_map!(
            "site" => shvproto::make_map!(
                "_meta" => shvproto::make_map!("type" => "DepotG3", "name" => "test1", "HP3" => shvproto::make_map!())
            ),
        );
        let sites = super::collect_sites(&[], &sites_tree);
        println!("{sites_tree:#?}");
        println!("sites: {}", sites
            .iter()
            .map(|(path, site)| format!("{path}: {site:?}"))
            .collect::<Vec<_>>()
            .join("\n")
        );
        assert_eq!(
            sites, [
            ("site".to_string(), SiteInfo {
                name: "test1".to_string(),
                site_type: "DepotG3".to_string(),
                sub_hp: Default::default(),
            })
        ]
        .into_iter()
        .collect::<BTreeMap<_,_>>());
    }

    #[test]
    fn collect_records_sub_hp() {
        let sites_tree = RpcValue::from_cpon(r#"{
            "records_site":{
                "_meta":{
                    "HP3":{
                        "type":"records",
                        "records":["passages", "maintenance"]
                    }
                }
            }
        }"#).unwrap();
        let sub_hps = super::collect_sub_hps(&[], sites_tree.as_map());
        let Some(super::SubHpInfo::Records { records }) = sub_hps.get("records_site") else {
            panic!("records_site should be a records sub HP");
        };
        assert_eq!(records, &vec!["passages".to_string(), "maintenance".to_string()]);
    }

    #[async_trait::async_trait]
    impl TestStep<SitesTaskTestState> for ClientEvent {
        async fn exec(&self, _client_command_reciever: &mut UnboundedReceiver<ClientCommand>,_subscriptions: &mut HashMap<String, UnboundedSender<RpcFrame>>, state: &mut SitesTaskTestState) {
            let x = state.sender.clone();
            x.broadcast(self.clone()).await.expect("Sending ClientEvents must work");
        }
    }

    #[async_trait::async_trait]
    impl TestStep<SitesTaskTestState> for SitesCommand {
        async fn exec(&self, _client_command_reciever: &mut UnboundedReceiver<ClientCommand>,_subscriptions: &mut HashMap<String, UnboundedSender<RpcFrame>>, state: &mut SitesTaskTestState) {
            match self {
                SitesCommand::ReloadSites => state.sites_cmd_tx.unbounded_send(SitesCommand::ReloadSites).unwrap(),
            }
        }
    }

    #[derive(Debug)]
    enum ExpectDirtylogCommand {
        ProcessNotification,
    }

    #[async_trait::async_trait]
    impl TestStep<SitesTaskTestState> for ExpectDirtylogCommand {
        async fn exec(&self, _client_command_reciever: &mut UnboundedReceiver<ClientCommand>,_subscriptions: &mut HashMap<String, UnboundedSender<RpcFrame>>, state: &mut SitesTaskTestState) {
            let event = state.dirtylog_cmd_rx.select_next_some().await;
            match (&event, self) {
                (DirtyLogCommand::ProcessNotification(..), ExpectDirtylogCommand::ProcessNotification) => {
                    log::debug!(target: "test-driver", "Got expected notification: {event:?}");
                },
                (got, expected) => {
                    panic!("Unexpected dirtylog command: {got:?}, expected: {expected:?}")
                }
            }
        }
    }

    #[derive(Debug)]
    enum ExpectSyncCommand {
        SyncSite{expected_site: String},
        SyncAll,
    }

    #[async_trait::async_trait]
    impl TestStep<SitesTaskTestState> for ExpectSyncCommand {
        async fn exec(&self, _client_command_reciever: &mut UnboundedReceiver<ClientCommand>,_subscriptions: &mut HashMap<String, UnboundedSender<RpcFrame>>, state: &mut SitesTaskTestState) {
            let Some(event) = state.sync_cmd_rx.next().await else {
                panic!("Expected a SyncCommand, but got none");
            };

            match (event, self) {
                (SyncCommand::SyncSite(site), ExpectSyncCommand::SyncSite { expected_site }) => {
                    assert_eq!(site, *expected_site);
                    log::debug!(target: "test-driver", "Got expected SyncSite({site})");
                },
                (SyncCommand::SyncAll, ExpectSyncCommand::SyncAll) => {
                    log::debug!(target: "test-driver", "Got expected SyncAll");
                },
                (got, expected) => {
                    panic!("Unexpected dirtylog command: {got:?}, expected: {expected:?}")
                }
            }
        }
    }

    struct SitesTaskTestState {
        sender: Sender<ClientEvent>,
        dirtylog_cmd_rx: UnboundedReceiver<DirtyLogCommand>,
        sync_cmd_rx: DedupReceiver<SyncCommand>,
        sites_cmd_tx: UnboundedSender<SitesCommand>,
    }

    struct TestCase<'a> {
        name: &'static str,
        steps: &'a [Box<dyn TestStep<SitesTaskTestState>>],
        starting_files: Vec<(&'static str, &'static str)>,
        expected_file_paths: Vec<(&'static str, &'a str)>,
        cleanup_steps: &'a [Box<dyn TestStep<SitesTaskTestState>>],
    }

    fn no_sites() -> RpcValue {
        RpcValue::from_cpon(r#"{
            "_meta":{
                "HP3":{"type": "HP3"}
            },
        }"#).unwrap()
    }

    fn some_broker() -> RpcValue {
        RpcValue::from_cpon(r#"{
            "_meta":{
                "HP3":{"type": "HP3"}
            },
            "node":{
                "_meta":{
                    "HP3":{"syncPath":".app/shvjournal"},
                    "type":"DepotG2"
                }
            },
            "pushlog_node":{
                "_meta":{
                    "HP3":{
                        "pushLog": true
                    }
                }
            },
            "legacy_sync_path_device":{
                "_meta":{
                    "HP":{
                    },
                    "type": "some_type"
                }
            },
            "node_with_hp_meta":{
                "_meta":{
                    "HP3":{
                        "readLogChunkLimit": 1000,
                        "syncPath": "test_sync_path"
                    },
                    "type": "some_type"
                }
            },
        }"#).unwrap()
    }

    fn records_broker() -> RpcValue {
        RpcValue::from_cpon(r#"{
            "_meta":{
                "HP3":{"type": "HP3"}
            },
            "records_node":{
                "_meta":{
                    "HP3":{
                        "type":"records",
                        "records":["maintenance", "passage"]
                    },
                    "type":"some_type"
                }
            },
        }"#).unwrap()
    }

    #[tokio::test]
    async fn sites_task_test() -> std::result::Result<(), PrettyJoinError> {
        init_logger();

        let test_cases = [
            TestCase {
                name: "disconnected when uninitialized",
                steps: &[
                    Box::new(ClientEvent::Disconnected),
                ],
                starting_files: vec![],
                expected_file_paths: vec![],
                cleanup_steps: &[],
            },
            TestCase {
                name: "Empty sites",
                steps: &[
                    Box::new(ClientEvent::Connected(shvclient::clientapi::ShvApiVersion::V3)),
                    Box::new(ExpectCall("sites", "getSites", Ok(no_sites()))),
                ],
                starting_files: vec![],
                expected_file_paths: vec![],
                cleanup_steps: &[],
            },
            TestCase {
                name: "Records HP subscriptions and sync trigger",
                steps: &[
                    Box::new(ClientEvent::Connected(shvclient::clientapi::ShvApiVersion::V3)),
                    Box::new(ExpectCall("sites", "getSites", Ok(records_broker()))),
                    Box::new(ExpectSubscription("shv/records_node/*:*:mntchng".try_into().unwrap())),
                    Box::new(ExpectSubscription("shv/records_node/*:*:chng".try_into().unwrap())),
                    Box::new(ExpectSubscription("shv/records_node/*:*:cmdlog".try_into().unwrap())),
                    Box::new(ExpectCall("sites/records_node/_files", "ls", Ok(shvproto::List::new().into()))),
                    Box::new(ExpectSyncCommand::SyncAll),
                    Box::new(SendSignal("shv/records_node/*:*:mntchng".to_string(), "shv/records_node".to_string(), "mntchng".to_string(), true.into())),
                    Box::new(ExpectSignal("records_node", "onlinestatuschng", 2.into())),
                    Box::new(ExpectSyncCommand::SyncSite { expected_site: "records_node".to_string() }),
                    Box::new(ClientEvent::Disconnected),
                    Box::new(ExpectSignal("records_node", "onlinestatuschng", 0.into())),
                ],
                starting_files: vec![],
                expected_file_paths: vec![],
                cleanup_steps: &[
                    Box::new(ExpectUnsubscription),
                    Box::new(ExpectUnsubscription),
                    Box::new(ExpectUnsubscription),
                ],
            },
            TestCase {
                name: "Test everything",
                steps: &[
                    Box::new(ClientEvent::Connected(shvclient::clientapi::ShvApiVersion::V3)),
                    Box::new(ExpectCall("sites", "getSites", Ok(some_broker()))),
                    Box::new(ExpectSubscription("shv/legacy_sync_path_device/*:*:mntchng".try_into().unwrap())),
                    Box::new(ExpectSubscription("shv/node/*:*:mntchng".try_into().unwrap())),
                    Box::new(ExpectSubscription("shv/node_with_hp_meta/*:*:mntchng".try_into().unwrap())),
                    Box::new(ExpectSubscription("shv/legacy_sync_path_device/*:*:chng".try_into().unwrap())),
                    Box::new(ExpectSubscription("shv/legacy_sync_path_device/*:*:cmdlog".try_into().unwrap())),
                    Box::new(ExpectSubscription("shv/node/*:*:chng".try_into().unwrap())),
                    Box::new(ExpectSubscription("shv/node/*:*:cmdlog".try_into().unwrap())),
                    Box::new(ExpectSubscription("shv/node_with_hp_meta/*:*:chng".try_into().unwrap())),
                    Box::new(ExpectSubscription("shv/node_with_hp_meta/*:*:cmdlog".try_into().unwrap())),
                    Box::new(ExpectCall("sites/legacy_sync_path_device/_files", "ls", Ok(shvproto::List::new().into()))),
                    Box::new(ExpectCall("sites/node/_files", "ls", Ok(shvproto::List::new().into()))),
                    Box::new(ExpectCall("sites/node_with_hp_meta/_files", "ls", Ok(shvproto::List::new().into()))),
                    Box::new(ExpectSyncCommand::SyncAll),
                    Box::new(SendSignal("shv/node/*:*:chng".to_string(), "shv/node/some_value".to_string(), "chng".to_string(), RpcValue::null())),
                    Box::new(ExpectDirtylogCommand::ProcessNotification),
                    Box::new(SendSignal("shv/node/*:*:mntchng".to_string(), "shv/node".to_string(), "mntchng".to_string(), true.into())),
                    Box::new(ExpectSignal("node", "onlinestatuschng", 2.into())),
                    Box::new(ExpectSyncCommand::SyncSite { expected_site: "node".to_string() }),
                    Box::new(ClientEvent::Disconnected),
                    Box::new(ExpectSignal("node", "onlinestatuschng", 0.into())),
                ],
                starting_files: vec![],
                expected_file_paths: vec![],
                cleanup_steps: &[
                    Box::new(ExpectUnsubscription),
                    Box::new(ExpectUnsubscription),
                    Box::new(ExpectUnsubscription),
                    Box::new(ExpectUnsubscription),
                    Box::new(ExpectUnsubscription),
                    Box::new(ExpectUnsubscription),
                    Box::new(ExpectUnsubscription),
                    Box::new(ExpectUnsubscription),
                    Box::new(ExpectUnsubscription),
                ],
            },
            TestCase {
                name: "Reload sites reuses subscriptions",
                steps: &[
                    Box::new(ClientEvent::Connected(shvclient::clientapi::ShvApiVersion::V3)),
                    Box::new(ExpectCall("sites", "getSites", Ok(some_broker()))),
                    Box::new(ExpectSubscription("shv/legacy_sync_path_device/*:*:mntchng".try_into().unwrap())),
                    Box::new(ExpectSubscription("shv/node/*:*:mntchng".try_into().unwrap())),
                    Box::new(ExpectSubscription("shv/node_with_hp_meta/*:*:mntchng".try_into().unwrap())),
                    Box::new(ExpectSubscription("shv/legacy_sync_path_device/*:*:chng".try_into().unwrap())),
                    Box::new(ExpectSubscription("shv/legacy_sync_path_device/*:*:cmdlog".try_into().unwrap())),
                    Box::new(ExpectSubscription("shv/node/*:*:chng".try_into().unwrap())),
                    Box::new(ExpectSubscription("shv/node/*:*:cmdlog".try_into().unwrap())),
                    Box::new(ExpectSubscription("shv/node_with_hp_meta/*:*:chng".try_into().unwrap())),
                    Box::new(ExpectSubscription("shv/node_with_hp_meta/*:*:cmdlog".try_into().unwrap())),
                    Box::new(ExpectCall("sites/legacy_sync_path_device/_files", "ls", Ok(shvproto::List::new().into()))),
                    Box::new(ExpectCall("sites/node/_files", "ls", Ok(shvproto::List::new().into()))),
                    Box::new(ExpectCall("sites/node_with_hp_meta/_files", "ls", Ok(shvproto::List::new().into()))),
                    Box::new(ExpectSyncCommand::SyncAll),
                    Box::new(SitesCommand::ReloadSites),
                    Box::new(ExpectCall("sites", "getSites", Ok(some_broker()))),
                    Box::new(ExpectCall("sites/legacy_sync_path_device/_files", "ls", Ok(shvproto::List::new().into()))),
                    Box::new(ExpectCall("sites/node/_files", "ls", Ok(shvproto::List::new().into()))),
                    Box::new(ExpectCall("sites/node_with_hp_meta/_files", "ls", Ok(shvproto::List::new().into()))),
                    Box::new(ExpectSyncCommand::SyncAll),
                ],
                starting_files: vec![],
                expected_file_paths: vec![],
                cleanup_steps: &[
                    Box::new(ExpectUnsubscription),
                    Box::new(ExpectUnsubscription),
                    Box::new(ExpectUnsubscription),
                    Box::new(ExpectUnsubscription),
                    Box::new(ExpectUnsubscription),
                    Box::new(ExpectUnsubscription),
                    Box::new(ExpectUnsubscription),
                    Box::new(ExpectUnsubscription),
                    Box::new(ExpectUnsubscription),
                ],
            },
            TestCase {
                name: "Periodic sync",
                steps: &[
                    Box::new(ClientEvent::Connected(shvclient::clientapi::ShvApiVersion::V3)),
                    Box::new(ExpectCall("sites", "getSites", Ok(no_sites()))),
                    Box::new(ExpectSyncCommand::SyncAll),
                    Box::new(ExpectSyncCommand::SyncAll),
                ],
                starting_files: vec![],
                expected_file_paths: vec![],
                cleanup_steps: &[],
            },
        ];


        for test_case in test_cases {
            run_test(
                test_case.name,
                test_case.steps,
                test_case.starting_files,
                test_case.expected_file_paths,
                |ccs, ces, cer, dirtylog_cmd_rx, sync_cmd_rx, state| {
                    let (sites_cmd_tx, sites_cmd_rx) = futures::channel::mpsc::unbounded();
                    let task_state = SitesTaskTestState {
                        sender: ces,
                        dirtylog_cmd_rx,
                        sync_cmd_rx,
                        sites_cmd_tx,
                    };
                    let sites_task = tokio::spawn(sites_task(ccs, cer, state, sites_cmd_rx));
                    (sites_task, task_state)
                },
                |state| {
                    state.sites_cmd_tx.close_channel();
                    state.sender.close();
                },
                test_case.cleanup_steps
            ).await?;
        }

        Ok(())
    }
}
