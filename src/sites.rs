use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures::channel::mpsc::UnboundedReceiver;
use futures::stream::{FuturesUnordered, SelectAll};
use futures::StreamExt;
use log::{debug, error, warn};
use shvclient::client::{CallRpcMethodErrorKind, RpcCall, RpcCallDirExists, RpcCallLsList};
use shvclient::clientnode::{find_longest_path_prefix, METH_DIR, SIG_CHNG};
use shvclient::{AppState, ClientEventsReceiver};
use shvproto::{DateTime, RpcValue};
use shvrpc::rpcmessage::{RpcError, RpcErrorCode};
use shvrpc::{join_path, RpcMessageMetaTags};
use tokio::time::timeout;

use crate::alarm::{collect_alarms, Alarm};
use crate::tree::getlog_handler;
use crate::typeinfo::TypeInfo;
use crate::util::{subscribe, subscription_prefix_path};
use crate::{AlarmWithTimestamp, ClientCommandSender, State, Subscriber};

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) enum SiteOnlineStatus {
    #[default]
    Unknown,
    Offline,
    Online,
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
        // Using the `type` node to detect sites.
        return sites_subtree
            .get("HP")
            .or_else(|| sites_subtree.get("HP3"))
            .and_then(|_| sites_subtree.get("type"))
            .and_then(|v| match &v.value {
                shvproto::Value::String(site_type) => Some(site_type),
                _ => None,
            }
            )
            .map_or_else(
                BTreeMap::new,
                |site_type|
                    BTreeMap::from([(
                        path_prefix.join("/"),
                        SiteInfo {
                            name: sites_subtree
                                .get("name")
                                .map(RpcValue::as_str)
                                .unwrap_or_default()
                                .into(),
                            site_type: site_type.to_string(),
                            sub_hp: Default::default(),
                        },
                    )])
            );
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

pub(crate) fn update_alarms(alarms_for_site: &mut Vec<AlarmWithTimestamp>, type_info: &TypeInfo, property_path: &str, value: &RpcValue, timestamp: shvproto::DateTime) -> Vec<AlarmWithTimestamp> {
    let new_alarms = collect_alarms(type_info, property_path, value)
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
    app_state: &AppState<State>
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
        .send_message(shvrpc::RpcMessage::new_signal(site, "onlinestatuschng", Some((new_status as i32).into())))
        .unwrap_or_else(|err| log::error!(target: "OnlineStatus", "[{site}] Cannot send 'onlinestatuschng' signal: {err}"));

    const SITE_OFFLINE_ALARM_KEY: &str = "site-offline";

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
            .send_message(shvrpc::RpcMessage::new_signal(site, "alarmmod", None))
            .unwrap_or_else(|err| log::error!(target: "OnlineStatus", "[{site}] Cannot send 'alarmmod' signal: {err}"));
    }
}

fn online_status_worker(
    site: impl Into<String>,
    mut events: UnboundedReceiver<SiteOnlineStatus>,
    client_commands: ClientCommandSender,
    app_state: AppState<State>
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
                    if dir_result.is_ok() {
                        set_online_status(&site, SiteOnlineStatus::Online, &client_commands, &app_state).await;
                    } else if let Err(err) = dir_result
                        && let CallRpcMethodErrorKind::RpcError(RpcError { code, .. }) = err.error()
                        && (*code == RpcErrorCode::MethodCallTimeout || *code == RpcErrorCode::MethodNotFound)
                    {
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
    pub(crate) param: RpcValue,
}

pub(crate) fn parse_notification(msg: &shvrpc::RpcMessage, sites_info: &BTreeMap<String, SiteInfo>) -> Option<ParsedNotification> {
    let signal = msg.method().unwrap_or_default().to_string();
    let path = msg.shv_path().unwrap_or_default().to_string();
    let param = msg.param().unwrap_or_default();
    let stripped_path = path.strip_prefix("shv/")?;
    let (site_path, property_path) = find_longest_path_prefix(sites_info, stripped_path)?;
    Some(ParsedNotification {
        site_path: site_path.to_string(),
        property_path: property_path.to_string(),
        signal: signal.to_string(),
        param: param.clone(),
    })
}

pub(crate) async fn sites_task(
    client_cmd_tx: ClientCommandSender,
    client_evt_rx: ClientEventsReceiver,
    app_state: AppState<State>,
)
{
    let mut client_evt_rx = client_evt_rx.fuse();
    let mut mntchng_subscribers = SelectAll::<Subscriber>::default();
    let mut subscribers = SelectAll::<Subscriber>::default();
    let mut online_status_channels = BTreeMap::new();

    enum PeriodicSyncCommand {
        Enable,
        Disable,
    }

    let (periodic_sync_tx, mut periodic_sync_rx) = futures::channel::mpsc::unbounded();
    {
        let app_state = app_state.clone();
        tokio::spawn(async move {
            const PERIODIC_SYNC_INTERVAL_DEFAULT: u64 = 60 * 60;
            let periodic_sync_interval = app_state.config.periodic_sync_interval.unwrap_or(PERIODIC_SYNC_INTERVAL_DEFAULT);
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
        });
    }

    loop {
        futures::select! {
            client_event = client_evt_rx.select_next_some() => match client_event {
                shvclient::ClientEvent::Connected(shv_api_version) => {
                    log::info!("Getting sites info");

                    let sites = RpcCall::new("sites", "getSites")
                        .exec(&client_cmd_tx)
                        .await;

                    let (sites_info, sub_hps) = match sites {
                        Ok(sites) => {
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
                            (Arc::new(sites_info), Arc::new(sub_hps))
                        }
                        Err(err) => match err.error() {
                            CallRpcMethodErrorKind::ConnectionClosed => {
                                log::warn!("Connection closed while getting sites info");
                                continue
                            }
                            _ => {
                                log::error!("Get sites info error: {err}");
                                Default::default()
                            }
                        }
                    };

                    log::info!("Loaded sites:\n{}", sites_info
                        .iter()
                        .map(|(path, site)| format!(" {path}: {site:?}"))
                        .collect::<Vec<_>>()
                        .join("\n")
                    );

                    log::info!("Loaded sub HPs:\n{}", sub_hps
                        .iter()
                        .map(|(path, hp)| format!(" {path}: {hp:?}"))
                        .collect::<Vec<_>>()
                        .join("\n")
                    );

                    // Subscribe mntchng
                    mntchng_subscribers = sites_info
                        .keys()
                        .map(|path| {
                            subscribe(&client_cmd_tx, subscription_prefix_path(join_path!("shv", path), &shv_api_version), "mntchng")
                        })
                        .collect::<FuturesUnordered<_>>()
                        .collect::<SelectAll<_>>()
                        .await;


                    subscribers = sites_info
                        .keys()
                        .flat_map(|path| {
                            let shv_path = join_path!("shv", path);
                            let sub_chng = subscribe(&client_cmd_tx, subscription_prefix_path(&shv_path, &shv_api_version), SIG_CHNG);
                            const SIG_CMDLOG: &str = "cmdlog";
                            let sub_cmdlog = subscribe(&client_cmd_tx, subscription_prefix_path(&shv_path, &shv_api_version), SIG_CMDLOG);
                            [sub_chng, sub_cmdlog]
                        })
                        .collect::<FuturesUnordered<_>>()
                        .collect::<SelectAll<_>>()
                        .await;

                    let typeinfos = sites_info
                        .keys()
                        .map(|path| {
                            let client_cmd_tx = client_cmd_tx.clone();
                            async move {
                                let result = 'result: {
                                    let files_path = join_path!("sites", &path, "_files");
                                    let files = RpcCallLsList::new(&files_path).exec(&client_cmd_tx)
                                        .await
                                        .unwrap_or_else(|err| panic!("Couldn't discover typeInfo support for {files_path}: {err}"));
                                    let Some(type_info_filename) = files
                                        .into_iter()
                                        .find(|file| file == "typeInfo.cpon") else {
                                            break 'result Err(format!("No typeInfo.cpon found for site {path}"));
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

                    let params = crate::journalrw::GetLog2Params {
                        since: crate::journalrw::GetLog2Since::LastEntry,
                        with_snapshot: true,
                        ..Default::default()
                    };
                    let mut alarms = BTreeMap::<String, Vec<AlarmWithTimestamp>>::new();
                    for site_path in sites_info.keys() {
                        let Some(Ok(type_info)) = typeinfos.get(site_path) else {
                            // No typeinfo for this site - skip
                            continue;
                        };

                        let log = match getlog_handler(site_path, &params, app_state.clone()).await {
                            Ok(log) => log,
                            Err(err) => {
                                log::error!("couldn't init alarms: getlog failed: {err}");
                                continue;
                            }
                        };

                        let alarms_for_site = alarms.entry(site_path.to_string()).or_default();

                        let chained_entries = log.snapshot_entries.iter().map(Arc::as_ref).chain(log.event_entries.iter().map(Arc::as_ref));
                        for entry in chained_entries  {
                            update_alarms(alarms_for_site, type_info, &entry.path, &entry.value, shvproto::DateTime::from_epoch_msec(entry.epoch_msec));
                        }
                    }

                    *app_state.alarms.write().await = alarms;

                    let mut online_status_workers = Vec::new();
                    for (site, info) in sites_info.iter() {
                        if sub_hps.get(&info.sub_hp).is_none_or(|sub_hp| matches!(sub_hp, SubHpInfo::PushLog)) {
                            continue
                        };
                        let (tx, rx) = futures::channel::mpsc::unbounded();
                        online_status_channels.insert(site.to_string(), tx);
                        online_status_workers.push(online_status_worker(site.clone(), rx, client_cmd_tx.clone(), app_state.clone()));
                    }
                    tokio::spawn(async move {
                        debug!(target: "OnlineStatus", "online status task starts");
                        futures::future::join_all(online_status_workers).await;
                        debug!(target: "OnlineStatus", "online status task finish");
                    });
                    *app_state.online_states.write().await = sites_info.keys().map(|site| (site.clone(), Default::default())).collect();

                    periodic_sync_tx
                        .unbounded_send(PeriodicSyncCommand::Enable)
                        .unwrap_or_else(|e| log::error!("Cannot send periodic sync enable command: {e}"));

                }
                _ => {
                    // TODO: Drain subscribers

                    subscribers.clear();
                    mntchng_subscribers.clear();
                    online_status_channels.clear();
                    periodic_sync_tx
                        .unbounded_send(PeriodicSyncCommand::Disable)
                        .unwrap_or_else(|e| log::error!("Cannot send periodic sync disable command: {e}"));
                }
            },
            mntchng_frame = mntchng_subscribers.select_next_some() => {
                let msg = match mntchng_frame.to_rpcmesage() {
                    Ok(msg) => msg,
                    Err(err) => {
                        warn!("Ignoring wrong RpcFrame: {err}");
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

                    if let Some(tx) = online_status_channels.get(&site_path) {
                        tx.unbounded_send(SiteOnlineStatus::Online).ok();
                    }
                } else {
                    log::info!("Site unmounted: {site_path}");
                    if let Some(tx) = online_status_channels.get(&site_path) {
                        tx.unbounded_send(SiteOnlineStatus::Offline).ok();
                    }
                }
            }
            notification_frame = subscribers.select_next_some() => {
                let msg = match notification_frame.to_rpcmesage() {
                    Ok(msg) => msg,
                    Err(err) => {
                        warn!("Ignoring wrong RpcFrame: {err}");
                        continue;
                    }
                };
                let Some(parsed_notification) = parse_notification(&msg, &app_state.sites_data.read().await.sites_info) else {
                    continue
                };
                if let Some(tx) = online_status_channels.get(&parsed_notification.site_path) {
                    tx.unbounded_send(SiteOnlineStatus::Online).ok();
                }

                app_state.dirtylog_cmd_tx
                    .unbounded_send(crate::dirtylog::DirtyLogCommand::ProcessNotification(parsed_notification.clone()))
                    .unwrap_or_else(|e| log::error!("Cannot send dirtylog ProcessNotification command: {e}"));

                let typeinfos = &app_state.sites_data.read().await.typeinfos;
                let Some(Ok(type_info)) = typeinfos.get(&parsed_notification.site_path) else {
                    continue;
                };

                let alarms = &mut *app_state.alarms.write().await;
                let alarms_for_site = alarms.entry(parsed_notification.site_path.clone()).or_default();

                let updated = update_alarms(alarms_for_site, type_info, &parsed_notification.property_path, &parsed_notification.param, shvproto::DateTime::now());
                if !updated.is_empty() {
                    client_cmd_tx.send_message(shvrpc::RpcMessage::new_signal(&parsed_notification.site_path, "alarmmod", None))
                        .unwrap_or_else(|err| log::error!("alarms: Cannot send signal ({err})"));
                }
            }
            complete => break,
        }
    }
    log::debug!("sites task finished");
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use async_broadcast::Sender;
    use futures::{channel::mpsc::{UnboundedReceiver, UnboundedSender}, StreamExt};
    use shvclient::{client::ClientCommand, ClientEvent};
    use shvproto::RpcValue;
    use shvrpc::rpcframe::RpcFrame;

    use crate::{dirtylog::DirtyLogCommand, sites::{sites_task, SiteInfo}, sync::SyncCommand, util::{init_logger, testing::{run_test, ExpectCall, ExpectSignal, ExpectSubscription, ExpectUnsubscription, PrettyJoinError, SendSignal, TestStep}, DedupReceiver}, State};

    #[test]
    fn parse_notification() {
        let dummy_siteinfo = || SiteInfo { name: Default::default(), site_type: Default::default(), sub_hp: Default::default() };
        let sites_info = BTreeMap::from([
            ("foo/site1".to_string(), dummy_siteinfo()),
            ("foo/site2".to_string(), dummy_siteinfo()),
        ]);
        assert_eq!(super::parse_notification(&shvrpc::RpcMessage::new_signal("something/site1/some_value_node", "chng", Some(20.into())), &sites_info), None);
        assert_eq!(super::parse_notification(&shvrpc::RpcMessage::new_signal("shv/bar/site1/some_value_node", "chng", Some(20.into())), &sites_info), None);
        assert_eq!(
            super::parse_notification(&shvrpc::RpcMessage::new_signal("shv/foo/site1/xyz/node", "chng", Some(20.into())), &sites_info),
            Some(super::ParsedNotification {
                site_path: "foo/site1".into(),
                property_path: "xyz/node".into(),
                signal: "chng".into(),
                param: 20.into(),
            })
        );
        assert_eq!(
            super::parse_notification(&shvrpc::RpcMessage::new_signal("shv/foo/site2/none", "chng", None), &sites_info),
            Some(super::ParsedNotification {
                site_path: "foo/site2".into(),
                property_path: "none".into(),
                signal: "chng".into(),
                param: RpcValue::null(),
            })
        );
    }

    #[test]
    fn collect_sites() {
        let sites_tree = shvproto::make_map!(
            "site" => shvproto::make_map!(
                "_meta" => shvproto::make_map!("type" => "DepotG3", "name" => "test1", "HP3" => "{}")
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

    #[async_trait::async_trait]
    impl TestStep<SitesTaskTestState> for ClientEvent {
        async fn exec(&self, _client_command_reciever: &mut UnboundedReceiver<ClientCommand<State>>,_subscriptions: &mut HashMap<String, UnboundedSender<RpcFrame>>, state: &mut SitesTaskTestState) {
            let x = state.sender.clone();
            x.broadcast(self.clone()).await.expect("Sending ClientEvents must work");
        }
    }

    #[derive(Debug)]
    enum ExpectDirtylogCommand {
        ProcessNotification,
    }

    #[async_trait::async_trait]
    impl TestStep<SitesTaskTestState> for ExpectDirtylogCommand {
        async fn exec(&self, _client_command_reciever: &mut UnboundedReceiver<ClientCommand<State>>,_subscriptions: &mut HashMap<String, UnboundedSender<RpcFrame>>, state: &mut SitesTaskTestState) {
            let event = state.dirtylog_cmd_rx.select_next_some().await;
            match (event, self) {
                (DirtyLogCommand::ProcessNotification(..), ExpectDirtylogCommand::ProcessNotification) => {
                    log::debug!(target: "test-driver", "Got expected notification");
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
        async fn exec(&self, _client_command_reciever: &mut UnboundedReceiver<ClientCommand<State>>,_subscriptions: &mut HashMap<String, UnboundedSender<RpcFrame>>, state: &mut SitesTaskTestState) {
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
    }

    struct TestCase<'a> {
        name: &'static str,
        steps: &'a [Box<dyn TestStep<SitesTaskTestState>>],
        starting_files: Vec<(&'static str, &'static str)>,
        expected_file_paths: Vec<(&'static str, &'a str)>,
        cleanup_steps: &'a [Box<dyn TestStep<SitesTaskTestState>>],
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
                    Box::new(ClientEvent::Connected(shvclient::client::ShvApiVersion::V3)),
                    Box::new(ExpectCall("sites", "getSites", Ok(shvproto::Map::new().into()))),
                ],
                starting_files: vec![],
                expected_file_paths: vec![],
                cleanup_steps: &[],
            },
            TestCase {
                name: "Test everything",
                steps: &[
                    Box::new(ClientEvent::Connected(shvclient::client::ShvApiVersion::V3)),
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
                    Box::new(ExpectSignal("node", "onlinestatuschng", 0.into())),
                ],
            },
            TestCase {
                name: "Periodic sync",
                steps: &[
                    Box::new(ClientEvent::Connected(shvclient::client::ShvApiVersion::V3)),
                    Box::new(ExpectCall("sites", "getSites", Ok(shvproto::Map::new().into()))),
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
                    let task_state = SitesTaskTestState {
                        sender: ces,
                        dirtylog_cmd_rx,
                        sync_cmd_rx,
                    };
                    let sites_task = tokio::spawn(sites_task(ccs, cer, state));
                    (sites_task, task_state)
                },
                |state| {
                    state.sender.close();
                },
                test_case.cleanup_steps
            ).await?;
        }

        Ok(())
    }
}

