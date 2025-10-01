use std::collections::BTreeMap;
use std::sync::Arc;

use futures::stream::{FuturesUnordered, SelectAll};
use futures::StreamExt;
use log::warn;
use shvclient::client::{CallRpcMethodErrorKind, RpcCall};
use shvclient::clientnode::{find_longest_path_prefix, SIG_CHNG};
use shvclient::{AppState, ClientEventsReceiver};
use shvproto::RpcValue;
use shvrpc::{join_path, RpcMessageMetaTags};

use crate::util::{subscribe, subscription_prefix_path};
use crate::{ClientCommandSender, State, Subscriber};


#[derive(Clone,Default)]
pub(crate) struct SitesData {
    pub(crate) sites_info: Arc<BTreeMap<String, SiteInfo>>,
    pub(crate) sub_hps: Arc<BTreeMap<String, SubHpInfo>>,
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

const SIG_CMDLOG: &str = "cmdlog";

pub(crate) async fn sites_task(
    client_cmd_tx: ClientCommandSender,
    client_evt_rx: ClientEventsReceiver,
    app_state: AppState<State>,
)
{
    let mut client_evt_rx = client_evt_rx.fuse();
    let mut mntchng_subscribers = SelectAll::<Subscriber>::default();
    let mut subscribers = SelectAll::<Subscriber>::default();

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

                    let (sites_info, sub_hps) = match sites
                        .map(|sites: shvproto::Map| {
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
                        }) {
                            Ok(res) => res,
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
                            let sub_cmdlog = subscribe(&client_cmd_tx, subscription_prefix_path(&shv_path, &shv_api_version), SIG_CMDLOG);
                            [sub_chng, sub_cmdlog]
                        })
                        .collect::<FuturesUnordered<_>>()
                        .collect::<SelectAll<_>>()
                        .await;

                    *app_state.sites_data.write().await = SitesData { sites_info, sub_hps };

                    periodic_sync_tx
                        .unbounded_send(PeriodicSyncCommand::Enable)
                        .unwrap_or_else(|e| log::error!("Cannot send periodic sync enable command: {e}"));

                }
                _ => {
                    // TODO: Drain subscribers

                    subscribers.clear();
                    mntchng_subscribers.clear();
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
                let signal = msg.method().unwrap_or_default().to_string();
                if signal != "mntchng" {
                    continue;
                }
                let path = msg.shv_path().unwrap_or_default().to_string();

                let Some(stripped_path) = path.strip_prefix("shv/") else {
                    continue;
                };
                let Some((site_path, _)) = find_longest_path_prefix(&*app_state.sites_data.read().await.sites_info, stripped_path) else {
                    continue;
                };
                if !msg.param().is_some_and(RpcValue::as_bool) {
                    log::info!("Site unmounted: {site_path}");
                    continue;
                }
                log::info!("Site mounted: {site_path}");
                app_state.sync_cmd_tx
                    .send(crate::sync::SyncCommand::SyncSite(site_path.into()))
                    .map(|_|())
                    .unwrap_or_else(|e| panic!("Cannot send SyncSite({site_path}) command: {e}"));
            }
            notification_frame = subscribers.select_next_some() => {
                let msg = match notification_frame.to_rpcmesage() {
                    Ok(msg) => msg,
                    Err(err) => {
                        warn!("Ignoring wrong RpcFrame: {err}");
                        continue;
                    }
                };
                app_state.dirtylog_cmd_tx
                    .unbounded_send(crate::dirtylog::DirtyLogCommand::ProcessNotification(msg))
                    .unwrap_or_else(|e| log::error!("Cannot send dirtylog ProcessNotification command: {e}"));
            }
            complete => break,
        }
    }
    eprintln!("task end");
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use async_broadcast::Sender;
    use futures::{channel::mpsc::{UnboundedReceiver, UnboundedSender}, StreamExt};
    use shvclient::{client::ClientCommand, ClientEvent};
    use shvproto::RpcValue;
    use shvrpc::rpcframe::RpcFrame;

    use crate::{State, dirtylog::DirtyLogCommand, sites::{SiteInfo, sites_task}, sync::SyncCommand, util::{DedupReceiver, init_logger, testing::{ExpectCall, ExpectSubscription, ExpectUnsubscription, PrettyJoinError, SendSignal, TestStep, run_test}}};

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
                    Box::new(SendSignal("shv/node/*:*:chng".to_string(), "shv/node/some_value".to_string(), "chng".to_string(), RpcValue::null())),
                    Box::new(ExpectDirtylogCommand::ProcessNotification),
                    Box::new(SendSignal("shv/node/*:*:mntchng".to_string(), "shv/node".to_string(), "mntchng".to_string(), true.into())),
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

