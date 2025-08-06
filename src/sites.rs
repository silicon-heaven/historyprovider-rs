use std::collections::BTreeMap;
use std::sync::Arc;

use futures::stream::{FuturesUnordered, SelectAll};
use futures::StreamExt;
use log::warn;
use shvclient::client::{CallRpcMethodErrorKind, RpcCall};
use shvclient::clientnode::find_longest_path_prefix;
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
                                getlog_path: if is_device { LEGACY_SYNC_PATH_DEVICE }
                                         else { LEGACY_SYNC_PATH_HP }.to_string(),
                            }
                        } else {
                            SubHpInfo::Normal {
                                sync_path: hp
                                    .get("syncPath")
                                    .map(RpcValue::as_str)
                                    .unwrap_or_else(||
                                        if is_device { DEFAULT_SYNC_PATH_DEVICE }
                                        else { DEFAULT_SYNC_PATH_HP }
                                    )
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

pub(crate) async fn load_sites(
    client_cmd_tx: ClientCommandSender,
    client_evt_rx: ClientEventsReceiver,
    app_state: AppState<State>,
)
{
    let mut client_evt_rx = client_evt_rx.fuse();
    let mut mntchng_subscribers = SelectAll::<Subscriber>::default();

    loop {
        tokio::select! {
            client_event = client_evt_rx.next() => match client_event {
                Some(client_event) => if let shvclient::ClientEvent::Connected(shv_api_version) = client_event {
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

                    *app_state.sites_data.write().await = SitesData { sites_info, sub_hps };
                    app_state.sync_cmd_tx.unbounded_send(crate::sync::SyncCommand::SyncAll)
                        .unwrap_or_else(|e| panic!("Cannot send SyncAll: {e}"));
                    app_state.dirtylog_cmd_tx.unbounded_send(crate::dirtylog::DirtyLogCommand::Subscribe(shv_api_version))
                        .unwrap_or_else(|e| panic!("Cannot send dirtylog Subscribe command: {e}"));
                },
                None => break,
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
                if !msg.param().map_or_else(|| false, RpcValue::as_bool) {
                    log::info!("Site unmounted: {site_path}");
                    continue;
                }
                log::info!("Site mounted: {site_path}");
                app_state.sync_cmd_tx.unbounded_send(crate::sync::SyncCommand::SyncSite(site_path.into()))
                    .unwrap_or_else(|e| panic!("Cannot send SyncSite({site_path}) command: {e}"));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::sites::SiteInfo;

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
}

