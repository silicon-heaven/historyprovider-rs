use std::collections::BTreeMap;

use futures::StreamExt;
use shvclient::client::CallRpcMethodErrorKind;
use shvclient::{AppState, ClientEventsReceiver};
use shvproto::RpcValue;
use tokio::sync::RwLock;

use crate::{ClientCommandSender, State};

pub struct Sites(pub(crate) RwLock<BTreeMap<String, Site>>);

#[derive(Debug, PartialEq)]
pub struct Site {
    pub name: String,
    pub site_type: String,
}

fn collect_sites<'a>(
    path_segments: &[&'a str],
    sites_subtree: &'a shvproto::Map,
) -> BTreeMap<String, Site>
{
    if let Some((&"_meta", path_prefix)) = path_segments.split_last() {
        // Using the `type` node to detect sites.
        return sites_subtree
            .get("type")
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
                        Site {
                            name: sites_subtree
                                .get("name")
                                .map(RpcValue::as_str)
                                .unwrap_or_default()
                                .into(),
                            site_type: site_type.to_string(),
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
pub(crate) enum SubHp {
    Normal {
        // SHV path of the log files relative to the sub HP node
        sync_path: String,
    },
    Legacy {
        // SHV path of the legacy HP relative to the sub HP node
        hp_path: String,
    },
    PushLog,
}

const DEFAULT_SYNC_PATH_DEVICE: &str = ".app/history";
const DEFAULT_SYNC_PATH_HP: &str = ".local/history/_shvjournal";
const LEGACY_SYNC_PATH_DEVICE: &str = "";
const LEGACY_SYNC_PATH_HP: &str = ".local/history";

fn collect_sub_hps<'a>(
    path_segments: &[&'a str],
    sites_subtree: &'a shvproto::Map,
) -> BTreeMap<String, SubHp>
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
                            SubHp::PushLog
                        } else if meta.contains_key("HP") {
                            SubHp::Legacy {
                                hp_path: if is_device { LEGACY_SYNC_PATH_DEVICE }
                                         else { LEGACY_SYNC_PATH_HP }.to_string(),
                            }
                        } else {
                            SubHp::Normal {
                                sync_path: hp
                                    .get("syncPath")
                                    .map(RpcValue::as_str)
                                    .unwrap_or_else(||
                                        if is_device { DEFAULT_SYNC_PATH_DEVICE }
                                        else { DEFAULT_SYNC_PATH_HP }
                                    )
                                    .to_string()
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::sites::Site;

    #[test]
    fn collect_sites() {
        let sites_tree = shvproto::make_map!(
            "site" => shvproto::make_map!(
                "_meta" => shvproto::make_map!("type" => "DepotG3", "name" => "test1")
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
            ("site".to_string(), Site { name: "test1".to_string(), site_type: "DepotG3".to_string() })
        ]
        .into_iter()
        .collect::<BTreeMap<_,_>>());
    }
}

pub(crate) async fn load_sites(
    client_cmd_tx: ClientCommandSender,
    client_evt_rx: ClientEventsReceiver,
    app_state: AppState<State>,
)
{
    let mut client_evt_rx = client_evt_rx.fuse();

    loop {
        tokio::select! {
            client_event = client_evt_rx.next() => match client_event {
                Some(client_event) => if matches!(client_event, shvclient::ClientEvent::Connected(_)) {
                    log::info!("Getting sites info");

                    let sites = client_cmd_tx
                        .call_rpc_method("sites", "getSites", None)
                        .await;

                    let (sites_info, sub_hps) = match sites
                        .map(|sites: shvproto::Map| (collect_sites(&[], &sites), collect_sub_hps(&[], &sites))) {
                            Ok(sites_info) => sites_info,
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
                        .map(|(path, site)| format!(" {path}: {site:?}"))
                        .collect::<Vec<_>>()
                        .join("\n")
                    );
                    *app_state.sites.0.write().await = sites_info;
                },
                None => break,
            }
        }
    }
}

