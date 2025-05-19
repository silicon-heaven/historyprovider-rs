use futures::channel::mpsc::UnboundedReceiver;
use futures::StreamExt;
use shvclient::{AppState, ClientEventsReceiver};
use shvproto::RpcValue;
use shvrpc::util::join_path;

use crate::sites::{SitesData, SubHpInfo};
use crate::tree::{FileType, LsFilesEntry};
use crate::{ClientCommandSender, State};

pub(crate) enum SyncCommand {
    SyncLogs,
}

impl TryFrom<String> for FileType {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "f" => Ok(Self::File),
            "d" => Ok(Self::Directory),
            _ => Err(format!("Invalid FileType `{value}`")),
        }
    }
}

impl TryFrom<RpcValue> for LsFilesEntry {
    type Error = String;

    fn try_from(value: RpcValue) -> Result<Self, Self::Error> {
        let shvproto::Value::List(list) = value.value else {
            return Err("Expected List for LsFiles".to_string());
        };
        let mut list_iter = list.into_iter().fuse();
        let name = list_iter.next()
            .ok_or("Missing `file_name` field".to_string())
            .and_then(|v| if let shvproto::Value::String(file_name) = v.value {
                Ok(*file_name)
            } else {
                Err("Invalid type of `file_name` field".to_string())
            })?;
        let ftype = list_iter.next()
            .ok_or("Missing `file_type` field".to_string())
            .and_then(|v|
                if let shvproto::Value::String(file_type) = v.value {
                    Ok(*file_type)
                } else {
                    Err("Invalid type of `file_type` field".to_string())
                }
            )
            .and_then(FileType::try_from)?;
        let size = list_iter.next()
            .ok_or("Missing `size` field".to_string())
            .and_then(|v| if let shvproto::Value::Int(size) = v.value {
                Ok(size)
            } else {
                Err("Invalid type of `size` field".to_string())
            })?;
        Ok(Self {
            name,
            ftype,
            size,
        })
    }
}

async fn sync_site(
    remote_path: impl AsRef<str>,
    client_cmd_tx: ClientCommandSender,
    app_state: AppState<State>
) {
    let remote_path = remote_path.as_ref();
    let local_path = join_path(&app_state.config.journal_dir, remote_path);
    log::info!("Sync dir: {local_path}");
    let file_list: Vec<LsFilesEntry> = client_cmd_tx.call_rpc_method(&join_path("shv", remote_path), "lsfiles", None).await.unwrap();
    log::info!("  files: [{}]", file_list.iter().map(LsFilesEntry::to_string).collect::<Vec<_>>().join(", "));
}

pub(crate) async fn sync_task(
    client_cmd_tx: ClientCommandSender,
    client_evt_rx: ClientEventsReceiver,
    app_state: AppState<State>,
    mut sync_cmd_rx: UnboundedReceiver<SyncCommand>,
)
{

    while let Some(cmd) = sync_cmd_rx.next().await {
        match cmd {
            SyncCommand::SyncLogs => {
                log::info!("Sync logs start");
                let SitesData { sites_info, sub_hps } = app_state.sites_data.read().await.clone();
                for (site_path, site_info) in sites_info.iter() {
                    let sub_hp = sub_hps
                        .get(&site_info.sub_hp)
                        .unwrap_or_else(|| panic!("Sub HP for site {site_path} should be set"));
                    match sub_hp {
                        SubHpInfo::Normal { sync_path, download_chunk_size } => {
                            let site_suffix = shvrpc::util::strip_prefix_path(&site_path, &site_info.sub_hp)
                                .unwrap_or_else(|| panic!("Site {site_path} should be under its sub HP {}", site_info.sub_hp));
                            let remote_journal_path = join_path(&site_info.sub_hp, &join_path(sync_path.as_str(), site_suffix));
                            log::info!("Syncing {remote_journal_path}, chunk size: {download_chunk_size}");
                            sync_site(remote_journal_path, client_cmd_tx.clone(), app_state.clone()).await;
                            // TODO
                        }
                        SubHpInfo::Legacy { getlog_path } => {
                            log::info!("Syncing {site_path} via getLog from {getlog_path}");
                            // TODO
                        }
                        SubHpInfo::PushLog => {
                            // TODO
                        }
                    }
                }
            }
        }
    }
}
