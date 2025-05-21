use std::cmp::Ordering;

use futures::channel::mpsc::UnboundedReceiver;
use futures::StreamExt;
use shvclient::{AppState, ClientEventsReceiver};
use shvproto::RpcValue;
use shvrpc::util::join_path;

use crate::sites::{SitesData, SubHpInfo};
use crate::tree::{FileType, LsFilesEntry, METH_READ};
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
    // FIXME: add remote_journal_path: impl AsRef<str>,
    site_path: impl AsRef<str>,
    download_chunk_size: i64,
    client_cmd_tx: ClientCommandSender,
    app_state: AppState<State>
) -> Result<(), String>
{
    fn to_string(v: impl ToString) -> String { v.to_string() }

    let site_path = site_path.as_ref();
    let site_path_remote = join_path("shv", site_path);
    let file_list: Vec<LsFilesEntry> = client_cmd_tx
        .call_rpc_method(&site_path_remote, "lsfiles", None)
        .await
        .map_err(to_string)?;
    log::info!("  files: [{}]", file_list.iter().map(LsFilesEntry::to_string).collect::<Vec<_>>().join(","));

    let site_path_local = join_path(&app_state.config.journal_dir, site_path);
    tokio::fs::create_dir_all(&site_path_local).await.map_err(to_string)?;

    let files_to_sync = futures::stream::iter(
            file_list.iter().filter(|f| matches!(f.ftype, FileType::File))
        )
        .filter_map(|remote_file| async {
            let local_path = join_path(&site_path_local, &remote_file.name);
            match tokio::fs::metadata(&local_path).await {
                Ok(local_file) if local_file.is_file() => {
                    let local_size = local_file.len() as i64;
                    let sync_offset = match local_size.cmp(&remote_file.size) {
                        Ordering::Less => local_size,
                        Ordering::Greater => 0,
                        Ordering::Equal => return None,
                    };
                    Some((remote_file.name.clone(), sync_offset, remote_file.size))
                }
                Ok(_not_a_file) => None,
                Err(_err) => Some((remote_file.name.clone(), 0, remote_file.size)),
            }
        })
        .collect::<Vec<_>>()
        .await;

    for (file_name, sync_offset, file_size) in files_to_sync {
        sync_file(
            client_cmd_tx.clone(),
            join_path(&site_path_remote, &file_name),
            join_path(&site_path_local, &file_name),
            download_chunk_size,
            sync_offset,
            file_size
        )
        .await
        .map_err(to_string)?;
    }

    Ok(())
}

async fn sync_file(
    client_cmd_tx: ClientCommandSender,
    file_path_remote: impl AsRef<str>,
    file_path_local: impl AsRef<str>,
    download_chunk_size: i64,
    sync_offset: i64,
    file_size: i64,
) -> Result<(), String>
{
    let file_path_remote = file_path_remote.as_ref();
    let file_path_local = file_path_local.as_ref();
    log::info!("Syncing file {file_path_remote}, start offset: {sync_offset}, file size: {file_size}, target: {file_path_local}");

    let mut sync_offset = sync_offset;

    while sync_offset < file_size {
        let remaining_bytes = file_size - sync_offset;
        let sync_size = remaining_bytes.max(0).min(download_chunk_size);
        log::info!("  downloading chunk, offset: {sync_offset}, size: {sync_size}, remaining: {} ({}%),",
            remaining_bytes,
            ((sync_offset as f64 / file_size as f64) * 100.0) as i32,
        );
        let param = shvproto::make_map!(
            "offset" => sync_offset,
            "size" => sync_size,
        ).into();

        let chunk: shvproto::Blob = client_cmd_tx
            .call_rpc_method(file_path_remote, METH_READ, Some(param))
            .await
            .map_err(|e| e.to_string())?;

        // TODO: save the chunk to the file

        sync_offset += chunk.len() as i64;
    }
    log::info!("Syncing file {file_path_remote} done");
    Ok(())
}


pub(crate) async fn sync_task(
    client_cmd_tx: ClientCommandSender,
    _client_evt_rx: ClientEventsReceiver,
    app_state: AppState<State>,
    mut sync_cmd_rx: UnboundedReceiver<SyncCommand>,
)
{

    while let Some(cmd) = sync_cmd_rx.next().await {
        match cmd {
            SyncCommand::SyncLogs => {
                log::info!("Sync logs start");
                let sync_start = tokio::time::Instant::now();
                let SitesData { sites_info, sub_hps } = app_state.sites_data.read().await.clone();
                for (site_path, site_info) in sites_info.iter() {
                    let sub_hp = sub_hps
                        .get(&site_info.sub_hp)
                        .unwrap_or_else(|| panic!("Sub HP for site {site_path} should be set"));
                    match sub_hp {
                        SubHpInfo::Normal { sync_path, download_chunk_size } => {
                            let site_suffix = shvrpc::util::strip_prefix_path(site_path, &site_info.sub_hp)
                                .unwrap_or_else(|| panic!("Site {site_path} should be under its sub HP {}", site_info.sub_hp));
                            let remote_journal_path = join_path(&site_info.sub_hp, &join_path(sync_path.as_str(), site_suffix));
                            log::info!("Syncing {remote_journal_path}, chunk size: {download_chunk_size}");
                            sync_site(remote_journal_path, *download_chunk_size, client_cmd_tx.clone(), app_state.clone())
                                .await
                                .unwrap_or_else(|err| log::error!("Error syncing site {site_path}: {err}"));
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
                log::info!("Sync logs done in {} s", sync_start.elapsed().as_secs());
            }
        }
    }
}
