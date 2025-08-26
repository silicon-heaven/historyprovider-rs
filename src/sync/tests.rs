use std::{collections::BTreeMap, sync::{Arc, LazyLock, Mutex}};

use futures::{channel::mpsc::{unbounded, UnboundedReceiver}, future::try_join_all, future::try_join};
use shvclient::{client::{ClientCommand}, AppState, ClientCommandSender, ClientEventsReceiver};
use shvproto::{make_list, RpcValue};
use shvrpc::RpcMessageMetaTags;
use tempfile::TempDir;
use tokio::sync::RwLock;
use tokio_stream::StreamExt;

use crate::{dirtylog::DirtyLogCommand, sites::{SiteInfo, SitesData, SubHpInfo}, sync::{sync_task, SyncCommand}, util::dedup_channel, State};
use std::sync::Once;
use simple_logger::SimpleLogger;

fn init_logger() {
    static INIT: Once = Once::new();

    INIT.call_once(|| {
        SimpleLogger::new()
            .with_level(log::LevelFilter::Debug) // show debug logs
            .init()
            .unwrap();
        });
}

async fn expect_rpc_call(client_command_reciever: &mut UnboundedReceiver<ClientCommand<State>>, shv_path: &str, method: &str, return_val: RpcValue) {
    let Some(event)  = client_command_reciever.next().await else {
        panic!("got unexpected event");
    };
    match event {
        ClientCommand::RpcCall { request, response_sender, .. } => {
            assert_eq!(request.shv_path().expect("shv path should exist"), shv_path);
            assert_eq!(request.method().expect("shv path should exist"), method);
            let mut response = request.prepare_response().expect("rpcmessage should be a request");
            response.set_result(return_val);
            response_sender.unbounded_send(response.to_frame().unwrap()).unwrap();
        },
        _ => {
            panic!("got unexpected event other than rpccall");
        }
    }

}

pub struct PrettyJoinError(String);

static DRIVER_TASK_ID: LazyLock<Mutex<Option<tokio::task::Id>>> = LazyLock::new(|| Mutex::new(None));
static SYNC_TASK_ID: LazyLock<Mutex<Option<tokio::task::Id>>> = LazyLock::new(|| Mutex::new(None));

impl From<tokio::task::JoinError> for PrettyJoinError {
    fn from(err: tokio::task::JoinError) -> Self {
        let id = err.id();
        let message = match err.try_into_panic() {
            Ok(payload) => {
                let panic_message = if let Some(s) = payload.downcast_ref::<&'static str>() {
                    s
                } else if let Some(s) = payload.downcast_ref::<String>() {
                    s.as_str()
                } else {
                    "unknown panic payload"
                };
                let task_id = if *DRIVER_TASK_ID.lock().unwrap() == Some(id) {
                    "Driver task"
                } else if *SYNC_TASK_ID.lock().unwrap() == Some(id) {
                    "Sync task"
                } else {
                    "UNKNOWN task"
                };

                format!("{task_id} panicked with message: {panic_message}")
            }
            Err(err) => err.to_string()
        };

        PrettyJoinError(message)
    }
}

impl std::fmt::Debug for PrettyJoinError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

fn _list_files(vec: &mut Vec<std::path::PathBuf>, path: &std::path::Path) -> std::io::Result<()> {
    if std::fs::metadata(path)?.is_dir() {
        let paths = std::fs::read_dir(path)?;
        for path_result in paths {
            let full_path = path_result?.path();
            if std::fs::metadata(&full_path)?.is_dir() {
                _list_files(vec, &full_path)?
            } else {
                vec.push(full_path);
            }
        }
    }
    Ok(())
}

fn list_files(path: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut vec = Vec::new();
    _list_files(&mut vec, path).expect("Failed to list journal files");
    vec
}

async fn run_test<F, Fut>(f: F) -> std::result::Result<(), PrettyJoinError>
    where
        F: FnOnce(TempDir, crate::util::DedupSender<SyncCommand>, UnboundedReceiver<ClientCommand<State>>) -> Fut + 'static + std::marker::Send,
        Fut: Future<Output = UnboundedReceiver<ClientCommand<State>>> + std::marker::Send,
{
    let (lol, client_command_reciever) = unbounded();
    let client_command_sender: ClientCommandSender<State> = ClientCommandSender::from_raw(lol);
    let (_client_events_tx, client_events_rx) = async_broadcast::broadcast(10);
    let client_events_receiver = ClientEventsReceiver::from_raw(client_events_rx.clone());
    let (dedup_sender, dedup_reciever) = dedup_channel::<SyncCommand>();
    let (dirtylog_cmd_tx, _dirtylog_cmd_rx) = unbounded::<DirtyLogCommand>();
    let journal_dir = TempDir::new().expect("tempdir should work");
    let state = AppState::new(State {
        config: crate::HpConfig {
            journal_dir: journal_dir.path().to_str().expect("path must work").to_string(),
            max_sync_tasks: None,
            max_journal_dir_size: None,
            periodic_sync_interval: None,
        },
        dirtylog_cmd_tx,
        sync_cmd_tx: dedup_sender.clone(),
        sites_data: RwLock::new(SitesData {
            sites_info: Arc::new(BTreeMap::from([
                ("site1".to_string(), SiteInfo{
                    name: "lol".into(),
                    site_type: "Type".to_string(),
                    sub_hp: "site1".to_owned(),
                })
            ])),
            sub_hps: Arc::new(BTreeMap::from([
                ("site1".to_string(), SubHpInfo::Normal {
                    sync_path: ".app/shvjournal".to_string(),
                    download_chunk_size: 10000,
                })
            ])),
        }),
        sync_info: Default::default(),
    });

    let dedup_sender2 = dedup_sender.clone();
    let driver_task = tokio::spawn(async move {
        f(journal_dir, dedup_sender2, client_command_reciever).await
    });
    *DRIVER_TASK_ID.lock().unwrap() = Some(driver_task.id());

    let sync_task = tokio::spawn(sync_task(client_command_sender.clone(), client_events_receiver.clone(), state.clone(), dedup_reciever));
    *SYNC_TASK_ID.lock().unwrap() = Some(sync_task.id());
    let mut x = driver_task.await.map_err(PrettyJoinError::from)?;
    x.close();
    dedup_sender.close_channel();
    sync_task.abort();
    try_join_all([sync_task]).await.map(|_| ()).map_err(PrettyJoinError::from)?;
    eprintln!("OK");
    Ok(())
}

#[tokio::test]
async fn sync_task_test() -> std::result::Result<(), PrettyJoinError> {
    init_logger();

    // run_test(async |journal_dir: TempDir, dedup_sender: crate::util::DedupSender<SyncCommand>, mut client_command_reciever: UnboundedReceiver<ClientCommand<State>>| {
    //     dedup_sender.send(SyncCommand::SyncAll).expect("Sending SyncAll should succeed");
    //     dedup_sender.close_channel();
    //     expect_rpc_call(&mut client_command_reciever, "shv/site1/.app/shvjournal", "lsfiles", RpcValue::null()).await;
    //     let expected_files = Vec::<std::path::PathBuf>::new();
    //     assert_eq!(list_files(journal_dir.path()), expected_files);
    //     client_command_reciever
    // }).await

    run_test(async |journal_dir: TempDir, dedup_sender: crate::util::DedupSender<SyncCommand>, mut client_command_reciever: UnboundedReceiver<ClientCommand<State>>| {
        dedup_sender.send(SyncCommand::SyncAll).expect("Sending SyncAll should succeed");
        dedup_sender.close_channel();
        expect_rpc_call(&mut client_command_reciever, "shv/site1/.app/shvjournal", "lsfiles", make_list![
            make_list!["2022-07-07T18-06-15-557.log2", "f", 308],
        ].into()).await;
        let expected_files = Vec::<std::path::PathBuf>::new();
        assert_eq!(list_files(journal_dir.path()), expected_files);
        client_command_reciever
    }).await
}
