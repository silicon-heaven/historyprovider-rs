use std::{collections::BTreeMap, path::Path, sync::{Arc, LazyLock, Mutex}};

use futures::{channel::mpsc::{unbounded, UnboundedReceiver}, StreamExt};
use log::debug;
use shvclient::{client::{ClientCommand}, AppState, ClientCommandSender, ClientEventsReceiver};
use shvproto::{make_list, make_map, RpcValue};
use shvrpc::{rpcmessage::RpcError, RpcMessageMetaTags};
use tempfile::TempDir;
use tokio::{io::AsyncWriteExt, sync::RwLock};

use crate::{dirtylog::DirtyLogCommand, sites::{SiteInfo, SitesData, SubHpInfo}, sync::{sync_task, SyncCommand}, util::{dedup_channel, init_logger}, State};

async fn expect_rpc_call(client_command_reciever: &mut UnboundedReceiver<ClientCommand<State>>, expected_shv_path: &str, expected_method: &str, expected_param: Option<RpcValue>, return_val: Result<RpcValue, RpcError>) {
    let Some(event) = client_command_reciever.next().await else {
        panic!("got unexpected event");
    };
    match event {
        ClientCommand::RpcCall { request, response_sender, .. } => {
            let shv_path = request.shv_path().expect("shv path should exist");
            let method = request.method().expect("shv path should exist");
            let param = request.param().unwrap_or_default();
            debug!(target: "test-driver", "<== {shv_path}:{method}, param: {param}");
            assert_eq!(shv_path, expected_shv_path);
            assert_eq!(method, expected_method);
            if let Some(expected_param) = expected_param {
                assert_eq!(param, &expected_param);
            }
            let mut response = request.prepare_response().expect("rpcmessage should be a request");
            debug!(target: "test-driver", "==> {return_val:?}");
            match return_val {
                Ok(result) => response.set_result(result),
                Err(rpc_error) => response.set_error(rpc_error),
            };
            response_sender.unbounded_send(response.to_frame().unwrap()).unwrap();
        },
        _ => {
            panic!("got unexpected event other than rpccall");
        }
    }
}

pub struct PrettyJoinError(String);

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
                let task_id = if *SYNC_TASK_ID.lock().unwrap() == Some(id) {
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

fn list_files(path: &std::path::Path) -> Vec<String> {
    let mut res = Vec::new();
    _list_files(&mut res, path).expect("Failed to list journal files");
    let mut res = res
        .into_iter()
        .map(|path| path.to_string_lossy().to_string())
        .collect::<Vec<_>>();
    res.sort();
    res
}

enum TestStep {
    SyncCommand(SyncCommand),
    ExpectCall(&'static str, &'static str, Result<RpcValue, RpcError>),
    ExpectCallParam(&'static str, &'static str, RpcValue, Result<RpcValue, RpcError>),
}

async fn run_test(test_name: &'static str, steps: &[TestStep], starting_files: Vec::<(&'static str, &'static str)>, expected_file_paths: Vec::<&'static str>) -> std::result::Result<(), PrettyJoinError> {
    debug!(target: "test-driver", "Running test '{test_name}'");
    let (client_command_sender, mut client_command_receiver) = unbounded();
    let client_command_sender: ClientCommandSender<State> = ClientCommandSender::from_raw(client_command_sender);
    let (_client_events_tx, client_events_rx) = async_broadcast::broadcast(10);
    let client_events_receiver = ClientEventsReceiver::from_raw(client_events_rx.clone());
    let (dedup_sender, dedup_reciever) = dedup_channel::<SyncCommand>();
    let (dirtylog_cmd_tx, _dirtylog_cmd_rx) = unbounded::<DirtyLogCommand>();
    let journal_dir = TempDir::with_prefix("test-hprs-sync_task.").expect("tempdir should work");
    for (starting_file_name, starting_file_content) in starting_files {
        let file_name = format!("{}/{}", journal_dir.path().to_string_lossy(), starting_file_name);
        let dir_name = Path::new(&file_name).parent().unwrap();
        tokio::fs::create_dir_all(dir_name).await
            .map_err(|e| PrettyJoinError(format!("Cannot create journal directory at {}: {e}", dir_name.to_string_lossy())))?;

        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .write(true)
            .open(file_name)
            .await
            .map_err(|e| PrettyJoinError(format!("Cannot open {starting_file_name}: {e}")))?;

        file.write_all(starting_file_content.as_bytes())
            .await
            .map_err(|e| PrettyJoinError(format!("Cannot write to {starting_file_name}: {e}")))?;
    }

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
                    download_chunk_size: 1000000,
                })
            ])),
        }),
        sync_info: Default::default(),
    });


    let sync_task = tokio::spawn(sync_task(client_command_sender.clone(), client_events_receiver.clone(), state.clone(), dedup_reciever));
    *SYNC_TASK_ID.lock().unwrap() = Some(sync_task.id());

    let last_sync_command = steps.iter().rposition(|step| matches!(step, TestStep::SyncCommand(_))).unwrap_or(0);
    for (ix, step) in steps.iter().enumerate() {
        match step {
            TestStep::ExpectCall(path, method, ret_val) => {
                expect_rpc_call(&mut client_command_receiver, path, method, None, ret_val.clone()).await;
            },
            TestStep::ExpectCallParam(path, method, param, ret_val) => {
                expect_rpc_call(&mut client_command_receiver, path, method, Some(param.clone()), ret_val.clone()).await;
            },
            TestStep::SyncCommand(cmd) => {
                debug!(target: "test-driver", "Sending SyncCommand::{cmd:?}");
                dedup_sender.send(cmd.clone()).expect("Sending SyncCommands should succeed");
            },
        }

        if ix == last_sync_command {
            debug!(target: "test-driver", "Signalling sync_task to exit");
            dedup_sender.close_channel();
        }
    }

    tokio::select! {
        task_end = sync_task => {
            task_end.map(|_| ()).map_err(PrettyJoinError::from)?;
        },
        unexpected_client_command = client_command_receiver.next() => {
            if let Some(unexpected_client_command) = unexpected_client_command {
                match unexpected_client_command {
                    ClientCommand::RpcCall { request, .. } => return Err(PrettyJoinError(format!("Unexpected RpcCall: {request}"))),
                        _ => return Err(PrettyJoinError("Unexpected ClientCommand".to_string()))
                }
            }
        }
    }

    let prefixed_expected_paths = expected_file_paths.iter().map(|path| format!("{}/{}", journal_dir.path().to_string_lossy(), path)).collect::<Vec<_>>();
    assert_eq!(list_files(journal_dir.path()), prefixed_expected_paths);

    Ok(())
}

struct TestCase<'a> {
    name: &'static str,
    steps: &'a [TestStep],
    starting_files: Vec::<(&'static str, &'static str)>,
    expected_file_paths: Vec::<&'static str>,
}

#[tokio::test]
async fn sync_task_test() -> std::result::Result<(), PrettyJoinError> {
    init_logger();

    static DUMMY_LOGFILE: &str = r"2022-07-07T18:06:15.557Z	809779	APP_START	true		SHV_SYS	0	
2022-07-07T18:06:17.784Z	809781	zone1/system/sig/plcDisconnected	false		chng	2	
2022-07-07T18:06:17.784Z	809781	zone1/zone/Zone1/plcDisconnected	false		chng	2	
2022-07-07T18:06:17.869Z	809781	zone1/pme/TSH1-1/switchRightCounterPermanent	0u		chng	2	
";

    let very_large_log_file: String = "2022-07-07T18:06:17.784Z	809781	zone1/system/sig/plcDisconnected	false		chng	2	\n".to_string().repeat(50000);
    let test_cases = [
        TestCase {
            name: "SyncSite: Remote and local - empty",
            steps: &[
                TestStep::SyncCommand(SyncCommand::SyncSite("site1".to_string())),
                TestStep::ExpectCall("shv/site1/.app/shvjournal", "lsfiles", Ok(RpcValue::null())),
            ],
            starting_files: vec![],
            expected_file_paths: vec![],
        },
        TestCase {
            name: "SyncSite: Remote - has files, local - empty",
            steps: &[
                TestStep::SyncCommand(SyncCommand::SyncSite("site1".to_string())),
                TestStep::ExpectCall("shv/site1/.app/shvjournal", "lsfiles", Ok(make_list![
                        make_list!["2022-07-07T18-06-15-557.log2", "f", DUMMY_LOGFILE.len() as i32],
                ].into())),
                TestStep::ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "dir", "sha1".into(), Ok(true.into())),
                TestStep::ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "read", make_map!("offset" => 0, "size" => DUMMY_LOGFILE.len() as i32).into(), Ok(DUMMY_LOGFILE.as_bytes().into())),
            ],
            starting_files: vec![],
            expected_file_paths: vec!["site1/2022-07-07T18-06-15-557.log2"],
        },
        TestCase {
            name: "SyncSite: chunk download",
            steps: &[
                TestStep::SyncCommand(SyncCommand::SyncSite("site1".to_string())),
                TestStep::ExpectCall("shv/site1/.app/shvjournal", "lsfiles", Ok(make_list![
                        make_list!["2022-07-07T18-06-15-557.log2", "f", DUMMY_LOGFILE.len() as i32],
                ].into())),
                TestStep::ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "dir", "sha1".into(), Ok(true.into())),
                TestStep::ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "read", make_map!("offset" => 0, "size" => DUMMY_LOGFILE.len() as i32).into(), Ok(DUMMY_LOGFILE.as_bytes()[..100].into())),
                TestStep::ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "read", make_map!("offset" => 100, "size" => (DUMMY_LOGFILE.len() as i32) - 100).into(), Ok(DUMMY_LOGFILE.as_bytes()[100..].into())),
            ],
            starting_files: vec![],
            expected_file_paths: vec!["site1/2022-07-07T18-06-15-557.log2"],
        },
        TestCase {
            name: "SyncSite: File API detection error",
            steps: &[
                TestStep::SyncCommand(SyncCommand::SyncSite("site1".to_string())),
                TestStep::ExpectCall("shv/site1/.app/shvjournal", "lsfiles", Ok(make_list![
                        make_list!["2022-07-07T18-06-15-557.log2", "f", DUMMY_LOGFILE.len() as i32],
                ].into())),
                TestStep::ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "dir", "sha1".into(), Ok(true.into())),
                TestStep::ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "read", make_map!("offset" => 0, "size" => DUMMY_LOGFILE.len() as i32).into(), Err(RpcError::new(shvrpc::rpcmessage::RpcErrorCode::MethodCallTimeout, "Simulated test timeout"))),
            ],
            starting_files: vec![],
            expected_file_paths: vec![],
        },
        TestCase {
            name: "SyncAll: File without chronological order",
            steps: &[
                TestStep::SyncCommand(SyncCommand::SyncAll),
                TestStep::ExpectCall("shv/site1/.app/shvjournal", "lsfiles", Ok(make_list![
                        make_list!["2022-07-08T18-06-15-557.log2", "f", DUMMY_LOGFILE.len() as i32],
                        make_list!["2022-07-07T18-06-15-557.log2", "f", DUMMY_LOGFILE.len() as i32],
                ].into())),
                TestStep::ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "dir", "sha1".into(), Ok(true.into())),
                TestStep::ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "read", make_map!("offset" => 0, "size" => DUMMY_LOGFILE.len() as i32).into(), Ok(DUMMY_LOGFILE.as_bytes().into())),
                TestStep::ExpectCallParam("shv/site1/.app/shvjournal/2022-07-08T18-06-15-557.log2", "read", make_map!("offset" => 0, "size" => DUMMY_LOGFILE.len() as i32).into(), Ok(DUMMY_LOGFILE.as_bytes().into())),
            ],
            starting_files: vec![],
            expected_file_paths: vec!["site1/2022-07-07T18-06-15-557.log2", "site1/2022-07-08T18-06-15-557.log2"],
        },
        TestCase {
            name: "SyncSite: File without chronological order",
            steps: &[
                TestStep::SyncCommand(SyncCommand::SyncSite("site1".to_string())),
                TestStep::ExpectCall("shv/site1/.app/shvjournal", "lsfiles", Ok(make_list![
                        make_list!["2022-07-08T18-06-15-557.log2", "f", DUMMY_LOGFILE.len() as i32],
                        make_list!["2022-07-07T18-06-15-557.log2", "f", DUMMY_LOGFILE.len() as i32],
                ].into())),
                TestStep::ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "dir", "sha1".into(), Ok(true.into())),
                TestStep::ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "read", make_map!("offset" => 0, "size" => DUMMY_LOGFILE.len() as i32).into(), Ok(DUMMY_LOGFILE.as_bytes().into())),
                TestStep::ExpectCallParam("shv/site1/.app/shvjournal/2022-07-08T18-06-15-557.log2", "read", make_map!("offset" => 0, "size" => DUMMY_LOGFILE.len() as i32).into(), Ok(DUMMY_LOGFILE.as_bytes().into())),
            ],
            starting_files: vec![],
            expected_file_paths: vec!["site1/2022-07-07T18-06-15-557.log2", "site1/2022-07-08T18-06-15-557.log2"],
        },
        TestCase {
            name: "SyncSite: Remote has one empty file and one non-empty file",
            steps: &[
                TestStep::SyncCommand(SyncCommand::SyncSite("site1".to_string())),
                TestStep::ExpectCall("shv/site1/.app/shvjournal", "lsfiles", Ok(make_list![
                        make_list![ "2022-07-05T18-06-15-557.log2", "f", 0 ],
                        make_list![ "2022-07-07T18-06-15-557.log2", "f", DUMMY_LOGFILE.len() as i32]
                ].into())),
                TestStep::ExpectCallParam("shv/site1/.app/shvjournal/2022-07-05T18-06-15-557.log2", "dir", "sha1".into(), Ok(true.into())),
                TestStep::ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "read", make_map!("offset" => 0, "size" => DUMMY_LOGFILE.len() as i32).into(), Ok(DUMMY_LOGFILE.as_bytes().into())),
            ],
            starting_files: vec![],
            expected_file_paths: vec!["site1/2022-07-05T18-06-15-557.log2", "site1/2022-07-07T18-06-15-557.log2"],
        },
        TestCase {
            name: "SyncSite: Don't download files older than we already have",
            steps: &[
                TestStep::SyncCommand(SyncCommand::SyncSite("site1".to_string())),
                TestStep::ExpectCall("shv/site1/.app/shvjournal", "lsfiles", Ok(make_list![
                        make_list!["2022-07-07T18-06-15-000.log2", "f", DUMMY_LOGFILE.len() as i32],
                ].into())),
            ],
            starting_files: vec![
                ("site1/2022-07-07T18-06-15-557.log2", DUMMY_LOGFILE),
                ("site1/2022-07-07T18-06-15-558.log2", DUMMY_LOGFILE),
            ],
            expected_file_paths: vec!["site1/2022-07-07T18-06-15-557.log2", "site1/2022-07-07T18-06-15-558.log2"],
        },
        TestCase {
            name: "SyncSite: Files with same size remote/local size aren't synced",
            steps: &[
                TestStep::SyncCommand(SyncCommand::SyncSite("site1".to_string())),
                TestStep::ExpectCall("shv/site1/.app/shvjournal", "lsfiles", Ok(make_list![
                        make_list!["2022-07-07T18-06-15-000.log2", "f", DUMMY_LOGFILE.len() as i32],
                ].into())),
            ],
            starting_files: vec![
                ("site1/2022-07-07T18-06-15-000.log2", DUMMY_LOGFILE),
            ],
            expected_file_paths: vec!["site1/2022-07-07T18-06-15-000.log2"],
        },
        TestCase {
            name: "SyncSite: Remote sends more data",
            steps: &[
                TestStep::SyncCommand(SyncCommand::SyncSite("site1".to_string())),
                TestStep::ExpectCall("shv/site1/.app/shvjournal", "lsfiles", Ok(make_list![
                        make_list!["2022-07-07T18-06-15-557.log2", "f", DUMMY_LOGFILE.len() as i32],
                ].into())),
                TestStep::ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "dir", "sha1".into(), Ok(true.into())),
                TestStep::ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "read", make_map!("offset" => 0, "size" => DUMMY_LOGFILE.len() as i32).into(), Ok(very_large_log_file.as_bytes().into())),
            ],
            starting_files: vec![],
            expected_file_paths: vec![],
        },
        TestCase {
            name: "SyncSite: chunk size",
            steps: &[
                TestStep::SyncCommand(SyncCommand::SyncSite("site1".to_string())),
                TestStep::ExpectCall("shv/site1/.app/shvjournal", "lsfiles", Ok(make_list![
                        make_list!["2022-07-07T18-06-15-557.log2", "f", very_large_log_file.len() as i32],
                ].into())),
                TestStep::ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "dir", "sha1".into(), Ok(true.into())),
                TestStep::ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "read", make_map!("offset" => 0, "size" => 1000000).into(), Ok(very_large_log_file.as_bytes()[..1000000].into())),
                TestStep::ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "read", make_map!("offset" => 1000000, "size" => 1000000).into(), Ok(very_large_log_file.as_bytes()[1000000..2000000].into())),
                TestStep::ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "read", make_map!("offset" => 2000000, "size" => 1000000).into(), Ok(very_large_log_file.as_bytes()[2000000..3000000].into())),
                TestStep::ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "read", make_map!("offset" => 3000000, "size" => 1000000).into(), Ok(very_large_log_file.as_bytes()[3000000..4000000].into())),
            ],
            starting_files: vec![],
            expected_file_paths: vec!["site1/2022-07-07T18-06-15-557.log2"],
        },
    ];

    for test_case in test_cases {
        run_test(test_case.name, test_case.steps, test_case.starting_files, test_case.expected_file_paths).await?;
    }

    Ok(())
}
