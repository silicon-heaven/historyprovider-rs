use crate::util::{dedup_channel, testing::*};
use crate::{sync::{sync_task, SyncCommand}, util::{init_logger, DedupSender}, State};
use futures::channel::mpsc::UnboundedReceiver;
use log::debug;
use shvclient::client::ClientCommand;
use shvproto::{make_list, make_map, RpcValue};
use shvrpc::rpcmessage::RpcError;

struct SyncTaskTestState {
    dedup_sender: DedupSender<crate::sync::SyncCommand>,
}

struct DoSyncCommand(crate::sync::SyncCommand);

#[async_trait::async_trait]
impl TestStep<SyncTaskTestState> for DoSyncCommand {
    async fn exec(&self, _client_command_reciever: &mut UnboundedReceiver<ClientCommand<State>>, state: &SyncTaskTestState) {
        let cmd = self.0.clone();
        debug!(target: "test-driver", "Sending SyncCommand::{cmd:?}");
        state.dedup_sender.send(cmd).expect("Sending SyncCommands should succeed");
    }
}

struct TestCase<'a> {
    name: &'static str,
    steps: &'a [Box<dyn TestStep<SyncTaskTestState>>],
    starting_files: Vec<(&'static str, &'static str)>,
    expected_file_paths: Vec<(&'static str, &'a str)>,
}

#[tokio::test]
async fn sync_task_test() -> std::result::Result<(), PrettyJoinError> {
    init_logger();

    static DUMMY_LOGFILE: &str = "2022-07-07T18:06:15.557Z\t809779\tAPP_START\ttrue\t\tSHV_SYS\t0\t
2022-07-07T18:06:17.784Z\t809781\tzone1/system/sig/plcDisconnected\tfalse\t\tchng\t2\t
2022-07-07T18:06:17.784Z\t809781\tzone1/zone/Zone1/plcDisconnected\tfalse\t\tchng\t2\t
2022-07-07T18:06:17.869Z\t809781\tzone1/pme/TSH1-1/switchRightCounterPermanent\t0u\t\tchng\t2\t
";

    let very_large_log_file: String = "2022-07-07T18:06:17.784Z\t809781\tzone1/system/sig/plcDisconnected\tfalse\t\tchng\t2\t\n".to_string().repeat(50000);
    let test_cases = [
        TestCase {
            name: "SyncSite: Remote and local - empty",
            steps: &[
                Box::new(DoSyncCommand(SyncCommand::SyncSite("site1".to_string()))),
                Box::new(ExpectCall("shv/site1/.app/shvjournal", "lsfiles", Ok(RpcValue::null()))),
            ],
            starting_files: vec![],
            expected_file_paths: vec![],
        },
        TestCase {
            name: "SyncSite: Remote - has files, local - empty",
            steps: &[
                Box::new(DoSyncCommand(SyncCommand::SyncSite("site1".to_string()))),
                Box::new(ExpectCall("shv/site1/.app/shvjournal", "lsfiles", Ok(make_list![
                        make_list!["2022-07-07T18-06-15-557.log2", "f", DUMMY_LOGFILE.len() as i32],
                ].into()))),
                Box::new(ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "dir", "sha1".into(), Ok(true.into()))),
                Box::new(ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "read", make_map!("offset" => 0, "size" => DUMMY_LOGFILE.len() as i32).into(), Ok(DUMMY_LOGFILE.as_bytes().into()))),
            ],
            starting_files: vec![],
            expected_file_paths: vec![("site1/2022-07-07T18-06-15-557.log2", DUMMY_LOGFILE)],
        },
        TestCase {
            name: "SyncSite: chunk download",
            steps: &[
                Box::new(DoSyncCommand(SyncCommand::SyncSite("site1".to_string()))),
                Box::new(ExpectCall("shv/site1/.app/shvjournal", "lsfiles", Ok(make_list![
                        make_list!["2022-07-07T18-06-15-557.log2", "f", DUMMY_LOGFILE.len() as i32],
                ].into()))),
                Box::new(ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "dir", "sha1".into(), Ok(true.into()))),
                Box::new(ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "read", make_map!("offset" => 0, "size" => DUMMY_LOGFILE.len() as i32).into(), Ok(DUMMY_LOGFILE.as_bytes()[..100].into()))),
                Box::new(ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "read", make_map!("offset" => 100, "size" => (DUMMY_LOGFILE.len() as i32) - 100).into(), Ok(DUMMY_LOGFILE.as_bytes()[100..].into()))),
            ],
            starting_files: vec![],
            expected_file_paths: vec![("site1/2022-07-07T18-06-15-557.log2", DUMMY_LOGFILE)],
        },
        TestCase {
            name: "SyncSite: File API detection error",
            steps: &[
                Box::new(DoSyncCommand(SyncCommand::SyncSite("site1".to_string()))),
                Box::new(ExpectCall("shv/site1/.app/shvjournal", "lsfiles", Ok(make_list![
                        make_list!["2022-07-07T18-06-15-557.log2", "f", DUMMY_LOGFILE.len() as i32],
                ].into()))),
                Box::new(ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "dir", "sha1".into(), Ok(true.into()))),
                Box::new(ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "read", make_map!("offset" => 0, "size" => DUMMY_LOGFILE.len() as i32).into(), Err(RpcError::new(shvrpc::rpcmessage::RpcErrorCode::MethodCallTimeout, "Simulated test timeout")))),
            ],
            starting_files: vec![],
            expected_file_paths: vec![],
        },
        TestCase {
            name: "SyncAll: File without chronological order",
            steps: &[
                Box::new(DoSyncCommand(SyncCommand::SyncAll)),
                Box::new(ExpectCall("shv/site1/.app/shvjournal", "lsfiles", Ok(make_list![
                        make_list!["2022-07-08T18-06-15-557.log2", "f", DUMMY_LOGFILE.len() as i32],
                        make_list!["2022-07-07T18-06-15-557.log2", "f", DUMMY_LOGFILE.len() as i32],
                ].into()))),
                Box::new(ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "dir", "sha1".into(), Ok(true.into()))),
                Box::new(ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "read", make_map!("offset" => 0, "size" => DUMMY_LOGFILE.len() as i32).into(), Ok(DUMMY_LOGFILE.as_bytes().into()))),
                Box::new(ExpectCallParam("shv/site1/.app/shvjournal/2022-07-08T18-06-15-557.log2", "read", make_map!("offset" => 0, "size" => DUMMY_LOGFILE.len() as i32).into(), Ok(DUMMY_LOGFILE.as_bytes().into()))),
            ],
            starting_files: vec![],
            expected_file_paths: vec![("site1/2022-07-07T18-06-15-557.log2", DUMMY_LOGFILE), ("site1/2022-07-08T18-06-15-557.log2", DUMMY_LOGFILE)],
        },
        TestCase {
            name: "SyncSite: File without chronological order",
            steps: &[
                Box::new(DoSyncCommand(SyncCommand::SyncSite("site1".to_string()))),
                Box::new(ExpectCall("shv/site1/.app/shvjournal", "lsfiles", Ok(make_list![
                        make_list!["2022-07-08T18-06-15-557.log2", "f", DUMMY_LOGFILE.len() as i32],
                        make_list!["2022-07-07T18-06-15-557.log2", "f", DUMMY_LOGFILE.len() as i32],
                ].into()))),
                Box::new(ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "dir", "sha1".into(), Ok(true.into()))),
                Box::new(ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "read", make_map!("offset" => 0, "size" => DUMMY_LOGFILE.len() as i32).into(), Ok(DUMMY_LOGFILE.as_bytes().into()))),
                Box::new(ExpectCallParam("shv/site1/.app/shvjournal/2022-07-08T18-06-15-557.log2", "read", make_map!("offset" => 0, "size" => DUMMY_LOGFILE.len() as i32).into(), Ok(DUMMY_LOGFILE.as_bytes().into()))),
            ],
            starting_files: vec![],
            expected_file_paths: vec![("site1/2022-07-07T18-06-15-557.log2", DUMMY_LOGFILE), ("site1/2022-07-08T18-06-15-557.log2", DUMMY_LOGFILE)],
        },
        TestCase {
            name: "SyncSite: Remote has one empty file and one non-empty file",
            steps: &[
                Box::new(DoSyncCommand(SyncCommand::SyncSite("site1".to_string()))),
                Box::new(ExpectCall("shv/site1/.app/shvjournal", "lsfiles", Ok(make_list![
                        make_list![ "2022-07-05T18-06-15-557.log2", "f", 0 ],
                        make_list![ "2022-07-07T18-06-15-557.log2", "f", DUMMY_LOGFILE.len() as i32]
                ].into()))),
                Box::new(ExpectCallParam("shv/site1/.app/shvjournal/2022-07-05T18-06-15-557.log2", "dir", "sha1".into(), Ok(true.into()))),
                Box::new(ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "read", make_map!("offset" => 0, "size" => DUMMY_LOGFILE.len() as i32).into(), Ok(DUMMY_LOGFILE.as_bytes().into()))),
            ],
            starting_files: vec![],
            expected_file_paths: vec![("site1/2022-07-05T18-06-15-557.log2", ""), ("site1/2022-07-07T18-06-15-557.log2", DUMMY_LOGFILE)],
        },
        TestCase {
            name: "SyncSite: Don't download files older than we already have",
            steps: &[
                Box::new(DoSyncCommand(SyncCommand::SyncSite("site1".to_string()))),
                Box::new(ExpectCall("shv/site1/.app/shvjournal", "lsfiles", Ok(make_list![
                        make_list!["2022-07-07T18-06-15-000.log2", "f", DUMMY_LOGFILE.len() as i32],
                ].into()))),
            ],
            starting_files: vec![
                ("site1/2022-07-07T18-06-15-557.log2", DUMMY_LOGFILE),
                ("site1/2022-07-07T18-06-15-558.log2", DUMMY_LOGFILE),
            ],
            expected_file_paths: vec![("site1/2022-07-07T18-06-15-557.log2", DUMMY_LOGFILE), ("site1/2022-07-07T18-06-15-558.log2", DUMMY_LOGFILE)],
        },
        TestCase {
            name: "SyncSite: Files with same size remote/local size aren't synced",
            steps: &[
                Box::new(DoSyncCommand(SyncCommand::SyncSite("site1".to_string()))),
                Box::new(ExpectCall("shv/site1/.app/shvjournal", "lsfiles", Ok(make_list![
                        make_list!["2022-07-07T18-06-15-000.log2", "f", DUMMY_LOGFILE.len() as i32],
                ].into()))),
            ],
            starting_files: vec![
                ("site1/2022-07-07T18-06-15-000.log2", DUMMY_LOGFILE),
            ],
            expected_file_paths: vec![("site1/2022-07-07T18-06-15-000.log2", DUMMY_LOGFILE)],
        },
        TestCase {
            name: "SyncSite: Remote sends more data",
            steps: &[
                Box::new(DoSyncCommand(SyncCommand::SyncSite("site1".to_string()))),
                Box::new(ExpectCall("shv/site1/.app/shvjournal", "lsfiles", Ok(make_list![
                        make_list!["2022-07-07T18-06-15-557.log2", "f", DUMMY_LOGFILE.len() as i32],
                ].into()))),
                Box::new(ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "dir", "sha1".into(), Ok(true.into()))),
                Box::new(ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "read", make_map!("offset" => 0, "size" => DUMMY_LOGFILE.len() as i32).into(), Ok(very_large_log_file.as_bytes().into()))),
            ],
            starting_files: vec![],
            expected_file_paths: vec![],
        },
        TestCase {
            name: "SyncSite: chunk size",
            steps: &[
                Box::new(DoSyncCommand(SyncCommand::SyncSite("site1".to_string()))),
                Box::new(ExpectCall("shv/site1/.app/shvjournal", "lsfiles", Ok(make_list![
                        make_list!["2022-07-07T18-06-15-557.log2", "f", very_large_log_file.len() as i32],
                ].into()))),
                Box::new(ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "dir", "sha1".into(), Ok(true.into()))),
                Box::new(ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "read", make_map!("offset" => 0, "size" => 1000000).into(), Ok(very_large_log_file.as_bytes()[..1000000].into()))),
                Box::new(ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "read", make_map!("offset" => 1000000, "size" => 1000000).into(), Ok(very_large_log_file.as_bytes()[1000000..2000000].into()))),
                Box::new(ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "read", make_map!("offset" => 2000000, "size" => 1000000).into(), Ok(very_large_log_file.as_bytes()[2000000..3000000].into()))),
                Box::new(ExpectCallParam("shv/site1/.app/shvjournal/2022-07-07T18-06-15-557.log2", "read", make_map!("offset" => 3000000, "size" => 1000000).into(), Ok(very_large_log_file.as_bytes()[3000000..4000000].into()))),
            ],
            starting_files: vec![],
            expected_file_paths: vec![("site1/2022-07-07T18-06-15-557.log2", very_large_log_file.as_str())],
        },
    ];

    for test_case in test_cases {
        run_test(
            test_case.name,
            test_case.steps,
            test_case.starting_files,
            test_case.expected_file_paths,
            |ccs, cer, state| {
                let (dedup_sender, receiver) = dedup_channel::<SyncCommand>();
                let task_state = SyncTaskTestState {
                    dedup_sender,
                };
                let sync_task = tokio::spawn(sync_task(ccs, cer, state, receiver));
                (sync_task, task_state)
            },
            |state| {
                state.dedup_sender.close_channel();
            }
        ).await?;
    }

    Ok(())
}
