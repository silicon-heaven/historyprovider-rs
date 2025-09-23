use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::pin::Pin;

use futures::channel::mpsc::UnboundedReceiver;
use futures::channel::oneshot::Sender as OneshotSender;
use futures::io::BufReader;
use futures::stream::FuturesUnordered;
use futures::{StreamExt};
use log::{error, info, warn};
use shvclient::{AppState, ClientEventsReceiver};
use shvproto::DateTime as ShvDateTime;
use shvrpc::metamethod::AccessLevel;
use shvrpc::{RpcMessage, RpcMessageMetaTags};
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::datachange::DataChange;
use crate::journalentry::JournalEntry;
use crate::journalrw::{JournalReaderLog2, JournalWriterLog2, VALUE_FLAG_SPONTANEOUS_BIT};
use crate::util::{get_files, is_log2_file};
use crate::{ClientCommandSender, State};

use shvclient::clientnode::find_longest_path_prefix;

#[derive(Debug)]
pub(crate) enum DirtyLogCommand {
    ProcessNotification(RpcMessage),
    Trim {
        site: String
    },
    Get {
        site: String,
        response_tx: OneshotSender<Vec<JournalEntry>>,
    }
}

pub(crate) async fn dirtylog_task(
    _client_cmd_tx: ClientCommandSender,
    _client_evt_rx: ClientEventsReceiver,
    app_state: AppState<State>,
    mut cmd_rx: UnboundedReceiver<DirtyLogCommand>,
) {
    // Per site request
    enum Request {
        Get(OneshotSender<Vec<JournalEntry>>),
        Append(JournalEntry),
        Trim,
    }

    async fn process_request(site: String, journal_dir: PathBuf, request: Request) {
        match request {
            Request::Get(response_sender) => {
                // Load the dirty log and return it in the response channel
                let dirty_log_path = journal_dir.join(site).join("dirtylog");
                let res = match tokio::fs::File::open(&dirty_log_path).await {
                    Ok(file) => {
                        let reader = JournalReaderLog2::new(BufReader::new(file.compat())).enumerate();
                        reader
                            .filter_map(|(entry_no, entry_res)| {
                                let entry = entry_res.inspect_err(|err|
                                    warn!("Invalid journal entry no. {entry_no} in dirty log at {log_path}: {err}",
                                        log_path = dirty_log_path.to_string_lossy()
                                    )
                                )
                                    .ok();
                                async { entry }
                            })
                        .collect::<Vec<_>>()
                            .await
                    }
                    Err(err) => {
                        if err.kind() != std::io::ErrorKind::NotFound {
                            error!("Cannot open {log_path}: {err}", log_path = dirty_log_path.to_string_lossy());
                        }
                        Vec::new()
                    }
                };
                response_sender.send(res).unwrap_or_default();
            }
            Request::Append(journal_entry) => {
                let journal_site_path = journal_dir.join(site);
                if !journal_site_path.exists() {
                    warn!("Ignoring notification while journal directory {journal_site_path} for the site does not exist",
                        journal_site_path = journal_site_path.to_string_lossy()
                    );
                    return;
                }
                // Append to the site's dirty log
                let dirty_log_path = journal_site_path.join("dirtylog");
                let dirty_log_file = match tokio::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&dirty_log_path)
                    .await {
                        Ok(file) => file,
                        Err(err) => {
                            error!("Cannot append a notification to dirty log. Cannot open file {file_path} for write: {err}",
                                file_path = dirty_log_path.to_string_lossy()
                            );
                            return;
                        }
                    };
                let mut writer = JournalWriterLog2::new(dirty_log_file.compat());
                writer.append(&journal_entry)
                    .await
                    .unwrap_or_else(|err|
                        error!("Cannot append a notification to dirty log {log_file}: {err}",
                            log_file = dirty_log_path.to_string_lossy()
                        )
                    );
            }
            Request::Trim => {
                info!("Trim dirty log start, site: {site}");
                // Get the latest entry from the site log and remove all entries up to that
                // entry from the dirty log
                let dirty_log_path = journal_dir.join(&site).join("dirtylog");
                let dirty_log_file = match tokio::fs::File::open(&dirty_log_path).await {
                    Ok(file) => file,
                    Err(err) => {
                        if err.kind() == std::io::ErrorKind::NotFound {
                            info!("Trim dirty log done, no dirty log file for the site");
                        } else {
                            error!("Cannot trim dirty log. Cannot open file {file_path}: {err}", file_path = dirty_log_path.to_string_lossy());
                        }
                        return;
                    }
                };

                let latest_entry = {
                    let mut log_files = match get_files(journal_dir.join(&site), is_log2_file).await {
                        Ok(files) => files,
                        Err(err) => {
                            error!("Cannot trim dirty log. Cannot read journal dir entries: {err}");
                            return;
                        }
                    };
                    log_files.sort_by_key(|entry| std::cmp::Reverse(entry.file_name()));

                    let latest_entry = Box::pin(futures::stream::iter(log_files)
                        .map(|file_entry| file_entry.path())
                        .then(|file_path|
                            async move {
                                match tokio::fs::File::open(&file_path).await {
                                    Ok(file) => {
                                        let reader = JournalReaderLog2::new(BufReader::new(file.compat()));
                                        reader.fold(None, async |_, entry| entry.ok()).await
                                    }
                                    Err(err) => {
                                        error!("Cannot open file {file_path} while getting the last journal entry for trim dirtylog: {err}",
                                            file_path = file_path.to_string_lossy()
                                        );
                                        None
                                    }
                        }
                            })
                        .filter_map(async |entry| entry))
                        .next()
                        .await;

                    match latest_entry {
                        Some(entry) => entry,
                        None => {
                            info!("Trim dirty log done, no journal entries in synced files");
                            return;
                        }
                    }
                };

                // Remove all entries older than the latest entry from the dirty log
                let reader = JournalReaderLog2::new(BufReader::new(dirty_log_file.compat()));
                let trimmed_log = reader.filter_map(|entry| {
                    let passed_entry = entry
                        .as_ref()
                        .ok()
                        .filter(|entry| entry.epoch_msec >= latest_entry.epoch_msec)
                        .cloned();
                    async move { passed_entry }
                })
                .collect::<Vec<_>>()
                    .await;

                // Write the dirty log
                let dirty_log_file = match tokio::fs::OpenOptions::new()
                    .write(true)
                    .truncate(true)
                    .open(&dirty_log_path)
                    .await {
                        Ok(file) => file,
                        Err(err) => {
                            error!("Cannot trim dirty log. Cannot open file {file_path} for write: {err}",
                                file_path = dirty_log_path.to_string_lossy()
                            );
                            return;
                        }
                    };
                let mut writer = JournalWriterLog2::new(dirty_log_file.compat());
                for entry in &trimmed_log {
                    writer.append(entry)
                        .await
                        .unwrap_or_else(|err|
                            error!("Cannot write a journal entry to dirty log {log_file}: {err}",
                                log_file = dirty_log_path.to_string_lossy()
                            )
                        );
                }
                info!("Trim dirty log done, site: {site}");

            }
        }
    }

    // Schedules requests per site in order, across sites concurrently
    struct RequestScheduler {
        per_site: HashMap<String, VecDeque<Request>>,
        running: HashSet<String>,
        // Global pool of running site tasks (max 1 per site)
        inflight: FuturesUnordered<Pin<Box<dyn Future<Output = String> + Send>>>,
        journal_dir: PathBuf,
    }

    impl RequestScheduler {
        fn new(journal_dir: PathBuf) -> Self {
            Self {
                per_site: Default::default(),
                running: Default::default(),
                inflight: Default::default(),
                journal_dir,
            }
        }

        fn _schedule_next(&mut self, site: String) {
            if self.running.contains(&site) {
                return;
            }
            if let Some(queue) = self.per_site.get_mut(&site)
                && let Some(request) = queue.pop_front() {
                    self.running.insert(site.clone());
                    let journal_dir = self.journal_dir.clone();
                    self.inflight.push(Box::pin(async move {
                        process_request(site.clone(), journal_dir, request).await;
                        site
                    }));
            }
        }

        fn schedule_new(&mut self, site: String, request: Request) {
            self.per_site.entry(site.clone()).or_default().push_back(request);
            self._schedule_next(site);
        }

        fn on_finished(&mut self, site: String) {
            self.running.remove(&site);
            self._schedule_next(site);
        }
    }

    let mut request_scheduler = RequestScheduler::new(Path::new(&app_state.config.journal_dir).into());

    loop {
        futures::select! {
            command = cmd_rx.select_next_some() => match command {
                DirtyLogCommand::Trim { site } => {
                    request_scheduler.schedule_new(site, Request::Trim);
                }
                DirtyLogCommand::Get { site, response_tx } => {
                    request_scheduler.schedule_new(site, Request::Get(response_tx));
                }
                DirtyLogCommand::ProcessNotification(msg) => {
                    let signal = msg.method().unwrap_or_default().to_string();
                    let path = msg.shv_path().unwrap_or_default().to_string();
                    let param = msg.param().unwrap_or_default();

                    let Some(stripped_path) = path.strip_prefix("shv/") else {
                        continue;
                    };
                    let sites_data = app_state
                        .sites_data
                        .read()
                        .await;
                    let Some((site_path, property_path)) = find_longest_path_prefix(&*sites_data.sites_info, stripped_path) else {
                        continue;
                    };
                    drop(sites_data);

                    let data_change = DataChange::from(param.clone());
                    let journal_entry = JournalEntry {
                        epoch_msec: data_change.date_time.unwrap_or_else(ShvDateTime::now).epoch_msec(),
                        path: property_path.to_string(),
                        signal,
                        source: Default::default(),
                        value: data_change.value,
                        access_level: AccessLevel::Read as _,
                        short_time: data_change.short_time.unwrap_or(-1),
                        user_id: Default::default(),
                        repeat: data_change.value_flags & (1 << VALUE_FLAG_SPONTANEOUS_BIT) == 0,
                        provisional: true, // data_change.value_flags & (1 << VALUE_FLAG_PROVISIONAL_BIT) != 0,
                    };
                    // Schedule next task
                    request_scheduler.schedule_new(site_path.into(), Request::Append(journal_entry));
                }
            },
            site = request_scheduler.inflight.select_next_some() => {
                request_scheduler.on_finished(site);
            }
            complete => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
    use log::debug;
    use shvclient::client::ClientCommand;
    use shvproto::DateTime;
    use shvrpc::{rpcframe::RpcFrame, RpcMessage};

    use crate::{State, datachange::DataChange, dirtylog::{DirtyLogCommand, dirtylog_task}, journalentry::JournalEntry, sync::SyncCommand, util::{DedupReceiver, init_logger, testing::{PrettyJoinError, TestStep, run_test}}};

    struct DirtylogTaskTestState {
        sender: UnboundedSender<DirtyLogCommand>,
        _sync_cmd_rx: DedupReceiver<SyncCommand>,
    }

    struct TestDirtyLogCommand(DirtyLogCommand);

    #[async_trait::async_trait]
    impl TestStep<DirtylogTaskTestState> for TestDirtyLogCommand {
        async fn exec(&self, _client_command_receiver: &mut UnboundedReceiver<ClientCommand<State>>, _subscriptions: &mut HashMap<String, UnboundedSender<RpcFrame>>, state: &mut DirtylogTaskTestState) {
            let cmd = match &self.0 {
                DirtyLogCommand::ProcessNotification(msg) => DirtyLogCommand::ProcessNotification(msg.clone()),
                DirtyLogCommand::Trim { site } => DirtyLogCommand::Trim { site: site.clone() },
                DirtyLogCommand::Get { .. } => panic!("Cannot send DirtyLogCommand::Get through TestDirtyLogCommand"),
            };
            debug!(target: "test-driver", "Sending DirtyLogCommand::{cmd:?}");
            state.sender.unbounded_send(cmd).expect("Sending DirtyLogCommands should succeed");
        }
    }

    struct TestGetDirtyLog {
        site: String,
        expected: Vec<JournalEntry>,
    }

    #[async_trait::async_trait]
    impl TestStep<DirtylogTaskTestState> for TestGetDirtyLog {
        async fn exec(&self, _client_command_receiver: &mut UnboundedReceiver<ClientCommand<State>>, _subscriptions: &mut HashMap<String, UnboundedSender<RpcFrame>>, state: &mut DirtylogTaskTestState) {
            debug!(target: "test-driver", "Sending DirtyLogCommand::Get");
            let (sender, receiver) = futures::channel::oneshot::channel();
            state.sender.unbounded_send(DirtyLogCommand::Get { site: self.site.clone(), response_tx: sender }).expect("Sending DirtyLogCommands should succeed");
            let dirtylog = receiver.await.expect("Getting dirtylog must succeed");
            assert_eq!(dirtylog, self.expected);
        }
    }

    struct TestCase<'a> {
        name: &'static str,
        steps: &'a [Box<dyn TestStep<DirtylogTaskTestState>>],
        starting_files: Vec<(&'static str, &'static str)>,
        expected_file_paths: Vec<(&'static str, &'a str)>,
    }

    #[tokio::test]
    async fn dirtylog_task_test() -> std::result::Result<(), PrettyJoinError> {
        init_logger();

        let test_cases = [
            TestCase {
                name: "ProcessNotification: journaldir doesn't exist",
                steps: &[
                    Box::new(TestDirtyLogCommand(DirtyLogCommand::ProcessNotification(RpcMessage::new_signal("shv/site1/some_value_node", "chng", Some(20.into())))))
                ],
                starting_files: vec![],
                expected_file_paths: vec![],
            },
            TestCase {
                name: "ProcessNotification: non-shv/ notifications are skipped",
                steps: &[
                    Box::new(TestDirtyLogCommand(DirtyLogCommand::ProcessNotification(RpcMessage::new_signal("something_else/site1/some_value_node", "chng", Some(20.into())))))
                ],
                starting_files: vec![],
                expected_file_paths: vec![],
            },
            TestCase {
                name: "ProcessNotification: notifications from unknown sites are skipped",
                steps: &[
                    Box::new(TestDirtyLogCommand(DirtyLogCommand::ProcessNotification(RpcMessage::new_signal("shv/unknown_site/some_value_node", "chng", Some(20.into())))))
                ],
                starting_files: vec![],
                expected_file_paths: vec![],
            },
            TestCase {
                name: "ProcessNotification: notifications get written to disk",
                steps: &[
                    Box::new(TestDirtyLogCommand(DirtyLogCommand::ProcessNotification(RpcMessage::new_signal("shv/site1/some_value_node", "chng", Some(DataChange{
                        value: 20.into(),
                        date_time: Some(DateTime::from_iso_str("2022-07-07T00:00:00.000").expect("DateTime must work")),
                        value_flags: 0,
                        short_time: None,
                    }.into()))))),
                ],
                starting_files: vec![("site1/2022-07-07T18-06-15-000.log2", "")],
                expected_file_paths: vec![
                    ("site1/2022-07-07T18-06-15-000.log2", ""),
                    (
                        "site1/dirtylog",
                        "2022-07-07T00:00:00.000Z\t\tsome_value_node\t20\t\tchng\t4\t\n"
                    )
                ],
            },
            TestCase {
                name: "Get nonexisting dirtylog",
                steps: &[
                    Box::new(TestGetDirtyLog{
                        site: "site1".to_string(),
                        expected: vec![],
                    })
                ],
                starting_files: vec![("site1/2022-07-07T18-06-15-000.log2", "")],
                expected_file_paths: vec![
                    ("site1/2022-07-07T18-06-15-000.log2", ""),
                ],
            },
            TestCase {
                name: "Get existing empty dirtylog",
                steps: &[
                    Box::new(TestGetDirtyLog{
                        site: "site1".to_string(),
                        expected: vec![],
                    })
                ],
                starting_files: vec![("site1/dirtylog", "")],
                expected_file_paths: vec![
                    ("site1/dirtylog", ""),
                ],
            },
            TestCase {
                name: "Get existing dirtylog with entry",
                steps: &[
                    Box::new(TestGetDirtyLog{
                        site: "site1".to_string(),
                        expected: vec![JournalEntry {
                            epoch_msec: 1657152000000,
                            path: "some_value_node".into(),
                            signal: "chng".into(),
                            source: "get".into(),
                            value: 20.into(),
                            access_level: 8,
                            short_time: -1,
                            user_id: None,
                            repeat: true,
                            provisional: true
                        }],
                    })
                ],
                starting_files: vec![("site1/dirtylog", "2022-07-07T00:00:00.000Z\t\tsome_value_node\t20\t\tchng\t4\t\n")],
                expected_file_paths: vec![
                    ("site1/dirtylog", "2022-07-07T00:00:00.000Z\t\tsome_value_node\t20\t\tchng\t4\t\n"),
                ],
            },
            TestCase {
                name: "Existing dirtylog with invalid entries",
                steps: &[
                    Box::new(TestGetDirtyLog{
                        site: "site1".to_string(),
                        expected: vec![JournalEntry {
                            epoch_msec: 1657152000000,
                            path: "some_value_node".into(),
                            signal: "chng".into(),
                            source: "get".into(),
                            value: 20.into(),
                            access_level: 8,
                            short_time: -1,
                            user_id: None,
                            repeat: true,
                            provisional: true
                        }],
                    })
                ],
                starting_files: vec![
                    (
                        "site1/dirtylog",
                        concat!(
                            "2022-07-07T00:00:00.000Z\t\tsome_value_node\t20\t\tchng\t4\t\n",
                            "sadjfasjn",
                        )
                    )
                ],
                expected_file_paths: vec![
                    (
                        "site1/dirtylog",
                        concat!(
                            "2022-07-07T00:00:00.000Z\t\tsome_value_node\t20\t\tchng\t4\t\n",
                            "sadjfasjn",
                        )
                    )
                ],
            },
            TestCase {
                name: "Trim: journaldir doesn't exist",
                steps: &[
                    Box::new(TestDirtyLogCommand(DirtyLogCommand::Trim{site: "site1".to_string()}))
                ],
                starting_files: vec![],
                expected_file_paths: vec![],
            },
            TestCase {
                name: "Trim: no dirtylog",
                steps: &[
                    Box::new(TestDirtyLogCommand(DirtyLogCommand::Trim{site: "site1".to_string()}))
                ],
                starting_files: vec![("site1/2022-07-07T18-06-15-000.log2", "")],
                expected_file_paths: vec![
                    ("site1/2022-07-07T18-06-15-000.log2", ""),
                ],
            },
            TestCase {
                name: "Trim: no files to trim from",
                steps: &[
                    Box::new(TestDirtyLogCommand(DirtyLogCommand::Trim{site: "site1".to_string()}))
                ],
                starting_files: vec![("site1/dirtylog", "2022-07-07T00:00:00.000Z\t\tsome_value_node\t20\t\tchng\t4\t\n")],
                expected_file_paths: vec![
                    ("site1/dirtylog", "2022-07-07T00:00:00.000Z\t\tsome_value_node\t20\t\tchng\t4\t\n"),
                ],
            },
            TestCase {
                name: "Trim: nothing to trim",
                steps: &[
                    Box::new(TestDirtyLogCommand(DirtyLogCommand::Trim{site: "site1".to_string()}))
                ],
                starting_files: vec![("site1/dirtylog", "2022-07-07T00:00:00.000Z\t\tsome_value_node\t20\t\tchng\t4\t\n")],
                expected_file_paths: vec![
                    ("site1/dirtylog", "2022-07-07T00:00:00.000Z\t\tsome_value_node\t20\t\tchng\t4\t\n"),
                ],
            },
            TestCase {
                name: "Trim: emptying dirtylog",
                steps: &[
                    Box::new(TestDirtyLogCommand(DirtyLogCommand::Trim{site: "site1".to_string()}))
                ],
                starting_files: vec![
                    (
                        "site1/2022-07-06T18-06-15-000.log2",
                        concat!(
                            "2022-07-06T00:00:00.000Z\t\tsome_value_node\t20\t\tchng\t4\t\n",
                            "2022-07-08T00:00:00.000Z\t\tsome_value_node\t20\t\tchng\t4\t\n"
                        )
                    ),
                    ("site1/dirtylog", "2022-07-07T00:00:00.000Z\t\tsome_value_node\t20\t\tchng\t4\t\n")
                ],
                expected_file_paths: vec![
                    (
                        "site1/2022-07-06T18-06-15-000.log2",
                        concat!(
                            "2022-07-06T00:00:00.000Z\t\tsome_value_node\t20\t\tchng\t4\t\n",
                            "2022-07-08T00:00:00.000Z\t\tsome_value_node\t20\t\tchng\t4\t\n",
                        )
                    ),
                    ("site1/dirtylog", ""),
                ],
            },
            TestCase {
                name: "Trim: trimming some entries from the dirtylog",
                steps: &[
                    Box::new(TestDirtyLogCommand(DirtyLogCommand::Trim{site: "site1".to_string()}))
                ],
                starting_files: vec![
                    (
                        "site1/2022-07-06T18-06-15-000.log2",
                        "2022-07-06T00:00:00.000Z\t\tsome_value_node\t20\t\tchng\t4\t\n"
                    ),
                    (
                        "site1/dirtylog",
                        concat!(
                            "2022-07-06T00:00:00.000Z\t\tsome_value_node\t20\t\tchng\t4\t\n",
                            "2022-07-08T00:00:00.000Z\t\tsome_value_node\t20\t\tchng\t4\t\n",
                        )
                    ),
                ],
                expected_file_paths: vec![
                    (
                        "site1/2022-07-06T18-06-15-000.log2",
                        "2022-07-06T00:00:00.000Z\t\tsome_value_node\t20\t\tchng\t4\t\n"
                    ),
                    (
                        "site1/dirtylog",
                        concat!(
                            "2022-07-06T00:00:00.000Z\t\tsome_value_node\t20\t\tchng\t4\t\n",
                            "2022-07-08T00:00:00.000Z\t\tsome_value_node\t20\t\tchng\t4\t\n",
                        )
                    ),
                ],
            },
        ];

        for test_case in test_cases {
            run_test(
                test_case.name,
                test_case.steps,
                test_case.starting_files,
                test_case.expected_file_paths,
                |ccs, _ces, cer, _dirtylog_cmd_rx, _sync_cmd_rx, state| {
                    let (sender, receiver) = unbounded();
                    let task_state = DirtylogTaskTestState {
                        sender,
                        _sync_cmd_rx,
                    };
                    let dirtylog_task = tokio::spawn(dirtylog_task(ccs, cer, state, receiver));
                    (dirtylog_task, task_state)
                },
                |state| {
                    state.sender.close_channel();
                },
                &[]
            ).await?;
        }

        Ok(())
    }
}
