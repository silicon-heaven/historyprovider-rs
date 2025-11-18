use std::borrow::Cow;
use std::collections::HashSet;
use std::hash::Hash;
use std::path::Path;
use std::pin::Pin;
use std::task::Poll;

use futures::{Stream, StreamExt, TryStreamExt};
use shvclient::client::ShvApiVersion;
use shvrpc::join_path;
use shvrpc::rpc::ShvRI;
use tokio::fs::DirEntry;
use tokio_stream::wrappers::ReadDirStream;

use crate::{ClientCommandSender, Subscriber};

#[cfg(test)]
use std::sync::Once;
#[cfg(test)]
use simple_logger::SimpleLogger;

#[cfg(test)]
pub(crate) fn init_logger() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        SimpleLogger::new()
            .with_level(log::LevelFilter::Debug)
            .init()
            .unwrap();
        });
}

pub(crate) fn subscription_prefix_path<'a>(path: impl Into<Cow<'a, str>>, api_version: &ShvApiVersion) -> String {
    let path = path.into();
    match api_version {
        ShvApiVersion::V2 => path.into(),
        ShvApiVersion::V3 => join_path!(&path, "*"),
    }
}

pub(crate) async fn subscribe(
    client_cmd_tx: &ClientCommandSender,
    path: impl AsRef<str>,
    signal: impl AsRef<str>,
) -> Subscriber
{
    let path = path.as_ref();
    let signal = signal.as_ref();
    client_cmd_tx
        .subscribe(
            ShvRI::from_path_method_signal(path, "*", Some(signal))
            .unwrap_or_else(|e| panic!("Invalid ShvRI for path `{path}`, signal `{signal}`: {e}"))
        )
        .await
        .unwrap_or_else(|e| panic!("Subscribe `{signal}` on path `{path}`: {e}"))
}

pub(crate) async fn get_files(dir_path: impl AsRef<Path>, file_filter_fn: impl Fn(&DirEntry) -> bool) -> Result<Vec<DirEntry>, String> {
    let dir_path = dir_path.as_ref();
    let journal_dir = ReadDirStream::new(tokio::fs::read_dir(dir_path)
        .await
        .map_err(|e|
            format!("Cannot read journal directory at {}: {}", dir_path.to_string_lossy(), e)
        )?
    );
    journal_dir.try_filter_map(async |entry| {
        Ok(entry
            .metadata()
            .await?
            .is_file()
            .then(|| file_filter_fn(&entry).then_some(entry))
            .flatten()
        )
    })
    .try_collect::<Vec<_>>()
    .await
    .map_err(|e| format!("Cannot read content of the journal directory {}: {}", dir_path.to_string_lossy(), e))
}

pub(crate) fn is_log2_file(entry: &DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .is_some_and(|file_name| file_name.ends_with(".log2"))
}

#[derive(Clone)]
pub(crate) struct DedupSender<T: Eq + Hash + Clone> {
    sender: futures::channel::mpsc::UnboundedSender<T>,
    pending: std::sync::Arc<std::sync::Mutex<HashSet<T>>>,
}

impl<T: Eq + Hash + Clone> DedupSender<T> {
    pub(crate) fn send(&self, msg: T) -> Result<bool, futures::channel::mpsc::TrySendError<T>> {
        let mut pending = self.pending.lock().expect("Tried to lock a mutex already held by the same thread");
        if pending.contains(&msg) {
            return Ok(false);
        }
        self.sender
            .unbounded_send(msg.clone())
            .map(|_| {
                pending.insert(msg);
                true
            })
    }

    #[cfg(test)]
    pub(crate) fn close_channel(&self) {
        self.sender.close_channel();
    }
}

pub(crate) struct DedupReceiver<T: Eq + Hash + Clone> {
    receiver: futures::channel::mpsc::UnboundedReceiver<T>,
    pending: std::sync::Arc<std::sync::Mutex<HashSet<T>>>,
}

impl<T: Eq + Hash + Clone + Unpin> Stream for DedupReceiver<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let next = this.receiver.poll_next_unpin(cx);
        if let Poll::Ready(Some(msg)) = &next {
            let mut pending = this.pending.lock().expect("Tried to lock a mutex already held by the same thread");
            pending.remove(msg);
        }
        next
    }
}

pub(crate) fn dedup_channel<T: Eq + Hash + Clone>() -> (DedupSender<T>, DedupReceiver<T>) {
    let pending = std::sync::Arc::new(std::sync::Mutex::new(HashSet::new()));
    let (sender, receiver) = futures::channel::mpsc::unbounded();
    (DedupSender { pending: pending.clone(), sender }, DedupReceiver { pending, receiver })
}

#[cfg(test)]
pub mod testing {
    use crate::{State, dirtylog::DirtyLogCommand, sites::{SiteInfo, SitesData, SubHpInfo}, sync::SyncCommand, util::{DedupReceiver, dedup_channel}};
    use futures::{channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender}, StreamExt};
    use log::debug;
    use shvclient::{client::{ClientCommand, ClientEventsReceiver}, AppState, ClientCommandSender};
    use shvproto::RpcValue;
    use shvrpc::{rpcframe::RpcFrame, rpcmessage::RpcError, RpcMessage, RpcMessageMetaTags};
    use std::{collections::{BTreeMap, HashMap}, path::{Path, PathBuf}, sync::Arc};
    use tempfile::TempDir;
    use tokio::{io::AsyncWriteExt, sync::RwLock};

    pub struct PrettyJoinError(String);

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
                    format!("task {id} panicked with message: {panic_message}")
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

    fn _list_files(vec: &mut Vec<PathBuf>, path: &Path) -> std::io::Result<()> {
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

    pub fn list_files(path: &Path) -> Vec<(String, String)> {
        let mut res = Vec::new();
        _list_files(&mut res, path).expect("Failed to list journal files");
        let mut res = res
            .into_iter()
            .map(|path| (path.to_string_lossy().to_string(), std::fs::read_to_string(path).expect("Reading file should work")))
            .collect::<Vec<_>>();
        res.sort_by(|(path, _), (path2, _)| path.cmp(path2));
        res
    }

    pub async fn expect_rpc_call(client_command_receiver: &mut UnboundedReceiver<ClientCommand<State>>, expected_shv_path: &str, expected_method: &str, expected_param: Option<RpcValue>, return_val: Result<RpcValue, RpcError>) {
        let Some(event) = client_command_receiver.next().await else {
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
                panic!("got unexpected event other than rpccall: {}", print_client_command(event));
            }
        }
    }

    pub fn print_client_command<T>(cmd: ClientCommand<T>) -> String {
        match cmd {
            ClientCommand::SendMessage { message } => format!("SendMessage({})", message.to_cpon()),
            ClientCommand::RpcCall { request, .. } => format!("RpcCall({:?}, {:?}, {:?})", request.shv_path(), request.method(), request.param()),
            ClientCommand::Subscribe { ri, subscription_id, .. } => format!("Subscribe({ri}) -> {subscription_id}"),
            ClientCommand::Unsubscribe { subscription_id } => format!("Unsubscribe({subscription_id})"),
            ClientCommand::MountNode { path, .. } => format!("MountNode({path})"),
            ClientCommand::UnmountNode { path } => format!("UnmountNode({path})"),
            ClientCommand::TerminateClient => "TerminateClient".to_string(),
        }
    }

    pub async fn expect_subscription(client_command_receiver: &mut UnboundedReceiver<ClientCommand<State>>, expected_ri: &shvrpc::rpc::ShvRI) -> UnboundedSender<RpcFrame> {
        let Some(event) = client_command_receiver.next().await else {
            panic!("expected event, but got none");
        };
        match event {
            ClientCommand::Subscribe { ri, notifications_tx, .. } => {
                debug!(target: "test-driver", "subscription: {ri}");
                assert_eq!(&ri, expected_ri);
                let subscribe_response = RpcMessage::new_request("dummy", "dummy", None).prepare_response().unwrap().to_frame().unwrap();
                notifications_tx.unbounded_send(subscribe_response).unwrap();
                notifications_tx
            },
            _ => {
                panic!("got unexpected event other than Subscribe: {}", print_client_command(event));
            }
        }
    }

    pub async fn expect_unsubscription(client_command_receiver: &mut UnboundedReceiver<ClientCommand<State>>) {
        let Some(event) = client_command_receiver.next().await else {
            panic!("expected event, but got none");
        };
        match event {
            ClientCommand::Unsubscribe { subscription_id } => {
                debug!(target: "test-driver", "unsubscription: {subscription_id}");
            },
            _ => {
                panic!("got unexpected event other than Unsubscribe: {}", print_client_command(event));
            }
        }
    }

    #[async_trait::async_trait]
    pub trait TestStep<TestState> {
        async fn exec(&self, client_command_receiver: &mut UnboundedReceiver<ClientCommand<State>>, subscriptions: &mut HashMap<String, UnboundedSender<RpcFrame>>, state: &mut TestState);
    }

    pub struct ExpectCall(pub &'static str, pub &'static str, pub Result<RpcValue, RpcError>);
    #[async_trait::async_trait]
    impl<TestState> TestStep<TestState> for ExpectCall {
        async fn exec(&self, client_command_receiver: &mut UnboundedReceiver<ClientCommand<State>>, _subscriptions: &mut HashMap<String, UnboundedSender<RpcFrame>>,  _state: &mut TestState) {
            let ExpectCall(path, method, ret_val) = self;
            expect_rpc_call(client_command_receiver, path, method, None, ret_val.clone()).await;
        }
    }

    pub struct ExpectSignal(pub &'static str, pub &'static str, pub RpcValue);
    #[async_trait::async_trait]
    impl<TestState> TestStep<TestState> for ExpectSignal {
        async fn exec(&self, client_command_receiver: &mut UnboundedReceiver<ClientCommand<State>>, _subscriptions: &mut HashMap<String, UnboundedSender<RpcFrame>>,  _state: &mut TestState) {
            let ExpectSignal(path, method, param) = self;
            expect_signal(client_command_receiver, path, method, param).await;
            pub async fn expect_signal(client_command_receiver: &mut UnboundedReceiver<ClientCommand<State>>, expected_shv_path: &str, expected_method: &str, expected_param: &RpcValue) {
                let Some(event) = client_command_receiver.next().await else {
                    panic!("got unexpected event");
                };
                match event {
                    ClientCommand::SendMessage { message} => {
                        let shv_path = message.shv_path().expect("shv path should exist");
                        let method = message.method().expect("shv path should exist");
                        let param = message.param().unwrap_or_default();
                        debug!(target: "test-driver", "<== {shv_path}:{method}, param: {param}");
                        assert!(message.is_signal());
                        assert_eq!(shv_path, expected_shv_path);
                        assert_eq!(method, expected_method);
                        assert_eq!(param, expected_param);
                    },
                    _ => {
                        panic!("got unexpected event other than SendMessage: {}", print_client_command(event));
                    }
                }
            }
        }
    }

    pub struct ExpectCallParam(pub &'static str, pub &'static str, pub RpcValue, pub Result<RpcValue, RpcError>);
    #[async_trait::async_trait]
    impl<TestState> TestStep<TestState> for ExpectCallParam {
        async fn exec(&self, client_command_receiver: &mut UnboundedReceiver<ClientCommand<State>>, _subscriptions: &mut HashMap<String, UnboundedSender<RpcFrame>>,  _state: &mut TestState) {
            let ExpectCallParam(path, method, param, ret_val) = self;
            expect_rpc_call(client_command_receiver, path, method, Some(param.clone()), ret_val.clone()).await;
        }
    }

    pub struct ExpectSubscription(pub shvrpc::rpc::ShvRI);
    #[async_trait::async_trait]
    impl<TestState> TestStep<TestState> for ExpectSubscription {
        async fn exec(&self, client_command_receiver: &mut UnboundedReceiver<ClientCommand<State>>, subscriptions: &mut HashMap<String, UnboundedSender<RpcFrame>>,  _state: &mut TestState) {
            let sub_id = self.0.to_string();
            let notifications_tx = expect_subscription(client_command_receiver, &self.0).await;
            subscriptions.insert(sub_id, notifications_tx);
        }
    }

    pub struct SendSignal(pub String, pub String, pub String, pub RpcValue);
    #[async_trait::async_trait]
    impl<TestState> TestStep<TestState> for SendSignal {
        async fn exec(&self, _client_command_receiver: &mut UnboundedReceiver<ClientCommand<State>>, subscriptions: &mut HashMap<String, UnboundedSender<RpcFrame>>,  _state: &mut TestState) {
            let sub_id = self.0.to_string();
            let (_, sender)  = subscriptions.iter().find(|(id, _)| **id == sub_id).expect("Sub must exist");
            let shv_path = self.1.as_str();
            let method = self.2.as_str();
            let param = self.3.clone();
            debug!(target: "test-driver", "==> {shv_path}:{method}, param: {param}");
            sender.unbounded_send(RpcMessage::new_signal(shv_path, method, Some(param)).to_frame().unwrap()).unwrap();
        }
    }

    pub struct ExpectUnsubscription;
    #[async_trait::async_trait]
    impl<TestState> TestStep<TestState> for ExpectUnsubscription {
        async fn exec(&self, client_command_receiver: &mut UnboundedReceiver<ClientCommand<State>>, _subscriptions: &mut HashMap<String, UnboundedSender<RpcFrame>>,  _state: &mut TestState) {
            expect_unsubscription(client_command_receiver).await;
        }
    }

    pub async fn run_test<TestState>(
        test_name: &str,
        steps: &[Box<dyn TestStep<TestState>>],
        starting_files: Vec<(&str, &str)>,
        expected_file_paths: Vec<(&str, &str)>,
        create_task: impl FnOnce(ClientCommandSender<State>, async_broadcast::Sender<shvclient::ClientEvent>, ClientEventsReceiver, UnboundedReceiver<DirtyLogCommand>, DedupReceiver<SyncCommand>, AppState<State>) -> (tokio::task::JoinHandle<()>, TestState),
        destroy_task: impl FnOnce(&mut TestState),
        cleanup_steps: &[Box<dyn TestStep<TestState>>]
    ) -> std::result::Result<(), PrettyJoinError> {
        debug!(target: "test-driver", "Running test '{test_name}'");
        let (client_command_sender, mut client_command_receiver) = unbounded();
        let client_command_sender: ClientCommandSender<State> = ClientCommandSender::from_raw(client_command_sender);
        let (client_events_sender, client_events_rx) = async_broadcast::broadcast(10);
        let (dedup_sender, dedup_receiver) = dedup_channel::<SyncCommand>();
        let client_events_receiver = ClientEventsReceiver::from_raw(client_events_rx.clone());
        let (dirtylog_cmd_tx, dirtylog_cmd_rx) = unbounded::<DirtyLogCommand>();
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
            start_time: std::time::Instant::now(),
            config: crate::HpConfig {
                journal_dir: journal_dir.path().to_str().expect("path must work").to_string(),
                max_sync_tasks: None,
                max_journal_dir_size: None,
                periodic_sync_interval: Some(3),
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
                typeinfos: Default::default(),
            }),
            sync_info: Default::default(),
            alarms: Default::default(),
            state_alarms: Default::default(),
            online_states: Default::default(),
        });


        let (sync_task, mut task_state) = create_task(client_command_sender.clone(), client_events_sender.clone(), client_events_receiver.clone(), dirtylog_cmd_rx, dedup_receiver, state.clone());

        let mut subscriptions = HashMap::new();

        for step in steps {
            step.exec(&mut client_command_receiver, &mut subscriptions, &mut task_state).await;
        }

        destroy_task(&mut task_state);
        subscriptions.clear();
        for step in cleanup_steps {
            step.exec(&mut client_command_receiver, &mut subscriptions, &mut task_state).await;
        }

        tokio::select! {
            task_end = sync_task => {
                task_end.map(|_| ()).map_err(PrettyJoinError::from)?;
            },
            unexpected_client_command = client_command_receiver.next() => {
                if let Some(unexpected_client_command) = unexpected_client_command {
                    match unexpected_client_command {
                        ClientCommand::RpcCall { request, .. } => return Err(PrettyJoinError(format!("Unexpected RpcCall: {request}"))),
                        _ => return Err(PrettyJoinError(format!("Unexpected ClientCommand: {}", print_client_command(unexpected_client_command))))
                    }
                }
            }
        }

        let prefixed_expected_paths = expected_file_paths.into_iter().map(|(path, content)| (format!("{}/{}", journal_dir.path().to_string_lossy(), path), content.to_string())).collect::<Vec<_>>();
        assert_eq!(list_files(journal_dir.path()), prefixed_expected_paths);

        Ok(())
    }
}
