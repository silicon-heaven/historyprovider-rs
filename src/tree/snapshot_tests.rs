use std::time::Duration;

use futures::channel::mpsc::{UnboundedReceiver, unbounded};
use shvclient::{ConnectionCommand, ConnectionEvent};
use shvproto::DateTime;
use shvrpc::journalentry::JournalEntry;

use super::*;
use crate::sites::SiteInfo;
use crate::snapshot::SnapshotEntries;

struct TestClient {
    state: Arc<State>,
    events: UnboundedSender<ConnectionEvent>,
    commands: UnboundedReceiver<ConnectionCommand>,
    task: tokio::task::JoinHandle<shvrpc::Result<()>>,
    _journal_dir: tempfile::TempDir,
}

impl TestClient {
    async fn new(sub_hp: SubHpInfo) -> Self {
        let journal_dir = tempfile::tempdir().unwrap();
        let config = HpConfig {
            journal_dir: journal_dir.path().to_str().unwrap().into(),
            ..Default::default()
        };
        let (state, client, _) = crate::make_client(&config, &mut crate::AppTasks::default()).unwrap();
        *state.sites_data.write().await = SitesData {
            sites_info: Arc::new(BTreeMap::from([("group/site".into(), SiteInfo {
                name: "site".into(), site_type: "test".into(), sub_hp: "group/site".into(),
            })])),
            sub_hps: Arc::new(BTreeMap::from([("group/site".into(), sub_hp)])),
            typeinfos: Arc::default(),
        };
        let (events, receiver) = unbounded();
        let (commands, command_receiver) = unbounded();
        events.unbounded_send(ConnectionEvent::Connected(commands)).unwrap();
        let task = tokio::spawn(client.mock_run_with_init(|_, _| {}, receiver));
        let mut result = Self { state, events, commands: command_receiver, task, _journal_dir: journal_dir };
        let request = result.message().await;
        assert_eq!(request.shv_path(), Some(".broker"));
        let mut response = request.prepare_response().unwrap();
        response.set_result(vec!["client"]);
        result.events.unbounded_send(ConnectionEvent::RpcFrameReceived(response.to_frame().unwrap())).unwrap();
        result
    }

    async fn message(&mut self) -> RpcMessage {
        let Some(ConnectionCommand::SendMessage(message)) = tokio::time::timeout(Duration::from_secs(2), self.commands.next()).await.unwrap() else {
            panic!("Expected RPC message");
        };
        message
    }

    async fn call(&mut self, path: &str, method: &str, param: RpcValue, access: AccessLevel) -> Result<RpcValue, RpcError> {
        let mut request = RpcMessage::new_request(path, method).with_param(param);
        request.set_access_level(access);
        self.events.unbounded_send(ConnectionEvent::RpcFrameReceived(request.to_frame().unwrap())).unwrap();
        let response = self.message().await;
        assert_eq!(response.request_id(), request.request_id());
        response.response().map(|response| response.success().unwrap().clone())
    }

    async fn set_snapshot(&self, snapshot: SiteSnapshot) {
        self.state.snapshots.write().await.insert("group/site".into(), snapshot);
    }

    async fn close(self) {
        self.events.unbounded_send(ConnectionEvent::Disconnected).unwrap();
        drop(self.events);
        tokio::time::timeout(Duration::from_secs(2), self.task).await.unwrap().unwrap().unwrap();
    }
}

fn entries() -> SnapshotEntries {
    [
        ("device/status", "chng", "get", 1, AccessLevel::Read, "", false),
        ("device/status", "cmdlog", "set", 2, AccessLevel::Read, "operator", true),
        ("device/status", "chng", "custom", 3, AccessLevel::Read, "", false),
        ("device/private", "chng", "get", 4, AccessLevel::Service, "", false),
        ("device2/status", "chng", "get", 5, AccessLevel::Read, "", false),
    ].into_iter().map(|(path, signal, source, value, access, user, repeat)| {
        let entry = JournalEntry {
            epoch_msec: 1000, path: path.into(), signal: signal.into(), source: source.into(),
            value: value.into(), access_level: access as i32, user_id: user.into(), repeat,
            epoch_msec_orig: None, short_time: -1, provisional: false,
        };
        ((path.into(), signal.into(), source.into()), entry)
    }).collect()
}

#[tokio::test]
async fn current_snapshot_format_filtering_and_discovery() {
    for sub_hp in [
        SubHpInfo::Normal { sync_path: ".app/history".into(), download_chunk_size: 1 },
        SubHpInfo::Legacy { getlog_path: ".local/history".into() },
        SubHpInfo::Records { records: vec!["maintenance".into()] },
        SubHpInfo::Records { records: vec![] },
    ] {
        let has_records = matches!(&sub_hp, SubHpInfo::Records { records } if !records.is_empty());
        let mut client = TestClient::new(sub_hp).await;
        client.set_snapshot(SiteSnapshot::Ready(entries())).await;
        let root = client.call("", "ls", RpcValue::null(), AccessLevel::Browse).await.unwrap();
        assert!(root.as_list().contains(&".history".into()));
        for (path, expected) in [(".history", vec!["group"]), (".history/group", vec!["site"])] {
            assert_eq!(client.call(path, "ls", RpcValue::null(), AccessLevel::Browse).await.unwrap(), expected.into());
        }
        let mut children = vec!["device", "device2"];
        if has_records {
            children.insert(0, ".records");
        }
        assert_eq!(client.call(".history/group/site", "ls", RpcValue::null(), AccessLevel::Browse).await.unwrap(), children.into());
        let method = client.call(".history/group/site", "dir", "getSnapshot".into(), AccessLevel::Browse).await.unwrap();
        assert_eq!(method, META_METHOD_GET_SNAPSHOT.to_rpcvalue(shvrpc::metamethod::DirFormat::IMap));
        assert_eq!(client.call(".history/group/site", "dir", "getLog".into(), AccessLevel::Browse).await.unwrap().is_imap(), has_records);

        let result = client.call(".history/group/site/device", "getSnapshot", RpcValue::null(), AccessLevel::Read).await.unwrap();
        let expected = RpcValue::from_cpon(r#"[
            i{1:d"1970-01-01T00:00:01Z", 3:"status", 5:"custom", 6:3},
            i{1:d"1970-01-01T00:00:01Z", 3:"status", 6:1},
            i{1:d"1970-01-01T00:00:01Z", 3:"status", 4:"cmdlog", 5:"set", 6:2, 7:"operator", 8:true}
        ]"#).unwrap();
        assert_eq!(result, expected);
        let filter = RpcValue::from_cpon(r#"i{2:"status:set:cmdlog"}"#).unwrap();
        let filtered = client.call(".history/group/site/device", "getSnapshot", filter, AccessLevel::Read).await.unwrap();
        assert_eq!(filtered, vec![expected.as_list().last().unwrap().clone()].into());
        let filter = RpcValue::from_cpon(r#"i{2:"**:get:chng"}"#).unwrap();
        let leaf = client.call(".history/group/site/device/status", "getSnapshot", filter, AccessLevel::Read).await.unwrap();
        assert_eq!(leaf, shvproto::make_list![shvproto::make_imap!(1 => DateTime::from_epoch_msec(1000), 6 => 1)].into());
        let all = client.call(".history/group/site", "getSnapshot", RpcValue::null(), AccessLevel::Service).await.unwrap();
        assert_eq!(all.as_list().len(), 5);
        let browse = client.call(".history/group/site", "getSnapshot", RpcValue::null(), AccessLevel::Browse).await.unwrap();
        assert_eq!(browse, shvproto::List::new().into());
        client.close().await;
    }
}

#[tokio::test]
async fn snapshot_unavailable_empty_and_invalid_requests() {
    let mut client = TestClient::new(SubHpInfo::Records { records: vec![] }).await;
    for snapshot in [SiteSnapshot::Invalid, SiteSnapshot::Initializing(entries())] {
        client.set_snapshot(snapshot).await;
        let error = client.call(".history/group/site", "getSnapshot", RpcValue::null(), AccessLevel::Read).await.unwrap_err();
        assert_eq!(error.code, RpcErrorCode::MethodCallException.into());
    }
    client.set_snapshot(SiteSnapshot::Ready(BTreeMap::new())).await;
    for param in ["null", "i{}", "i{2:null}"] {
        let result = client.call(".history/group/site", "getSnapshot", RpcValue::from_cpon(param).unwrap(), AccessLevel::Read).await.unwrap();
        assert_eq!(result, shvproto::List::new().into());
    }
    for param in ["42", "{}", "i{1:null}", "i{1:1000}", "i{2:1}", "i{2:\"invalid\"}", "i{2:\"[:get:chng\"}"] {
        let error = client.call(".history/group/site", "getSnapshot", RpcValue::from_cpon(param).unwrap(), AccessLevel::Read).await.unwrap_err();
        assert_eq!(error.code, RpcErrorCode::InvalidParam.into(), "{param}");
    }
    for path in [".history/missing", ".history/group/site2", ".history/group/site/.records"] {
        let error = client.call(path, "getSnapshot", RpcValue::null(), AccessLevel::Read).await.unwrap_err();
        assert_eq!(error.code, RpcErrorCode::MethodNotFound.into(), "{path}");
    }
    client.close().await;
    let mut client = TestClient::new(SubHpInfo::PushLog).await;
    let root = client.call("", "ls", RpcValue::null(), AccessLevel::Browse).await.unwrap();
    assert!(!root.as_list().contains(&".history".into()));
    let error = client.call(".history/group/site", "getSnapshot", RpcValue::null(), AccessLevel::Read).await.unwrap_err();
    assert_eq!(error.code, RpcErrorCode::MethodNotFound.into());
    client.close().await;
}
