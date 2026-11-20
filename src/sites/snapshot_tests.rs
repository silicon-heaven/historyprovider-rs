use futures::channel::mpsc::unbounded;
use shvclient::clientapi::ClientCommand;
use shvrpc::{RpcMessage, rpcframe::RpcFrame};

use super::*;

struct PendingSnapshot {
    request: RpcMessage,
    responses: UnboundedSender<RpcFrame>,
}

impl PendingSnapshot {
    fn respond(self, value: RpcValue) {
        let mut response = self.request.prepare_response().unwrap();
        response.set_result(value);
        self.responses.unbounded_send(response.to_frame().unwrap()).unwrap();
    }
}

struct Worker {
    state: Arc<State>,
    events: UnboundedSender<SiteEvent>,
    commands: UnboundedReceiver<ClientCommand>,
    task: tokio::task::JoinHandle<()>,
    _journal_dir: tempfile::TempDir,
}

impl Worker {
    async fn new() -> Self {
        let journal_dir = tempfile::tempdir().unwrap();
        let config = crate::HpConfig {
            journal_dir: journal_dir.path().to_str().unwrap().into(),
            ..Default::default()
        };
        let mut tasks = crate::AppTasks::default();
        let (state, _, _) = crate::make_client(&config, &mut tasks).unwrap();
        let type_info = RpcValue::from_cpon(r#"<"version":4>{
            "deviceDescriptions":{"device":{"properties":[
                {"name":"status", "typeName":"Status"}
            ]}},
            "devicePaths":{"":"device"},
            "types":{"Status":{"typeName":"Enum", "fields":[
                {"name":"Normal", "value":0},
                {"name":"Alarm", "value":1, "alarm":"warning", "stateAlarm":"error"}
            ]}}
        }"#).unwrap();
        state.sites_data.write().await.typeinfos = Arc::new(BTreeMap::from([
            ("site".into(), Ok(TypeInfo::try_from(&type_info).unwrap())),
        ]));
        state.online_states.write().await.insert("site".into(), SiteOnlineStatus::Unknown);
        state.snapshots.write().await.insert("site".into(), SiteSnapshot::Invalid);
        let (events, receiver) = unbounded();
        let (commands, command_receiver) = unbounded();
        let info = SiteInfo { name: "site".into(), site_type: "test".into(), sub_hp: "site".into() };
        let task = tokio::spawn(online_status_worker(
            "site", receiver, ClientCommandSender::from_raw(commands), state.clone(),
            SnapshotPaths::new("site", &info, &SubHpInfo::Records { records: vec![] }),
            Arc::new(Semaphore::new(1)),
        ));
        Self { state, events, commands: command_receiver, task, _journal_dir: journal_dir }
    }

    fn status(&self, status: SiteOnlineStatus) {
        self.events.unbounded_send(SiteEvent::Status(status)).unwrap();
    }

    fn notification(&self, path: &str, signal: &str, source: &str, value: i32) {
        let entry = notification_entry(ParsedNotification {
            site_path: "site".into(), property_path: path.into(), signal: signal.into(), source: source.into(),
            param: value.into(), user_id: None,
        }, DateTime::from_epoch_msec(100));
        self.events.unbounded_send(SiteEvent::Notification(entry)).unwrap();
    }

    async fn command(&mut self) -> ClientCommand {
        timeout(Duration::from_secs(2), self.commands.next()).await.unwrap().unwrap()
    }

    async fn signal(&mut self, method: &str, param: RpcValue) {
        let ClientCommand::SendMessage { message } = self.command().await else {
            panic!("Expected signal {method}");
        };
        assert_eq!(message.shv_path(), Some("site"));
        assert_eq!(message.method(), Some(method));
        assert_eq!(message.param().unwrap_or_default(), &param);
    }

    async fn alarm_signals(&mut self) {
        self.signal("alarmmod", RpcValue::null()).await;
        self.signal("statealarmmod", RpcValue::null()).await;
    }

    async fn snapshot_request(&mut self) -> PendingSnapshot {
        let ClientCommand::RpcCall { request, response_sender, .. } = self.command().await else {
            panic!("Expected getSnapshot call");
        };
        assert_eq!(request.shv_path(), Some("shv/site/.history"));
        assert_eq!(request.method(), Some("getSnapshot"));
        PendingSnapshot { request, responses: response_sender }
    }

    async fn entries(&self, ready: bool, expected: &[(&str, &str, &str, i32)]) {
        timeout(Duration::from_secs(2), async {
            loop {
                let snapshots = self.state.snapshots.read().await;
                let entries = match &snapshots["site"] {
                    SiteSnapshot::Ready(entries) if ready => Some(entries),
                    SiteSnapshot::Initializing(entries) if !ready => Some(entries),
                    _ => None,
                };
                if entries.is_some_and(|entries| entries.len() == expected.len() && expected.iter().all(|(path, signal, source, value)| {
                    entries.get(&(path.to_string(), signal.to_string(), source.to_string())).is_some_and(|entry| entry.value == (*value).into())
                })) {
                    break;
                }
                drop(snapshots);
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
    }

    async fn close(mut self) {
        self.events.close_channel();
        self.signal("onlinestatuschng", 0.into()).await;
        timeout(Duration::from_secs(2), self.task).await.unwrap().unwrap();
        assert!(matches!(self.state.snapshots.read().await["site"], SiteSnapshot::Invalid));
    }
}

#[tokio::test]
async fn snapshot_buffers_notifications_and_initializes_alarms() {
    let mut worker = Worker::new().await;
    // The notification which first makes the site online must also be buffered.
    worker.notification("status", "chng", "", 1);
    worker.signal("onlinestatuschng", 2.into()).await;
    let request = worker.snapshot_request().await;
    worker.notification("status", "chng", "", 0);
    worker.notification("status", "chng", "", 1);
    worker.notification("other", "chng", "get", 11);
    worker.notification("other", "cmdlog", "get", 12);
    worker.notification("other", "chng", "custom", 13);
    worker.notification("other", "chng", "get", 14);
    worker.entries(false, &[
        ("status", "chng", "get", 1), ("other", "chng", "get", 14),
        ("other", "cmdlog", "get", 12), ("other", "chng", "custom", 13),
    ]).await;
    assert!(worker.state.alarms.read().await.is_empty());
    assert!(worker.state.state_alarms.read().await.is_empty());

    request.respond(RpcValue::from_cpon(r#"[
        i{1:d"2026-11-14T12:00:00Z", 3:"status", 6:0},
        i{1:d"2026-11-14T12:00:00Z", 3:"baseline", 6:20}
    ]"#).unwrap());
    worker.alarm_signals().await;
    worker.entries(true, &[
        ("status", "chng", "get", 1), ("other", "chng", "get", 14),
        ("other", "cmdlog", "get", 12), ("other", "chng", "custom", 13),
        ("baseline", "chng", "get", 20),
    ]).await;
    let snapshot_timestamp = RpcValue::from_cpon(r#"d"2026-11-14T12:00:00Z""#).unwrap().as_datetime().epoch_msec();
    {
        let snapshots = worker.state.snapshots.read().await;
        let SiteSnapshot::Ready(entries) = &snapshots["site"] else { panic!("Snapshot must be ready") };
        assert!(entries.values().all(|entry| entry.epoch_msec == snapshot_timestamp));
    }
    for table in [&worker.state.alarms, &worker.state.state_alarms] {
        let table = table.read().await;
        let alarms = &table["site"];
        assert_eq!(alarms.len(), 1);
        assert_eq!(alarms[0].alarm.path, "status");
        assert_eq!(alarms[0].timestamp.epoch_msec(), snapshot_timestamp);
        assert!(!alarms[0].stale);
    }

    worker.notification("status", "chng", "get", 0);
    worker.alarm_signals().await;
    assert!(worker.state.alarms.read().await["site"].is_empty());
    assert!(worker.state.state_alarms.read().await["site"].is_empty());
    let snapshots = worker.state.snapshots.read().await;
    let SiteSnapshot::Ready(entries) = &snapshots["site"] else { panic!("Snapshot must remain ready") };
    assert_eq!(entries[&("status".into(), "chng".into(), "get".into())].value, 0.into());
    drop(snapshots);

    worker.notification("status", "chng", "get", 1);
    worker.alarm_signals().await;
    worker.status(SiteOnlineStatus::Offline);
    worker.signal("onlinestatuschng", 1.into()).await;
    worker.signal("statealarmmod", RpcValue::null()).await;
    worker.signal("alarmmod", RpcValue::null()).await;
    assert!(matches!(worker.state.snapshots.read().await["site"], SiteSnapshot::Invalid));
    for table in [&worker.state.alarms, &worker.state.state_alarms] {
        let table = table.read().await;
        for alarm in &table["site"] {
            assert_eq!(alarm.stale, alarm.alarm.path != SITE_OFFLINE_ALARM_KEY);
        }
    }

    worker.status(SiteOnlineStatus::Online);
    worker.signal("onlinestatuschng", 2.into()).await;
    worker.signal("alarmmod", RpcValue::null()).await;
    let request = worker.snapshot_request().await;
    worker.entries(false, &[]).await;
    request.respond(shvproto::List::new().into());
    worker.alarm_signals().await;
    worker.entries(true, &[]).await;
    assert!(worker.state.alarms.read().await["site"].is_empty());
    assert!(worker.state.state_alarms.read().await["site"].is_empty());
    worker.close().await;
}

#[tokio::test]
async fn offline_cancels_snapshot_fetch_before_reconnect() {
    let mut worker = Worker::new().await;
    worker.notification("old", "chng", "get", 1);
    worker.signal("onlinestatuschng", 2.into()).await;
    let old_request = worker.snapshot_request().await;
    worker.status(SiteOnlineStatus::Offline);
    worker.signal("onlinestatuschng", 1.into()).await;
    // The offline alarm must exist even before the first snapshot initializes.
    worker.signal("alarmmod", RpcValue::null()).await;
    assert!(matches!(worker.state.snapshots.read().await["site"], SiteSnapshot::Invalid));
    let alarms = worker.state.alarms.read().await;
    assert_eq!(alarms["site"].len(), 1);
    assert_eq!(alarms["site"][0].alarm.path, SITE_OFFLINE_ALARM_KEY);
    assert!(!alarms["site"][0].stale);
    drop(alarms);

    worker.notification("new", "chng", "get", 2);
    worker.signal("onlinestatuschng", 2.into()).await;
    worker.signal("alarmmod", RpcValue::null()).await;
    let new_request = worker.snapshot_request().await;
    assert!(old_request.responses.is_closed());
    new_request.respond(shvproto::List::new().into());
    worker.entries(true, &[("new", "chng", "get", 2)]).await;
    worker.close().await;
}

#[tokio::test]
async fn disconnect_cancels_snapshot_fetch() {
    let mut worker = Worker::new().await;
    worker.status(SiteOnlineStatus::Online);
    worker.signal("onlinestatuschng", 2.into()).await;
    let request = worker.snapshot_request().await;
    worker.close().await;
    assert!(request.responses.is_closed());
}
