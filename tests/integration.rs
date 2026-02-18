use std::{error::Error, marker::{Send, Sync}};

use futures::{StreamExt, channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded}};
use historyprovider::{AppTasks, HpConfig, make_client};
use historyprovider::init_logger;
use log::debug;
use shvclient::{ConnectionCommand, ConnectionEvent};
use shvproto::RpcValue;
use shvrpc::{RpcMessage, RpcMessageMetaTags};
use tokio::task::JoinHandle;

pub async fn mock_run(hp_config: &HpConfig, conn_evt_rx: futures::channel::mpsc::UnboundedReceiver<shvclient::ConnectionEvent>) -> shvrpc::Result<()> {
    let mut tasks = AppTasks::default();
    let (app_state, client, init_function) = make_client(hp_config, &mut tasks)?;
    client
        .mock_run_with_init(init_function, conn_evt_rx)
        .await?;

    tasks.wait_until_finished(app_state.as_ref()).await;
    Ok(())
}

async fn request(conn_evt_tx: &mut UnboundedSender<ConnectionEvent>, path: &str, method: &str, param: RpcValue) {
    conn_evt_tx.unbounded_send(ConnectionEvent::RpcFrameReceived(RpcMessage::new_request(path, method).with_param(param).to_frame().unwrap())).unwrap();
}

struct PendingRequest(RpcMessage);
impl PendingRequest {
    async fn respond(mut self, conn_evt_tx: &mut UnboundedSender<ConnectionEvent>, result: &str) {
        let result = RpcValue::from_cpon(result).unwrap_or_else(|err| panic!("Invalid CPON '{result}': {err}"));
        debug!(target: "test-driver", "==> {result:?}");
        self.0.set_result(result);
        conn_evt_tx.unbounded_send(ConnectionEvent::RpcFrameReceived(self.0.to_frame().expect("to_frame() must work"))).expect("sending ConnectionEvent must work");
    }
}

async fn await_request(conn_cmd_rx: &mut UnboundedReceiver<ConnectionCommand>, expected_path: &str, expected_method: &str, expected_param: RpcValue) -> PendingRequest {
    let Some(command) = conn_cmd_rx.next().await else {
        panic!("Expected a {expected_path}:{expected_method} - {expected_param}, but no call accepted");
    };

    match command {
        ConnectionCommand::SendMessage(rpc_message) => {
            let shv_path = rpc_message.shv_path().expect("msg must have a path");
            let method = rpc_message.method().expect("msg must have a method");
            let param = rpc_message.param().cloned().unwrap_or_else(RpcValue::null);
            debug!(target: "test-driver", "<== {shv_path}:{method}, param: {param}");
            assert_eq!(shv_path, expected_path);
            assert_eq!(method, expected_method);
            assert_eq!(param, expected_param);

            PendingRequest(rpc_message.prepare_response().expect("prepare_response must work"))
        },
    }
}

struct TestApp {
    app: JoinHandle<Result<(), Box<dyn Error + Send + Sync>>>,
    conn_evt_tx: UnboundedSender<ConnectionEvent>,
    conn_cmd_rx: UnboundedReceiver<ConnectionCommand>,
}

impl TestApp {
    fn new() -> Self {
        let (conn_evt_tx, conn_evt_rx) = futures::channel::mpsc::unbounded::<ConnectionEvent>();
        let config = Default::default();
        let app = tokio::spawn(async move {
            mock_run(&config, conn_evt_rx).await
        });

        let (conn_cmd_tx, conn_cmd_rx) = unbounded();
        conn_evt_tx.unbounded_send(ConnectionEvent::Connected(conn_cmd_tx)).expect("Events must work");
        Self {
            app,
            conn_evt_tx,
            conn_cmd_rx,
        }
    }
}

#[tokio::test]
async fn test_start_and_end() -> shvrpc::Result<()> {
    init_logger();
    let TestApp {app, mut conn_evt_tx, mut conn_cmd_rx} = TestApp::new();
    await_request(&mut conn_cmd_rx, ".broker", "ls", RpcValue::null()).await
        .respond(&mut conn_evt_tx, r#"["client"]"#).await;
    await_request(&mut conn_cmd_rx, "sites", "getSites", RpcValue::null()).await
        .respond(&mut conn_evt_tx, r#"{
            "_meta":{
                "HP3":{"type":"HP3"},
            }
        },"#).await;

    request(&mut conn_evt_tx, "", "syncLog", RpcValue::null()).await;
    conn_evt_tx.unbounded_send(ConnectionEvent::Disconnected)?;
    drop(conn_evt_tx);
    app.await?
}
