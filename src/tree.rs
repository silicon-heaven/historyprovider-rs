use shvclient::client::MetaMethods;
use shvclient::clientnode::children_on_path;
use shvclient::{AppState, ClientCommandSender};
use shvrpc::metamethod::MetaMethod;
use shvrpc::rpcmessage::{RpcError, RpcErrorCode};
use shvrpc::{RpcMessage, RpcMessageMetaTags};

use crate::State;

const METH_GET_LOG: &str = "getLog";

const META_METHOD_GET_LOG: MetaMethod = MetaMethod {
    name: METH_GET_LOG,
    flags: 0,
    access: shvrpc::metamethod::AccessLevel::Read,
    param: "RpcValue",
    result: "RpcValue",
    signals: &[],
    description: "",
};

pub(crate) async fn methods_getter(path: String, app_state: Option<AppState<State>>) -> Option<MetaMethods> {
    let app_state = app_state.expect("AppState is Some");
    let children = children_on_path(&*app_state.sites.0.read().await, path)?;
    if children.is_empty() {
        // `path` is a site path
        Some(MetaMethods::from(&[&META_METHOD_GET_LOG]))
    } else {
        // `path` is a dir in the middle of the tree
        Some(MetaMethods::from(&[]))
    }
    // TODO: put the processed data to a request cache in the app state: path -> children
}

pub(crate) async fn request_handler(rq: RpcMessage, client_cmd_tx: ClientCommandSender, app_state: Option<AppState<State>>) {
    let app_state = app_state.expect("AppState is Some");
    let mut resp = rq.prepare_response().unwrap();
    macro_rules! send_response_and_return {
        ($resp_arg:expr) => {
            resp.set_result($resp_arg);
            client_cmd_tx.send_message(resp).unwrap();
            return;
        }
    }
    let path = rq.shv_path().unwrap_or_default();
    let children = children_on_path(&*app_state.sites.0.read().await, path)
        .unwrap_or_else(|| panic!("Children on path `{path}` should be Some after methods processing"));

    if let Some(shvclient::clientnode::METH_LS) = rq.method() {
        send_response_and_return!(children);
    }

    if children.is_empty() {
        // Handle methods for a site path
        match rq.method() {
            Some(self::METH_GET_LOG) => {
                send_response_and_return!(vec!["getLog: TODO"]);
            },
            _ => {}
        }
    }

    resp.set_error(RpcError::new(
            RpcErrorCode::MethodNotFound,
            format!("Unknown method '{:?}'", rq.method())));
    client_cmd_tx.send_message(resp).unwrap();
}

