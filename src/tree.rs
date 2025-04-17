use shvclient::client::MetaMethods;
use shvclient::clientnode::{children_on_path, ConstantNode};
use shvclient::AppState;
use shvrpc::metamethod::MetaMethod;
use shvrpc::rpcmessage::{RpcError, RpcErrorCode};
use shvrpc::{RpcMessage, RpcMessageMetaTags};

use crate::{ClientCommandSender, State};

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

// root node methods:
//
// appName
// deviceId
// deviceType
// version
// gitCommit
// shvVersion
// shvGitCommit
// uptime
// reloadSites (wr) -> Bool

const METH_UPTIME: &str = "uptime";
const METH_RELOAD_SITES: &str = "reloadSites";

const META_METHODS_ROOT_NODE: &[&MetaMethod] = &[
    &MetaMethod {
        name: METH_UPTIME,
        flags: 0,
        access: shvrpc::metamethod::AccessLevel::Read,
        param: "Null",
        result: "String",
        signals: &[],
        description: "",
    },
    &MetaMethod {
        name: METH_RELOAD_SITES,
        flags: 0,
        access: shvrpc::metamethod::AccessLevel::Write,
        param: "Null",
        result: "Bool",
        signals: &[],
        description: "",
    },
];

static DOT_APP_NODE: std::sync::LazyLock<shvclient::appnodes::DotAppNode> = std::sync::LazyLock::new(||
    shvclient::appnodes::DotAppNode::new("historyprovider-rs")
);

pub(crate) async fn methods_getter(path: String, app_state: Option<AppState<State>>) -> Option<MetaMethods> {
    let app_state = app_state.expect("AppState is Some");
    if path.as_str() == ".app" {
        return Some(MetaMethods::from(shvclient::appnodes::DOT_APP_METHODS));
    }
    let children = children_on_path(&*app_state.sites.0.read().await, path.clone())?;
    // root node
    if path.is_empty() {
        return Some(MetaMethods::from(META_METHODS_ROOT_NODE));
    }
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
            {
                resp.set_result($resp_arg);
                client_cmd_tx.send_message(resp).unwrap();
                return;
            }
        }
    }

    let path = rq.shv_path().unwrap_or_default();
    if path == ".app" {
        if let Some(shvclient::clientnode::METH_LS) = rq.method() {
            send_response_and_return!(shvproto::List::new());
        }
        let res = &DOT_APP_NODE.process_request(&rq).unwrap();
        resp.set_result_or_error(res.clone());
        client_cmd_tx.send_message(resp).unwrap();
        return;
    }

    if path == "_shvjournal" {
        if let Some(shvclient::clientnode::METH_LS) = rq.method() {
            send_response_and_return!(());
        }
        let res = &DOT_APP_NODE.process_request(&rq).unwrap();
        resp.set_result_or_error(res.clone());
        client_cmd_tx.send_message(resp).unwrap();
        return;
    }

    if path == "_valuecache" {
        if let Some(shvclient::clientnode::METH_LS) = rq.method() {
            send_response_and_return!(());
        }
        let res = &DOT_APP_NODE.process_request(&rq).unwrap();
        resp.set_result_or_error(res.clone());
        client_cmd_tx.send_message(resp).unwrap();
        return;
    }

    if path.is_empty() {
        match rq.method() {
            Some(shvclient::clientnode::METH_LS) => {
                let mut nodes = vec![
                    ".app".to_string(),
                    "_shvjournal".to_string(),
                    "_valuecache".to_string()
                ];
                nodes.append(&mut children_on_path(&*app_state.sites.0.read().await, path).unwrap_or_default());
                send_response_and_return!(nodes)
            }
            _ => send_response_and_return!("Not implemented"),
        }
    }

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

