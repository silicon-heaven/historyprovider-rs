use shvclient::appnodes::DotAppNode;
use shvclient::AppState;
use shvrpc::client::ClientConfig;

mod sites;
mod tree;

pub struct HpConfig {
}

impl HpConfig {
    pub fn load(config_file: impl AsRef<str>) -> Result<Self, String> {
        Ok(HpConfig { })
    }
}

// pub(crate) trait Client {
//     fn send_message(message: RpcMessage);
//     async fn call_rpc_method<T, E>(
//         &self,
//         path: &str,
//         method: &str,
//         param: Option<RpcValue>,
//     ) -> Result<T, CallRpcMethodError>
//     where
//         T: TryFrom<RpcValue, Error = E>,
//         E: std::fmt::Display;
//     async fn subscribe(&self, ri: ShvRI) -> Result<Subscriber, CallRpcMethodError>;
// }

pub struct State {
    sites: sites::Sites,
}

pub(crate) type ClientCommandSender = shvclient::ClientCommandSender<State>;

pub async fn run(hp_config: &HpConfig, client_config: &ClientConfig) -> shvrpc::Result<()> {
    let app_state = AppState::new(State {
        sites: sites::Sites(Default::default()),
    });

    shvclient::Client::new()
        .mount_dynamic("",
            shvclient::MethodsGetter::new(tree::methods_getter),
            shvclient::RequestHandler::stateful(tree::request_handler)
        )
        .with_app_state(app_state.clone())
        .run_with_init(&client_config, |client_cmd_tx, client_evt_rx| {
            tokio::spawn(sites::load_sites(client_cmd_tx, client_evt_rx, app_state));
        })
        .await
}
