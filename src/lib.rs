use shvclient::appnodes::DotAppNode;
use shvclient::AppState;
use shvrpc::client::ClientConfig;

mod sites;

pub struct HpConfig {
}

impl HpConfig {
    pub fn load(config_file: impl AsRef<str>) -> Result<Self, String> {
        Ok(HpConfig { })
    }
}

pub struct State {
    sites: sites::Sites,
}

pub async fn run(hp_config: &HpConfig, client_config: &ClientConfig) -> shvrpc::Result<()> {
    let app_state = AppState::new(State {
        sites: sites::Sites(Default::default()),
    });

    shvclient::Client::new(DotAppNode::new("historyprovider-rs"))
        .with_app_state(app_state.clone())
        .run_with_init(&client_config, |client_cmd_tx, client_evt_rx| {
            tokio::spawn(sites::load_sites(client_cmd_tx, client_evt_rx, app_state));
        })
        .await
}
