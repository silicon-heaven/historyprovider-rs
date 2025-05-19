use log::info;
use serde::Deserialize;
use shvclient::AppState;
use shvrpc::client::ClientConfig;

mod sites;
mod tree;

fn default_journal_dir() -> String {
    "/tmp/hp-rs/shvjournal".into()
}

#[derive(Clone, Deserialize)]
pub struct HpConfig {
    #[serde(default = "default_journal_dir")]
    journal_dir: String,
}

impl HpConfig {
    pub fn load(config_file: impl AsRef<std::path::Path>) -> Result<Self, String> {
        let config = std::fs::read_to_string(config_file)
            .map_err(|e| format!("Config file read error: {}", e))?;
        serde_yaml_ng::from_str(&config)
            .map_err(|e| format!("Config file format error: {}", e))
    }
}

struct State {
    sites: sites::Sites,
    sub_hps: sites::SubHps,
    config: HpConfig,
}

pub(crate) type ClientCommandSender = shvclient::ClientCommandSender<State>;

pub async fn run(hp_config: &HpConfig, client_config: &ClientConfig) -> shvrpc::Result<()> {
    info!("Setting up journal dir: {}", &hp_config.journal_dir);
    std::fs::create_dir_all(&hp_config.journal_dir)?;
    info!("Journal dir path: {}", std::fs::canonicalize(&hp_config.journal_dir).expect("Invalid journal dir").to_string_lossy());

    let app_state = AppState::new(State {
        sites: sites::Sites(Default::default()),
        sub_hps: sites::SubHps(Default::default()),
        config: hp_config.clone(),
    });

    shvclient::Client::new()
        .mount_dynamic("",
            shvclient::MethodsGetter::new(tree::methods_getter),
            shvclient::RequestHandler::stateful(tree::request_handler)
        )
        .with_app_state(app_state.clone())
        .run_with_init(client_config, |client_cmd_tx, client_evt_rx| {
            tokio::spawn(sites::load_sites(client_cmd_tx, client_evt_rx, app_state));
        })
        .await
}
