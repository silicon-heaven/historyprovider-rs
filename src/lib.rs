use futures::channel::mpsc::{unbounded, UnboundedSender};
use log::info;
use serde::Deserialize;
use shvclient::AppState;
use shvrpc::client::ClientConfig;
use tokio::sync::RwLock;

mod sites;
mod tree;
mod sync;

fn default_journal_dir() -> String {
    "/tmp/hp-rs/shvjournal".into()
}

#[derive(Clone, Deserialize)]
pub struct HpConfig {
    #[serde(default = "default_journal_dir")]
    journal_dir: String,
    max_sync_tasks: Option<usize>,
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
    sites_data: RwLock<sites::SitesData>,
    sync_info: sync::SyncInfo,
    config: HpConfig,
    sync_cmd_tx: UnboundedSender<sync::SyncCommand>,
}

pub(crate) type ClientCommandSender = shvclient::ClientCommandSender<State>;

pub async fn run(hp_config: &HpConfig, client_config: &ClientConfig) -> shvrpc::Result<()> {
    info!("Setting up journal dir: {}", &hp_config.journal_dir);
    std::fs::create_dir_all(&hp_config.journal_dir)?;
    info!("Journal dir path: {}", std::fs::canonicalize(&hp_config.journal_dir).expect("Invalid journal dir").to_string_lossy());

    let (sync_cmd_tx, sync_cmd_rx) = unbounded();

    let app_state = AppState::new(State {
        sites_data: RwLock::default(),
        sync_info: Default::default(),
        config: hp_config.clone(),
        sync_cmd_tx,
    });

    shvclient::Client::new()
        .mount_dynamic("",
            shvclient::MethodsGetter::new(tree::methods_getter),
            shvclient::RequestHandler::stateful(tree::request_handler)
        )
        .with_app_state(app_state.clone())
        .run_with_init(client_config, |client_cmd_tx, client_evt_rx| {
            tokio::spawn(sites::load_sites(client_cmd_tx.clone(), client_evt_rx.clone(), app_state.clone()));
            tokio::spawn(sync::sync_task(client_cmd_tx, client_evt_rx, app_state, sync_cmd_rx));
        })
        .await
}
