use std::collections::BTreeMap;
use std::sync::Arc;

use futures::channel::mpsc::{unbounded, UnboundedSender};
use log::info;
use serde::Deserialize;
use shvproto::RpcValue;
use shvrpc::client::ClientConfig;
use tokio::sync::RwLock;

use crate::alarm::Alarm;
use self::sites::SiteOnlineStatus;
use self::util::DedupSender;

mod getlog;
mod alarmlog;
mod pushlog;
mod sites;
mod tree;
mod sync;
mod cleanup;
mod journalentry;
mod journalrw;
mod dirtylog;
mod datachange;
mod util;
pub mod typeinfo;
pub mod alarm;

const fn max_sync_tasks_default() -> usize { 8 }
const fn max_journal_dir_size_default() -> usize { 30 * 1_000_000_000 } // 30 GB
const fn periodic_sync_interval_default() -> u64 { 60 * 60 } // 1 hour

fn journal_dir_default() -> String {
    "/tmp/hp-rs/shvjournal".into()
}

#[derive(Clone, Deserialize)]
pub struct HpConfig {
    #[serde(default = "journal_dir_default")]
    pub journal_dir: String,
    #[serde(default = "max_sync_tasks_default")]
    pub max_sync_tasks: usize,
    #[serde(default = "max_journal_dir_size_default")]
    pub max_journal_dir_size: usize,
    #[serde(default = "periodic_sync_interval_default")]
    pub periodic_sync_interval: u64,
    #[serde(default)]
    pub days_to_keep: i64,
}

impl Default for HpConfig {
    fn default() -> Self {
        Self {
            journal_dir: journal_dir_default(),
            max_sync_tasks: max_sync_tasks_default(),
            max_journal_dir_size: max_journal_dir_size_default(),
            periodic_sync_interval: periodic_sync_interval_default(),
            days_to_keep: Default::default(),
        }
    }
}

impl HpConfig {
    pub fn load(config_file: impl AsRef<std::path::Path>) -> Result<Self, String> {
        let config = std::fs::read_to_string(config_file)
            .map_err(|e| format!("Config file read error: {e}"))?;
        serde_yaml_ng::from_str(&config)
            .map_err(|e| format!("Config file format error: {e}"))
    }
}

#[derive(Clone)]
struct AlarmWithTimestamp {
    alarm: Alarm,
    timestamp: shvproto::DateTime,
    stale: bool,
}

impl From<AlarmWithTimestamp> for RpcValue {
    fn from(value: AlarmWithTimestamp) -> Self {
        let mut alarm_map = value.alarm.into_rpc_map(true);
        alarm_map.insert("timestamp".to_string(), value.timestamp.into());
        alarm_map.insert("stale".to_string(), value.stale.into());
        alarm_map.into()
    }
}

struct State {
    start_time: std::time::Instant,
    sites_data: RwLock<sites::SitesData>,
    sync_info: sync::SyncInfo,
    alarms: RwLock<BTreeMap<String, Vec<AlarmWithTimestamp>>>,
    state_alarms: RwLock<BTreeMap<String, Vec<AlarmWithTimestamp>>>,
    online_states: RwLock<BTreeMap<String, SiteOnlineStatus>>,
    config: HpConfig,
    sync_cmd_tx: DedupSender<sync::SyncCommand>,
    dirtylog_cmd_tx: UnboundedSender<dirtylog::DirtyLogCommand>,
}

pub async fn run(hp_config: &HpConfig, client_config: &ClientConfig) -> shvrpc::Result<()> {
    info!("Setting up journal dir: {}", &hp_config.journal_dir);
    std::fs::create_dir_all(&hp_config.journal_dir)?;
    info!("Journal dir path: {}", std::fs::canonicalize(&hp_config.journal_dir).expect("Invalid journal dir").to_string_lossy());

    let (sync_cmd_tx, sync_cmd_rx) = crate::util::dedup_channel();
    let (dirtylog_cmd_tx, dirtylog_cmd_rx) = unbounded();

    let app_state = Arc::new(State {
        start_time: std::time::Instant::now(),
        sites_data: RwLock::default(),
        sync_info: Default::default(),
        alarms: Default::default(),
        state_alarms: Default::default(),
        online_states: Default::default(),
        config: hp_config.clone(),
        sync_cmd_tx,
        dirtylog_cmd_tx,
    });

    shvclient::Client::new()
        .mount_dynamic("", {
            let app_state = app_state.clone();
            move |rq, cmd_sender|
                tree::request_handler(rq, cmd_sender, app_state.clone())
        })
        .run_with_init(client_config, |client_cmd_tx, client_evt_rx| {
            tokio::spawn(sites::sites_task(client_cmd_tx.clone(), client_evt_rx.clone(), app_state.clone()));
            tokio::spawn(sync::sync_task(client_cmd_tx.clone(), client_evt_rx.clone(), app_state.clone(), sync_cmd_rx));
            tokio::spawn(dirtylog::dirtylog_task(client_cmd_tx, client_evt_rx, app_state, dirtylog_cmd_rx));
        })
        .await
}
