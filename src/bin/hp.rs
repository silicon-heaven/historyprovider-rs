use clap::Parser;
use historyprovider::HpConfig;
use log::*;
use shvrpc::{client::ClientConfig, util::parse_log_verbosity};
use simple_logger::SimpleLogger;
use url::Url;

#[derive(Parser, Debug)]
//#[structopt(name = "device", version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"), about = "SHV call")]
struct Opts {
    /// Config file path
    #[arg(long)]
    config: Option<String>,
    /// Create default config file if one specified by --config is not found
    #[arg(short, long)]
    create_default_config: bool,
    ///Url to connect to, example tcp://admin@localhost:3755?password=dj4j5HHb, localsocket:path/to/socket
    #[arg(short = 's', long)]
    url: Option<String>,
    #[arg(short = 'i', long)]
    device_id: Option<String>,
    /// Mount point on broker connected to, note that broker might not accept any path.
    #[arg(short, long)]
    mount: Option<String>,
    /// Device tries to reconnect to broker after this interval, if connection to broker is lost.
    /// Example values: 1s, 1h, etc.
    #[arg(short, long)]
    reconnect_interval: Option<String>,
    /// Client should ping broker with this interval. Broker will disconnect device, if ping is not received twice.
    /// Example values: 1s, 1h, etc.
    #[arg(long)]
    heartbeat_interval: Option<String>,
    /// Verbose mode (module, .)
    #[arg(short, long)]
    verbose: Option<String>,
    /// Print version and exit
    #[arg(short = 'V', long)]
    version: bool,

    // HP specific options
    //
    /// Path to journal directory
    #[arg(long)]
    journal_dir: Option<String>,
    /// Maximum number of parallel sync tasks
    #[arg(long)]
    max_sync_tasks: Option<usize>,
    /// Maximum size of journal directory in bytes
    #[arg(long)]
    max_journal_dir_size: Option<usize>,
    /// Periodic sync interval in seconds
    #[arg(long)]
    periodic_sync_interval: Option<u64>,
    /// Number of days to keep history data
    #[arg(long)]
    days_to_keep: Option<i64>,
}

fn init_logger(cli_opts: &Opts) {
    let mut logger = SimpleLogger::new();
    logger = logger.with_level(LevelFilter::Info);
    if let Some(module_names) = &cli_opts.verbose {
        for (module, level) in parse_log_verbosity(module_names, module_path!()) {
            if let Some(module) = module {
                logger = logger.with_module_level(module, level);
            } else {
                logger = logger.with_level(level);
            }
        }
    }
    logger.init().unwrap();
}

fn print_banner(text: impl AsRef<str>) {
    let text = text.as_ref();
    let width = text.len() + 2;
    let banner_line = format!("{:=^width$}", "");
    info!("{banner_line}");
    info!("{text:^width$}");
    info!("{banner_line}");
}

fn load_client_config(cli_opts: Opts) -> shvrpc::Result<(ClientConfig, HpConfig)> {
    let (mut client_config, mut hp_config) = if let Some(config_file) = &cli_opts.config {
        (
            ClientConfig::from_file_or_default(config_file, cli_opts.create_default_config)?,
            HpConfig::load(config_file)?,
        )
    } else {
        (Default::default(), Default::default())
    };
    client_config.url = cli_opts.url.map_or_else(|| Ok(client_config.url), |url_str| Url::parse(&url_str))?;
    client_config.device_id = cli_opts.device_id.or(client_config.device_id);
    client_config.mount = cli_opts.mount.or(client_config.mount);
    client_config.reconnect_interval = cli_opts.reconnect_interval.map_or_else(
        || Ok(client_config.reconnect_interval),
        |interval_str| duration_str::parse(&interval_str).map(Some)
    )?;
    client_config.heartbeat_interval = cli_opts.heartbeat_interval.map_or_else(
        || Ok(client_config.heartbeat_interval),
        |interval_str| duration_str::parse(&interval_str)
    )?;

    hp_config.journal_dir = cli_opts.journal_dir.unwrap_or(hp_config.journal_dir);
    hp_config.max_sync_tasks = cli_opts.max_sync_tasks.or(hp_config.max_sync_tasks);
    hp_config.max_journal_dir_size = cli_opts.max_journal_dir_size.or(hp_config.max_journal_dir_size);
    hp_config.periodic_sync_interval = cli_opts.periodic_sync_interval.or(hp_config.periodic_sync_interval);
    hp_config.days_to_keep = cli_opts.days_to_keep.or(hp_config.days_to_keep);

    Ok((client_config, hp_config))
}

#[tokio::main]
pub(crate) async fn main() -> shvrpc::Result<()> {
    static PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
    let cli_opts = Opts::parse();

    if cli_opts.version {
        println!("{PKG_VERSION}");
        return Ok(());
    }

    init_logger(&cli_opts);
    print_banner(format!("{} {} starting", std::module_path!(), PKG_VERSION));
    let (client_config, hp_config) = load_client_config(cli_opts).expect("Invalid config");

    historyprovider::run(&hp_config, &client_config).await
}
