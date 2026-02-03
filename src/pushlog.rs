use std::path::Path;
use std::sync::Arc;

use futures::io::{BufReader, BufWriter};
use futures::StreamExt as _;
use log::{error, info, warn};
use shvproto::RpcValue;
use shvrpc::journalrw::{JournalReaderLog2, JournalWriterLog2, Log2Header, Log2Reader};
use tokio_util::compat::TokioAsyncReadCompatExt as _;

use crate::util::{get_files, is_log2_file};
use crate::State;

pub(crate) struct PushLogResult {
    since: Option<shvproto::DateTime>,
    until: Option<shvproto::DateTime>,
    msg: String,
}

impl From<PushLogResult> for RpcValue {
    fn from(value: PushLogResult) -> Self {
        shvproto::make_map!(
            "since" => value.since.map_or_else(RpcValue::null, RpcValue::from),
            "until" => value.until.map_or_else(RpcValue::null, RpcValue::from),
            "msg" => value.msg,
        ).into()
    }
}

pub(crate) async fn pushlog_impl(
    log_reader: Log2Reader,
    site_path: &str,
    app_state: Arc<State>,
) -> PushLogResult
{
    info!("pushLog handler, site: {site_path}, log header: {header}", header = log_reader.header());

    let Log2Header {since, until, ..} = log_reader.header().clone();
    let local_journal_path = Path::new(&app_state.config.journal_dir).join(site_path);

    let local_latest_entries = {
        let log_files = get_files(&local_journal_path, is_log2_file)
            .await
            .map(|mut files| { files.sort_by_key(|entry| std::cmp::Reverse(entry.file_name())); files })
            .inspect_err(|err| {error!("Cannot read journal dir entries: {err}"); })
            .unwrap_or_default();

        Box::pin(futures::stream::iter(log_files)
            .map(|file_entry| file_entry.path())
            .then(|file_path|
                async move {
                    match tokio::fs::File::open(&file_path).await {
                        Ok(file) => {
                            let reader = JournalReaderLog2::new(BufReader::new(file.compat()));
                            let (_, latest_entries) = reader
                                .filter_map(async |res| res.ok())
                                .fold((None, Vec::new()), async |(latest_ts, mut latest_entries), entry| {
                                    match latest_ts {
                                        None => (Some(entry.epoch_msec), vec![entry]),
                                        Some(ts) if entry.epoch_msec > ts =>
                                            (Some(entry.epoch_msec), vec![entry]),
                                        Some(ts) if entry.epoch_msec == ts => {
                                            latest_entries.push(entry);
                                            (Some(ts), latest_entries)
                                        }
                                        Some(ts) => (Some(ts), latest_entries),
                                    }
                                })
                                .await;
                            (!latest_entries.is_empty()).then_some(latest_entries)
                        }
                        Err(err) => {
                            error!("Cannot open file {file_path} while getting the last journal entries in pushLog handler: {err}",
                                file_path = file_path.to_string_lossy()
                            );
                            None
                        }
                    }
                })
            .filter_map(async |latest_entries| latest_entries))
            .next()
            .await
            .unwrap_or_default()
    };
    let local_latest_entry_msec = local_latest_entries.first().map_or(0, |entry| entry.epoch_msec);

    let journal_file_path = local_journal_path.join(since.to_chrono_datetime().format("%Y-%m-%dT%H-%M-%S-%3f.log2").to_string());
    let journal_file = match tokio::fs::OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(&journal_file_path)
        .await {
            Ok(file) => file,
            Err(err) => {
                error!("Cannot open journal file {} for writing: {}", journal_file_path.to_string_lossy(), err);
                // Return Ok to the device assuming that the devices are not smart enough to handle
                // the error in this case. It is not their fault that we cannot create the file.
                // FIXME: if there is a better approach
                return PushLogResult {
                    since: Some(since),
                    until: Some(until),
                    msg: "success".into(),
                }
            }
        };

    let mut remote_entries_count = 0;
    let mut log_reader_stream = futures::stream::iter(log_reader
        .into_iter()
        .filter_map(|entry_res| {
            let entry = entry_res
                .inspect_err(|err| warn!("Ignoring invalid pushLog entry: {err}"))
                .ok()?;

            remote_entries_count += 1;

            if entry.epoch_msec < local_latest_entry_msec {
                warn!("Ignoring pushLog entry for: {entry_path} with timestamp: {entry_msec}, because a newer one exists with ts: {local_latest_entry_msec}",
                    entry_path = entry.path,
                    entry_msec = entry.epoch_msec,
                );
                return None;
            }
            if entry.epoch_msec == local_latest_entry_msec && local_latest_entries.iter().any(|latest_entry| latest_entry.path == entry.path) {
                warn!("Ignoring pushLog entry for: {entry_path} with timestamp: {entry_msec}, because we already have an entry with this timestamp and path",
                    entry_path = entry.path,
                    entry_msec = entry.epoch_msec,
                );
                return None;
            }
            Some(entry)
        }));

    let mut writer = JournalWriterLog2::new(BufWriter::new(journal_file.compat()));
    let mut written_entries_count = 0;
    while let Some(entry) = log_reader_stream.next().await {
        if let Err(err) = writer.append(&entry).await {
            warn!("Cannot append to journal file {path}: {err}", path = journal_file_path.to_string_lossy());
            break;
        }
        written_entries_count += 1;
    }

    if written_entries_count == 0 {
        tokio::fs::remove_file(&journal_file_path)
            .await
            .unwrap_or_else(|err|
                error!("Cannot remove empty journal file {path}: {err}", path = journal_file_path.to_string_lossy())
            );
    }

    let (since, until) = if remote_entries_count == 0 {
        (None, None)
    } else {
        (Some(since), Some(until))
    };

    info!("pushLog for site: {site_path} done, got {remote_entries_count}, rejected {rejected_count} entries",
        rejected_count = remote_entries_count - written_entries_count
    );

    PushLogResult { since, until, msg: "success".into() }
}
