use std::collections::BTreeMap;
use std::error::Error;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

use chrono::TimeZone;
use futures::io::BufReader;
use futures::{Stream, StreamExt, TryStreamExt};
use log::{error, info, warn};
use shvrpc::rpcmessage::{RpcError, RpcErrorCode};
use tokio::fs::DirEntry;
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::dirtylog::DirtyLogCommand;
use crate::journalentry::JournalEntry;
use crate::journalrw::{matches_path_pattern, GetLog2Params, GetLog2Since, JournalReaderLog2};
use crate::util::{get_files, is_log2_file};
use crate::State;

pub(crate) struct GetLogResult {
    pub(crate) record_count: i64,
    pub(crate) record_count_limit: i64,
    pub(crate) record_count_limit_hit: bool,
    pub(crate) date_time: shvproto::DateTime,
    pub(crate) since: shvproto::DateTime,
    pub(crate) until: shvproto::DateTime,
    pub(crate) with_paths_dict: bool,
    pub(crate) with_snapshot: bool,
    pub(crate) snapshot_entries: Vec<Arc<JournalEntry>>,
    pub(crate) event_entries: Vec<Arc<JournalEntry>>,
}

fn file_name_to_file_msec(filename: &str) -> Result<i64, String> {
    let without_ext = filename
        .strip_suffix(".log2")
        .ok_or_else(|| format!("Invalid file extension in '{filename}'"))?;

    let datetime = chrono::NaiveDateTime::parse_from_str(without_ext, "%Y-%m-%dT%H-%M-%S-%3f")
        .map_err(|e| format!("Failed to parse '{filename}': {e}"))?;

    Ok(chrono::Utc.from_utc_datetime(&datetime).timestamp_millis())
}


pub(crate) async fn getlog_handler(
    site_path: &str,
    params: &GetLog2Params,
    app_state: Arc<State>,
) -> Result<GetLogResult, RpcError> {
    if !app_state.sites_data.read().await.sites_info.contains_key(site_path) {
        return Err(RpcError::new(RpcErrorCode::InvalidParam, format!("Wrong getLog path: {site_path}")));
    }
    let local_journal_path = Path::new(&app_state.config.journal_dir).join(site_path);
    info!("getLog handler, site: {site_path}, params: {params}");
    let mut log_files = get_files(&local_journal_path, is_log2_file)
        .await
        .map_err(|err| RpcError::new(RpcErrorCode::InternalError, format!("Cannot read log files: {err}")))?;
    log_files.sort_by_key(|entry| entry.file_name());

    // Skip files with no journal entries
    let log_files = tokio_stream::iter(log_files)
        .filter(|file_entry| {
            let file_path = file_entry.path();
            async move {
                match tokio::fs::File::open(&file_path).await {
                    Ok(file) => {
                        JournalReaderLog2::new(BufReader::new(file.compat()))
                            .next()
                            .await
                            .is_some()
                    }
                    Err(err) => {
                        error!("Cannot open file {file_path} in call to getLog: {err}",
                            file_path = file_path.to_string_lossy()
                        );
                        false
                    }
                }
            }
        })
        .collect::<Vec<_>>()
        .await;

    let file_start_index = {
        if log_files.is_empty() {
            0
        } else {
            match &params.since {
                GetLog2Since::DateTime(date_time) => {
                    let since_ms = date_time.epoch_msec();

                    log_files
                        .iter()
                        .map(DirEntry::file_name)
                        .enumerate()
                        .rev()
                        .find(|(_,file)| file_name_to_file_msec(&file.to_string_lossy()).is_ok_and(|ms| ms < since_ms))
                        .map(|(idx, _)| idx)
                        .unwrap_or(0)
                }
                GetLog2Since::LastEntry => log_files.len() - 1,
                GetLog2Since::None => 0,
            }
        }
    };

    let file_readers = tokio_stream::iter(&log_files[file_start_index..])
        .then(|file_entry| async {
            let file_path = file_entry.path();
            tokio::fs::File::open(&file_path)
                .await
                .map_err(|err| format!("Cannot open file {file_path} in call to getLog: {err}", file_path = file_path.to_string_lossy()))
                .map(|file| JournalReaderLog2::new(BufReader::new(file.compat())))
        })
        .try_collect::<Vec<_>>()
        .await
        .map_err(|err| RpcError::new(RpcErrorCode::InternalError, err))?;


    let (dirtylog_tx, dirtylog_rx) = futures::channel::oneshot::channel();
    app_state.dirtylog_cmd_tx
        .unbounded_send(DirtyLogCommand::Get {
            site: site_path.into(),
            response_tx: dirtylog_tx,
        })
        .map_err(|err| RpcError::new(RpcErrorCode::InternalError, format!("Cannot get dirtylog: {err}")))?;
    let dirtylog = dirtylog_rx
        .await
        .map_err(|err| RpcError::new(RpcErrorCode::InternalError, format!("Cannot get dirtylog: {err}")))?;

    Ok(getlog_impl(file_readers.into_iter().map(|s| Box::pin(s) as _), dirtylog, params).await)
}

pub(crate) type JournalEntryStream = Pin<Box<dyn Stream<Item = Result<JournalEntry, Box<dyn Error + Send + Sync>>> + Send + Sync>>;

pub(crate) async fn getlog_impl(
    journal_readers: impl IntoIterator<Item = JournalEntryStream>,
    dirty_log: impl IntoIterator<Item = JournalEntry>,
    params: &GetLog2Params
) -> GetLogResult
{
    const RECORD_COUNT_LIMIT_MAX: i64 = 100_000;

    enum Since {
        Msec(i64),
        Last,
    }

    fn is_snapshot_entry(entry: &JournalEntry, since: &Since) -> bool {
        match since {
            Since::Msec(since_msec) => entry.epoch_msec <= *since_msec,
            Since::Last => true,
        }
    }

    struct Context<'a> {
        params: &'a GetLog2Params,
        since: Option<Since>,
        snapshot: BTreeMap<String, Arc<JournalEntry>>,
        entries: Vec<Arc<JournalEntry>>,
        record_count: usize,
        record_count_limit: usize,
        record_count_limit_hit: bool,
        last_entry: Option<Arc<JournalEntry>>,
        first_unmatched_entry_msec: Option<i64>,
        until_ms: i64,
    }

    fn process_journal_entry(
        entry: JournalEntry,
        context: &mut Context,
    ) -> bool {
        if context
            .params
            .path_pattern
                .as_ref()
                .is_some_and(|p| !matches_path_pattern(&entry.path, p))
        {
            return true;
        }

        let entry = Arc::new(entry);

        let since_val = context.since.get_or_insert_with(|| determine_since(&entry, &context.params.since));
        let is_snapshot_entry = is_snapshot_entry(&entry, since_val);

        if is_snapshot_entry {
            context.snapshot.insert(entry.path.clone(), Arc::clone(&entry));
        } else if entry.epoch_msec >= context.until_ms {
            context.first_unmatched_entry_msec = Some(entry.epoch_msec);
            return false;
        }

        if !is_snapshot_entry {
            let total = context.snapshot.len() + context.record_count + 1;
            if total > context.record_count_limit {
                context.record_count_limit_hit = true;

                // Record all consecutive entries with the same timestamp
                if context.last_entry.as_ref().is_some_and(|last_entry| last_entry.epoch_msec != entry.epoch_msec) {
                    context.first_unmatched_entry_msec = Some(entry.epoch_msec);
                    return false;
                }
            }

            context.entries.push(Arc::clone(&entry));
            context.record_count += 1;
        }

        context.last_entry = Some(entry);
        true
    }


    fn determine_since(entry: &JournalEntry, params_since: &GetLog2Since) -> Since {
        match params_since {
            GetLog2Since::DateTime(dt) => Since::Msec(dt.epoch_msec().max(entry.epoch_msec)),
            GetLog2Since::LastEntry => Since::Last,
            GetLog2Since::None => Since::Msec(entry.epoch_msec),
        }
    }

    let mut context = Context {
        params,
        since: None,
        snapshot: BTreeMap::new(),
        entries: Vec::new(),
        record_count: 0,
        record_count_limit: params.record_count_limit.clamp(0, RECORD_COUNT_LIMIT_MAX) as _,
        record_count_limit_hit: false,
        last_entry: None,
        first_unmatched_entry_msec: None,
        until_ms: params.until.as_ref().map_or(i64::MAX, shvproto::DateTime::epoch_msec),
    };

    'outer: for mut reader in journal_readers {
        while let Some(entry_res) = reader.next().await {
            match entry_res {
                Ok(entry) => {
                    if !process_journal_entry(entry, &mut context) {
                        break 'outer;
                    }
                }
                Err(err) => error!("Skipping corrupted journal entry: {err}"),
            }
        }
    }

    // Only append entries from the dirtylog if they are adjacent to the journal entries or else
    // the resulting `until` will be incorrectly taken from the first dirtylog entry.
    if !context.record_count_limit_hit {
        for entry in dirty_log {
            // Filter out entries that overlap with the entries from the synced files
            if context.last_entry.as_ref().is_some_and(|last_entry| entry.epoch_msec < last_entry.epoch_msec) {
                continue;
            }

            if !process_journal_entry(entry, &mut context) {
                break;
            }
        }
    }

    let since_ms = match context.since {
        Some(Since::Msec(msec)) => msec,
        Some(Since::Last) => context.last_entry.map_or(0, |entry| entry.epoch_msec),
        None => 0, // No entries
    };

    let with_snapshot = if matches!(params.since, GetLog2Since::None) && params.with_snapshot {
        warn!("Requested snapshot without a valid `since`. No snapshot will be provided.");
        // FIXME: we can provide a snapshot at the first entry datetime
        false
    } else {
        params.with_snapshot
    };

    let snapshot_entries = if with_snapshot {
        context.snapshot
            .into_values()
            .map(|entry| Arc::new(JournalEntry {
                epoch_msec: since_ms,
                repeat: true,
                ..(*entry).clone()
            }))
            .collect::<Vec<_>>()
    } else {
        // Include all entries from the snapshot if their timestamp >= since
        context.snapshot
            .into_values()
            .filter(|entry| entry.epoch_msec >= since_ms)
            .collect::<Vec<_>>()
    };

    let current_datetime = shvproto::DateTime::now();
    let until_ms = match context.first_unmatched_entry_msec {
        Some(msec) => msec, // set `until` to the first entry that is not included in the log
        None =>
            if let Some(last_entry_msec) = context.entries.last().map(|entry| entry.epoch_msec) {
                // There are other entries than just the snapshot
                if matches!(params.since, GetLog2Since::LastEntry) {
                    // The last entry is explicitly requested, return all we have.
                    // Set `until` to the timestamp of the last entry to indicate that
                    // there might be more data with this timestamp on the next call.
                    last_entry_msec
                } else if last_entry_msec.abs_diff(current_datetime.epoch_msec()) < 1000 {
                    // If the latest entries with the same timestmao are very recent,
                    // remove them same timestamp from the end of the log
                    while context.entries.last().is_some_and(|entry| entry.epoch_msec == last_entry_msec) {
                        context.entries.pop();
                    }
                    // result_entries.retain(|e| e.epoch_msec != last_entry_msec);
                    last_entry_msec
                } else {
                    // Keep the last entries, set `until` just after the last entry timestamp
                    last_entry_msec + 1
                }
            } else {
                // Empty result or only the snapshot, set `until` equal to `since`
                since_ms
            }
    };

    let record_count = snapshot_entries.len() as i64 + context.entries.len() as i64;

    GetLogResult {
        record_count,
        snapshot_entries,
        event_entries: context.entries.into_iter().collect(),
        record_count_limit: context.record_count_limit as _,
        record_count_limit_hit: context.record_count_limit_hit,
        date_time: current_datetime,
        since: shvproto::DateTime::from_epoch_msec(since_ms),
        until: shvproto::DateTime::from_epoch_msec(until_ms),
        with_paths_dict: params.with_paths_dict,
        with_snapshot,
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use shvproto::{DateTime, RpcValue};

    use crate::{getlog::{getlog_impl, GetLogResult, JournalEntryStream}, journalentry::JournalEntry, journalrw::{GetLog2Params, GetLog2Since}};


    fn ts(ts_str: &str) -> shvproto::DateTime {
        DateTime::from_iso_str(ts_str).unwrap()
    }

    fn since(ts_str: &str)-> crate::journalrw::GetLog2Since {
        crate::journalrw::GetLog2Since::DateTime(ts(ts_str))
    }

    fn make_entry(timestamp: &str, path: &str, value: impl Into<RpcValue>) -> Result<JournalEntry, Box<dyn Error + Send + Sync>> {
        Ok(JournalEntry {
            path: path.to_string(),
            epoch_msec: DateTime::from_iso_str(timestamp).unwrap().epoch_msec(),
            signal: "chng".to_string(),
            short_time: -1,
            access_level: 32,
            source: "".to_string(),
            value: value.into(),
            user_id: None,
            repeat: false,
            provisional: false,
        })
    }

    fn create_reader(entries: Vec<Result<JournalEntry, Box<dyn Error + Send + Sync + 'static>>>) -> JournalEntryStream {
        Box::pin(tokio_stream::iter(entries))
    }

    async fn get_log_entries(readers: Vec<JournalEntryStream>, params: GetLog2Params) -> GetLogResult {
        getlog_impl(readers, [], &params).await
    }

    #[tokio::test]
    async fn getlog() {
        fn data_1() -> Vec<JournalEntryStream> {
            vec![
                create_reader(vec![
                    make_entry("2022-07-07T18:06:15.557Z", "APP_START", true),
                    make_entry("2022-07-07T18:06:15.557Z", "zone1/system/sig/plcDisconnected", false),
                    make_entry("2022-07-07T18:06:15.557Z", "zone1/zone/Zone1/plcDisconnected", false),
                    make_entry("2022-07-07T18:06:15.557Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32),

                    make_entry("2022-07-07T18:06:17.784Z", "zone1/system/sig/plcDisconnected", false),
                    make_entry("2022-07-07T18:06:17.784Z", "zone1/zone/Zone1/plcDisconnected", false),
                    make_entry("2022-07-07T18:06:17.869Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32),
                ]),
                create_reader(vec![
                    make_entry("2022-07-07T18:06:17.872Z", "APP_START", true),
                    make_entry("2022-07-07T18:06:17.872Z", "zone1/system/sig/plcDisconnected", false),
                    make_entry("2022-07-07T18:06:17.872Z", "zone1/zone/Zone1/plcDisconnected", false),
                    make_entry("2022-07-07T18:06:17.872Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32),

                    make_entry("2022-07-07T18:06:17.873Z", "zone1/system/sig/plcDisconnected", false),
                    make_entry("2022-07-07T18:06:17.874Z", "zone1/zone/Zone1/plcDisconnected", false),
                    make_entry("2022-07-07T18:06:17.880Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32),
                ])
            ]
        }

        fn data_2() -> Vec<JournalEntryStream> {
            vec![
                create_reader(vec![
                    make_entry("2022-07-07T18:06:17.784Z", "value1", 0),
                    make_entry("2022-07-07T18:06:17.784Z", "value2", 1),
                    make_entry("2022-07-07T18:06:17.784Z", "value3", 3),
                    make_entry("2022-07-07T18:06:17.800Z", "value3", 200),
                    make_entry("2022-07-07T18:06:17.950Z", "value2", 10),
                ]),
            ]
        }

        fn data_3() -> Vec<JournalEntryStream> {
            vec![]
        }

        fn data_4() -> Vec<JournalEntryStream> {
            vec![
                create_reader(vec![
                    make_entry("2022-07-07T18:06:14.000Z", "value1", 10),
                    make_entry("2022-07-07T18:06:15.557Z", "value2", 20),
                    make_entry("2022-07-07T18:06:16.600Z", "value3", 30),
                    make_entry("2022-07-07T18:06:17.784Z", "value4", 40),
                ])
            ]
        }

        #[derive(Default)]
        struct TestCase {
            name: &'static str,
            params: GetLog2Params,
            expected: Vec<(&'static str, &'static str, RpcValue)>,
            expected_record_count_limit_hit: Option<bool>,
            expected_since: Option<&'static str>,
            expected_until: Option<&'static str>,
        }

        let test_cases = [
            TestCase {
                name: "default params (no snapshot)",
                params: Default::default(),
                expected: vec![
                    ("2022-07-07T18:06:15.557Z", "APP_START", true.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/zone/Zone1/plcDisconnected", false.into()),

                    ("2022-07-07T18:06:17.784Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.784Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.869Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),

                    ("2022-07-07T18:06:17.872Z", "APP_START", true.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),

                    ("2022-07-07T18:06:17.873Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.874Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.880Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                ],
                ..Default::default()
            },
            TestCase {
                name: "since",
                params: GetLog2Params { since: since("2022-07-07T18:06:17.872Z"), ..Default::default() },
                expected: vec![
                    ("2022-07-07T18:06:17.872Z", "APP_START", true.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/zone/Zone1/plcDisconnected", false.into()),

                    ("2022-07-07T18:06:17.873Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.874Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.880Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                ],
                ..Default::default()
            },
            TestCase {
                name: "until",
                params: GetLog2Params { until: Some(ts("2022-07-07T18:06:17.872Z")), ..Default::default() },
                expected: vec![
                    ("2022-07-07T18:06:15.557Z", "APP_START", true.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/zone/Zone1/plcDisconnected", false.into()),

                    ("2022-07-07T18:06:17.784Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.784Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.869Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                ],
                ..Default::default()
            },
            TestCase {
                name: "until after last entry",
                params: GetLog2Params { until: Some(ts("2022-07-07T18:06:17.900")), ..Default::default() },
                expected: vec![
                    ("2022-07-07T18:06:15.557Z", "APP_START", true.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/zone/Zone1/plcDisconnected", false.into()),

                    ("2022-07-07T18:06:17.784Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.784Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.869Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),

                    ("2022-07-07T18:06:17.872Z", "APP_START", true.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),

                    ("2022-07-07T18:06:17.873Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.874Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.880Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                ],
                ..Default::default()
            },
            TestCase {
                name: "until on last entry",
                params: GetLog2Params { until: Some(ts("2022-07-07T18:06:17.880")), ..Default::default() },
                expected: vec![
                    ("2022-07-07T18:06:15.557Z", "APP_START", true.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/zone/Zone1/plcDisconnected", false.into()),

                    ("2022-07-07T18:06:17.784Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.784Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.869Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),

                    ("2022-07-07T18:06:17.872Z", "APP_START", true.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),

                    ("2022-07-07T18:06:17.873Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.874Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                ],
                ..Default::default()
            },
            TestCase {
                name: "exact path",
                params: GetLog2Params { path_pattern: "zone1/pme/TSH1-1/switchRightCounterPermanent".to_string().into(), ..Default::default() },
                expected: vec![
                    ("2022-07-07T18:06:15.557Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                    ("2022-07-07T18:06:17.869Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                    ("2022-07-07T18:06:17.880Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                ],
                ..Default::default()
            },
            TestCase {
                name: "wildcard",
                params: GetLog2Params { path_pattern: "zone1/**".to_string().into(), ..Default::default() },
                expected: vec![
                    ("2022-07-07T18:06:15.557Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.784Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.784Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.869Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                    ("2022-07-07T18:06:17.873Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.874Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.880Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                ],
                ..Default::default()
            },
            TestCase {
                name: "record count higher",
                params: GetLog2Params { record_count_limit: 1000, ..Default::default() },
                expected_record_count_limit_hit: Some(false),
                expected: vec![
                    ("2022-07-07T18:06:15.557Z", "APP_START", true.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/zone/Zone1/plcDisconnected", false.into()),

                    ("2022-07-07T18:06:17.784Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.784Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.869Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),

                    ("2022-07-07T18:06:17.872Z", "APP_START", true.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.872Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),

                    ("2022-07-07T18:06:17.873Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.874Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.880Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                ],
                ..Default::default()
            },
            TestCase {
                name: "record count lower",
                params: GetLog2Params { record_count_limit: 7, ..Default::default() },
                expected_record_count_limit_hit: Some(true),
                expected: vec![
                    ("2022-07-07T18:06:15.557Z", "APP_START", true.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/zone/Zone1/plcDisconnected", false.into()),

                    ("2022-07-07T18:06:17.784Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.784Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.869Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                ],
                ..Default::default()
            },
            TestCase {
                name: "records from the same timestamp are always returned",
                params: GetLog2Params { record_count_limit: 5, ..Default::default() },
                expected_record_count_limit_hit: Some(true),
                expected: vec![
                    ("2022-07-07T18:06:15.557Z", "APP_START", true.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/pme/TSH1-1/switchRightCounterPermanent", 0u32.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:15.557Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.784Z", "zone1/system/sig/plcDisconnected", false.into()),
                    ("2022-07-07T18:06:17.784Z", "zone1/zone/Zone1/plcDisconnected", false.into()),
                ],
                ..Default::default()
            },
        ].into_iter().map(|test_case| (data_1 as fn() -> _, test_case)).chain([
            TestCase {
                name: "snapshot without since",
                params: GetLog2Params { with_snapshot: true, ..Default::default() },
                expected: vec![
                    ("2022-07-07T18:06:17.784Z", "value1", 0.into()),
                    ("2022-07-07T18:06:17.784Z", "value2", 1.into()),
                    ("2022-07-07T18:06:17.784Z", "value3", 3.into()),
                    ("2022-07-07T18:06:17.800Z", "value3", 200.into()),
                    ("2022-07-07T18:06:17.950Z", "value2", 10.into()),
                ],
                ..Default::default()
            },
            TestCase {
                name: "snapshot - one entry between snapshot and since",
                params: GetLog2Params { since: since("2022-07-07T18:06:17.850"), with_snapshot: true, ..Default::default() },
                expected: vec![
                    ("2022-07-07T18:06:17.850Z", "value1", 0.into()),
                    ("2022-07-07T18:06:17.850Z", "value2", 1.into()),
                    ("2022-07-07T18:06:17.850Z", "value3", 200.into()),
                    ("2022-07-07T18:06:17.950Z", "value2", 10.into()),
                ],
                ..Default::default()
            },
            TestCase {
                name: "snapshot - one entry exactly on since",
                params: GetLog2Params { since: since("2022-07-07T18:06:17.800"), with_snapshot: true, ..Default::default() },
                expected: vec![
                    ("2022-07-07T18:06:17.800Z", "value1", 0.into()),
                    ("2022-07-07T18:06:17.800Z", "value2", 1.into()),
                    ("2022-07-07T18:06:17.800Z", "value3", 200.into()),
                    ("2022-07-07T18:06:17.950Z", "value2", 10.into()),
                ],
                ..Default::default()
            },
            TestCase {
                name: "snapshot - with record cound limit smaller than the snapshot",
                params: GetLog2Params { since: since("2022-07-07T18:06:17.800"), with_snapshot: true, record_count_limit: 1, ..Default::default() },
                expected_record_count_limit_hit: Some(true),
                // The whole snapshot should be sent regardless of the small recordCountLimit.
                expected: vec![
                    ("2022-07-07T18:06:17.800Z", "value1", 0.into()),
                    ("2022-07-07T18:06:17.800Z", "value2", 1.into()),
                    ("2022-07-07T18:06:17.800Z", "value3", 200.into()),
                ],
                ..Default::default()
            },
            TestCase {
                name: "snapshot - with since after the last entry",
                params: GetLog2Params { since: since("2022-07-07T18:06:20.850"), with_snapshot: true, ..Default::default() },
                expected: vec![
                    ("2022-07-07T18:06:20.850Z", "value1", 0.into()),
                    ("2022-07-07T18:06:20.850Z", "value2", 10.into()),
                    ("2022-07-07T18:06:20.850Z", "value3", 200.into()),
                ],
                ..Default::default()
            },
            TestCase {
                name: "since last with snapshot",
                params: GetLog2Params { since: GetLog2Since::LastEntry, with_snapshot: true, ..Default::default() },
                expected: vec![
                    ("2022-07-07T18:06:17.950Z", "value1", 0.into()),
                    ("2022-07-07T18:06:17.950Z", "value2", 10.into()),
                    ("2022-07-07T18:06:17.950Z", "value3", 200.into()),
                ],
                expected_since: Some("2022-07-07T18:06:17.950Z"),
                expected_until: Some("2022-07-07T18:06:17.950Z"),
                ..Default::default()
            },
            TestCase {
                name: "since last without snapshot",
                params: GetLog2Params { since: GetLog2Since::LastEntry, with_snapshot: false, ..Default::default() },
                expected: vec![
                    ("2022-07-07T18:06:17.950Z", "value2", 10.into()),
                ],
                expected_since: Some("2022-07-07T18:06:17.950Z"),
                expected_until: Some("2022-07-07T18:06:17.950Z"),
                ..Default::default()
            },
        ].into_iter().map(|test_case| (data_2 as fn() -> _, test_case))).chain([
            ("result since/until - default params", Default::default()),
            ("result since/until - since set", GetLog2Params { since: since("2022-07-07T18:06:15.557Z"), ..Default::default() }),
            ("result since/until - until set", GetLog2Params { until: Some(ts("2022-07-07T18:06:15.557Z")), ..Default::default() }),
            ("result since/until - both since/until set", GetLog2Params { since: since("2022-07-07T18:06:15.557Z"), until: Some(ts("2022-07-07T18:06:20.000Z")), ..Default::default() }),
        ].into_iter().map(|(name, params)| (data_3 as fn() -> _, TestCase {
            name,
            params,
            expected: vec![],
            // For empty dataset, getlog always returns this since and until.
            expected_since: Some("1970-01-01T00:00:00.000Z"),
            expected_until: Some("1970-01-01T00:00:00.000Z"),
            ..Default::default()
        }))).chain([
            TestCase {
                name: "since/until not set",
                params: Default::default(),
                expected: vec![
                    ("2022-07-07T18:06:14.000Z", "value1", 10.into()),
                    ("2022-07-07T18:06:15.557Z", "value2", 20.into()),
                    ("2022-07-07T18:06:16.600Z", "value3", 30.into()),
                    ("2022-07-07T18:06:17.784Z", "value4", 40.into()),
                ],
                expected_since: Some("2022-07-07T18:06:14.000Z"),
                expected_until: Some("2022-07-07T18:06:17.785Z"),
                ..Default::default()
            },
            TestCase {
                name: "since on the first entry",
                params: GetLog2Params{since: since("2022-07-07T18:06:15.557Z"), ..Default::default()},
                expected: vec![
                    ("2022-07-07T18:06:15.557Z", "value2", 20.into()),
                    ("2022-07-07T18:06:16.600Z", "value3", 30.into()),
                    ("2022-07-07T18:06:17.784Z", "value4", 40.into()),
                ],
                expected_since: Some("2022-07-07T18:06:15.557Z"),
                expected_until: Some("2022-07-07T18:06:17.785Z"),
                ..Default::default()
            },
            TestCase {
                name: "since before the first entry",
                params: GetLog2Params{since: since("2022-07-07T18:06:13.000Z"), ..Default::default()},
                expected: vec![
                    ("2022-07-07T18:06:14.000Z", "value1", 10.into()),
                    ("2022-07-07T18:06:15.557Z", "value2", 20.into()),
                    ("2022-07-07T18:06:16.600Z", "value3", 30.into()),
                    ("2022-07-07T18:06:17.784Z", "value4", 40.into()),
                ],
                expected_since: Some("2022-07-07T18:06:14.000Z"),
                expected_until: Some("2022-07-07T18:06:17.785Z"),
                ..Default::default()
            },
            TestCase {
                name: "since after the first entry",
                params: GetLog2Params{since: since("2022-07-07T18:06:15.553Z"), ..Default::default()},
                expected: vec![
                    ("2022-07-07T18:06:15.557Z", "value2", 20.into()),
                    ("2022-07-07T18:06:16.600Z", "value3", 30.into()),
                    ("2022-07-07T18:06:17.784Z", "value4", 40.into()),
                ],
                expected_since: Some("2022-07-07T18:06:15.553Z"),
                expected_until: Some("2022-07-07T18:06:17.785Z"),
                ..Default::default()
            },
            TestCase {
                name: "until set",
                params: GetLog2Params{until: Some(ts("2022-07-07T18:06:18.700")), ..Default::default()},
                expected: vec![
                    ("2022-07-07T18:06:14.000Z", "value1", 10.into()),
                    ("2022-07-07T18:06:15.557Z", "value2", 20.into()),
                    ("2022-07-07T18:06:16.600Z", "value3", 30.into()),
                    ("2022-07-07T18:06:17.784Z", "value4", 40.into()),
                ],
                expected_since: Some("2022-07-07T18:06:14.000Z"),
                expected_until: Some("2022-07-07T18:06:17.785Z"),
                ..Default::default()
            },
            TestCase {
                name: "until set - record count limit hit",
                params: GetLog2Params{record_count_limit: 2, until: Some(ts("2022-07-07T18:06:18.700")), ..Default::default()},
                expected: vec![
                    ("2022-07-07T18:06:14.000Z", "value1", 10.into()),
                    ("2022-07-07T18:06:15.557Z", "value2", 20.into()),
                ],
                expected_since: Some("2022-07-07T18:06:14.000Z"),
                expected_until: Some("2022-07-07T18:06:16.600Z"),
                ..Default::default()
            },
            TestCase {
                name: "since/until set",
                params: GetLog2Params{since: since("2022-07-07T18:06:10.000Z"), until: Some(ts("2022-07-07T18:06:20.000Z")), ..Default::default()},
                expected: vec![
                    ("2022-07-07T18:06:14.000Z", "value1", 10.into()),
                    ("2022-07-07T18:06:15.557Z", "value2", 20.into()),
                    ("2022-07-07T18:06:16.600Z", "value3", 30.into()),
                    ("2022-07-07T18:06:17.784Z", "value4", 40.into()),
                ],
                expected_since: Some("2022-07-07T18:06:14.000Z"),
                expected_until: Some("2022-07-07T18:06:17.785Z"),
                ..Default::default()
            },
            TestCase {
                name: "since/until set - record_count_limit hit",
                params: GetLog2Params{record_count_limit: 2, since: since("2022-07-07T18:06:10.000Z"), until: Some(ts("2022-07-07T18:06:20.000Z")), ..Default::default()},
                expected: vec![
                    ("2022-07-07T18:06:14.000Z", "value1", 10.into()),
                    ("2022-07-07T18:06:15.557Z", "value2", 20.into()),
                ],
                expected_since: Some("2022-07-07T18:06:14.000Z"),
                expected_until: Some("2022-07-07T18:06:16.600Z"),
                ..Default::default()
            }
        ].into_iter().map(|test_case| (data_4 as fn() -> _, test_case)));

        for (data, case) in test_cases {
            let result = get_log_entries(data(), case.params).await;
            let chained_entries = result.snapshot_entries.into_iter().chain(result.event_entries.into_iter()).collect::<Vec<_>>();
            let expected = case.expected.into_iter().map(|(ts, path, val)| (ts.to_string(), path.to_string(), val)).collect::<Vec<_>>();
            assert_eq!(chained_entries.into_iter().map(|entry_val| (DateTime::from_epoch_msec(entry_val.epoch_msec).to_iso_string(), entry_val.path.clone(), entry_val.value.clone())).collect::<Vec<_>>(), expected, "Test case failed: {}", case.name);
            if let Some(expected_record_count_limit_hit) = case.expected_record_count_limit_hit {
                assert_eq!(result.record_count_limit_hit, expected_record_count_limit_hit, "Test case failed: {}", case.name);
            }
            if let Some(expected_since) = case.expected_since {
                assert_eq!(result.since.to_iso_string(), expected_since, "Test case failed: {}", case.name);
            }
            if let Some(expected_until) = case.expected_until {
                assert_eq!(result.until.to_iso_string(), expected_until, "Test case failed: {}", case.name);
            }
        }
    }
}
