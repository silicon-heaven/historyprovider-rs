use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use shvclient::ClientCommandSender;
use shvclient::clientapi::{CallRpcMethodErrorKind, RpcCall};
use shvproto::{DateTime, RpcValue};
use shvrpc::journalentry::JournalEntry;
use shvrpc::journalrw::{GetLog2Params, GetLog2Since, Log2Reader};
use shvrpc::metamethod::AccessLevel;
use shvrpc::rpcmessage::{RpcErrorCode, RpcErrorCodeKind};
use tokio::sync::Semaphore;

use crate::sites::{ParsedNotification, SiteInfo, SubHpInfo};

pub(crate) type SnapshotEntries = BTreeMap<(String, String, String), JournalEntry>;

#[derive(Default)]
pub(crate) enum SiteSnapshot {
    #[default]
    Invalid,
    Initializing(SnapshotEntries),
    Ready(SnapshotEntries),
}

impl SiteSnapshot {
    pub(crate) fn is_ready(&self) -> bool {
        matches!(self, Self::Ready(_))
    }

    pub(crate) fn record(&mut self, entry: JournalEntry) {
        match self {
            Self::Initializing(entries) | Self::Ready(entries) => insert(entries, entry),
            Self::Invalid => (),
        }
    }

    pub(crate) fn complete(&mut self, mut entries: SnapshotEntries) -> Option<&SnapshotEntries> {
        let Self::Initializing(buffered) = self else {
            return None;
        };
        entries.append(buffered);
        if let Some(timestamp) = entries.values().map(|entry| entry.epoch_msec).max() {
            for entry in entries.values_mut() {
                entry.epoch_msec = timestamp;
            }
        }
        *self = Self::Ready(entries);
        let Self::Ready(entries) = self else { unreachable!() };
        Some(entries)
    }
}

fn insert(entries: &mut SnapshotEntries, entry: JournalEntry) {
    entries.insert((entry.path.clone(), entry.signal.clone(), entry.source.clone()), entry);
}

pub(crate) fn notification_entry(notification: ParsedNotification, timestamp: DateTime) -> JournalEntry {
    JournalEntry {
        epoch_msec: timestamp.epoch_msec(),
        epoch_msec_orig: None,
        path: notification.property_path,
        signal: notification.signal,
        source: if notification.source.is_empty() { "get".into() } else { notification.source },
        value: notification.param,
        access_level: AccessLevel::Read as i32,
        short_time: -1,
        user_id: notification.user_id.unwrap_or_default(),
        repeat: false,
        provisional: false,
    }
}

// getSnapshot uses snapshot IMap keys, which differ from the log3 record keys.
fn parse_snapshot(value: RpcValue) -> Result<SnapshotEntries, String> {
    let rows = <Vec<RpcValue>>::try_from(value)?;
    let mut entries = SnapshotEntries::new();
    for row in rows {
        let row = shvproto::IMap::try_from(row)?;
        let timestamp = row.get(&1).ok_or("Snapshot entry has no timestamp")?;
        let timestamp = DateTime::try_from(timestamp)?;
        let string_field = |key, default: &str| -> Result<String, String> {
            row.get(&key).map_or_else(|| Ok(default.into()), String::try_from)
        };
        let entry = JournalEntry {
            epoch_msec: timestamp.epoch_msec(),
            epoch_msec_orig: None,
            path: string_field(3, "")?,
            signal: string_field(4, "chng")?,
            source: string_field(5, "get")?,
            value: row.get(&6).cloned().unwrap_or_else(RpcValue::null),
            access_level: AccessLevel::Read as i32,
            short_time: -1,
            user_id: string_field(7, "")?,
            repeat: row.get(&8).map_or(Ok(false), bool::try_from)?,
            provisional: false,
        };
        insert(&mut entries, entry);
    }
    Ok(entries)
}

// TODO: Define getLog/getSnapshot for each site statically
#[derive(Clone)]
pub(crate) struct SnapshotPaths {
    snapshot: String,
    getlog: String,
}

impl SnapshotPaths {
    pub(crate) fn new(site: &str, info: &SiteInfo, sub_hp: &SubHpInfo) -> Self {
        let suffix = shvrpc::util::strip_prefix_path(site, &info.sub_hp).expect("Site must be under its sub HP");
        Self {
            snapshot: if matches!(sub_hp, SubHpInfo::Records { .. }) {
                shvrpc::join_path!("shv", &info.sub_hp, ".history", suffix)
            } else {
                shvrpc::join_path!("shv", site, ".history")
            },
            getlog: if let SubHpInfo::Legacy { getlog_path } = sub_hp {
                shvrpc::join_path!("shv", &info.sub_hp, getlog_path, suffix)
            } else {
                shvrpc::join_path!("shv", site)
            },
        }
    }
}

async fn fetch(paths: &SnapshotPaths, client_commands: &ClientCommandSender) -> Result<SnapshotEntries, String> {
    let response: Result<RpcValue, _> = RpcCall::new(&paths.snapshot, "getSnapshot")
        .exec(client_commands).await;
    match response {
        Ok(value) => return parse_snapshot(value),
        Err(err) if matches!(err.error(), CallRpcMethodErrorKind::RpcError(error)
            if error.code == RpcErrorCodeKind::RpcError(RpcErrorCode::MethodNotFound)) => (),
        Err(err) => return Err(err.to_string()),
    }

    let params = GetLog2Params {
        since: GetLog2Since::LastEntry,
        with_snapshot: true,
        record_count_limit: i64::MAX,
        ..Default::default()
    };
    let value = RpcCall::new(&paths.getlog, "getLog").param(params)
        .exec(client_commands).await.map_err(|err| err.to_string())?;
    let reader = Log2Reader::new(value).map_err(|err| err.to_string())?;
    if reader.header().record_count_limit_hit {
        return Err("Snapshot getLog response hit the record count limit".into());
    }
    let mut entries = SnapshotEntries::new();
    for entry in reader {
        insert(&mut entries, entry.map_err(|err| err.to_string())?);
    }
    Ok(entries)
}

pub(crate) async fn fetch_with_retry(
    site: String,
    paths: SnapshotPaths,
    client_commands: ClientCommandSender,
    semaphore: Arc<Semaphore>,
) -> SnapshotEntries {
    loop {
        let result = {
            let _permit = semaphore.acquire().await.expect("Snapshot semaphore must be open");
            fetch(&paths, &client_commands).await
        };
        match result {
            Ok(entries) => return entries,
            Err(err) => log::warn!("[{site}] Cannot fetch snapshot: {err}"),
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}

#[cfg(test)]
mod tests {
    use futures::channel::mpsc::unbounded;
    use shvrpc::rpcmessage::RpcError;

    use super::*;
    use crate::util::testing::expect_rpc_call;

    #[test]
    fn snapshot_format_and_defaults() {
        let entries = parse_snapshot(RpcValue::from_cpon(r#"[
            i{1:d"2026-11-14T12:00:00Z", 3:"status", 5:"get", 6:1},
            i{1:d"2026-11-14T12:00:01Z", 3:"status", 6:2},
            i{1:d"2026-11-14T12:00:02Z", 3:"status", 4:"cmdlog", 5:"set", 6:3, 7:"user", 8:true},
            i{1:d"2026-11-14T12:00:03Z"}
        ]"#).unwrap()).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[&("status".into(), "chng".into(), "get".into())].value, 2.into());
        let command = &entries[&("status".into(), "cmdlog".into(), "set".into())];
        assert_eq!(command.value, 3.into());
        assert_eq!(command.user_id, "user");
        assert!(command.repeat);
        assert!(entries[&("".into(), "chng".into(), "get".into())].value.is_null());

        for invalid in ["{}", "[1]", "[i{3:\"status\"}]", "[i{1:123}]", "[i{1:d\"2026-11-14T12:00:00Z\", 3:42}]"] {
            assert!(parse_snapshot(RpcValue::from_cpon(invalid).unwrap()).is_err(), "{invalid}");
        }
    }

    #[tokio::test]
    async fn fetch_getsnapshot_and_legacy_getlog() {
        for use_getlog in [false, true] {
            let (sender, mut receiver) = unbounded();
            let sender = ClientCommandSender::from_raw(sender);
            let paths = SnapshotPaths { snapshot: "shv/site/.history".into(), getlog: "shv/site".into() };
            let driver = async {
                if use_getlog {
                    expect_rpc_call(&mut receiver, "shv/site/.history", "getSnapshot", None,
                        Err(RpcError::new(RpcErrorCode::MethodNotFound, "No getSnapshot"))).await;
                    expect_rpc_call(&mut receiver, "shv/site", "getLog", Some(GetLog2Params {
                        since: GetLog2Since::LastEntry, with_snapshot: true, record_count_limit: i64::MAX, ..Default::default()
                    }.into()), Ok(RpcValue::from_cpon(r#"[
                        [d"2026-11-14T12:00:00Z", "status", 42]
                    ]"#).unwrap())).await;
                } else {
                    expect_rpc_call(&mut receiver, "shv/site/.history", "getSnapshot", Some(RpcValue::null()),
                        Ok(RpcValue::from_cpon(r#"[
                            i{1:d"2026-11-14T12:00:00Z", 3:"status", 6:42}
                        ]"#).unwrap())).await;
                }
            };
            let (result, ()) = tokio::time::timeout(Duration::from_secs(2), async {
                tokio::join!(fetch(&paths, &sender), driver)
            }).await.unwrap();
            let entries = result.unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[&("status".into(), "chng".into(), "get".into())].value, 42.into());
        }
    }

    #[tokio::test]
    async fn fetch_rejects_rpc_errors_and_incomplete_logs() {
        for incomplete in [false, true] {
            let (sender, mut receiver) = unbounded();
            let sender = ClientCommandSender::from_raw(sender);
            let paths = SnapshotPaths { snapshot: "shv/site/.history".into(), getlog: "shv/site".into() };
            let driver = async {
                expect_rpc_call(&mut receiver, "shv/site/.history", "getSnapshot", None,
                    Err(RpcError::new(if incomplete { RpcErrorCode::MethodNotFound } else { RpcErrorCode::InternalError }, "error"))).await;
                if incomplete {
                    expect_rpc_call(&mut receiver, "shv/site", "getLog", None,
                        Ok(RpcValue::from_cpon(r#"<"recordCountLimitHit":true>[]"#).unwrap())).await;
                }
            };
            let (result, ()) = tokio::time::timeout(Duration::from_secs(2), async {
                tokio::join!(fetch(&paths, &sender), driver)
            }).await.unwrap();
            assert!(result.is_err());
        }
    }

    #[test]
    fn snapshot_paths_for_nested_sites() {
        let info = SiteInfo { name: "site".into(), site_type: "test".into(), sub_hp: "parent".into() };
        let records = SnapshotPaths::new("parent/site", &info, &SubHpInfo::Records { records: vec![] });
        assert_eq!(records.snapshot, "shv/parent/.history/site");
        let legacy = SnapshotPaths::new("parent/site", &info, &SubHpInfo::Legacy { getlog_path: ".local/history".into() });
        assert_eq!(legacy.getlog, "shv/parent/.local/history/site");
    }
}
