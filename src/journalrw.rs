use futures::{AsyncWrite, AsyncWriteExt, Stream};
use futures::io::{AsyncBufRead, AsyncBufReadExt, Lines};
use shvclient::clientnode::{METH_GET, SIG_CHNG};
use shvproto::RpcValue;
use shvrpc::metamethod::AccessLevel;
use std::collections::BTreeMap;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::error::Error;

use crate::journalentry::JournalEntry;

const JOURNAL_ENTRIES_SEPARATOR: &str = "\t";

const VALUE_FLAG_SPONTANEOUS_BIT: i32 = 1;
const VALUE_FLAG_PROVISIONAL_BIT: i32 = 2;

fn parse_journal_entry_log2(line: &str) -> Result<JournalEntry, Box<dyn Error>> {
    let parts: Vec<&str> = line.split(JOURNAL_ENTRIES_SEPARATOR).collect();
    let mut parts_iter = parts.iter().copied();

    let epoch_msec = shvproto::datetime::DateTime::from_iso_str(parts_iter
        .next()
        .ok_or_else(|| format!("Missing timestamp on line: {line}"))?)
        .map_err(|e| format!("Cannot parse timestamp on line: {line}, error: {e}"))?
        .epoch_msec();

    let _up_time = parts_iter.next();
    let path = parts_iter.next().ok_or_else(|| format!("Missing path on line: {line}"))?.to_string();
    let value = RpcValue::from_cpon(parts_iter.next().ok_or_else(|| format!("Missing value on line: {line}"))?)?;
    let short_time = parts_iter.next().unwrap_or_default().parse().unwrap_or(-1);
    let domain = parts_iter.next();
    let value_flags = parts_iter.next().unwrap_or_default().parse().unwrap_or(0);
    let user_id = parts_iter.next().and_then(|u| if u.is_empty() { None } else { Some(u.to_string()) });

    Ok(JournalEntry {
        epoch_msec,
        path,
        signal: domain.unwrap_or(SIG_CHNG).into(),
        source: METH_GET.into(),
        value,
        access_level: AccessLevel::Read as _,
        short_time,
        user_id,
        repeat: value_flags & (1 << VALUE_FLAG_SPONTANEOUS_BIT) == 0,
        provisional: value_flags & (1 << VALUE_FLAG_PROVISIONAL_BIT) != 0,
    })
}

pub struct JournalReaderLog2<R> {
    lines: Lines<R>,
}

impl<R> JournalReaderLog2<R>
where
    R: AsyncBufRead + Unpin,
{
    pub fn new(reader: R) -> Self {
        JournalReaderLog2 {
            lines: reader.lines(),
        }
    }
}

impl<R> Stream for JournalReaderLog2<R>
where
    R: AsyncBufRead + Unpin,
{
    type Item = Result<JournalEntry, Box<dyn Error>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let lines = Pin::new(&mut self.lines);

        match lines.poll_next(cx) {
            Poll::Ready(Some(Ok(line))) => Poll::Ready(Some(parse_journal_entry_log2(&line))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(Box::new(e)))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub struct JournalWriterLog2<W> {
    writer: W,
}

impl<W> JournalWriterLog2<W>
where
    W: AsyncWrite + Unpin,
{
    pub fn new(writer: W) -> Self {
        Self {
            writer,
        }
    }

    pub async fn append(&mut self, entry: &JournalEntry)  -> Result<(), std::io::Error> {
        let mut msec = entry.epoch_msec;
        if msec == 0 {
            msec = shvproto::DateTime::now().epoch_msec();
        }
        self.append_with_time(msec, msec, entry).await
    }

    pub async fn append_with_time(&mut self, msec: i64, orig_time: i64, entry: &JournalEntry) -> Result<(), std::io::Error> {
        let line = [
            shvproto::DateTime::from_epoch_msec(msec).to_iso_string(),
            if orig_time == msec { "".into() } else { shvproto::DateTime::from_epoch_msec(orig_time).to_iso_string() },
            entry.path.clone(),
            entry.value.to_cpon(),
            if entry.short_time >= 0 { entry.short_time.to_string() } else { "".into() },
            entry.signal.clone(),
            {
                let mut value_flags = 0u32;
                if !entry.repeat {
                    value_flags |= 1 << VALUE_FLAG_SPONTANEOUS_BIT;
                }
                if entry.provisional {
                    value_flags |= 1 << VALUE_FLAG_PROVISIONAL_BIT;
                }
                value_flags.to_string()
            },
            entry.user_id.clone().unwrap_or_default(),
        ].join(JOURNAL_ENTRIES_SEPARATOR) + "\n";
        self.writer.write_all(line.as_bytes()).await?;
        self.writer.flush().await
    }
}


// Reader of a result of SHV v2 `getLog`

pub struct Log2Reader {
    log: std::vec::IntoIter<RpcValue>,
    pub(crate) header: Log2Header,
}

impl Log2Reader {
    pub fn new(log: RpcValue) -> Result<Self, Box<dyn Error>> {
        let shvproto::Value::List(list) = log.value else {
            return Err("Wrong log format - not a list".into());
        };
        Ok(Self {
            log: list.into_iter(),
            header: Log2Header::from_meta(*log.meta.unwrap_or_default())?,
        })
    }
}

impl Iterator for Log2Reader {
    type Item = Result<JournalEntry, Box<dyn Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.log.next().map(|entry| rpc_value_to_journal_entry(&entry, &self.header))
    }
}

const NO_SHORT_TIME: i32 = -1;

fn rpc_value_to_journal_entry(entry: &RpcValue, header: &Log2Header) -> Result<JournalEntry, Box<dyn Error>> {
    let make_err = |msg| Err(format!("{msg}: {}", entry.to_cpon()).into());
    let shvproto::Value::List(row) = &entry.value else {
        return make_err("Log entry is not a list");
    };
    let mut row = row.as_slice().into_iter();

    let timestamp = row.next().unwrap_or_default();
    let timestamp = match &timestamp.value {
        shvproto::Value::DateTime(date_time) => *date_time,
        _ => return make_err(&format!("Wrong `timestamp` `{}` of journal entry", timestamp.to_cpon())),
    };

    let path = row.next().unwrap_or_default();
    let path = match &path.value {
        shvproto::Value::Int(idx) => header
            .paths_dict
            .get(&(*idx as i32))
            .ok_or_else(|| format!("Wrong path reference {idx} of journal entry: {}", entry.to_cpon()))?,
        shvproto::Value::String(path) => path,
        _ => return make_err(&format!("Wrong path `{}` of journal entry", path.to_cpon())),
    }.to_string();

    let value = row.next().unwrap_or_default();

    let short_time = row.next().unwrap_or_default();
    let short_time = match short_time.value {
        shvproto::Value::Int(val) if val as i32 >= 0 => val as _,
        _ => NO_SHORT_TIME,
    };

    let domain = row.next().unwrap_or_default();
    let signal = match &domain.value {
        shvproto::Value::String(domain) if domain.is_empty() || domain.as_str() == "C" => SIG_CHNG,
        shvproto::Value::String(domain) => domain.as_str(),
        _ => SIG_CHNG,
    }.to_string();

    let value_flags = row.next();
    let value_flags = match value_flags {
        Some(value_flags) => match &value_flags.value {
            shvproto::Value::UInt(val) => *val,
            shvproto::Value::Int(val) => *val as u64,
            _ => return make_err(&format!("Wrong `valueFlags` {} of journal entry", value_flags.to_cpon())),
        },
        None => 0,
    };

    let user_id = row.next().unwrap_or_default();
    let user_id = match &user_id.value {
        shvproto::Value::String(user_id) => Some(user_id.to_string()),
        shvproto::Value::Null => None,
        _ => return make_err(&format!("Wrong `userId` `{}` of journal entry", user_id.to_cpon())),
    };

    Ok(JournalEntry {
        epoch_msec: timestamp.epoch_msec(),
        path,
        signal,
        source: METH_GET.into(),
        value: value.clone(),
        access_level: AccessLevel::Read as _,
        short_time,
        user_id,
        repeat: if value_flags & (1 << VALUE_FLAG_SPONTANEOUS_BIT) == 0 { true } else { false },
        provisional: if value_flags & (1 << VALUE_FLAG_PROVISIONAL_BIT) != 0 { true } else { false },
    })
}

#[derive(Clone, Debug)]
pub(crate) struct GetLog2Params {
    pub(crate) since: Option<shvproto::DateTime>,
    pub(crate) until: Option<shvproto::DateTime>,
    pub(crate) path_pattern: Option<String>,
    pub(crate) with_paths_dict: bool,
    pub(crate) with_snapshot: bool,
    pub(crate) record_count_limit: i64,
}

const RECORD_COUNT_LIMIT_DEFAULT: i64 = 10000;

impl Default for GetLog2Params {
    fn default() -> Self {
        Self {
            since: None,
            until: None,
            path_pattern: None,
            with_paths_dict: true,
            with_snapshot: false,
            record_count_limit: RECORD_COUNT_LIMIT_DEFAULT,
        }
    }
}

impl From<GetLog2Params> for RpcValue {
    fn from(value: GetLog2Params) -> Self {
        let mut map = shvproto::Map::new();
        if let Some(since) = value.since {
            map.insert("since".into(), since.into());
        }
        if let Some(until) = value.until {
            map.insert("until".into(), until.into());
        }
        if let Some(path_pattern) = value.path_pattern {
            map.insert("pathPattern".into(), path_pattern.into());
        }
        map.insert("withPathsDict".into(), value.with_paths_dict.into());
        map.insert("withSnapshot".into(), value.with_snapshot.into());
        map.insert("recordCountLimit".into(), value.record_count_limit.into());
        map.into()
    }
}

impl TryFrom<&RpcValue> for GetLog2Params {
    type Error = String;

    fn try_from(value: &RpcValue) -> Result<Self, Self::Error> {
        let shvproto::Value::Map(map) = &value.value else {
            return Err(format!("getLog params has wrong type, expected Map, got {}", value.type_name()));
        };
        let since = match map.get("since") {
            Some(since) => Some(since.to_datetime().ok_or_else(|| "Invalid `since` value type".to_string())?),
            None => None,
        };
        let until = match map.get("until") {
            Some(until) => Some(until.to_datetime().ok_or_else(|| "Invalid `until` value type".to_string())?),
            None => None,
        };
        let path_pattern = match map.get("pathPattern").map(|v| &v.value) {
            Some(shvproto::Value::String(path_pattern)) => Some(path_pattern.to_string()),
            Some(_) => return Err("Invalid `pathPattern` type".into()),
            None => None,
        };
        let with_paths_dict = match map.get("withPathsDict").map(|v| &v.value) {
            Some(shvproto::Value::Bool(val)) => *val,
            Some(_) => return Err("Invalid `withPathsDict` type".into()),
            None => true,
        };
        let with_snapshot = match map.get("withSnapshot").map(|v| &v.value) {
            Some(shvproto::Value::Bool(val)) => *val,
            Some(_) => return Err("Invalid `withSnapshot` type".into()),
            None => false,
        };
        let record_count_limit = match map.get("recordCountLimit").map(|v| &v.value) {
            Some(shvproto::Value::Int(val)) => *val,
            Some(_) => return Err("Invalid `recordCountLimit` type".into()),
            None => RECORD_COUNT_LIMIT_DEFAULT,
        };
        Ok(Self { since, until, path_pattern, with_paths_dict, with_snapshot, record_count_limit })
    }
}

#[cfg(test)]
pub(crate) fn matches_path_pattern(path: impl AsRef<str>, pattern: impl AsRef<str>) -> bool {
    let path_parts: Vec<&str> = path.as_ref().split('/').collect();
    let pattern_parts: Vec<&str> = pattern.as_ref().split('/').collect();

    let (mut path_ix, mut patt_ix) = (0, 0);
    let (mut last_starstar_patt_ix, mut last_starstar_path_ix) = (None, 0);

    while path_ix < path_parts.len() {
        match pattern_parts.get(patt_ix).copied() {
            Some("**") => {
                last_starstar_patt_ix = Some(patt_ix);
                last_starstar_path_ix = path_ix;
                patt_ix += 1;
            }
            Some("*") => {
                path_ix += 1;
                patt_ix += 1;
            }
            Some(literal) if literal == path_parts[path_ix] => {
                path_ix += 1;
                patt_ix += 1;
            }
            _ => {
                if let Some(starstart_patt_ix) = last_starstar_patt_ix {
                    // Backtrack to the last occurence of "**"
                    last_starstar_path_ix += 1;
                    path_ix = last_starstar_path_ix;
                    patt_ix = starstart_patt_ix + 1;
                } else {
                    return false;
                }
            }
        }
    }

    // Match "**" at the end of the pattern
    while let Some("**") = pattern_parts.get(patt_ix).copied() {
        patt_ix += 1;
    }

    patt_ix == pattern_parts.len()
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct Log2Header {
    pub(crate) record_count: i64,
    pub(crate) record_count_limit: i64,
    pub(crate) record_count_limit_hit: bool,
    pub(crate) date_time: shvproto::DateTime,
    pub(crate) since: shvproto::DateTime,
    pub(crate) until: shvproto::DateTime,
    pub(crate) with_paths_dict: bool,
    pub(crate) with_snapshot: bool,
    pub(crate) paths_dict: BTreeMap<i32, String>,
    pub(crate) log_params: GetLog2Params,
    pub(crate) log_version: i64,
}

impl Log2Header {
    fn from_meta(meta: shvproto::MetaMap) -> Result<Self, Box<dyn Error>> {
        let current_datetime = shvproto::DateTime::now();
        let record_count = match meta.get("recordCount").map(|v| &v.value) {
            Some(shvproto::Value::Int(record_count)) => *record_count,
            Some(v) => return Err(format!("Invalid `recordCount` type: {}", v.type_name()).into()),
            None => 0,
        };
        let record_count_limit = match meta.get("recordCountLimit").map(|v| &v.value) {
            Some(shvproto::Value::Int(record_count_limit)) => *record_count_limit,
            Some(v) => return Err(format!("Invalid `recordCountLimit` type: {}", v.type_name()).into()),
            None => 0,
        };
        let record_count_limit_hit = match meta.get("recordCountLimitHit").map(|v| &v.value) {
            Some(shvproto::Value::Bool(record_count_limit_hit)) => *record_count_limit_hit,
            Some(v) => return Err(format!("Invalid `recordCountLimitHit` type: {}", v.type_name()).into()),
            None => false,
        };
        let date_time = match meta.get("dateTime").map(|v| &v.value) {
            Some(shvproto::Value::DateTime(date_time)) => *date_time,
            Some(shvproto::Value::Null) | None => current_datetime,
            Some(v) => return Err(format!("Invalid `dateTime` type: {}", v.type_name()).into()),
        };
        let since = match meta.get("since").map(|v| &v.value) {
            Some(shvproto::Value::DateTime(since)) => *since,
            Some(shvproto::Value::Null) | None => current_datetime,
            Some(v) => return Err(format!("Invalid `since` type: {}", v.type_name()).into()),
        };
        let until = match meta.get("until").map(|v| &v.value) {
            Some(shvproto::Value::DateTime(until)) => *until,
            Some(shvproto::Value::Null) | None => current_datetime,
            Some(v) => return Err(format!("Invalid `until` type: {}", v.type_name()).into()),
        };
        let with_paths_dict = match meta.get("withPathsDict").map(|v| &v.value) {
            Some(shvproto::Value::Bool(val)) => *val,
            Some(v) => return Err(format!("Invalid `withPathsDict` type: {}", v.type_name()).into()),
            None => true,
        };
        let with_snapshot = match meta.get("withSnapshot").map(|v| &v.value) {
            Some(shvproto::Value::Bool(val)) => *val,
            Some(v) => return Err(format!("Invalid `withSnapshot` type: {}", v.type_name()).into()),
            None => false,
        };
        let paths_dict: BTreeMap<i32, String> = match meta.get("pathsDict").map(|v| &v.value) {
            Some(shvproto::Value::IMap(val)) => val
                .iter()
                .map(|(i, v)| v
                    .try_into()
                    .map(|s| (*i, s)))
                .collect::<Result<BTreeMap<_, _>,_>>()
                .map_err(|e| format!("Corrupted paths dictionary: {e}"))?,
            Some(v) => return Err(format!("Invalid `pathsDict` type: {}", v.type_name()).into()),
            None => Default::default(),
        };
        let log_params = match meta.get("logParams") {
            Some(val) => GetLog2Params::try_from(val)?,
            None => GetLog2Params::try_from(&shvproto::Map::new().into())?,
        };

        let log_version = match meta.get("logVersion").map(|v| &v.value) {
            Some(shvproto::Value::Int(val)) => *val,
            Some(v) => return Err(format!("Invalid `logVersion` type: {}", v.type_name()).into()),
            None => 2,
        };
        Ok(Self {
            record_count,
            record_count_limit,
            record_count_limit_hit,
            date_time,
            since,
            until,
            with_paths_dict,
            with_snapshot,
            paths_dict,
            log_params,
            log_version,
        })
    }
}

#[cfg(test)]
mod tests {
    use futures::io::{BufReader, Cursor};
    use futures::StreamExt;
    use shvclient::clientnode::{METH_GET, SIG_CHNG};
    use shvproto::{CponReader, Reader};
    use shvrpc::metamethod::AccessLevel;
    use tokio::fs::File;
    use tokio_util::compat::TokioAsyncReadCompatExt;

    use crate::journalentry::JournalEntry;
    use crate::journalrw::{matches_path_pattern, JournalReaderLog2, JournalWriterLog2, Log2Reader};

    #[tokio::test]
    async fn read_file_journal() {
        let file = File::open("tests/test.log2").await.unwrap();
        let mut reader = JournalReaderLog2::new(BufReader::new(file.compat()));
        while let Some(result) = reader.next().await {
            println!("{:?}", result.unwrap());
        }
    }

    #[tokio::test]
    async fn journal_write_and_read() {
        let entries = [
            JournalEntry {
                epoch_msec: shvproto::DateTime::now().epoch_msec(),
                path: "test/path".into(),
                signal: SIG_CHNG.into(),
                source: METH_GET.into(),
                value: 42.into(),
                access_level: AccessLevel::Read as _, // Field is not used in SHV2
                short_time: 0,
                user_id: Some("user".into()),
                repeat: false,
                provisional: false,
            },
            JournalEntry {
                epoch_msec: shvproto::DateTime::now().epoch_msec(),
                path: "test/path2".into(),
                signal: SIG_CHNG.into(),
                source: METH_GET.into(),
                value: shvproto::make_map!("a" => 1, "b" => 2).into(),
                access_level: AccessLevel::Read as _, // Field is not used in SHV2
                short_time: 123,
                user_id: None,
                repeat: true,
                provisional: true,
            },
            ];
        let mut writer = JournalWriterLog2::new(Cursor::new(Vec::new()));
        for entry in &entries {
            writer.append(entry).await.unwrap();
        }

        let data = writer.writer.into_inner();
        let reader = JournalReaderLog2::new(Cursor::new(data));
        let mut enumerated_reader = reader.enumerate();
        while let Some((ix, result)) = enumerated_reader.next().await {
            let entry = &entries[ix];
            let entry2 = result.unwrap();
            println!("{:?}", entry);
            println!("{:?}", entry2);
            assert!(entry == &entry2);
        }
    }

    #[tokio::test]
    async fn get_log_2() {
        let mut file = std::fs::File::open("tests/log2.cpon").unwrap();
        let mut reader = CponReader::new(&mut file);
        let reader = Log2Reader::new(reader.read().unwrap()).unwrap();
        let epoch_ms_now = shvproto::DateTime::now().epoch_msec();
        let res = reader
            .map(|item|
                item.map(|mut entry| {
                    if entry.epoch_msec == 0 {
                        entry.epoch_msec = epoch_ms_now
                    }
                    entry
                })
            )
            .collect::<Result<Vec<_>,_>>().unwrap();
        println!("{res:?}");
    }

    #[test]
    fn path_pattern_match() {
        assert!(matches_path_pattern("foo/bar/x/bar/baz", "foo/**/bar/baz"));
        assert!(matches_path_pattern("foo/a/b/c/bar/baz", "foo/**/bar/baz"));
        assert!(matches_path_pattern("foo/bar/baz", "foo/**/bar/baz"));
        assert!(!matches_path_pattern("foo/bar/x/baz", "foo/**/bar/baz"));
        assert!(!matches_path_pattern("foo/bar/x/bar", "foo/**/bar/baz"));
    }
}
