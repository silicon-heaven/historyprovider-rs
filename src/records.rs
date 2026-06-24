use std::path::{Path, PathBuf};

use shvproto::RpcValue;
use shvrpc::metamethod::AccessLevel;
use shvrpc::rpc::ShvRI;
use shvrpc::journalrw::matches_path_pattern;
use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection, SqliteRow};
use sqlx::{AssertSqlSafe, Connection, Row};

use crate::record::{IMap, LogEntry, LogRecord, RecordType, log_records_from_fetch};

pub const RECORD_FETCH_COUNT: i64 = 50;

pub(crate) fn db_path(journal_dir: impl AsRef<str>, site_path: impl AsRef<str>, record_name: impl AsRef<str>) -> PathBuf {
    Path::new(journal_dir.as_ref())
        .join(site_path.as_ref())
        .join(format!("{}.db", record_name.as_ref()))
}

async fn open_db(path: impl AsRef<Path>) -> Result<SqliteConnection, String> {
    let options = SqliteConnectOptions::new()
        .filename(path.as_ref())
        .create_if_missing(true);
    SqliteConnection::connect_with(&options)
        .await
        .map_err(|e| e.to_string())
}

pub(crate) async fn ensure_db(path: impl AsRef<Path>) -> Result<SqliteConnection, String> {
    if let Some(parent) = path.as_ref().parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|err| format!("Cannot create records DB directory {}: {err}", parent.to_string_lossy()))?;
    }
    let mut conn = open_db(path).await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS journal_entries (
            id INTEGER PRIMARY KEY,
            type INTEGER NOT NULL,
            epoch_msec INTEGER NOT NULL,
            path TEXT,
            signal TEXT,
            source TEXT,
            value TEXT,
            access_level INTEGER,
            user_id TEXT,
            repeat INTEGER,
            time_jump INTEGER
        )",
    )
    .execute(&mut conn)
    .await
    .map_err(|e| e.to_string())?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_journal_entries_epoch ON journal_entries (epoch_msec);",
    )
    .execute(&mut conn)
    .await
    .map_err(|e| e.to_string())?;
    Ok(conn)
}

pub(crate) async fn next_offset(path: impl AsRef<Path>) -> Result<i64, String> {
    let mut conn = ensure_db(path).await?;
    let row = sqlx::query("SELECT COALESCE(MAX(id), -1) + 1 FROM journal_entries")
        .fetch_optional(&mut conn)
        .await
        .map_err(|e| e.to_string())?;
    let Some(row) = row else {
        return Ok(0);
    };
    row.try_get::<i64, _>(0).map_err(|e| e.to_string())
}

pub(crate) async fn span_records(path: impl AsRef<Path>) -> Result<(i64, i64, i64), String> {
    if tokio::fs::metadata(path.as_ref()).await.is_err() {
        return Ok((0, 0, 1));
    }

    let mut conn = open_db(path).await?;
    let row = sqlx::query("SELECT MIN(id), COALESCE(MAX(id), -1) + 1 FROM journal_entries")
        .fetch_optional(&mut conn)
        .await
        .map_err(|e| e.to_string())?;
    let Some(row) = row else {
        return Ok((0, 0, 1));
    };
    let smallest = row.try_get::<Option<i64>, _>(0).map_err(|e| e.to_string())?.unwrap_or(0);
    let biggest = row.try_get::<i64, _>(1).map_err(|e| e.to_string())?;
    Ok((smallest, biggest, 1))
}

type LogRecordRow = (i64, i64, Option<String>, Option<String>, Option<String>, Option<String>, Option<i64>, Option<String>, Option<i64>, Option<i64>);

fn record_to_values(record: &LogRecord) -> Result<LogRecordRow, String> {
    let mut path = None;
    let mut signal = None;
    let mut source = None;
    let mut value = None;
    let mut access_level = None;
    let mut user_id = None;
    let mut repeat = None;
    let mut time_jump = None;

    match &record.record_type {
        RecordType::Normal(entry) | RecordType::Keep(entry) => {
            if !entry.path.is_empty() {
                path = Some(entry.path.clone());
            }
            if entry.signal != "chng" {
                signal = Some(entry.signal.clone());
            }
            if entry.source != "get" {
                source = Some(entry.source.clone());
            }
            if let Some(entry_value) = &entry.value {
                value = Some(entry_value.to_cpon());
            }
            if entry.access_level != AccessLevel::Read as i64 {
                access_level = Some(entry.access_level);
            }
            user_id = entry.user_id.clone();
            if entry.repeat {
                repeat = Some(1);
            }
        }
        RecordType::TimeJump(offset) => {
            time_jump = Some(*offset);
        }
        RecordType::TimeAmbig => {}
    }

    Ok((record.record_type.type_id(), record.timestamp.epoch_msec(), path, signal, source, value, access_level, user_id, repeat, time_jump))
}

pub(crate) async fn insert_records(path: impl AsRef<Path>, records: &[LogRecord]) -> Result<(), String> {
    let mut conn = ensure_db(path).await?;

    for record in records {
        let (type_id, epoch_msec, path, signal, source, value, access_level, user_id, repeat, time_jump) = record_to_values(record)?;
        sqlx::query(
            "INSERT OR IGNORE INTO journal_entries (
                id, type, epoch_msec, path, signal, source, value, access_level, user_id, repeat, time_jump
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        .bind(record.id)
        .bind(type_id)
        .bind(epoch_msec)
        .bind(&path)
        .bind(&signal)
        .bind(&source)
        .bind(&value)
        .bind(access_level)
        .bind(&user_id)
        .bind(repeat)
        .bind(time_jump)
        .execute(&mut conn)
        .await
        .map_err(|e| e.to_string())?;
    }

    Ok(())
}

fn parse_value(value: Option<String>, id: i64) -> Result<Option<RpcValue>, String> {
    value
        .map(|value| RpcValue::from_cpon(&value)
            .map_err(|err| format!("Cannot parse stored record value for id {id}: {err}")))
        .transpose()
}

fn row_to_record(row: &SqliteRow) -> Result<LogRecord, String> {
    let id = row.try_get::<i64, _>(0).map_err(|e| e.to_string())?;
    let type_id = row.try_get::<i64, _>(1).map_err(|e| e.to_string())?;
    let epoch_msec = row.try_get::<i64, _>(2).map_err(|e| e.to_string())?;
    let path = row.try_get::<Option<String>, _>(3).map_err(|e| e.to_string())?.unwrap_or_default();
    let signal = row.try_get::<Option<String>, _>(4).map_err(|e| e.to_string())?.unwrap_or_else(|| "chng".to_string());
    let source = row.try_get::<Option<String>, _>(5).map_err(|e| e.to_string())?.unwrap_or_else(|| "get".to_string());
    let value = parse_value(row.try_get::<Option<String>, _>(6).map_err(|e| e.to_string())?, id)?;
    let access_level = row.try_get::<Option<i64>, _>(7).map_err(|e| e.to_string())?.unwrap_or(AccessLevel::Read as i64);
    let user_id = row.try_get::<Option<String>, _>(8).map_err(|e| e.to_string())?;
    let repeat = row.try_get::<Option<i64>, _>(9).map_err(|e| e.to_string())?.is_some_and(|repeat| repeat != 0);
    let time_jump = row.try_get::<Option<i64>, _>(10).map_err(|e| e.to_string())?;

    let record_type = match type_id {
        1 => RecordType::Normal(LogEntry { path, signal, source, value, access_level, user_id, repeat }),
        2 => RecordType::Keep(LogEntry { path, signal, source, value, access_level, user_id, repeat }),
        3 => RecordType::TimeJump(time_jump.ok_or_else(|| format!("Missing time jump offset for record id {id}"))?),
        4 => RecordType::TimeAmbig,
        _ => return Err(format!("Wrong stored record type {type_id} for id {id}")),
    };

    Ok(LogRecord {
        id,
        timestamp: shvproto::DateTime::from_epoch_msec(epoch_msec),
        record_type,
    })
}

pub(crate) async fn fetch_records(path: impl AsRef<Path>, offset: i64, count: i64) -> Result<Vec<IMap>, String> {
    if count == 0 || tokio::fs::metadata(path.as_ref()).await.is_err() {
        return Ok(Vec::new());
    }

    let mut conn = open_db(path).await?;
    let rows = sqlx::query(
        "SELECT id, type, epoch_msec, path, signal, source, value, access_level, user_id, repeat, time_jump
        FROM journal_entries
        WHERE id >= ? AND id < ?
        ORDER BY id
        LIMIT ?"
    )
    .bind(offset)
    .bind(offset + count)
    .bind(count)
    .fetch_all(&mut conn)
    .await
    .map_err(|e| e.to_string())?;
    let mut records = Vec::new();
    for row in &rows {
        records.push(row_to_record(row)?.to_imap());
    }
    Ok(records)
}

async fn query_log_records(
    path: impl AsRef<Path>,
    since: shvproto::DateTime,
    until: shvproto::DateTime,
    path_prefix: &str,
) -> Result<Vec<LogRecord>, String> {
    if tokio::fs::metadata(path.as_ref()).await.is_err() {
        return Ok(Vec::new());
    }

    let mut conn = open_db(path).await?;
    let reverse = until.epoch_msec() < since.epoch_msec();
    let (from, to, from_op, to_op, order) = if reverse {
        (until.epoch_msec(), since.epoch_msec(), ">=", "<", "DESC")
    } else {
        (since.epoch_msec(), until.epoch_msec(), ">", "<=", "ASC")
    };
    let path_like = if path_prefix.is_empty() {
        "%".to_string()
    } else {
        format!("{path_prefix}/%")
    };
    let query = format!(
        "SELECT id, type, epoch_msec, path, signal, source, value, access_level, user_id, repeat, time_jump
        FROM journal_entries
        WHERE epoch_msec {from_op} ? AND epoch_msec {to_op} ? AND (? = '' OR path = ? OR path LIKE ?)
        ORDER BY epoch_msec {order}, id {order}"
    );
    let rows = sqlx::query(AssertSqlSafe(query.as_str()))
        .bind(from)
        .bind(to)
        .bind(path_prefix)
        .bind(path_prefix)
        .bind(path_like)
        .fetch_all(&mut conn)
        .await
        .map_err(|e| e.to_string())?;
    let mut records = Vec::new();
    for row in &rows {
        records.push(row_to_record(row)?);
    }
    Ok(records)
}

fn relative_path<'a>(path: &'a str, prefix: &str) -> Option<&'a str> {
    if prefix.is_empty() {
        return Some(path);
    }
    if path == prefix {
        return Some("");
    }
    path.strip_prefix(prefix)?.strip_prefix('/')
}

fn getlog_record_to_imap(record: LogRecord, path_prefix: &str, path_pattern: Option<&ShvRI>) -> Option<(i64, IMap)> {
    let (entry, timestamp) = match record.record_type {
        RecordType::Normal(entry) | RecordType::Keep(entry) => (entry, record.timestamp),
        RecordType::TimeJump(_) | RecordType::TimeAmbig => return None,
    };
    let path = relative_path(&entry.path, path_prefix)?;
    if path_pattern.is_some_and(|pattern| !matches_path_pattern(path, pattern.to_string())) {
        return None;
    }

    let mut map = IMap::new();
    map.insert(1, timestamp.into());
    if !path.is_empty() {
        map.insert(3, path.into());
    }
    if entry.signal != "chng" {
        map.insert(4, entry.signal.into());
    }
    if entry.source != "get" {
        map.insert(5, entry.source.into());
    }
    if let Some(value) = entry.value {
        map.insert(6, value);
    }
    if let Some(user_id) = entry.user_id {
        map.insert(7, user_id.into());
    }
    if entry.repeat {
        map.insert(8, true.into());
    }
    Some((timestamp.epoch_msec(), map))
}

#[expect(clippy::too_many_arguments, reason = "This is AI slop")]
pub(crate) async fn getlog_records(
    journal_dir: impl AsRef<str>,
    site_path: &str,
    record_names: &[String],
    path_prefix: &str,
    since: shvproto::DateTime,
    until: shvproto::DateTime,
    count: Option<i64>,
    path_pattern: Option<&ShvRI>,
) -> Result<Vec<IMap>, String> {
    let reverse = until.epoch_msec() < since.epoch_msec();
    let mut records = Vec::new();
    for record_name in record_names {
        let db_path = db_path(journal_dir.as_ref(), site_path, record_name);
        records.extend(
            query_log_records(db_path, since, until, path_prefix)
                .await?
                .into_iter()
                .filter_map(|record| getlog_record_to_imap(record, path_prefix, path_pattern))
        );
    }
    records.sort_by(|(ts_a, _), (ts_b, _)| {
        if reverse {
            ts_b.cmp(ts_a)
        } else {
            ts_a.cmp(ts_b)
        }
    });
    let limit = count.and_then(|count| usize::try_from(count).ok());
    if let Some(limit) = limit && limit < records.len() {
        if limit == 0 {
            records.clear();
        } else {
            let boundary_ts = records[limit - 1].0;
            let keep_count = records
                .iter()
                .enumerate()
                .skip(limit)
                .find(|(_, (ts, _))| *ts != boundary_ts)
                .map(|(idx, _)| idx)
                .unwrap_or(records.len());
            records.truncate(keep_count);
        }
    }
    Ok(records.into_iter().map(|(_, record)| record).collect())
}

pub(crate) async fn children_for_path(
    journal_dir: impl AsRef<str>,
    site_path: &str,
    record_names: &[String],
    path_prefix: &str,
) -> Result<Vec<String>, String> {
    let mut children = std::collections::BTreeSet::new();
    for record_name in record_names {
        let db_path = db_path(journal_dir.as_ref(), site_path, record_name);
        if tokio::fs::metadata(&db_path).await.is_err() {
            continue;
        }
        let mut conn = open_db(db_path).await?;
        let path_like = if path_prefix.is_empty() {
            "%".to_string()
        } else {
            format!("{path_prefix}/%")
        };
        let rows = sqlx::query("SELECT DISTINCT path FROM journal_entries WHERE ? = '' OR path = ? OR path LIKE ?")
            .bind(path_prefix)
            .bind(path_prefix)
            .bind(path_like)
            .fetch_all(&mut conn)
            .await
            .map_err(|e| e.to_string())?;
        for row in &rows {
            let path = row.try_get::<Option<String>, _>(0).map_err(|e| e.to_string())?.unwrap_or_default();
            let Some(relative) = relative_path(&path, path_prefix) else {
                continue;
            };
            if let Some(child) = relative.split('/').find(|part| !part.is_empty()) {
                children.insert(child.to_string());
            }
        }
    }
    Ok(children.into_iter().collect())
}

pub(crate) fn rpcvalue_to_records(start_offset: i64, value: &RpcValue) -> Result<Vec<LogRecord>, String> {
    let srcs = Vec::<IMap>::try_from(value)
        .map_err(|err| format!("Cannot parse fetched records: {err}"))?;
    log_records_from_fetch(start_offset, &srcs).map_err(|err| err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{LogEntry, LogRecord, RecordType};

    fn record(id: i64, timestamp: i64, path: &str, value: impl Into<RpcValue>) -> LogRecord {
        LogRecord {
            id,
            timestamp: shvproto::DateTime::from_epoch_msec(timestamp),
            record_type: RecordType::Normal(LogEntry {
                path: path.to_string(),
                signal: "chng".to_string(),
                source: "get".to_string(),
                value: Some(value.into()),
                access_level: AccessLevel::Read as i64,
                user_id: None,
                repeat: false,
            }),
        }
    }

    #[tokio::test]
    async fn getlog_records_aggregate() {
        let journal_dir = tempfile::TempDir::with_prefix("test-hprs-records-getlog.").unwrap();
        let journal_dir = journal_dir.path().to_str().unwrap();
        insert_records(db_path(journal_dir, "site1", "maintenance"), &[record(0, 1000, "some/a", 1)]).await.unwrap();
        insert_records(db_path(journal_dir, "site1", "passage"), &[record(0, 2000, "other/b", 2)]).await.unwrap();

        let result = getlog_records(
            journal_dir,
            "site1",
            &["maintenance".to_string(), "passage".to_string()],
            "",
            shvproto::DateTime::from_epoch_msec(0),
            shvproto::DateTime::from_epoch_msec(3000),
            None,
            None,
        ).await.unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(String::try_from(result[0].get(&3).unwrap()).unwrap(), "some/a");
        assert_eq!(i64::try_from(result[0].get(&6).unwrap()).unwrap(), 1);
        assert_eq!(String::try_from(result[1].get(&3).unwrap()).unwrap(), "other/b");
        assert_eq!(i64::try_from(result[1].get(&6).unwrap()).unwrap(), 2);
    }

    #[tokio::test]
    async fn getlog_records_relative_path_and_count() {
        let journal_dir = tempfile::TempDir::with_prefix("test-hprs-records-getlog.").unwrap();
        let journal_dir = journal_dir.path().to_str().unwrap();
        insert_records(db_path(journal_dir, "site1", "maintenance"), &[
            record(0, 1000, "some/a", 1),
            record(1, 1000, "some/b", 2),
            record(2, 2000, "some/c", 3),
        ]).await.unwrap();

        let result = getlog_records(
            journal_dir,
            "site1",
            &["maintenance".to_string()],
            "some",
            shvproto::DateTime::from_epoch_msec(0),
            shvproto::DateTime::from_epoch_msec(3000),
            Some(1),
            None,
        ).await.unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(String::try_from(result[0].get(&3).unwrap()).unwrap(), "a");
        assert_eq!(String::try_from(result[1].get(&3).unwrap()).unwrap(), "b");
    }
}
