use std::collections::BTreeMap;
use shvproto::RpcValue;

pub type IMap = BTreeMap<i32, RpcValue>;

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogField {
    Type = 0,
    Timestamp = 1,
    Path = 2,
    Signal = 3,
    Source = 4,
    Value = 5,
    AccessLevel = 6,
    UserId = 7,
    Repeat = 8,
    Id = 9,
    Ref = 10,
    TimeJump = 60,
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub path: String,       // default: ""
    pub signal: String,     // default: "chng"
    pub source: String,     // default: "get"
    pub value: Option<RpcValue>,    // default: null
    pub access_level: i64,  // default: Read
    pub user_id: Option<String>,
    pub repeat: bool,       // default: false
}

#[derive(Debug, Clone)]
pub enum RecordType {
    Normal(LogEntry),
    Keep(LogEntry),
    TimeJump(i64),
    TimeAmbig,
}

#[derive(Debug, Clone)]
pub struct LogRecord {
    pub timestamp: shvproto::DateTime,
    pub id: i64,
    pub record_type: RecordType,
}

impl RecordType {
    pub fn type_id(&self) -> i64 {
        match self {
            RecordType::Normal(_) => 1,
            RecordType::Keep(_) => 2,
            RecordType::TimeJump(_) => 3,
            RecordType::TimeAmbig => 4,
        }
    }
}

fn get_field<T>(imap: &IMap, field: LogField) -> Option<T>
where
    T: for<'a> TryFrom<&'a RpcValue>
{
    imap.get(&(field as i32))
        .and_then(|v| T::try_from(v).ok())
}

fn get_field_checked<T>(imap: &IMap, field: LogField) -> Result<Option<T>, <T as TryFrom<&RpcValue>>::Error>
where
    T: for<'a> TryFrom<&'a RpcValue>,
{
    imap
        .get(&(field as i32))
        .map_or_else(|| Ok(None), |v| T::try_from(v).map(Some))
}

impl LogRecord {
    pub fn to_imap(&self) -> IMap {
        let mut map = IMap::new();

        map.insert(LogField::Type as i32, self.record_type.type_id().into());
        map.insert(LogField::Timestamp as i32, self.timestamp.into());

        map.insert(LogField::Id as i32, self.id.into());

        match &self.record_type {
            RecordType::Normal(entry) | RecordType::Keep(entry) => {
                if !entry.path.is_empty() {
                    map.insert(LogField::Path as i32, RpcValue::from(&entry.path));
                }
                if entry.signal != "chng" {
                    map.insert(LogField::Signal as i32, RpcValue::from(&entry.signal));
                }
                if entry.source != "get" {
                    map.insert(LogField::Source as i32, RpcValue::from(&entry.source));
                }
                if let Some(value) = &entry.value {
                    map.insert(LogField::Value as i32, value.clone());
                }
                if entry.access_level != shvrpc::metamethod::AccessLevel::Read as i64 {
                    map.insert(LogField::AccessLevel as i32, entry.access_level.into());
                }
                if let Some(user_id) = &entry.user_id {
                    map.insert(LogField::UserId as i32, user_id.into());
                }
                if entry.repeat {
                    map.insert(LogField::Repeat as i32, entry.repeat.into());
                }
            }
            RecordType::TimeJump(offset) => {
                map.insert(LogField::TimeJump as i32, (*offset).into());
            }
            RecordType::TimeAmbig => { /* no extra fields */ }
        }

        map
    }

    pub fn from_imap(map: &IMap) -> Option<Self> {
        let type_id = get_field(map, LogField::Type)?;
        let timestamp = get_field(map, LogField::Timestamp)?;
        let id = get_field(map, LogField::Id)?;

        let record_type = match type_id {
            1 | 2 => {
                let entry = LogEntry {
                    path: get_field(map, LogField::Path).unwrap_or_else(|| "".into()),
                    signal: get_field(map, LogField::Signal).unwrap_or_else(|| "chng".into()),
                    source: get_field(map, LogField::Source).unwrap_or_else(|| "get".into()),
                    value: get_field(map, LogField::Value),
                    access_level: get_field(map, LogField::AccessLevel).unwrap_or_else(|| shvrpc::metamethod::AccessLevel::Read as _),
                    user_id: get_field(map, LogField::UserId),
                    repeat: get_field(map, LogField::Repeat).unwrap_or_default(),
                    // reference: get_field(map, LogField::Ref),
                };
                if type_id == 1 {
                    RecordType::Normal(entry)
                } else {
                    RecordType::Keep(entry)
                }
            }
            3 => {
                let offset = get_field(map, LogField::TimeJump).unwrap_or_default();
                RecordType::TimeJump(offset)
            }
            4 => RecordType::TimeAmbig,
            _ => return None,
        };

        Some(Self {
            timestamp,
            id,
            record_type,
        })
    }
}

pub(crate) fn log_records_from_fetch(start_offset: i64, srcs: &[IMap]) -> shvrpc::Result<Vec<LogRecord>> {
    let mut result = Vec::<LogRecord>::with_capacity(srcs.len());

    for (i, src) in srcs.into_iter().enumerate() {
        let id: i64 = get_field(src, LogField::Id)
            .or_else(|| result.last().map(|last| last.id + 1))
            .unwrap_or(start_offset);

        let record_err = |reason: &str| format!("Record id: {id}, idx: {i}, {reason}");

        let timestamp: shvproto::DateTime = get_field_checked(src, LogField::Timestamp)
            .map_err(|e| record_err(&format!("wrong timestamp type: {e}")))?
            .ok_or_else(|| record_err("missing timestamp"))?;

        let log_record = if let Some(ref_offset) = get_field::<i64>(src, LogField::Ref) {
            // Copy fields from the referenced record
            let ref_offset: usize = ref_offset
                .try_into()
                .map_err(|e| record_err(&format!("bad ref offset {ref_offset}: {e}")))?;
            let ref_index = i
                .checked_sub(ref_offset + 1)
                .ok_or_else(|| record_err(&format!("ref offset {ref_offset} out of range")))?;
            let ref_record = &result[ref_index];
            let mut log_record = LogRecord {
                timestamp,
                id,
                ..ref_record.clone()
            };
            match &mut log_record.record_type {
                RecordType::Normal(log_entry) | RecordType::Keep(log_entry) => {
                    log_entry.value = get_field(src, LogField::Value);
                    log_entry.user_id = get_field(src, LogField::UserId);
                    log_entry.repeat = get_field(src, LogField::Repeat).unwrap_or_default();
                }
                _ => {}
            }
            log_record
        } else {
            let type_id: i64 = get_field_checked(src, LogField::Type)
                .map_err(|e| record_err(&format!("wrong `typeId` type: {e}")))?
                .ok_or_else(|| format!("Record id: {id}, idx: {i}, missing typeId"))?;

            let record_type = match type_id {
                1 | 2 => {
                    let entry = LogEntry {
                        path: get_field_checked(src, LogField::Path)
                            .map_err(|e| record_err(&format!("wrong `path` type: {e}")))?
                            .unwrap_or_default(),
                        signal: get_field_checked(src, LogField::Signal)
                            .map_err(|e| record_err(&format!("wrong `signal` type: {e}")))?
                            .unwrap_or_else(|| "chng".into()),
                        source: get_field_checked(src, LogField::Source)
                            .map_err(|e| record_err(&format!("wrong `source` type: {e}")))?
                            .unwrap_or_else(|| "get".into()),
                        value: get_field(src, LogField::Value),
                        access_level: get_field_checked(src, LogField::AccessLevel)
                            .map_err(|e| record_err(&format!("wrong `accessLevel` type: {e}")))?
                            .unwrap_or(shvrpc::metamethod::AccessLevel::Read as _),
                        user_id: get_field_checked(src, LogField::UserId)
                            .map_err(|e| record_err(&format!("wrong `userId` type: {e}")))?,
                        repeat: get_field_checked(src, LogField::Repeat)
                            .map_err(|e| record_err(&format!("wrong `repeat` type: {e}")))?
                            .unwrap_or_default(),
                    };
                    if type_id == 1 {
                        RecordType::Normal(entry)
                    } else {
                        RecordType::Keep(entry)
                    }
                }
                3 => {
                    let offset = get_field_checked(src, LogField::TimeJump)
                        .map_err(|e| record_err(&format!("wrong `timeJump` type: {e}")))?
                        .ok_or_else(|| record_err("undefined time jump offset"))?;
                    RecordType::TimeJump(offset)
                }
                4 => RecordType::TimeAmbig,
                _ => return Err(record_err(&format!("wrong record type: {type_id}")).into()),
            };
            LogRecord {
                id,
                timestamp,
                record_type,
            }
        };

        result.push(log_record);
    }

    Ok(result)
}
