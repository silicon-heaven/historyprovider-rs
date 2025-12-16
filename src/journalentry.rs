use shvclient::shvproto::RpcValue;

#[derive(Debug, Clone, PartialEq)]
pub struct JournalEntry {
    pub epoch_msec: i64,
    pub path: String,
    pub signal: String,
    pub source: String,
    pub value: RpcValue,
    pub access_level: i32,
    pub short_time: i32,
    pub user_id: Option<String>,
    pub repeat: bool,
    pub provisional: bool,
}
