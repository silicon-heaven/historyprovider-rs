use shvproto::RpcValue;

#[derive(Debug, Clone, PartialEq)]
pub struct JournalEntry {
    pub epoch_msec: i64,
    pub path: String,
    pub value: RpcValue,
    pub short_time: i32,
    pub domain: String,
    pub value_flags: u32,
    pub user_id: String,
}

// impl ShvJournalEntry {
//     // Constants
//     pub const DOMAIN_VAL_CHANGE: &'static str = Rpc::SIG_VAL_CHANGED;
//     pub const DOMAIN_VAL_FASTCHANGE: &'static str = Rpc::SIG_VAL_FASTCHANGED;
//     pub const DOMAIN_VAL_SERVICECHANGE: &'static str = Rpc::SIG_SERVICE_VAL_CHANGED;
//     pub const DOMAIN_SHV_SYSTEM: &'static str = "SHV_SYS";
//     pub const DOMAIN_SHV_COMMAND: &'static str = Rpc::SIG_COMMAND_LOGGED;
//
//     pub const PATH_APP_START: &'static str = "APP_START";
//     pub const PATH_DATA_MISSING: &'static str = "DATA_MISSING";
//     pub const PATH_DATA_DIRTY: &'static str = "DATA_DIRTY";
//
//     pub const DATA_MISSING_UNAVAILABLE: &'static str = "Unavailable";
//     pub const DATA_MISSING_NOT_EXISTS: &'static str = "NotExists";
//
//     pub const NO_SHORT_TIME: i32 = DataChange::NO_SHORT_TIME;
//     pub const NO_VALUE_FLAGS: u32 = DataChange::NO_VALUE_FLAGS;
//
//     // Constructors
//     pub fn new(
//         path: String,
//         value: RpcValue,
//         domain: String,
//         short_time: i32,
//         value_flags: u32,
//         epoch_msec: i64,
//     ) -> Self {
//         ShvJournalEntry {
//             path,
//             value,
//             domain,
//             short_time,
//             value_flags,
//             epoch_msec,
//             user_id: String::new(),
//         }
//     }
//
//     pub fn from_path_value(path: String, value: RpcValue) -> Self {
//         ShvJournalEntry::new(
//             path,
//             value,
//             String::new(),
//             Self::NO_SHORT_TIME,
//             Self::NO_VALUE_FLAGS,
//             0,
//         )
//     }
//
//     // Methods
//     pub fn is_valid(&self) -> bool {
//         !self.path.is_empty()
//     }
//
//     pub fn is_spontaneous(&self) -> bool {
//         (self.value_flags & DataChange::FLAG_SPONTANEOUS) != 0
//     }
//
//     pub fn set_spontaneous(&mut self, val: bool) {
//         if val {
//             self.value_flags |= DataChange::FLAG_SPONTANEOUS;
//         } else {
//             self.value_flags &= !DataChange::FLAG_SPONTANEOUS;
//         }
//     }
//
//     pub fn is_snapshot_value(&self) -> bool {
//         (self.value_flags & DataChange::FLAG_SNAPSHOT) != 0
//     }
//
//     pub fn set_snapshot_value(&mut self, val: bool) {
//         if val {
//             self.value_flags |= DataChange::FLAG_SNAPSHOT;
//         } else {
//             self.value_flags &= !DataChange::FLAG_SNAPSHOT;
//         }
//     }
//
//     pub fn set_short_time(&mut self, short_time: i32) {
//         self.short_time = short_time;
//     }
//
//     pub fn date_time(&self) -> Option<DateTime> {
//         RpcValue::date_time_from_epoch_msec(self.epoch_msec)
//     }
//
//     pub fn to_rpc_value_map(&self) -> RpcValue {
//         // Assuming implementation of conversion exists
//         RpcValue::from_journal_entry_map(self)
//     }
//
//     pub fn to_rpc_value_list<F>(&self, map_path: Option<F>) -> RpcValue
//     where
//         F: Fn(&str) -> RpcValue,
//     {
//         RpcValue::from_journal_entry_list(self, map_path)
//     }
//
//     pub fn to_data_change(&self) -> DataChange {
//         DataChange::from_journal_entry(self)
//     }
//
//     // Static methods
//     pub fn is_shv_journal_entry(rv: &RpcValue) -> bool {
//         // Some condition to check type
//         rv.is_journal_entry()
//     }
//
//     pub fn from_rpc_value(rv: &RpcValue) -> Option<Self> {
//         rv.to_journal_entry()
//     }
//
//     pub fn from_rpc_value_map(map: &RpcMap) -> Option<Self> {
//         RpcValue::journal_entry_from_map(map)
//     }
//
//     pub fn from_rpc_value_list<F>(row: &RpcList, unmap_path: Option<F>) -> Option<Self>
//     where
//         F: Fn(&RpcValue) -> String,
//     {
//         RpcValue::journal_entry_from_list(row, unmap_path)
//     }
// }
