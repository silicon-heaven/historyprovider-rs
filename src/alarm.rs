use shvproto::{Map as RpcMap, RpcValue};

use crate::typeinfo::{FieldDescriptionMethods, PathInfo, Type, TypeDescriptionMethods, TypeInfo};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Alarm {
    pub path: String,
    pub is_active: bool,
    pub description: String,
    pub label: String,
    pub level: i32,
    // FIXME: maybe use a log::Level
    pub severity: Severity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Invalid,
    Fatal,
    Error,
    Warning,
    Info,
    Message,
    Debug,
}

impl From<i32> for Severity {
    fn from(value: i32) -> Self {
        match value {
            v if v == Self::Fatal as _ => Self::Fatal,
            v if v == Self::Error as _ => Self::Error,
            v if v == Self::Warning as _ => Self::Warning,
            v if v == Self::Info as _ => Self::Info,
            v if v == Self::Message as _ => Self::Message,
            v if v == Self::Debug as _ => Self::Debug,
            _ => Self::Invalid,
        }
    }
}

impl From<&str> for Severity {
    fn from(value: &str) -> Self {
        match value.chars().next().map(|c| c.to_ascii_lowercase()) {
            Some('d') => Self::Debug,
            Some('m') => Self::Message,
            Some('i') => Self::Info,
            Some('w') => Self::Warning,
            Some('e') => Self::Error,
            Some('f') => Self::Fatal,
            _ => Self::Invalid,
        }
    }
}

impl Alarm {
    pub fn is_valid(&self) -> bool {
        !self.path.is_empty()
    }

    pub fn from_rpc_map(map: RpcMap) -> Self {
        fn extract_key_or_default<T>(map: &RpcMap, key: impl AsRef<str>) -> T
        where
            T: for<'a> TryFrom<&'a RpcValue> + Default
        {
            map.get(key.as_ref())
                .and_then(|v| T::try_from(v).ok())
                .unwrap_or_default()
        }
        Self {
            path: extract_key_or_default(&map, "path"),
            is_active: extract_key_or_default(&map, "isActive"),
            description: extract_key_or_default(&map, "description"),
            label: extract_key_or_default(&map, "label"),
            level: extract_key_or_default(&map, "alarmLevel"),
            severity: extract_key_or_default::<i32>(&map, "severity").into(),
        }
    }

    pub fn into_rpc_map(self, all_fields_if_not_active: bool) -> RpcMap {
        let mut ret = RpcMap::new();

        ret.insert("path".to_string(), self.path.into());
        ret.insert("isActive".to_string(), self.is_active.into());

        if all_fields_if_not_active || self.is_active {
            if self.severity != Severity::Invalid {
                ret.insert("severity".to_string(), (self.severity as i32).into());
                // ret.insert("severityName".to_string(), self.severity_name().into());
            }
            if self.level > 0 {
                ret.insert("alarmLevel".to_string(), self.level.into());
            }
            if !self.description.is_empty() {
                ret.insert("description".to_string(), self.description.into());
            }
            if !self.label.is_empty() {
                ret.insert("label".to_string(), self.label.into());
            }
        }
        ret
    }
}

impl TryFrom<RpcValue> for Alarm {
    type Error = String;

    fn try_from(value: RpcValue) -> Result<Self, Self::Error> {
        let map: RpcMap = value.try_into()?;
        Ok(Self::from_rpc_map(map))
    }
}

impl TryFrom<&RpcValue> for Alarm {
    type Error = String;

    fn try_from(value: &RpcValue) -> Result<Self, Self::Error> {
        value.clone().try_into()
    }
}

impl From<Alarm> for RpcValue {
    fn from(value: Alarm) -> Self {
        value.into_rpc_map(false).into()
    }
}

trait AlarmGetter {
    fn alarm_getter(f: &impl FieldDescriptionMethods) -> Option<&str>;
}

struct CommonAlarm;
impl AlarmGetter for CommonAlarm {
    fn alarm_getter(f: &impl FieldDescriptionMethods) -> Option<&str> {
        f.alarm()
    }
}

struct StateAlarm;
impl AlarmGetter for StateAlarm {
    fn alarm_getter(f: &impl FieldDescriptionMethods) -> Option<&str> {
        f.state_alarm()
    }
}

pub fn collect_alarms(type_info: &TypeInfo, shv_path: impl AsRef<str>, value: &RpcValue) -> Vec<Alarm> {
    impl_collect_alarms::<CommonAlarm>(type_info, shv_path, value)
}

pub fn collect_state_alarms(type_info: &TypeInfo, shv_path: impl AsRef<str>, value: &RpcValue) -> Vec<Alarm> {
    impl_collect_alarms::<StateAlarm>(type_info, shv_path, value)
}

fn impl_collect_alarms<Getter: AlarmGetter>(type_info: &TypeInfo, shv_path: impl AsRef<str>, value: &RpcValue) -> Vec<Alarm> {
    if value.is_null() {
        // value not available, keep previous alarms active
        return vec![];
    }

    let shv_path = shv_path.as_ref();
    let PathInfo { property_description, .. } = type_info.path_info(shv_path);
    if !property_description.is_valid() {
        return vec![];
    }

    if let Some(alarm) = Getter::alarm_getter(&property_description) && !alarm.is_empty() {
        vec![
            Alarm {
                path: shv_path.into(),
                is_active: value.as_bool(),
                description: property_description.description().unwrap_or_default().into(),
                label: property_description.label().unwrap_or_default().into(),
                level: property_description.alarm_level().unwrap_or_default(),
                severity: alarm.into(),
            }
        ]
    } else {
        collect_alarms_for_type::<Getter>(type_info, shv_path, property_description.type_name().unwrap_or_default(), value)
    }
}

fn collect_alarms_for_type<Getter: AlarmGetter>(type_info: &TypeInfo, shv_path: impl AsRef<str>, type_name: impl AsRef<str>, value: &RpcValue) -> Vec<Alarm> {
    let Some(type_descr) = type_info.find_type_description(type_name).filter(|descr| descr.is_valid()) else {
        return vec![]
    };

    let shv_path = shv_path.as_ref();
    match type_descr.type_id() {
        Some(Type::BitField) => {
            type_descr.fields().iter()
                .flat_map(|fld_descr| {
                    let sub_path = format!("{shv_path}/{fld_descr_name}", fld_descr_name = fld_descr.name());
                    let bitfield_value = fld_descr.bitfield_value(value.as_u64());
                    if let Some(alarm) = Getter::alarm_getter(fld_descr).filter(|alarm| !alarm.is_empty()) {
                        vec![
                            Alarm {
                                path: sub_path,
                                is_active: bitfield_value != 0,
                                description: fld_descr.description().unwrap_or_default().into(),
                                label: fld_descr.label().unwrap_or_default().into(),
                                level: fld_descr.alarm_level().unwrap_or_default(),
                                severity: alarm.into(),
                            }
                        ]
                    } else {
                        collect_alarms_for_type::<Getter>(
                            type_info,
                            sub_path,
                            fld_descr.type_name().unwrap_or_default(),
                            &RpcValue::from(bitfield_value),
                        )
                    }
                })
                .collect()
        }
        Some(Type::Enum) => {
            let fields = type_descr.fields();

            let has_alarm_definition = fields
                .iter()
                .any(|field| Getter::alarm_getter(field)
                    .is_some_and(|f| !f.is_empty())
                );
            if !has_alarm_definition {
                return vec![];
            }

            let active_alarm_field = fields
                .into_iter()
                .find(|field| Getter::alarm_getter(field).is_some_and(|alarm| !alarm.is_empty())
                    && field.bit_range().is_some_and(|bit_range| bit_range.as_u64() == value.as_u64()));
            match active_alarm_field {
                Some(field) => vec![
                    Alarm {
                        path: shv_path.into(),
                        is_active: true,
                        description: field.description().unwrap_or_default().into(),
                        label: field.label().unwrap_or_default().into(),
                        level: field.alarm_level().unwrap_or_default(),
                        severity: Getter::alarm_getter(&field).unwrap_or_default().into(),
                    }
                ],
                None => vec![
                    Alarm {
                        path: shv_path.into(),
                        is_active: false,
                        description: String::new(),
                        label: String::new(),
                        level: 0,
                        severity: Severity::Invalid,
                    }
                ],
            }
        }
        _ => vec![],
    }
}

#[cfg(test)]
mod tests {
    use shvproto::RpcValue;

    use crate::alarm::{collect_alarms, Severity};
    use crate::typeinfo::TypeInfo;

    const TYPE_INFO: &str = r#"
<"version":4>{
    "deviceDescriptions":{
        "device1":{
            "properties":[
                {
                    "name":"status1",
                    "typeName":"BitField"
                },
                {
                    "name":"status2",
                    "typeName":"Map"
                },
                {
                    "name":"status3",
                    "typeName":"Enum"
                },
            ]
        },
    },
    "devicePaths":{
        "foo/bar":"device1",
    },
    "types":{
        "BitField":{
            "fields":[
                {"alarm":"warning", "description":"Alarm 1", "label":"Alarm 1 label", "name":"field1", "value": [0,7]},
                {"alarm":"error", "description":"Alarm 2", "label":"Alarm 2 label", "name":"field2", "value": 24 },
                {"name":"field3", "value": [25, 26], "typeName": "Enum" },
            ],
            "typeName":"BitField"
        },
        "Map":{
            "fields":[
                {"description":"Description 1", "label":"Label 1", "name":"mapField1", "typeName":"Int"},
                {"description":"Description 2", "label":"Label 2", "name":"mapField2", "typeName":"String"},
            ],
            "typeName":"Map",
            "sampleType":"Discrete"
        },
        "Enum":{
            "fields":[
                {"description":"", "label":"", "name":"Unknown", "value":0},
                {"description":"", "label":"", "name":"Normal", "value":1},
                {
                    "alarm":"warning",
                    "alarmLevel":50,
                    "description":"Warning description",
                    "label":"Warning label",
                    "name":"Warning",
                    "value":2
                },
                {
                    "alarm":"error",
                    "alarmLevel":100,
                    "description":"Error description",
                    "label":"Error label",
                    "name":"Error",
                    "value":3
                }
            ],
            "typeName":"Enum"
        },
    }
}
"#;

    #[test]
    fn alarms() {
        let rv = RpcValue::from_cpon(TYPE_INFO).unwrap_or_else(|e| panic!("Cannot parse typeInfo: {e}"));
        let type_info = TypeInfo::try_from(&rv).unwrap_or_else(|e| panic!("Cannot convert RpcValue to TypeInfo: {e}"));

        let alarms = collect_alarms(&type_info, "foo/bar/status1", &RpcValue::from(1_u64 << 24));
        println!("{alarms:?}");
        assert_eq!(alarms.len(), 3);
        let alarm1 = &alarms[0];
        assert!(!alarm1.is_active);
        assert_eq!(alarm1.path, "foo/bar/status1/field1".to_owned());
        assert_eq!(alarm1.severity, Severity::Warning);
        assert_eq!(alarm1.level, 0);
        assert_eq!(alarm1.label, "Alarm 1 label".to_owned());
        assert_eq!(alarm1.description, "Alarm 1".to_owned());

        let alarm2 = &alarms[1];
        assert!(alarm2.is_active);
        assert_eq!(alarm2.path, "foo/bar/status1/field2".to_owned());
        assert_eq!(alarm2.severity, Severity::Error);
        assert_eq!(alarm2.level, 0);
        assert_eq!(alarm2.label, "Alarm 2 label".to_owned());
        assert_eq!(alarm2.description, "Alarm 2".to_owned());

        let alarms = collect_alarms(&type_info, "foo/bar/status1", &RpcValue::from((3 << 25) | (1 << 1)));
        println!("{alarms:?}");
        assert_eq!(alarms.len(), 3);
        let alarm1 = &alarms[0];
        assert!(alarm1.is_active);
        assert_eq!(alarm1.path, "foo/bar/status1/field1".to_owned());
        assert_eq!(alarm1.severity, Severity::Warning);
        assert_eq!(alarm1.level, 0);
        assert_eq!(alarm1.label, "Alarm 1 label".to_owned());
        assert_eq!(alarm1.description, "Alarm 1".to_owned());

        let alarm2 = &alarms[1];
        assert!(!alarm2.is_active);
        assert_eq!(alarm2.path, "foo/bar/status1/field2".to_owned());
        assert_eq!(alarm2.severity, Severity::Error);
        assert_eq!(alarm2.level, 0);
        assert_eq!(alarm2.label, "Alarm 2 label".to_owned());
        assert_eq!(alarm2.description, "Alarm 2".to_owned());

        let alarm3 = &alarms[2];
        assert!(alarm3.is_active);
        assert_eq!(alarm3.path, "foo/bar/status1/field3".to_owned());
        assert_eq!(alarm3.severity, Severity::Error);
        assert_eq!(alarm3.level, 100);
        assert_eq!(alarm3.label, "Error label".to_owned());
        assert_eq!(alarm3.description, "Error description".to_owned());

        let alarms = collect_alarms(&type_info, "foo/bar/status3", &RpcValue::from(0));
        println!("{alarms:?}");
        assert_eq!(alarms.len(), 1);
        let alarm = &alarms[0];
        assert!(!alarm.is_active);
        assert_eq!(alarm.path, "foo/bar/status3".to_owned());
        assert_eq!(alarm.severity, Severity::Invalid);
        assert_eq!(alarm.level, 0);

        let alarms = collect_alarms(&type_info, "foo/bar/status3", &RpcValue::from(2));
        println!("{alarms:?}");
        assert_eq!(alarms.len(), 1);
        let alarm = &alarms[0];
        assert!(alarm.is_active);
        assert_eq!(alarm.path, "foo/bar/status3".to_owned());
        assert_eq!(alarm.severity, Severity::Warning);
        assert_eq!(alarm.level, 50);
        assert_eq!(alarm.label, "Warning label".to_owned());
        assert_eq!(alarm.description, "Warning description".to_owned());

        let alarms = collect_alarms(&type_info, "foo/bar/status3", &RpcValue::from(3));
        println!("{alarms:?}");
        assert_eq!(alarms.len(), 1);
        let alarm = &alarms[0];
        assert!(alarm.is_active);
        assert_eq!(alarm.path, "foo/bar/status3".to_owned());
        assert_eq!(alarm.severity, Severity::Error);
        assert_eq!(alarm.level, 100);
        assert_eq!(alarm.label, "Error label".to_owned());
        assert_eq!(alarm.description, "Error description".to_owned());
    }

}
