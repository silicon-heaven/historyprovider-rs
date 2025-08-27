use std::collections::BTreeMap;

use log::debug;
use log::warn;
use shvproto::RpcValue;
use shvproto::Map as RpcMap;
use shvrpc::metamethod::AccessLevel;
use shvrpc::metamethod::DirAttribute;

pub const KEY_DEVICE_TYPE: &str = "deviceType";
pub const KEY_TYPE_NAME: &str = "typeName";
pub const KEY_LABEL: &str = "label";
pub const KEY_DESCRIPTION: &str = "description";
pub const KEY_UNIT: &str = "unit";
pub const KEY_NAME: &str = "name";
pub const KEY_TYPE: &str = "type";
pub const KEY_VALUE: &str = "value";
pub const KEY_FIELDS: &str = "fields";
pub const KEY_SAMPLE_TYPE: &str = "sampleType";
pub const KEY_RESTRICTION_OF_DEVICE: &str = "restrictionOfDevice";
pub const KEY_RESTRICTION_OF_TYPE: &str = "restrictionOfType";
pub const KEY_SITE_SPECIFIC_LOCALIZATION: &str = "siteSpecificLocalization";
pub const KEY_TAGS: &str = "tags";
pub const KEY_METHODS: &str = "methods";
pub const KEY_BLACKLIST: &str = "blacklist";
pub const KEY_DEC_PLACES: &str = "decPlaces";
pub const KEY_VISUAL_STYLE: &str = "visualStyle";
pub const KEY_ALARM: &str = "alarm";
pub const KEY_ALARM_LEVEL: &str = "alarmLevel";

fn merge_tags(mut map: RpcMap) -> RpcMap {
    if let Some(rv) = map.remove(KEY_TAGS)
        && let shvproto::Value::Map(tags) = rv.value {
            map.extend(*tags);
    }
    map
}

fn set_data_value(descr: &mut (impl TypeDescriptionMethods + ?Sized), key: impl Into<String>, value: impl Into<RpcValue>) {
    descr.data_mut().insert(key.into(), value.into());
}

pub trait TypeDescriptionMethods {
    fn data(&self) -> &RpcMap;
    fn data_mut(&mut self) -> &mut RpcMap;

    fn data_value(&self, key: impl AsRef<str>) -> Option<&RpcValue> {
        self.data().get(key.as_ref())
    }

    fn data_value_into<'a, T>(&'a self, key: impl AsRef<str>) -> Option<T>
    where
        T: TryFrom<&'a RpcValue>,
    {
        self.data_value(key).and_then(|v| <T>::try_from(v).ok())
    }

    fn is_valid(&self) -> bool {
        !self.data().is_empty()
    }

    fn type_id(&self) -> Option<Type> {
        self.data_value_into::<i32>(KEY_TYPE).and_then(Type::try_from_i32)
    }

    fn set_type_id(&mut self, type_id: Type) {
        set_data_value(self, KEY_TYPE, type_id as i32);
    }

    fn type_name(&self) -> Option<&str> {
        if let Some(tn) = self.data_value_into::<&str>(KEY_TYPE_NAME) && !tn.is_empty() {
            Some(tn)
        } else {
            self.type_id().as_ref().map(Type::as_str)
        }
    }

    fn set_type_name(&mut self, type_name: impl AsRef<str>) {
        set_data_value(self, KEY_TYPE_NAME, type_name.as_ref());
        if self.type_id().is_none() && let Some(new_type_id) = Type::try_from_str(type_name.as_ref()) {
            set_data_value(self, KEY_TYPE, new_type_id as i32);
        }
    }

    fn sample_type(&self) -> Option<SampleType> {
        self.data_value_into::<i32>(KEY_SAMPLE_TYPE).and_then(SampleType::try_from_i32)
    }

    fn set_sample_type(&mut self, sample_type_id: SampleType) {
        set_data_value(self, KEY_SAMPLE_TYPE, sample_type_id as i32);
    }

    fn fields(&self) -> Vec<FieldDescription> {
        self.data_value_into::<Vec<FieldDescription>>(KEY_FIELDS).unwrap_or_default()
    }

    fn set_fields(&mut self, fields: Vec<FieldDescription>) {
        set_data_value(self, KEY_FIELDS, fields);
    }

    fn bit_range(&self) -> Option<&RpcValue> {
        self.data_value(KEY_VALUE)
    }

    fn bit_range_pair(&self) -> (u64, u64) {
        let Some(rv) = self.bit_range() else {
            return (0, 0);
        };

        if rv.is_list() {
            let mut it = Vec::<u64>::try_from(rv).unwrap_or_default().into_iter();
            let (b1, b2) = (it.next().unwrap_or_default(), it.next().unwrap_or_default());
            if b2 < b1 {
                warn!("Invalid bit specification: {val}", val = rv.to_cpon());
                (b1, b1)

            } else {
                (b1, b2)
            }
        } else {
            let val = rv.as_u64();
            (val, val)
        }
    }

    fn bitfield_value(&self, val: u64) -> u64 {
        if !self.is_valid() {
            return 0;
        }
        let (b1, b2) = self.bit_range_pair();
        let mask = if b2 >= 63 {
            u64::MAX
        } else {
            (1u64 << (b2 + 1)) - 1
        };

        debug!("bits: {b1} {b2} val: {val:#x} mask: {mask:#x}");

        let new_val = if b1 > 63 {
            0
        } else {
            (val & mask) >> b1
        };
        debug!("val masked and rotated right by: {b1} bits, new_val: {new_val:#x}");

        if b1 == b2 {
            (new_val != 0).into()
        } else {
            new_val
        }
    }

    fn set_bitfield_value(&self, bitfield: u64, val: u64) -> u64 {
        if !self.is_valid() {
            return val;
        }

        let (b1, b2) = self.bit_range_pair();

        // Width of the field in bits
        let width = b2.saturating_sub(b1) + 1;

        // Safe mask: width == 64 → all ones, else (1 << width) - 1
        let mask = if width >= 64 {
            u64::MAX
        } else {
            (1u64 << width) - 1
        };

        // Keep only the bits that fit into the field
        let val = val & mask;

        // Shift mask and value into correct position
        let shifted_mask = if b1 >= 64 {
            0 // shifting by 64 would overflow
        } else {
            mask << b1
        };

        let shifted_val = if b1 >= 64 {
            0
        } else {
            val << b1
        };

        // Clear the field and insert new value
        let mut bitfield = bitfield & !shifted_mask;
        bitfield |= shifted_val;
        bitfield
    }

    fn decimal_places(&self) -> Option<i64> {
        self.data_value_into(KEY_DEC_PLACES)
    }

    fn set_decimal_places(&mut self, n: i64) {
        set_data_value(self, KEY_DEC_PLACES, n);
    }

    fn unit(&self) -> Option<&str> {
        self.data_value_into(KEY_UNIT)
    }

    fn set_unit(&mut self, unit: impl AsRef<str>) {
        set_data_value(self, KEY_UNIT, unit.as_ref());
    }

    fn label(&self) -> Option<&str> {
        self.data_value_into(KEY_LABEL)
    }

    fn set_label(&mut self, label: impl AsRef<str>) {
        set_data_value(self, KEY_LABEL, label.as_ref());
    }

    fn description(&self) -> Option<&str> {
        self.data_value_into(KEY_DESCRIPTION)
    }

    fn set_description(&mut self, description: impl AsRef<str>) {
        set_data_value(self, KEY_DESCRIPTION, description.as_ref());
    }

    fn visual_style_name(&self) -> Option<&str> {
        self.data_value_into(KEY_VISUAL_STYLE)
    }

    fn set_visual_style_name(&mut self, visual_style_name: impl AsRef<str>) {
        set_data_value(self, KEY_VISUAL_STYLE, visual_style_name.as_ref());
    }

    fn restriction_of_type(&self) -> Option<&str> {
        self.data_value_into(KEY_RESTRICTION_OF_TYPE)
    }

    fn is_site_specific_localization(&self) -> bool {
        self.data_value_into(KEY_SITE_SPECIFIC_LOCALIZATION).unwrap_or_default()
    }

    fn field(&self, field_name: impl AsRef<str>) -> Option<FieldDescription> {
        self.data_value_into::<shvproto::List>(KEY_FIELDS)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|rpc_value| FieldDescription::try_from(rpc_value).ok())
            .find(|descr| descr.name() == field_name.as_ref())
    }

    fn field_value(&self, val: impl Into<RpcValue>, field_name: impl AsRef<str>) -> Option<RpcValue> {
        let val = val.into();
        match self.type_id()? {
            Type::BitField => {
                self.field(field_name).map(|descr| descr.bitfield_value(val.as_u64()).into())
            }
            Type::Map => {
                if let shvproto::Value::Map(map) = &val.value {
                    map.get(field_name.as_ref()).cloned()
                } else {
                    None
                }
            }
            Type::Enum => {
                self.field(field_name)
                    .and_then(|v| v
                        .bit_range()
                        .and_then(|v| <i64>::try_from(v)
                            .is_ok()
                            .then(|| v.clone())
                        )
                    )
            }
            _ => None,
        }
    }
}

pub trait FieldDescriptionMethods: TypeDescriptionMethods {
    fn name(&self) -> &str {
        self.data_value(KEY_NAME).map_or("", RpcValue::as_str)
    }
	fn set_name(&mut self, name: impl AsRef<str>) {
        set_data_value(self, KEY_NAME, name.as_ref());
    }
	fn alarm(&self) -> Option<&str> {
        self.data_value(KEY_ALARM).map(RpcValue::as_str)
    }
	fn set_alarm(&mut self, alarm: impl AsRef<str>) {
        set_data_value(self, KEY_ALARM, alarm.as_ref());
    }
	fn alarm_level(&self) -> Option<i32> {
        self.data_value(KEY_ALARM_LEVEL).map(RpcValue::as_i32)
    }
}

// impl<T: FieldDescriptionMethods> TypeDescriptionMethods for T { }

#[derive(PartialEq)]
pub enum Type {
    BitField = 1,
    Enum,
    Bool,
    UInt,
    Int,
    Decimal,
    Double,
    String,
    DateTime,
    List,
    Map,
    IMap,
}

impl Type {
    pub fn as_str(&self) -> &'static str {
        match self {
            Type::BitField  => "BitField",
            Type::Enum      => "Enum",
            Type::Bool      => "Bool",
            Type::UInt      => "UInt",
            Type::Int       => "Int",
            Type::Decimal   => "Decimal",
            Type::Double    => "Double",
            Type::String    => "String",
            Type::DateTime  => "DateTime",
            Type::List      => "List",
            Type::Map       => "Map",
            Type::IMap      => "IMap",
        }
    }

    pub fn try_from_i32(value: i32) -> Option<Self> {
        match value {
            v if v == Self::BitField as _ => Some(Self::BitField),
            v if v == Self::Enum as _ => Some(Self::Enum),
            v if v == Self::Bool as _ => Some(Self::Bool),
            v if v == Self::UInt as _ => Some(Self::UInt),
            v if v == Self::Int as _ => Some(Self::Int),
            v if v == Self::Decimal as _ => Some(Self::Decimal),
            v if v == Self::Double as _ => Some(Self::Double),
            v if v == Self::String as _ => Some(Self::String),
            v if v == Self::DateTime as _ => Some(Self::DateTime),
            v if v == Self::List as _ => Some(Self::List),
            v if v == Self::Map as _ => Some(Self::Map),
            v if v == Self::IMap as _ => Some(Self::IMap),
            _ => None,
        }
    }

    pub fn try_from_str(s: &str) -> Option<Self> {
        match s {
            "BitField" => Some(Self::BitField),
            "Enum" => Some(Self::Enum),
            "Bool" => Some(Self::Bool),
            "UInt" => Some(Self::UInt),
            "Int" => Some(Self::Int),
            "Decimal" => Some(Self::Decimal),
            "Double" => Some(Self::Double),
            "String" => Some(Self::String),
            "DateTime" => Some(Self::DateTime),
            "List" => Some(Self::List),
            "Map" => Some(Self::Map),
            "IMap" => Some(Self::IMap),
            _ => None,
        }
    }
}

pub enum SampleType {
    Continuous = 1,
    Discrete
}

impl SampleType {
    pub fn as_str(&self) -> &'static str {
        match self {
            SampleType::Continuous => "Continuos",
            SampleType::Discrete => "Discrete",
        }
    }

    pub fn try_from_i32(value: i32) -> Option<Self> {
        match value {
            v if v == Self::Continuous as _ => Some(Self::Continuous),
            v if v == Self::Discrete as _ => Some(Self::Discrete),
            _ => None,
        }
    }

    pub fn try_from_str(s: &str) -> Option<Self> {
        match s {
            "Continuous" => Some(Self::Continuous),
            "Discrete" | "discrete" | "D" | "2" => Some(Self::Discrete),
            _ => None,
        }
    }
}


pub struct TypeDescription {
    data: RpcMap,
}

impl TypeDescription {
    pub fn from_rpc_map(mut map: RpcMap) -> Self {
        // --- Extract fields and merge tags ---
        let src_fields: Vec<RpcValue> = map.remove("fields")
            .and_then(|v| v.try_into().ok())
            .unwrap_or_default();

        let fields = src_fields
            .into_iter()
            .map(|v| merge_tags(RpcMap::try_from(v).unwrap_or_default()))
            .collect::<Vec<_>>();

        let mut map = merge_tags(map);
        map.insert(KEY_FIELDS.into(),fields.into());
        let mut ret = Self { data: map };

        // --- Type name from KEY_TYPE_NAME ---
        if let Some(s) = ret.data_value_into::<String>(KEY_TYPE_NAME) {
            ret.set_type_name(s);
        }

        // --- Obsolete fallbacks ---
        if ret.type_id().is_none()
            && let Some(s) = ret.data_value_into::<String>(KEY_NAME)
        {
            ret.set_type_name(s);
        }
        if ret.type_id().is_none()
            && let Some(s) = ret.data_value_into::<String>(KEY_TYPE)
        {
            ret.set_type_name(s);
        }

        // --- Sample type ---
        if let Some(s) = ret.data_value_into::<&str>(KEY_SAMPLE_TYPE)
            && let Some(sample_type) = SampleType::try_from_str(s) {
                ret.set_sample_type(sample_type);
        }

        if ret.is_valid() && ret.sample_type().is_none() {
            ret.set_sample_type(SampleType::Continuous);
        }

        ret
    }

    pub fn new(t: Type, fields: Vec<FieldDescription>, sample_type: SampleType, tags: RpcMap) -> Self {
        let mut ret = Self { data: tags };
        ret.set_type_id(t);
        ret.set_fields(fields);
        ret.set_sample_type(sample_type);
        ret
    }
}

impl TypeDescriptionMethods for TypeDescription {
    fn data(&self) -> &RpcMap {
        &self.data
    }
    fn data_mut(&mut self) -> &mut RpcMap {
        &mut self.data
    }
}

impl TryFrom<RpcValue> for TypeDescription {
    type Error = String;

    fn try_from(value: RpcValue) -> Result<Self, Self::Error> {
        let map: RpcMap = value.try_into()?;
        Ok(Self::from_rpc_map(map))
    }
}

impl TryFrom<&RpcValue> for TypeDescription {
    type Error = String;

    fn try_from(value: &RpcValue) -> Result<Self, Self::Error> {
        value.clone().try_into()
    }
}

// impl From<TypeDescription> for RpcValue {
//     fn from(value: TypeDescription) -> Self {
//         value.into_rpc_map().into()
//     }
// }

#[derive(Clone, Default)]
pub struct FieldDescription {
    data: RpcMap,
}

impl FieldDescription {
    pub fn from_rpc_map(map: RpcMap) -> Self {
        Self { data: merge_tags(map) }
    }

    pub fn into_rpc_map(self) -> RpcMap {
        let is_descr_empty = self.description().is_none_or(str::is_empty);
        let mut ret = self.data;
        if is_descr_empty {
            ret.remove(KEY_DESCRIPTION);
        }
        ret
    }
}

impl From<FieldDescription> for RpcValue {
    fn from(value: FieldDescription) -> Self {
        value.into_rpc_map().into()
    }
}

impl TryFrom<RpcValue> for FieldDescription {
    type Error = String;

    fn try_from(value: RpcValue) -> Result<Self, Self::Error> {
        value.try_into().map(Self::from_rpc_map)
    }
}

impl TryFrom<&RpcValue> for FieldDescription {
    type Error = String;

    fn try_from(value: &RpcValue) -> Result<Self, Self::Error> {
        value.to_owned().try_into()
    }
}

impl TypeDescriptionMethods for FieldDescription {
    fn data(&self) -> &RpcMap {
        &self.data
    }
    fn data_mut(&mut self) -> &mut RpcMap {
        &mut self.data
    }
}
impl FieldDescriptionMethods for FieldDescription { }

#[derive(Clone, Default)]
pub struct PropertyDescription {
    base: FieldDescription,
}

impl PropertyDescription {
    // returns (Self, extra tags)
    pub fn from_rpc_map(mut map: RpcMap) -> (Self, RpcMap) {
        const KNOWN_TAGS: &[&str] = &[
            KEY_DEVICE_TYPE,
            // KEY_SUPER_DEVICE_TYPE,
            KEY_NAME,
            KEY_TYPE_NAME,
            KEY_LABEL,
            KEY_DESCRIPTION,
            KEY_UNIT,
            KEY_METHODS,
            "autoload",
            "autorefresh",
            "monitored",
            "monitorOptions",
            KEY_SAMPLE_TYPE,
            KEY_ALARM,
            KEY_ALARM_LEVEL,
        ];

        let node_map = KNOWN_TAGS
            .iter()
            .filter_map(|tag| map.remove(*tag).map(|val| (tag.to_string(), val)))
            .filter(|(tag, _val)| tag != KEY_DEVICE_TYPE)
            .collect::<RpcMap>();

        (Self { base: FieldDescription { data: merge_tags(node_map) }}, map)
    }

    pub fn into_rpc_map(self) -> RpcMap {
        let methods = self.methods();
        let mut res = self.base.into_rpc_map();
        if !methods.is_empty() {
            res.insert(KEY_METHODS.into(), methods.into());
        }
        res
    }

    pub fn methods(&self) -> Vec<MethodDescription> {
        self.data_value_into(KEY_METHODS).unwrap_or_default()
    }

    pub fn method(&self, method_name: impl AsRef<str>) -> Option<MethodDescription> {
        self.data_value_into::<shvproto::List>(KEY_METHODS)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|rpc_value| MethodDescription::try_from(rpc_value).ok())
            .find(|descr| descr.name == method_name.as_ref())
    }

    // pub fn add_method(&mut self, descr: MethodDescription) { }
    // pub fn set_method(&mut self, descr: MethodDescription) { }
}

impl TryFrom<RpcValue> for PropertyDescription {
    type Error = String;

    fn try_from(value: RpcValue) -> Result<Self, Self::Error> {
        let map: RpcMap = value.try_into()?;
        let (res, _) = Self::from_rpc_map(map);
        Ok(res)
    }
}

impl TryFrom<&RpcValue> for PropertyDescription {
    type Error = String;

    fn try_from(value: &RpcValue) -> Result<Self, Self::Error> {
        value.clone().try_into()
    }
}

impl From<PropertyDescription> for RpcValue {
    fn from(value: PropertyDescription) -> Self {
        value.into_rpc_map().into()
    }
}

impl TypeDescriptionMethods for PropertyDescription {
    fn data(&self) -> &RpcMap {
        self.base.data()
    }
    fn data_mut(&mut self) -> &mut RpcMap {
        self.base.data_mut()
    }
}

impl FieldDescriptionMethods for PropertyDescription { }

#[derive(Clone, Debug)]
pub struct MethodDescription {
    pub name: String,
    pub flags: u32,
    pub access: AccessLevel,
    pub param: String,
    pub result: String,
    pub signals: Vec::<(String, Option<String>)>,
    pub description: String,
}

impl Default for MethodDescription {
    fn default() -> Self {
        Self {
            name: Default::default(),
            flags: Default::default(),
            access: AccessLevel::Browse,
            param: Default::default(),
            result: Default::default(),
            signals: Default::default(),
            description: Default::default(),
        }
    }
}

const IKEY_EXTRA: i32 = (DirAttribute::Signals as i32) + 1;

impl TryFrom<RpcValue> for MethodDescription {
    type Error = String;

    fn try_from(value: RpcValue) -> Result<Self, Self::Error> {
        match value.value {
            shvproto::Value::Map(map) => {
                Ok(Self {
                    name: map.get(DirAttribute::Name.into()).map(|rv| rv.as_str().to_string()).unwrap_or_default(),
                    flags: map.get(DirAttribute::Flags.into()).map(|rv| rv.as_u32()).unwrap_or_default(),
                    param: map.get(DirAttribute::Param.into()).map(|rv| rv.as_str().to_string()).unwrap_or_default(),
                    result: map.get(DirAttribute::Result.into()).map(|rv| rv.as_str().to_string()).unwrap_or_default(),
                    access: AccessLevel::try_from(
                            map.get(DirAttribute::AccessLevel.into())
                            .map(|rv| rv.as_i32())
                            .unwrap_or_default()
                        )
                        .unwrap_or(AccessLevel::Browse),
                    signals: {
                        let signals_map: RpcMap = map.get(DirAttribute::Signals.into()).and_then(|rv| rv.try_into().ok()).unwrap_or_default();
                        signals_map.into_iter()
                            .map(|(k,v)| {
                                (k, v.try_into().ok())
                            })
                            .collect()
                    },
                    description: map.get("description")
                        .map(|d| d.as_str().to_string())
                        .unwrap_or_default(),
                })
            }
            shvproto::Value::IMap(imap) => {
                Ok(Self {
                    name: imap.get(&(DirAttribute::Name as _)).map(|rv| rv.as_str().to_string()).unwrap_or_default(),
                    flags: imap.get(&(DirAttribute::Flags as _)).map(|rv| rv.as_u32()).unwrap_or_default(),
                    param: imap.get(&(DirAttribute::Param as _)).map(|rv| rv.as_str().to_string()).unwrap_or_default(),
                    result: imap.get(&(DirAttribute::Result as _)).map(|rv| rv.as_str().to_string()).unwrap_or_default(),
                    access: AccessLevel::try_from(
                            imap.get(&(DirAttribute::AccessLevel as _))
                            .map(|rv| rv.as_i32())
                            .unwrap_or_default()
                        )
                        .unwrap_or(AccessLevel::Browse),
                    signals: {
                        let signals_map: RpcMap = imap.get(&(DirAttribute::Signals as _)).and_then(|rv| rv.try_into().ok()).unwrap_or_default();
                        signals_map.into_iter()
                            .map(|(k,v)| {
                                (k, v.try_into().ok())
                            })
                            .collect()
                    },
                    description: imap.get(&IKEY_EXTRA)
                        .and_then(|extra| extra
                            .get("description")
                            .and_then(|d| d.try_into().ok())
                        ).unwrap_or_default(),
                })
            }
            _ => Err(format!("Unexpected type: {type}", type = value.type_name())),
        }
    }
}


impl TryFrom<&RpcValue> for MethodDescription {
    type Error = String;

    fn try_from(value: &RpcValue) -> Result<Self, Self::Error> {
        value.clone().try_into()
    }
}

impl From<MethodDescription> for RpcValue {
    fn from(value: MethodDescription) -> Self {
        let mut res = shvproto::IMap::new();
        res.insert(DirAttribute::Name.into(), value.name.into());
        res.insert(DirAttribute::Flags.into(), value.flags.into());
        res.insert(DirAttribute::Param.into(), value.param.into());
        res.insert(DirAttribute::Result.into(), value.result.into());
        res.insert(DirAttribute::AccessLevel.into(), (value.access as i32).into());
        res.insert(DirAttribute::Signals.into(), value.signals
            .into_iter()
            .map(|(name, value)| (name, value.map_or_else(RpcValue::null, RpcValue::from)))
            .collect::<BTreeMap<_,_>>()
            .into()
        );
        res.insert(IKEY_EXTRA, shvproto::make_map!("description" => value.description).into());
        res.into()
    }
}

#[derive(Default)]
pub struct DeviceDescription {
    properties: Vec<PropertyDescription>,
	// restriction_of_device: String,
	// site_specific_localization: bool,
}

impl DeviceDescription {
    pub fn from_rpc_map(map: RpcMap) -> Self {
        Self {
            properties: map.get("properties").and_then(|v| v.try_into().ok()).unwrap_or_default()
        }
    }

    pub fn into_rpc_map(self) -> RpcMap {
        shvproto::make_map!("properties" => RpcValue::from(self.properties))
    }

    pub fn find_property(&self, prop_name: impl AsRef<str>) -> Option<&PropertyDescription> {
        self.properties.iter().find(|prop| prop.name() == prop_name.as_ref())
    }

    pub fn find_longest_property_prefix(&self, path: impl AsRef<str>) -> Option<&PropertyDescription> {
        let path = path.as_ref();
        self.properties
            .iter()
            .filter(|prop| shvrpc::util::starts_with_path(path, prop.name()))
            .max_by_key(|prop| prop.name().len())
    }

    pub fn set_property_description(&mut self, descr: PropertyDescription) {
        let prop_name = descr.name();
        if let Some(prop) = self.properties.iter_mut().find(|p| p.name() == prop_name) {
            *prop = descr;
        } else {
            self.properties.push(descr);
        }
    }

    pub fn remove_property_description(&mut self, prop_name: impl AsRef<str>) {
        let prop_name = prop_name.as_ref();
        self.properties.retain(|prop| prop.name() != prop_name);
    }
}

impl TryFrom<RpcValue> for DeviceDescription {
    type Error = String;

    fn try_from(value: RpcValue) -> Result<Self, Self::Error> {
        let map: RpcMap = value.try_into()?;
        Ok(Self::from_rpc_map(map))
    }
}

impl TryFrom<&RpcValue> for DeviceDescription {
    type Error = String;

    fn try_from(value: &RpcValue) -> Result<Self, Self::Error> {
        value.clone().try_into()
    }
}

#[derive(Default)]
pub struct TypeInfo {
    /// type-name -> type-description
    types: BTreeMap<String, TypeDescription>,

    /// path -> device-type-name
    device_paths: BTreeMap<String, String>,

    /// device-type-name -> device-descr
    device_descriptions: BTreeMap<String, DeviceDescription>,

    /// shv-path -> tags
    extra_tags: BTreeMap<String, RpcValue>,

    /// shv-path-root -> system-path
    system_paths_roots: BTreeMap<String, String>,

    /// shv-path -> blacklist
    blacklisted_paths: BTreeMap<String, RpcValue>,

    /// should be empty, devices should not have different property descriptions for same device-type
    property_deviations: BTreeMap<String, PropertyDescription>,
}

#[derive(Default)]
pub struct PathInfo
{
    pub device_path: String,
    pub device_type: String,
    pub property_description: PropertyDescription,
    pub field_path: String,
}

#[derive(Default)]
pub struct DeviceType {
    pub device_path: String,
    pub device_type: String,
    pub property_path: String,
}

fn find_longest_prefix_entry<'a, V>(
    map: &'a BTreeMap<String, V>,
    path: &'a str,
) -> Option<(&'a str, &'a V)> {
    let mut path = path;
    loop {
        if let Some(val) = map.get(path) {
            return Some((path, val));
        }
        if path.is_empty() {
            break;
        }
        if let Some(slash_ix) = path.rfind('/') {
            path = &path[..slash_ix];
        } else {
            path = "";
        };
    }
    None
}

impl TypeInfo {
    const VERSION: &str = "version";
    const PATHS: &str = "paths"; // SHV2
    const TYPES: &str = "types"; // SHV2 + SHV3
    const DEVICE_PATHS: &str = "devicePaths";
    const DEVICE_PROPERTIES: &str = "deviceProperties";
    const DEVICE_DESCRIPTIONS: &str = "deviceDescriptions";
    const PROPERTY_DEVIATIONS: &str = "propertyDeviations";
    const EXTRA_TAGS: &str = "extraTags";
    const SYSTEM_PATHS_ROOTS: &str = "systemPathsRoots";
    const BLACKLISTED_PATHS: &str = "blacklistedPaths";

    pub fn find_type_description(&self, type_name: impl AsRef<str>) -> Option<&TypeDescription> {
        self.types.get(type_name.as_ref())
    }

    pub fn find_property_description(
        &self,
        device_type: impl AsRef<str>,
        property_path: impl AsRef<str>,
    ) -> Option<(PropertyDescription, String)>
    {
        if let Some(dev_descr) = self.device_descriptions.get(device_type.as_ref()) {
            let property_path = property_path.as_ref();
            if let Some(prop_descr) = dev_descr.find_longest_property_prefix(property_path) {
                let own_property_path = prop_descr.name();
                let field_path = property_path.strip_prefix(own_property_path).unwrap_or_default();
                return Some((prop_descr.clone(), field_path.into()));
            }
        }
        None
    }

    pub fn find_device_type(&self, shv_path: impl AsRef<str>) -> Option<DeviceType> {
        let shv_path = shv_path.as_ref();
        if let Some((device_path, device_type)) = find_longest_prefix_entry(&self.device_paths, shv_path) {
            let property_path = if device_path.is_empty() {
                shv_path.to_string()
            } else {
                shv_path.get(device_path.len() + 1 ..).unwrap_or("").to_string()
            };
            Some(DeviceType {
                device_path: device_path.into(),
                device_type: device_type.into(),
                property_path
            })
        } else {
            None
        }
    }

    pub fn path_info(&self, shv_path: impl AsRef<str>) -> PathInfo {
        let mut ret = PathInfo::default();
        let mut deviation_found = false;
        let shv_path = shv_path.as_ref();

        if let Some((own_property_path, prop_descr)) = find_longest_prefix_entry(&self.property_deviations, shv_path) {
            deviation_found = true;
            ret.field_path = shv_path.strip_prefix(own_property_path).unwrap_or_default().into();
            ret.property_description = prop_descr.clone();
        }

        let DeviceType { device_path, device_type, property_path } = self.find_device_type(shv_path).unwrap_or_default();
        ret.device_path = device_path;
        ret.device_type = device_type.clone();

        if deviation_found {
            if ret.property_description.is_valid() {
                let name = shv_path
                    .strip_prefix(&ret.device_path)
                    .and_then(|s| s.strip_suffix(&ret.field_path))
                    .unwrap_or_default();
                ret.property_description.set_name(name);
            }
        } else {
            let (property_descr, field_path) = self.find_property_description(&device_type, &property_path).unwrap_or_default();
            if property_descr.is_valid() {
                ret.field_path = field_path;
                ret.property_description = property_descr;
            }
        }

        ret
    }

    pub fn type_description_for_path(&self, shv_path: impl AsRef<str>) -> Option<&TypeDescription> {
        let PathInfo { property_description, .. } = self.path_info(shv_path);
        property_description.type_name().and_then(|type_name| self.find_type_description(type_name))
    }

    // NOTE: Users should just use path_info and extract only values field_path and property_description
    // pub fn property_description_for_path(shv_path: impl AsRef<str>, std::string *p_field_name) const
}

impl TryFrom<&RpcValue> for TypeInfo {
    type Error = String;

    fn try_from(value: &RpcValue) -> Result<Self, Self::Error> {
        let version = value.meta
            .as_ref()
            .and_then(|m| m.get(Self::VERSION).map(RpcValue::as_i64))
            .unwrap_or_default();

        let map: RpcMap = value.try_into()?;

        let mut res = Self::default();

        if version == 3 || version == 4 {
            res.types = map.get(Self::TYPES).and_then(|rv| rv.try_into().ok()).unwrap_or_default();
            res.device_paths = map.get(Self::DEVICE_PATHS).and_then(|rv| rv.try_into().ok()).unwrap_or_default();

            if version == 3 {
                let m: RpcMap = map.get(Self::DEVICE_PROPERTIES).and_then(|rv| rv.try_into().ok()).unwrap_or_default();
                for(device_type, prop_map) in m {
                    let pm = prop_map.as_map();
                    let dev_descr = res.device_descriptions.entry(device_type).or_insert_with(DeviceDescription::default);
                    for (property_path, property_descr_rv) in pm {
                        let mut property_descr: PropertyDescription = property_descr_rv.try_into().unwrap_or_default();
                        property_descr.set_name(property_path);
                        dev_descr.properties.push(property_descr);
                    }
                }
            }
            else {
                res.device_descriptions = map.get(Self::DEVICE_DESCRIPTIONS).and_then(|rv| rv.try_into().ok()).unwrap_or_default();
            }
            res.extra_tags = map.get(Self::EXTRA_TAGS).and_then(|rv| rv.try_into().ok()).unwrap_or_default();
            res.system_paths_roots = map.get(Self::SYSTEM_PATHS_ROOTS).and_then(|rv| rv.try_into().ok()).unwrap_or_default();
            res.blacklisted_paths = map.get(Self::BLACKLISTED_PATHS).and_then(|rv| rv.try_into().ok()).unwrap_or_default();
            res.property_deviations = map.get(Self::PROPERTY_DEVIATIONS).and_then(|rv| rv.try_into().ok()).unwrap_or_default();
        } else if map.contains_key(Self::PATHS) && map.contains_key(Self::TYPES) {
            // version 2
            res.types = map.get(Self::TYPES).and_then(|rv| rv.try_into().ok()).unwrap_or_default();
            let dev_descr = res.device_descriptions.entry("".into()).or_insert_with(DeviceDescription::default);
            let m: RpcMap = map.get(Self::PATHS).and_then(|rv| rv.try_into().ok()).unwrap_or_default();
            for (path, val) in m {
                let val_map: RpcMap = val.try_into().unwrap_or_default();
                let (mut descr, _) = PropertyDescription::from_rpc_map(val_map.clone());
                descr.set_type_name(val_map.get("type").map(RpcValue::as_str).unwrap_or_default());
                descr.set_name(path);
                dev_descr.properties.push(descr);
            }
        }
        else {
            // res = from_nodes_tree(v);
            // TODO
        }

        if res.system_paths_roots.is_empty() {
            res.system_paths_roots.insert("".into(), "system".into());
        }

        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use shvproto::RpcValue;

    use crate::typeinfo::{FieldDescriptionMethods, SampleType, Type, TypeDescriptionMethods, TypeInfo};
    use std::collections::BTreeMap;
    use std::sync::Once;
    use simple_logger::SimpleLogger;

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
				{"alarm":"warning", "description":"Alarm 1", "label":"Alarm 1 label", "name":"field1", "value": [0,7] },
				{"alarm":"error", "description":"Alarm 2", "label":"Alarm 2 label", "name":"field2", "value": 24 },
				{"name":"field3", "value": [25, 26] },
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
					"description":"",
					"label":"",
					"name":"Warning",
					"value":2
				},
				{
					"alarm":"error",
					"alarmLevel":100,
					"description":"",
					"label":"",
					"name":"Error",
					"value":3
				}
			],
			"typeName":"Enum"
		},
	}
}
"#;


    fn init_logger() {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            SimpleLogger::new()
                .with_level(log::LevelFilter::Debug)
                .init()
                .unwrap();
            });
    }

    #[test]
    fn parse_type_info() {
        init_logger();
        let rv = RpcValue::from_cpon(TYPE_INFO).unwrap_or_else(|e| panic!("Cannot parse typeInfo: {e}"));
        let type_info = TypeInfo::try_from(&rv).unwrap_or_else(|e| panic!("Cannot convert RpcValue to TypeInfo: {e}"));

        let type_descr = type_info.type_description_for_path("foo/bar/status1").unwrap();
        assert!(matches!(type_descr.type_id(), Some(Type::BitField)));
        assert!(type_descr.field_value(0x1234, "field1").is_some_and(|v| v.as_u32() == 0x34));

        let bitfield_type_descr = type_info.find_type_description("BitField").unwrap();
        assert!(bitfield_type_descr.is_valid());
        assert!(matches!(bitfield_type_descr.type_id(), Some(Type::BitField)));
        assert!(bitfield_type_descr.sample_type().is_some_and(|st| matches!(st, SampleType::Continuous)));
        assert_eq!(bitfield_type_descr.field_value(0xfffa, "field1").unwrap().as_u32(), 0xfa);
        assert_eq!(bitfield_type_descr.field_value(0x7fffffff, "field2").unwrap().as_u32(), 1);
        assert_eq!(bitfield_type_descr.field_value(0x7effffff, "field2").unwrap().as_u32(), 0);
        assert_eq!(bitfield_type_descr.field_value(0x1cffffff, "field3").unwrap().as_u32(), 2);

        let map_type_descr = type_info.find_type_description("Map").unwrap();
        assert!(matches!(map_type_descr.type_id(), Some(Type::Map)));
        assert!(map_type_descr.sample_type().is_some_and(|st| matches!(st, SampleType::Discrete)));
        let vehicle_data: RpcValue = shvproto::make_map!("mapField1" => 123).into();
        assert_eq!(map_type_descr.field_value(&vehicle_data, "mapField1").unwrap().as_int(), 123);
        assert!(map_type_descr.field_value(&vehicle_data, "noMapField").is_none());

        let enum_type_descr = type_info.find_type_description("Enum").unwrap();
        assert!(matches!(enum_type_descr.type_id(), Some(Type::Enum)));
        assert!(enum_type_descr.sample_type().is_some_and(|st| matches!(st, SampleType::Continuous)));
        assert_eq!(enum_type_descr.field_value((), "Warning").unwrap().as_u32(), 2);
        let field_alarms = BTreeMap::from([
            ("Unknown", (None, None)),
            ("Normal", (None, None)),
            ("Warning", (Some("warning"), Some(50))),
            ("Error", (Some("error"), Some(100))),
        ]);
        for fld in &enum_type_descr.fields() {
            assert!(field_alarms.get(fld.name()).is_some_and(|(alarm, level)| alarm == &fld.alarm() && level == &fld.alarm_level()));
        }
    }
}
