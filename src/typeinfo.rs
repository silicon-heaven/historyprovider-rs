use shvproto::RpcValue;
use shvproto::Map as RpcMap;

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

    fn set_fields(&mut self, fields: Vec<FieldDescription>) {
        set_data_value(self, KEY_FIELDS, fields);
    }

    fn description(&self) -> Option<&str> {
        self.data_value_into(KEY_DESCRIPTION)
    }

    fn set_description(&mut self, value: impl AsRef<str>) {
        set_data_value(self, KEY_DESCRIPTION, value.as_ref());
    }
}

pub trait FieldDescriptionMethods: TypeDescriptionMethods {
    fn name(&self) -> &str {
        self.data_value(KEY_NAME).map_or("", RpcValue::as_str)
    }
	fn set_name(&mut self, name: impl AsRef<str>) {
        set_data_value(self, KEY_NAME, name.as_ref());
    }
	fn alarm(&self) -> &str {
        self.data_value(KEY_ALARM).map_or("", RpcValue::as_str)
    }
	fn set_alarm(&mut self, alarm: impl AsRef<str>) {
        set_data_value(self, KEY_ALARM, alarm.as_ref());
    }
	fn alarm_level(&self) -> i32 {
        self.data_value(KEY_ALARM_LEVEL).map_or(0, RpcValue::as_i32)
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
            "Discrete" => Some(Self::Discrete),
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

impl TypeDescriptionMethods for FieldDescription {
    fn data(&self) -> &RpcMap {
        &self.data
    }
    fn data_mut(&mut self) -> &mut RpcMap {
        &mut self.data
    }
}
impl FieldDescriptionMethods for FieldDescription { }

pub struct PropertyDescription {
    base: FieldDescription,
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
