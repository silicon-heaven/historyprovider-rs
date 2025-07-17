use shvproto::{DateTime as ShvDateTime, RpcValue};

pub(crate) struct DataChange {
    pub(crate) value: RpcValue,
    pub(crate) date_time: Option<ShvDateTime>,
    pub(crate) short_time: Option<i32>,
    pub(crate) value_flags: u64,
}

enum DataChangeMetaTag {
    DateTime = shvrpc::rpctype::Tag::USER as isize,
    ShortTime,
    ValueFlags,
    SpecialListValue,
}

fn meta_value<I: shvproto::metamap::GetIndex>(rv: &RpcValue, key: I) -> Option<&RpcValue> {
    rv.meta.as_ref().and_then(| meta| meta.get(key))
}

fn is_data_change(rv: &RpcValue) -> bool {
    meta_value(rv, shvrpc::rpctype::Tag::MetaTypeNameSpaceId as usize).unwrap_or_default().as_int() == shvrpc::rpctype::NameSpaceID::Global as i64 &&
        meta_value(rv, shvrpc::rpctype::Tag::MetaTypeId as usize).unwrap_or_default().as_int() == shvrpc::rpctype::GlobalNS::MetaTypeID::ValueChange as i64
}

impl From<RpcValue> for DataChange {
    fn from(value: RpcValue) -> Self {
        if !is_data_change(&value) {
            return DataChange {
                value,
                date_time: None,
                short_time: None,
                value_flags: Default::default(),
            }
        }

        let date_time = meta_value(&value, DataChangeMetaTag::DateTime as usize)
            .and_then(|v| v.to_datetime());
        let short_time = meta_value(&value, DataChangeMetaTag::ShortTime as usize)
            .map(|v| v.as_i32());
        let value_flags = meta_value(&value, DataChangeMetaTag::ValueFlags as usize)
            .map(|v| v.as_u64()).unwrap_or_default();

        let unpacked_value = if let shvproto::Value::List(lst) = &value.value &&
            lst.len() == 1 &&
            lst[0].meta.as_ref().is_some_and(|meta| !meta.is_empty()) &&
            value.meta.as_ref().is_none_or(|meta| meta.get(DataChangeMetaTag::SpecialListValue as usize).is_none())
        {
            lst[0].clone()
        } else {
            RpcValue {
                meta: None,
                value: value.value,
            }
        };
        DataChange {
            value: unpacked_value,
            date_time,
            short_time,
            value_flags,
        }
    }
}

impl From<DataChange> for RpcValue {
    fn from(data_change: DataChange) -> Self {
        let mut res = if data_change.value.meta.as_ref().is_none_or(|meta| meta.is_empty()) {
            let val = data_change.value.value.clone();
            if let shvproto::Value::List(lst) = &val &&
                lst.len() == 1 &&
                lst[0].meta.as_ref().is_some_and(|meta| !meta.is_empty()) {
                    let mut mm = shvproto::MetaMap::new();
                    mm.insert(DataChangeMetaTag::SpecialListValue as usize, true.into());
                    RpcValue::new(val, Some(mm))
            } else {
                RpcValue::new(val, None)
            }
        } else {
            shvproto::make_list!(data_change.value).into()
        };
        let mm = res.meta
            .get_or_insert_default()
            .insert(shvrpc::rpctype::Tag::MetaTypeId as usize, RpcValue::from(shvrpc::rpctype::GlobalNS::MetaTypeID::ValueChange as i64));
        if let Some(date_time) = data_change.date_time {
            mm.insert(DataChangeMetaTag::DateTime as usize, date_time.into());
        }
        if let Some(short_time) = data_change.short_time {
            mm.insert(DataChangeMetaTag::ShortTime as usize, short_time.into());
        }
        if data_change.value_flags != 0 {
            mm.insert(DataChangeMetaTag::ValueFlags as usize, data_change.value_flags.into());
        }
        res
    }
}
