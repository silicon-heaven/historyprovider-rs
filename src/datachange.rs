use shvproto::{DateTime as ShvDateTime, RpcValue};

#[derive(Clone,Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub(crate) struct DataChange {
    pub(crate) value: RpcValue,
    pub(crate) date_time: Option<ShvDateTime>,
    pub(crate) short_time: Option<i32>,
    pub(crate) value_flags: u64,
}

#[derive(Copy,Clone,Debug)]
enum DataChangeMetaTag {
    DateTime = shvrpc::rpctype::Tag::USER as isize,
    ShortTime,
    ValueFlags,
    SpecialListValue,
}

fn meta_value<I: shvproto::metamap::GetIndex>(rv: &RpcValue, key: I) -> Option<&RpcValue> {
    rv.meta.as_ref().and_then(|meta| meta.get(key))
}

fn is_data_change(rv: &RpcValue) -> bool {
    meta_value(rv, shvrpc::rpctype::Tag::MetaTypeNameSpaceId as usize).unwrap_or_default().as_int() == shvrpc::rpctype::NameSpaceID::Global as i64 &&
        meta_value(rv, shvrpc::rpctype::Tag::MetaTypeId as usize).unwrap_or_default().as_int() == shvrpc::rpctype::GlobalNS::MetaTypeID::ValueChange as i64
}

impl shvproto::metamap::GetIndex for DataChangeMetaTag {
    fn make_key(&self) -> shvproto::metamap::GetKey<'_> {
        shvproto::metamap::GetKey::Int(*self as i32)
    }
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

        let date_time = meta_value(&value, DataChangeMetaTag::DateTime)
            .and_then(|v| v.to_datetime());
        let short_time = meta_value(&value, DataChangeMetaTag::ShortTime)
            .map(|v| v.as_i32());
        let value_flags = meta_value(&value, DataChangeMetaTag::ValueFlags)
            .map(|v| v.as_u64()).unwrap_or_default();

        let unpacked_value = if let shvproto::Value::List(lst) = &value.value &&
            lst.len() == 1 &&
            lst[0].meta.as_ref().is_some_and(|meta| !meta.is_empty()) &&
            value.meta.as_ref().is_none_or(|meta| meta.get(DataChangeMetaTag::SpecialListValue).is_none())
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
            mm.insert(DataChangeMetaTag::DateTime, date_time.into());
        }
        if let Some(short_time) = data_change.short_time {
            mm.insert(DataChangeMetaTag::ShortTime, short_time.into());
        }
        if data_change.value_flags != 0 {
            mm.insert(DataChangeMetaTag::ValueFlags, data_change.value_flags.into());
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use shvproto::{DateTime as ShvDateTime, RpcValue};

    use crate::journalrw::VALUE_FLAG_PROVISIONAL_BIT;

    use super::DataChange;

    #[test]
    fn rpcvalue_to_datachange() {
        let notification_param = RpcValue::from_cpon(r#"<1:5,8:d"2025-07-14T15:01:00.201Z",10:2>false"#).unwrap();
        let data_change = DataChange::from(notification_param);
        assert_eq!(data_change.value, false.into());
        assert_eq!(data_change.date_time, Some(ShvDateTime::from_iso_str("2025-07-14T15:01:00.201Z").unwrap()));
        assert_eq!(data_change.short_time, None);

        let notification_param = RpcValue::from_cpon("true").unwrap();
        let data_change = DataChange::from(notification_param);
        assert_eq!(data_change.value, true.into());
        assert_eq!(data_change.date_time, None);
        assert_eq!(data_change.short_time, None);

        let notification_param = RpcValue::from_cpon("<1:5,10:4>[<1:123>true]").unwrap();
        let data_change = DataChange::from(notification_param);
        assert_eq!(data_change.value.value, true.into());
        assert!(data_change.value.meta.is_some_and(|m| m.get(1).is_some_and(|v| v == &123.into())));
        assert_eq!(data_change.date_time, None);
        assert_eq!(data_change.short_time, None);
        assert_eq!(data_change.value_flags, 4);

        let notification_param = RpcValue::from_cpon("<1:5,10:4,11:true>[<1:123>true]").unwrap();
        let data_change = DataChange::from(notification_param);
        assert_eq!(data_change.value, shvproto::make_list!(RpcValue::new(true.into(), {let mut mm = shvproto::metamap::MetaMap::new(); mm.insert(1, 123.into()); Some(mm)})).into());
        assert!(data_change.value.meta.is_none());
        assert_eq!(data_change.date_time, None);
        assert_eq!(data_change.short_time, None);
        assert_eq!(data_change.value_flags, 4);
    }

    #[test]
    fn rpcvalue_datachange_conversions() {
        let data_change = DataChange {
            value: 123.into(),
            date_time: None,
            short_time: None,
            value_flags: 0,
        };
        let rv: RpcValue = data_change.clone().into();
        assert_eq!(data_change, rv.into());

        let data_change = DataChange {
            value: "foo".into(),
            date_time: Some(ShvDateTime::from_iso_str("2025-07-14T15:01:00.201Z").unwrap()),
            short_time: Some(10),
            value_flags: 1 << VALUE_FLAG_PROVISIONAL_BIT,
        };
        let rv: RpcValue = data_change.clone().into();
        assert_eq!(data_change, rv.into());
    }
}
