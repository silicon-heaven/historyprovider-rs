use std::collections::BTreeMap;
use std::sync::Arc;

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use log::warn;
use shvclient::AppState;
use shvproto::{FromRpcValue, ToRpcValue};
use tokio::sync::Semaphore;

use crate::alarm::collect_alarms;
use crate::getlog::getlog_handler;
use crate::journalrw::{GetLog2Params, GetLog2Since};
use crate::sites::update_alarms;
use crate::{AlarmWithTimestamp, State};


#[derive(FromRpcValue)]
pub(crate) struct AlarmLogParams {
    since: shvproto::DateTime,
    until: shvproto::DateTime,
}

#[derive(ToRpcValue)]
pub(crate) struct AlarmLog {
    snapshot: Vec<AlarmWithTimestamp>,
    events: Vec<AlarmWithTimestamp>,
}

pub(crate) async fn alarmlog_impl(
    site_path_prefix: &str,
    params: &AlarmLogParams,
    app_state: AppState<State>,
) -> BTreeMap<String, AlarmLog>
{
    let typeinfos = app_state.sites_data.read().await.typeinfos.clone();

    let valid_typeinfos = typeinfos
        .iter()
        .filter_map(|(site_path, type_info)|
            type_info.as_ref().ok().map(|type_info| (site_path, type_info))
        );

    let getlog_params = Arc::new(GetLog2Params {
        since: GetLog2Since::DateTime(params.since),
        until: Some(params.until),
        with_snapshot: true,
        ..Default::default()
    });

    let site_path_matches_prefix = |site_path: &str|
        site_path.starts_with(site_path_prefix) && (site_path.len() == site_path_prefix.len()
            || site_path.as_bytes()[site_path_prefix.len()] == b'/');

    const MAX_SYNC_TASKS_DEFAULT: usize = 8;
    let semaphore = Arc::new(Semaphore::new(MAX_SYNC_TASKS_DEFAULT));

    valid_typeinfos
        .filter(|(site_path, _)| site_path_matches_prefix(site_path))
        .map(|(site_path, type_info)| {
            let app_state = app_state.clone();
            let getlog_params = getlog_params.clone();
            let semaphore = semaphore.clone();
            async move {
                let permit = semaphore
                    .clone()
                    .acquire_owned()
                    .await
                    .unwrap_or_else(|e| panic!("Cannot acquire semaphore: {e}"));
                let log = getlog_handler(site_path.as_str(), getlog_params.clone().as_ref(), app_state).await;
                drop(permit);
                (site_path, log, type_info)
            }
        })
        .collect::<FuturesUnordered<_>>()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .filter_map(|(site_path, result, type_info)|
            result
            .inspect_err(|err| warn!("alarmlog: Failed to fetch log for site '{site_path}': {err}"))
            .ok()
            .map(|result| (site_path, result, type_info))
        )
        .map(|(site_path, log, type_info)| {
            let mut tmp_alarms = Vec::<AlarmWithTimestamp>::new();
            for entry in log.snapshot_entries {
                update_alarms(collect_alarms, &mut tmp_alarms, type_info, &entry.path, &entry.value, shvproto::DateTime::from_epoch_msec(entry.epoch_msec));
            }

            let snapshot = tmp_alarms.clone();

            let events = log.event_entries
                .into_iter()
                .flat_map(|entry| update_alarms(collect_alarms, &mut tmp_alarms, type_info, &entry.path, &entry.value, shvproto::DateTime::from_epoch_msec(entry.epoch_msec)))
                .collect::<Vec<_>>();

            (site_path.to_string(), AlarmLog {
                events,
                snapshot,
            })
        })
        .collect::<BTreeMap<_,_>>()
}

