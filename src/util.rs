use std::borrow::Cow;
use std::path::Path;

use futures::TryStreamExt;
use shvclient::client::ShvApiVersion;
use shvrpc::join_path;
use shvrpc::rpc::ShvRI;
use tokio::fs::DirEntry;
use tokio_stream::wrappers::ReadDirStream;

use crate::{ClientCommandSender, Subscriber};


pub(crate) fn subscription_prefix_path<'a>(path: impl Into<Cow<'a, str>>, api_version: &ShvApiVersion) -> String {
    let path = path.into();
    match api_version {
        ShvApiVersion::V2 => path.into(),
        ShvApiVersion::V3 => join_path!(&path, "*"),
    }
}

pub(crate) async fn subscribe(
    client_cmd_tx: &ClientCommandSender,
    path: impl AsRef<str>,
    signal: impl AsRef<str>,
) -> Subscriber
{
    let path = path.as_ref();
    let signal = signal.as_ref();
    client_cmd_tx
        .subscribe(
            ShvRI::from_path_method_signal(path, "*", Some(signal))
            .unwrap_or_else(|e| panic!("Invalid ShvRI for path `{path}`, signal `{signal}`: {e}"))
        )
        .await
        .unwrap_or_else(|e| panic!("Subscribe `{signal}` on path `{path}`: {e}"))
}

pub(crate) async fn get_files(dir_path: impl AsRef<Path>, file_filter_fn: impl Fn(&DirEntry) -> bool) -> Result<Vec<DirEntry>, String> {
    let dir_path = dir_path.as_ref();
    let journal_dir = ReadDirStream::new(tokio::fs::read_dir(dir_path)
        .await
        .map_err(|e|
            format!("Cannot read journal directory at {}: {}", dir_path.to_string_lossy(), e)
        )?
    );
    journal_dir.try_filter_map(async |entry| {
        Ok(entry
            .metadata()
            .await?
            .is_file()
            .then(|| file_filter_fn(&entry).then_some(entry))
            .flatten()
        )
    })
    .try_collect::<Vec<_>>()
    .await
    .map_err(|e| format!("Cannot read content of the journal directory {}: {}", dir_path.to_string_lossy(), e))
}

pub(crate) fn is_log2_file(entry: &DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .is_some_and(|file_name| file_name.ends_with(".log2"))
}
