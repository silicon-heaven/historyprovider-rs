use std::borrow::Cow;
use std::collections::HashSet;
use std::hash::Hash;
use std::path::Path;
use std::pin::Pin;
use std::task::Poll;

use futures::{Stream, StreamExt, TryStreamExt};
use shvclient::client::ShvApiVersion;
use shvrpc::join_path;
use shvrpc::rpc::ShvRI;
use tokio::fs::DirEntry;
use tokio_stream::wrappers::ReadDirStream;

use crate::{ClientCommandSender, Subscriber};

#[cfg(test)]
use std::sync::Once;
#[cfg(test)]
use simple_logger::SimpleLogger;

#[cfg(test)]
pub(crate) fn init_logger() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        SimpleLogger::new()
            .with_level(log::LevelFilter::Debug)
            .init()
            .unwrap();
        });
}

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

#[derive(Clone)]
pub(crate) struct DedupSender<T: Eq + Hash + Clone> {
    sender: futures::channel::mpsc::UnboundedSender<T>,
    pending: std::sync::Arc<std::sync::Mutex<HashSet<T>>>,
}

impl<T: Eq + Hash + Clone> DedupSender<T> {
    pub(crate) fn send(&self, msg: T) -> Result<bool, futures::channel::mpsc::TrySendError<T>> {
        let mut pending = self.pending.lock().expect("Tried to lock a mutex already held by the same thread");
        if pending.contains(&msg) {
            return Ok(false);
        }
        self.sender
            .unbounded_send(msg.clone())
            .map(|_| {
                pending.insert(msg);
                true
            })
    }

    #[cfg(test)]
    pub(crate) fn close_channel(&self) {
        self.sender.close_channel();
    }
}

pub(crate) struct DedupReceiver<T: Eq + Hash + Clone> {
    receiver: futures::channel::mpsc::UnboundedReceiver<T>,
    pending: std::sync::Arc<std::sync::Mutex<HashSet<T>>>,
}

impl<T: Eq + Hash + Clone + Unpin> Stream for DedupReceiver<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let next = this.receiver.poll_next_unpin(cx);
        if let Poll::Ready(Some(msg)) = &next {
            let mut pending = this.pending.lock().expect("Tried to lock a mutex already held by the same thread");
            pending.remove(msg);
        }
        next
    }
}

pub(crate) fn dedup_channel<T: Eq + Hash + Clone>() -> (DedupSender<T>, DedupReceiver<T>) {
    let pending = std::sync::Arc::new(std::sync::Mutex::new(HashSet::new()));
    let (sender, receiver) = futures::channel::mpsc::unbounded();
    (DedupSender { pending: pending.clone(), sender }, DedupReceiver { pending, receiver })
}
