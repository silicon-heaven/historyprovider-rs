use std::borrow::Cow;

use shvclient::client::ShvApiVersion;
use shvrpc::join_path;
use shvrpc::rpc::ShvRI;

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
