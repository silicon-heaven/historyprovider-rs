
use futures::Stream;
use futures::io::{AsyncBufRead, Lines, AsyncBufReadExt};
use shvproto::RpcValue;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::error::Error;

use crate::journalentry::JournalEntry;

fn parse_journal_entry(line: &str) -> Result<JournalEntry, Box<dyn Error>> {
    const JOURNAL_ENTRIES_SEPARATOR: char = '\t';
    let parts: Vec<&str> = line.split(JOURNAL_ENTRIES_SEPARATOR).collect();
    let mut parts_iter = parts.iter().copied();

    let epoch_msec = shvproto::datetime::DateTime::from_iso_str(parts_iter
        .next()
        .ok_or_else(|| format!("Missing timestamp on line: {line}"))?)
        .map_err(|e| format!("Cannot parse timestamp on line: {line}, error: {e}"))?
        .epoch_msec();

    let _up_time = parts_iter.next();
    let path = parts_iter.next().ok_or_else(|| format!("Missing path on line: {line}"))?.to_string();
    let value = RpcValue::from_cpon(parts_iter.next().unwrap_or_default())?;
    let short_time = parts_iter.next().unwrap_or_default().parse().unwrap_or(-1);
    let domain = parts_iter.next().unwrap_or_default().into();
    let value_flags = parts_iter.next().unwrap_or_default().parse().unwrap_or(0);
    let user_id = parts_iter.next().unwrap_or_default().into();

    Ok(JournalEntry {
        epoch_msec,
        path,
        value,
        short_time,
        domain,
        value_flags,
        user_id,
    })
}

pub struct FileJournalReader<R> {
    lines: Lines<R>,
}

impl<R> FileJournalReader<R>
where
    R: AsyncBufRead + Unpin,
{
    pub fn new(reader: R) -> Self {
        FileJournalReader {
            lines: reader.lines(),
        }
    }
}

impl<R> Stream for FileJournalReader<R>
where
    R: AsyncBufRead + Unpin,
{
    type Item = Result<JournalEntry, Box<dyn Error>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let lines = Pin::new(&mut self.lines);

        match lines.poll_next(cx) {
            Poll::Ready(Some(Ok(line))) => Poll::Ready(Some(parse_journal_entry(&line))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(Box::new(e)))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::io::BufReader;
    use futures::StreamExt;
    use tokio::fs::File;
    use tokio_util::compat::TokioAsyncReadCompatExt;

    use crate::journalrw::FileJournalReader;

    #[tokio::test]
    async fn read_file_journal() {
        let file = File::open("test.log2").await.unwrap();
        let mut reader = FileJournalReader::new(BufReader::new(file.compat()));
        while let Some(result) = reader.next().await {
            println!("{:?}", result.unwrap());
        }
    }
}
