
use futures::{AsyncWrite, AsyncWriteExt, Stream};
use futures::io::{AsyncBufRead, AsyncBufReadExt, BufWriter, Lines};
use shvproto::RpcValue;
use shvrpc::metamethod::AccessLevel;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::error::Error;

use crate::journalentry::JournalEntry;

const JOURNAL_ENTRIES_SEPARATOR: &str = "\t";

const VALUE_FLAG_SPONTANEOUS_BIT: i32 = 1;
const VALUE_FLAG_PROVISIONAL_BIT: i32 = 2;

fn parse_journal_entry_log2(line: &str) -> Result<JournalEntry, Box<dyn Error>> {
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
    let domain = parts_iter.next();
    let value_flags = parts_iter.next().unwrap_or_default().parse().unwrap_or(0);
    let user_id = parts_iter.next().unwrap_or_default().into();

    Ok(JournalEntry {
        epoch_msec,
        path,
        signal: domain.unwrap_or("chng").into(),
        source: "get".into(),
        value,
        access_level: AccessLevel::Read as i32,
        short_time,
        user_id,
        repeat: value_flags & (1 << VALUE_FLAG_SPONTANEOUS_BIT) == 0,
        provisional: value_flags & (1 << VALUE_FLAG_PROVISIONAL_BIT) != 0,
    })
}

pub struct JournalReaderLog2<R> {
    lines: Lines<R>,
}

impl<R> JournalReaderLog2<R>
where
    R: AsyncBufRead + Unpin,
{
    pub fn new(reader: R) -> Self {
        JournalReaderLog2 {
            lines: reader.lines(),
        }
    }
}

impl<R> Stream for JournalReaderLog2<R>
where
    R: AsyncBufRead + Unpin,
{
    type Item = Result<JournalEntry, Box<dyn Error>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let lines = Pin::new(&mut self.lines);

        match lines.poll_next(cx) {
            Poll::Ready(Some(Ok(line))) => Poll::Ready(Some(parse_journal_entry_log2(&line))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(Box::new(e)))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub struct JournalWriterLog2<W> {
    writer: BufWriter<W>,
}

impl<W> JournalWriterLog2<W>
where
    W: AsyncWrite + Unpin,
{
    pub fn new(writer: W) -> Self {
        Self {
            writer: BufWriter::new(writer)
        }
    }

    pub async fn append(&mut self, msec: i64, orig_time: i64, entry: &JournalEntry) -> Result<(), std::io::Error> {
        let line = {
            let mut fields = vec![];
            fields.push(shvproto::DateTime::from_epoch_msec(msec).to_iso_string());
            fields.push(if orig_time == msec { "".into() } else { shvproto::DateTime::from_epoch_msec(orig_time).to_iso_string() });
            fields.push(entry.path.clone());
            fields.push(entry.value.to_cpon());
            fields.push(if entry.short_time >= 0 { entry.short_time.to_string() } else { "".into() });
            fields.push(entry.signal.clone());
            let mut value_flags = 0;
            if !entry.repeat {
                value_flags |= 1 << VALUE_FLAG_SPONTANEOUS_BIT;
            }
            if entry.provisional {
                value_flags |= 1 << VALUE_FLAG_PROVISIONAL_BIT;
            }
            fields.push(value_flags.to_string());
            fields.push(entry.user_id.clone());
            fields.join(JOURNAL_ENTRIES_SEPARATOR) + "\n"
        };
        self.writer.write_all(line.as_bytes()).await?;
        self.writer.flush().await
    }
}

#[cfg(test)]
mod tests {
    use futures::io::BufReader;
    use futures::StreamExt;
    use tokio::fs::File;
    use tokio_util::compat::TokioAsyncReadCompatExt;

    use crate::journalrw::JournalReaderLog2;

    #[tokio::test]
    async fn read_file_journal() {
        let file = File::open("test.log2").await.unwrap();
        let mut reader = JournalReaderLog2::new(BufReader::new(file.compat()));
        while let Some(result) = reader.next().await {
            println!("{:?}", result.unwrap());
        }
    }
}
