use log::error;
use log::info;
use tokio::fs;
use tokio::io;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::util::msec_to_log2_filename;

#[derive(Debug, Clone)]
pub(crate) struct LogFile {
    pub(crate) name: PathBuf,
    pub(crate) parent_dir: PathBuf,
    pub(crate) size: u64,
}

pub(crate) async fn collect_log2_files(dir: impl AsRef<Path>) -> io::Result<Vec<LogFile>> {
    let mut result = Vec::new();
    let mut dirs = vec![dir.as_ref().to_path_buf()];

    while let Some(current_dir) = dirs.pop() {
        let mut entries = fs::read_dir(&current_dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let metadata = entry.metadata().await?;

            if metadata.is_dir() {
                dirs.push(path);
            } else if metadata.is_file() && path.extension().is_some_and(|ext| ext == "log2")
                && let (Some(file_name), Some(parent)) = (path.file_name(), path.parent()) {
                    result.push(LogFile {
                        name: file_name.into(),
                        parent_dir: parent.to_path_buf(),
                        size: metadata.len(),
                    });
            }
        }
    }

    Ok(result)
}

/// Prune `.log2` files while keeping the newest one per directory
pub(crate) async fn cleanup_log2_files(dir: impl AsRef<Path>, size_limit: u64, days_to_keep: i64) -> io::Result<()> {
    let files = collect_log2_files(dir).await?;
    let mut files_size: u64 = files.iter().map(|f| f.size).sum();

    info!("log2 files size: {files_size}, size limit: {size_limit}, days_to_keep: {days_to_keep}");
    if files_size < size_limit {
        info!("Size limit not hit, nothing to cleanup");
        return Ok(());
    }

    // Group by parent directory
    let mut grouped: HashMap<PathBuf, Vec<LogFile>> = HashMap::new();
    for file in files {
        grouped.entry(file.parent_dir.clone()).or_default().push(file);
    }

    let mut deletable_files = Vec::new();

    // This file doesn't have to exist, I'm only constructing the filename for log retention.
    let oldest_file_to_keep = PathBuf::from(msec_to_log2_filename(shvproto::DateTime::now().add_days(-days_to_keep).epoch_msec()));
    info!("keeping files younger than {filename}", filename = oldest_file_to_keep.to_string_lossy());

    for (_dir, mut group) in grouped {
        // Sort descending (newest first)
        group.sort_by(|a, b| b.name.cmp(&a.name));
        // Keep the newest file, and keep files newer than oldest_file_to_delete
        deletable_files.extend(group.into_iter().skip(1).filter(|log_file| log_file.name < oldest_file_to_keep));
    }

    // Sort deletable files (oldest first for deletion)
    deletable_files.sort_by(|a, b| a.name.cmp(&b.name));

    let files_size_orig = files_size;
    for file in deletable_files {
        if files_size <= size_limit {
            break;
        }

        let file_path = file.parent_dir.join(file.name);

        if let Err(err) = fs::remove_file(&file_path).await {
            error!("Cannot delete {path}: {err}", path = file_path.to_string_lossy());
            continue;
        }

        files_size -= file.size;
        info!("Deleted: {path} ({size} bytes)", path = file_path.to_string_lossy(), size = file.size);
    }

    info!("Cleaned up size: {cleaned_up}", cleaned_up = files_size_orig - files_size);

    Ok(())
}
