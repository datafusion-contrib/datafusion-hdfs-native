use async_trait::async_trait;
use futures::{AsyncRead, Future};
use std::sync::{Arc, Mutex};

use crate::{FileStatus, HdfsErr, HdfsFile, HdfsFs, HdfsRegistry, RawHdfsFileWrapper};
use chrono::{Local, TimeZone, Utc};
use datafusion::datasource::object_store::{
    FileMeta, FileMetaStream, ListEntry, ListEntryStream, ObjectReader, ObjectStore,
};
use datafusion::error::{DataFusionError, Result};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::io::{ErrorKind, Read};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    task,
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

#[derive(Debug)]
pub struct HdfsStore {
    fs_registry: HdfsRegistry,
}

impl HdfsStore {
    #[allow(dead_code)]
    // We will finally move HdfsStore into its own crate when Hdfs-native is mature,
    // therefore ignore the warning here.
    pub fn new() -> Result<Self> {
        Ok(HdfsStore {
            fs_registry: HdfsRegistry::new(),
        })
    }

    pub fn new_from(fs: Arc<Mutex<HashMap<String, HdfsFs>>>) -> Self {
        HdfsStore {
            fs_registry: HdfsRegistry::new_from(fs),
        }
    }

    pub fn get_fs(&self, prefix: &str) -> std::result::Result<HdfsFs, HdfsErr> {
        self.fs_registry.get(prefix)
    }

    fn all_fs(&self) -> Arc<Mutex<HashMap<String, HdfsFs>>> {
        self.fs_registry.all_fs.clone()
    }
}

fn list_dir_sync(
    all_fs: Arc<Mutex<HashMap<String, HdfsFs>>>,
    prefix: &str,
    response_tx: Sender<Result<ListEntry>>,
) -> Result<()> {
    let store = HdfsStore::new_from(all_fs);
    let fs = store.get_fs(prefix)?;
    let all_status = fs.list_status(prefix)?;
    for status in &all_status {
        response_tx
            .blocking_send(Ok(ListEntry::from(status)))
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    }
    Ok(())
}

impl<'a> TryFrom<&FileStatus<'a>> for FileMeta {
    type Error = DataFusionError;

    fn try_from(status: &FileStatus) -> Result<Self> {
        let rs: ListEntry = status.into();
        match rs {
            ListEntry::FileMeta(f) => Ok(f),
            ListEntry::Prefix(path) => {
                Err(std::io::Error::new(ErrorKind::Other, format!("{} is not a file", path)).into())
            }
        }
    }
}

impl<'a> From<&FileStatus<'a>> for ListEntry {
    fn from(status: &FileStatus) -> Self {
        if status.is_directory() {
            ListEntry::Prefix(status.name().to_owned())
        } else {
            let time = Local
                .timestamp(status.last_modified(), 0)
                .with_timezone(&Utc);
            ListEntry::FileMeta(FileMeta {
                path: status.name().to_owned(),
                last_modified: Some(time),
                size: status.len() as u64,
            })
        }
    }
}

#[async_trait]
impl ObjectStore for HdfsStore {
    async fn list_file(&self, prefix: &str) -> Result<FileMetaStream> {
        let entry_stream = self.list_dir(prefix, None).await?;
        let result = entry_stream.map(|r| match r {
            Ok(entry) => match entry {
                ListEntry::FileMeta(fm) => Ok(fm),
                ListEntry::Prefix(path) => Err(DataFusionError::from(std::io::Error::new(
                    ErrorKind::InvalidInput,
                    format!("{} is not a file", path),
                ))),
            },
            Err(e) => Err(e),
        });

        Ok(Box::pin(result))
    }

    async fn list_dir(&self, prefix: &str, _delimiter: Option<String>) -> Result<ListEntryStream> {
        let (response_tx, response_rx): (Sender<Result<ListEntry>>, Receiver<Result<ListEntry>>) =
            channel(2);
        let prefix = prefix.to_owned();
        let all_fs = self.all_fs();
        task::spawn_blocking(move || {
            if let Err(e) = list_dir_sync(all_fs, &prefix, response_tx) {
                println!("List status thread terminated due to error {:?}", e)
            }
        });
        Ok(Box::pin(ReceiverStream::new(response_rx)))
    }

    fn file_reader(&self, file: FileMeta) -> Result<Arc<dyn ObjectReader>> {
        let fs = self.all_fs();
        let reader = HdfsFileReader::new(HdfsStore::new_from(fs), file);
        Ok(Arc::new(reader))
    }
}

pub struct HdfsFileReader {
    store: HdfsStore,
    file: FileMeta,
}

struct HdfsAsyncRead {
    store: HdfsStore,
    file: RawHdfsFileWrapper,
    start: u64,
    length: usize,
}

impl AsyncRead for HdfsAsyncRead {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let path = self.file.path.clone();
        let all_fs = self.store.all_fs();
        let file_wrapper = self.file.clone();
        let start = self.start as i64;
        let length = self.length;
        let buf_len = buf.len();

        let mut read_sync = task::spawn_blocking(move || {
            let store = HdfsStore::new_from(all_fs);
            let fs = store.get_fs(&*path);
            let mut vec = vec![0u8; buf_len];
            match fs {
                Ok(fs) => {
                    let file = HdfsFile::from_raw(&file_wrapper, &fs);
                    file.read_with_pos_length(start as i64, &mut *vec, length)
                        .map_err(std::io::Error::from)
                        .map(|s| (vec, s as usize))
                }
                Err(e) => Err(std::io::Error::from(e)),
            }
        });

        match Pin::new(&mut read_sync).poll(cx) {
            Poll::Ready(r) => match r {
                Ok(vl_r) => match vl_r {
                    Ok(vl) => match vl.0.as_slice().read(buf) {
                        Ok(_) => Poll::Ready(Ok(vl.1)),
                        Err(e) => Poll::Ready(Err(e)),
                    },
                    Err(e) => Poll::Ready(Err(e)),
                },
                Err(e) => Poll::Ready(Err(std::io::Error::from(e))),
            },
            Poll::Pending => Poll::Pending,
        }
    }
}

impl HdfsFileReader {
    pub fn new(store: HdfsStore, file: FileMeta) -> Self {
        Self { store, file }
    }
}

#[async_trait]
impl ObjectReader for HdfsFileReader {
    async fn chunk_reader(&self, start: u64, length: usize) -> Result<Arc<dyn AsyncRead>> {
        let file = self.file.path.clone();
        let fs = self.store.all_fs();
        let x = task::spawn_blocking(move || {
            let store = HdfsStore::new_from(fs);
            let fs_result = store.get_fs(&*file).map_err(DataFusionError::from);
            match fs_result {
                Ok(fs) => {
                    let file_result = fs.open(&*file).map_err(DataFusionError::from);
                    match file_result {
                        Ok(file) => {
                            let x = (&file).into();
                            Ok(HdfsAsyncRead {
                                store: HdfsStore::new_from(store.all_fs()),
                                file: x,
                                start,
                                length,
                            })
                        }
                        Err(e) => Err(e),
                    }
                }
                Err(e) => Err(e),
            }
        })
        .await;
        match x {
            Ok(r) => Ok(Arc::new(r?)),
            Err(e) => Err(DataFusionError::Execution(format!(
                "Open hdfs file thread terminated due to error: {:?}",
                e
            ))),
        }
    }

    fn length(&self) -> u64 {
        self.file.size
    }
}

#[cfg(test)]
mod tests {
    use crate::hdfs_store::HdfsStore;

    #[test]
    fn it_works() {
        let _hdfs_store = HdfsStore::new();
    }
}
