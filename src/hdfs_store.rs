use async_trait::async_trait;
use futures::{AsyncRead, Stream};
use std::sync::{Arc, Mutex};

use crate::{cr, FileStatus, HdfsFs, HdfsRegistry};
use chrono::{Local, TimeZone, Utc};
use datafusion::datasource::object_store::{
    FileMeta, FileMetaStream, ListEntry, ListEntryStream, ObjectReader, ObjectStore,
};
use datafusion::error::{DataFusionError, Result};
use futures::stream::{self, StreamExt};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::io::ErrorKind;
use std::pin::Pin;
use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    task,
};
use tokio_stream::wrappers::ReceiverStream;

#[derive(Debug)]
pub struct HdfsStore<'a> {
    fs_registry: HdfsRegistry<'a>,
}

impl<'a> HdfsStore<'a> {
    pub fn new() -> Result<Self> {
        Ok(HdfsStore {
            fs_registry: HdfsRegistry::new(),
        })
    }

    pub fn new_from(fs: Arc<Mutex<HashMap<String, HdfsFs<'a>>>>) -> Result<Self> {
        Ok(HdfsStore {
            fs_registry: HdfsRegistry::new_from(fs),
        })
    }

    pub fn get_fs(&self, prefix: &str) -> Result<HdfsFs> {
        cr!(self.fs_registry.get(prefix))
    }
}

fn list_dir_sync(
    fs: Arc<Mutex<HashMap<String, HdfsFs>>>,
    prefix: &str,
    response_tx: Sender<Result<ListEntry>>,
) -> Result<()> {
    let store = HdfsStore::new_from(fs)?;
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
impl<'a> ObjectStore for HdfsStore<'a> {
    async fn list_file(&self, prefix: &str) -> Result<FileMetaStream> {
        let entry_stream = self.list_dir(prefix, None).await?;
        todo!()
    }

    async fn list_dir(&self, prefix: &str, delimiter: Option<String>) -> Result<ListEntryStream> {
        let (response_tx, response_rx): (Sender<Result<ListEntry>>, Receiver<Result<ListEntry>>) =
            channel(2);

        let prefix = prefix.to_owned();
        let store = self.fs_registry.fs.clone();
        task::spawn_blocking(move || {
            if let Err(e) = list_dir_sync(store, &prefix, response_tx) {
                println!("List status thread terminated due to error {:?}", e)
            }
        });
        Ok(Box::pin(ReceiverStream::new(response_rx)))
    }

    fn file_reader(&self, file: FileMeta) -> Result<Arc<dyn ObjectReader>> {
        todo!()
    }
}

pub struct HdfsFileReader {
    file: FileMeta,
}

impl HdfsFileReader {
    pub fn new(file: FileMeta) -> Result<Self> {
        Ok(Self { file })
    }
}

#[async_trait]
impl ObjectReader for HdfsFileReader {
    async fn chunk_reader(&self, start: u64, length: usize) -> Result<Arc<dyn AsyncRead>> {
        todo!()
    }

    fn length(&self) -> u64 {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
