// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! A rust wrapper over libhdfs3

/// Rust APIs wrapping libhdfs3 API, providing better semantic and abstraction
pub mod dfs;
pub mod err;
/// libhdfs3 native binding APIs
pub mod native;
pub mod util;

pub use crate::dfs::*;
pub use crate::err::HdfsErr;
pub use crate::util::HdfsUtil;

use crate::native::{
    hdfsBuilderConnect, hdfsBuilderSetNameNode, hdfsBuilderSetNameNodePort, hdfsNewBuilder,
};
use log::info;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use url::Url;

static LOCAL_FS_SCHEME: &str = "file";

/// HdfsRegistry which stores seen HdfsFs instances.
pub struct HdfsRegistry<'a> {
    fs: Arc<Mutex<HashMap<String, HdfsFs<'a>>>>,
}

impl<'a> Default for HdfsRegistry<'a> {
    fn default() -> Self {
        HdfsRegistry::new()
    }
}

struct HostPort {
    host: String,
    port: u16,
}

enum NNScheme {
    Local,
    Remote(HostPort),
}

impl ToString for NNScheme {
    fn to_string(&self) -> String {
        match self {
            NNScheme::Local => "file:///".to_string(),
            NNScheme::Remote(hp) => format!("{}:{}", hp.host, hp.port),
        }
    }
}

impl<'a> HdfsRegistry<'a> {
    pub fn new() -> HdfsRegistry<'a> {
        HdfsRegistry {
            fs: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn get_namenode(&self, path: &str) -> Result<NNScheme, HdfsErr> {
        match Url::parse(path) {
            Ok(url) => {
                if url.scheme() == LOCAL_FS_SCHEME {
                    Ok(NNScheme::Local)
                } else if url.host().is_some() && url.port().is_some() {
                    Ok(NNScheme::Remote(HostPort {
                        host: format!("{}://{}", &url.scheme(), url.host().unwrap()),
                        port: url.port().unwrap(),
                    }))
                } else {
                    Err(HdfsErr::InvalidUrl(path.to_string()))
                }
            }
            Err(_) => Err(HdfsErr::InvalidUrl(path.to_string())),
        }
    }

    pub fn get(&mut self, path: &str) -> Result<HdfsFs<'a>, HdfsErr> {
        let host_port = self.get_namenode(path)?;

        let mut map = self.fs.lock().unwrap();

        let entry: &mut HdfsFs = map.entry(host_port.to_string()).or_insert({
            let hdfs_fs = unsafe {
                let hdfs_builder = hdfsNewBuilder();
                match host_port {
                    NNScheme::Local => {} //NO-OP
                    NNScheme::Remote(ref hp) => {
                        hdfsBuilderSetNameNode(hdfs_builder, to_raw!(&*hp.host));
                        hdfsBuilderSetNameNodePort(hdfs_builder, hp.port);
                    }
                }
                info!("Connecting to NameNode ({})", &host_port.to_string());
                hdfsBuilderConnect(hdfs_builder)
            };

            if hdfs_fs.is_null() {
                return Err(HdfsErr::CannotConnectToNameNode(host_port.to_string()));
            }
            info!("Connected to NameNode ({})", &host_port.to_string());
            HdfsFs::new(host_port.to_string(), hdfs_fs)
        });

        Ok(entry.clone())
    }
}

#[cfg(test)]
mod test {
    use super::HdfsRegistry;
    use crate::HdfsErr;
    use log::debug;

    #[test]
    fn test_hdfs_connection() -> Result<(), HdfsErr> {
        let port = 9000;

        let dfs_addr = format!("hdfs://localhost:{}", port);
        let mut fs_registry = HdfsRegistry::new();

        let test_path = format!("hdfs://localhost:{}/users/test", port);
        debug!("Trying to get {}", &test_path);

        assert_eq!(dfs_addr, fs_registry.get(&test_path)?.url);

        // create a file, check existence, and close
        let fs = fs_registry.get(&test_path)?;
        let test_file = "/test_file";
        if fs.exist(test_file) {
            fs.delete(test_file, true)?;
        }
        let created_file = match fs.create(test_file) {
            Ok(f) => f,
            Err(e) => panic!("Couldn't create a file {:?}", e),
        };
        assert!(created_file.close().is_ok());
        assert!(fs.exist(test_file));

        // open a file and close
        let opened_file = fs.open(test_file)?;
        assert!(opened_file.close().is_ok());

        match fs.mkdir("/dir1") {
            Ok(_) => debug!("/dir1 created"),
            Err(_) => panic!("Couldn't create /dir1 directory"),
        };

        let file_info = fs.get_file_status("/dir1")?;

        assert_eq!("/dir1", file_info.name());
        assert!(!file_info.is_file());
        assert!(file_info.is_directory());

        let sub_dir_num = 3;
        let mut expected_list = Vec::new();
        for x in 0..sub_dir_num {
            let filename = format!("/dir1/{}", x);
            expected_list.push(format!("/dir1/{}", x));

            match fs.mkdir(&filename) {
                Ok(_) => debug!("{} created", filename),
                Err(_) => panic!("Couldn't create {} directory", filename),
            };
        }

        let mut list = fs.list_status("/dir1")?;
        assert_eq!(sub_dir_num, list.len());

        list.sort_by(|a, b| Ord::cmp(a.name(), b.name()));

        for (expected, name) in expected_list
            .iter()
            .zip(list.iter().map(|status| status.name()))
        {
            assert_eq!(expected, name);
        }
        Ok(())
    }
}
