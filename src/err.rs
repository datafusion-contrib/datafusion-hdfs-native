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

use datafusion::error::DataFusionError;
use std::io::ErrorKind;
use thiserror::Error;

/// Errors which can occur during accessing Hdfs cluster
#[derive(Error, Debug)]
pub enum HdfsErr {
    #[error("Unknown hdfs error")]
    Unknown,
    /// file path
    #[error("File not found `{0}`")]
    FileNotFound(String),
    /// file path           
    #[error("File already exists `{0}`")]
    FileAlreadyExists(String),
    /// namenode address
    #[error("Cannot connect to NameNode `{0}`")]
    CannotConnectToNameNode(String),
    /// URL
    #[error("Invalid URL `{0}`")]
    InvalidUrl(String),
}

fn get_error_kind(e: &HdfsErr) -> ErrorKind {
    match e {
        HdfsErr::Unknown => ErrorKind::Other,
        HdfsErr::FileNotFound(_) => ErrorKind::NotFound,
        HdfsErr::FileAlreadyExists(_) => ErrorKind::AlreadyExists,
        HdfsErr::CannotConnectToNameNode(_) => ErrorKind::ConnectionRefused,
        HdfsErr::InvalidUrl(_) => ErrorKind::AddrNotAvailable,
    }
}

impl From<HdfsErr> for DataFusionError {
    fn from(e: HdfsErr) -> DataFusionError {
        let transformed_kind = get_error_kind(&e);
        DataFusionError::IoError(std::io::Error::new(transformed_kind, e))
    }
}

impl From<HdfsErr> for std::io::Error {
    fn from(e: HdfsErr) -> std::io::Error {
        let transformed_kind = get_error_kind(&e);
        std::io::Error::new(transformed_kind, e)
    }
}
