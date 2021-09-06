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

use std::ffi::{CStr, CString};
use autocxx::include_cpp;
use autocxx::c_int;
use std::marker::PhantomData;
use std::rc::Rc;
use libc::{time_t, c_char};


include_cpp! {
    #include "hdfs/hdfs.h"

    safety!(unsafe_ffi)

    // generate!("tSize")
    // generate!("tTime")
    // generate!("tOffset")
    // generate!("tPort")
    generate!("tObjectKind")
    generate!("HdfsFileSystemInternalWrapper")
    generate!("hdfsFS")
    generate!("HdfsFileInternalWrapper")
    generate!("hdfsFile")
    generate!("hdfsBuilder")
    generate!("hdfsGetLastError")
    generate!("hdfsFileIsOpenForRead")
    generate!("hdfsFileIsOpenForWrite")
    generate!("hdfsConnectAsUser")
    generate!("hdfsConnect")
    generate!("hdfsConnectAsUserNewInstance")
    generate!("hdfsConnectNewInstance")
    generate!("hdfsBuilderConnect")
    generate!("hdfsNewBuilder")
    generate!("hdfsBuilderSetForceNewInstance")
    generate!("hdfsBuilderSetNameNode")
    generate!("hdfsBuilderSetNameNodePort")
    generate!("hdfsBuilderSetUserName")
    generate!("hdfsBuilderSetKerbTicketCachePath")
    generate!("hdfsBuilderSetToken")
    generate!("hdfsFreeBuilder")
    generate!("hdfsBuilderConfSetStr")
    generate!("hdfsConfGetStr")
    generate!("hdfsConfGetInt")
    generate!("hdfsConfStrFree")
    generate!("hdfsDisconnect")
    generate!("hdfsOpenFile")
    generate!("hdfsCloseFile")
    generate!("hdfsExists")
    generate!("hdfsSeek")
    generate!("hdfsTell")
    generate!("hdfsRead")
    generate!("hdfsWrite")
    generate!("hdfsFlush")
    generate!("hdfsHFlush")
    generate!("hdfsSync")
    generate!("hdfsAvailable")
    generate!("hdfsCopy")
    generate!("hdfsMove")
    generate!("hdfsDelete")
    generate!("hdfsRename")
    generate!("hdfsGetWorkingDirectory")
    generate!("hdfsSetWorkingDirectory")
    generate!("hdfsCreateDirectory")
    generate!("hdfsSetReplication")
    // generate!("hdfsEncryptionZoneInfo")
    // generate!("hdfsEncryptionFileInfo")
    generate!("hdfsFileInfo")
    generate!("hdfsListDirectory")
    generate!("hdfsGetPathInfo")
    generate!("hdfsFreeFileInfo")
    // generate!("hdfsFreeEncryptionZoneInfo")
    generate!("hdfsGetHosts")
    generate!("hdfsFreeHosts")
    generate!("hdfsGetDefaultBlockSize")
    generate!("hdfsGetCapacity")
    generate!("hdfsGetUsed")
    generate!("hdfsChown")
    generate!("hdfsChmod")
    generate!("hdfsUtime")
    generate!("hdfsTruncate")
    // generate!("hdfsGetDelegationToken")
    // generate!("hdfsFreeDelegationToken")
    // generate!("hdfsRenewDelegationToken")
    // generate!("hdfsCancelDelegationToken")
    generate!("Namenode")
    generate!("hdfsGetHANamenodes")
    generate!("hdfsGetHANamenodesWithConfig")
    generate!("hdfsFreeNamenodeInformation")
    generate!("BlockLocation")
    generate!("hdfsGetFileBlockLocations")
    generate!("hdfsFreeFileBlockLocations")
    // generate!("hdfsCreateEncryptionZone")
    // generate!("hdfsGetEZForPath")
    // generate!("hdfsListEncryptionZones")
}

pub fn get_hdfs() -> ffi::hdfsFS {
    let h = CString::new("hdfs://localhost").unwrap();

    unsafe {
        let bld = ffi::hdfsNewBuilder();
        ffi::hdfsBuilderSetNameNode(bld, h.as_ptr());
        ffi::hdfsBuilderSetNameNodePort(bld, 9000);
        ffi::hdfsBuilderConnect(bld)
    }
}

fn get_info(path: &str) -> *mut ffi::hdfsFileInfo {
    unsafe {
        let x = CString::new(path).unwrap();
        let h = x.as_ptr();
        let fs = get_hdfs();
        ffi::hdfsGetPathInfo(fs, h)
    }
}

/// Safely deallocable hdfsFileInfo pointer
struct HdfsFileInfoPtr {
    pub ptr: *mut ffi::hdfsFileInfo,
    pub len: i32,
}

/// for safe deallocation
impl<'a> Drop for HdfsFileInfoPtr {
    fn drop(&mut self) {
        unsafe { ffi::hdfsFreeFileInfo(self.ptr, c_int(self.len)) };
    }
}

impl HdfsFileInfoPtr {
    fn new(ptr: *mut ffi::hdfsFileInfo) -> HdfsFileInfoPtr {
        HdfsFileInfoPtr { ptr, len: 1 }
    }

    pub fn new_array(ptr: *mut ffi::hdfsFileInfo, len: i32) -> HdfsFileInfoPtr {
        HdfsFileInfoPtr { ptr, len }
    }
}


pub fn test_connect() {
    let a = get_info("hdfs://localhost:9000/tpch_parquet/customer");
    println!("{:?}", a)
}
