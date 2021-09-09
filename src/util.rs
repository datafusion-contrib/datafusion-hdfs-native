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

use std::str;

use crate::dfs::HdfsFs;
use crate::err::HdfsErr;
use crate::native::*;

#[macro_export]
macro_rules! to_raw {
    ($str:expr) => {{
        let c_str = std::ffi::CString::new($str).unwrap();
        c_str.into_raw()
    }};
}

#[macro_export]
macro_rules! from_raw {
    ($chars:expr) => {{
        let slice = unsafe { std::ffi::CStr::from_ptr($chars) }.to_bytes();
        std::str::from_utf8(slice).unwrap()
    }};
}

// pub fn chars_to_str<'a>(chars: *const c_char) -> &'a str {
//     let slice = unsafe { CStr::from_ptr(chars) }.to_bytes();
//     str::from_utf8(slice).unwrap()
// }

#[macro_export]
macro_rules! b2i {
    ($b:expr) => {{
        if $b {
            1
        } else {
            0
        }
    }};
}

#[macro_export]
macro_rules! cr {
    ($result:expr) => {{
        $result.map_err(|e| e.into())
    }};
}

/// Hdfs Utility
pub struct HdfsUtil;

/// HDFS Utility
impl HdfsUtil {
    /// Copy file from one filesystem to another.
    ///
    /// #### Params
    /// * ```srcFS``` - The handle to source filesystem.
    /// * ```src``` - The path of source file.
    /// * ```dstFS``` - The handle to destination filesystem.
    /// * ```dst``` - The path of destination file.
    pub fn copy(
        src_fs: &HdfsFs<'_>,
        src: &str,
        dst_fs: &HdfsFs<'_>,
        dst: &str,
    ) -> Result<bool, HdfsErr> {
        let res = unsafe { hdfsCopy(src_fs.raw(), to_raw!(src), dst_fs.raw(), to_raw!(dst)) };

        if res == 0 {
            Ok(true)
        } else {
            Err(HdfsErr::Unknown)
        }
    }

    /// Move file from one filesystem to another.
    ///
    /// #### Params
    /// * ```srcFS``` - The handle to source filesystem.
    /// * ```src``` - The path of source file.
    /// * ```dstFS``` - The handle to destination filesystem.
    /// * ```dst``` - The path of destination file.
    pub fn mv(
        src_fs: &HdfsFs<'_>,
        src: &str,
        dst_fs: &HdfsFs<'_>,
        dst: &str,
    ) -> Result<bool, HdfsErr> {
        let res = unsafe { hdfsMove(src_fs.raw(), to_raw!(src), dst_fs.raw(), to_raw!(dst)) };

        if res == 0 {
            Ok(true)
        } else {
            Err(HdfsErr::Unknown)
        }
    }
}
