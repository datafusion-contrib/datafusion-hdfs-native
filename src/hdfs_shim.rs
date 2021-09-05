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

use std::ffi::CString;
use autocxx::include_cpp;

include_cpp! {
    #include "hdfs/hdfs.h"
    safety!(unsafe_ffi)
    generate!("hdfsBuilderConnect")
    generate!("hdfsBuilderSetNameNode")
    generate!("hdfsBuilderSetNameNodePort")
    generate!("hdfsNewBuilder")
    generate!("hdfsBuilder")
}

pub fn test_connect() {
    let h = CString::new("hdfs://localhost").unwrap();

    unsafe {
        let bld = ffi::hdfsNewBuilder();
        ffi::hdfsBuilderSetNameNode(bld, h.as_ptr());
        ffi::hdfsBuilderSetNameNodePort(bld, 10010);
        ffi::hdfsBuilderConnect(bld);
    }
}
