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

fn main() {
    println!("cargo:rustc-link-lib=dylib=hdfs3");

    // It's necessary to use an absolute path here because the
    // C++ codegen and the macro codegen appears to be run from different
    // working directories.
    let path = std::path::PathBuf::from("src");
    let path2 = std::path::PathBuf::from("src/adapter.h");
    let mut b = autocxx_build::build("src/hdfs_shim.rs", &[&path, &path2], &[]).unwrap();
    b.flag_if_supported("-std=c++14")
        .compile("hdfs-native");
    println!("cargo:rerun-if-changed=src/hdfs_shim.rs");
}
