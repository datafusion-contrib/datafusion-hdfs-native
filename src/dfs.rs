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

use std::marker::PhantomData;
use std::mem;
use std::slice;
use std::string::String;
use std::sync::Arc;

use libc::{c_char, c_int, c_short, c_void, time_t};

use crate::err::HdfsErr;
use crate::native::*;
use crate::{b2i, from_raw, to_raw};
use std::fmt::{Debug, Formatter};

const O_RDONLY: c_int = 0;
const O_WRONLY: c_int = 1;
const O_APPEND: c_int = 1024;

/// Options for zero-copy read
pub struct RzOptions {
    ptr: *const hadoopRzOptions,
}

impl Drop for RzOptions {
    fn drop(&mut self) {
        unsafe { hadoopRzOptionsFree(self.ptr) }
    }
}

impl Default for RzOptions {
    fn default() -> Self {
        RzOptions::new()
    }
}

impl RzOptions {
    pub fn new() -> RzOptions {
        RzOptions {
            ptr: unsafe { hadoopRzOptionsAlloc() },
        }
    }

    pub fn skip_checksum(&self, skip: bool) -> Result<bool, HdfsErr> {
        let res = unsafe { hadoopRzOptionsSetSkipChecksum(self.ptr, b2i!(skip)) };

        if res == 0 {
            Ok(true)
        } else {
            Err(HdfsErr::Unknown)
        }
    }

    pub fn set_bytebuffer_pool(&self, class_name: &str) -> Result<bool, HdfsErr> {
        let res = unsafe { hadoopRzOptionsSetByteBufferPool(self.ptr, to_raw!(class_name)) };

        if res == 0 {
            Ok(true)
        } else {
            Err(HdfsErr::Unknown)
        }
    }
}

/// A buffer returned from zero-copy read.
/// This buffer will be automatically freed when its lifetime is finished.
pub struct RzBuffer<'a> {
    file: &'a HdfsFile<'a>,
    ptr: *const hadoopRzBuffer,
}

impl<'a> Drop for RzBuffer<'a> {
    fn drop(&mut self) {
        unsafe { hadoopRzBufferFree(self.file.file, self.ptr) }
    }
}

impl<'a> RzBuffer<'a> {
    /// Get the length of a raw buffer returned from zero-copy read.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> i32 {
        (unsafe { hadoopRzBufferLength(self.ptr) }) as i32
    }

    /// Get a pointer to the raw buffer returned from zero-copy read.
    pub fn as_ptr(&self) -> Result<*const u8, HdfsErr> {
        let ptr = unsafe { hadoopRzBufferGet(self.ptr) };

        if !ptr.is_null() {
            Ok(ptr as *const u8)
        } else {
            Err(HdfsErr::Unknown)
        }
    }

    /// Get a Slice transformed from a raw buffer
    pub fn as_slice(&'a self) -> Result<&[u8], HdfsErr> {
        let ptr = unsafe { hadoopRzBufferGet(self.ptr) as *const u8 };

        let len = unsafe { hadoopRzBufferLength(self.ptr) as usize };

        if !ptr.is_null() {
            Ok(unsafe { mem::transmute(slice::from_raw_parts(ptr, len as usize)) })
        } else {
            Err(HdfsErr::Unknown)
        }
    }
}

/// Includes hostnames where a particular block of a file is stored.
pub struct BlockHosts {
    ptr: *const *const *const c_char,
}

impl Drop for BlockHosts {
    fn drop(&mut self) {
        unsafe { hdfsFreeHosts(self.ptr) };
    }
}

/// Safely deallocable hdfsFileInfo pointer
struct HdfsFileInfoPtr {
    pub ptr: *const hdfsFileInfo,
    pub len: i32,
}

/// for safe deallocation
impl<'a> Drop for HdfsFileInfoPtr {
    fn drop(&mut self) {
        unsafe { hdfsFreeFileInfo(self.ptr, self.len) };
    }
}

impl HdfsFileInfoPtr {
    fn new(ptr: *const hdfsFileInfo) -> HdfsFileInfoPtr {
        HdfsFileInfoPtr { ptr, len: 1 }
    }

    pub fn new_array(ptr: *const hdfsFileInfo, len: i32) -> HdfsFileInfoPtr {
        HdfsFileInfoPtr { ptr, len }
    }
}

/// Interface that represents the client side information for a file or directory.
pub struct FileStatus<'a> {
    raw: Arc<HdfsFileInfoPtr>,
    idx: u32,
    _marker: PhantomData<&'a ()>,
}

impl<'a> FileStatus<'a> {
    #[inline]
    /// create FileStatus from *const hdfsFileInfo
    fn new(ptr: *const hdfsFileInfo) -> FileStatus<'a> {
        FileStatus {
            raw: Arc::new(HdfsFileInfoPtr::new(ptr)),
            idx: 0,
            _marker: PhantomData,
        }
    }

    /// create FileStatus from *const hdfsFileInfo which points
    /// to dynamically allocated array.
    #[inline]
    fn from_array(raw: Arc<HdfsFileInfoPtr>, idx: u32) -> FileStatus<'a> {
        FileStatus {
            raw,
            idx,
            _marker: PhantomData,
        }
    }

    #[inline]
    fn ptr(&self) -> *const hdfsFileInfo {
        unsafe { self.raw.ptr.offset(self.idx as isize) }
    }

    /// Get the name of the file
    #[inline]
    pub fn name(&self) -> &'a str {
        from_raw!((*self.ptr()).mName)
    }

    /// Is this a file?
    #[inline]
    pub fn is_file(&self) -> bool {
        match unsafe { &*self.ptr() }.mKind {
            tObjectKind::kObjectKindFile => true,
            tObjectKind::kObjectKindDirectory => false,
        }
    }

    /// Is this a directory?
    #[inline]
    pub fn is_directory(&self) -> bool {
        match unsafe { &*self.ptr() }.mKind {
            tObjectKind::kObjectKindFile => false,
            tObjectKind::kObjectKindDirectory => true,
        }
    }

    /// Get the owner of the file
    #[inline]
    pub fn owner(&self) -> &'a str {
        from_raw!((*self.ptr()).mOwner)
    }

    /// Get the group associated with the file
    #[inline]
    pub fn group(&self) -> &'a str {
        from_raw!((*self.ptr()).mGroup)
    }

    /// Get the permissions associated with the file
    #[inline]
    pub fn permission(&self) -> i16 {
        unsafe { &*self.ptr() }.mPermissions as i16
    }

    /// Get the length of this file, in bytes.
    #[inline]
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        unsafe { &*self.ptr() }.mSize as usize
    }

    /// Get the block size of the file.
    #[inline]
    pub fn block_size(&self) -> usize {
        unsafe { &*self.ptr() }.mBlockSize as usize
    }

    /// Get the replication factor of a file.
    #[inline]
    pub fn replica_count(&self) -> i16 {
        unsafe { &*self.ptr() }.mReplication as i16
    }

    /// Get the last modification time for the file in seconds
    #[inline]
    pub fn last_modified(&self) -> time_t {
        unsafe { &*self.ptr() }.mLastMod
    }

    /// Get the last access time for the file in seconds
    #[inline]
    pub fn last_accced(&self) -> time_t {
        unsafe { &*self.ptr() }.mLastAccess
    }
}

/// Hdfs Filesystem
///
/// It is basically thread safe because the native API for hdfsFs is thread-safe.
#[derive(Clone)]
pub struct HdfsFs<'a> {
    pub url: String,
    raw: *const hdfsFS,
    _marker: PhantomData<&'a ()>,
}
unsafe impl<'a> Send for HdfsFs<'a> {}
unsafe impl<'a> Sync for HdfsFs<'a> {}

impl<'a> Debug for HdfsFs<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Hdfs").field("url", &self.url).finish()
    }
}

impl<'a> HdfsFs<'a> {
    /// create HdfsFs instance. Please use HdfsFsCache rather than using this API directly.
    #[inline]
    pub(crate) fn new(url: String, raw: *const hdfsFS) -> HdfsFs<'a> {
        HdfsFs {
            url,
            raw,
            _marker: PhantomData,
        }
    }

    /// Get HDFS namenode url
    #[inline]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Get a raw pointer of JNI API's HdfsFs
    #[inline]
    pub fn raw(&self) -> *const hdfsFS {
        self.raw
    }

    /// Open a file for append
    pub fn append(&self, path: &str) -> Result<HdfsFile<'_>, HdfsErr> {
        if !self.exist(path) {
            return Err(HdfsErr::FileNotFound(path.to_owned()));
        }

        let file = unsafe { hdfsOpenFile(self.raw, to_raw!(path), O_APPEND, 0, 0, 0) };

        if file.is_null() {
            Err(HdfsErr::Unknown)
        } else {
            Ok(HdfsFile {
                fs: self,
                path: path.to_owned(),
                file,
            })
        }
    }

    /// set permission
    pub fn chmod(&self, path: &str, mode: i16) -> bool {
        (unsafe { hdfsChmod(self.raw, to_raw!(path), mode as c_short) }) == 0
    }

    pub fn chown(&self, path: &str, owner: &str, group: &str) -> bool {
        (unsafe { hdfsChown(self.raw, to_raw!(path), to_raw!(owner), to_raw!(group)) }) == 0
    }

    #[inline]
    pub fn create(&self, path: &str) -> Result<HdfsFile<'_>, HdfsErr> {
        self.create_with_params(path, false, 0, 0, 0)
    }

    #[inline]
    pub fn create_with_overwrite(
        &self,
        path: &str,
        overwrite: bool,
    ) -> Result<HdfsFile<'_>, HdfsErr> {
        self.create_with_params(path, overwrite, 0, 0, 0)
    }

    pub fn create_with_params(
        &'a self,
        path: &str,
        overwrite: bool,
        buf_size: i32,
        replica_num: i16,
        block_size: i32,
    ) -> Result<HdfsFile<'_>, HdfsErr> {
        if !overwrite && self.exist(path) {
            return Err(HdfsErr::FileAlreadyExists(path.to_owned()));
        }

        let file = unsafe {
            hdfsOpenFile(
                self.raw,
                to_raw!(path),
                O_WRONLY,
                buf_size as c_int,
                replica_num as c_short,
                block_size as i32,
            )
        };

        if file.is_null() {
            Err(HdfsErr::Unknown)
        } else {
            Ok(HdfsFile {
                fs: self,
                path: path.to_owned(),
                file,
            })
        }
    }

    /// Get the default blocksize.
    pub fn default_blocksize(&self) -> Result<usize, HdfsErr> {
        let block_sz = unsafe { hdfsGetDefaultBlockSize(self.raw) };

        if block_sz > 0 {
            Ok(block_sz as usize)
        } else {
            Err(HdfsErr::Unknown)
        }
    }

    /// Get the default blocksize at the filesystem indicated by a given path.
    pub fn block_size(&self, path: &str) -> Result<usize, HdfsErr> {
        let block_sz = unsafe { hdfsGetDefaultBlockSizeAtPath(self.raw, to_raw!(path)) };

        if block_sz > 0 {
            Ok(block_sz as usize)
        } else {
            Err(HdfsErr::Unknown)
        }
    }

    /// Return the raw capacity of the filesystem.
    pub fn capacity(&self) -> Result<usize, HdfsErr> {
        let block_sz = unsafe { hdfsGetCapacity(self.raw) };

        if block_sz > 0 {
            Ok(block_sz as usize)
        } else {
            Err(HdfsErr::Unknown)
        }
    }

    /// Delete file.
    pub fn delete(&self, path: &str, recursive: bool) -> Result<bool, HdfsErr> {
        let res = unsafe { hdfsDelete(self.raw, to_raw!(path), recursive as c_int) };

        if res == 0 {
            Ok(true)
        } else {
            Err(HdfsErr::Unknown)
        }
    }

    /// Checks if a given path exsits on the filesystem
    pub fn exist(&self, path: &str) -> bool {
        unsafe { hdfsExists(self.raw, to_raw!(path)) == 0 }
    }

    /// Get hostnames where a particular block (determined by
    /// pos & blocksize) of a file is stored. The last element in the array
    /// is NULL. Due to replication, a single block could be present on
    /// multiple hosts.
    pub fn get_hosts(
        &self,
        path: &str,
        start: usize,
        length: usize,
    ) -> Result<BlockHosts, HdfsErr> {
        let ptr = unsafe { hdfsGetHosts(self.raw, to_raw!(path), start as i64, length as i64) };

        if !ptr.is_null() {
            Ok(BlockHosts { ptr })
        } else {
            Err(HdfsErr::Unknown)
        }
    }

    /// create a directory
    pub fn mkdir(&self, path: &str) -> Result<bool, HdfsErr> {
        if unsafe { hdfsCreateDirectory(self.raw, to_raw!(path)) } == 0 {
            Ok(true)
        } else {
            Err(HdfsErr::Unknown)
        }
    }

    /// open a file to read
    #[inline]
    pub fn open(&self, path: &str) -> Result<HdfsFile<'_>, HdfsErr> {
        self.open_with_bufsize(path, 0)
    }

    /// open a file to read with a buffer size
    pub fn open_with_bufsize(&self, path: &str, buf_size: i32) -> Result<HdfsFile<'_>, HdfsErr> {
        let file =
            unsafe { hdfsOpenFile(self.raw, to_raw!(path), O_RDONLY, buf_size as c_int, 0, 0) };

        if file.is_null() {
            Err(HdfsErr::Unknown)
        } else {
            Ok(HdfsFile {
                fs: self,
                path: path.to_owned(),
                file,
            })
        }
    }

    /// Set the replication of the specified file to the supplied value
    pub fn set_replication(&self, path: &str, num: i16) -> Result<bool, HdfsErr> {
        let res = unsafe { hdfsSetReplication(self.raw, to_raw!(path), num as i16) };

        if res == 0 {
            Ok(true)
        } else {
            Err(HdfsErr::Unknown)
        }
    }

    /// Rename file.
    pub fn rename(&self, old_path: &str, new_path: &str) -> Result<bool, HdfsErr> {
        let res = unsafe { hdfsRename(self.raw, to_raw!(old_path), to_raw!(new_path)) };

        if res == 0 {
            Ok(true)
        } else {
            Err(HdfsErr::Unknown)
        }
    }

    /// Return the total raw size of all files in the filesystem.
    pub fn used(&self) -> Result<usize, HdfsErr> {
        let block_sz = unsafe { hdfsGetUsed(self.raw) };

        if block_sz > 0 {
            Ok(block_sz as usize)
        } else {
            Err(HdfsErr::Unknown)
        }
    }

    pub fn list_status(&self, path: &str) -> Result<Vec<FileStatus<'_>>, HdfsErr> {
        let mut entry_num: c_int = 0;

        let ptr = unsafe { hdfsListDirectory(self.raw, to_raw!(path), &mut entry_num) };

        if ptr.is_null() {
            return Err(HdfsErr::Unknown);
        }

        let shared_ptr = Arc::new(HdfsFileInfoPtr::new_array(ptr, entry_num));

        let mut list = Vec::new();
        for idx in 0..entry_num {
            list.push(FileStatus::from_array(shared_ptr.clone(), idx as u32));
        }

        Ok(list)
    }

    pub fn get_file_status(&self, path: &str) -> Result<FileStatus<'_>, HdfsErr> {
        let ptr = unsafe { hdfsGetPathInfo(self.raw, to_raw!(path)) };

        if ptr.is_null() {
            Err(HdfsErr::Unknown)
        } else {
            Ok(FileStatus::new(ptr))
        }
    }
}

/// open hdfs file
pub struct HdfsFile<'a> {
    fs: &'a HdfsFs<'a>,
    path: String,
    file: *const hdfsFile,
}

impl<'a> HdfsFile<'a> {
    pub fn available(&self) -> Result<bool, HdfsErr> {
        if unsafe { hdfsAvailable(self.fs.raw, self.file) } == 0 {
            Ok(true)
        } else {
            Err(HdfsErr::Unknown)
        }
    }

    /// Close the opened file
    pub fn close(&self) -> Result<bool, HdfsErr> {
        if unsafe { hdfsCloseFile(self.fs.raw, self.file) } == 0 {
            Ok(true)
        } else {
            Err(HdfsErr::Unknown)
        }
    }

    /// Flush the data.
    pub fn flush(&self) -> bool {
        (unsafe { hdfsFlush(self.fs.raw, self.file) }) == 0
    }

    /// Flush out the data in client's user buffer. After the return of this
    /// call, new readers will see the data.
    pub fn hflush(&self) -> bool {
        (unsafe { hdfsHFlush(self.fs.raw, self.file) }) == 0
    }

    /// Similar to posix fsync, Flush out the data in client's
    /// user buffer. all the way to the disk device (but the disk may have
    /// it in its cache).
    pub fn hsync(&self) -> bool {
        (unsafe { hdfsHSync(self.fs.raw, self.file) }) == 0
    }

    /// Determine if a file is open for read.
    pub fn is_readable(&self) -> bool {
        (unsafe { hdfsFileIsOpenForRead(self.file) }) == 1
    }

    /// Determine if a file is open for write.
    pub fn is_writable(&self) -> bool {
        (unsafe { hdfsFileIsOpenForWrite(self.file) }) == 1
    }

    /// Return a file path
    pub fn path(&'a self) -> &'a str {
        &self.path
    }

    /// Get the current offset in the file, in bytes.
    pub fn pos(&self) -> Result<u64, HdfsErr> {
        let pos = unsafe { hdfsTell(self.fs.raw, self.file) };

        if pos > 0 {
            Ok(pos as u64)
        } else {
            Err(HdfsErr::Unknown)
        }
    }

    /// Read data from an open file.
    pub fn read(&self, buf: &mut [u8]) -> Result<i32, HdfsErr> {
        let read_len = unsafe {
            hdfsRead(
                self.fs.raw,
                self.file,
                buf.as_ptr() as *mut c_void,
                buf.len() as tSize,
            )
        };

        if read_len > 0 {
            Ok(read_len as i32)
        } else {
            Err(HdfsErr::Unknown)
        }
    }

    /// Positional read of data from an open file.
    pub fn read_with_pos(&self, pos: i64, buf: &mut [u8]) -> Result<i32, HdfsErr> {
        let read_len = unsafe {
            hdfsPread(
                self.fs.raw,
                self.file,
                pos as tOffset,
                buf.as_ptr() as *mut c_void,
                buf.len() as tSize,
            )
        };

        if read_len > 0 {
            Ok(read_len as i32)
        } else {
            Err(HdfsErr::Unknown)
        }
    }

    /// Perform a byte buffer read. If possible, this will be a zero-copy
    /// (mmap) read.
    pub fn read_zc(&'a self, opts: &RzOptions, max_len: i32) -> Result<RzBuffer<'a>, HdfsErr> {
        let buf: *const hadoopRzBuffer =
            unsafe { hadoopReadZero(self.file, opts.ptr, max_len as i32) };

        if !buf.is_null() {
            Ok(RzBuffer {
                file: self,
                ptr: buf,
            })
        } else {
            Err(HdfsErr::Unknown)
        }
    }

    /// Seek to given offset in file.
    pub fn seek(&self, offset: u64) -> bool {
        (unsafe { hdfsSeek(self.fs.raw, self.file, offset as tOffset) }) == 0
    }

    /// Write data into an open file.
    pub fn write(&self, buf: &[u8]) -> Result<i32, HdfsErr> {
        let written_len = unsafe {
            hdfsWrite(
                self.fs.raw,
                self.file,
                buf.as_ptr() as *mut c_void,
                buf.len() as tSize,
            )
        };

        if written_len > 0 {
            Ok(written_len)
        } else {
            Err(HdfsErr::Unknown)
        }
    }
}
