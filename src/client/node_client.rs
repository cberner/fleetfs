use std::cell::RefCell;
use std::ffi::OsString;
use std::net::SocketAddr;

use crate::base::response_or_error;
use crate::base::{
    EntryMetadata, ErrorCode, FileKind, Request, ResponseView, Timestamp, UserContext,
    WireResponse, encode_request,
};
use crate::client::tcp_client::TcpClient;
use crate::storage::ROOT_INODE;
use fuser::FileAttr;
use std::time::SystemTime;
use zerialize::List;

fn to_fuse_file_type(file_type: FileKind) -> fuser::FileType {
    match file_type {
        FileKind::File => fuser::FileType::RegularFile,
        FileKind::Directory => fuser::FileType::Directory,
        FileKind::Symlink => fuser::FileType::Symlink,
    }
}

pub struct StatFS {
    pub block_size: u32,
    pub max_name_length: u32,
}

fn metadata_to_fuse_fileattr(metadata: &EntryMetadata) -> FileAttr {
    FileAttr {
        ino: fuser::INodeNo(metadata.inode),
        size: metadata.size_bytes,
        blocks: metadata.size_blocks,
        atime: metadata.last_access_time.into(),
        mtime: metadata.last_modified_time.into(),
        ctime: metadata.last_metadata_modified_time.into(),
        crtime: SystemTime::UNIX_EPOCH,
        kind: to_fuse_file_type(metadata.kind),
        perm: metadata.mode,
        nlink: metadata.hard_links,
        uid: metadata.user_id,
        gid: metadata.group_id,
        rdev: metadata.device_id,
        flags: 0,
        blksize: metadata.block_size,
    }
}

thread_local! {
    static RESPONSE_BUFFERS: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

pub struct NodeClient {
    tcp_client: TcpClient,
}

impl NodeClient {
    pub fn new(server_ip_port: SocketAddr) -> NodeClient {
        NodeClient {
            tcp_client: TcpClient::new(server_ip_port),
        }
    }

    fn send<'a>(
        &self,
        request: Request<'_>,
        buffer: &'a mut Vec<u8>,
    ) -> Result<ResponseView<'a>, ErrorCode> {
        let request_buffer = encode_request(&request);
        self.tcp_client
            .send_and_receive(&request_buffer, buffer)
            .map_err(|_| ErrorCode::Uncategorized)?;
        response_or_error(buffer)
    }

    pub fn filesystem_ready(&self) -> Result<(), ErrorCode> {
        RESPONSE_BUFFERS.with_borrow_mut(|buffer| {
            let response = self.send(Request::FilesystemReady, buffer)?;
            response.as_empty_response().ok_or(ErrorCode::BadResponse)?;

            Ok(())
        })
    }

    pub fn fsck(&self) -> Result<(), ErrorCode> {
        RESPONSE_BUFFERS.with_borrow_mut(|buffer| {
            let response = self.send(Request::FilesystemCheck, buffer)?;
            response.as_empty_response().ok_or(ErrorCode::BadResponse)?;

            Ok(())
        })
    }

    pub fn mkdir(
        &self,
        parent: u64,
        name: &str,
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> Result<FileAttr, ErrorCode> {
        let request = Request::Mkdir {
            parent,
            name,
            uid,
            gid,
            mode,
        };

        RESPONSE_BUFFERS.with_borrow_mut(|buffer| {
            let response = self.send(request, buffer)?;

            Ok(metadata_to_fuse_fileattr(
                &response.as_attr_response().unwrap(),
            ))
        })
    }

    pub fn lookup(&self, parent: u64, name: &str, context: UserContext) -> Result<u64, ErrorCode> {
        RESPONSE_BUFFERS.with_borrow_mut(|buffer| {
            let response = self.send(
                Request::Lookup {
                    parent,
                    name,
                    context,
                },
                buffer,
            )?;

            response.as_inode_response().ok_or(ErrorCode::BadResponse)
        })
    }

    pub fn create(
        &self,
        parent: u64,
        name: &str,
        uid: u32,
        gid: u32,
        mode: u16,
        kind: FileKind,
    ) -> Result<FileAttr, ErrorCode> {
        RESPONSE_BUFFERS.with_borrow_mut(|buffer| {
            let response = self.send(
                Request::Create {
                    parent,
                    name,
                    uid,
                    gid,
                    mode,
                    kind,
                },
                buffer,
            )?;
            Ok(metadata_to_fuse_fileattr(
                &response.as_attr_response().unwrap(),
            ))
        })
    }

    pub fn statfs(&self) -> Result<StatFS, ErrorCode> {
        RESPONSE_BUFFERS.with_borrow_mut(|buffer| {
            let response = self.send(Request::FilesystemInformation, buffer)?;
            if let WireResponse::FilesystemInformation {
                block_size,
                max_name_length,
            } = response
            {
                Ok(StatFS {
                    block_size,
                    max_name_length,
                })
            } else {
                Err(ErrorCode::BadResponse)
            }
        })
    }

    pub fn getattr(&self, inode: u64) -> Result<FileAttr, ErrorCode> {
        RESPONSE_BUFFERS.with_borrow_mut(|buffer| {
            let response = self.send(Request::GetAttr { inode }, buffer)?;

            Ok(metadata_to_fuse_fileattr(
                &response.as_attr_response().unwrap(),
            ))
        })
    }

    pub fn getxattr(
        &self,
        inode: u64,
        key: &str,
        context: UserContext,
    ) -> Result<Vec<u8>, ErrorCode> {
        RESPONSE_BUFFERS.with_borrow_mut(|buffer| {
            let response = self.send(
                Request::GetXattr {
                    inode,
                    key,
                    context,
                },
                buffer,
            )?;
            let data = response.as_read_response().ok_or(ErrorCode::BadResponse)?;

            Ok(data.to_vec())
        })
    }

    pub fn listxattr(&self, inode: u64) -> Result<Vec<String>, ErrorCode> {
        RESPONSE_BUFFERS.with_borrow_mut(|buffer| {
            let response = self.send(Request::ListXattrs { inode }, buffer)?;

            let xattrs = response
                .as_xattrs_response()
                .ok_or(ErrorCode::BadResponse)?;

            let attrs = xattrs.iter().map(|x| x.to_string()).collect();

            Ok(attrs)
        })
    }

    pub fn setxattr(
        &self,
        inode: u64,
        key: &str,
        value: &[u8],
        context: UserContext,
    ) -> Result<(), ErrorCode> {
        RESPONSE_BUFFERS.with_borrow_mut(|buffer| {
            let response = self.send(
                Request::SetXattr {
                    inode,
                    key,
                    value,
                    context,
                },
                buffer,
            )?;
            response.as_empty_response().ok_or(ErrorCode::BadResponse)?;

            Ok(())
        })
    }

    pub fn removexattr(
        &self,
        inode: u64,
        key: &str,
        context: UserContext,
    ) -> Result<(), ErrorCode> {
        let request = Request::RemoveXattr {
            inode,
            key,
            context,
        };

        RESPONSE_BUFFERS.with_borrow_mut(|buffer| {
            let response = self.send(request, buffer)?;
            response.as_empty_response().ok_or(ErrorCode::BadResponse)?;

            Ok(())
        })
    }

    pub fn utimens(
        &self,
        inode: u64,
        atime: Option<Timestamp>,
        mtime: Option<Timestamp>,
        context: UserContext,
    ) -> Result<(), ErrorCode> {
        assert_ne!(inode, ROOT_INODE);
        let request = Request::Utimens {
            inode,
            atime,
            mtime,
            context,
        };

        RESPONSE_BUFFERS.with_borrow_mut(|buffer| {
            let response = self.send(request, buffer)?;
            response.as_empty_response().ok_or(ErrorCode::BadResponse)?;

            Ok(())
        })
    }

    pub fn chmod(&self, inode: u64, mode: u32, context: UserContext) -> Result<(), ErrorCode> {
        if inode == ROOT_INODE {
            return Err(ErrorCode::OperationNotPermitted);
        }
        let request = Request::Chmod {
            inode,
            mode,
            context,
        };

        RESPONSE_BUFFERS.with_borrow_mut(|buffer| {
            let response = self.send(request, buffer)?;
            response.as_empty_response().ok_or(ErrorCode::BadResponse)?;

            Ok(())
        })
    }

    pub fn chown(
        &self,
        inode: u64,
        uid: Option<u32>,
        gid: Option<u32>,
        context: UserContext,
    ) -> Result<(), ErrorCode> {
        assert_ne!(inode, ROOT_INODE);
        let request = Request::Chown {
            inode,
            uid,
            gid,
            context,
        };

        RESPONSE_BUFFERS.with_borrow_mut(|buffer| {
            let response = self.send(request, buffer)?;
            response.as_empty_response().ok_or(ErrorCode::BadResponse)?;

            Ok(())
        })
    }

    pub fn hardlink(
        &self,
        inode: u64,
        new_parent: u64,
        new_name: &str,
        context: UserContext,
    ) -> Result<FileAttr, ErrorCode> {
        assert_ne!(inode, ROOT_INODE);
        let request = Request::Hardlink {
            inode,
            new_parent,
            new_name,
            context,
        };

        RESPONSE_BUFFERS.with_borrow_mut(|buffer| {
            let response = self.send(request, buffer)?;

            Ok(metadata_to_fuse_fileattr(
                &response.as_attr_response().unwrap(),
            ))
        })
    }

    pub fn rename(
        &self,
        parent: u64,
        name: &str,
        new_parent: u64,
        new_name: &str,
        context: UserContext,
    ) -> Result<(), ErrorCode> {
        let request = Request::Rename {
            parent,
            name,
            new_parent,
            new_name,
            context,
        };

        RESPONSE_BUFFERS.with_borrow_mut(|buffer| {
            let response = self.send(request, buffer)?;
            response.as_empty_response().ok_or(ErrorCode::BadResponse)?;

            Ok(())
        })
    }

    pub fn readlink(&self, inode: u64) -> Result<Vec<u8>, ErrorCode> {
        assert_ne!(inode, ROOT_INODE);

        // TODO: this just tries to read a value longer than the longest link.
        // instead we should be using a special readlink message
        RESPONSE_BUFFERS.with_borrow_mut(|buffer| {
            let response = self.send(
                Request::Read {
                    inode,
                    offset: 0,
                    read_size: 999_999,
                },
                buffer,
            )?;
            Ok(response
                .as_read_response()
                .ok_or(ErrorCode::BadResponse)?
                .to_vec())
        })
    }

    pub fn read<F: FnOnce(Result<&[u8], ErrorCode>)>(
        &self,
        inode: u64,
        offset: u64,
        size: u32,
        callback: F,
    ) {
        assert_ne!(inode, ROOT_INODE);

        RESPONSE_BUFFERS.with_borrow_mut(|buffer| {
            match self.send(
                Request::Read {
                    inode,
                    offset,
                    read_size: size,
                },
                buffer,
            ) {
                Ok(response) => {
                    let data = response.as_read_response().unwrap();
                    callback(Ok(data));
                }
                Err(e) => {
                    callback(Err(e));
                }
            };
        })
    }

    pub fn readdir(&self, inode: u64) -> Result<Vec<(u64, OsString, fuser::FileType)>, ErrorCode> {
        RESPONSE_BUFFERS.with_borrow_mut(|buffer| {
            let response = self.send(Request::ListDir { inode }, buffer)?;

            let mut result = vec![];
            let entries = response
                .as_directory_listing_response()
                .ok_or(ErrorCode::BadResponse)?;
            for entry in entries.iter() {
                result.push((
                    entry.inode,
                    OsString::from(entry.name),
                    to_fuse_file_type(entry.kind),
                ));
            }

            Ok(result)
        })
    }

    pub fn truncate(&self, inode: u64, length: u64, context: UserContext) -> Result<(), ErrorCode> {
        assert_ne!(inode, ROOT_INODE);
        let request = Request::Truncate {
            inode,
            new_length: length,
            context,
        };

        RESPONSE_BUFFERS.with_borrow_mut(|buffer| {
            let response = self.send(request, buffer)?;
            response.as_empty_response().ok_or(ErrorCode::BadResponse)?;

            Ok(())
        })
    }

    pub fn write(&self, inode: u64, data: &[u8], offset: u64) -> Result<u32, ErrorCode> {
        RESPONSE_BUFFERS.with_borrow_mut(|buffer| {
            let response = self.send(
                Request::Write {
                    inode,
                    offset,
                    data,
                },
                buffer,
            )?;

            response
                .as_bytes_written_response()
                .ok_or(ErrorCode::BadResponse)
        })
    }

    pub fn fsync(&self, inode: u64) -> Result<(), ErrorCode> {
        RESPONSE_BUFFERS.with_borrow_mut(|buffer| {
            let response = self.send(Request::Fsync { inode }, buffer)?;
            response.as_empty_response().ok_or(ErrorCode::BadResponse)?;

            Ok(())
        })
    }

    pub fn unlink(&self, parent: u64, name: &str, context: UserContext) -> Result<(), ErrorCode> {
        let request = Request::Unlink {
            parent,
            name,
            context,
        };

        RESPONSE_BUFFERS.with_borrow_mut(|buffer| {
            let response = self.send(request, buffer)?;
            response.as_empty_response().ok_or(ErrorCode::BadResponse)?;

            Ok(())
        })
    }

    pub fn rmdir(&self, parent: u64, name: &str, context: UserContext) -> Result<(), ErrorCode> {
        let request = Request::Rmdir {
            parent,
            name,
            context,
        };

        RESPONSE_BUFFERS.with_borrow_mut(|buffer| {
            let response = self.send(request, buffer)?;
            response.as_empty_response().ok_or(ErrorCode::BadResponse)?;

            Ok(())
        })
    }
}
