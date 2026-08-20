use redb::{TypeName, Value};
use redb_derive::Value;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::ops::Add;
use std::time::{Duration, SystemTime};
use zerialize::{List, ListView, Zerializable, zerializable};

pub struct RequestMetaInfo {
    pub raft_group: Option<u16>,
    pub inode: Option<u64>, // Some if the request accesses a single inode (i.e. None for Rename)
    pub lock_id: Option<u64>,
    pub access_type: AccessType, // Used to determine locks to acquire
    pub distribution_requirement: DistributionRequirement,
}

pub enum AccessType {
    ReadData,
    ReadMetadata,
    LockMetadata,
    WriteMetadata,
    WriteDataAndMetadata,
    NoAccess,
}

// Where this message can be processed
pub enum DistributionRequirement {
    Any,                    // Any node can process this message
    TransactionCoordinator, // Any node can process this message by acting as a transcation coordinator
    RaftGroup,              // Must be processed by a specific rgroup
    Node,                   // Must be processed by a specific node
}

#[derive(Zerializable, Debug, PartialEq, Eq, Clone, Copy)]
pub enum ErrorCode {
    #[variant(0)]
    DoesNotExist,
    #[variant(1)]
    InodeDoesNotExist,
    #[variant(2)]
    FileTooLarge,
    #[variant(3)]
    AccessDenied,
    #[variant(4)]
    OperationNotPermitted,
    #[variant(5)]
    AlreadyExists,
    #[variant(6)]
    NameTooLong,
    #[variant(7)]
    NotEmpty,
    #[variant(8)]
    MissingXattrKey,
    #[variant(9)]
    BadResponse,
    #[variant(10)]
    BadRequest,
    #[variant(11)]
    Corrupted,
    #[variant(12)]
    RaftFailure,
    #[variant(13)]
    InvalidXattrNamespace,
    #[variant(14)]
    Uncategorized,
}

#[derive(Zerializable, Debug, PartialEq, Eq, Clone, Copy)]
pub enum FileKind {
    #[variant(0)]
    File,
    #[variant(1)]
    Directory,
    #[variant(2)]
    Symlink,
}

impl Value for FileKind {
    type SelfType<'a> = FileKind;
    type AsBytes<'a> = [u8; 1];

    fn fixed_width() -> Option<usize> {
        Some(1)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        match data[0] {
            1 => FileKind::File,
            2 => FileKind::Directory,
            3 => FileKind::Symlink,
            _ => unreachable!(),
        }
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        match value {
            FileKind::File => [1],
            FileKind::Directory => [2],
            FileKind::Symlink => [3],
        }
    }

    fn type_name() -> TypeName {
        TypeName::new("fleetfs::FileKind")
    }
}

#[derive(Zerializable, Debug, PartialEq, Eq, Clone, Copy)]
pub struct CommitId {
    #[n(0)]
    pub term: u64,
    #[n(1)]
    pub index: u64,
}

impl CommitId {
    pub fn new(term: u64, index: u64) -> Self {
        Self { term, index }
    }
}

#[derive(Zerializable, Debug, PartialEq, Eq, Clone, Copy)]
pub struct InodeUidPair {
    #[n(0)]
    pub inode: u64,
    #[n(1)]
    pub uid: u32,
}

impl InodeUidPair {
    pub fn new(inode: u64, uid: u32) -> Self {
        Self { inode, uid }
    }
}

#[derive(Zerializable, Debug, PartialEq, Eq, Clone, Copy)]
pub struct UserContext {
    #[n(0)]
    pub uid: u32,
    #[n(1)]
    pub gid: u32,
}

impl UserContext {
    pub fn new(uid: u32, gid: u32) -> Self {
        Self { uid, gid }
    }

    pub fn uid(&self) -> u32 {
        self.uid
    }

    pub fn gid(&self) -> u32 {
        self.gid
    }
}

#[derive(Zerializable, Debug, PartialEq, Eq, Clone, Copy, Value)]
pub struct Timestamp {
    #[n(0)]
    pub seconds: i64,
    #[n(1)]
    pub nanos: i32,
}

impl Timestamp {
    pub fn new(seconds: i64, nanos: i32) -> Self {
        Self { seconds, nanos }
    }
}

impl From<Timestamp> for SystemTime {
    fn from(timestamp: Timestamp) -> Self {
        SystemTime::UNIX_EPOCH.add(Duration::new(
            timestamp.seconds as u64,
            timestamp.nanos as u32,
        ))
    }
}

#[derive(Zerializable, Debug, PartialEq, Eq, Clone, Copy)]
pub struct EntryMetadata {
    #[n(0)]
    pub inode: u64,
    #[n(1)]
    pub size_bytes: u64,
    #[n(2)]
    pub size_blocks: u64,
    #[n(3)]
    pub last_access_time: Timestamp,
    #[n(4)]
    pub last_modified_time: Timestamp,
    #[n(5)]
    pub last_metadata_modified_time: Timestamp,
    #[n(6)]
    pub kind: FileKind,
    #[n(7)]
    pub mode: u16,
    #[n(8)]
    pub hard_links: u32,
    #[n(9)]
    pub user_id: u32,
    #[n(10)]
    pub group_id: u32,
    #[n(11)]
    pub device_id: u32,
    #[n(12)]
    pub block_size: u32,
    // The number of directory entries in the directory. Only available if kind == Directory
    #[n(13)]
    pub directory_entries: Option<u32>,
}

/// One entry of a directory listing, pointing into the message it was read from
#[derive(Zerializable, Debug, PartialEq, Eq, Clone, Copy)]
pub struct DirectoryEntry<'a> {
    #[n(0)]
    pub inode: u64,
    #[n(1)]
    pub name: &'a str,
    #[n(2)]
    pub kind: FileKind,
}

/// The checksum of one raft group's data, pointing into the message it was read from
#[derive(Zerializable, Debug, PartialEq, Eq, Clone, Copy)]
pub struct Checksum<'a> {
    #[n(0)]
    pub raft_group: u16,
    #[n(1)]
    pub checksum: &'a [u8],
}

/// A request, over the buffer it was decoded from or over data the sender owns.
///
/// The same type is what a client builds to send and what a server reads, since
/// every field it carries is either copied or a handle over bytes the builder
/// already has.
#[zerializable]
pub enum Request<'a> {
    #[variant(0)]
    FilesystemReady,
    #[variant(1)]
    FilesystemInformation,
    #[variant(2)]
    FilesystemChecksum,
    #[variant(3)]
    FilesystemCheck,
    #[variant(4)]
    Fsync {
        #[n(0)]
        inode: u64,
    },
    #[variant(5)]
    GetAttr {
        #[n(0)]
        inode: u64,
    },
    #[variant(6)]
    ListDir {
        #[n(0)]
        inode: u64,
    },
    #[variant(7)]
    Create {
        #[n(0)]
        parent: u64,
        #[n(1)]
        name: &'a str,
        #[n(2)]
        uid: u32,
        #[n(3)]
        gid: u32,
        #[n(4)]
        mode: u16,
        #[n(5)]
        kind: FileKind,
    },
    #[variant(8)]
    Mkdir {
        #[n(0)]
        parent: u64,
        #[n(1)]
        name: &'a str,
        #[n(2)]
        uid: u32,
        #[n(3)]
        gid: u32,
        #[n(4)]
        mode: u16,
    },
    #[variant(9)]
    Unlink {
        #[n(0)]
        parent: u64,
        #[n(1)]
        name: &'a str,
        #[n(2)]
        context: UserContext,
    },
    #[variant(10)]
    Rmdir {
        #[n(0)]
        parent: u64,
        #[n(1)]
        name: &'a str,
        #[n(2)]
        context: UserContext,
    },
    #[variant(11)]
    Truncate {
        #[n(0)]
        inode: u64,
        #[n(1)]
        new_length: u64,
        #[n(2)]
        context: UserContext,
    },
    #[variant(12)]
    Rename {
        #[n(0)]
        parent: u64,
        #[n(1)]
        name: &'a str,
        #[n(2)]
        new_parent: u64,
        #[n(3)]
        new_name: &'a str,
        #[n(4)]
        context: UserContext,
    },
    #[variant(13)]
    Lookup {
        #[n(0)]
        parent: u64,
        #[n(1)]
        name: &'a str,
        #[n(2)]
        context: UserContext,
    },
    #[variant(14)]
    Chown {
        #[n(0)]
        inode: u64,
        #[n(1)]
        uid: Option<u32>,
        #[n(2)]
        gid: Option<u32>,
        #[n(3)]
        context: UserContext,
    },
    #[variant(15)]
    Chmod {
        #[n(0)]
        inode: u64,
        #[n(1)]
        mode: u32,
        #[n(2)]
        context: UserContext,
    },
    #[variant(16)]
    Utimens {
        #[n(0)]
        inode: u64,
        #[n(1)]
        atime: Option<Timestamp>,
        #[n(2)]
        mtime: Option<Timestamp>,
        #[n(3)]
        context: UserContext,
    },
    #[variant(17)]
    Hardlink {
        #[n(0)]
        inode: u64,
        #[n(1)]
        new_parent: u64,
        #[n(2)]
        new_name: &'a str,
        #[n(3)]
        context: UserContext,
    },
    #[variant(18)]
    ListXattrs {
        #[n(0)]
        inode: u64,
    },
    #[variant(19)]
    GetXattr {
        #[n(0)]
        inode: u64,
        #[n(1)]
        key: &'a str,
        #[n(2)]
        context: UserContext,
    },
    #[variant(20)]
    SetXattr {
        #[n(0)]
        inode: u64,
        #[n(1)]
        key: &'a str,
        #[n(2)]
        value: &'a [u8],
        #[n(3)]
        context: UserContext,
    },
    #[variant(21)]
    RemoveXattr {
        #[n(0)]
        inode: u64,
        #[n(1)]
        key: &'a str,
        #[n(2)]
        context: UserContext,
    },
    // Reads only the blocks of data on this node
    #[variant(22)]
    ReadRaw {
        #[n(0)]
        required_commit: CommitId,
        #[n(1)]
        inode: u64,
        #[n(2)]
        offset: u64,
        #[n(3)]
        read_size: u32,
    },
    #[variant(23)]
    Read {
        #[n(0)]
        inode: u64,
        #[n(1)]
        offset: u64,
        #[n(2)]
        read_size: u32,
    },
    #[variant(24)]
    Write {
        #[n(0)]
        inode: u64,
        #[n(1)]
        offset: u64,
        #[n(2)]
        data: &'a [u8],
    },
    #[variant(25)]
    LatestCommit {
        #[n(0)]
        raft_group: u16,
    },
    #[variant(26)]
    RaftGroupLeader {
        #[n(0)]
        raft_group: u16,
    },
    #[variant(27)]
    ConsensusMessage {
        #[n(0)]
        raft_group: u16,
        #[n(1)]
        data: &'a [u8],
    },
    // Internal request to lock an inode
    #[variant(28)]
    Lock {
        #[n(0)]
        inode: u64,
    },
    // Internal request to unlock an inode
    #[variant(29)]
    Unlock {
        #[n(0)]
        inode: u64,
        #[n(1)]
        lock_id: u64,
    },

    // Internal transaction messages

    // Used internally to rollback hardlink transactions
    #[variant(30)]
    HardlinkRollback {
        #[n(0)]
        inode: u64,
        #[n(1)]
        last_modified_time: Timestamp,
    },
    // Internal request to create directory link as part of a transaction. Does not increment the inode's link count.
    #[variant(31)]
    CreateLink {
        #[n(0)]
        parent: u64,
        #[n(1)]
        name: &'a str,
        #[n(2)]
        inode: u64,
        #[n(3)]
        kind: FileKind,
        #[n(4)]
        lock_id: Option<u64>,
        #[n(5)]
        context: UserContext,
    },
    // Internal request to atomically replace a directory link, so that it points to a different inode,
    // as part of a transaction. Does not change either inode's link count. It is the callers responsibility to ensure
    // that replacing the existing link is safe (i.e. it doesn't point to a non-empty directory)
    #[variant(32)]
    ReplaceLink {
        #[n(0)]
        parent: u64,
        #[n(1)]
        name: &'a str,
        #[n(2)]
        new_inode: u64,
        #[n(3)]
        kind: FileKind,
        #[n(4)]
        lock_id: Option<u64>,
        #[n(5)]
        context: UserContext,
    },
    // Used internally to remove a link entry from a directory. Does *not* decrement the hard link count of the target inode
    #[variant(33)]
    RemoveLink {
        #[n(0)]
        parent: u64,
        #[n(1)]
        name: &'a str,
        #[n(2)]
        link_inode_and_uid: Option<InodeUidPair>,
        #[n(3)]
        lock_id: Option<u64>,
        #[n(4)]
        context: UserContext,
    },
    // Internal request to create an inode as part of a create() or mkdir() transaction
    #[variant(34)]
    CreateInode {
        #[n(0)]
        raft_group: u16,
        #[n(1)]
        parent: u64,
        #[n(2)]
        uid: u32,
        #[n(3)]
        gid: u32,
        #[n(4)]
        mode: u16,
        #[n(5)]
        kind: FileKind,
    },
    // Used internally for stage0 of hardlink transactions
    #[variant(35)]
    HardlinkIncrement {
        #[n(0)]
        inode: u64,
    },
    // Internal request to update the parent link of a directory inode
    #[variant(36)]
    UpdateParent {
        #[n(0)]
        inode: u64,
        #[n(1)]
        new_parent: u64,
        #[n(2)]
        lock_id: Option<u64>,
    },
    // Internal request to update the metadata changed time an inode
    #[variant(37)]
    UpdateMetadataChangedTime {
        #[n(0)]
        inode: u64,
        #[n(1)]
        lock_id: Option<u64>,
    },
    // TODO: raft messages have to be idempotent. This one is not.
    // Internal request to decrement inode link count. Will delete the inode if its count reaches zero.
    #[variant(38)]
    DecrementInode {
        #[n(0)]
        inode: u64,
        // The number of times to decrement the link count
        #[n(1)]
        decrement_count: u32,
        #[n(2)]
        lock_id: Option<u64>,
    },
}

/// A response, over the buffer it was decoded from or over data the sender owns.
///
/// The lists it carries are parameters, because a list is a handle over the
/// buffer rather than a name: [`ResponseView`] is what the wire format is, and
/// [`Response`] is what a handler builds, handed out over this by
/// [`Response::as_view`].
#[zerializable]
pub enum WireResponse<
    'a,
    X: List<Item = &'a str>,
    D: List<Item = DirectoryEntry<'a>>,
    C: List<Item = Checksum<'a>>,
> {
    #[variant(0)]
    Lock {
        #[n(0)]
        lock_id: u64,
    },
    #[variant(1)]
    FilesystemInformation {
        #[n(0)]
        block_size: u32,
        #[n(1)]
        max_name_length: u32,
    },
    #[variant(2)]
    NodeId {
        #[n(0)]
        id: u64,
    },
    #[variant(3)]
    Inode {
        #[n(0)]
        id: u64,
    },
    #[variant(4)]
    RemovedInode {
        #[n(0)]
        id: u64,
        #[n(1)]
        complete: bool,
    },
    #[variant(5)]
    Written {
        #[n(0)]
        bytes_written: u32,
    },
    #[variant(6)]
    Xattrs {
        #[n(0)]
        attrs: X,
    },
    #[variant(7)]
    Read {
        #[n(0)]
        data: &'a [u8],
    },
    #[variant(8)]
    LatestCommit {
        #[n(0)]
        term: u64,
        #[n(1)]
        index: u64,
    },
    #[variant(9)]
    DirectoryListing(#[n(0)] D),
    #[variant(10)]
    EntryMetadata(#[n(0)] EntryMetadata),
    #[variant(11)]
    HardlinkTransaction {
        #[n(0)]
        rollback_last_modified: Timestamp,
        #[n(1)]
        attrs: EntryMetadata,
    },
    #[variant(12)]
    Empty,
    #[variant(13)]
    ErrorOccurred(#[n(0)] ErrorCode),
    // Mapping from raft group ids to their checksum
    #[variant(14)]
    Checksums(#[n(0)] C),
}

/// The schema a response is encoded as, and the view decoding one gives back
pub type ResponseView<'a> = WireResponse<
    'a,
    ListView<'a, str>,
    ListView<'a, DirectoryEntry<'static>>,
    ListView<'a, Checksum<'static>>,
>;

/// A list of strings owned elsewhere, handed out as the `&str`s a response holds
pub struct StrList<'a>(&'a [String]);

impl<'a> List for StrList<'a> {
    type Item = &'a str;

    fn len(&self) -> usize {
        self.0.len()
    }

    fn get(&self, index: usize) -> Option<&'a str> {
        self.0.get(index).map(String::as_str)
    }
}

/// A directory listing owned elsewhere, handed out as the entries a response holds
pub struct DirectoryEntryList<'a>(&'a [OwnedDirectoryEntry]);

impl<'a> List for DirectoryEntryList<'a> {
    type Item = DirectoryEntry<'a>;

    fn len(&self) -> usize {
        self.0.len()
    }

    fn get(&self, index: usize) -> Option<DirectoryEntry<'a>> {
        self.0.get(index).map(|entry| DirectoryEntry {
            inode: entry.inode,
            name: &entry.name,
            kind: entry.kind,
        })
    }
}

/// Checksums owned elsewhere, handed out as the pairs a response holds
pub struct ChecksumList<'a>(Vec<(u16, &'a [u8])>);

impl<'a> List for ChecksumList<'a> {
    type Item = Checksum<'a>;

    fn len(&self) -> usize {
        self.0.len()
    }

    fn get(&self, index: usize) -> Option<Checksum<'a>> {
        self.0.get(index).map(|(raft_group, checksum)| Checksum {
            raft_group: *raft_group,
            checksum,
        })
    }
}

/// The response as a handler builds it, owning what it will be encoded from
pub type OwnedResponseView<'a> =
    WireResponse<'a, StrList<'a>, DirectoryEntryList<'a>, ChecksumList<'a>>;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct OwnedDirectoryEntry {
    pub inode: u64,
    pub name: String,
    pub kind: FileKind,
}

/// A response a handler owns, which is what it is encoded from.
///
/// The wire format is [`WireResponse`], whose borrowed fields point into the
/// message they were read from, so a handler that builds a response rather than
/// decoding one owns it as this and hands it out by [`Response::as_view`].
#[derive(Debug)]
pub enum Response {
    Lock {
        lock_id: u64,
    },
    FilesystemInformation {
        block_size: u32,
        max_name_length: u32,
    },
    NodeId {
        id: u64,
    },
    Inode {
        id: u64,
    },
    RemovedInode {
        id: u64,
        complete: bool,
    },
    Written {
        bytes_written: u32,
    },
    Xattrs {
        attrs: Vec<String>,
    },
    Read {
        data: Vec<u8>,
    },
    LatestCommit {
        term: u64,
        index: u64,
    },
    DirectoryListing(Vec<OwnedDirectoryEntry>),
    EntryMetadata(EntryMetadata),
    HardlinkTransaction {
        rollback_last_modified: Timestamp,
        attrs: EntryMetadata,
    },
    Empty,
    ErrorOccurred(ErrorCode),
    // Mapping from raft group ids to their checksum
    Checksums(HashMap<u16, Vec<u8>>),
}

impl Response {
    pub fn as_view(&self) -> OwnedResponseView<'_> {
        match self {
            Response::Lock { lock_id } => WireResponse::Lock { lock_id: *lock_id },
            Response::FilesystemInformation {
                block_size,
                max_name_length,
            } => WireResponse::FilesystemInformation {
                block_size: *block_size,
                max_name_length: *max_name_length,
            },
            Response::NodeId { id } => WireResponse::NodeId { id: *id },
            Response::Inode { id } => WireResponse::Inode { id: *id },
            Response::RemovedInode { id, complete } => WireResponse::RemovedInode {
                id: *id,
                complete: *complete,
            },
            Response::Written { bytes_written } => WireResponse::Written {
                bytes_written: *bytes_written,
            },
            Response::Xattrs { attrs } => WireResponse::Xattrs {
                attrs: StrList(attrs),
            },
            Response::Read { data } => WireResponse::Read { data },
            Response::LatestCommit { term, index } => WireResponse::LatestCommit {
                term: *term,
                index: *index,
            },
            Response::DirectoryListing(entries) => {
                WireResponse::DirectoryListing(DirectoryEntryList(entries))
            }
            Response::EntryMetadata(metadata) => WireResponse::EntryMetadata(*metadata),
            Response::HardlinkTransaction {
                rollback_last_modified,
                attrs,
            } => WireResponse::HardlinkTransaction {
                rollback_last_modified: *rollback_last_modified,
                attrs: *attrs,
            },
            Response::Empty => WireResponse::Empty,
            Response::ErrorOccurred(error_code) => WireResponse::ErrorOccurred(*error_code),
            Response::Checksums(checksums) => WireResponse::Checksums(ChecksumList(
                checksums
                    .iter()
                    .map(|(raft_group, checksum)| (*raft_group, checksum.as_slice()))
                    .collect(),
            )),
        }
    }
}

/// Encodes `response` as the wire format
pub fn encode_response(response: &Response) -> Vec<u8> {
    zerialize::encode::<ResponseView<'_>>(&response.as_view())
}

/// Decodes a response, which is a handle over `buffer`
pub fn decode_response(buffer: &[u8]) -> Result<ResponseView<'_>, zerialize::Error> {
    zerialize::decode::<ResponseView<'_>>(buffer)
}

/// Encodes `request` as the wire format
pub fn encode_request(request: &Request<'_>) -> Vec<u8> {
    zerialize::encode::<Request<'_>>(request)
}

/// Decodes a request, which is a handle over `buffer`
pub fn decode_request(buffer: &[u8]) -> Result<Request<'_>, zerialize::Error> {
    zerialize::decode::<Request<'_>>(buffer)
}

// Only the variant, and the inode or raft group it names, so that a log line
// never carries the data a write or an xattr holds
impl Debug for Request<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Request::FilesystemReady => write!(f, "FilesystemReady"),
            Request::FilesystemInformation => write!(f, "FilesystemInformation"),
            Request::FilesystemChecksum => write!(f, "FilesystemChecksum"),
            Request::FilesystemCheck => write!(f, "FilesystemCheck"),
            Request::Create { .. } => write!(f, "Create"),
            Request::Mkdir { .. } => write!(f, "Mkdir"),
            Request::Unlink { .. } => write!(f, "Unlink"),
            Request::Truncate { .. } => write!(f, "Truncate"),
            Request::Rmdir { .. } => write!(f, "Rmdir"),
            Request::Rename { .. } => write!(f, "Rename"),
            Request::Lookup { .. } => write!(f, "Lookup"),
            Request::Chown { .. } => write!(f, "Chown"),
            Request::Chmod { .. } => write!(f, "Chmod"),
            Request::Utimens { .. } => write!(f, "Utimens"),
            Request::Hardlink { .. } => write!(f, "Hardlink"),
            Request::CreateLink { .. } => write!(f, "CreateLink"),
            Request::ReplaceLink { .. } => write!(f, "ReplaceLink"),
            Request::RemoveLink { .. } => write!(f, "RemoveLink"),
            Request::CreateInode { .. } => write!(f, "CreateInode"),
            Request::Fsync { inode } => write!(f, "Fsync: {inode}"),
            Request::GetAttr { inode } => write!(f, "GetAttr: {inode}"),
            Request::ListDir { inode } => write!(f, "ListDir: {inode}"),
            Request::ListXattrs { inode } => write!(f, "ListXattrs: {inode}"),
            Request::GetXattr { .. } => write!(f, "GetXattr"),
            Request::SetXattr { .. } => write!(f, "SetXattr"),
            Request::RemoveXattr { .. } => write!(f, "RemoveXattr"),
            Request::Write { inode, .. } => write!(f, "Write: {inode}"),
            Request::Read { inode, .. } => write!(f, "Read: {inode}"),
            Request::ReadRaw { inode, .. } => write!(f, "ReadRaw: {inode}"),
            Request::LatestCommit { raft_group } => {
                write!(f, "LatestCommit: {raft_group}")
            }
            Request::RaftGroupLeader { raft_group } => {
                write!(f, "RaftGroupLeader: {raft_group}")
            }
            Request::ConsensusMessage { raft_group, .. } => {
                write!(f, "ConsensusMessage: {raft_group}")
            }
            Request::Lock { inode } => write!(f, "Lock: {inode}"),
            Request::Unlock { inode, lock_id } => {
                write!(f, "Unlock: {inode}, {lock_id}")
            }
            Request::HardlinkRollback { inode, .. } => {
                write!(f, "HardlinkRollback: {inode}")
            }
            Request::HardlinkIncrement { inode, .. } => {
                write!(f, "HardlinkIncrement: {inode}")
            }
            Request::DecrementInode { inode, .. } => {
                write!(f, "DecrementInode: {inode}")
            }
            Request::UpdateParent { inode, .. } => {
                write!(f, "UpdateParent: {inode}")
            }
            Request::UpdateMetadataChangedTime { inode, .. } => {
                write!(f, "UpdateMetadataChangedTime: {inode}")
            }
        }
    }
}

impl Request<'_> {
    pub fn meta_info(&self) -> RequestMetaInfo {
        match self {
            Request::FilesystemReady
            | Request::FilesystemInformation
            | Request::FilesystemCheck
            | Request::FilesystemChecksum => RequestMetaInfo {
                raft_group: None,
                inode: None,
                lock_id: None,
                access_type: AccessType::NoAccess,
                distribution_requirement: DistributionRequirement::Any,
            },
            Request::Fsync { inode } => RequestMetaInfo {
                raft_group: None,
                inode: Some(*inode),
                lock_id: None,
                access_type: AccessType::NoAccess,
                distribution_requirement: DistributionRequirement::RaftGroup,
            },
            Request::ReadRaw { inode, .. } | Request::Read { inode, .. } => RequestMetaInfo {
                raft_group: None,
                inode: Some(*inode),
                lock_id: None,
                access_type: AccessType::ReadData,
                distribution_requirement: DistributionRequirement::RaftGroup,
            },
            Request::SetXattr { inode, .. }
            | Request::RemoveXattr { inode, .. }
            | Request::Unlink { parent: inode, .. }
            | Request::Rmdir { parent: inode, .. } => RequestMetaInfo {
                raft_group: None,
                inode: Some(*inode),
                lock_id: None,
                access_type: AccessType::WriteMetadata,
                distribution_requirement: DistributionRequirement::RaftGroup,
            },
            Request::Hardlink { .. } | Request::Rename { .. } => RequestMetaInfo {
                raft_group: None,
                inode: None,
                lock_id: None,
                access_type: AccessType::WriteMetadata,
                distribution_requirement: DistributionRequirement::TransactionCoordinator,
            },
            Request::Write { inode, .. } => RequestMetaInfo {
                raft_group: None,
                inode: Some(*inode),
                lock_id: None,
                access_type: AccessType::WriteDataAndMetadata,
                distribution_requirement: DistributionRequirement::RaftGroup,
            },
            Request::ListXattrs { inode }
            | Request::GetXattr { inode, .. }
            | Request::Lookup { parent: inode, .. }
            | Request::ListDir { inode }
            | Request::GetAttr { inode } => RequestMetaInfo {
                raft_group: None,
                inode: Some(*inode),
                lock_id: None,
                access_type: AccessType::ReadMetadata,
                distribution_requirement: DistributionRequirement::RaftGroup,
            },
            Request::LatestCommit { raft_group }
            | Request::RaftGroupLeader { raft_group }
            | Request::ConsensusMessage { raft_group, .. } => RequestMetaInfo {
                raft_group: Some(*raft_group),
                inode: None,
                lock_id: None,
                access_type: AccessType::NoAccess,
                distribution_requirement: DistributionRequirement::RaftGroup,
            },
            Request::Lock { inode } => RequestMetaInfo {
                raft_group: None,
                inode: Some(*inode),
                lock_id: None,
                access_type: AccessType::LockMetadata,
                distribution_requirement: DistributionRequirement::RaftGroup,
            },
            Request::Unlock { inode, lock_id } => RequestMetaInfo {
                raft_group: None,
                inode: Some(*inode),
                lock_id: Some(*lock_id),
                access_type: AccessType::NoAccess,
                distribution_requirement: DistributionRequirement::RaftGroup,
            },
            Request::CreateInode { raft_group, .. } => RequestMetaInfo {
                raft_group: Some(*raft_group),
                inode: None,
                lock_id: None,
                access_type: AccessType::WriteMetadata,
                distribution_requirement: DistributionRequirement::RaftGroup,
            },
            Request::Mkdir { parent, .. } => RequestMetaInfo {
                raft_group: None,
                inode: Some(*parent),
                lock_id: None,
                access_type: AccessType::WriteMetadata,
                distribution_requirement: DistributionRequirement::TransactionCoordinator,
            },
            Request::Create { parent, .. } => RequestMetaInfo {
                raft_group: None,
                inode: Some(*parent),
                lock_id: None,
                access_type: AccessType::WriteMetadata,
                distribution_requirement: DistributionRequirement::TransactionCoordinator,
            },
            Request::RemoveLink {
                parent: inode,
                lock_id,
                ..
            }
            | Request::CreateLink {
                parent: inode,
                lock_id,
                ..
            }
            | Request::DecrementInode { inode, lock_id, .. }
            | Request::UpdateParent { inode, lock_id, .. }
            | Request::UpdateMetadataChangedTime { inode, lock_id, .. }
            | Request::ReplaceLink {
                parent: inode,
                lock_id,
                ..
            } => RequestMetaInfo {
                raft_group: None,
                inode: Some(*inode),
                lock_id: *lock_id,
                access_type: AccessType::WriteMetadata,
                distribution_requirement: DistributionRequirement::RaftGroup,
            },
            Request::Truncate { inode, .. } => RequestMetaInfo {
                raft_group: None,
                inode: Some(*inode),
                lock_id: None,
                access_type: AccessType::WriteDataAndMetadata,
                distribution_requirement: DistributionRequirement::RaftGroup,
            },
            Request::HardlinkRollback { inode, .. }
            | Request::Chown { inode, .. }
            | Request::Chmod { inode, .. }
            | Request::HardlinkIncrement { inode, .. }
            | Request::Utimens { inode, .. } => RequestMetaInfo {
                raft_group: None,
                inode: Some(*inode),
                lock_id: None,
                access_type: AccessType::WriteMetadata,
                distribution_requirement: DistributionRequirement::RaftGroup,
            },
        }
    }
}

// Helper methods for reading a decoded response
impl<'a, X: List<Item = &'a str>, D: List<Item = DirectoryEntry<'a>>, C: List<Item = Checksum<'a>>>
    WireResponse<'a, X, D, C>
{
    pub fn as_checksum_response(&self) -> Option<HashMap<u16, Vec<u8>>> {
        if let WireResponse::Checksums(checksums) = self {
            let mut result = HashMap::new();
            for checksum in checksums.iter() {
                result.insert(checksum.raft_group, checksum.checksum.to_vec());
            }
            Some(result)
        } else {
            None
        }
    }

    pub fn as_attr_response(&self) -> Option<EntryMetadata> {
        if let WireResponse::EntryMetadata(attr) = self {
            Some(*attr)
        } else {
            None
        }
    }

    pub fn as_directory_listing_response(&self) -> Option<&D> {
        if let WireResponse::DirectoryListing(entries) = self {
            Some(entries)
        } else {
            None
        }
    }

    pub fn as_error_response(&self) -> Option<ErrorCode> {
        if let WireResponse::ErrorOccurred(error_code) = self {
            Some(*error_code)
        } else {
            None
        }
    }

    pub fn as_empty_response(&self) -> Option<()> {
        if matches!(self, WireResponse::Empty) {
            Some(())
        } else {
            None
        }
    }

    pub fn as_read_response(&self) -> Option<&'a [u8]> {
        if let WireResponse::Read { data } = self {
            Some(data)
        } else {
            None
        }
    }

    pub fn as_xattrs_response(&self) -> Option<&X> {
        if let WireResponse::Xattrs { attrs } = self {
            Some(attrs)
        } else {
            None
        }
    }

    pub fn as_bytes_written_response(&self) -> Option<u32> {
        if let WireResponse::Written { bytes_written } = self {
            Some(*bytes_written)
        } else {
            None
        }
    }

    pub fn as_inode_response(&self) -> Option<u64> {
        if let WireResponse::Inode { id } = self {
            Some(*id)
        } else {
            None
        }
    }

    pub fn as_latest_commit_response(&self) -> Option<(u64, u64)> {
        if let WireResponse::LatestCommit { term, index } = self {
            Some((*term, *index))
        } else {
            None
        }
    }
}
