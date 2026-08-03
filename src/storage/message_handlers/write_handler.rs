use crate::base::{ErrorCode, Request, Response};
use crate::storage::local::FileStorage;

pub fn commit_write(
    request: &Request<'_>,
    file_storage: &FileStorage,
) -> Result<Response, ErrorCode> {
    match request {
        Request::Fsync { inode } => file_storage.fsync(*inode),
        Request::HardlinkRollback {
            inode,
            last_modified_time,
        } => file_storage.hardlink_rollback(*inode, *last_modified_time),
        Request::Utimens {
            inode,
            atime,
            mtime,
            context,
        } => file_storage.utimens(*inode, *atime, *mtime, *context),
        Request::SetXattr {
            inode,
            key,
            value,
            context,
        } => file_storage.set_xattr(*inode, key, value, *context),
        Request::RemoveXattr {
            inode,
            key,
            context,
        } => file_storage.remove_xattr(*inode, key, *context),
        Request::Mkdir { .. }
        | Request::Hardlink { .. }
        | Request::Rename { .. }
        | Request::Create { .. }
        | Request::Unlink { .. }
        | Request::Rmdir { .. } => {
            unreachable!("Transaction coordinator should break these up into internal requests");
        }
        Request::Chmod {
            inode,
            mode,
            context,
        } => file_storage.chmod(*inode, *mode, *context),
        Request::Chown {
            inode,
            uid,
            gid,
            context,
        } => file_storage.chown(*inode, *uid, *gid, *context),
        Request::Truncate {
            inode,
            new_length,
            context,
        } => file_storage.truncate(*inode, *new_length, *context),
        Request::Write {
            inode,
            offset,
            data,
        } => file_storage.write(*inode, *offset, data),
        Request::RemoveLink {
            parent,
            name,
            link_inode_and_uid,
            context,
            ..
        } => file_storage.remove_link(
            *parent,
            name,
            link_inode_and_uid.map(|x| (x.inode, x.uid)),
            *context,
        ),
        Request::ReplaceLink {
            parent,
            name,
            new_inode,
            kind,
            context,
            ..
        } => file_storage.replace_link(*parent, name, *new_inode, *kind, *context),
        Request::CreateLink {
            inode,
            parent,
            name,
            kind,
            context,
            ..
        } => file_storage.create_link(*inode, *parent, name, *context, *kind),
        Request::CreateInode {
            parent,
            uid,
            gid,
            mode,
            kind,
            ..
        } => file_storage.create_inode(*parent, *uid, *gid, *mode, *kind),
        Request::HardlinkIncrement { inode } => file_storage.hardlink_stage0_link_increment(*inode),
        Request::UpdateParent {
            inode, new_parent, ..
        } => file_storage.update_parent(*inode, *new_parent),
        Request::UpdateMetadataChangedTime { inode, .. } => {
            file_storage.update_metadata_changed_time(*inode)
        }
        Request::DecrementInode {
            inode,
            decrement_count,
            ..
        } => file_storage.decrement_inode_link_count(*inode, *decrement_count),
        Request::Lock { .. } | Request::Unlock { .. } => {
            unreachable!("This should have been handled by the LockTable");
        }
        Request::FilesystemReady
        | Request::FilesystemInformation
        | Request::FilesystemChecksum
        | Request::FilesystemCheck
        | Request::Read { .. }
        | Request::ReadRaw { .. }
        | Request::Lookup { .. }
        | Request::GetAttr { .. }
        | Request::ListDir { .. }
        | Request::ListXattrs { .. }
        | Request::GetXattr { .. }
        | Request::LatestCommit { .. }
        | Request::RaftGroupLeader { .. }
        | Request::RaftMessage { .. } => {
            unreachable!()
        }
    }
}
