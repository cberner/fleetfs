use crate::base::DistributionRequirement;
use crate::base::{CommitId, FileKind, Request, decode_request, encode_response};
use crate::base::{ErrorCode, Response};
use crate::base::{LocalContext, RequestMetaInfo};
use crate::client::RemoteRaftGroups;
use crate::storage::message_handlers::fsck_handler::{checksum_request, fsck};
use crate::storage::message_handlers::transaction_coordinator::{
    create_transaction, hardlink_transaction, rename_transaction, rmdir_transaction,
    unlink_transaction,
};
use crate::storage::raft_group_manager::LocalRaftGroupManager;
use crate::storage::raft_node::sync_with_leader;
use protobuf::Message as ProtobufMessage;
use raft::prelude::Message;
use std::sync::Arc;

pub fn to_error_response(error_code: ErrorCode) -> Vec<u8> {
    encode_response(&Response::ErrorOccurred(error_code))
}

// Determines whether the request can be handled by the local node, or whether it needs to be
// forwarded to a different raft group
fn can_handle_locally(request_meta: &RequestMetaInfo, local_rafts: &LocalRaftGroupManager) -> bool {
    match request_meta.distribution_requirement {
        DistributionRequirement::Any => true,
        DistributionRequirement::TransactionCoordinator => true,
        // TODO: check that this message was sent to the right node. At the moment, we assume the client handled that
        DistributionRequirement::Node => true,
        DistributionRequirement::RaftGroup => {
            if let Some(group) = request_meta.raft_group {
                local_rafts.has_raft_group(group)
            } else {
                local_rafts.inode_stored_locally(request_meta.inode.unwrap())
            }
        }
    }
}

async fn forward_request(
    request: Vec<u8>,
    meta: RequestMetaInfo,
    rafts: Arc<RemoteRaftGroups>,
) -> Vec<u8> {
    match rafts.forward_raw_request(request, meta).await {
        Ok(response) => response,
        _ => to_error_response(ErrorCode::Uncategorized),
    }
}

pub async fn request_router(
    request_data: Vec<u8>,
    raft: Arc<LocalRaftGroupManager>,
    remote_rafts: Arc<RemoteRaftGroups>,
    context: LocalContext,
) -> Vec<u8> {
    let meta = decode_request(&request_data).unwrap().meta_info();
    if !can_handle_locally(&meta, &raft) {
        return forward_request(request_data, meta, remote_rafts.clone()).await;
    }

    match request_router_inner(request_data, raft, remote_rafts, context).await {
        // TODO: optimize Read responses to avoid copying all the data. We should just take the .data Vec and write it out
        Ok(response) => encode_response(&response),
        Err(error_code) => to_error_response(error_code),
    }
}

async fn request_router_inner(
    request_data: Vec<u8>,
    raft: Arc<LocalRaftGroupManager>,
    remote_rafts: Arc<RemoteRaftGroups>,
    context: LocalContext,
) -> Result<Response, ErrorCode> {
    let request = decode_request(&request_data).unwrap();
    match request {
        Request::FilesystemReady => {
            for node in raft.all_groups() {
                node.get_leader().await?;
            }
            // Ensure that all other nodes are ready too
            remote_rafts
                .wait_for_ready()
                .await
                .map_err(|_| ErrorCode::Uncategorized)?;

            Ok(Response::Empty)
        }
        Request::FilesystemInformation => {
            Ok(raft.all_groups().next().unwrap().file_storage().statfs())
        }
        Request::FilesystemCheck => fsck(context.clone(), raft.clone()).await,
        Request::FilesystemChecksum => checksum_request(raft.clone()).await,
        Request::CreateInode { raft_group, .. } => {
            // Internal request used during transaction processing
            raft.lookup_by_raft_group(raft_group)
                .propose_raw(request_data)
                .await
        }
        Request::CreateLink { parent: inode, .. }
        | Request::RemoveLink { parent: inode, .. }
        | Request::ReplaceLink { parent: inode, .. }
        | Request::HardlinkRollback { inode, .. }
        | Request::HardlinkIncrement { inode }
        | Request::DecrementInode { inode, .. }
        | Request::UpdateParent { inode, .. }
        | Request::UpdateMetadataChangedTime { inode, .. } => {
            // Internal request used during transaction processing
            raft.lookup_by_inode(inode).propose_raw(request_data).await
        }
        Request::Write { inode, .. }
        | Request::Lock { inode }
        | Request::Unlock { inode, .. }
        | Request::Fsync { inode }
        | Request::Chmod { inode, .. }
        | Request::Chown { inode, .. }
        | Request::Truncate { inode, .. }
        | Request::SetXattr { inode, .. }
        | Request::RemoveXattr { inode, .. }
        | Request::Utimens { inode, .. } => {
            raft.lookup_by_inode(inode).propose_raw(request_data).await
        }
        Request::Unlink {
            parent,
            name,
            context,
        } => unlink_transaction(parent, name, context, raft.clone(), remote_rafts.clone()).await,
        Request::Read {
            inode,
            offset,
            read_size,
        } => {
            let latest_commit = raft
                .lookup_by_inode(inode)
                .get_latest_commit_from_leader()
                .await?;
            raft.lookup_by_inode(inode).sync(latest_commit).await?;
            raft.lookup_by_inode(inode)
                .file_storage()
                // TODO: Use the real term, not zero
                .read(inode, offset, read_size, CommitId::new(0, latest_commit))
                .await
        }
        Request::ReadRaw {
            inode,
            required_commit,
            offset,
            read_size,
        } => {
            raft.lookup_by_inode(inode)
                .sync(required_commit.index)
                .await?;
            raft.lookup_by_inode(inode)
                .file_storage()
                .read_raw(inode, offset, read_size)
        }
        Request::Rmdir {
            parent,
            name,
            context,
        } => rmdir_transaction(parent, name, context, raft.clone(), remote_rafts.clone()).await,
        Request::Mkdir {
            parent,
            name,
            uid,
            gid,
            mode,
        } => {
            create_transaction(
                parent,
                name,
                uid,
                gid,
                mode,
                FileKind::Directory,
                raft.clone(),
                remote_rafts.clone(),
            )
            .await
        }
        Request::Create {
            parent,
            name,
            uid,
            gid,
            mode,
            kind,
        } => {
            create_transaction(
                parent,
                name,
                uid,
                gid,
                mode,
                kind,
                raft.clone(),
                remote_rafts.clone(),
            )
            .await
        }
        Request::Lookup {
            parent,
            name,
            context,
        } => {
            sync_with_leader(raft.lookup_by_inode(parent)).await?;
            raft.lookup_by_inode(parent)
                .file_storage()
                .lookup(parent, name, context)
        }
        Request::GetXattr {
            inode,
            key,
            context,
        } => {
            sync_with_leader(raft.lookup_by_inode(inode)).await?;
            raft.lookup_by_inode(inode)
                .file_storage()
                .get_xattr(inode, key, context)
        }
        Request::Hardlink {
            inode,
            new_parent,
            new_name,
            context,
        } => {
            hardlink_transaction(
                inode,
                new_parent,
                new_name,
                context,
                raft.clone(),
                remote_rafts.clone(),
            )
            .await
        }
        Request::Rename {
            parent,
            name,
            new_parent,
            new_name,
            context,
        } => {
            rename_transaction(
                parent,
                name,
                new_parent,
                new_name,
                context,
                raft.clone(),
                remote_rafts.clone(),
            )
            .await
        }
        Request::GetAttr { inode } => {
            sync_with_leader(raft.lookup_by_inode(inode)).await?;
            raft.lookup_by_inode(inode).file_storage().getattr(inode)
        }
        Request::ListDir { inode } => {
            sync_with_leader(raft.lookup_by_inode(inode)).await?;
            raft.lookup_by_inode(inode).file_storage().readdir(inode)
        }
        Request::ListXattrs { inode } => {
            sync_with_leader(raft.lookup_by_inode(inode)).await?;
            raft.lookup_by_inode(inode)
                .file_storage()
                .list_xattrs(inode)
        }
        Request::LatestCommit { raft_group } => {
            let index = raft
                .lookup_by_raft_group(raft_group)
                .get_latest_local_commit();
            Ok(Response::LatestCommit { term: 0, index })
        }
        Request::RaftGroupLeader { raft_group } => {
            let rgroup = raft.lookup_by_raft_group(raft_group);
            let leader = rgroup.get_leader().await?;

            Ok(Response::NodeId { id: leader })
        }
        Request::RaftMessage { raft_group, data } => {
            let mut deserialized_message = Message::new();
            deserialized_message.merge_from_bytes(data).unwrap();
            raft.lookup_by_raft_group(raft_group)
                .apply_messages(&[deserialized_message])
                .unwrap();
            Ok(Response::Empty)
        }
    }
}
