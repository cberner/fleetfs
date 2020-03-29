use crate::generated::*;
use crate::handlers::fsck_handler::{checksum_request, fsck};
use crate::handlers::router::FullOrPartialResponse::{Full, Partial};
use crate::handlers::transaction_coordinator::{
    create_transaction, hardlink_transaction, rename_transaction, rmdir_transaction,
    unlink_transaction,
};
use crate::storage::raft_group_manager::LocalRaftGroupManager;
use crate::storage::raft_node::RaftNode;
use crate::storage_node::LocalContext;
use crate::utils::{empty_response, finalize_response, FlatBufferResponse, FlatBufferWithResponse};
use flatbuffers::FlatBufferBuilder;
use protobuf::Message as ProtobufMessage;
use raft::prelude::Message;
use std::sync::Arc;

// Sync to ensure replicas serve latest data
async fn sync_with_leader(raft: &RaftNode) -> Result<(), ErrorCode> {
    let latest_commit = raft.get_latest_commit_from_leader().await?;
    raft.sync(latest_commit).await
}

enum FullOrPartialResponse {
    Full(FlatBufferWithResponse<'static>),
    Partial(FlatBufferResponse<'static>),
}

async fn request_router_inner(
    request: GenericRequest<'_>,
    raft: Arc<LocalRaftGroupManager>,
    context: LocalContext,
    mut builder: FlatBufferBuilder<'static>,
) -> Result<FullOrPartialResponse, ErrorCode> {
    match request.request_type() {
        RequestType::FilesystemCheckRequest => {
            for rgroup in raft.all_groups() {
                sync_with_leader(rgroup).await?;
            }
            return fsck(context.clone(), builder).await.map(Partial);
        }
        RequestType::FilesystemChecksumRequest => {
            return checksum_request(&context, builder).map(Partial);
        }
        RequestType::ReadRequest => {
            if let Some(read_request) = request.request_as_read_request() {
                let inode = read_request.inode();
                let offset = read_request.offset();
                let read_size = read_request.read_size();
                sync_with_leader(raft.lookup_by_inode(inode)).await?;
                return raft
                    .lookup_by_inode(inode)
                    .file_storage()
                    .read(inode, offset, read_size, builder)
                    .await
                    .map(Full);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::ReadRawRequest => {
            if let Some(read_request) = request.request_as_read_raw_request() {
                return Ok(Full(
                    raft.lookup_by_inode(read_request.inode())
                        .file_storage()
                        .read_raw(
                            read_request.inode(),
                            read_request.offset(),
                            read_request.read_size(),
                            builder,
                        ),
                ));
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::SetXattrRequest => {
            if let Some(set_xattr_request) = request.request_as_set_xattr_request() {
                return raft
                    .lookup_by_inode(set_xattr_request.inode())
                    .propose(request, builder)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::RemoveXattrRequest => {
            if let Some(remove_xattr_request) = request.request_as_remove_xattr_request() {
                return raft
                    .lookup_by_inode(remove_xattr_request.inode())
                    .propose(request, builder)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::UnlinkRequest => {
            if let Some(unlink_request) = request.request_as_unlink_request() {
                return unlink_transaction(
                    unlink_request.parent(),
                    unlink_request.name().to_string(),
                    *unlink_request.context(),
                    builder,
                    raft.clone(),
                )
                .await
                .map(Full);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::RmdirRequest => {
            if let Some(rmdir_request) = request.request_as_rmdir_request() {
                return rmdir_transaction(
                    rmdir_request.parent(),
                    rmdir_request.name().to_string(),
                    *rmdir_request.context(),
                    builder,
                    raft.clone(),
                )
                .await
                .map(Full);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::WriteRequest => {
            if let Some(write_request) = request.request_as_write_request() {
                return raft
                    .lookup_by_inode(write_request.inode())
                    .propose(request, builder)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::UtimensRequest => {
            if let Some(utimens_request) = request.request_as_utimens_request() {
                return raft
                    .lookup_by_inode(utimens_request.inode())
                    .propose(request, builder)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::ChmodRequest => {
            if let Some(chmod_request) = request.request_as_chmod_request() {
                return raft
                    .lookup_by_inode(chmod_request.inode())
                    .propose(request, builder)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::ChownRequest => {
            if let Some(chown_request) = request.request_as_chown_request() {
                return raft
                    .lookup_by_inode(chown_request.inode())
                    .propose(request, builder)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::TruncateRequest => {
            if let Some(truncate_request) = request.request_as_truncate_request() {
                return raft
                    .lookup_by_inode(truncate_request.inode())
                    .propose(request, builder)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::FsyncRequest => {
            if let Some(fsync_request) = request.request_as_fsync_request() {
                return raft
                    .lookup_by_inode(fsync_request.inode())
                    .propose(request, builder)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::MkdirRequest => {
            if let Some(mkdir_request) = request.request_as_mkdir_request() {
                return create_transaction(
                    mkdir_request.parent(),
                    mkdir_request.name().to_string(),
                    mkdir_request.uid(),
                    mkdir_request.gid(),
                    mkdir_request.mode(),
                    FileKind::Directory,
                    builder,
                    raft.clone(),
                )
                .await
                .map(Full);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::CreateRequest => {
            if let Some(create_request) = request.request_as_create_request() {
                return create_transaction(
                    create_request.parent(),
                    create_request.name().to_string(),
                    create_request.uid(),
                    create_request.gid(),
                    create_request.mode(),
                    create_request.kind(),
                    builder,
                    raft.clone(),
                )
                .await
                .map(Full);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::LockRequest => {
            if let Some(lock_request) = request.request_as_lock_request() {
                return raft
                    .lookup_by_inode(lock_request.inode())
                    .propose(request, builder)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::UnlockRequest => {
            if let Some(unlock_request) = request.request_as_unlock_request() {
                return raft
                    .lookup_by_inode(unlock_request.inode())
                    .propose(request, builder)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::HardlinkIncrementRequest => {
            // Internal request used during transaction processing
            if let Some(increment_request) = request.request_as_hardlink_increment_request() {
                return raft
                    .lookup_by_inode(increment_request.inode())
                    .propose(request, builder)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::HardlinkRollbackRequest => {
            // Internal request used during transaction processing
            if let Some(rollback_request) = request.request_as_hardlink_rollback_request() {
                return raft
                    .lookup_by_inode(rollback_request.inode())
                    .propose(request, builder)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::CreateInodeRequest => {
            // Internal request used during transaction processing
            if request.request_as_create_inode_request().is_some() {
                return raft
                    .least_loaded_group()
                    .propose(request, builder)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::DecrementInodeRequest => {
            // Internal request used during transaction processing
            if let Some(decrement_request) = request.request_as_decrement_inode_request() {
                return raft
                    .lookup_by_inode(decrement_request.inode())
                    .propose(request, builder)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::RemoveLinkRequest => {
            // Internal request used during transaction processing
            if let Some(remove_request) = request.request_as_remove_link_request() {
                return raft
                    .lookup_by_inode(remove_request.parent())
                    .propose(request, builder)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::ReplaceLinkRequest => {
            // Internal request used during transaction processing
            if let Some(replace_request) = request.request_as_replace_link_request() {
                return raft
                    .lookup_by_inode(replace_request.parent())
                    .propose(request, builder)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::UpdateParentRequest => {
            // Internal request used during transaction processing
            if let Some(update_request) = request.request_as_update_parent_request() {
                return raft
                    .lookup_by_inode(update_request.inode())
                    .propose(request, builder)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::UpdateMetadataChangedTimeRequest => {
            // Internal request used during transaction processing
            if let Some(update_request) = request.request_as_update_metadata_changed_time_request()
            {
                return raft
                    .lookup_by_inode(update_request.inode())
                    .propose(request, builder)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::CreateLinkRequest => {
            // Internal request used during transaction processing
            if let Some(create_link_request) = request.request_as_create_link_request() {
                return raft
                    .lookup_by_inode(create_link_request.parent())
                    .propose(request, builder)
                    .await
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::HardlinkRequest => {
            if let Some(hardlink_request) = request.request_as_hardlink_request() {
                return hardlink_transaction(hardlink_request, builder, raft.clone())
                    .await
                    .map(Full);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::RenameRequest => {
            if let Some(rename_request) = request.request_as_rename_request() {
                return rename_transaction(
                    rename_request.parent(),
                    rename_request.name().to_string(),
                    rename_request.new_parent(),
                    rename_request.new_name().to_string(),
                    *rename_request.context(),
                    builder,
                    raft.clone(),
                )
                .await
                .map(Full);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::LookupRequest => {
            if let Some(lookup_request) = request.request_as_lookup_request() {
                let parent = lookup_request.parent();
                let name = lookup_request.name().to_string();
                let user_context = *lookup_request.context();
                sync_with_leader(raft.lookup_by_inode(parent)).await?;
                return raft
                    .lookup_by_inode(parent)
                    .file_storage()
                    .lookup(parent, &name, user_context, builder)
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::GetXattrRequest => {
            if let Some(get_xattr_request) = request.request_as_get_xattr_request() {
                let inode = get_xattr_request.inode();
                let key = get_xattr_request.key().to_string();
                sync_with_leader(raft.lookup_by_inode(inode)).await?;
                return raft
                    .lookup_by_inode(inode)
                    .file_storage()
                    .get_xattr(inode, &key, builder)
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::ListXattrsRequest => {
            if let Some(list_xattrs_request) = request.request_as_list_xattrs_request() {
                let inode = list_xattrs_request.inode();
                sync_with_leader(raft.lookup_by_inode(inode)).await?;
                return raft
                    .lookup_by_inode(inode)
                    .file_storage()
                    .list_xattrs(inode, builder)
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::ReaddirRequest => {
            if let Some(readdir_request) = request.request_as_readdir_request() {
                let inode = readdir_request.inode();
                sync_with_leader(raft.lookup_by_inode(inode)).await?;
                return raft
                    .lookup_by_inode(inode)
                    .file_storage()
                    .readdir(inode, builder)
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::GetattrRequest => {
            if let Some(getattr_request) = request.request_as_getattr_request() {
                let inode = getattr_request.inode();
                sync_with_leader(raft.lookup_by_inode(inode)).await?;
                return raft
                    .lookup_by_inode(inode)
                    .file_storage()
                    .getattr(inode, builder)
                    .map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::RaftRequest => {
            if let Some(raft_request) = request.request_as_raft_request() {
                let mut deserialized_message = Message::new();
                deserialized_message
                    .merge_from_bytes(raft_request.message())
                    .unwrap();
                raft.lookup_by_raft_group(raft_request.raft_group())
                    .apply_messages(&[deserialized_message])
                    .unwrap();
                return empty_response(builder).map(Partial);
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::LatestCommitRequest => {
            if let Some(latest_commit_request) = request.request_as_latest_commit_request() {
                let index = raft
                    .lookup_by_raft_group(latest_commit_request.raft_group())
                    .get_latest_local_commit();
                let mut response_builder = LatestCommitResponseBuilder::new(&mut builder);
                response_builder.add_index(index);
                let response_offset = response_builder.finish().as_union_value();
                return Ok(Partial((
                    builder,
                    ResponseType::LatestCommitResponse,
                    response_offset,
                )));
            } else {
                return Err(ErrorCode::BadRequest);
            }
        }
        RequestType::FilesystemReadyRequest => {
            for node in raft.all_groups() {
                node.get_leader().await?;
            }
            let response_builder = EmptyResponseBuilder::new(&mut builder);
            let response_offset = response_builder.finish().as_union_value();
            return Ok(Partial((
                builder,
                ResponseType::EmptyResponse,
                response_offset,
            )));
        }
        RequestType::NONE => unreachable!(),
    }
}

pub async fn request_router<'a>(
    request: GenericRequest<'a>,
    raft: Arc<LocalRaftGroupManager>,
    context: LocalContext,
    builder: FlatBufferBuilder<'static>,
) -> FlatBufferWithResponse<'static> {
    match request_router_inner(request, raft, context, builder).await {
        Ok(response) => match response {
            Full(full_response) => return full_response,
            Partial((mut builder, response_type, response_offset)) => {
                finalize_response(&mut builder, response_type, response_offset);
                return FlatBufferWithResponse::new(builder);
            }
        },
        Err(error_code) => {
            let mut builder = FlatBufferBuilder::new();
            let args = ErrorResponseArgs { error_code };
            let response_offset = ErrorResponse::create(&mut builder, &args).as_union_value();
            finalize_response(&mut builder, ResponseType::ErrorResponse, response_offset);
            return FlatBufferWithResponse::new(builder);
        }
    };
}
