use log::{error, info, warn};
use std::sync::Mutex;

use crate::base::LocalContext;
use crate::base::node_contains_raft_group;
use crate::base::node_id_from_address;
use crate::client::{PeerClient, TcpPeerClient};
use crate::storage::local::FileStorage;
use crate::storage::lock_table::LockTable;
use crate::storage::message_handlers::commit_write;
use futures::FutureExt;
use futures::channel::oneshot;
use futures::channel::oneshot::Sender;
use futures::future::{Either, Ready, ready};
use futures::{Future, TryFutureExt};
use rand::Rng;
use raxos::{Action, CommandId, Config, Replica, ReplicaId, Slot};
use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use crate::base::{ErrorCode, Request, Response, decode_request, encode_request};

// Warn when a group retains this much consensus history for a lagging
// replica (the raft integration warned at 2x its 10MB compaction threshold).
const RETAINED_WARN_BYTES: usize = 32 * 1024 * 1024;

type PendingResponse = Sender<Result<Response, ErrorCode>>;

// A member of one replication group, wrapping the raxos consensus replica
// and applying its decided commands to the local FileStorage.
pub struct ConsensusNode {
    replica: Mutex<Replica>,
    pending_responses: Mutex<HashMap<CommandId, PendingResponse>>,
    sync_requests: Mutex<Vec<(u64, Sender<()>)>>,
    applied_index: AtomicU64,
    peers: HashMap<u64, TcpPeerClient>,
    raft_group_id: u16,
    file_storage: FileStorage,
    lock_table: Mutex<LockTable>,
    // Origin of the monotonic clock fed to raxos.
    start: Instant,
    // Rate limiter for the retained-history warning (nanos of last warn).
    last_retained_warn: AtomicU64,
}

impl ConsensusNode {
    pub fn new(context: LocalContext, raft_group_id: u16, num_raft_groups: u16) -> ConsensusNode {
        // TODO: currently all rgroups reuse the same set of node_ids. Debugging would be easier,
        // if they had unique ids
        let node_id = context.node_id;
        let mut member_ids: Vec<u64> = context
            .peers_with_node_indices()
            .iter()
            .map(|(peer, peer_index)| (node_id_from_address(peer), *peer_index))
            .filter(|(_, peer_index)| {
                node_contains_raft_group(
                    *peer_index,
                    context.total_nodes(),
                    raft_group_id,
                    context.replicas_per_raft_group,
                )
            })
            .map(|(peer_id, _)| peer_id)
            .collect();
        assert!(node_contains_raft_group(
            context.node_index(),
            context.total_nodes(),
            raft_group_id,
            context.replicas_per_raft_group,
        ));
        member_ids.push(node_id);

        let replicas: Vec<ReplicaId> = member_ids.iter().map(|&id| ReplicaId(id)).collect();
        // The seed randomizes proposal priorities and marks this process
        // start for command deduplication, so it must be fresh entropy.
        //
        // Retention is unbounded: decided values are kept until every member
        // acknowledges them, so a partitioned replica can always catch up
        // once it reconnects (FleetFS has no snapshot/state-transfer path).
        // A replica that is down forever pins memory on the others, as the
        // raft log did before; background_tick warns when that grows.
        let config = Config::new(replicas, ReplicaId(node_id), rand::rng().random())
            .expect("static group membership is valid")
            .max_retained_bytes(usize::MAX);
        let replica = Replica::new(config);

        let path = Path::new(&context.data_dir).join(format!("rgroup_{raft_group_id}"));
        #[allow(clippy::expect_fun_call)]
        fs::create_dir_all(&path).expect(&format!("Failed to create storage dir: {path:?}"));

        let peer_addresses: Vec<SocketAddr> = context
            .peers
            .iter()
            .filter(|peer| member_ids.contains(&node_id_from_address(peer)))
            .cloned()
            .collect();

        ConsensusNode {
            replica: Mutex::new(replica),
            pending_responses: Mutex::new(HashMap::new()),
            sync_requests: Mutex::new(vec![]),
            applied_index: AtomicU64::new(0),
            peers: peer_addresses
                .iter()
                .map(|peer| (node_id_from_address(peer), TcpPeerClient::new(*peer)))
                .collect(),
            raft_group_id,
            file_storage: FileStorage::new(
                node_id,
                raft_group_id,
                num_raft_groups,
                &path,
                &peer_addresses,
            ),
            lock_table: Mutex::new(LockTable::new()),
            start: Instant::now(),
            last_retained_warn: AtomicU64::new(0),
        }
    }

    pub fn get_raft_group_id(&self) -> u16 {
        self.raft_group_id
    }

    pub fn local_data_checksum(&self) -> Result<Vec<u8>, ErrorCode> {
        self.file_storage.local_data_checksum()
    }

    // TODO: remove this method
    pub fn file_storage(&self) -> &FileStorage {
        &self.file_storage
    }

    fn now(&self) -> u64 {
        self.start.elapsed().as_nanos() as u64
    }

    // Runs an input against the consensus replica and carries out the
    // effects it emits. Decided commands are applied while the replica lock
    // is held, which keeps deliveries strictly ordered across concurrently
    // draining tasks (and makes sync()'s applied_index check race-free);
    // outbound messages are sent after releasing it.
    fn drive(&self, input: impl FnOnce(&mut Replica, u64)) {
        let mut sends = vec![];
        {
            let mut replica = self.replica.lock().unwrap();
            input(&mut replica, self.now());
            while let Some(action) = replica.poll_action() {
                match action {
                    Action::Send { to, message } => sends.push((to.0, message.encode())),
                    Action::Deliver { slot, commands } => self.apply_delivery(slot, commands),
                }
            }
        }
        for (to, data) in sends {
            let peer = &self.peers[&to];
            // TODO: errors
            tokio::spawn(peer.send_consensus_message(self.raft_group_id, data));
        }
    }

    // Feeds a consensus message received from a peer into the replica.
    pub fn apply_message(&self, data: &[u8]) {
        match raxos::Message::decode(data) {
            Ok(message) => self.drive(|replica, now| replica.receive(now, message)),
            Err(_) => warn!(
                "Dropping undecodable consensus message ({} bytes)",
                data.len()
            ),
        }
    }

    pub fn get_latest_local_commit(&self) -> u64 {
        self.applied_index.load(Ordering::SeqCst)
    }

    // A linearizable read barrier: commits an empty command through the log
    // and resolves once it has been applied locally. FleetFS acknowledges a
    // write only once the write's slot is applied, and applies slots
    // contiguously; a slot decided before the barrier was submitted can never
    // contain the barrier, so the barrier lands in a strictly later slot, and
    // its local application implies every write acknowledged (anywhere)
    // before this call is applied locally too. Completing a barrier requires
    // a full consensus round, so it doubles as the group readiness check.
    // Concurrent barriers and writes batch into shared slots inside raxos.
    pub fn read_barrier(&self) -> impl Future<Output = Result<(), ErrorCode>> + use<> {
        self.propose_raw(Vec::new()).map_ok(|_| ())
    }

    // The leader is always defined in raxos (first replica of the hedging
    // schedule), so unlike raft there is no waiting for an election.
    pub fn get_leader(&self) -> Ready<Result<u64, ErrorCode>> {
        let leader = self.replica.lock().unwrap().leader().0;
        ready(Ok(leader))
    }

    // Wait until the given index has been committed
    pub fn sync(&self, index: u64) -> impl Future<Output = Result<(), ErrorCode>> + use<> {
        // The replica lock makes the applied_index check atomic with respect
        // to the apply path in drive().
        let _replica_locked = self.replica.lock().unwrap();

        if self.applied_index.load(Ordering::SeqCst) >= index {
            Either::Left(ready(Ok(())))
        } else {
            let (sender, receiver) = oneshot::channel();
            self.sync_requests.lock().unwrap().push((index, sender));
            Either::Right(receiver.map(|x| x.map_err(|_| ErrorCode::Uncategorized)))
        }
    }

    // Should be called once every 100ms to handle background tasks
    pub fn background_tick(&self) {
        self.drive(|replica, now| replica.tick(now));

        // Retention is unbounded so lagging replicas always remain
        // recoverable (see new()); surface sustained growth, which means
        // some replica has been unreachable for a long time.
        let retained = self.replica.lock().unwrap().retained_bytes();
        if retained > RETAINED_WARN_BYTES {
            let now = self.now();
            let last = self.last_retained_warn.load(Ordering::Relaxed);
            if now.saturating_sub(last) > 60_000_000_000
                && self
                    .last_retained_warn
                    .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
            {
                warn!(
                    "rgroup {}: retaining {} bytes of consensus history for a lagging replica",
                    self.raft_group_id, retained
                );
            }
        }
    }

    fn _process_lock_table(
        &self,
        request_data: Vec<u8>,
        pending_response: Option<PendingResponse>,
    ) -> Vec<(Vec<u8>, Option<PendingResponse>)> {
        let request = decode_request(&request_data).unwrap();
        let request_meta = request.meta_info();

        let mut lock_table = self.lock_table.lock().unwrap();

        let mut to_process = vec![];
        if let Some(inode) = request_meta.inode {
            if lock_table.is_locked(&request_meta) {
                lock_table.wait_for_lock(inode, (request_data, pending_response));
            } else {
                match request {
                    Request::Lock { inode } => {
                        let lock_id = lock_table.lock(inode);
                        if let Some(sender) = pending_response {
                            sender.send(Ok(Response::Lock { lock_id })).ok().unwrap();
                        }
                    }
                    Request::Unlock { inode, lock_id } => {
                        let (mut requests, new_lock_id) = lock_table.unlock(inode, lock_id);
                        if let Some(sender) = pending_response {
                            sender.send(Ok(Response::Empty)).ok().unwrap();
                        }
                        if let Some(id) = new_lock_id {
                            let (lock_request_data, pending) = requests.pop().unwrap();
                            let lock_request = decode_request(&lock_request_data).unwrap();
                            assert!(matches!(lock_request, Request::Lock { .. }));
                            if let Some(sender) = pending {
                                sender
                                    .send(Ok(Response::Lock { lock_id: id }))
                                    .ok()
                                    .unwrap();
                            }
                        }
                        to_process.extend(requests);
                    }
                    _ => {
                        // Default to processing the request
                        to_process.push((request_data, pending_response));
                    }
                }
            }
        } else {
            // If it doesn't access an inode, then just process the request
            to_process.push((request_data, pending_response));
        }

        to_process
    }

    // Applies one decided slot to the local state machine, resolving the
    // pending client response if this node was the submitter. Runs while the
    // replica lock is held; must not re-enter self.replica.
    fn apply_delivery(&self, slot: Slot, commands: Vec<(CommandId, Vec<u8>)>) {
        for (command_id, data) in commands {
            let pending_response = self.pending_responses.lock().unwrap().remove(&command_id);
            if data.is_empty() {
                // A read barrier: nothing to apply, committing it was the point.
                if let Some(sender) = pending_response {
                    sender.send(Ok(Response::Empty)).ok();
                }
                continue;
            }
            let to_process = self._process_lock_table(data, pending_response);

            for (data, pending_response) in to_process {
                let request = decode_request(&data).unwrap();
                if let Some(sender) = pending_response {
                    match commit_write(&request, &self.file_storage) {
                        Ok(response) => sender.send(Ok(response)).ok().unwrap(),
                        // TODO: handle this somehow. If not all nodes failed, then the filesystem
                        // is probably corrupted, since some will have applied the write, but not all
                        // There should only be a few types of messages that can fail here. truncate is one,
                        // since you can call it with LONG_MAX or some other value that balloons
                        // the message into a huge write. Probably most other messages can't fail
                        Err(error_code) => {
                            // Ignore errors which the user caused
                            if error_code != ErrorCode::InodeDoesNotExist
                                && error_code != ErrorCode::DoesNotExist
                                && error_code != ErrorCode::AccessDenied
                                && error_code != ErrorCode::OperationNotPermitted
                                && error_code != ErrorCode::AlreadyExists
                                && error_code != ErrorCode::NotEmpty
                                && error_code != ErrorCode::InvalidXattrNamespace
                                && error_code != ErrorCode::MissingXattrKey
                            {
                                error!("Commit failed {:?} {:?}", error_code, request);
                            }
                            sender.send(Err(error_code)).ok().unwrap()
                        }
                    }
                } else {
                    // Replicas won't have a pending response to reply to, since the node
                    // that submitted the proposal will reply to the client.
                    if let Err(error_code) = commit_write(&request, &self.file_storage) {
                        // TODO: handle this somehow. If not all nodes failed, then the filesystem
                        // is probably corrupted, since some will have applied the write, but not all.
                        // There should only be a few types of messages that can fail here. truncate is one,
                        // since you can call it with LONG_MAX or some other value that balloons
                        // the message into a huge write. Probably most other messages can't fail

                        // Ignore errors which the user caused
                        if error_code != ErrorCode::InodeDoesNotExist
                            && error_code != ErrorCode::DoesNotExist
                            && error_code != ErrorCode::AccessDenied
                            && error_code != ErrorCode::OperationNotPermitted
                            && error_code != ErrorCode::AlreadyExists
                            && error_code != ErrorCode::NotEmpty
                            && error_code != ErrorCode::InvalidXattrNamespace
                            && error_code != ErrorCode::MissingXattrKey
                        {
                            error!("Commit failed {:?} {:?}", error_code, request);
                        }
                    }
                }

                info!("Committed write slot {}: {:?}", slot.0, request);
            }
        }

        self.applied_index.store(slot.0, Ordering::SeqCst);

        // TODO: once drain_filter is stable, it could be used to make this a lot nicer
        let mut sync_requests = self.sync_requests.lock().unwrap();
        while !sync_requests.is_empty() {
            if slot.0 >= sync_requests[0].0 {
                let (_, sender) = sync_requests.remove(0);
                sender.send(()).unwrap();
            } else {
                break;
            }
        }
    }

    pub fn propose(
        &self,
        request: &Request<'_>,
    ) -> impl Future<Output = Result<Response, ErrorCode>> + use<> {
        self.propose_raw(encode_request(request))
    }

    pub fn propose_raw(
        &self,
        request: Vec<u8>,
    ) -> impl Future<Output = Result<Response, ErrorCode>> + use<> {
        let (sender, receiver) = oneshot::channel();
        let mut sender = Some(sender);
        self.drive(|replica, now| match replica.submit(now, &request) {
            Ok(command_id) => {
                self.pending_responses
                    .lock()
                    .unwrap()
                    .insert(command_id, sender.take().unwrap());
            }
            Err(error) => {
                warn!("Rejecting proposal: {error}");
                sender
                    .take()
                    .unwrap()
                    .send(Err(ErrorCode::Uncategorized))
                    .ok();
            }
        });

        receiver.map(|x| x.unwrap_or(Err(ErrorCode::Uncategorized)))
    }
}
