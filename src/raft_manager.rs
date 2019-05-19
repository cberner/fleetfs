use log::info;
use raft::eraftpb::Message;
use raft::prelude::EntryType;
use raft::storage::MemStorage;
use raft::{Config, RawNode};
use std::sync::Mutex;

use crate::generated::{get_root_as_generic_request, GenericRequest};
use crate::local_storage::LocalStorage;
use crate::peer_client::PeerClient;
use crate::storage_node::{handler, LocalContext};
use crate::utils::is_write_request;
use flatbuffers::FlatBufferBuilder;
use futures::sync::oneshot;
use futures::sync::oneshot::Sender;
use futures::Future;
use std::collections::HashMap;

pub struct RaftManager<'a> {
    raft_node: Mutex<RawNode<MemStorage>>,
    pending_responses: Mutex<HashMap<u64, (FlatBufferBuilder<'a>, Sender<FlatBufferBuilder<'a>>)>>,
    peers: HashMap<u64, PeerClient>,
    node_id: u64,
    context: LocalContext,
}

impl<'a> RaftManager<'a> {
    pub fn new(context: LocalContext) -> RaftManager<'a> {
        let node_id = context.node_id;
        let mut peer_ids: Vec<u64> = context
            .peers
            .iter()
            // TODO: huge hack. Assume the port is the node id
            .map(|peer| u64::from(peer.port()))
            .collect();
        peer_ids.push(node_id);

        let raft_config = Config {
            id: node_id,
            peers: peer_ids,
            learners: vec![],
            // TODO: set good value
            election_tick: 10 * 3,
            // TODO: set good value
            heartbeat_tick: 3,
            // TODO: need to restore this from storage
            applied: 0,
            max_size_per_msg: 1024 * 1024 * 1024,
            max_inflight_msgs: 256,
            tag: format!("peer_{}", node_id).to_string(),
            ..Default::default()
        };
        let raft_storage = MemStorage::new();
        let raft_node = RawNode::new(&raft_config, raft_storage, vec![]).unwrap();

        RaftManager {
            raft_node: Mutex::new(raft_node),
            pending_responses: Mutex::new(HashMap::new()),
            peers: context
                .peers
                .iter()
                .map(|peer| (u64::from(peer.port()), PeerClient::new(*peer)))
                .collect(),
            node_id,
            context,
        }
    }

    pub fn apply_messages(&self, messages: &[Message]) -> raft::Result<()> {
        let mut raft_node = self.raft_node.lock().unwrap();

        for message in messages {
            assert_eq!(message.to, self.node_id);
            raft_node.step(message.clone())?;
        }

        // TODO: should call process_queue here, but we can't because it would deadlock
        // because this is a message from a peer, and we would create an infinite cycle of TCP calls

        Ok(())
    }

    fn send_outgoing_raft_messages(&self, messages: Vec<Message>) {
        for message in messages {
            let peer = &self.peers[&message.to];
            // TODO: errors
            tokio::spawn(peer.send_raft_message(message));
        }
    }

    // Should be called once every 100ms to handle background tasks
    pub fn background_tick(&self) {
        {
            let mut raft_node = self.raft_node.lock().unwrap();
            raft_node.tick();
        }
        // TODO: should be able to only do this on ready, but apply_messages() doesn't process the queue right now, because it would deadlock
        self.process_raft_queue();
    }

    fn process_raft_queue(&self) {
        let messages = self._process_raft_queue().unwrap();
        self.send_outgoing_raft_messages(messages);
    }

    // Returns the last applied index
    fn _process_raft_queue(&self) -> raft::Result<Vec<Message>> {
        let mut raft_node = self.raft_node.lock().unwrap();

        if !raft_node.has_ready() {
            return Ok(vec![]);
        }

        let mut ready = raft_node.ready();

        if !raft::is_empty_snap(ready.snapshot()) {
            raft_node
                .mut_store()
                .wl()
                .apply_snapshot(ready.snapshot().clone())?;
        }

        if !ready.entries().is_empty() {
            raft_node.mut_store().wl().append(ready.entries())?;
        }

        if let Some(hard_state) = ready.hs() {
            raft_node.mut_store().wl().set_hardstate(hard_state.clone());
        }

        //        let mut applied_index = 0;
        if let Some(committed_entries) = ready.committed_entries.take() {
            for entry in committed_entries {
                // TODO save this
                //                applied_index = max(applied_index, entry.index);

                if entry.data.is_empty() {
                    // New leaders send empty entries
                    continue;
                }

                assert_eq!(entry.entry_type, EntryType::EntryNormal);

                let mut pending_responses = self.pending_responses.lock().unwrap();

                let local_storage = LocalStorage::new(self.context.clone());
                let request = get_root_as_generic_request(&entry.data);
                if let Some((mut builder, sender)) = pending_responses.remove(&entry.index) {
                    handler(request, &local_storage, &self.context, &mut builder);
                    sender.send(builder).ok().unwrap();
                } else {
                    let mut builder = FlatBufferBuilder::new();
                    // TODO: pass None for builder to avoid this useless allocation
                    handler(request, &local_storage, &self.context, &mut builder);
                }

                info!("Committed write index {}", entry.index);
            }
        }

        let messages = ready.messages.drain(..).collect();
        raft_node.advance(ready);

        Ok(messages)
    }

    fn _propose(&self, data: Vec<u8>) -> raft::Result<u64> {
        let mut raft_node = self.raft_node.lock().unwrap();
        raft_node.propose(vec![], data)?;
        return Ok(raft_node.raft.raft_log.last_index());
    }

    pub fn initialize(&self) {
        for _ in 0..100 {
            {
                // TODO: probably don't need to tick() here, since background timer does that
                let mut raft_node = self.raft_node.lock().unwrap();
                raft_node.tick();
                if raft_node.raft.leader_id > 0 {
                    println!("Leader elected {}", raft_node.raft.leader_id);
                    return;
                }
            }
            // Wait until there is a leader
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        panic!("No leader elected");
    }

    pub fn propose(
        &self,
        request: GenericRequest,
        builder: FlatBufferBuilder<'a>,
    ) -> impl Future<Item = FlatBufferBuilder<'a>, Error = ()> {
        assert!(is_write_request(request.request_type()));
        let index = self._propose(request._tab.buf.to_vec()).unwrap();

        // TODO: fix race. proposal could get accepted before this builder is inserted into response map
        let (sender, receiver) = oneshot::channel();
        {
            let mut pending_responses = self.pending_responses.lock().unwrap();
            pending_responses.insert(index, (builder, sender));
        }

        // TODO: Force immediate processing, since we know there's a proposal
        //        self.process_raft_queue();

        return receiver.map_err(|_| ());
    }
}