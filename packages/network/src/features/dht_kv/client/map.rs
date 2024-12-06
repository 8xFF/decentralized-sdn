use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
};

use crate::{
    base::FeatureControlActor,
    features::dht_kv::{
        msg::{ClientMapCommand, NodeSession, ServerMapEvent, Version},
        Key, MapControl, MapEvent,
    },
};

const RESEND_MS: u64 = 200; //We will resend set or del command if we don't get ack in this time
const SYNC_MS: u64 = 1500; //We will predict send current state for avoiding out-of-sync, this can be waste of bandwidth but it's better than out-of-sync
const UNSUB_TIMEOUT_MS: u64 = 10000; //We will remove the slot if it's not synced in this time

/// MapSlot manage state of single sub-key inside a map.
enum MapSlot {
    Unspecific {
        key: Key,
    },
    Remote {
        key: Key,
        version: Version,
        value: Option<Vec<u8>>,
    },
    Local {
        key: Key,
        value: Option<Vec<u8>>,
        version: Version,
        syncing: bool,
        last_sync: u64,
    },
}

impl MapSlot {
    pub fn new(key: Key) -> Self {
        Self::Unspecific { key }
    }

    pub fn data(&self) -> Option<&[u8]> {
        match self {
            MapSlot::Unspecific { .. } => None,
            MapSlot::Remote { value, .. } => value.as_deref(),
            MapSlot::Local { value, .. } => value.as_deref(),
        }
    }

    /// This method is called when a new value is set to the map, this will overwrite the old value even if it's not synced or from remote.
    pub fn set(&mut self, now: u64, new_data: Vec<u8>) -> Option<ClientMapCommand> {
        match self {
            MapSlot::Unspecific { key } | MapSlot::Remote { key, .. } => {
                let version = Version(now); //TODO use real version
                let key = *key;
                *self = MapSlot::Local {
                    key,
                    value: Some(new_data.clone()),
                    version,
                    syncing: true,
                    last_sync: now,
                };
                Some(ClientMapCommand::Set(key, version, new_data))
            }
            MapSlot::Local {
                key,
                value,
                version,
                syncing,
                last_sync,
            } => {
                *value = Some(new_data.clone());
                *version = Version(now); //TODO use real version
                *syncing = true;
                *last_sync = now;
                Some(ClientMapCommand::Set(*key, *version, new_data))
            }
        }
    }

    /// We only can delete if the slot is local, if the slot is remote, we need to wait for the slot to be removed or timeout.
    pub fn del(&mut self, now: u64) -> Option<ClientMapCommand> {
        match self {
            MapSlot::Unspecific { .. } | MapSlot::Remote { .. } => None,
            MapSlot::Local {
                key,
                value,
                version,
                syncing,
                last_sync,
                ..
            } => {
                *value = None;
                *syncing = true;
                *last_sync = now;
                Some(ClientMapCommand::Del(*key, *version))
            }
        }
    }

    /// We resend the last command if we don't get ack in RESEND_MS, and we predictcaly send current state in SYNC_MS
    pub fn sync(&mut self, now: u64, force: bool) -> Option<ClientMapCommand> {
        match self {
            MapSlot::Unspecific { .. } | MapSlot::Remote { .. } => None,
            MapSlot::Local {
                key,
                value,
                version,
                syncing,
                last_sync,
            } => {
                if (*syncing && now >= *last_sync + RESEND_MS) || now >= *last_sync + SYNC_MS || force {
                    *last_sync = now;
                    if let Some(value) = value {
                        Some(ClientMapCommand::Set(*key, *version, value.clone()))
                    } else {
                        Some(ClientMapCommand::Del(*key, *version))
                    }
                } else {
                    None
                }
            }
        }
    }

    pub fn set_ok(&mut self, version: Version) {
        match self {
            MapSlot::Unspecific { .. } | MapSlot::Remote { .. } => {}
            MapSlot::Local {
                version: slot_version,
                syncing,
                value,
                ..
            } => {
                if *slot_version == version && value.is_some() {
                    *syncing = false;
                }
            }
        }
    }

    pub fn del_ok(&mut self, version: Version) {
        match self {
            MapSlot::Unspecific { .. } | MapSlot::Remote { .. } => {}
            MapSlot::Local {
                version: slot_version,
                syncing,
                value,
                ..
            } => {
                if *slot_version == version && value.is_none() {
                    *syncing = false;
                }
            }
        }
    }

    /// slot will be updated if the version is newer than the current version
    /// if version is same, we still sending back OnSetAck for avoiding flooding by server resend
    /// Return:
    ///     Option(ClientMapCommand, bool)
    ///         - ClientMapCommand: OnSetAck
    ///         - bool: true if the slot is updated
    pub fn on_set(&mut self, _now: u64, key: Key, source: NodeSession, version: Version, data: Vec<u8>) -> Option<(ClientMapCommand, bool)> {
        match self {
            MapSlot::Unspecific { .. } => {
                *self = MapSlot::Remote { key, version, value: Some(data) };
                Some((ClientMapCommand::OnSetAck(key, source, version), true))
            }
            MapSlot::Remote { version: old_version, .. } => match old_version.0.cmp(&version.0) {
                std::cmp::Ordering::Less => {
                    *self = MapSlot::Remote { key, version, value: Some(data) };
                    Some((ClientMapCommand::OnSetAck(key, source, version), true))
                }
                std::cmp::Ordering::Equal => Some((ClientMapCommand::OnSetAck(key, source, version), false)),
                std::cmp::Ordering::Greater => None,
            },
            _ => None,
        }
    }

    pub fn on_del(&mut self, _now: u64, key: Key, source: NodeSession, version: Version) -> Option<ClientMapCommand> {
        match self {
            MapSlot::Remote { version: old_version, .. } => {
                if old_version.0 <= version.0 {
                    *self = MapSlot::Unspecific { key };
                    Some(ClientMapCommand::OnDelAck(key, source, version))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn should_cleanup(&self) -> bool {
        match self {
            MapSlot::Unspecific { .. } => true,
            MapSlot::Remote { value, .. } => value.is_none(),
            MapSlot::Local { value, syncing, .. } => value.is_none() && !*syncing,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum LocalMapOutput<UserData> {
    Remote(ClientMapCommand),
    Local(FeatureControlActor<UserData>, MapEvent),
}

enum SubState {
    NotSub,
    Subscribing { id: u64, sent_ts: u64 },
    Subscribed { id: u64, remote: NodeSession, sync_ts: u64 },
    Unsubscribing { id: u64, started_at: u64, remote: Option<NodeSession>, sent_ts: u64 },
}

/// LocalMap manage state of map, which is a collection of MapSlot.
/// We allow multi-source for a single sub-key with reason for solving conflict between nodes.
pub struct LocalMap<UserData> {
    session: NodeSession,
    slots: HashMap<(Key, NodeSession), MapSlot>,
    subscribers: Vec<FeatureControlActor<UserData>>,
    sub_state: SubState,
    queue: VecDeque<LocalMapOutput<UserData>>,
}

impl<UserData: Eq + Copy + Debug> LocalMap<UserData> {
    pub fn new(session: NodeSession) -> Self {
        Self {
            session,
            slots: HashMap::new(),
            subscribers: Vec::new(),
            sub_state: SubState::NotSub,
            queue: VecDeque::new(),
        }
    }

    pub fn on_tick(&mut self, now: u64) {
        match &mut self.sub_state {
            SubState::NotSub => {}
            SubState::Subscribing { id, sent_ts } => {
                if now >= *sent_ts + RESEND_MS {
                    log::debug!("[ClientMap] Resend sub command in Subscribing state after {RESEND_MS} ms");
                    self.queue.push_back(LocalMapOutput::Remote(ClientMapCommand::Sub(*id, None)));
                    *sent_ts = now;
                }
            }
            SubState::Subscribed { id, remote, sync_ts } => {
                if now >= *sync_ts + SYNC_MS {
                    log::debug!("[ClientMap] Resend sub command in Subscribed state after {SYNC_MS} ms");
                    self.queue.push_back(LocalMapOutput::Remote(ClientMapCommand::Sub(*id, Some(*remote))));
                    *sync_ts = now;
                }
            }
            SubState::Unsubscribing { id, started_at, sent_ts, .. } => {
                if now >= *started_at + UNSUB_TIMEOUT_MS {
                    log::warn!("[ClientMap] Unsubscribing timeout after {UNSUB_TIMEOUT_MS} ms, switch to NotSub");
                    self.sub_state = SubState::NotSub;
                } else if now >= *sent_ts + RESEND_MS {
                    log::debug!("[ClientMap] Resend unsub command in Unsubscribing state after {RESEND_MS} ms");
                    self.queue.push_back(LocalMapOutput::Remote(ClientMapCommand::Unsub(*id)));
                    *sent_ts = now;
                }
            }
        }

        self.sync_slots(now, false);

        // remove all empty slots
        let mut to_remove = vec![];
        for (key, slot) in self.slots.iter_mut() {
            if slot.should_cleanup() {
                log::info!("[ClientMap] Remove empty slot for key {}", key.0);
                to_remove.push(*key);
            }
        }

        for key in to_remove {
            self.slots.remove(&key);
        }
    }

    pub fn on_control(&mut self, now: u64, actor: FeatureControlActor<UserData>, control: MapControl) -> Option<ClientMapCommand> {
        match control {
            MapControl::Set(key, data) => {
                let slot = self.get_slot(key, self.session, true).expect("Must have slot for set");
                if let Some(out) = slot.set(now, data.clone()) {
                    log::debug!("[ClientMap] Set key {} with data len {}", key, data.len());
                    self.fire_event(MapEvent::OnSet(key, self.session.0, data));
                    Some(out)
                } else {
                    log::warn!("[ClientMap] Set key {} failed", key);
                    None
                }
            }
            MapControl::Del(key) => {
                let slot = self.get_slot(key, self.session, false)?;
                if let Some(out) = slot.del(now) {
                    log::debug!("[ClientMap] Del key {}", key);
                    self.fire_event(MapEvent::OnDel(key, self.session.0));
                    Some(out)
                } else {
                    log::warn!("[ClientMap] Del key {} failed", key);
                    None
                }
            }
            MapControl::Sub => {
                let send_sub = self.subscribers.is_empty();
                if self.subscribers.contains(&actor) {
                    log::warn!("[ClientMap] Actor {:?} already subscribed, Sub failed", actor);
                    return None;
                }

                log::debug!("[ClientMap] Actor {:?} subscribe", actor);
                self.subscribers.push(actor);
                if send_sub {
                    log::debug!("[ClientMap] Send sub command");
                    self.sub_state = SubState::Subscribing { sent_ts: now, id: now };

                    //We need to send all current local data to the new subscriber, because RELAY will not send it to source node.
                    self.restore_events(actor, true);

                    Some(ClientMapCommand::Sub(now, None))
                } else {
                    self.restore_events(actor, false);
                    None
                }
            }
            MapControl::Unsub => {
                if !self.subscribers.contains(&actor) || self.subscribers.is_empty() {
                    log::warn!("[ClientMap] Actor {:?} not subscribed, Unsub failed", actor);
                    return None;
                }
                self.subscribers.retain(|&x| x != actor);
                if self.subscribers.is_empty() {
                    match &self.sub_state {
                        SubState::Subscribed { id, remote, .. } => {
                            log::debug!("[ClientMap] Send unsub command, switch to Unsubscribing state from Subscribed");
                            let id = *id;
                            self.sub_state = SubState::Unsubscribing {
                                id,
                                remote: Some(*remote),
                                started_at: now,
                                sent_ts: now,
                            };
                            Some(ClientMapCommand::Unsub(id))
                        }
                        SubState::Subscribing { id, .. } => {
                            let id = *id;
                            log::debug!("[ClientMap] Send unsub command, switch to Unsubscribing state from Subscribing");
                            self.sub_state = SubState::Unsubscribing {
                                id,
                                remote: None,
                                started_at: now,
                                sent_ts: now,
                            };
                            Some(ClientMapCommand::Unsub(id))
                        }
                        _ => panic!("Should in Subscribed or Subscribing state when do unsub"),
                    }
                } else {
                    log::debug!("[ClientMap] Actor {:?} unsubscribed, after that remain {} actors", actor, self.subscribers.len());
                    None
                }
            }
        }
    }

    /// For OnSet and OnDel event, we need to solve problems: key moved to other server or source server changed.
    /// In case 1 key moved to other server:
    pub fn on_server(&mut self, now: u64, remote: NodeSession, cmd: ServerMapEvent) -> Option<ClientMapCommand> {
        match cmd {
            ServerMapEvent::SetOk(key, version) => {
                let slot = self.get_slot(key, self.session, false)?;
                log::debug!("[ClientMap] SetOk for key {}", key);
                slot.set_ok(version);
                None
            }
            ServerMapEvent::DelOk(key, version) => {
                let slot = self.get_slot(key, self.session, false)?;
                log::debug!("[ClientMap] DelOk for key {}", key);
                slot.del_ok(version);
                None
            }
            ServerMapEvent::SubOk(id) => {
                match &mut self.sub_state {
                    SubState::Subscribing { id: sub_id, .. } => {
                        if *sub_id == id {
                            log::info!("[ClientMap] Received SubOk with id {}, switched to Subscribed with sync_ts {}", id, now);
                            self.sub_state = SubState::Subscribed { id, remote, sync_ts: now };
                            self.fire_event(MapEvent::OnRelaySelected(remote.0));
                        } else {
                            log::warn!("[ClientMap] Received SubOk with id {} but current id is {}", id, sub_id);
                        }
                    }
                    SubState::Subscribed {
                        id: sub_id, remote: locked, sync_ts, ..
                    } => {
                        if *sub_id == id {
                            let old_locked = *locked;
                            *locked = remote;
                            *sync_ts = now;

                            if old_locked.0 != remote.0 {
                                log::info!(
                                    "[ClientMap] Received SubOk with id {}, in Subscribed from new remote {} with sync_ts {} => resync all local slots now",
                                    id,
                                    remote.0,
                                    now
                                );
                                self.sync_slots(now, true);
                                self.fire_event(MapEvent::OnRelaySelected(remote.0));
                            } else {
                                log::debug!("[ClientMap] Received SubOk with id {} from same remote {} vs {}", id, locked.0, remote.0);
                            }
                        } else {
                            log::warn!("[ClientMap] Received SubOk with id {} but current id is {}", id, sub_id);
                        }
                    }
                    _ => {
                        log::warn!("[ClientMap] Received SubOk but not in Subscribing or Subscribed state");
                    }
                }
                None
            }
            ServerMapEvent::UnsubOk(id) => {
                if let SubState::Unsubscribing { id: sub_id, remote: locked, .. } = &self.sub_state {
                    if *sub_id == id && (*locked).unwrap_or(remote) == remote {
                        log::info!("[ClientMap] Received UnsubOk with id {}, switched to NotSub", id);
                        self.sub_state = SubState::NotSub;
                    } else {
                        log::warn!(
                            "[ClientMap] Received UnsubOk with id {} but current id is {} vs {}, remote is {:?} vs {:?}",
                            id,
                            sub_id,
                            id,
                            locked,
                            remote
                        );
                    }
                } else {
                    log::warn!("[ClientMap] Received UnsubOk but not in Unsubscribing state");
                }
                None
            }
            ServerMapEvent::OnSet { key, version, source, data } => {
                if !self.accept_event(remote) {
                    log::warn!("[ClientMap] Received OnSet {key} but state or remote is not correct");
                    return None;
                }
                let slot = self.get_slot(key, source, true).expect("Must have slot for set");
                let (event, updated) = slot.on_set(now, key, source, version, data.clone())?;
                log::debug!("[ClientMap] Received OnSet for key {}", key);
                if updated {
                    self.fire_event(MapEvent::OnSet(key, source.0, data));
                }
                Some(event)
            }
            ServerMapEvent::OnDel { key, version, source } => {
                if !self.accept_event(remote) {
                    log::warn!("[ClientMap] Received OnDel {key} but state or remote is not correct");
                    return None;
                }

                let slot = self.get_slot(key, source, true).expect("Must have slot for set");
                let event = slot.on_del(now, key, source, version)?;
                log::debug!("[ClientMap] Received OnDel for key {}", key);
                self.fire_event(MapEvent::OnDel(key, source.0));
                Some(event)
            }
        }
    }

    pub fn pop_action(&mut self) -> Option<LocalMapOutput<UserData>> {
        self.queue.pop_front()
    }

    pub fn should_cleanup(&self) -> bool {
        self.slots.is_empty() && self.subscribers.is_empty() && matches!(self.sub_state, SubState::NotSub)
    }

    fn get_slot(&mut self, key: Key, source: NodeSession, auto_create: bool) -> Option<&mut MapSlot> {
        if !self.slots.contains_key(&(key, source)) && auto_create {
            log::debug!("[ClientMap] Create new slot for key {} from source {}", key, source.0);
            self.slots.insert((key, source), MapSlot::new(key));
        }
        self.slots.get_mut(&(key, source))
    }

    /// only accept event from locked remote in Subscribed state, otherwise, we will ignore the event.
    fn accept_event(&self, remote: NodeSession) -> bool {
        match &self.sub_state {
            SubState::Subscribed { remote: locked_remote, .. } => *locked_remote == remote,
            _ => false,
        }
    }

    fn fire_event(&mut self, event: MapEvent) {
        for sub in self.subscribers.iter() {
            log::debug!("[ClientMap] Fire to {:?}, event {:?}", sub, event);
            self.queue.push_back(LocalMapOutput::Local(*sub, event.clone()));
        }
    }

    fn restore_events(&mut self, actor: FeatureControlActor<UserData>, only_local: bool) {
        for ((key, source), slot) in self.slots.iter() {
            if only_local && self.session != *source {
                continue;
            }
            if let Some(data) = slot.data() {
                let event = MapEvent::OnSet(*key, source.0, data.to_vec());
                log::debug!("[ClientMap] Fire to {:?}, key: {key}, event {:?}", actor, event);
                self.queue.push_back(LocalMapOutput::Local(actor, event));
            }
        }
    }

    fn sync_slots(&mut self, now: u64, force: bool) {
        for slot in self.slots.values_mut() {
            if let Some(cmd) = slot.sync(now, force) {
                log::debug!("[ClientMap] Sync slot with command {:?}", cmd);
                self.queue.push_back(LocalMapOutput::Remote(cmd));
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        base::FeatureControlActor,
        features::dht_kv::{
            client::map::{LocalMapOutput, RESEND_MS, SYNC_MS},
            msg::{ClientMapCommand, Key, NodeSession, ServerMapEvent, Version},
            MapControl, MapEvent,
        },
    };

    use super::{LocalMap, MapSlot};

    #[test]
    fn map_slot_set_local() {
        let key = Key(1);
        let mut slot = MapSlot::new(key);

        //we must output set command with new slot, with Version is now_ms
        assert_eq!(slot.set(100, vec![1, 2, 3, 4]), Some(ClientMapCommand::Set(key, Version(100), vec![1, 2, 3, 4])));

        //we can delete the slot
        assert_eq!(slot.del(200), Some(ClientMapCommand::Del(key, Version(100))));
    }

    #[test]
    fn map_slot_sync_local() {
        let key = Key(1);
        let mut slot = MapSlot::new(key);

        //we must output set command with new slot, with Version is now_ms
        assert_eq!(slot.set(100, vec![1, 2, 3, 4]), Some(ClientMapCommand::Set(key, Version(100), vec![1, 2, 3, 4])));

        assert_eq!(slot.sync(101, false), None);
        assert_eq!(slot.sync(100 + RESEND_MS, false), Some(ClientMapCommand::Set(key, Version(100), vec![1, 2, 3, 4])));

        slot.set_ok(Version(100));
        assert_eq!(slot.sync(100 + RESEND_MS * 2, false), None);

        //after set_ok we only resend with SYNC_MS
        assert_eq!(slot.sync(100 + RESEND_MS + SYNC_MS, false), Some(ClientMapCommand::Set(key, Version(100), vec![1, 2, 3, 4])));
    }

    #[test]
    fn map_slot_sync_local_force() {
        let key = Key(1);
        let mut slot = MapSlot::new(key);

        //we must output set command with new slot, with Version is now_ms
        assert_eq!(slot.set(100, vec![1, 2, 3, 4]), Some(ClientMapCommand::Set(key, Version(100), vec![1, 2, 3, 4])));

        assert_eq!(slot.sync(101, false), None);
        assert_eq!(slot.sync(101, true), Some(ClientMapCommand::Set(key, Version(100), vec![1, 2, 3, 4])));
    }

    #[test]
    fn map_slot_full_ack() {
        let key = Key(1);
        let mut slot = MapSlot::new(key);

        //we must output set command with new slot, with Version is now_ms
        assert_eq!(slot.set(100, vec![1, 2, 3, 4]), Some(ClientMapCommand::Set(key, Version(100), vec![1, 2, 3, 4])));
        slot.set_ok(Version(100));
        assert_eq!(slot.sync(100 + RESEND_MS, false), None);

        //we must output del command with new slot, with Version is now_ms
        assert_eq!(slot.del(200), Some(ClientMapCommand::Del(key, Version(100))));
        slot.del_ok(Version(100));
        assert_eq!(slot.sync(200 + RESEND_MS, false), None);
    }

    #[test]
    fn map_slot_disconnect_ack() {
        let key = Key(1);
        let mut slot = MapSlot::new(key);

        //we must output set command with new slot, with Version is now_ms
        assert_eq!(slot.set(100, vec![1, 2, 3, 4]), Some(ClientMapCommand::Set(key, Version(100), vec![1, 2, 3, 4])));

        //we missing set_ok, but we delete now
        //we must output del command with new slot, with Version is now_ms
        assert_eq!(slot.del(200), Some(ClientMapCommand::Del(key, Version(100))));
        slot.del_ok(Version(100));
        assert_eq!(slot.sync(200 + RESEND_MS, false), None);
    }

    #[test]
    fn map_slot_wrong_ack() {
        let key = Key(1);
        let mut slot = MapSlot::new(key);

        //we must output set command with new slot, with Version is now_ms
        assert_eq!(slot.set(100, vec![1, 2, 3, 4]), Some(ClientMapCommand::Set(key, Version(100), vec![1, 2, 3, 4])));
        slot.set_ok(Version(101));
        assert_ne!(slot.sync(100 + RESEND_MS, false), None);

        assert_eq!(slot.del(200 + RESEND_MS), Some(ClientMapCommand::Del(key, Version(100))));
        slot.del_ok(Version(101));
        assert_ne!(slot.sync(200 + RESEND_MS + RESEND_MS, false), None);
    }

    #[test]
    fn map_slot_handle_server_event() {
        let source = NodeSession(1, 2);
        let key = Key(1);
        let mut slot = MapSlot::new(key);

        let version = Version(100);
        assert_eq!(slot.on_set(100, key, source, version, vec![1, 2, 3, 4]), Some((ClientMapCommand::OnSetAck(key, source, version), true)));
        assert!(!slot.should_cleanup());

        assert_eq!(slot.on_del(200, key, source, version), Some(ClientMapCommand::OnDelAck(key, source, version)));
        assert!(slot.should_cleanup());
    }

    #[test]
    fn map_slot_should_reject_server_event_with_local() {
        let key = Key(1);
        let mut slot = MapSlot::new(key);

        let version = Version(100);
        assert_eq!(slot.set(100, vec![1, 2, 3, 4]), Some(ClientMapCommand::Set(key, version, vec![1, 2, 3, 4])));

        let source = NodeSession(1, 2);
        assert_eq!(slot.on_set(100, key, source, version, vec![1, 2, 3, 4]), None);
        assert_eq!(slot.on_del(200, key, source, version), None);
        assert!(!slot.should_cleanup());
    }

    #[test]
    fn map_slot_should_reject_server_event_without_newer_version() {
        let key = Key(1);
        let mut slot = MapSlot::new(key);

        let source = NodeSession(1, 2);
        assert_eq!(
            slot.on_set(100, key, source, Version(100), vec![1, 2, 3, 4]),
            Some((ClientMapCommand::OnSetAck(key, source, Version(100)), true))
        );
        //same version will only send ack, not update the slot
        assert_eq!(
            slot.on_set(100, key, source, Version(100), vec![1, 2, 3, 4]),
            Some((ClientMapCommand::OnSetAck(key, source, Version(100)), false))
        );
        assert_eq!(slot.on_set(100, key, source, Version(90), vec![1, 2, 3]), None);
        assert_eq!(slot.on_del(200, key, source, Version(90)), None);
        assert!(!slot.should_cleanup());
    }

    #[test]
    fn map_handle_control_set_del_correct() {
        let session = NodeSession(1, 2);
        let actor = FeatureControlActor::Controller(());
        let mut map = LocalMap::new(session);

        let key = Key(1);

        assert_eq!(
            map.on_control(100, actor, MapControl::Set(key, vec![1, 2, 3, 4])),
            Some(ClientMapCommand::Set(key, Version(100), vec![1, 2, 3, 4]))
        );
        assert_eq!(map.on_control(102, actor, MapControl::Sub), Some(ClientMapCommand::Sub(102, None))); //Sub will be sent include time ms
        assert_eq!(map.pop_action(), Some(LocalMapOutput::Local(actor, MapEvent::OnSet(key, session.0, vec![1, 2, 3, 4]))));
        assert_eq!(map.pop_action(), None);

        assert_eq!(map.on_control(101, actor, MapControl::Del(key)), Some(ClientMapCommand::Del(key, Version(100))));
        assert_eq!(map.pop_action(), Some(LocalMapOutput::Local(actor, MapEvent::OnDel(key, session.0))));
        assert_eq!(map.pop_action(), None);
    }

    #[test]
    fn map_handle_sub_with_local_data_correct() {
        let session = NodeSession(1, 2);
        let actor = FeatureControlActor::Controller(());
        let mut map = LocalMap::new(session);

        let key = Key(1);

        assert_eq!(map.on_control(102, actor, MapControl::Sub), Some(ClientMapCommand::Sub(102, None)));
        assert_eq!(
            map.on_control(103, actor, MapControl::Set(key, vec![1, 2, 3, 4])),
            Some(ClientMapCommand::Set(key, Version(103), vec![1, 2, 3, 4]))
        );
        assert_eq!(map.pop_action(), Some(LocalMapOutput::Local(actor, MapEvent::OnSet(key, session.0, vec![1, 2, 3, 4]))));
        assert_eq!(map.pop_action(), None);

        assert_eq!(map.on_control(103, actor, MapControl::Unsub), Some(ClientMapCommand::Unsub(102)));
        assert_eq!(map.on_control(104, actor, MapControl::Del(key)), Some(ClientMapCommand::Del(key, Version(103))));
        assert_eq!(map.pop_action(), None); //should not output local event after unsub
    }

    #[test]
    fn map_unsub_ok() {
        let session = NodeSession(1, 2);
        let actor = FeatureControlActor::Controller(());
        let mut map = LocalMap::new(session);

        let relay = NodeSession(5, 6);

        assert_eq!(map.on_control(102, actor, MapControl::Sub), Some(ClientMapCommand::Sub(102, None)));
        assert_eq!(map.on_server(103, relay, ServerMapEvent::SubOk(102)), None);
        assert_eq!(map.pop_action(), Some(LocalMapOutput::Local(actor, MapEvent::OnRelaySelected(relay.0))));

        map.on_tick(102 + RESEND_MS);
        assert_eq!(map.pop_action(), None);

        assert_eq!(map.on_control(103, actor, MapControl::Unsub), Some(ClientMapCommand::Unsub(102)));
        assert_eq!(map.on_server(104, relay, ServerMapEvent::UnsubOk(102)), None);
        map.on_tick(103 + RESEND_MS);
        assert_eq!(map.pop_action(), None);

        assert!(map.should_cleanup());
    }

    #[test]
    fn map_handle_auto_resend_sub_unsub() {
        let session = NodeSession(1, 2);
        let actor = FeatureControlActor::Controller(());
        let mut map = LocalMap::new(session);

        assert_eq!(map.on_control(102, actor, MapControl::Sub), Some(ClientMapCommand::Sub(102, None)));
        map.on_tick(102 + RESEND_MS);
        assert_eq!(map.pop_action(), Some(LocalMapOutput::Remote(ClientMapCommand::Sub(102, None))));
        assert_eq!(map.pop_action(), None);
    }

    #[test]
    fn map_handle_set_del_ack() {
        let session = NodeSession(1, 2);
        let actor = FeatureControlActor::Controller(());
        let mut map = LocalMap::new(session);

        let relay = NodeSession(3, 4);

        assert_eq!(map.on_control(102, actor, MapControl::Sub), Some(ClientMapCommand::Sub(102, None)));
        assert_eq!(map.on_server(103, relay, ServerMapEvent::SubOk(102)), None);
        assert_eq!(map.pop_action(), Some(LocalMapOutput::Local(actor, MapEvent::OnRelaySelected(relay.0))));
        map.on_tick(102 + RESEND_MS);
        assert_eq!(map.pop_action(), None);
    }

    #[test]
    fn map_reject_event_unknown_relay() {
        let session = NodeSession(1, 2);
        let mut map = LocalMap::<()>::new(session);

        let key = Key(1);
        let source = NodeSession(3, 4);
        let relay = NodeSession(3, 4);
        assert_eq!(
            map.on_server(
                103,
                relay,
                ServerMapEvent::OnSet {
                    key,
                    source,
                    version: Version(2000),
                    data: vec![1, 2, 3, 4]
                }
            ),
            None
        );
    }

    #[test]
    fn map_handle_sub_with_remote_data_correct() {
        let session = NodeSession(1, 2);
        let actor = FeatureControlActor::Controller(());
        let mut map = LocalMap::new(session);

        let key = Key(1);

        let source = NodeSession(3, 4);
        let relay = NodeSession(5, 6);
        assert_eq!(map.on_control(102, actor, MapControl::Sub), Some(ClientMapCommand::Sub(102, None)));

        //We need SubOk for accepting OnSet event
        assert_eq!(map.on_server(103, relay, ServerMapEvent::SubOk(102)), None);
        assert_eq!(map.pop_action(), Some(LocalMapOutput::Local(actor, MapEvent::OnRelaySelected(relay.0))));

        assert_eq!(
            map.on_server(
                103,
                relay,
                ServerMapEvent::OnSet {
                    key,
                    source,
                    version: Version(2000),
                    data: vec![1, 2, 3, 4]
                }
            ),
            Some(ClientMapCommand::OnSetAck(key, source, Version(2000)))
        );
        assert_eq!(map.pop_action(), Some(LocalMapOutput::Local(actor, MapEvent::OnSet(key, source.0, vec![1, 2, 3, 4]))));
        assert_eq!(map.pop_action(), None);

        assert_eq!(
            map.on_server(103, relay, ServerMapEvent::OnDel { key, source, version: Version(2000) }),
            Some(ClientMapCommand::OnDelAck(key, source, Version(2000)))
        );
        assert_eq!(map.pop_action(), Some(LocalMapOutput::Local(actor, MapEvent::OnDel(key, source.0))));
        assert_eq!(map.pop_action(), None);
    }

    #[test]
    fn map_handle_switch_new_relay() {
        let session = NodeSession(1, 2);
        let actor = FeatureControlActor::Controller(());
        let mut map = LocalMap::new(session);

        let key = Key(1);
        let source = NodeSession(3, 4);
        let relay1 = NodeSession(5, 6);
        let relay2 = NodeSession(7, 8);

        assert_eq!(map.on_control(102, actor, MapControl::Sub), Some(ClientMapCommand::Sub(102, None)));
        assert_eq!(map.on_server(103, relay1, ServerMapEvent::SubOk(102)), None);
        assert_eq!(map.pop_action(), Some(LocalMapOutput::Local(actor, MapEvent::OnRelaySelected(relay1.0))));

        assert_eq!(
            map.on_server(
                103,
                relay1,
                ServerMapEvent::OnSet {
                    key,
                    source,
                    version: Version(2000),
                    data: vec![1, 2, 3, 4]
                }
            ),
            Some(ClientMapCommand::OnSetAck(key, source, Version(2000)))
        );

        //simulate sub to new relay
        assert_eq!(map.on_server(105, relay2, ServerMapEvent::SubOk(102)), None);

        assert_eq!(map.pop_action(), Some(LocalMapOutput::Local(actor, MapEvent::OnSet(key, source.0, vec![1, 2, 3, 4]))));
        assert_eq!(map.pop_action(), Some(LocalMapOutput::Local(actor, MapEvent::OnRelaySelected(relay2.0))));
        assert_eq!(map.pop_action(), None);

        //Reject from relay1
        assert_eq!(map.on_server(106, relay1, ServerMapEvent::OnDel { key, source, version: Version(2000) }), None,);

        //from relay2
        assert_eq!(
            map.on_server(
                107,
                relay2,
                ServerMapEvent::OnSet {
                    key,
                    source,
                    version: Version(2000),
                    data: vec![1, 2, 3, 4]
                }
            ),
            Some(ClientMapCommand::OnSetAck(key, source, Version(2000)))
        );
    }

    #[test]
    fn map_with_multi_sources() {
        let local_session = NodeSession(1, 2);
        let relay = NodeSession(3, 4);
        let other_source = NodeSession(5, 6);
        let mut map = LocalMap::new(local_session);

        let key = Key(1);

        assert_eq!(map.on_control(100, FeatureControlActor::Controller(()), MapControl::Sub), Some(ClientMapCommand::Sub(100, None)));
        assert_eq!(map.on_server(100, relay, ServerMapEvent::SubOk(100)), None);

        assert_eq!(
            map.on_control(100, FeatureControlActor::Controller(()), MapControl::Set(key, vec![1, 2, 3, 4])),
            Some(ClientMapCommand::Set(key, Version(100), vec![1, 2, 3, 4]))
        );

        assert_eq!(
            map.on_server(
                120,
                relay,
                ServerMapEvent::OnSet {
                    key,
                    source: other_source,
                    version: Version(1000),
                    data: vec![2, 3, 4]
                }
            ),
            Some(ClientMapCommand::OnSetAck(key, other_source, Version(1000)))
        );

        assert_eq!(map.slots.len(), 2);
    }
}
