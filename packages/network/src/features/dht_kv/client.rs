use atm0s_sdn_router::RouteRule;
use std::collections::{HashMap, VecDeque};

use crate::base::ServiceId;

use self::map::{LocalMap, LocalMapOutput};

const MAP_GET_TIMEOUT_MS: u64 = 5000;

use super::{
    msg::{ClientCommand, NodeSession, ServerEvent},
    Control, Event, Key,
};

mod map;

fn route(key: Key) -> RouteRule {
    RouteRule::ToKey(key.0 as u32)
}

pub enum LocalStorageOutput {
    Local(ServiceId, Event),
    Remote(RouteRule, ClientCommand),
}

pub struct LocalStorage {
    session: NodeSession,
    maps: HashMap<Key, LocalMap>,
    map_get_waits: HashMap<(Key, u64), (ServiceId, u64)>,
    queue: VecDeque<LocalStorageOutput>,
    req_id_seed: u64,
}

impl LocalStorage {
    pub fn new(session: NodeSession) -> Self {
        Self {
            session,
            maps: HashMap::new(),
            map_get_waits: HashMap::new(),
            queue: VecDeque::new(),
            req_id_seed: 0,
        }
    }

    pub fn on_tick(&mut self, now: u64) {
        // tick all maps and finding out if any of them should be removed
        let mut to_remove = vec![];
        for (key, map) in self.maps.iter_mut() {
            map.on_tick(now);
            while let Some(out) = map.pop_action() {
                self.queue.push_back(match out {
                    LocalMapOutput::Local(service, event) => LocalStorageOutput::Local(service, Event::MapEvent(*key, event)),
                    LocalMapOutput::Remote(cmd) => LocalStorageOutput::Remote(route(*key), ClientCommand::MapCmd(*key, cmd)),
                });
            }
            if map.should_cleanup() {
                to_remove.push(*key);
            }
        }

        for key in to_remove {
            self.maps.remove(&key);
        }

        // finding timeout map_get requests
        let mut to_remove = vec![];
        for (key, info) in self.map_get_waits.iter() {
            if now >= info.1 + MAP_GET_TIMEOUT_MS {
                to_remove.push(*key);
            }
        }

        for key in to_remove {
            self.map_get_waits.remove(&key);
        }
    }

    pub fn on_local(&mut self, now: u64, service: ServiceId, control: Control) {
        match control {
            Control::MapCmd(key, control) => {
                if let Some(map) = Self::get_map(&mut self.maps, self.session, key, control.is_creator()) {
                    if let Some(event) = map.on_control(now, service, control) {
                        self.queue.push_back(LocalStorageOutput::Remote(route(key), ClientCommand::MapCmd(key, event)));
                        while let Some(out) = map.pop_action() {
                            self.queue.push_back(match out {
                                LocalMapOutput::Local(service, event) => LocalStorageOutput::Local(service, Event::MapEvent(key, event)),
                                LocalMapOutput::Remote(cmd) => LocalStorageOutput::Remote(route(key), ClientCommand::MapCmd(key, cmd)),
                            });
                        }
                    }
                }
            }
            Control::MapGet(key) => {
                let req_id = self.req_id_seed;
                self.req_id_seed += 1;
                self.map_get_waits.insert((key, req_id), (service, req_id));
                self.queue.push_back(LocalStorageOutput::Remote(route(key), ClientCommand::MapGet(key, req_id)));
            }
        }
    }

    pub fn on_server(&mut self, now: u64, remote: NodeSession, cmd: ServerEvent) {
        match cmd {
            ServerEvent::MapEvent(key, cmd) => {
                if let Some(map) = self.maps.get_mut(&key) {
                    if let Some(cmd) = map.on_server(now, remote, cmd) {
                        self.queue.push_back(LocalStorageOutput::Remote(route(key), ClientCommand::MapCmd(key, cmd)));
                    }
                } else {
                    log::warn!("Received remote command for unknown map: {:?}", key);
                }
            }
            ServerEvent::MapGetRes(key, req_id, res) => {
                if let Some((service, req_id)) = self.map_get_waits.remove(&(key, req_id)) {
                    self.queue.push_back(LocalStorageOutput::Local(service, Event::MapGetRes(key, Ok(res))));
                }
            }
        }
    }

    pub fn pop_action(&mut self) -> Option<LocalStorageOutput> {
        self.queue.pop_front()
    }

    fn get_map(maps: &mut HashMap<Key, LocalMap>, session: NodeSession, key: Key, auto_create: bool) -> Option<&mut LocalMap> {
        if !maps.contains_key(&key) && auto_create {
            maps.insert(key, LocalMap::new(session));
        }
        maps.get_mut(&key)
    }
}
