//! `horizon-body`: one player on the network protocol, with no game behind it, for an agent to drive
//! (docs/agent-body.md). It joins offline like `horizon-bots`, flies in creative, and takes the commands
//! `horizon-ctl` gives a full client's body: goto, place, break, chat, signedit, messages, status.
//! A body costs a thread and its chat log; it never decodes chunks.

pub mod control;
pub mod mcp;

use horizon_protocol::{
    PROTOCOL_VERSION,
    buf::{Reader, Writer},
    connection::{ConnWriter, Connection},
    crypto::offline_uuid,
    ids,
    item::Stack,
    packets::{
        Empty, configuration,
        handshake::{self, Intention},
        login::{self, ServerHello},
        play,
    },
    registry::Builtin,
};
use std::{
    collections::VecDeque,
    path::Path,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicI32, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

/// Blocks a creative flyer covers in a tick, under vanilla's flying speed (about 0.55).
pub const STEP: f64 = 0.5;
/// How close the eyes must be to a block's centre to use it: under creative's 5-block reach.
pub const REACH: f64 = 4.5;
/// Player eye height when standing.
const EYES: f64 = 1.62;
/// Chat lines kept for `messages`.
const CHAT_KEPT: usize = 500;
/// The hotbar slot a body holds what it places in (inventory slot 36 is hotbar 0).
const HOTBAR_SLOT: i16 = 36;

fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}

pub fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn raw(id: i32, write: impl FnOnce(&mut Writer)) -> Vec<u8> {
    let mut w = Writer::new();
    w.varint(id);
    write(&mut w);
    w.data
}

/// What the body knows and wants, shared by its reader, its ticker and its commands.
#[derive(Default)]
pub struct State {
    pub pos: Option<[f64; 3]>,
    pub goal: Option<[f64; 3]>,
    /// Chat lines with a running number, oldest first.
    pub chat: VecDeque<(u64, String)>,
    pub chat_count: u64,
    pub error: Option<String>,
}

pub struct Body {
    pub name: String,
    pub state: Mutex<State>,
    pub online: AtomicBool,
    sequence: AtomicI32,
    writer: Mutex<ConnWriter>,
    items: Option<Builtin>,
}

/// Handshake, offline login and configuration; returns the connection in the play state.
pub fn join(mut c: Connection, host: &str, port: u16, name: &str) -> Result<Connection, String> {
    c.send(
        &handshake::Serverbound::Intention(Intention {
            protocol: PROTOCOL_VERSION,
            host: host.into(),
            port,
            intent: handshake::LOGIN,
        })
        .encode(),
    )
    .map_err(err)?;
    c.send(
        &login::Serverbound::Hello(ServerHello {
            name: name.into(),
            uuid: offline_uuid(name),
        })
        .encode(),
    )
    .map_err(err)?;
    loop {
        let (id, body) = c.recv().map_err(err)?;
        match login::Clientbound::decode(id, &body).map_err(err)? {
            login::Clientbound::Compression(p) => c.set_compression(p.threshold),
            login::Clientbound::LoginFinished(_) => {
                c.send(&login::Serverbound::LoginAcknowledged(Empty).encode())
                    .map_err(err)?;
                break;
            }
            login::Clientbound::Disconnect(p) => return Err(format!("refused: {}", p.reason_json)),
            login::Clientbound::Hello(_) => {
                return Err("the server is in online mode; bodies need online-mode=false".into());
            }
            login::Clientbound::CustomQuery(p) => c
                .send(
                    &login::Serverbound::CustomQueryAnswer(login::CustomQueryAnswer {
                        transaction: p.transaction,
                        payload: None,
                    })
                    .encode(),
                )
                .map_err(err)?,
            login::Clientbound::CookieRequest(p) => c
                .send(
                    &login::Serverbound::CookieResponse(login::CookieResponse {
                        key: p.key,
                        payload: None,
                    })
                    .encode(),
                )
                .map_err(err)?,
            login::Clientbound::Unknown(..) => {}
        }
    }
    c.send(
        &configuration::Serverbound::ClientInformation(configuration::ClientInformation::new(2))
            .encode(),
    )
    .map_err(err)?;
    loop {
        let (id, body) = c.recv().map_err(err)?;
        let reply = match configuration::Clientbound::decode(id, &body) {
            Ok(configuration::Clientbound::SelectKnownPacks(p)) => {
                configuration::Serverbound::SelectKnownPacks(p)
            }
            Ok(configuration::Clientbound::KeepAlive(p)) => {
                configuration::Serverbound::KeepAlive(p)
            }
            Ok(configuration::Clientbound::Ping(p)) => configuration::Serverbound::Pong(p),
            Ok(configuration::Clientbound::CodeOfConduct(_)) => {
                configuration::Serverbound::AcceptCodeOfConduct(Empty)
            }
            Ok(configuration::Clientbound::CookieRequest(p)) => {
                configuration::Serverbound::CookieResponse(configuration::CookieResponse {
                    key: p.key,
                    payload: None,
                })
            }
            Ok(configuration::Clientbound::Disconnect(p)) => {
                let reason = horizon_protocol::text::plain(&p.reason, &Default::default());
                return Err(format!("disconnected during configuration: {reason}"));
            }
            Ok(configuration::Clientbound::FinishConfiguration(_)) => {
                c.send(&configuration::Serverbound::FinishConfiguration(Empty).encode())
                    .map_err(err)?;
                return Ok(c);
            }
            _ => continue,
        };
        c.send(&reply.encode()).map_err(err)?;
    }
}

/// The item registry from a server's prepared resources (`registries.json`), for `place`.
pub fn load_items(resources: &Path) -> Result<Builtin, String> {
    let path = Builtin::find(resources)
        .ok_or_else(|| format!("no registries.json under {}", resources.display()))?;
    Builtin::load(&path).map_err(err)
}

impl Body {
    /// Start reading and ticking a connection that [`join`] brought to the play state.
    pub fn spawn(conn: Connection, name: &str, items: Option<Builtin>) -> Arc<Body> {
        let (reader, writer) = conn.split();
        let body = Arc::new(Body {
            name: name.into(),
            state: Mutex::new(State::default()),
            online: AtomicBool::new(true),
            sequence: AtomicI32::new(1),
            writer: Mutex::new(writer),
            items,
        });
        let b = body.clone();
        thread::spawn(move || b.read(reader));
        let b = body.clone();
        thread::spawn(move || b.tick());
        body
    }

    fn send(&self, packet: Vec<u8>) -> Result<(), String> {
        if !self.online.load(Ordering::SeqCst) {
            return Err(self.why_offline());
        }
        lock(&self.writer).send(&packet).map_err(err)
    }

    fn why_offline(&self) -> String {
        lock(&self.state)
            .error
            .clone()
            .unwrap_or_else(|| "offline".into())
    }

    fn drop_out(&self, why: String) {
        if self.online.swap(false, Ordering::SeqCst) {
            lock(&self.state).error = Some(why);
        }
    }

    fn heard(&self, line: String) {
        let mut s = lock(&self.state);
        s.chat_count += 1;
        let n = s.chat_count;
        s.chat.push_back((n, line));
        while s.chat.len() > CHAT_KEPT {
            s.chat.pop_front();
        }
    }

    fn read(&self, mut reader: horizon_protocol::connection::ConnReader) {
        use ids::play::clientbound as cb;
        use ids::play::serverbound as sb;
        loop {
            let (id, body) = match reader.recv() {
                Ok(p) => p,
                Err(e) => return self.drop_out(e.to_string()),
            };
            let mut r = Reader::new(&body);
            let sent = match id {
                cb::KEEP_ALIVE => r
                    .i64()
                    .map_or(Ok(()), |v| self.send(raw(sb::KEEP_ALIVE, |w| w.i64(v)))),
                cb::PING => r
                    .i32()
                    .map_or(Ok(()), |v| self.send(raw(sb::PONG, |w| w.i32(v)))),
                cb::CHUNK_BATCH_FINISHED => self.send(
                    play::Serverbound::ChunkBatchReceived(play::ChunkBatchReceived {
                        chunks_per_tick: 25.0,
                    })
                    .encode(),
                ),
                cb::PLAYER_POSITION => match play::Clientbound::decode(id, &body) {
                    Ok(play::Clientbound::PlayerPosition(p)) => {
                        let mut s = lock(&self.state);
                        let first = s.pos.is_none();
                        let previous = s.pos.unwrap_or([0.0; 3]);
                        let pos: [f64; 3] = std::array::from_fn(|i| {
                            if p.relatives & (1 << i) != 0 {
                                previous[i] + p.position[i]
                            } else {
                                p.position[i]
                            }
                        });
                        s.pos = Some(pos);
                        drop(s);
                        self.send(
                            play::Serverbound::AcceptTeleportation(play::AcceptTeleportation {
                                id: p.teleport,
                                x: pos[0],
                                y: pos[1],
                                z: pos[2],
                                yaw: p.yaw,
                                pitch: p.pitch,
                            })
                            .encode(),
                        )
                        .and_then(|()| {
                            if first {
                                self.send(play::Serverbound::PlayerLoaded(Empty).encode())
                            } else {
                                Ok(())
                            }
                        })
                    }
                    _ => Ok(()),
                },
                cb::SYSTEM_CHAT => {
                    if let Ok(play::Clientbound::SystemChat(p)) =
                        play::Clientbound::decode(id, &body)
                        && !p.overlay
                    {
                        self.heard(horizon_protocol::text::plain(
                            &p.content,
                            &Default::default(),
                        ));
                    }
                    Ok(())
                }
                cb::PLAYER_CHAT => {
                    if let Ok(play::Clientbound::PlayerChat(p)) =
                        play::Clientbound::decode(id, &body)
                    {
                        let who =
                            horizon_protocol::text::plain(&p.chat_type.name, &Default::default());
                        self.heard(format!("<{who}> {}", p.content));
                    }
                    Ok(())
                }
                cb::DISGUISED_CHAT => {
                    if let Ok(play::Clientbound::DisguisedChat(p)) =
                        play::Clientbound::decode(id, &body)
                    {
                        let who =
                            horizon_protocol::text::plain(&p.chat_type.name, &Default::default());
                        let text = horizon_protocol::text::plain(&p.message, &Default::default());
                        self.heard(format!("<{who}> {text}"));
                    }
                    Ok(())
                }
                cb::DISCONNECT => return self.drop_out("disconnected by the server".into()),
                cb::START_CONFIGURATION => {
                    return self.drop_out("sent back to configuration".into());
                }
                _ => Ok(()),
            };
            if let Err(e) = sent {
                return self.drop_out(e);
            }
        }
    }

    /// 20 Hz: fly toward the goal, then hold position, as a client's tick does.
    fn tick(&self) {
        use ids::play::serverbound as sb;
        let mut next = Instant::now();
        while self.online.load(Ordering::SeqCst) {
            let pos = {
                let mut s = lock(&self.state);
                if let (Some(pos), Some(goal)) = (s.pos, s.goal) {
                    let moved = step_toward(pos, goal);
                    s.pos = Some(moved);
                    if moved == goal {
                        s.goal = None;
                    }
                }
                s.pos
            };
            if let Some([x, y, z]) = pos {
                let ok = self
                    .send(
                        play::Serverbound::MovePlayerPos(play::MovePlayerPos { x, y, z, flags: 0 })
                            .encode(),
                    )
                    .and_then(|()| self.send(raw(sb::CLIENT_TICK_END, |_| {})));
                if ok.is_err() {
                    return;
                }
            }
            next += Duration::from_millis(50);
            if let Some(wait) = next.checked_duration_since(Instant::now()) {
                thread::sleep(wait);
            } else {
                next = Instant::now();
            }
        }
    }

    /// Fly to `goal`, waiting until there or `timeout`.
    pub fn goto(&self, goal: [f64; 3], timeout: Duration) -> Result<[f64; 3], String> {
        if lock(&self.state).pos.is_none() {
            return Err("not placed in the world yet".into());
        }
        lock(&self.state).goal = Some(goal);
        let until = Instant::now() + timeout;
        loop {
            if !self.online.load(Ordering::SeqCst) {
                return Err(self.why_offline());
            }
            let s = lock(&self.state);
            if s.goal.is_none() {
                return s.pos.ok_or_else(|| "lost position".into());
            }
            drop(s);
            if Instant::now() > until {
                return Err("did not get there in time".into());
            }
            thread::sleep(Duration::from_millis(20));
        }
    }

    /// Fly within reach of `block` when it is out of reach.
    fn approach(&self, block: [i32; 3]) -> Result<(), String> {
        let pos = lock(&self.state).pos.ok_or("not placed in the world yet")?;
        if within_reach(pos, block) {
            return Ok(());
        }
        let stand = [
            f64::from(block[0]) + 0.5,
            f64::from(block[1]) + 1.0,
            f64::from(block[2]) - 1.5,
        ];
        self.goto(stand, Duration::from_secs(60)).map(|_| ())
    }

    /// Place `item` (a registry name such as `minecraft:stone` or `stone`) at `block`, in creative.
    pub fn place(&self, block: [i32; 3], item: &str) -> Result<(), String> {
        let items = self
            .items
            .as_ref()
            .and_then(|b| b.get("minecraft:item"))
            .ok_or("place needs the item registry: start the body with --resources")?;
        let name = if item.contains(':') {
            item.to_string()
        } else {
            format!("minecraft:{item}")
        };
        let id = items
            .id(&name)
            .ok_or_else(|| format!("no item named {name}"))?;
        self.approach(block)?;
        self.send(
            play::Serverbound::SetCreativeModeSlot(play::SetCreativeModeSlot {
                slot: HOTBAR_SLOT,
                item: Some(Stack::new(id, 1)),
            })
            .encode(),
        )?;
        self.send(play::Serverbound::SetCarriedItem(play::SetCarriedItem { slot: 0 }).encode())?;
        self.send(
            play::Serverbound::UseItemOn(play::UseItemOn {
                hand: 0,
                pos: block,
                face: 1,
                cursor: [0.5, 1.0, 0.5],
                inside: false,
                world_border: false,
                sequence: self.sequence.fetch_add(1, Ordering::SeqCst),
            })
            .encode(),
        )
    }

    /// Break the block at `block`; creative breaks on the first hit.
    pub fn break_block(&self, block: [i32; 3]) -> Result<(), String> {
        self.approach(block)?;
        self.send(
            play::Serverbound::PlayerAction(play::PlayerAction {
                action: 0,
                pos: block,
                direction: 1,
                sequence: self.sequence.fetch_add(1, Ordering::SeqCst),
            })
            .encode(),
        )
    }

    /// Write the four lines of the sign at `block` (front side).
    pub fn sign(&self, block: [i32; 3], lines: [String; 4]) -> Result<(), String> {
        self.approach(block)?;
        self.send(
            play::Serverbound::SignUpdate(play::SignUpdate {
                pos: block,
                lines,
                front: true,
            })
            .encode(),
        )
    }

    /// Say `text` in chat, unsigned (offline servers accept it).
    pub fn chat(&self, text: &str) -> Result<(), String> {
        if text.starts_with('/') {
            return Err("chat takes words; commands go through `command`".into());
        }
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as i64);
        self.send(
            play::Serverbound::Chat(play::Chat {
                message: text.into(),
                timestamp,
                salt: 0,
                signature: None,
                last_seen: play::LastSeenUpdate {
                    offset: 0,
                    acknowledged: [0; 3],
                    checksum: 0,
                },
            })
            .encode(),
        )
    }

    /// Run a command as this player (`setblock ...`, without the slash); the server checks its permission.
    pub fn command(&self, command: &str) -> Result<(), String> {
        let command = command.trim_start_matches('/');
        self.send(
            play::Serverbound::ChatCommand(play::ChatCommand {
                command: command.into(),
            })
            .encode(),
        )
    }

    /// Chat lines numbered after `since`.
    pub fn messages(&self, since: u64) -> Vec<(u64, String)> {
        lock(&self.state)
            .chat
            .iter()
            .filter(|(n, _)| *n > since)
            .cloned()
            .collect()
    }
}

/// One tick of flight from `pos` toward `goal`: at most [`STEP`], landing exactly on it.
pub fn step_toward(pos: [f64; 3], goal: [f64; 3]) -> [f64; 3] {
    let d: [f64; 3] = std::array::from_fn(|i| goal[i] - pos[i]);
    let len = (d[0] * d[0] + d[1] * d[1] + d[2] * d[2]).sqrt();
    if len <= STEP {
        return goal;
    }
    std::array::from_fn(|i| pos[i] + d[i] / len * STEP)
}

/// Whether a player standing at `pos` reaches the centre of `block`.
pub fn within_reach(pos: [f64; 3], block: [i32; 3]) -> bool {
    let eyes = [pos[0], pos[1] + EYES, pos[2]];
    let centre: [f64; 3] = std::array::from_fn(|i| f64::from(block[i]) + 0.5);
    let d: f64 = (0..3)
        .map(|i| (eyes[i] - centre[i]).powi(2))
        .sum::<f64>()
        .sqrt();
    d <= REACH
}

#[cfg(test)]
mod tests;
