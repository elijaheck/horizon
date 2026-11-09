use super::*;
use horizon_protocol::{
    connection::Pipe,
    packets::login::{GameProfile, LoginFinished},
};
use serde_json::Value;

/// A server's end: answers the login and configuration a body sends, then places it.
fn fake_server(mut s: Connection) -> Connection {
    let (id, _) = s.recv().unwrap();
    assert_eq!(id, ids::handshake::serverbound::INTENTION);
    let (id, body) = s.recv().unwrap();
    let login::Serverbound::Hello(hello) = login::Serverbound::decode(id, &body).unwrap() else {
        panic!("expected hello")
    };
    assert_eq!(hello.name, "EckoClaude");
    let profile = GameProfile {
        uuid: hello.uuid,
        name: hello.name,
        properties: vec![],
    };
    s.send(
        &login::Clientbound::LoginFinished(LoginFinished {
            profile,
            session: 1,
        })
        .encode(),
    )
    .unwrap();
    let (id, _) = s.recv().unwrap();
    assert_eq!(id, ids::login::serverbound::LOGIN_ACKNOWLEDGED);
    let (id, _) = s.recv().unwrap();
    assert_eq!(id, ids::configuration::serverbound::CLIENT_INFORMATION);
    s.send(&configuration::Clientbound::FinishConfiguration(Empty).encode())
        .unwrap();
    let (id, _) = s.recv().unwrap();
    assert_eq!(id, ids::configuration::serverbound::FINISH_CONFIGURATION);
    s
}

fn connected() -> (Arc<Body>, Connection) {
    let (a, b) = Pipe::pair();
    let server = thread::spawn(move || fake_server(Connection::local(b)));
    let conn = join(Connection::local(a), "localhost", 25565, "EckoClaude").unwrap();
    let mut server = server.join().unwrap();
    let body = Body::spawn(conn, "EckoClaude", None);
    server
        .send(
            &play::Clientbound::PlayerPosition(play::PlayerPosition {
                teleport: 9,
                position: [0.5, 64.0, 0.5],
                delta: [0.0; 3],
                yaw: 0.0,
                pitch: 0.0,
                relatives: 0,
            })
            .encode(),
        )
        .unwrap();
    (body, server)
}

/// The next serverbound play packet that is not the tick's movement.
fn next(server: &mut Connection) -> (i32, Vec<u8>) {
    use ids::play::serverbound as sb;
    loop {
        let (id, body) = server.recv().unwrap();
        if id != sb::MOVE_PLAYER_POS && id != sb::CLIENT_TICK_END {
            return (id, body);
        }
    }
}

fn wait_for(what: impl Fn() -> bool) {
    let until = Instant::now() + Duration::from_secs(5);
    while !what() {
        assert!(Instant::now() < until, "timed out");
        thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn joins_accepts_the_teleport_and_says_it_has_loaded() {
    let (body, mut server) = connected();
    let (id, payload) = next(&mut server);
    let play::Serverbound::AcceptTeleportation(t) =
        play::Serverbound::decode(id, &payload).unwrap()
    else {
        panic!("expected accept teleportation, got {id}")
    };
    assert_eq!((t.id, t.y), (9, 64.0));
    assert_eq!(next(&mut server).0, ids::play::serverbound::PLAYER_LOADED);
    assert_eq!(lock(&body.state).pos, Some([0.5, 64.0, 0.5]));
}

#[test]
fn answers_keepalive_and_logs_chat() {
    let (body, mut server) = connected();
    next(&mut server);
    next(&mut server);
    server
        .send(&raw(ids::play::clientbound::KEEP_ALIVE, |w| w.i64(77)))
        .unwrap();
    let (id, payload) = next(&mut server);
    assert_eq!(id, ids::play::serverbound::KEEP_ALIVE);
    assert_eq!(Reader::new(&payload).i64().unwrap(), 77);
    let say = |text: &str, overlay| {
        play::Clientbound::SystemChat(play::SystemChat {
            content: horizon_protocol::text::literal(text),
            overlay,
        })
        .encode()
    };
    server.send(&say("<Elijahoak> !quest 2", false)).unwrap();
    server.send(&say("action bar", true)).unwrap();
    server.send(&say("second", false)).unwrap();
    wait_for(|| body.messages(0).len() == 2);
    assert_eq!(
        body.messages(0),
        vec![
            (1, "<Elijahoak> !quest 2".to_string()),
            (2, "second".to_string())
        ]
    );
    assert_eq!(body.messages(1).len(), 1);
    let log = control::handle(&body, "chat-log");
    assert_eq!(
        log["chat"],
        serde_json::json!(["<Elijahoak> !quest 2", "second"])
    );
}

#[test]
fn flies_to_a_goal_and_moves_the_server_side_player() {
    let (body, mut server) = connected();
    next(&mut server);
    next(&mut server);
    let b = body.clone();
    let flight = thread::spawn(move || b.goto([3.5, 65.0, 0.5], Duration::from_secs(5)));
    let mut last = [0.0; 3];
    while last != [3.5, 65.0, 0.5] {
        let (id, payload) = server.recv().unwrap();
        if let Ok(play::Serverbound::MovePlayerPos(m)) = play::Serverbound::decode(id, &payload) {
            last = [m.x, m.y, m.z];
        }
    }
    assert_eq!(flight.join().unwrap().unwrap(), [3.5, 65.0, 0.5]);
}

#[test]
fn breaks_writes_signs_and_runs_commands_with_the_right_packets() {
    let (body, mut server) = connected();
    next(&mut server);
    next(&mut server);
    body.break_block([1, 64, 2]).unwrap();
    let (id, payload) = next(&mut server);
    let play::Serverbound::PlayerAction(a) = play::Serverbound::decode(id, &payload).unwrap()
    else {
        panic!()
    };
    assert_eq!((a.action, a.pos), (0, [1, 64, 2]));

    let reply = control::handle(&body, "signedit 1 64 1 tests: 3/4 | lamp row");
    assert!(reply.get("error").is_none(), "{reply}");
    let (id, payload) = next(&mut server);
    let play::Serverbound::SignUpdate(s) = play::Serverbound::decode(id, &payload).unwrap() else {
        panic!()
    };
    assert_eq!(
        s.lines,
        [
            "tests: 3/4".to_string(),
            "lamp row".into(),
            String::new(),
            String::new()
        ]
    );

    assert!(body.chat("/op me").is_err());
    control::handle(
        &body,
        "command /setblock 1 64 1 minecraft:redstone_lamp[lit=true]",
    );
    let (id, payload) = next(&mut server);
    let play::Serverbound::ChatCommand(c) = play::Serverbound::decode(id, &payload).unwrap() else {
        panic!()
    };
    assert_eq!(
        c.command,
        "setblock 1 64 1 minecraft:redstone_lamp[lit=true]"
    );
}

#[test]
fn a_disconnect_takes_the_body_offline() {
    let (body, server) = connected();
    drop(server);
    wait_for(|| !body.online.load(Ordering::SeqCst));
    assert!(body.chat("hi").is_err());
    assert_eq!(
        control::handle(&body, "status")["online"],
        Value::Bool(false)
    );
}

#[test]
fn place_without_items_says_how_to_fix_it() {
    let (body, _server) = connected();
    let reply = control::handle(&body, "place 1 64 1 --item stone");
    assert!(
        reply["error"].as_str().unwrap().contains("--resources"),
        "{reply}"
    );
}

#[test]
fn steps_are_capped_and_land_exactly() {
    assert_eq!(step_toward([0.0; 3], [0.3, 0.0, 0.0]), [0.3, 0.0, 0.0]);
    let s = step_toward([0.0; 3], [10.0, 0.0, 0.0]);
    assert_eq!(s, [STEP, 0.0, 0.0]);
    assert!(within_reach([0.5, 64.0, 0.5], [2, 65, 0]));
    assert!(!within_reach([0.5, 64.0, 0.5], [8, 64, 0]));
}

#[test]
fn mcp_tools_become_socket_lines_and_refuse_commands() {
    let args = serde_json::json!({ "x": 1, "y": 64, "z": 2, "item": "oak_planks" });
    assert_eq!(
        mcp::line_for("place", &args).unwrap(),
        "place 1 64 2 --item oak_planks"
    );
    let sign = serde_json::json!({ "x": 0, "y": 64, "z": 0, "lines": ["a|b", "c\nd"] });
    assert_eq!(
        mcp::line_for("signedit", &sign).unwrap(),
        "signedit 0 64 0 a/b|c d"
    );
    assert!(mcp::line_for("chat", &serde_json::json!({ "message": " /op me" })).is_err());
    assert!(mcp::line_for("command", &serde_json::json!({})).is_err());
    let names: Vec<_> = mcp::tools()
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect();
    assert!(!names.contains(&"command".to_string()));
}

#[cfg(unix)]
#[test]
fn a_harness_drives_the_body_over_tcp_and_its_socket() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        fake_server(Connection::from_stream(stream).unwrap())
    });
    let stream = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    let conn = join(
        Connection::from_stream(stream).unwrap(),
        "127.0.0.1",
        port,
        "EckoClaude",
    )
    .unwrap();
    let mut server = server.join().unwrap();
    let body = Body::spawn(conn, "EckoClaude", None);
    let dir = std::env::temp_dir().join(format!("horizon-body-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let socket = dir.join("body.sock");
    let (b, path) = (body.clone(), socket.clone());
    thread::spawn(move || control::serve(b, &path));
    wait_for(|| socket.exists());

    let hello = control::request(&socket, "hello").unwrap();
    assert_eq!(hello["name"], "EckoClaude");
    assert_eq!(
        control::request(&socket, "goto 1 64 1").unwrap()["error"],
        "not placed in the world yet"
    );
    server
        .send(
            &play::Clientbound::SystemChat(play::SystemChat {
                content: horizon_protocol::text::literal("<Elijahoak> !go"),
                overlay: false,
            })
            .encode(),
        )
        .unwrap();
    wait_for(|| !body.messages(0).is_empty());
    let reply = control::request(&socket, "messages 5").unwrap();
    assert_eq!(reply["chat"][0]["text"], "<Elijahoak> !go");
    let _ = std::fs::remove_dir_all(&dir);
}
