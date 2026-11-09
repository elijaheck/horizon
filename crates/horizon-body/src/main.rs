//! `horizon-body`: run a light agent body, talk to one, or serve it over MCP (docs/agent-body.md).

use std::{path::PathBuf, process::ExitCode};

const USAGE: &str = "usage:
  horizon-body serve --server HOST:PORT --name NAME --socket PATH [--resources DIR]
  horizon-body --socket PATH mcp
  horizon-body --socket PATH COMMAND...   (status, goto X Y Z, place X Y Z --item I, break X Y Z,
                                          signedit X Y Z A|B|C|D, chat TEXT, command TEXT,
                                          messages [N] [--since SEQ], chat-log [N], quit)";

fn flag(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("horizon-body: {e}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(not(unix))]
fn run(_: &[String]) -> Result<(), String> {
    Err("bodies are served on Unix sockets; this platform is not supported yet".into())
}

#[cfg(unix)]
fn run(args: &[String]) -> Result<(), String> {
    if args.first().map(String::as_str) == Some("serve") {
        return serve(&args[1..]);
    }
    let socket = PathBuf::from(flag(args, "--socket").ok_or(USAGE)?);
    let i = args.iter().position(|a| a == "--socket").unwrap_or(0);
    let words: Vec<String> = args
        .iter()
        .enumerate()
        .filter(|(j, _)| *j != i && *j != i + 1)
        .map(|(_, a)| a.clone())
        .collect();
    match words.first().map(String::as_str) {
        None | Some("help" | "--help" | "-h") => {
            println!("{USAGE}");
            Ok(())
        }
        Some("mcp") => horizon_body::mcp::serve(&socket),
        Some(cmd) => {
            let reply = horizon_body::control::request(&socket, &words.join(" "))?;
            if cmd == "chat-log" && reply.get("error").is_none() {
                for line in reply["chat"].as_array().into_iter().flatten() {
                    println!("{}", line.as_str().unwrap_or_default());
                }
                return Ok(());
            }
            println!("{reply}");
            match reply.get("error") {
                Some(e) => Err(e.as_str().unwrap_or("failed").to_string()),
                None => Ok(()),
            }
        }
    }
}

#[cfg(unix)]
fn serve(args: &[String]) -> Result<(), String> {
    use horizon_protocol::connection::Connection;
    use std::sync::{Arc, atomic::Ordering};
    let server = flag(args, "--server").unwrap_or_else(|| "127.0.0.1:25565".into());
    let name = flag(args, "--name").ok_or(USAGE)?;
    let socket = PathBuf::from(flag(args, "--socket").ok_or(USAGE)?);
    let items = match flag(args, "--resources") {
        Some(dir) => Some(horizon_body::load_items(std::path::Path::new(&dir))?),
        None => None,
    };
    let (host, port) = server
        .rsplit_once(':')
        .map_or((server.as_str(), 25565), |(h, p)| {
            (h, p.parse().unwrap_or(25565))
        });
    let stream =
        std::net::TcpStream::connect((host, port)).map_err(|e| format!("{server}: {e}"))?;
    let _ = stream.set_nodelay(true);
    let conn = horizon_body::join(
        Connection::from_stream(stream).map_err(|e| e.to_string())?,
        host,
        port,
        &name,
    )?;
    let body = horizon_body::Body::spawn(conn, &name, items);
    eprintln!(
        "horizon-body: {name} joined {server}; control socket {}",
        socket.display()
    );
    let watched = Arc::clone(&body);
    let path = socket.clone();
    std::thread::spawn(move || {
        while watched.online.load(Ordering::SeqCst) {
            std::thread::sleep(std::time::Duration::from_millis(250));
        }
        let why = horizon_body::lock(&watched.state)
            .error
            .clone()
            .unwrap_or_default();
        eprintln!("horizon-body: {} left the server: {why}", watched.name);
        let _ = std::fs::remove_file(&path);
        std::process::exit(1);
    });
    horizon_body::control::serve(body, &socket)
}
