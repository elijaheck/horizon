//! The body's control socket: one command per line, one JSON reply per line, in the words
//! `horizon-ctl` uses for a full client's body, so a harness can drive either.

use crate::Body;
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader, Write},
    sync::Arc,
    time::Duration,
};

/// Commands the socket answers, for `help` and errors.
pub const USAGE: &str = "hello | status | goto X Y Z [--timeout S] | place X Y Z [--item ITEM] | break X Y Z | signedit X Y Z LINE1|LINE2|LINE3|LINE4 | chat TEXT | command TEXT | messages [N] [--since SEQ] | chat-log [N] | quit";

fn number(words: &[&str], i: usize, what: &str) -> Result<f64, String> {
    let w = words.get(i).ok_or_else(|| format!("missing {what}"))?;
    w.parse::<f64>()
        .ok()
        .filter(|v| v.is_finite())
        .ok_or_else(|| format!("{what} must be a number, not {w}"))
}

fn block(words: &[&str]) -> Result<[i32; 3], String> {
    let mut out = [0; 3];
    for (i, axis) in ["X", "Y", "Z"].into_iter().enumerate() {
        out[i] = number(words, i + 1, axis)?.floor() as i32;
    }
    Ok(out)
}

/// `--flag VALUE` anywhere after the command.
fn flag<'a>(words: &[&'a str], name: &str) -> Option<&'a str> {
    words
        .iter()
        .position(|w| *w == name)
        .and_then(|i| words.get(i + 1).copied())
}

/// The words after the command, as typed (chat and commands keep their spacing).
fn rest(line: &str) -> &str {
    line.trim_start()
        .split_once(char::is_whitespace)
        .map_or("", |(_, r)| r.trim_start())
}

/// Whole-number X and Z name a block and aim at its centre, as `horizon-ctl goto` does.
fn aim(v: f64) -> f64 {
    if v.fract() == 0.0 { v + 0.5 } else { v }
}

/// Answer one command line.
pub fn handle(body: &Body, line: &str) -> Value {
    match run(body, line) {
        Ok(v) => v,
        Err(e) => json!({ "error": e }),
    }
}

fn run(body: &Body, line: &str) -> Result<Value, String> {
    let words: Vec<&str> = line.split_whitespace().collect();
    let Some(&name) = words.first() else {
        return Err(format!("no command; {USAGE}"));
    };
    match name {
        "hello" => Ok(
            json!({ "protocol": "horizon-body/1", "release": env!("CARGO_PKG_VERSION"), "name": body.name, "features": ["body"] }),
        ),
        "status" => {
            let s = crate::lock(&body.state);
            Ok(json!({
                "name": body.name,
                "online": body.online.load(std::sync::atomic::Ordering::SeqCst),
                "screen": "playing",
                "position": s.pos,
                "goal": s.goal,
                "error": s.error,
            }))
        }
        "goto" => {
            let goal = [
                aim(number(&words, 1, "X")?),
                number(&words, 2, "Y")?,
                aim(number(&words, 3, "Z")?),
            ];
            let timeout = flag(&words, "--timeout")
                .and_then(|t| t.parse::<f64>().ok())
                .unwrap_or(60.0);
            let pos = body.goto(goal, Duration::from_secs_f64(timeout.clamp(0.0, 600.0)))?;
            Ok(json!({ "arrived": true, "position": pos }))
        }
        "place" => {
            let at = block(&words)?;
            let item = flag(&words, "--item")
                .or_else(|| words.get(4).copied().filter(|w| !w.starts_with("--")))
                .ok_or("place needs --item ITEM: a body holds nothing until told")?;
            let outcome = body.place(at, item)?;
            Ok(
                json!({ "placed": item, "at": at, "confirmed": matches!(outcome, crate::Outcome::Now(_)) }),
            )
        }
        "break" => {
            let at = block(&words)?;
            let outcome = body.break_block(at)?;
            Ok(json!({ "broken": at, "confirmed": matches!(outcome, crate::Outcome::Now(_)) }))
        }
        "signedit" => {
            let at = block(&words)?;
            let text = words.get(4..).map(|w| w.join(" ")).unwrap_or_default();
            let mut lines: [String; 4] = Default::default();
            for (slot, part) in lines.iter_mut().zip(text.split('|')) {
                *slot = part.trim().chars().take(90).collect();
            }
            body.sign(at, lines.clone())?;
            Ok(json!({ "sign": at, "lines": lines }))
        }
        "chat" => {
            let text = rest(line);
            if text.is_empty() {
                return Err("chat needs TEXT".into());
            }
            body.chat(text)?;
            Ok(json!({ "sent": text }))
        }
        "command" => {
            let text = rest(line);
            if text.is_empty() {
                return Err("command needs TEXT".into());
            }
            body.command(text)?;
            Ok(json!({ "ran": text }))
        }
        "messages" => {
            let since = flag(&words, "--since")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let n = words
                .get(1)
                .filter(|w| !w.starts_with("--"))
                .and_then(|w| w.parse::<usize>().ok())
                .unwrap_or(20);
            let all = body.messages(since);
            let chat: Vec<Value> = all
                .iter()
                .rev()
                .take(n)
                .map(|(seq, text)| json!({ "seq": seq, "text": text }))
                .collect();
            Ok(json!({ "chat": chat, "last": crate::lock(&body.state).chat_count }))
        }
        "chat-log" => {
            let all = body.messages(0);
            let n = words
                .get(1)
                .and_then(|w| w.parse::<usize>().ok())
                .unwrap_or(all.len());
            let lines: Vec<&String> = all
                .iter()
                .skip(all.len().saturating_sub(n))
                .map(|(_, t)| t)
                .collect();
            Ok(json!({ "chat": lines }))
        }
        "help" => Ok(json!({ "usage": USAGE })),
        other => Err(format!("unknown command {other}; {USAGE}")),
    }
}

/// Serve the socket; `quit` ends the process.
#[cfg(unix)]
pub fn serve(body: Arc<Body>, path: &std::path::Path) -> Result<(), String> {
    use std::os::unix::net::UnixListener;
    let _ = std::fs::remove_file(path);
    let listener = UnixListener::bind(path).map_err(|e| format!("{}: {e}", path.display()))?;
    for conn in listener.incoming() {
        let Ok(conn) = conn else { continue };
        let body = body.clone();
        std::thread::spawn(move || {
            let Ok(mut out) = conn.try_clone() else {
                return;
            };
            for line in BufReader::new(conn).lines() {
                let Ok(line) = line else { break };
                if line.trim().is_empty() {
                    continue;
                }
                if line.trim() == "quit" {
                    let _ = writeln!(out, "{}", json!({ "ok": true }));
                    std::process::exit(0);
                }
                let reply = handle(&body, &line);
                if writeln!(out, "{reply}").and_then(|()| out.flush()).is_err() {
                    break;
                }
            }
        });
    }
    Ok(())
}

/// Send one line to a body's socket and read its reply.
#[cfg(unix)]
/// The error in a reply, if any. `status` always carries an `"error"` field (null when the
/// body is fine), so a null error is no error.
pub fn reply_error(reply: &Value) -> Option<&Value> {
    reply.get("error").filter(|e| !e.is_null())
}

pub fn request(path: &std::path::Path, line: &str) -> Result<Value, String> {
    use std::os::unix::net::UnixStream;
    let mut conn = UnixStream::connect(path).map_err(|e| {
        format!(
            "cannot connect to {} ({e}); is horizon-body serve running?",
            path.display()
        )
    })?;
    writeln!(conn, "{line}").map_err(|e| e.to_string())?;
    let mut reply = String::new();
    BufReader::new(conn)
        .read_line(&mut reply)
        .map_err(|e| e.to_string())?;
    if reply.is_empty() {
        return Err("the body closed the connection".into());
    }
    serde_json::from_str(reply.trim_end()).map_err(|e| format!("not JSON ({e}): {reply}"))
}
