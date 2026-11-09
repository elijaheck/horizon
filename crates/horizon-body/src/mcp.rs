//! `horizon-body --socket PATH mcp`: the body's tools over the Model Context Protocol, named and
//! shaped as `horizon-ctl mcp` names them, so a harness's policy reads both the same way.
//! There is no `command` tool: an agent says words; the harness runs commands.

use serde_json::{Value, json};
use std::path::Path;

const INSTRUCTIONS: &str = "You have a body in a Horizon (Minecraft Java) world: a creative-mode player that flies. Read where you are with status and what people said with messages; act with goto, place, break, signedit and chat. Coordinates are block coordinates. Each action blocks until it is done and reports what happened.";

fn xyz(extra: Value) -> Value {
    let mut props = json!({
        "x": { "type": "number", "description": "block X" },
        "y": { "type": "number", "description": "block Y" },
        "z": { "type": "number", "description": "block Z" },
    });
    let mut required = vec![json!("x"), json!("y"), json!("z")];
    if let (Some(p), Some(e)) = (props.as_object_mut(), extra.as_object()) {
        for (k, v) in e {
            if v.get("required").and_then(Value::as_bool) == Some(true) {
                required.push(json!(k));
            }
            let mut v = v.clone();
            if let Some(o) = v.as_object_mut() {
                o.remove("required");
            }
            p.insert(k.clone(), v);
        }
    }
    json!({ "type": "object", "properties": props, "required": required })
}

pub fn tools() -> Vec<Value> {
    let none = json!({ "type": "object", "properties": {} });
    vec![
        json!({ "name": "status", "description": "Where the body is, where it is flying to, and whether it is online.", "inputSchema": none }),
        json!({ "name": "goto", "description": "Fly to a position. Whole-number X and Z aim at the block's centre.", "inputSchema": xyz(json!({ "timeout": { "type": "number", "description": "seconds (default 60)" } })) }),
        json!({ "name": "place", "description": "Place a block of ITEM (such as oak_planks) at X Y Z, flying within reach first.", "inputSchema": xyz(json!({ "item": { "type": "string", "description": "item name", "required": true } })) }),
        json!({ "name": "break", "description": "Break the block at X Y Z, flying within reach first.", "inputSchema": xyz(json!({})) }),
        json!({ "name": "signedit", "description": "Write up to four lines on the front of the sign at X Y Z.", "inputSchema": xyz(json!({ "lines": { "type": "array", "items": { "type": "string" }, "maxItems": 4, "required": true } })) }),
        json!({ "name": "chat", "description": "Say a line in chat (words only, no /commands).", "inputSchema": { "type": "object", "properties": { "message": { "type": "string" } }, "required": ["message"] } }),
        json!({ "name": "messages", "description": "Recent chat, newest first. since: only lines after this seq.", "inputSchema": { "type": "object", "properties": { "n": { "type": "integer" }, "since": { "type": "integer" } } } }),
    ]
}

fn num(args: &Value, k: &str) -> Result<f64, String> {
    args.get(k)
        .and_then(Value::as_f64)
        .ok_or_else(|| format!("{k} is required and must be a number"))
}

fn one_line(s: &str) -> String {
    s.replace(['\n', '\r'], " ")
}

/// The control-socket line a tool call becomes.
pub fn line_for(name: &str, args: &Value) -> Result<String, String> {
    let at = || -> Result<String, String> {
        Ok(format!(
            "{} {} {}",
            num(args, "x")?,
            num(args, "y")?,
            num(args, "z")?
        ))
    };
    Ok(match name {
        "status" => "status".into(),
        "goto" => match args.get("timeout").and_then(Value::as_f64) {
            Some(t) => format!("goto {} --timeout {t}", at()?),
            None => format!("goto {}", at()?),
        },
        "place" => {
            let item = args
                .get("item")
                .and_then(Value::as_str)
                .ok_or("item is required")?;
            if item.is_empty() || item.contains(char::is_whitespace) {
                return Err("item is one name, such as oak_planks".into());
            }
            format!("place {} --item {item}", at()?)
        }
        "break" => format!("break {}", at()?),
        "signedit" => {
            let lines: Vec<String> = args
                .get("lines")
                .and_then(Value::as_array)
                .ok_or("lines is required")?
                .iter()
                .take(4)
                .map(|l| one_line(l.as_str().unwrap_or_default()).replace('|', "/"))
                .collect();
            format!("signedit {} {}", at()?, lines.join("|"))
        }
        "chat" => {
            let text = one_line(
                args.get("message")
                    .and_then(Value::as_str)
                    .ok_or("message is required")?,
            );
            if text.trim_start().starts_with('/') {
                return Err("chat takes words, not /commands".into());
            }
            format!("chat {text}")
        }
        "messages" => {
            let n = args.get("n").and_then(Value::as_u64).unwrap_or(20);
            let since = args.get("since").and_then(Value::as_u64).unwrap_or(0);
            format!("messages {n} --since {since}")
        }
        other => return Err(format!("unknown tool {other}")),
    })
}

fn handle(socket: &Path, method: &str, params: &Value) -> Result<Value, (i64, String)> {
    match method {
        "tools/list" => Ok(json!({ "tools": tools() })),
        "tools/call" => {
            let name = params
                .get("name")
                .and_then(Value::as_str)
                .ok_or((-32602, "name is required".to_string()))?;
            if !tools().iter().any(|t| t["name"] == name) {
                return Err((-32602, format!("Unknown tool {name}")));
            }
            let args = params.get("arguments").cloned().unwrap_or(json!({}));
            let reply =
                line_for(name, &args).and_then(|line| crate::control::request(socket, &line));
            let (reply, error) = match reply {
                Ok(v) => {
                    let error = crate::control::reply_error(&v).is_some();
                    (v, error)
                }
                Err(e) => (json!({ "error": e }), true),
            };
            Ok(json!({
                "content": [{ "type": "text", "text": serde_json::to_string_pretty(&reply).unwrap_or_default() }],
                "isError": error,
            }))
        }
        _ => Err((-32601, format!("Method not found: {method}"))),
    }
}

pub fn serve(socket: &Path) -> Result<(), String> {
    horizon_mcp::Server::new("horizon-body", env!("CARGO_PKG_VERSION"), INSTRUCTIONS)
        .serve(|method, params| handle(socket, method, params))
}
