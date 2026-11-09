# Light agent bodies (`horizon-body`)

An agent driving Horizon through `horizon-ctl` needs a whole game client per body: renderer, world, chunk meshing, a few hundred MB each. `horizon-body` is the other option for agents that only build, chat and read chat: one offline player on the network protocol, with no game behind it. It costs one connection, two threads and a 500-line chat log, so a classroom of thirty agents fits on one machine.

It speaks the same control socket shape (`--socket PATH`, one command per line, one JSON reply per line) and the same MCP tool names as `horizon-ctl`, so a harness written for `horizon-ctl` can point at either.

## Run one

```sh
horizon-body serve --server 127.0.0.1:25565 --name EckoClaude --socket ~/.ecko/hz/EckoClaude.sock \
  --resources "$HORIZON_HOME/server/resources"     # item registry, only needed for place
horizon-body --socket ~/.ecko/hz/EckoClaude.sock status
horizon-body --socket ~/.ecko/hz/EckoClaude.sock goto 10 70 -4
horizon-body --socket ~/.ecko/hz/EckoClaude.sock place 10 64 -4 --item oak_planks
horizon-body --socket ~/.ecko/hz/EckoClaude.sock chat-log      # raw lines, oldest first
horizon-body --socket ~/.ecko/hz/EckoClaude.sock mcp           # stdio MCP for an agent
```

The server must run with `online-mode=false` (bodies log in like `horizon-bots`) and the body must be whitelisted and in creative: it flies straight to its goal and places by setting a creative slot. `serve` exits when the server drops the body.

## Commands

| Command | Does |
| --- | --- |
| `hello`, `status` | identity; position, goal, online and the last error |
| `goto X Y Z [--timeout S]` | fly straight there at creative flying speed; whole-number X and Z aim at the block centre |
| `place X Y Z --item ITEM` | fly within reach, put ITEM in hotbar slot 0, click X Y Z (an empty position is replaceable, so the block lands there) |
| `break X Y Z` | fly within reach, start-destroy (instant in creative) |
| `signedit X Y Z A\|B\|C\|D` | write the front of the sign at X Y Z |
| `chat TEXT` | unsigned chat; refuses `/` |
| `command TEXT` | run a command as this player (server permission applies); not an MCP tool |
| `messages [N] [--since SEQ]` | chat newest first, each with a running `seq` |
| `chat-log [N]` | chat oldest first; the CLI prints raw lines |

## What it does not do

It decodes no chunks and has no physics: `goto` flies through blocks, and `place` and `break` trust the caller about what is there. It cannot look, attack, open containers or use items. Those need a full client (`horizon-ctl`). The proper fix, bodies the server spawns itself with no connection at all, is the next step in the fork; this crate is the version that needs no server change.
