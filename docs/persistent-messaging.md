# Durable messages, and delivering them into an OpenCode session

A collab message reaches whoever is connected. This is the other kind: a
message that is written down on the sender's machine before it leaves, accepted
and sequenced by a queue server, and still pending after the editor was closed,
the hub restarted and the laptop went to sleep — until the agent that received
it says, by id, that it has it.

It exists because of a specific failure: two agents on two machines could only
talk while both were watching, and the person had to tell each of them to look.

> **Status.** The queue server, the outbox, the CLI and the OpenCode adapter
> are implemented and tested, and the adapter has been checked against a real
> OpenCode 1.18.30 host (`docs/superpowers/evidence/2026-09-10-opencode-compatibility.md`).
> Automatic delivery into a live session, and the Linux side, are NOT yet
> verified end to end. Until they are, treat `notification` mode as the
> supported one.

## The four states, and what each does not mean

| State | What it means | What it does not mean |
|---|---|---|
| `queued` | The sender's machine has it on disk and will keep trying. | That anything else knows about it. |
| `accepted` | The server has the message and one row per recipient, committed. | That anybody has read it. |
| `delivered` | The bound session was given it, proved by the marker in that session's own history. | That the agent understood it, or acted. |
| `acknowledged` | The agent named its id back with the `collab_ack` tool. | That the work is done — that is the task board's business. |

A message stops being pending when it is acknowledged. Nothing expires on its
own, and nothing is deleted to save room.

## Setting it up

The queue server is a collab hub. It keeps its tables in the same SQLite file,
so a session's backup carries its pending messages with it, and a hub started
with `--keep` outlives the agent that started it:

```bash
collab host --resume s_yoursession --name mac --port 9920 --bind 0.0.0.0 \
  --no-tunnel --keep --no-update-check
```

Point this machine at it, once:

```bash
collab queue configure --server http://queue.example.lan:9920 \
  --profile s_yoursession --identity mac/ios-jarvis --mode notification
```

`--profile` names a saved session, and the token is read from that profile —
it is never an argument, so it is not in the shell history or in `ps`.
`--identity` is this agent's mailbox name; it is stable across rooms and across
OpenCode sessions. `--mode` is chosen explicitly because it decides whether
another agent's message may start a turn here.

Then send and look:

```bash
collab queue send --to mb_4f3a2b1c9d8e --kind request "diagnose the second turn"
collab queue status --json
collab queue retry --message-id m_7c2
```

## Delivering into an OpenCode session

The plugin in `plugins/opencode-collab` binds ONE session to ONE mailbox and
delivers batches into it. It never creates a session, never forks one and never
picks the most recent: the session is the one whose tool call bound it, or one
restored from a binding whose session id and directory both still match.

Install it as a local plugin — one file in the directory OpenCode loads local
plugins from, re-exporting the one in this checkout:

```bash
mkdir -p ~/.config/opencode/plugins
cat > ~/.config/opencode/plugins/collab-queue.ts <<'TS'
export { CollabQueue } from "/path/to/collab-a2a/plugins/opencode-collab/src/index.ts"
TS
```

**Not through the `plugin` array in `opencode.jsonc`.** That array takes npm
package names; a `file://` entry there is accepted by the config parser and
then silently ignored — verified on 1.18.30, where the tools did not appear and
nothing was logged. The `plugins/` directory is the supported way, and with it
`collab_bind`, `collab_ack`, `collab_send`, `collab_status`, `collab_process`,
`collab_pause` and `collab_resume` are all registered by the host.

OpenCode must be restarted to load it. If `collab` is not on the PATH the host
starts with, set `COLLAB_BIN` to the absolute path of the CLI.

Then, in the session you want messages delivered into:

- `collab_bind` — bind this session to a mailbox, choosing `automatic` or
  `notification`.
- `collab_process` — take one batch now.
- `collab_ack` — acknowledge ids this session was given. Ids delivered to any
  other session are refused.
- `collab_send`, `collab_status`, `collab_pause`, `collab_resume`.

### When a batch is delivered, and when it is not

| Situation | What happens |
|---|---|
| Session busy | Nothing. The batch is kept and offered when the turn ends. |
| Session idle, `automatic`, a request or response is waiting | One turn, with the pending grouped into it. |
| `notification` mode | The person is told; `collab_process` takes a batch. |
| Only informational records waiting | Nothing is activated; they ride along with the next batch. |
| Session deleted, or its directory changed | Delivery pauses and asks to be bound again. Pending stay pending. |
| Reservation lost, or a delivery that cannot be checked | Delivery suspends until it is reconciled or resumed. |

### Defaults

| Setting | Value | Why |
|---|---|---|
| Settling window | 1 s, at most 5 s from the oldest | Groups a burst into one turn without holding the first message. |
| Batch | 20 messages or 32 KiB | One readable turn. A single message bigger than that travels alone and whole; nothing is ever cut. |
| Reservation | 60 s, renewed every 20 s | Two failures' room before somebody else may take the mailbox. |
| Retry | 1 s doubling to 60 s, jittered | A hub that is off for the night is asked once a minute. |
| Consecutive automatic turns | 10 | Then delivery pauses until a person resumes it. The count is on disk. |
| Held «anything pending?» request | 25 s client, 30 s server cap | The model is never what polls. |

## When something is wrong

`collab queue status` names the server, the identity, the counts by state, the
oldest unsent message, the last error and the next retry. Nothing in it is a
credential.

- **`blocked` messages** are a fault retrying cannot fix — a rejected token, or
  a payload the server refuses. Deal with the cause, then
  `collab queue retry`.
- **A paused binding** says why: a loop limit reached, a cancelled turn, a
  session that moved, or a delivery that could not be confirmed either way.
  `collab queue resume` starts delivery again and clears the turn count.
- **An uncertain delivery** is the one case where nothing happens
  automatically. The attempt is on the disk, the session's history could not be
  read, and a blind resend could make an agent do the same work twice. It waits
  for a person.

## Limits, said plainly

- At-least-once with deduplication. Not exactly-once: an operation that must
  survive being repeated needs its own idempotency.
- Order is per mailbox, by acceptance. There is no global order between
  machines and no ordering by the sender's clock.
- Changing the server URL does not migrate anything.
- Text and references only. Files still travel with `collab file send`, and a
  reference to one is not a promise that the file is still there.
- A reservation does not stop an action a previous session already started.
