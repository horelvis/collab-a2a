# OpenCode compatibility gate — run on 2026-09-10

The plan's Task 6 makes automatic delivery conditional on this: not on the SDK's
types looking right, but on a real host doing these four things. It was run
against a headless `opencode serve --pure` on 127.0.0.1:47311, in a scratch
directory, with the adapter under `plugins/opencode-collab/src/adapter.ts`.

**No model was called.** Incorporation was proved with `noReply: true`, and the
busy/cancel pair with `session.shell` running `sleep 8` — a real turn in the
session that costs nobody any tokens.

| Version | Value |
|---|---|
| OpenCode | 1.18.30 |
| `@opencode-ai/sdk`, `@opencode-ai/plugin` | 1.18.29 |
| Node | 24.20.0 (Bun not on PATH, and not needed) |

## What was proved

| Requirement | Call | Result |
|---|---|---|
| The bound session, and only it, receives the delivery | `session.promptAsync({path:{id}, body:{parts}})` | `ses_f747d4005ffe5P2M6Lopcn9fQw` carried `[collab-delivery:gate3]` |
| A session in another directory is refused | `session.get` → `directory` | `wrong_directory`, nothing sent |
| A session that is not there is refused | `session.get` → 404 | `no_session` |
| The history exposes the marker | `session.messages` | found, `msg_08b82c023001AimvKBxpiyvysa` |
| …and still does after the host restarts | server killed and restarted, same query | found, same message id |
| A running turn is identifiable | `session.status()` while `sleep 8` ran | `busy` |
| Cancellation is distinguishable | `session.abort({path:{id}})` | `true`, and the session was no longer busy |

## Two findings that changed the design

**`promptAsync` answers `204` with no body.** There is no message id in the
reply to correlate on, which is the premise of the marker rather than a
disappointment: the only evidence that survives this process dying is the one
written into the session's own input. `deliver()` therefore returns
`message_id: null` on this version, and nothing depends on it.

**The input is persisted a moment after it is accepted.** A marker lookup made
immediately after `promptAsync` returned «not found» for a message that was
there 300 ms later. Since «conclusively absent» is a conclusion the scheduler
acts on — it is what makes a retry legitimate — `findMarker` takes a settling
window (`MARKER_SETTLE_MS`, 3 s) before it will say no.

**An idle session is absent from `session.status()`, not listed as `idle`.**
The map held only the busy session. So `unknown` means «no turn running that
this host knows about», and the scheduler gates on `busy` — a rule of «deliver
only when idle» would have delivered nothing, ever.

## The plugin, loaded by a real host

Installed as `~/.config/opencode/plugins/collab-queue.ts`, re-exporting
`plugins/opencode-collab/src/index.ts`. A fresh `opencode serve` on 1.18.30
then listed all seven tools alongside the built-ins:

```
["invalid","question","bash","read","glob","grep","edit","write","task",
 "webfetch","todowrite","websearch","skill","apply_patch",
 "collab_bind","collab_ack","collab_send","collab_status","collab_process",
 "collab_pause","collab_resume"]
```

**A `file://` entry in the config's `plugin` array does nothing.** It was tried
first, following the shape of the existing entries: the config parsed, the host
started, no tool appeared and nothing was logged at DEBUG. That array takes npm
package names. The local-plugin directory is the supported route, and the entry
was removed again from `opencode.jsonc`.

## What this does NOT prove

- That a delivered batch activates a model turn and the agent reads it as peer
  communication. That is `noReply: false`, a designated session and a real
  model: the spec's acceptance scenarios, in Task 8.
- Anything about Linux. The Linux side is the backend instance's to run, by
  agreement, and is requested through Collab.
- That a TUI session behaves as the headless host did. The plugin registers its
  tools under `opencode serve`; the interactive host is the same process, but
  that has not been watched.
