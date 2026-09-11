# Fork roadmap

## Native wake for existing OpenCode sessions

Status: implemented and unit-tested; NOT yet verified end to end. Requested
2026-09-09, built 2026-09-10 as the durable queue plus an OpenCode plugin
(`src/collab/messaging/`, `plugins/opencode-collab/`, `docs/persistent-messaging.md`).

What is verified: the server contract, the outbox, the CLI and bridge, and the
OpenCode adapter against a real 1.18.30 host — same session receives the
marker, the history still holds it after a restart, a running turn is
identifiable and cancellation is distinguishable
(`docs/superpowers/evidence/2026-09-10-opencode-compatibility.md`).

What is NOT verified, and remains open below: automatic delivery activating a
turn in a live session with a model, the acceptance scenarios of the design
spec, and everything on Linux.

### Goal

Deliver Collab messages to an existing OpenCode session when it is idle, and
queue them when it is busy, preserving the session's context and workspace.
This should not require tmux or create a new `opencode run` for each delivery.

### Current baseline

- Collab's tmux wake can reach an existing interactive session in a tmux pane.
- Its OpenCode wake recipe starts a new non-interactive run.
- Polling works while an agent is taking turns, but does not wake an idle agent.

### Proposed implementation boundary

- Use a supported OpenCode session API or an explicit OpenCode plugin.
- Bind the wake to an operator-selected runtime, session ID and workspace.
- Reuse Collab's unread-message tracking, settling window, minimum gap and
  delivery diagnostics.
- Keep configuration outside application repositories and independent of any
  particular project or agent such as Hermes/Jarvis.
- Make activation explicit and reversible; preserve the host's permissions.

### Acceptance criteria

- An idle session receives a message and responds through Collab using the same ID.
- A busy session receives a queued message without interrupting its current task.
- Message bursts are grouped, with no duplicate delivery or unnecessary new runs.
- A closed session or changed workspace fails visibly instead of targeting another.
- Restarting the daemon resumes delivery without losing pending messages.
- Report queued, delivered and agent-acknowledged states distinctly.
- Validate on macOS and Linux with two actual OpenCode sessions, including
  reconnection, cancellation, and token/turn usage compared with polling.

### Upstream contribution

Prepare a focused PR against `rperez93/collab-a2a` after the behavior is implemented
and independently verified. Keep project-specific addresses and credentials out
of the change. Existing wake recipes must continue to work.
