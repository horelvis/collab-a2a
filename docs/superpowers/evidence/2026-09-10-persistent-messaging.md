# Persistent messaging — what has been proved, and what has not

Built 2026-09-10 against the plan
`docs/superpowers/plans/2026-09-10-persistent-messaging-opencode.md` and the
spec `docs/superpowers/specs/2026-09-09-persistent-messaging-opencode-design.md`.

A passing component is not the spec. This page separates the two.

## What was run

| Suite | Command | Result |
|---|---|---|
| Queue server, leases, routes, outbox, client, CLI, bridge | `.venv/bin/python -m pytest tests/test_queue_*.py -q` | 91 passed |
| Recovery, with real processes killed mid-flight | `.venv/bin/python -m pytest tests/test_queue_recovery.py -q` | 6 passed |
| Whole suite | `.venv/bin/python -m pytest -q` | 3309 passed, 19 failed, 28 errors — the SAME set as before this work (`time.tzset` missing in this interpreter, curses scrolling, the quota probe's timings). None is in `collab.messaging`; none was masked. |
| Plugin | `npm test` and `npm run typecheck` in `plugins/opencode-collab` | 52 passed, typecheck clean |
| OpenCode host | see `2026-09-10-opencode-compatibility.md` | the four gate conditions met on 1.18.30 |

## The spec's twelve acceptance scenarios

«Automated» means a test kills or refuses something real and asserts on what is
on the disk afterwards. «Blocked» means it needs a live model turn, a second
machine, or the backend instance — and is NOT claimed.

| # | Scenario | State | Where |
|---|---|---|---|
| 1 | Send with the server down, restart the client, deliver afterwards | automated | `test_a_sender_that_dies_with_an_outbox_sends_it_when_it_comes_back`, `test_an_outbox_survives_the_process_being_killed_outright` |
| 2 | Send with OpenCode closed, open it, recover after binding | automated + live | unit tests, and the live pass below |
| 3 | Server restarted between commit and reply → one logical message | automated | `test_the_hub_dying_between_the_commit_and_the_reply_makes_one_message` |
| 4 | Idle session receives the batch; a busy one finishes first; a human turn races it | automated + live | scheduler tests, and the live pass below |
| 5 | Notification mode and technical events: zero model invocations | automated (scheduler) | `notification mode delivers nothing until somebody says so`, `an informational record is filed and starts nothing` |
| 6 | Interrupt the plugin before and after incorporation; reconcile or show uncertainty | automated (scheduler + adapter) | `a delivery whose marker is nowhere is retryable`, `a delivery we cannot check is uncertain`, `a prepared attempt from a previous run is reconciled` |
| 7 | Two sessions contend for a mailbox; expired reservations authorise nothing; partition | automated | `test_expired_consumer_cannot_renew_after_takeover`, `test_a_consumer_that_is_partitioned_cannot_confirm_over_its_successor` |
| 8 | Cancel a turn: not reactivated; acknowledged and unacknowledged states preserved | automated (scheduler) | `cancelling a turn does not start it again` |
| 9 | Close a room and restart the service: its messages are still recoverable | automated | `test_a_room_that_disappears_leaves_its_messages_recoverable` |
| 10 | Bursts: order, grouping, limits, the excess left pending; acks do not activate; a loop pauses and survives a restart | automated | batch tests, `ten turns in a row pause automatic delivery`, `test_the_turn_counter_and_the_pause_outlive_the_process` |
| 11 | Failed writes and rejected credentials: no false acceptance, nothing lost | automated | `test_a_write_that_fails_is_not_reported_as_accepted`, `test_a_rejected_credential_stops_retrying_and_says_so` |
| 12 | Reuse an id with a different payload: a visible conflict, original untouched | automated | `test_the_same_id_with_a_different_body_is_a_visible_conflict` |

## Coordination, as it stands

The hub had been offline since the previous evening and was resumed on the same
session — `collab host --resume s_19328386 --name mac --port 9920 --bind
0.0.0.0 --no-tunnel --keep` — and the Linux request was sent to the room:
scope, the four compatibility points to check, the local-plugin install, and
the two version-specific findings to confirm there. It is also on the board as
`T_3b750710e49d`.

`backend` was offline when it was sent. **No response has been received**, and
none is claimed: a message in the room is not a reply, and a daemon coming back
online is not one either.

## The live pass on the Mac, 2026-09-10

A real OpenCode 1.18.30 session (`ses_f73cd1338ffefUAKIBrmuXGQsn`) bound to a
real mailbox on the running hub, with a second, isolated collab profile as the
sender. The model was `opencode/big-pickle` — a free model on the provider the
host already had, so a real turn with real tool calls cost nobody anything.

| # | Scenario | What happened | Evidence |
|---|---|---|---|
| 2 | Pending waits for the binding | `m_esc2_uno` accepted 16:42:02 with nothing bound; `collab_bind` at 16:44:24; delivered 16:44:27, acknowledged 16:44:28 | attempt `at_9a638005e7e5`, three transitions |
| 4 | A busy session is not interrupted | `m_esc4_uno` accepted 16:45:58 while a 45 s tool call ran; the session stayed `busy` (40 of 40 one-second samples through another such turn); delivered 16:46:22, one second after the turn ended; acknowledged 16:46:23 | attempt `at_3faf18c9aa66` |
| 5 | Notification mode starts nothing | 45 s with a pending request and the session's message count unchanged at 23. `collab_process` from inside a turn then delivered it as that turn ended | attempt `at_f70a1907d156` |
| 8 | A cancelled turn is not restarted | Delivered 16:59:38, the turn aborted before the agent could answer. Forty seconds later: still `delivered`, never `acknowledged`, and no second delivery | attempt `at_c9f001330d60`, one delivery transition |
| 10 | Bursts, grouping and the loop cap | 25 informational records opened no turn in 35 s; one request then activated delivery, and the batches were exactly 20 and then 6 (the remaining five plus the request). Separately, with the cap lowered to two, the binding paused itself at three turns and said so | attempts `at_19aeb8aeb846` (n=20) and `at_0466d6725809` (n=6) |

Each attempt row carries the OpenCode message id the marker landed in, so every
delivery above can be found in that session's own history.

## Four faults the live pass found

None of them showed up in 145 unit tests, and each was a «works on the bench,
fails on the machine» of a different kind.

1. **No mailbox was ever created.** `configure` recorded the name a person
   chose and nothing asked the server for the id that name belongs to, so the
   first real send would have been refused. Minted now at configure, or on the
   first flush that reaches the server.
2. **The bridge could not find its credentials.** It is started by the plugin
   in the OpenCode project's directory, and `SessionProfile.load` finds a
   profile by walking up from the current directory — which is another
   repository, or none. The config now records where the profile lives.
3. **«Process one batch now» could never work.** The tool call that asks for it
   runs inside a turn, so the session is busy by definition, and the answer was
   «the session is busy» forever. The ask is now remembered and honoured the
   moment that turn ends.
4. **A resume from a terminal reached nobody.** The plugin holds its own idea of
   being paused and cannot see the outbox change; the notification loop also
   suppressed the pending set as «already announced». The lifting of a pause is
   now said out loud, what was announced while paused is forgotten, and the
   scheduler asks the outbox before every batch.

## Blocked, and why

- **A capable model, and a long session.** The live pass ran on a free model in
  a session of a few dozen messages. Nothing here says how a batch reads to an
  agent deep in its own work, or whether a larger model acknowledges more
  reliably than this one did.
- **Linux.** By the standing agreement, the Linux side and anything touching
  the Jarvis backend belong to the `backend` instance and are requested through
  Collab. Not started here.
- **A TUI session.** The plugin is installed at
  `~/.config/opencode/plugins/collab-queue.ts` and a real host registered all
  seven of its tools, but that was `opencode serve`; nobody has watched an
  interactive session bind a mailbox and take a batch.

## Measurements not made

No comparison of turns or tokens against polling. The design says not to claim
a saving without measuring both sides the same way, and neither side has been
measured.
