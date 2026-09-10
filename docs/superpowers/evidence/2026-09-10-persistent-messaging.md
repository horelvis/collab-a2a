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
| 2 | Send with OpenCode closed, open it, recover after binding | partial | the outbox and the binding are covered by unit tests; the recovery INTO a live session is blocked with 4 |
| 3 | Server restarted between commit and reply → one logical message | automated | `test_the_hub_dying_between_the_commit_and_the_reply_makes_one_message` |
| 4 | Idle session receives the batch; a busy one finishes first; a human turn races it | partial | scheduler tests cover all three decisions against a fake host; the live session turn is blocked |
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

## Blocked, and why

- **A real model turn.** Everything about activation is proved against a fake
  host or a `noReply` delivery. Whether an agent reads a delivered batch as
  peer communication, acknowledges by id and answers is the designated-session
  run that has not happened yet.
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
