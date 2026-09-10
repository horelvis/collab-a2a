# Persistent Messaging and OpenCode Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Deliver durable Collab messages into explicitly bound existing OpenCode sessions, surviving disconnections and process restarts without equating transport delivery with task execution.

**Architecture:** Extend the existing Collab hub with mailbox tables and authenticated queue routes. A Python local client owns the durable outbox and delivery journal; a small TypeScript OpenCode plugin drives it through a JSON-lines child-process interface and uses the host SDK for session operations. Keep conversation rooms separate from mailbox lifetime.

**Tech Stack:** Python >=3.10, sqlite3, existing FastAPI/httpx stack, pytest; TypeScript, OpenCode plugin SDK, Node >=22 for plugin unit tests. The installed Mac has OpenCode 1.18.30 and Node 24.20.0; Bun is not on PATH.

**Spec:** `docs/superpowers/specs/2026-09-09-persistent-messaging-opencode-design.md`

## Global Constraints

- One configurable queue server per collaboration environment; no machine-specific defaults.
- Stable agent/project identity, independent of rooms and OpenCode session IDs.
- Python >=3.10; macOS and Linux clients.
- Preserve existing CLI, A2A, room and wake behavior for unconfigured users.
- Existing SQLite databases migrate additively; never delete history to adopt queues.
- Durable storage precedes every success acknowledgment.
- At-least-once transport with deduplication; no exactly-once external-action promise.
- Explicit binding and mode choice. Never spawn OpenCode for a closed session.
- Text and references only; no new durable attachment service.
- Keep user changes, including untracked ROADMAP.md. No commits, pushes or PRs.
- Backend Jarvis changes, Linux service operations and deployment are performed by the backend instance through Collab, not SSH from Mac.
- Do not install the development plugin globally until adapter tests pass; then verify with the actual OpenCode host.

## Structure and shared contracts

Create focused modules rather than expanding the existing large CLI/store files:

| File | Responsibility |
| --- | --- |
| `src/collab/messaging/model.py` | Validated wire records, errors and bounds |
| `src/collab/messaging/store.py` | Server mailbox/message/receipt/lease transactions |
| `src/collab/messaging/routes.py` | Authenticated queue HTTP router |
| `src/collab/messaging/local.py` | Outbox, binding and attempt journal |
| `src/collab/messaging/client.py` | HTTP/retry client and recoverable queue consumption |
| `src/collab/messaging/cli.py` | `collab queue` parser, configuration and local bridge |
| `plugins/opencode-collab/src/adapter.ts` | Version-specific OpenCode SDK calls |
| `plugins/opencode-collab/src/scheduler.ts` | Batching, idle gating, cancellation and loop limits |
| `plugins/opencode-collab/src/bridge.ts` | Framed Python process transport |
| `plugins/opencode-collab/src/index.ts` | Plugin hooks and tools |
| `plugins/opencode-collab/test/*.test.ts` | Adapter, bridge and scheduler behavior |
| `docs/persistent-messaging.md` | Configuration, state meanings and recovery operations |

Create `src/collab/messaging/__init__.py`. Use snake_case JSON fields consistently in Python and TypeScript. Public methods below return JSON-compatible dictionaries; domain failures use `QueueError(code, detail)` with codes `invalid`, `conflict`, `forbidden`, `not_found`, `stale_lease`, `unavailable`.

```python
from dataclasses import dataclass

@dataclass(frozen=True)
class Message:
    id: str
    sender: str  # stable mailbox ID, owner checked against authentication
    recipients: tuple[str, ...]
    kind: str  # request | response | informational
    text: str
    created_at: str
    conversation: str
    reply_to: str | None = None

@dataclass(frozen=True)
class Binding:
    mailbox: str
    runtime: str
    session: str
    directory: str
    mode: str  # automatic | notification
```

Use existing protocol text limits; validate UTF-8 byte bounds for batching separately. Defaults: settling window 1 second, 20 messages/32 KiB per batch, maximum wait 5 seconds, lease TTL 60 seconds renewed every 20 seconds, retry base 1 second/cap 60 seconds with jitter, automatic-turn limit 10 per binding. Reject impossible or nonpositive settings. An individual legal message larger than the batch byte limit is delivered alone, without truncation. Keep the turn counter and pauses on disk.

## Task 1: Transactional server mailbox store

**Files:** create model.py, store.py and `tests/test_queue_store.py`.

**Interfaces:** `QueueStore(path: Path, clock: Callable[[], float] = time.time)` owns its connection; `close()`. `mailbox(owner: str, name: str) -> dict` creates/idempotently retrieves a mailbox. `accept(owner: str, message: Message) -> dict` returns `{id, accepted_at, recipients: [{mailbox, seq}]}`. `pending(owner: str, mailbox: str, limit: int = 100) -> list[dict]` authorizes ownership and returns unacknowledged records ordered by sequence.

- [x] Write the restart/deduplication test first:

```python
from collab.messaging.model import Message
from collab.messaging.store import QueueStore

def test_acceptance_survives_reopen_and_lost_reply(tmp_path):
    path = tmp_path / 'queue.db'
    q = QueueStore(path)
    a = q.mailbox('p_a', 'mac/ios')['id']
    b = q.mailbox('p_b', 'backend/api')['id']
    m = Message('m_1', a, (b,), 'request', 'diagnose',
                '2026-09-10T00:00:00Z', 'c_1')
    receipt = q.accept('p_a', m)
    q.close()
    q = QueueStore(path)
    assert q.accept('p_a', m) == receipt
    assert [x['id'] for x in q.pending('p_b', b)] == ['m_1']
    q.close()
```

- [x] Run `.venv/bin/python -m pytest tests/test_queue_store.py -q`; confirm missing implementation failure.
- [x] Implement additive tables `queue_mailboxes`, `queue_messages`, `queue_deliveries`, `queue_transitions`, `queue_leases`. Scope message uniqueness to sender mailbox/ID, unique mailbox ownership/name, per-mailbox sequence. Use `BEGIN IMMEDIATE`, rollback on any failure, WAL with synchronous FULL, parameterized SQL, bounded lock wait. Compare canonical immutable message fields on duplicate; changed recipients/content yields conflict. Allocate all recipient sequences and insert all delivery rows atomically.
- [x] Extend tests with changed-body conflict, forged sender, recipient authorization, two concurrent connections accepting the same message, missing recipient rollback, order, and trigger-induced write failure. Assertions check reopen state, not only return values.
- [x] Run `.venv/bin/python -m pytest tests/test_queue_store.py -q` and inspect the patch. Do not mark transport implemented at this stage.

## Task 2: Fenced consumption and distinct receipts

**Files:** modify store.py; create `tests/test_queue_leases.py`.

**Interfaces:** `acquire(owner: str, binding: Binding) -> dict` returns `{token, generation, expires_at}`; `renew(owner, mailbox, token, generation) -> dict`; `release(owner, mailbox, token, generation) -> None`; `receipt(owner, mailbox, message_id, token, generation, state, attempt_id) -> dict`. States are `delivered` and `acknowledged`. `invalidate_leases()` runs once when a server lifetime starts, retaining generations.

- [x] Write a fake-clock test for contention and fencing:

```python
import pytest
from collab.messaging.model import Binding, QueueError
from collab.messaging.store import QueueStore

def test_expired_consumer_cannot_renew_after_takeover(tmp_path):
    now = [100.0]
    q = QueueStore(tmp_path / 'q.db', clock=lambda: now[0])
    box = q.mailbox('p_a', 'mac/ios')['id']
    one = Binding(box, 'r1', 's1', '/project', 'automatic')
    two = Binding(box, 'r2', 's2', '/project', 'automatic')
    old = q.acquire('p_a', one)
    with pytest.raises(QueueError):
        q.acquire('p_a', two)
    now[0] += 61
    fresh = q.acquire('p_a', two)
    assert fresh['generation'] > old['generation']
    with pytest.raises(QueueError):
        q.renew('p_a', box, old['token'], old['generation'])
```

- [x] Run `.venv/bin/python -m pytest tests/test_queue_leases.py -q` and confirm failure.
- [x] Implement transaction-scoped owner/token/generation/expiry checks, rotating generations after expiry or server restart. Acquiring does not acknowledge messages. Receipts only advance state, must name a delivered message of the authorized mailbox, and append durable transition evidence. Release invalidates the reservation. Repeated identical receipts return the current result.
- [x] Test cross-mailbox receipts, stale ACKs after takeover, restart invalidation, duplicate ACK, ACK-before-delivery refusal, delivered messages remaining pending, and acknowledged messages retained in history.
- [x] Run both queue store and lease suites and review.

## Task 3: Authenticated hub queue API

**Files:** create routes.py, `tests/test_queue_routes.py`; modify `src/collab/server/app.py` at create_app, plus lifecycle cleanup. Keep the queue tables in the hub SQLite database so backup/restart retains owners and queues together.

**Interfaces:** `register_queue_routes(app, queue, require, participants)` installs `/ext/collab/v1/queue/*`. Pass existing `_require` and participant lookup; never accept owner IDs as authority from JSON. APIs: POST `/mailboxes` with `{name}`; POST `/messages` with Message JSON; GET `/mailboxes/{id}/pending`; POST `/mailboxes/{id}/lease` with Binding; POST `/mailboxes/{id}/renew`; POST `/mailboxes/{id}/release`; POST `/mailboxes/{id}/receipts`; GET `/mailboxes/{id}/status`.

- [x] Add this integration test using existing fixtures:

```python
def test_queue_requires_existing_collab_auth(client, host_headers):
    path = '/ext/collab/v1/queue/mailboxes'
    assert client.post(path, json={'name': 'mac/ios'}).status_code == 401
    r = client.post(path, json={'name': 'mac/ios'}, headers=host_headers)
    assert r.status_code == 200
    assert r.json()['name'] == 'mac/ios'
```

- [x] Run `.venv/bin/python -m pytest tests/test_queue_routes.py -q`; expect missing-route failure.
- [x] Register routes using `asyncio.to_thread` for SQLite calls and existing authentication. Translate invalid=400, conflict/stale_lease=409, forbidden=403, not_found=404, unavailable=503. Validate bounded JSON and reject unknown message kinds. Derive actor from `request.user.id`. Mount queue lifecycle on the same hub process, with lease invalidation at startup and connection cleanup at shutdown.
- [x] For room fan-out, accept an explicit room target instead of recipients; snapshot mailboxes whose owners belong to the authorized conversation in the same acceptance transaction. Reject ambiguous multiple target forms and empty destination sets; retries reuse the original snapshot. Room removal never cascades into queue tables.
- [x] Test authenticated send/lease/deliver/ACK end-to-end, guest ownership checks, malformed payloads, room closure, room-member changes between retries, and reopen the hub database with the same participant token.
- [x] Run `.venv/bin/python -m pytest tests/test_queue_routes.py tests/test_hub.py tests/test_resume.py tests/test_chat_is_the_only_kind_a_client_sends.py -q`.

## Task 4: Durable local outbox and delivery journal

**Files:** create local.py, `tests/test_queue_local.py`.

**Interfaces:** `LocalStore(path: Path)`; `enqueue(server: str, message: Message) -> str`; `due(now: float) -> list[dict]`; `accepted(id: str, receipt: dict) -> None`; `retry(id: str, error: str, next_at: float) -> None`; `bind(server: str, binding: Binding) -> None`; `begin_attempt(binding: Binding, message_ids: list[str]) -> dict`; `finish_attempt(id: str, state: str, opencode_message_id: str | None) -> None`; `status() -> dict`; `close()`. Attempt states: prepared, delivered, uncertain, cancelled. Persist the session/directory/mode and loop counter with each binding.

- [x] Write reopen tests first:

```python
from collab.messaging.local import LocalStore
from collab.messaging.model import Message

def test_offline_outbox_survives_process_exit(tmp_path):
    path = tmp_path / 'local.db'
    s = LocalStore(path)
    m = Message('m1', 'a', ('b',), 'request', 'hello', 'now', 'c')
    assert s.enqueue('http://queue:9920', m) == 'm1'
    s.close()
    s = LocalStore(path)
    rows = s.due(float('inf'))
    assert len(rows) == 1
    assert rows[0]['message']['id'] == 'm1'
```

- [x] Run `.venv/bin/python -m pytest tests/test_queue_local.py -q`; confirm failure.
- [x] Implement transactional outbox and journal. Separate server acceptance from session delivery. Preserve record IDs across retries. Store global data under the Collab config parent, overrideable by the existing test environment; do not use application `.collab` paths for this new durable state. Protect configuration permissions; credentials are references to existing private profiles rather than printed arguments.
- [x] Test write failures, server isolation, duplicate enqueue conflict, due ordering/backoff persistence, prepared attempt after crash, cancelled state and persisted turn cap. No automatic history deletion.
- [x] Run local tests and `tests/test_the_suite_never_reads_your_config.py`.

## Task 5: Queue client, CLI and JSON-lines bridge

**Files:** create client.py, messaging/cli.py, `tests/test_queue_client.py`, `tests/test_queue_cli.py`; minimally register subparser in `src/collab/cli.py`. Update CLI reference and command-coverage docs alongside this task.

**Interfaces:** `QueueClient(url, token, local, transport=None)`; `flush(now: float) -> dict`; `poll(binding: Binding) -> list[dict]`; lease/receipt methods use Task 3 API. `collab queue configure --server URL --profile SESSION --identity NAME --mode MODE`, `send --to MAILBOX --kind KIND TEXT`, `status --json`, `bridge`. Bridge stdin/stdout messages are `{id, method, params}` and `{id, result}` or `{id, error: {code, detail}}`; stderr only for diagnostics. Methods: bind, poll, begin_attempt, finish_attempt, receipt, send, status, pause, resume, release. Every request includes the bound session context where relevant. Do not invoke a shell for message content.

- [x] Write a client test using `httpx.MockTransport`: first request raises ConnectError, reopen LocalStore, second succeeds, and both attempts carry the identical stored message ID. Add a command subprocess test proving `send` reports queued while the server is down.

```python
def assert_same_logical_message(requests):
    import json
    bodies = [json.loads(request.content) for request in requests]
    assert len(bodies) == 2
    assert bodies[0]['id'] == bodies[1]['id']
```

- [x] Run `.venv/bin/python -m pytest tests/test_queue_client.py tests/test_queue_cli.py -q`; confirm failure.
- [x] Flush using persisted scheduling; transient connection/5xx failures retry, 401/403 pause for credential correction, 400/409 payload conflicts remain visibly blocked. Never erase an outbox record on ambiguous failure. Implement a client-owned background loop while bridge runs; use a held server request for pending changes with bounded timeout and reconnect backoff, avoiding model-driven polling. SQLite remains the source of truth; notification loss triggers a pending query on reconnect.
- [x] Implement bounded JSON-lines framing and ID matching. On bridge EOF suspend deliveries/release if reachable; leave outbox durable. Capture stderr without message bodies/tokens. Configure is explicit, validates destination URL/profile ownership and requires chosen mode; status omits credentials.
- [x] Test reconnect, cancellation, unknown method, invalid JSON, bridge crash, stale reservation, oversized input, 401 blocking, server-down persistence, and status counts by actual database state.
- [x] Run `.venv/bin/python -m pytest tests/test_queue_client.py tests/test_queue_cli.py tests/test_docs_match_cli.py tests/test_every_verb_an_agent_needs_is_on_a_page_it_reads.py -q`.

## Task 6: OpenCode adapter and compatibility gate

**Files:** create plugin package.json/tsconfig.json, adapter.ts and `test/adapter.test.ts`. Use Node test runner and TypeScript compiler; pin SDK/plugin dependencies after verifying exported types match installed OpenCode. No Bun executable required for tests.

**Interfaces:** `SessionHost.get(session): Promise<{id,directory,status}>`; `messages(session): Promise<Array<{id,text}>>`; `deliver(session, marker, text): Promise<{message_id}>`; `notify(text): Promise<void>`. Adapter wraps actual supplied plugin `client`; never `createOpencode()` or session.create(). Marker is `[collab-delivery:<attempt-id>]` in the persisted input part.

- [x] Write adapter contract tests with a recording SDK double:

```typescript
import assert from 'node:assert/strict'
import test from 'node:test'
import { createHost } from '../src/adapter.js'

test('delivery targets the bound session and carries its marker', async () => {
  const calls: any[] = []
  const client: any = { session: { prompt: async (x: any) => {
    calls.push(x); return { data: { info: { id: 'reply1' } } }
  } } }
  const host = createHost(client)
  await host.deliver('ses_bound', '[collab-delivery:a1]', 'hello')
  assert.equal(calls[0].path.id, 'ses_bound')
  assert.match(JSON.stringify(calls[0].body.parts), /collab-delivery:a1/)
})
```

- [x] Run `npm test` in `plugins/opencode-collab`; observe missing adapter failure.
- [x] Inspect the SDK types installed with the target OpenCode version. Implement exact call shapes and error extraction, await host readiness, and verify message incorporation by querying the input marker, not by using the assistant reply ID as proof. Reject missing session/different canonical directory. A failed SDK call is not proof of absence.
- [x] Run a local isolated OpenCode host with the plugin adapter, using existing supported APIs, and prove: same session ID receives the marker; history exposes it after restart; status identifies a running turn; cancellation is distinguishable. Do not call a model merely for smoke-test configuration parsing. Model-dependent behavior requires a designated test session.
- [x] If history correlation or cancellation cannot be established, record the concrete incompatibility and pause automatic delivery implementation. Do not replace this with new `opencode run` sessions. Run `npm test` and `npm run typecheck`.

## Task 7: Scheduler, agent tools and host plugin

**Files:** create bridge.ts, scheduler.ts, index.ts, `test/scheduler.test.ts`, `test/bridge.test.ts`; modify package exports.

**Interfaces:** scheduler receives SessionHost, a bridge implementing Task 5 methods, a clock, and the explicit Binding. `onStatus(status)`, `onPending(records)`, `processOnce()`, `pause(reason)`, `resume()`, `cancel()`; all return promises. Persist attempt BEFORE host deliver. Confirm delivered through bridge AFTER history incorporation. Tool `collab_ack` explicitly calls receipt for IDs delivered to its execution-context session. Tools also expose bind, send, status, process, pause and resume.

- [x] Write fake-clock scheduler tests before production code: busy+pending gives zero host calls; idle+automatic+request gives one grouped call; notification mode gives zero calls until processOnce; informational records and receipts never activate. Assert generated marker and begin_attempt ordering before host invocation.

```typescript
test('busy session never receives a new prompt', async () => {
  const h = makeHarness({ mode: 'automatic', status: 'busy' })
  await h.scheduler.onPending([h.request('m1')])
  await h.advance(5000)
  assert.equal(h.deliveries.length, 0)
})
```

Define `makeHarness` in `test/helpers.ts`: in-memory bridge matching Task 5, configurable fake SessionHost, advanceable timers, recorded deliveries, and a `request(id)` factory returning the Message fields plus seq. Recreate schedulers over the same journal double for restart tests.

- [x] Run `npm test`; confirm failing cases.
- [x] Implement serialized per-binding scheduling; recheck session status/directory and lease immediately before submit. If a human turn wins the race, retain the batch rather than interrupt. Bind explicitly through tool execution context; never pick the latest session. A child process transports JSON without shell interpolation. Events update state and do not block the host event callback while waiting for model completion.
- [x] Register plugin tools with schemas. Acuses require bound tool context and delivered IDs. Incoming text is clearly peer communication. Manual process handles one batch in notification mode. Cancellation pauses the affected attempt until explicit retry; acknowledged IDs retain state. Persist consecutive automatic turns before scheduling; reaching 10 pauses until user intervention.
- [x] Reconcile prepared attempts via marker lookup. Found=delivered; conclusively absent with completed old attempt=retryable; unknown=uncertain and paused. Lost lease suspends work. New-session binding does not copy uncertain attempts into that session silently.
- [x] Test crash boundaries, marker found/absent/unknown, duplicated notifications, batch overflow/large single message, loop counter restart, two runtime conflicts, cancellation, wrong workspace, ACK context forgery, bridge EOF and command injection payload as literal text.
- [x] Run `npm test` and `npm run typecheck`; review all activation paths for absence of spontaneous retries of delivered-but-unacknowledged work.

## Task 8: Recovery evidence, installation and coordinated acceptance

**Files:** create `tests/test_queue_recovery.py`, `docs/persistent-messaging.md`, `docs/superpowers/evidence/2026-09-10-persistent-messaging.md`; update ROADMAP.md, README.md and SPEC.md with versioned queue capability and commands. Existing ROADMAP content is user work: edit narrowly.

- [x] Add subprocess recovery tests: sender dies with outbox, hub dies between commit and response, room disappears, lease owner partitions, write failure causes no acceptance. Use isolated COLLAB_CONFIG/COLLAB_HOME/COLLAB_PEERS_DIR and ephemeral local ports.
- [x] Run `.venv/bin/python -m pytest tests/test_queue_recovery.py -q`; implement any missing recovery path, then rerun affected tests.
- [x] Run the full `.venv/bin/python -m pytest -q` once and plugin `npm test`/`npm run typecheck`. Investigate new failures; do not mask baseline failures.
- [x] Document exact configure/bind/mode/pause/retry commands produced in Task 5, state semantics, chosen defaults, recovery limits, supported OpenCode version, and a persistent-server example using existing `host --resume ... --keep` under the operator-selected service manager. Do not install a service on the user's behalf in this step.
- [ ] Install the verified plugin in Mac OpenCode using the supported local-package config entry, preserving existing plugins. Tell the user a restart is necessary. Test actual plugin load without claiming mock tests prove host compatibility.
- [ ] Recover the existing Collab coordination room if required and request Linux counterpart validation/deployment from backend. Record an explicit response; daemon connectivity is not a response. Do not modify Jarvis backend from Mac.
- [ ] Execute the spec's 12 acceptance scenarios with real Mac/Linux OpenCode sessions. Evidence rows contain scenario, command/action, server message ID, session ID, observed transitions, result and artifact reference. Mark unavailable Linux or restart windows blocked, never passed.
- [ ] Update roadmap to implemented only for verified capabilities; retain exact unverified acceptance items. Review diff, secrets, untracked files and test outputs. No commit/push.

## Self-review / coverage map

| Spec requirement | Tasks |
| --- | --- |
| Durable acceptance, ordering, fan-out, conflict dedupe | 1, 3 |
| Identity, owner authorization, expiring single consumer | 1–3 |
| Offline send/retry, recovery, status | 4–5, 8 |
| Existing OpenCode session/history compatibility | 6 |
| Modes, batching, cancellation, loop cap, ACK tools | 7 |
| Uncertain delivery reconciliation | 4, 6–7 |
| Configurable server, lifecycle independence | 3, 5, 8 |
| Real macOS/Linux proof and legacy regression | 8 |

Execution checkpoints: after Tasks 1–3 (server contract), 4–5 (durable client), 6 (host compatibility gate), 7–8 (plugin and acceptance). A passing component is not completion of the overall spec.
