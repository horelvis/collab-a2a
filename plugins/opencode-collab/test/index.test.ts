/**
 * The tools an agent can actually call, and the one thing they must refuse:
 * answering for a session that is not the bound one.
 */
import assert from 'node:assert/strict'
import test from 'node:test'

import { createCollab, collabCommand } from '../src/index.ts'
import { DeliveryError } from '../src/adapter.ts'

function fakes(overrides: any = {}) {
  const calls: any[] = []
  const host: any = {
    async requireSession(session: string, directory: string) {
      if (overrides.badSession) throw new DeliveryError('no_session', 'gone')
      return { id: session, directory }
    },
    async status() { return overrides.status ?? 'unknown' },
    async messages() { return [] },
    async deliver(session: string, marker: string, text: string) {
      calls.push({ deliver: { session, marker, text } })
      return { message_id: null }
    },
    async findMarker() { return { found: true, messageId: 'msg_1' } },
    async cancel() { return true },
    async notify(text: string) { calls.push({ notify: text }) },
  }
  const bridge: any = {
    async bind(params: any) { calls.push({ bind: params }); return { lease: {} } },
    ...(overrides.waiting ? {} : {}),
    async begin_attempt(params: any) {
      return { id: 'at_1', marker: '[collab-delivery:at_1]', message_ids: params.message_ids }
    },
    async finish_attempt(params: any) { calls.push({ finish: params }); return params },
    async receipt(params: any) { calls.push({ receipt: params }); return params },
    async turn_taken() { return { turns: 1 } },
    async unfinished_attempts() { return [] },
    async pause(params: any) { calls.push({ pause: params }); return params },
    async resume(params: any) { calls.push({ resume: params }); return params },
    async poll() { return overrides.waiting ?? [] },
    async send(params: any) { return { id: 'm_1', state: 'queued', ...params } },
    async status() { return { outbox: { queued: 1 }, bindings: [] } },
    async release() { return { released: true } },
  }
  const collab = createCollab({ host, bridge, directory: '/project' })
  return { collab, host, bridge, calls }
}

const context = (session = 'ses_1', directory = '/project') => ({
  sessionID: session, directory, worktree: directory, messageID: 'msg', agent: 'build',
  abort: new AbortController().signal, metadata() {}, async ask() {},
}) as any

test('every tool an agent needs is registered, and named for what it does', () => {
  const { collab } = fakes()
  assert.deepEqual(Object.keys(collab.tools).sort(), [
    'collab_ack', 'collab_bind', 'collab_pause', 'collab_process',
    'collab_resume', 'collab_send', 'collab_status',
  ])
  for (const [name, definition] of Object.entries(collab.tools)) {
    assert.ok((definition as any).description.length > 20, `${name} explains itself`)
  }
})

test('binding takes the session from the tool context, never from an argument', async () => {
  const { collab, calls } = fakes()
  const said = await collab.tools.collab_bind.execute(
    { mailbox: 'mb_mine', mode: 'automatic' } as any, context('ses_real'))
  assert.match(String(said), /mb_mine/)
  const bound = calls.find((c) => c.bind)?.bind
  assert.equal(bound.session, 'ses_real')
  assert.equal(bound.directory, '/project')
})

test('binding asks for what was already waiting, not only for what arrives next', async () => {
  const { collab } = fakes({ waiting: [
    { id: 'm_old', sender: 'mb_them', kind: 'request', text: 'from before',
      seq: 1, state: 'pending' }] })
  await collab.tools.collab_bind.execute({ mailbox: 'mb_mine', mode: 'automatic' } as any,
    context('ses_bound'))
  assert.equal(collab.scheduler!.state.pending, 1)
})

test('a session that is not there is not bound', async () => {
  const { collab } = fakes({ badSession: true })
  await assert.rejects(() => collab.tools.collab_bind.execute(
    { mailbox: 'mb_mine' } as any, context('ses_gone')))
  assert.equal(collab.scheduler, null)
})

test('another session cannot acknowledge this one\'s messages', async () => {
  const { collab } = fakes()
  await collab.tools.collab_bind.execute({ mailbox: 'mb_mine', mode: 'automatic' } as any,
    context('ses_bound'))
  await collab.scheduler!.onPending([{ id: 'm1', sender: 'mb_them', kind: 'request',
    text: 'hello', seq: 1, state: 'pending' }])
  await collab.scheduler!.processOnce()

  await assert.rejects(
    () => collab.tools.collab_ack.execute({ ids: ['m1'] } as any, context('ses_other')),
    /not the one bound/)
})

test('acknowledging names only ids this session was given', async () => {
  const { collab, calls } = fakes()
  await collab.tools.collab_bind.execute({ mailbox: 'mb_mine', mode: 'automatic' } as any,
    context('ses_bound'))
  await collab.scheduler!.onPending([{ id: 'm1', sender: 'mb_them', kind: 'request',
    text: 'hello', seq: 1, state: 'pending' }])
  await collab.scheduler!.processOnce()

  const said = await collab.tools.collab_ack.execute(
    { ids: ['m1', 'm_never_sent'] } as any, context('ses_bound'))
  assert.match(String(said), /acknowledged m1/)
  assert.match(String(said), /not delivered to this session: m_never_sent/)
  const acks = calls.filter((c) => c.receipt?.state === 'acknowledged')
  assert.deepEqual(acks.map((c) => c.receipt.message_id), ['m1'])
})

test('a tool that needs a binding says so rather than guessing at one', async () => {
  const { collab } = fakes()
  await assert.rejects(() => collab.tools.collab_process.execute({} as any, context()),
    /collab_bind/)
  await assert.rejects(() => collab.tools.collab_ack.execute({ ids: [] } as any, context()),
    /collab_bind/)
})

test('an event for another session changes nothing here', async () => {
  const { collab } = fakes()
  await collab.tools.collab_bind.execute({ mailbox: 'mb_mine', mode: 'automatic' } as any,
    context('ses_bound'))
  await collab.onEvent({ type: 'session.status',
    properties: { sessionID: 'ses_elsewhere', status: { type: 'busy' } } })
  assert.equal(collab.scheduler!.state.session, 'unknown')
  await collab.onEvent({ type: 'session.status',
    properties: { sessionID: 'ses_bound', status: { type: 'busy' } } })
  assert.equal(collab.scheduler!.state.session, 'busy')
})

test('a pending notification with nothing bound is ignored, not queued blindly', async () => {
  const { collab, calls } = fakes()
  await collab.onNotification({ method: 'pending', params: { records: [
    { id: 'm1', sender: 'mb_them', kind: 'request', text: 'hi', seq: 1, state: 'pending' }] } })
  assert.equal(calls.length, 0)
})

test('sending goes through the bridge and reports what it did', async () => {
  const { collab } = fakes()
  const said = await collab.tools.collab_send.execute(
    { to: 'mb_them', text: 'hello' } as any, context())
  assert.equal(said, 'queued: m_1')
})

test('the collab command can be pointed somewhere else by the operator', () => {
  assert.equal(collabCommand({} as any), 'collab')
  assert.equal(collabCommand({ COLLAB_BIN: '/opt/collab/.venv/bin/collab' } as any),
    '/opt/collab/.venv/bin/collab')
})
