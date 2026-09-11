/**
 * The adapter is the only place that knows what this OpenCode version's SDK
 * calls look like, so it is the only place these tests are about: the exact
 * shape of a prompt, where the marker ends up, and what counts as evidence
 * that a message landed.
 *
 * A recording double stands in for the client the plugin host supplies. It
 * proves the CALL is right; it cannot prove the host accepts it, which is what
 * the live check in Task 6 of the plan is for and why this file does not claim
 * compatibility on its own.
 */
import assert from 'node:assert/strict'
import test from 'node:test'

import { createHost, DeliveryError } from '../src/adapter.ts'

test('delivery targets the bound session and carries its marker', async () => {
  const calls: any[] = []
  const client: any = {
    session: {
      promptAsync: async (x: any) => {
        calls.push(x)
        return { data: { info: { id: 'reply1' } } }
      },
    },
  }
  const host = createHost(client)
  await host.deliver('ses_bound', '[collab-delivery:a1]', 'hello')
  assert.equal(calls[0].path.id, 'ses_bound')
  assert.match(JSON.stringify(calls[0].body.parts), /collab-delivery:a1/)
})

test('the marker and the text arrive as one part, in that order', async () => {
  const calls: any[] = []
  const client: any = {
    session: { promptAsync: async (x: any) => { calls.push(x); return { data: {} } } },
  }
  await createHost(client).deliver('ses_1', '[collab-delivery:a2]', 'diagnose this')
  const parts = calls[0].body.parts
  assert.equal(parts.length, 1)
  assert.equal(parts[0].type, 'text')
  assert.ok(parts[0].text.startsWith('[collab-delivery:a2]'))
  assert.ok(parts[0].text.includes('diagnose this'))
})

test('a session in another directory is refused before anything is sent', async () => {
  const calls: any[] = []
  const client: any = {
    session: {
      get: async () => ({ data: { id: 'ses_1', directory: '/elsewhere' } }),
      promptAsync: async (x: any) => { calls.push(x); return { data: {} } },
    },
  }
  const host = createHost(client)
  await assert.rejects(() => host.requireSession('ses_1', '/project'),
    (error: any) => error instanceof DeliveryError && error.code === 'wrong_directory')
  assert.equal(calls.length, 0)
})

test('a session that is gone is refused, and is not confused with a fault', async () => {
  const missing: any = {
    session: { get: async () => ({ error: { data: { message: 'not found' } }, response: { status: 404 } }) },
  }
  await assert.rejects(() => createHost(missing).requireSession('ses_gone', '/project'),
    (error: any) => error.code === 'no_session')

  const broken: any = {
    session: { get: async () => { throw new Error('socket hang up') } },
  }
  await assert.rejects(() => createHost(broken).requireSession('ses_1', '/project'),
    (error: any) => error.code === 'unreachable')
})

test('busy and idle are told apart, and an unknown session is neither', async () => {
  const client: any = {
    session: {
      status: async () => ({ data: { ses_busy: { type: 'busy' }, ses_free: { type: 'idle' } } }),
    },
  }
  const host = createHost(client)
  assert.equal(await host.status('ses_busy'), 'busy')
  assert.equal(await host.status('ses_free'), 'idle')
  assert.equal(await host.status('ses_unknown'), 'unknown')
})

test('a retrying session is busy, not idle', async () => {
  const client: any = {
    session: {
      status: async () => ({ data: { ses_1: { type: 'retry', attempt: 2, message: 'rate limited', next: 5 } } }),
    },
  }
  assert.equal(await createHost(client).status('ses_1'), 'busy')
})

test('incorporation is proved by the marker in the history, not by the reply', async () => {
  const client: any = {
    session: {
      messages: async () => ({
        data: [
          { info: { id: 'm1', role: 'user' }, parts: [{ type: 'text', text: 'unrelated' }] },
          { info: { id: 'm2', role: 'user' },
            parts: [{ type: 'text', text: '[collab-delivery:a7] hello' }] },
        ],
      }),
    },
  }
  const found = await createHost(client).findMarker('ses_1', '[collab-delivery:a7]')
  assert.deepEqual(found, { found: true, messageId: 'm2' })
})

test('a history that does not hold the marker says so, and one we cannot read does not', async () => {
  const empty: any = { session: { messages: async () => ({ data: [] }) } }
  assert.deepEqual(await createHost(empty).findMarker('ses_1', '[collab-delivery:a7]'),
    { found: false, messageId: null })

  const unreadable: any = {
    session: { messages: async () => { throw new Error('socket hang up') } },
  }
  await assert.rejects(
    () => createHost(unreadable).findMarker('ses_1', '[collab-delivery:a7]'),
    (error: any) => error.code === 'unreachable',
    'a failed call is not proof of absence')
})

test('a marker that has not been written down yet is waited for, briefly', async () => {
  let asked = 0
  const client: any = {
    session: {
      messages: async () => {
        asked += 1
        return asked < 3
          ? { data: [] }
          : { data: [{ info: { id: 'm5' }, parts: [{ type: 'text', text: '[collab-delivery:a9] hi' }] }] }
      },
    },
  }
  const host = createHost(client)
  // Without waiting, the host has not written it yet and the answer is «no».
  assert.deepEqual(await host.findMarker('ses_1', '[collab-delivery:a9]'),
    { found: false, messageId: null })
  const found = await host.findMarker('ses_1', '[collab-delivery:a9]', { settleMs: 2000 })
  assert.deepEqual(found, { found: true, messageId: 'm5' })
})

test('a settling lookup still gives up, rather than waiting forever', async () => {
  const client: any = { session: { messages: async () => ({ data: [] }) } }
  const started = Date.now()
  const answer = await createHost(client).findMarker('ses_1', '[collab-delivery:none]',
    { settleMs: 400 })
  assert.deepEqual(answer, { found: false, messageId: null })
  assert.ok(Date.now() - started >= 350, 'it did wait')
  assert.ok(Date.now() - started < 4000, 'and it did stop')
})

test('the adapter never creates a session, forks one, or picks the latest', async () => {
  const forbidden = ['create', 'fork', 'list', 'delete']
  const client: any = { session: {} }
  for (const name of forbidden) {
    client.session[name] = () => { throw new Error(`the adapter called session.${name}`) }
  }
  client.session.promptAsync = async () => ({ data: {} })
  client.session.status = async () => ({ data: {} })
  client.session.messages = async () => ({ data: [] })
  const host = createHost(client)
  await host.deliver('ses_1', '[collab-delivery:a1]', 'hello')
  await host.status('ses_1')
  await host.findMarker('ses_1', '[collab-delivery:a1]')
})

test('a cancelled turn is asked for by id and reported as done or not', async () => {
  const asked: any[] = []
  const client: any = {
    session: { abort: async (x: any) => { asked.push(x); return { data: true } } },
  }
  assert.equal(await createHost(client).cancel('ses_1'), true)
  assert.equal(asked[0].path.id, 'ses_1')
})

test('an SDK error carries its message out without the caller unwrapping it', async () => {
  const client: any = {
    session: {
      promptAsync: async () => ({ error: { data: { message: 'session is busy' } },
                                  response: { status: 409 } }),
    },
  }
  await assert.rejects(
    () => createHost(client).deliver('ses_1', '[collab-delivery:a1]', 'hi'),
    (error: any) => error instanceof DeliveryError && /session is busy/.test(error.message))
})

test('a notification reaches the person without starting a turn', async () => {
  const shown: any[] = []
  const client: any = {
    tui: { showToast: async (x: any) => { shown.push(x); return { data: true } } },
    session: { promptAsync: async () => { throw new Error('a toast must not prompt') } },
  }
  await createHost(client).notify('2 messages waiting from backend/api')
  assert.match(JSON.stringify(shown[0]), /2 messages waiting/)
})
