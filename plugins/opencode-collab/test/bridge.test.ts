/**
 * The pipe, on its own: framing, matching, limits and what happens when the
 * other end stops being there.
 */
import assert from 'node:assert/strict'
import test from 'node:test'
import { PassThrough } from 'node:stream'

import { Bridge, BridgeError, MAX_LINE_BYTES } from '../src/bridge.ts'

function fakeChild() {
  const stdout = new PassThrough()
  const stderr = new PassThrough()
  const written: string[] = []
  const handlers: Record<string, (...args: any[]) => void> = {}
  const child: any = {
    stdin: { write: (chunk: string) => { written.push(chunk); return true }, end: () => {} },
    stdout, stderr,
    kill: () => {},
    on: (event: string, fn: any) => { handlers[event] = fn },
  }
  return { child, stdout, stderr, written, handlers }
}

function harness(extra: any = {}) {
  const fake = fakeChild()
  const diagnostics: string[] = []
  const notices: any[] = []
  const bridge = new Bridge({
    spawn: () => fake.child,
    timeoutMs: 200,
    onDiagnostic: (what) => diagnostics.push(what),
    onNotification: (message) => notices.push(message),
    ...extra,
  })
  bridge.start()
  return { bridge, ...fake, diagnostics, notices }
}

test('an answer is matched to its request by id, whatever order they come in', async () => {
  const h = harness()
  const first = h.bridge.call('status', {})
  const second = h.bridge.call('poll', { mailbox: 'mb_1' })
  h.stdout.write(JSON.stringify({ id: '2', result: ['a record'] }) + '\n')
  h.stdout.write(JSON.stringify({ id: '1', result: { outbox: {} } }) + '\n')
  assert.deepEqual(await second, ['a record'])
  assert.deepEqual(await first, { outbox: {} })
  assert.equal(JSON.parse(h.written[0]).method, 'status')
})

test('an error reply arrives as an error with its code', async () => {
  const h = harness()
  const call = h.bridge.call('receipt', {})
  h.stdout.write(JSON.stringify({ id: '1', error: { code: 'stale_lease', detail: 'gone' } }) + '\n')
  await assert.rejects(() => call,
    (error: any) => error instanceof BridgeError && error.code === 'stale_lease')
})

test('a message travels as JSON, never through a shell', async () => {
  const h = harness()
  // Nothing answers it; the point is what went down the pipe, so its eventual
  // timeout is caught here rather than left to become an unhandled rejection.
  h.bridge.call('send', { to: 'mb_1', text: '$(rm -rf ~); `whoami`' }).catch(() => {})
  const sent = JSON.parse(h.written[0])
  assert.equal(sent.params.text, '$(rm -rf ~); `whoami`')
  assert.equal(h.written[0].endsWith('\n'), true)
  assert.equal(h.written[0].split('\n').length, 2, 'one line, one request')
})

test('a line that is not JSON is noted and the next one is still answered', async () => {
  const h = harness()
  const call = h.bridge.call('status', {})
  h.stdout.write('{not json\n')
  h.stdout.write(JSON.stringify({ id: '1', result: 'fine' }) + '\n')
  assert.equal(await call, 'fine')
  assert.deepEqual(h.diagnostics, ['unparseable-line'])
})

test('an oversized line is dropped rather than held, and does not eat the next', async () => {
  const h = harness()
  const call = h.bridge.call('status', {})
  h.stdout.write('x'.repeat(MAX_LINE_BYTES + 10))
  h.stdout.write('still the same line\n')
  h.stdout.write(JSON.stringify({ id: '1', result: 'fine' }) + '\n')
  assert.equal(await call, 'fine')
  assert.ok(h.diagnostics.includes('oversized-line'))
})

test('a line with no id and a method is a notification', async () => {
  const h = harness()
  h.stdout.write(JSON.stringify({ method: 'pending', params: { records: [1, 2] } }) + '\n')
  await new Promise((r) => setImmediate(r))
  assert.deepEqual(h.notices, [{ method: 'pending', params: { records: [1, 2] } }])
})

test('a call that is never answered fails rather than waiting forever', async () => {
  const h = harness()
  await assert.rejects(() => h.bridge.call('status', {}),
    (error: any) => error.code === 'unavailable' && /not answered/.test(error.message))
})

test('the child going away fails everything outstanding at once', async () => {
  const h = harness()
  const one = h.bridge.call('status', {})
  const two = h.bridge.call('poll', {})
  h.handlers.exit?.(1)
  await assert.rejects(() => one, (error: any) => error.code === 'unavailable')
  await assert.rejects(() => two, (error: any) => error.code === 'unavailable')
})

test('stderr reaches the diagnostic callback as a classification, one line at most', async () => {
  const h = harness()
  h.stderr.write('collab queue bridge: forbidden\nand more\n')
  await new Promise((r) => setImmediate(r))
  assert.deepEqual(h.diagnostics, ['collab queue bridge: forbidden'])
})

test('a real child process answers over a real pipe', async () => {
  // Not the collab bridge — a stand-in that speaks the same framing, so the
  // transport is exercised for real without needing a configured queue.
  const script = `
    let buffer = ''
    process.stdin.on('data', (chunk) => {
      buffer += chunk
      let at
      while ((at = buffer.indexOf('\\n')) >= 0) {
        const line = buffer.slice(0, at)
        buffer = buffer.slice(at + 1)
        const request = JSON.parse(line)
        process.stdout.write(JSON.stringify({ id: request.id, result: request.params }) + '\\n')
      }
    })
  `
  const bridge = new Bridge({ command: process.execPath, args: ['-e', script],
                              timeoutMs: 5000 })
  bridge.start()
  const answer = await bridge.call('send', { text: 'hello `whoami`' })
  assert.deepEqual(answer, { text: 'hello `whoami`' })
  bridge.close()
})
