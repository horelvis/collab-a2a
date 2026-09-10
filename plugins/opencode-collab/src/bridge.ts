/**
 * The pipe to `collab queue bridge`: one JSON object per line, each way.
 *
 * **No shell, ever.** The child is spawned with an argument array and
 * `shell: false`, and every message travels as a JSON value. A peer's text
 * reaching a shell would make the contents of a message from another machine
 * into a command on this one, which is the single worst thing this design
 * could do.
 *
 * **Three limits, doing three jobs.** A cap on one line (memory), a deadline on
 * one call (a child that stops answering), and rejection of everything
 * outstanding when the child exits (so a caller is never left waiting on a
 * process that has gone). A reader of a pipe with no ceiling grows until the
 * writer stops.
 */

import { spawn as nodeSpawn } from 'node:child_process'

import type { BridgeCalls, PendingRecord } from './scheduler.ts'

/** The longest line we will hold, matching the Python side's own cap. */
export const MAX_LINE_BYTES = 256 * 1024

/** How long one call may take before it is answered with a failure. */
export const CALL_TIMEOUT_MS = 30_000

export class BridgeError extends Error {
  readonly code: string

  constructor(code: string, detail: string) {
    super(detail)
    this.name = 'BridgeError'
    this.code = code
  }
}

type ChildLike = {
  stdin: { write(chunk: string): unknown; end(): unknown } | null
  stdout: NodeJS.ReadableStream | null
  stderr: NodeJS.ReadableStream | null
  kill(signal?: any): unknown
  on(event: string, listener: (...args: any[]) => void): unknown
}

export type BridgeInput = {
  /** How to start the child. Injected in tests; a real spawn by default. */
  spawn?: () => ChildLike
  command?: string
  args?: string[]
  timeoutMs?: number
  /** A line the child sent us that was not an answer to anything. */
  onNotification?: (message: { method: string; params: any }) => void
  /** Classifications only — never message text, never a token. */
  onDiagnostic?: (what: string) => void
}

export class Bridge implements BridgeCalls {
  private child: ChildLike | null = null
  private buffer = ''
  private dropping = false
  private nextId = 1
  private readonly waiting = new Map<string, {
    resolve: (value: any) => void
    reject: (error: Error) => void
    timer: ReturnType<typeof setTimeout>
  }>()

  private readonly input: BridgeInput

  constructor(input: BridgeInput = {}) {
    this.input = input
  }

  start(): void {
    if (this.child) return
    const child = this.input.spawn
      ? this.input.spawn()
      : (nodeSpawn(this.input.command ?? 'collab',
          this.input.args ?? ['queue', 'bridge'],
          { stdio: ['pipe', 'pipe', 'pipe'], shell: false }) as unknown as ChildLike)
    this.child = child
    child.stdout?.setEncoding?.('utf8')
    child.stdout?.on('data', (chunk: string) => this.received(String(chunk)))
    child.stderr?.setEncoding?.('utf8')
    child.stderr?.on('data', (chunk: string) => {
      // The child writes classifications here and nothing else; even so it is
      // reported as one word, so a future slip cannot log a message body.
      this.input.onDiagnostic?.(String(chunk).trim().split('\n')[0] ?? 'stderr')
    })
    child.on('exit', (code: number | null) => {
      this.child = null
      this.failAll(new BridgeError('unavailable', `the queue bridge exited (${code})`))
    })
    child.on('error', (error: Error) => {
      this.failAll(new BridgeError('unavailable', error.message))
    })
  }

  close(): void {
    const child = this.child
    this.child = null
    try {
      child?.stdin?.end()
    } catch {
      /* the child is already gone */
    }
    child?.kill()
    this.failAll(new BridgeError('unavailable', 'the queue bridge was closed'))
  }

  private failAll(error: Error): void {
    for (const [id, pending] of [...this.waiting]) {
      clearTimeout(pending.timer)
      this.waiting.delete(id)
      pending.reject(error)
    }
  }

  private received(chunk: string): void {
    this.buffer += chunk
    for (;;) {
      const at = this.buffer.indexOf('\n')
      if (at < 0) {
        if (Buffer.byteLength(this.buffer, 'utf8') > MAX_LINE_BYTES) {
          // Past the cap with no newline in sight: stop holding it, and skip
          // whatever is left of that line rather than parsing its tail.
          this.buffer = ''
          this.dropping = true
          this.input.onDiagnostic?.('oversized-line')
        }
        return
      }
      const line = this.buffer.slice(0, at)
      this.buffer = this.buffer.slice(at + 1)
      if (this.dropping) {
        this.dropping = false
        continue
      }
      if (Buffer.byteLength(line, 'utf8') > MAX_LINE_BYTES) {
        this.input.onDiagnostic?.('oversized-line')
        continue
      }
      this.handle(line)
    }
  }

  private handle(line: string): void {
    if (!line.trim()) return
    let message: any
    try {
      message = JSON.parse(line)
    } catch {
      this.input.onDiagnostic?.('unparseable-line')
      return
    }
    const id = message?.id
    if (id === undefined || id === null) {
      if (message?.method) {
        this.input.onNotification?.({ method: String(message.method),
                                      params: message.params })
      } else {
        this.input.onDiagnostic?.('unaddressed-line')
      }
      return
    }
    const pending = this.waiting.get(String(id))
    if (!pending) {
      this.input.onDiagnostic?.('unmatched-id')
      return
    }
    clearTimeout(pending.timer)
    this.waiting.delete(String(id))
    if (message.error) {
      pending.reject(new BridgeError(String(message.error.code ?? 'unavailable'),
                                     String(message.error.detail ?? 'no detail')))
      return
    }
    pending.resolve(message.result)
  }

  call<T = any>(method: string, params: Record<string, unknown> = {}): Promise<T> {
    if (!this.child) this.start()
    const id = String(this.nextId++)
    const timeoutMs = this.input.timeoutMs ?? CALL_TIMEOUT_MS
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.waiting.delete(id)
        reject(new BridgeError('unavailable', `${method} was not answered in time`))
      }, timeoutMs)
      // Unref so a pending call cannot keep the host process alive.
      ;(timer as any).unref?.()
      this.waiting.set(id, { resolve, reject, timer })
      try {
        this.child?.stdin?.write(JSON.stringify({ id, method, params }) + '\n')
      } catch (error: any) {
        clearTimeout(timer)
        this.waiting.delete(id)
        reject(new BridgeError('unavailable', error?.message ?? 'the pipe is closed'))
      }
    })
  }

  // The methods the scheduler uses, named so a reader can see the whole
  // surface without following `call` around.
  begin_attempt(params: any) { return this.call<any>('begin_attempt', params) }
  finish_attempt(params: any) { return this.call<any>('finish_attempt', params) }
  receipt(params: any) { return this.call<any>('receipt', params) }
  turn_taken(params: any) { return this.call<{ turns: number }>('turn_taken', params) }
  unfinished_attempts(params: any) { return this.call<any[]>('unfinished_attempts', params) }
  pause(params: any) { return this.call<any>('pause', params) }
  resume(params: any) { return this.call<any>('resume', params) }
  poll(params: any) { return this.call<PendingRecord[]>('poll', params) }
  send(params: any) { return this.call<any>('send', params) }
  status(params: any) { return this.call<any>('status', params) }
  release(params: any) { return this.call<any>('release', params) }
  bind(params: any) { return this.call<any>('bind', params) }
  bind_local(params: any) { return this.call<any>('bind_local', params) }
}
