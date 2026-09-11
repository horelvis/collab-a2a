/**
 * A scheduler with no host, no child process and no clock but the one the test
 * moves. Everything the scheduler talks to is recorded, so a test can assert on
 * ORDER — which is where most of the safety lives: the attempt is written down
 * before the host is called, and the lease and the session are rechecked after
 * the batch is chosen and before it is submitted.
 */
import type { Binding, PendingRecord } from '../src/scheduler.ts'
import { Scheduler } from '../src/scheduler.ts'
import type { SessionHost, SessionState, MarkerLookup } from '../src/adapter.ts'
import { DeliveryError } from '../src/adapter.ts'

export type HarnessOptions = {
  loopLimit?: number
  mode?: 'automatic' | 'notification'
  status?: SessionState
  directory?: string
  session?: string
  markerFound?: boolean | 'throw'
  deliverFails?: DeliveryError | null
  turns?: number
  unfinished?: any[]
}

export function makeHarness(options: HarnessOptions = {}) {
  const state = {
    status: options.status ?? 'unknown',
    directory: options.directory ?? '/project',
    markerFound: options.markerFound ?? true,
    deliverFails: options.deliverFails ?? null,
    turns: options.turns ?? 0,
  }
  const deliveries: Array<{ session: string; marker: string; text: string }> = []
  const order: string[] = []
  const attempts: any[] = []
  const receipts: any[] = []
  const finished: any[] = []
  const paused: any[] = []
  const cancels: string[] = []
  const notices: string[] = []

  const binding: Binding = {
    mailbox: 'mb_mine',
    runtime: 'opencode',
    session: options.session ?? 'ses_1',
    directory: options.directory ?? '/project',
    mode: options.mode ?? 'automatic',
  }

  const host: SessionHost = {
    async requireSession(session: string, directory: string) {
      order.push('requireSession')
      if (directory !== state.directory) {
        throw new DeliveryError('wrong_directory', `${session} moved`)
      }
      return { id: session, directory }
    },
    async status() {
      order.push('status')
      return state.status
    },
    async messages() {
      return []
    },
    async deliver(session: string, marker: string, text: string) {
      order.push('deliver')
      if (state.deliverFails) throw state.deliverFails
      deliveries.push({ session, marker, text })
      return { message_id: null }
    },
    async findMarker(_session: string, marker: string): Promise<MarkerLookup> {
      order.push('findMarker')
      if (state.markerFound === 'throw') {
        throw new DeliveryError('unreachable', 'the host went away')
      }
      const seen = deliveries.some((d) => d.marker === marker)
      return state.markerFound && seen
        ? { found: true, messageId: 'msg_1' }
        : { found: false, messageId: null }
    },
    async cancel(session: string) {
      cancels.push(session)
      return true
    },
    async notify(text: string) {
      notices.push(text)
    },
  }

  const bridge = {
    async begin_attempt(params: any) {
      order.push('begin_attempt')
      const attempt = {
        id: `at_${attempts.length + 1}`,
        marker: `[collab-delivery:at_${attempts.length + 1}]`,
        message_ids: params.message_ids,
        session: params.session,
      }
      attempts.push(attempt)
      return attempt
    },
    async finish_attempt(params: any) {
      order.push('finish_attempt')
      finished.push(params)
      return params
    },
    async receipt(params: any) {
      order.push('receipt')
      receipts.push(params)
      return params
    },
    async turn_taken(params: any) {
      order.push('turn_taken')
      state.turns += 1
      return { mailbox: params.mailbox, turns: state.turns }
    },
    async unfinished_attempts() {
      return options.unfinished ?? []
    },
    async pause(params: any) {
      paused.push(params)
      return params
    },
    async resume(params: any) {
      paused.push({ ...params, resumed: true })
      // As `LocalStore.resume` does: a person saying carry on is what makes a
      // run of automatic turns a new run.
      state.turns = 0
      return params
    },
    async poll() {
      return []
    },
    async send(params: any) {
      return { id: 'm_sent', state: 'queued', ...params }
    },
    async status(): Promise<any> {
      return { outbox: {}, bindings: [] as any[] }
    },
    async release() {
      return { released: true }
    },
    async bind(params: any) {
      return { binding: params, lease: { generation: 1 } }
    },
  }

  // A clock and a timer queue the test drives by hand.
  let now = 1_000
  const timers: Array<{ at: number; fn: () => void; id: number }> = []
  let nextTimer = 1
  const clock = () => now
  const scheduler = new Scheduler({
    host,
    bridge,
    binding,
    clock,
    loopLimit: options.loopLimit,
    timers: {
      setTimeout(fn: () => void, ms: number) {
        const id = nextTimer++
        timers.push({ at: now + ms, fn, id })
        return id as unknown as ReturnType<typeof setTimeout>
      },
      clearTimeout(id: any) {
        const at = timers.findIndex((t) => t.id === id)
        if (at >= 0) timers.splice(at, 1)
      },
    },
  })

  async function advance(ms: number) {
    now += ms
    const due = timers.filter((t) => t.at <= now).sort((a, b) => a.at - b.at)
    for (const timer of due) {
      const at = timers.indexOf(timer)
      if (at >= 0) timers.splice(at, 1)
      timer.fn()
    }
    // Let whatever those timers started settle before the test asserts.
    await new Promise((resume) => setImmediate(resume))
    await new Promise((resume) => setImmediate(resume))
    await new Promise((resume) => setImmediate(resume))
  }

  function request(id: string, over: Partial<PendingRecord> = {}): PendingRecord {
    return {
      id,
      sender: 'mb_them',
      kind: 'request',
      text: `please ${id}`,
      seq: Number(id.replace(/\D/g, '')) || 1,
      state: 'pending',
      conversation: 'c_1',
      ...over,
    }
  }

  return {
    scheduler, host, bridge, binding, deliveries, order, attempts, receipts,
    finished, paused, cancels, notices, state, advance, request,
    now: () => now,
  }
}
