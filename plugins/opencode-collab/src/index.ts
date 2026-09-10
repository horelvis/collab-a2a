/**
 * The plugin OpenCode loads: hooks, tools, and the wiring between the session
 * it is running in and the queue on the other side of a pipe.
 *
 * **A binding is explicit.** The session this plugin delivers into is the one a
 * tool call came from — `context.sessionID` — or one restored from a binding
 * whose session AND directory still match. Nothing here ever picks «the latest
 * session», and nothing creates one.
 *
 * **An acknowledgement is proof, so it is checked like one.** `collab_ack`
 * refuses ids that were not delivered to the session calling it. An agent
 * saying it has read something is the only evidence the sender gets, and one
 * session must not be able to answer for another's messages.
 */

import { tool } from '@opencode-ai/plugin'
import type { Plugin } from '@opencode-ai/plugin'

import { createHost } from './adapter.ts'
import type { SessionHost } from './adapter.ts'
import { Bridge } from './bridge.ts'
import type { BridgeCalls } from './scheduler.ts'
import { Scheduler } from './scheduler.ts'
import type { Binding, PendingRecord } from './scheduler.ts'

export type CollabInput = {
  host: SessionHost
  bridge: BridgeCalls & { start?: () => void; close?: () => void }
  directory: string
  diagnostics?: (what: string) => void
}

/** Everything the plugin does, with its two edges injected so it can be tested. */
export function createCollab(input: CollabInput) {
  let scheduler: Scheduler | null = null

  function requireScheduler(sessionID: string): Scheduler {
    if (!scheduler) {
      throw new Error('no mailbox is bound to this session — call collab_bind first')
    }
    if (scheduler.binding.session !== sessionID) {
      // The tool ran in a session other than the bound one. Answering for a
      // different session's messages is exactly what must not be possible.
      throw new Error(
        `this session is not the one bound to ${scheduler.binding.mailbox}`)
    }
    return scheduler
  }

  async function bind(args: { mailbox: string; mode?: 'automatic' | 'notification' },
                      sessionID: string, directory: string): Promise<Binding> {
    const binding: Binding = {
      mailbox: args.mailbox,
      runtime: 'opencode',
      session: sessionID,
      directory,
      mode: args.mode ?? 'notification',
    }
    // The session first: a binding to a session that is not there, or is in
    // another directory, is refused before the server is told anything.
    await input.host.requireSession(sessionID, directory)
    await input.bridge.bind(binding as any)
    scheduler = new Scheduler({ host: input.host, bridge: input.bridge, binding })
    // Anything a previous run prepared and never concluded is settled before
    // this one delivers, so a restart cannot become a resend.
    await scheduler.reconcile()
    return binding
  }

  async function onNotification(message: { method: string; params: any }) {
    if (message.method !== 'pending' || !scheduler) return
    const records: PendingRecord[] = message.params?.records ?? []
    if (!records.length) return
    await scheduler.onPending(records)
  }

  async function onEvent(event: any) {
    if (!scheduler) return
    if (event?.type === 'session.status') {
      const { sessionID, status } = event.properties ?? {}
      if (sessionID !== scheduler.binding.session) return
      await scheduler.onStatus(status?.type === 'busy' || status?.type === 'retry'
        ? 'busy' : 'idle')
      return
    }
    if (event?.type === 'session.idle') {
      if (event.properties?.sessionID !== scheduler.binding.session) return
      await scheduler.onStatus('idle')
    }
  }

  const tools = {
    collab_bind: tool({
      description: 'Bind this session to a collab mailbox so queued messages ' +
        'from other agents are delivered into it.',
      args: {
        mailbox: tool.schema.string().describe('the mailbox id to consume'),
        mode: tool.schema.enum(['automatic', 'notification']).optional()
          .describe('automatic starts a turn with pending work; notification ' +
                    'only tells you it is there'),
      },
      async execute(args, context) {
        const binding = await bind(args as any, context.sessionID, context.directory)
        return `bound ${binding.mailbox} to this session in ${binding.directory} ` +
          `(${binding.mode})`
      },
    }),

    collab_ack: tool({
      description: 'Acknowledge collab messages by id. Only ids delivered to ' +
        'this session may be acknowledged, and acknowledging is not the same ' +
        'as finishing the work they asked for.',
      args: {
        ids: tool.schema.array(tool.schema.string())
          .describe('the message ids this session was given'),
      },
      async execute(args, context) {
        const active = requireScheduler(context.sessionID)
        const done = await active.acknowledge(args.ids)
        const refused = args.ids.filter((id) => !done.includes(id))
        const said = done.length ? `acknowledged ${done.join(', ')}` : 'acknowledged nothing'
        return refused.length
          ? `${said}; not delivered to this session: ${refused.join(', ')}`
          : said
      },
    }),

    collab_send: tool({
      description: 'Send a durable message to another agent through collab. It ' +
        'is written down here first and survives the server being unreachable.',
      args: {
        text: tool.schema.string().describe('what to say'),
        to: tool.schema.string().optional().describe('the recipient mailbox id'),
        room: tool.schema.string().optional().describe('or everyone in a room'),
        kind: tool.schema.enum(['request', 'response', 'informational']).optional(),
        reply_to: tool.schema.string().optional()
          .describe('the message id this answers'),
      },
      async execute(args) {
        const answer: any = await input.bridge.send(args as any)
        return `${answer.state}: ${answer.id}`
      },
    }),

    collab_status: tool({
      description: 'What collab is holding: the outbox, the binding, pending ' +
        'messages and why delivery is paused if it is.',
      args: {},
      async execute() {
        const state: any = await input.bridge.status({})
        return JSON.stringify({ ...state, delivery: scheduler?.state ?? null }, null, 2)
      },
    }),

    collab_process: tool({
      description: 'Take one batch of pending collab messages into this ' +
        'session now, rather than waiting for automatic delivery.',
      args: {},
      async execute(_args, context) {
        const active = requireScheduler(context.sessionID)
        const outcome = await active.processOnce()
        return outcome.delivered
          ? `delivered ${outcome.ids?.join(', ')}`
          : `nothing delivered: ${outcome.reason}`
      },
    }),

    collab_pause: tool({
      description: 'Stop delivering collab messages into this session. Nothing ' +
        'is deleted; the pending stay pending.',
      args: { reason: tool.schema.string().optional() },
      async execute(args, context) {
        const active = requireScheduler(context.sessionID)
        await active.pause(args.reason ?? 'paused from this session')
        return 'delivery paused'
      },
    }),

    collab_resume: tool({
      description: 'Deliver collab messages into this session again, and start ' +
        'the consecutive-turn count over.',
      args: {},
      async execute(_args, context) {
        const active = requireScheduler(context.sessionID)
        await active.resume()
        return 'delivery resumed'
      },
    }),
  }

  return {
    tools,
    onEvent,
    onNotification,
    bind,
    get scheduler() { return scheduler },
  }
}

/** Where the collab CLI lives, if the operator put it somewhere of their own. */
export function collabCommand(env: NodeJS.ProcessEnv = process.env): string {
  return env.COLLAB_BIN && env.COLLAB_BIN.trim() ? env.COLLAB_BIN : 'collab'
}

export const CollabQueue: Plugin = async ({ client, directory }) => {
  const host = createHost(client)
  const bridge = new Bridge({
    command: collabCommand(),
    args: ['queue', 'bridge'],
    onDiagnostic: (what) => console.error(`[collab-queue] ${what}`),
    onNotification: (message) => { void collab.onNotification(message) },
  })
  const collab = createCollab({ host, bridge, directory })
  bridge.start()

  return {
    event: async ({ event }) => { await collab.onEvent(event) },
    tool: collab.tools,
    dispose: async () => { bridge.close() },
  }
}

export default CollabQueue
