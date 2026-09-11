/**
 * Everything this plugin knows about THIS version of OpenCode's SDK.
 *
 * Verified against OpenCode 1.18.30 with `@opencode-ai/sdk` 1.18.29, by
 * reading the generated types the host ships:
 *
 *   session.get({path:{id}})       -> { id, directory, ... }  (Session)
 *   session.status()               -> { [sessionID]: {type: 'idle'|'busy'|'retry'} }
 *   session.messages({path:{id}})  -> [{ info, parts }]       (input parts included)
 *   session.promptAsync({path:{id}, body:{parts}}) -> returns without waiting
 *   session.abort({path:{id}})     -> stops the running turn
 *   tui.showToast({body:{message, variant}})
 *
 * Three rules hold this file together.
 *
 * **We never create, fork or choose a session.** The session is bound, by id,
 * by a person or by a restored binding — `session.create` and `session.list`
 * are not called here at all, so a bug cannot turn «I could not reach the
 * bound session» into «I delivered it to a different one».
 *
 * **The marker goes into the message, not into our notes.** What proves a
 * delivery landed is finding `[collab-delivery:<attempt>]` in the session's own
 * history. The id the SDK returns for the assistant's reply proves the call
 * returned, which is a different fact and the one a crash destroys.
 *
 * **A failed call is not proof of absence.** Every read that cannot complete
 * raises `unreachable`, and the scheduler above treats that as «unknown», never
 * as «not there» — a blind resend is the failure mode this design exists to
 * avoid.
 */

export type SessionState = 'idle' | 'busy' | 'unknown'

/** How often a settling marker lookup asks again. */
export const MARKER_POLL_MS = 150

/**
 * How long «is it in the history?» waits before answering «no».
 *
 * The host accepts a prompt with 204 AND NO BODY — there is no message id in
 * the reply to hold on to, which is the design's premise rather than a
 * disappointment: the marker in the persisted input is the only evidence that
 * survives this process dying. The wait covers the gap between the two.
 */
export const MARKER_SETTLE_MS = 3000

export type MarkerLookup = { found: boolean; messageId: string | null }

export type DeliveryCode =
  | 'no_session'
  | 'wrong_directory'
  | 'unreachable'
  | 'refused'

export class DeliveryError extends Error {
  readonly code: DeliveryCode
  readonly status: number | null

  constructor(code: DeliveryCode, message: string, status: number | null = null) {
    super(message)
    this.name = 'DeliveryError'
    this.code = code
    this.status = status
  }
}

export type SessionHost = {
  /** The bound session, checked to be the one we mean. */
  requireSession(session: string, directory: string): Promise<{ id: string; directory: string }>
  /** Whether a turn is running in it right now. */
  status(session: string): Promise<SessionState>
  /** Its messages, oldest first, with their parts. */
  messages(session: string): Promise<Array<{ id: string; text: string }>>
  /** Put a batch into the session. Returns without waiting for the model. */
  deliver(session: string, marker: string, text: string,
          options?: { noReply?: boolean }): Promise<{ message_id: string | null }>
  /** Is this marker in the session's own history? */
  findMarker(session: string, marker: string,
             options?: { settleMs?: number }): Promise<MarkerLookup>
  /** Stop the running turn. */
  cancel(session: string): Promise<boolean>
  /** Tell the person, without starting a turn. */
  notify(text: string): Promise<void>
}

/** The `{data, error}` envelope every generated SDK call answers with. */
type Answer<T> = { data?: T; error?: unknown; response?: { status?: number } }

function detail(answer: Answer<unknown>): string {
  const error: any = answer.error
  if (!error) return `HTTP ${answer.response?.status ?? '?'}`
  if (typeof error === 'string') return error
  return String(error?.data?.message ?? error?.message ?? JSON.stringify(error))
}

/** Run one SDK call, turning both failure shapes into one refusal. */
async function call<T>(what: string, run: () => Promise<Answer<T>>): Promise<T> {
  let answer: Answer<T>
  try {
    answer = await run()
  } catch (cause: any) {
    // A thrown error is the transport: a socket that closed, a host that went
    // away mid-request. It says nothing about what the session holds.
    throw new DeliveryError('unreachable', `${what}: ${cause?.message ?? cause}`)
  }
  if (answer && answer.error !== undefined && answer.error !== null) {
    const status = answer.response?.status ?? null
    if (status === 404) {
      throw new DeliveryError('no_session', `${what}: ${detail(answer)}`, status)
    }
    throw new DeliveryError('refused', `${what}: ${detail(answer)}`, status)
  }
  return answer?.data as T
}

/** The same path, as the host would write it: no trailing slash, no `.` steps. */
export function canonicalDirectory(path: string): string {
  const trimmed = String(path ?? '').replace(/\/+$/, '')
  return trimmed === '' ? '/' : trimmed
}

export function createHost(client: any): SessionHost {
  return {
    async requireSession(session: string, directory: string) {
      const info = await call<{ id?: string; directory?: string }>(
        'session.get', () => client.session.get({ path: { id: session } }))
      if (!info || !info.id) {
        throw new DeliveryError('no_session', `no session ${session}`)
      }
      const here = canonicalDirectory(info.directory ?? '')
      const wanted = canonicalDirectory(directory)
      if (here !== wanted) {
        // The session id was reused by a different checkout, or the binding is
        // stale. Either way this is not the session the messages were meant
        // for, and delivering into it would put one project's work in another.
        throw new DeliveryError(
          'wrong_directory',
          `session ${session} is in ${here}, and the binding says ${wanted}`)
      }
      return { id: info.id as string, directory: here }
    },

    async status(session: string) {
      const all = await call<Record<string, { type?: string }>>(
        'session.status', () => client.session.status({}))
      const state = (all ?? {})[session]
      // AN IDLE SESSION IS SIMPLY NOT IN THE MAP. Measured on 1.18.30: only
      // the session running a turn was listed, both for a session that had
      // never run one and for one whose turn had just been cancelled. So
      // `unknown` means «no turn this host knows about», and the scheduler
      // gates on `busy` — «deliver only when idle» would deliver nothing.
      if (!state) return 'unknown'
      // `retry` is the model waiting to be asked again — the session is not
      // free, and putting a batch in front of it would queue behind that.
      if (state.type === 'idle') return 'idle'
      return 'busy'
    },

    async messages(session: string) {
      const rows = await call<Array<any>>('session.messages', () =>
        client.session.messages({ path: { id: session } }))
      return (rows ?? []).map((row: any) => ({
        id: row?.info?.id ?? '',
        text: (row?.parts ?? [])
          .filter((part: any) => part?.type === 'text')
          .map((part: any) => String(part.text ?? ''))
          .join('\n'),
      }))
    },

    async deliver(session: string, marker: string, text: string,
                  options: { noReply?: boolean } = {}) {
      // ONE PART, marker first. The marker has to be inside the persisted
      // input text — a metadata field we cannot read back is not evidence —
      // and `promptAsync` returns as soon as the message is taken, so the
      // event handler that called us is not held for the length of a turn.
      const answer = await call<any>('session.promptAsync', () =>
        client.session.promptAsync({
          path: { id: session },
          body: {
            parts: [{ type: 'text', text: `${marker}\n${text}` }],
            // `noReply` is for the compatibility check only: it proves the
            // message is incorporated into the session's history without
            // asking a model to answer it. A real delivery leaves it off —
            // activating the turn is the point.
            ...(options.noReply ? { noReply: true } : {}),
          },
        }))
      const id = (answer as any)?.info?.id ?? (answer as any)?.id ?? null
      return { message_id: id }
    },

    async findMarker(session: string, marker: string,
                     options: { settleMs?: number } = {}) {
      // THE HOST WRITES THE INPUT DOWN A MOMENT AFTER IT ACCEPTS IT. Measured
      // on OpenCode 1.18.30: `promptAsync` answers 204 and the user message
      // appears in `session.messages` shortly afterwards — a lookup made
      // immediately came back empty for a message that was, in fact, there.
      // «Absent» is a conclusion the scheduler acts on, so it is only reached
      // after waiting this out; the default of zero is for callers that
      // already know the write has settled.
      const deadline = Date.now() + Math.max(0, options.settleMs ?? 0)
      for (;;) {
        const rows = await this.messages(session)
        for (const row of rows) {
          if (row.text.includes(marker)) {
            return { found: true, messageId: row.id || null }
          }
        }
        if (Date.now() >= deadline) return { found: false, messageId: null }
        await new Promise((resume) => setTimeout(resume, MARKER_POLL_MS))
      }
    },

    async cancel(session: string) {
      const done = await call<boolean>('session.abort', () =>
        client.session.abort({ path: { id: session } }))
      return done !== false
    },

    async notify(text: string) {
      await call<boolean>('tui.showToast', () =>
        client.tui.showToast({ body: { message: text, variant: 'info' } }))
    },
  }
}
