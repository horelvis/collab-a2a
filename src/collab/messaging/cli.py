"""`collab queue`: configuring the outbox, sending through it, and the bridge.

Four things a person does — say where the queue is, send something, look at
what is waiting, and clear a block — plus one thing a program does: `bridge`,
a line of JSON in and a line of JSON out, which is how the OpenCode plugin
drives all of the above without a shell ever seeing a message's text.

**The credential is never an argument.** `configure` records WHICH session
profile to use, and the token is read out of that profile, which already lives
at 0600 in the session's own directory. A token on a command line is in the
shell history, in `ps`, and in whatever the terminal scrolled past.

httpx is imported inside the functions that need it, so `import collab.cli`
stays free of it — see `tests/test_cli_imports_no_httpx.py`.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import uuid
from pathlib import Path
from typing import Any, BinaryIO, Callable, TextIO

from .local import LocalStore, local_store_path
from .model import KINDS, MODES, Binding, Message, QueueError, stamp

#: The longest line the bridge will read. A message is capped at eight thousand
#: characters by the protocol, so this is several of them plus their framing;
#: past it the line is refused and skipped rather than buffered, because a
#: writer that can make this process hold a megabyte can make it hold a hundred.
MAX_LINE_BYTES = 256 * 1024

#: Methods the bridge answers. Named here rather than discovered by attribute
#: lookup: what a caller may reach into this process should be a list somebody
#: can read, not whatever happens to be defined.
BRIDGE_METHODS = ("bind", "bind_local", "poll", "begin_attempt", "finish_attempt",
                  "receipt", "send", "status", "pause", "resume", "release",
                  # The loop counter and the crash boundary are the scheduler's
                  # two questions about state it does not own: how many turns
                  # in a row it has taken, and what it prepared and never
                  # concluded. Both are answers only the disk has.
                  "turn_taken", "unfinished_attempts")


def config_path() -> Path:
    return local_store_path().parent / "config.json"


def load_queue_config() -> dict[str, Any] | None:
    path = config_path()
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


def save_queue_config(config: dict[str, Any]) -> None:
    path = config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    path.write_text(json.dumps(config, indent=2, sort_keys=True) + "\n")
    os.chmod(path, 0o600)


def _profile(session_id: str, home: str | None = None):
    """The saved session this queue borrows its identity and token from.

    BY PATH WHEN WE HAVE ONE. A session profile lives under the `.collab` of
    the repository it was joined from, and `SessionProfile.load` finds it by
    walking up from the current directory — which is fine for a person typing
    in that repository and useless for the bridge, which the plugin starts in
    whatever project the OpenCode session is in. So `configure` writes down
    where the profile is, and this reads it from there.
    """
    from ..config import SessionProfile

    if home:
        found = SessionProfile.load_from(Path(home) / "sessions" / session_id)
        if found is not None:
            return found
    return SessionProfile.load(session_id)


def _fail(message: str, err: TextIO | None = None) -> int:
    print(f"collab queue: {message}", file=err or sys.stderr)
    return 1


def _remember_mailbox(config: dict[str, Any]) -> Callable[[str], None]:
    """Write the id the server assigned us back into the config, once."""
    def keep(mailbox: str) -> None:
        current = load_queue_config() or dict(config)
        if current.get("mailbox") == mailbox:
            return
        current["mailbox"] = mailbox
        save_queue_config(current)
    return keep


def _open(config: dict[str, Any]):
    """The client and its outbox, or a QueueError saying what is missing."""
    from .client import QueueClient

    profile = _profile(config["profile"], config.get("home"))
    if profile is None:
        raise QueueError("not_found",
                         f"the session profile {config['profile']} is gone —"
                         " run `collab queue configure` again")
    local = LocalStore(local_store_path())
    client = QueueClient(config["server"], profile.token, local,
                         identity=config.get("identity"),
                         mailbox=config.get("mailbox"),
                         on_mailbox=_remember_mailbox(config))
    return client, local


# --- the commands -------------------------------------------------------------

def cmd_queue(args: argparse.Namespace) -> int:
    actions: dict[str, Callable[[argparse.Namespace], int]] = {
        "configure": _configure, "send": _send, "status": _status,
        "bridge": _bridge, "bind": _bind, "pause": _pause, "resume": _resume,
        "retry": _retry,
    }
    return actions[args.action](args)


def _configure(args: argparse.Namespace) -> int:
    """Say where the queue is and whose identity this agent speaks with."""
    if not args.server:
        return _fail("--server is where the queue lives, e.g. "
                     "`collab queue configure --server http://host:9920 "
                     "--profile s_1 --identity mac/ios --mode automatic`")
    if not args.profile:
        return _fail("--profile names the saved session whose token this uses")
    if not args.identity:
        return _fail("--identity is this agent's mailbox name, e.g. mac/ios")
    if not args.mode:
        return _fail("--mode is `automatic` or `notification`, and is chosen "
                     "explicitly: it decides whether a message can start a turn")
    profile = _profile(args.profile)
    if profile is None:
        return _fail(f"no saved session {args.profile} — `collab sessions` lists "
                     "the ones this repo has")
    config = {"server": args.server.rstrip("/"), "profile": args.profile,
              "home": profile.home, "identity": args.identity, "mode": args.mode}
    save_queue_config(config)
    print(f"queue: {args.server} as {args.identity} ({args.mode} mode)")
    # THE MAILBOX IS THE SERVER'S TO MINT. Asking now means the id is on disk
    # before anything needs it; failing to ask is not fatal, because the outbox
    # holds messages under the name and puts the id in on the way out.
    try:
        client, local = _open(config)
    except QueueError as exc:
        print(f"  the server could not be reached yet: {exc.detail}")
        return 0
    try:
        print(f"  mailbox {client.identify()}")
    except QueueError as exc:
        print(f"  not registered with the server yet ({exc.code}) — it will be"
              " the first time it answers")
    finally:
        client.close()
        local.close()
    return 0


def _configured() -> dict[str, Any] | None:
    return load_queue_config()


def _send(args: argparse.Namespace) -> int:
    config = _configured()
    if config is None:
        return _fail("not configured yet — run `collab queue configure --server "
                     "URL --profile SESSION --identity NAME --mode MODE`")
    text = " ".join(args.text).strip() if args.text else ""
    if not text:
        return _fail("nothing to send")
    if not args.to and not args.room:
        return _fail("--to names a mailbox, or --room a conversation")
    if args.to and args.room:
        return _fail("--to or --room, not both")
    try:
        client, local = _open(config)
    except QueueError as exc:
        return _fail(exc.detail)
    try:
        message = Message(
            id=args.message_id or ("m_" + uuid.uuid4().hex[:12]),
            sender=config.get("mailbox") or config["identity"],
            recipients=(args.to,) if args.to else (),
            kind=args.kind, text=text, created_at=stamp(_now()),
            conversation=args.conversation or "c_default",
            reply_to=args.reply_to)
        outcome = client.send(message, room=args.room)
    except QueueError as exc:
        return _fail(exc.detail)
    finally:
        client.close()
        local.close()
    if outcome["state"] == "accepted":
        print(f"accepted by the server: {outcome['id']}")
        return 0
    if outcome["state"] == "blocked":
        return _fail(f"{outcome['id']} is queued but blocked: {outcome['error']}")
    print(f"queued: {outcome['id']} — it will be sent when the server answers")
    return 0


def _now() -> float:
    import time

    return time.time()


def _status(args: argparse.Namespace) -> int:
    config = _configured()
    local = LocalStore(local_store_path())
    try:
        state = local.status()
    finally:
        local.close()
    state["server"] = (config or {}).get("server")
    state["identity"] = (config or {}).get("identity")
    state["mode"] = (config or {}).get("mode")
    if args.json:
        print(json.dumps(state, indent=2, sort_keys=True))
        return 0
    counts = state["outbox"] or {}
    print(f"server:   {state['server'] or 'not configured'}")
    print(f"identity: {state['identity'] or '-'} ({state['mode'] or '-'})")
    print("outbox:   " + (", ".join(f"{n} {name}" for name, n in sorted(counts.items()))
                          or "empty"))
    if state["last_error"]:
        print(f"last error: {state['last_error']}")
    for binding in state["bindings"]:
        held = "paused" if binding["paused"] else "live"
        print(f"bound:    {binding['mailbox']} → {binding['session']} "
              f"in {binding['directory']} ({binding['mode']}, {held}, "
              f"{binding['turns']} turns)")
    return 0


def _bind(args: argparse.Namespace) -> int:
    config = _configured()
    if config is None:
        return _fail("not configured yet — run `collab queue configure` first")
    if not (args.mailbox and args.session and args.directory):
        return _fail("--mailbox, --session and --directory say which session "
                     "consumes which mailbox, and from where")
    binding = Binding(args.mailbox, args.runtime or "opencode", args.session,
                      args.directory, args.mode or config.get("mode") or "notification")
    local = LocalStore(local_store_path())
    try:
        local.bind(config["server"], binding)
    except QueueError as exc:
        return _fail(exc.detail)
    finally:
        local.close()
    print(f"bound {binding.mailbox} to {binding.session} in {binding.directory}")
    return 0


def _pause(args: argparse.Namespace) -> int:
    return _binding_switch(args, pause=True)


def _resume(args: argparse.Namespace) -> int:
    return _binding_switch(args, pause=False)


def _binding_switch(args: argparse.Namespace, pause: bool) -> int:
    config = _configured()
    if config is None:
        return _fail("not configured yet — run `collab queue configure` first")
    local = LocalStore(local_store_path())
    try:
        bindings = [b for b in local.bindings()
                    if args.mailbox in (None, b["mailbox"])]
        if not bindings:
            return _fail("no binding to pause — `collab queue bind` makes one"
                         if pause else "no binding to resume")
        for binding in bindings:
            if pause:
                local.pause(binding["server"], binding["mailbox"],
                            args.reason or "paused by hand")
            else:
                local.resume(binding["server"], binding["mailbox"])
            print(f"{'paused' if pause else 'resumed'} {binding['mailbox']}")
    except QueueError as exc:
        return _fail(exc.detail)
    finally:
        local.close()
    return 0


def _retry(args: argparse.Namespace) -> int:
    """Put blocked messages back in the queue, once the fault is dealt with."""
    local = LocalStore(local_store_path())
    try:
        blocked = [r for r in local.history() if r["state"] == "blocked"
                   and args.message_id in (None, r["id"])]
        if not blocked:
            return _fail("nothing is blocked")
        for record in blocked:
            local.unblock(record["id"])
            print(f"queued again: {record['id']}")
    except QueueError as exc:
        return _fail(exc.detail)
    finally:
        local.close()
    return 0


def _bridge(args: argparse.Namespace) -> int:
    return run_bridge(notifications=not args.no_notifications)


# --- pending work, without anybody being asked to look ------------------------

#: How long the loop holds one request open waiting for work, and how long it
#: waits after a failure before opening another. The hold is what keeps this
#: off a polling interval: the server answers when there is something, or when
#: the hold runs out.
HOLD_SECONDS = 25.0
RETRY_SECONDS = 2.0
RETRY_CAP_SECONDS = 60.0

#: The shortest gap between two polls of one mailbox. A pending record STAYS
#: pending until the session has been given it, so a held request that answers
#: at once answers at once again — without this gap the loop would spin at the
#: speed of the network for as long as anything is waiting.
MIN_GAP_SECONDS = 1.0

#: A reservation lasts sixty seconds; renewing every twenty leaves room for two
#: failures before somebody else may take the mailbox.
RENEW_SECONDS = 20.0


def _breathe(stop: Any, started: float) -> None:
    """Hold the floor under one poll per mailbox per second.

    The hold on the server does the waiting when the server is well. This is
    for when it is not: an endpoint answering instantly — because work is
    waiting, because a proxy short-circuits the hold, because the hold is not
    supported — would otherwise be asked again at the speed of the network.
    """
    spent = _now() - started
    if spent < MIN_GAP_SECONDS:
        stop.wait(MIN_GAP_SECONDS - spent)


def notify_loop(client: Any, local: LocalStore, write: Callable[[dict], None],
                stop: Any, holds: dict[str, dict[str, Any]] | None = None,
                hold_seconds: float = HOLD_SECONDS,
                diagnostic: Callable[[str], None] | None = None) -> None:
    """Push pending work to the plugin as it arrives, and renew what we hold.

    THE MODEL IS NEVER THE THING THAT POLLS. A held request costs one open
    socket; a model asked every few seconds whether anything has arrived costs
    a turn every few seconds, and is the arrangement this replaces.

    Every failure is a classification — never a message, never a token — and
    every failure widens the wait, so a server that is down is asked for once a
    minute rather than continuously.
    """
    from .model import Binding

    failures = 0
    last_renewed = 0.0
    announced: dict[str, tuple[str, ...]] = {}
    asleep: set[str] = set()
    while not stop.is_set():
        live = []
        for record in local.bindings():
            if record["paused"]:
                # A paused mailbox is not consumed — and what was announced
                # before the pause is forgotten, so that resuming announces it
                # again. Without that, a person who resumed watched nothing
                # happen: the news was the same news, and had already been told.
                announced.pop(record["mailbox"], None)
                asleep.add(record["mailbox"])
                continue
            if record["mailbox"] in asleep:
                asleep.discard(record["mailbox"])
                # Said out loud, because the plugin holds its own idea of being
                # paused and cannot see this file.
                write({"method": "resumed",
                       "params": {"mailbox": record["mailbox"]}})
            live.append(record)
        bindings = live
        if not bindings:
            stop.wait(RETRY_SECONDS)
            continue
        for record in bindings:
            if stop.is_set():
                return
            binding = Binding(record["mailbox"], record["runtime"],
                              record["session"], record["directory"],
                              record["mode"])
            started = _now()
            try:
                now = started
                if holds and record["mailbox"] in holds and \
                        now - last_renewed >= RENEW_SECONDS:
                    client.renew(record["mailbox"], holds[record["mailbox"]])
                    last_renewed = now
                records = client.poll(binding, wait=hold_seconds)
            except QueueError as exc:
                failures += 1
                if diagnostic is not None:
                    diagnostic(exc.code)
                if exc.code in ("forbidden", "stale_lease"):
                    # Not something waiting fixes: the plugin is told, and this
                    # mailbox is left alone until it binds again.
                    write({"method": "blocked",
                           "params": {"mailbox": record["mailbox"], "code": exc.code}})
                    stop.wait(RETRY_CAP_SECONDS)
                    continue
                stop.wait(min(RETRY_CAP_SECONDS, RETRY_SECONDS * (2 ** min(failures, 5))))
                continue
            failures = 0
            if not records:
                _breathe(stop, started)
                continue
            # The same set as last time is the same news: the plugin has it and
            # is working through it, and repeating it every second would be a
            # notification storm made of one message.
            seen = tuple(f"{r.get('sender')}:{r.get('id')}" for r in records)
            if announced.get(record["mailbox"]) != seen:
                announced[record["mailbox"]] = seen
                write({"method": "pending",
                       "params": {"mailbox": record["mailbox"], "records": records}})
            _breathe(stop, started)


# --- the bridge ---------------------------------------------------------------

class Bridge:
    """One request in, one answer out, and never a shell in between.

    The plugin hands this process JSON and reads JSON back. Message text is a
    value in a JSON object from beginning to end — it is never interpolated
    into a command line, which is why a message whose text is `$(rm -rf ~)`
    is a message whose text is `$(rm -rf ~)`.
    """

    def __init__(self, config: dict[str, Any] | None, local: LocalStore,
                 client: Any | None = None) -> None:
        self.config = config or {}
        self.local = local
        self.client = client
        self.holds: dict[str, dict[str, Any]] = {}

    # Every method takes the parsed params and returns a JSON-able answer.
    def call(self, method: str, params: dict[str, Any]) -> Any:
        if method not in BRIDGE_METHODS:
            raise QueueError("invalid", f"no method {method}")
        return getattr(self, f"do_{method}")(params)

    def _need_client(self):
        if self.client is None:
            raise QueueError("unavailable",
                             "this bridge has no connection configured")
        return self.client

    def _binding(self, params: dict[str, Any]) -> Binding:
        return Binding.from_json({
            "mailbox": params.get("mailbox"), "runtime": params.get("runtime"),
            "session": params.get("session"), "directory": params.get("directory"),
            "mode": params.get("mode")})

    def do_bind_local(self, params: dict[str, Any]) -> Any:
        """Write the binding down without asking the server for anything."""
        binding = self._binding(params)
        self.local.bind(self.config.get("server", ""), binding)
        return self.local.binding(self.config.get("server", ""), binding.mailbox)

    def do_bind(self, params: dict[str, Any]) -> Any:
        binding = self._binding(params)
        hold = self._need_client().acquire(binding)
        self.holds[binding.mailbox] = hold
        return {"binding": binding.to_json(), "lease": _without_token(hold)}

    def do_poll(self, params: dict[str, Any]) -> Any:
        binding = self._binding(params)
        return self._need_client().poll(binding, wait=float(params.get("wait") or 0))

    def do_begin_attempt(self, params: dict[str, Any]) -> Any:
        binding = self._binding(params)
        ids = params.get("message_ids")
        if not isinstance(ids, list):
            raise QueueError("invalid", "message_ids is a list")
        return self.local.begin_attempt(binding, ids)

    def do_finish_attempt(self, params: dict[str, Any]) -> Any:
        self.local.finish_attempt(params.get("attempt_id"), params.get("state"),
                                  params.get("opencode_message_id"))
        return self.local.attempt(params.get("attempt_id"))

    def do_receipt(self, params: dict[str, Any]) -> Any:
        mailbox = params.get("mailbox")
        hold = self.holds.get(mailbox)
        if hold is None:
            raise QueueError("stale_lease",
                             f"{mailbox} is not held by this bridge")
        return self._need_client().receipt(
            mailbox, params.get("message_id"), hold, params.get("state"),
            params.get("attempt_id"), sender=params.get("sender"))

    def do_send(self, params: dict[str, Any]) -> Any:
        text = params.get("text")
        to = params.get("to")
        room = params.get("room")
        if not to and not room:
            raise QueueError("invalid", "a message names a mailbox or a room")
        kind = params.get("kind") or "request"
        if kind not in KINDS:
            raise QueueError("invalid", f"kind is one of {', '.join(KINDS)}")
        message = Message(
            id=params.get("id") or ("m_" + uuid.uuid4().hex[:12]),
            sender=self.config.get("mailbox") or self.config.get("identity", ""),
            recipients=(to,) if to else (), kind=kind, text=text or "",
            created_at=stamp(_now()),
            conversation=params.get("conversation") or "c_default",
            reply_to=params.get("reply_to"))
        if self.client is None:
            self.local.enqueue(self.config.get("server", ""), message, room=room)
            record = self.local.record(message.id)
            return {"id": message.id, "state": record["state"]}
        return self.client.send(message, room=room)

    def do_turn_taken(self, params: dict[str, Any]) -> Any:
        """Count one automatic activation, before it is scheduled."""
        turns = self.local.turn_taken(self.config.get("server", ""),
                                      params.get("mailbox"))
        return {"mailbox": params.get("mailbox"), "turns": turns}

    def do_unfinished_attempts(self, params: dict[str, Any]) -> Any:
        return self.local.unfinished_attempts(params.get("mailbox"))

    def do_status(self, params: dict[str, Any]) -> Any:
        state = self.local.status()
        state["server"] = self.config.get("server")
        state["identity"] = self.config.get("identity")
        state["mode"] = self.config.get("mode")
        return state

    def do_pause(self, params: dict[str, Any]) -> Any:
        self.local.pause(self.config.get("server", ""), params.get("mailbox"),
                         params.get("reason") or "paused")
        return self.local.binding(self.config.get("server", ""),
                                  params.get("mailbox"))

    def do_resume(self, params: dict[str, Any]) -> Any:
        self.local.resume(self.config.get("server", ""), params.get("mailbox"))
        return self.local.binding(self.config.get("server", ""),
                                  params.get("mailbox"))

    def do_release(self, params: dict[str, Any]) -> Any:
        mailbox = params.get("mailbox")
        hold = self.holds.pop(mailbox, None)
        if hold is None:
            return {"mailbox": mailbox, "released": False}
        self._need_client().release(mailbox, hold)
        return {"mailbox": mailbox, "released": True}


def _without_token(hold: dict[str, Any]) -> dict[str, Any]:
    """A reservation as it may be logged: everything but the credential."""
    return {k: v for k, v in hold.items() if k != "token"}


def read_line(stream: BinaryIO, cap: int = MAX_LINE_BYTES) -> tuple[bytes, bool]:
    """One line, or as much of one as we will hold. Returns (line, oversized).

    An oversized line is DRAINED to its newline rather than left in the buffer:
    leaving it there turns one refused request into a stream of nonsense
    requests made of its remainder.
    """
    line = stream.readline(cap + 1)
    if len(line) <= cap or line.endswith(b"\n"):
        return line, False
    while True:
        more = stream.readline(cap + 1)
        if not more or more.endswith(b"\n"):
            break
    return line, True


def run_bridge(stdin: BinaryIO | None = None, stdout: BinaryIO | None = None,
               stderr: TextIO | None = None, notifications: bool = True) -> int:
    """Read requests until the other end goes away, answering each one.

    On EOF the outbox is left exactly as it is and any reservation this bridge
    holds is handed back if the server can still be reached: the plugin has
    gone, so consuming on its behalf would be claiming deliveries nobody is
    there to receive. Nothing is deleted, and nothing is marked delivered.
    """
    source = stdin if stdin is not None else sys.stdin.buffer
    sink = stdout if stdout is not None else sys.stdout.buffer
    noise = stderr if stderr is not None else sys.stderr

    config = load_queue_config()
    local = LocalStore(local_store_path())
    client = None
    if config is not None:
        try:
            client, local = _open(config)
        except QueueError as exc:
            # Diagnostics, never content: the reason is a classification, and
            # no message text or token goes near this stream.
            print(f"collab queue bridge: {exc.code}", file=noise)
    bridge = Bridge(config, local, client)

    # ONE WRITER AT A TIME. The answering loop and the notification thread both
    # write whole lines to the same pipe, and a line torn in half is a line the
    # plugin cannot parse.
    pen = threading.Lock()

    def write(payload: dict[str, Any]) -> None:
        with pen:
            sink.write(json.dumps(payload).encode("utf-8") + b"\n")
            sink.flush() if hasattr(sink, "flush") else None

    stop = threading.Event()
    watcher = None
    if notifications and client is not None:
        watcher = threading.Thread(
            target=notify_loop, daemon=True,
            args=(client, local, write, stop, bridge.holds),
            kwargs={"diagnostic": lambda code: print(
                f"collab queue bridge: {code}", file=noise)})
        watcher.start()

    try:
        while True:
            line, oversized = read_line(source)
            if not line:
                break
            if oversized:
                write({"id": None, "error": {
                    "code": "invalid",
                    "detail": f"a bridge line is at most {MAX_LINE_BYTES:,} bytes"}})
                continue
            try:
                request = json.loads(line)
                if not isinstance(request, dict):
                    raise ValueError("a request is a JSON object")
            except ValueError as exc:
                write({"id": None,
                       "error": {"code": "invalid", "detail": f"malformed JSON: {exc}"}})
                continue
            rid = request.get("id")
            params = request.get("params")
            try:
                if not isinstance(params, dict) and params is not None:
                    raise QueueError("invalid", "params is a JSON object")
                result = bridge.call(str(request.get("method")), params or {})
            except QueueError as exc:
                write({"id": rid, "error": {"code": exc.code, "detail": exc.detail}})
                continue
            except Exception as exc:  # a fault of ours, not of the caller's
                print(f"collab queue bridge: {type(exc).__name__}", file=noise)
                write({"id": rid, "error": {"code": "unavailable",
                                            "detail": type(exc).__name__}})
                continue
            write({"id": rid, "result": result})
    finally:
        stop.set()
        if watcher is not None:
            watcher.join(timeout=2.0)
        for mailbox in list(bridge.holds):
            try:
                bridge.do_release({"mailbox": mailbox})
            except QueueError:
                pass  # unreachable server: the reservation expires on its own
        if client is not None:
            client.close()
        local.close()
    return 0


def add_queue_parser(sub: Any) -> None:
    """Register `collab queue` on the main parser."""
    q = sub.add_parser("queue",
                       help="durable messages between agents, delivered into an "
                            "OpenCode session")
    q.add_argument("action",
                   choices=["configure", "send", "status", "bridge", "bind",
                            "pause", "resume", "retry"],
                   help="set it up, send, look at it, run the plugin bridge, "
                        "bind a session, pause or resume delivery, or retry a "
                        "blocked message")
    q.add_argument("text", nargs="*", metavar="TEXT",
                   help="with `send`: what to say")
    q.add_argument("--server", metavar="URL", help="where the queue server is")
    q.add_argument("--profile", metavar="SESSION",
                   help="the saved session whose token this queue uses")
    q.add_argument("--identity", metavar="NAME",
                   help="this agent's mailbox name, e.g. mac/ios")
    q.add_argument("--mode", choices=list(MODES),
                   help="automatic delivery into the session, or notification only")
    q.add_argument("--to", metavar="MAILBOX", help="with `send`: the recipient")
    q.add_argument("--room", metavar="ROOM",
                   help="with `send`: everyone in that conversation")
    q.add_argument("--kind", choices=list(KINDS), default="request",
                   help="with `send`: request, response or informational")
    q.add_argument("--conversation", metavar="ID",
                   help="with `send`: which conversation this belongs to")
    q.add_argument("--reply-to", metavar="ID", dest="reply_to",
                   help="with `send`: the message this answers")
    q.add_argument("--message-id", metavar="ID", dest="message_id",
                   help="with `send` and `retry`: a specific message id")
    q.add_argument("--mailbox", metavar="ID",
                   help="with `bind`, `pause` and `resume`: which mailbox")
    q.add_argument("--session", metavar="ID",
                   help="with `bind`: the OpenCode session that consumes it")
    q.add_argument("--directory", metavar="PATH",
                   help="with `bind`: the project directory of that session")
    q.add_argument("--runtime", metavar="NAME",
                   help="with `bind`: which coding tool, default opencode")
    q.add_argument("--reason", metavar="TEXT", help="with `pause`: why")
    q.add_argument("--json", action="store_true",
                   help="with `status`: the whole state as JSON")
    q.add_argument("--no-notifications", action="store_true",
                   help="with `bridge`: answer requests only, and never push")
    q.set_defaults(func=cmd_queue)
