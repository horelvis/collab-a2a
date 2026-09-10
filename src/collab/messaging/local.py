"""The sender's own durable record: the outbox, the bindings and the journal.

Three things live here, and they are separate on purpose.

**The outbox** is what «queued» means. A message is written down before the
first request leaves, so a client that is told «queued» has said something that
is true after it dies. Server acceptance is recorded against the same record,
never a new one — the id the sender minted is the id every retry carries, which
is what lets the server recognise a repeat instead of filing a second message.

**The bindings** are which OpenCode session a mailbox is being consumed by, in
which directory and in which mode, plus the consecutive-turn counter. That
counter is on the disk because a loop that resets itself every time the plugin
restarts is not a limit.

**The journal** is one row per delivery attempt, written BEFORE the host is
called. A process that dies mid-delivery leaves a `prepared` row, and the
difference between «prepared and never sent» and «prepared and maybe sent» is
the whole of the reconciliation in Task 6/7 — so the row is never overwritten
in place once it reaches a terminal state.

None of this lives in a project's `.collab`: an outbox belongs to the agent,
not to the checkout it happened to be standing in, and a repository that is
deleted must not take somebody's undelivered messages with it.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterator

from .model import Binding, Message, QueueError, bound, fingerprint, identifier, validated

#: Outbox states. `queued` is ours to keep trying; `accepted` is the server's
#: to answer for; `blocked` is a fault a person has to clear — a rejected
#: credential or a payload the server refuses — and it never retries itself,
#: because a retry loop against a 401 is how a token gets locked out.
OUTBOX_STATES = ("queued", "accepted", "blocked")

#: Attempt states. `prepared` is written before the host is called; the other
#: three are terminal, and which one it reached is the evidence a restart reads.
ATTEMPT_STATES = ("prepared", "delivered", "uncertain", "cancelled")

SCHEMA = """
CREATE TABLE IF NOT EXISTS queue_outbox (
    id             TEXT PRIMARY KEY,
    server         TEXT NOT NULL,
    payload        TEXT NOT NULL,
    room           TEXT,
    fingerprint    TEXT NOT NULL,
    state          TEXT NOT NULL DEFAULT 'queued',
    attempts       INTEGER NOT NULL DEFAULT 0,
    created_at     REAL NOT NULL,
    next_at        REAL NOT NULL,
    last_error     TEXT,
    last_error_at  REAL,
    accepted_at    REAL,
    receipt        TEXT
);
CREATE INDEX IF NOT EXISTS idx_queue_outbox_due
    ON queue_outbox(state, next_at);

CREATE TABLE IF NOT EXISTS queue_bindings (
    server        TEXT NOT NULL,
    mailbox       TEXT NOT NULL,
    runtime       TEXT NOT NULL,
    session       TEXT NOT NULL,
    directory     TEXT NOT NULL,
    mode          TEXT NOT NULL,
    turns         INTEGER NOT NULL DEFAULT 0,
    paused        INTEGER NOT NULL DEFAULT 0,
    paused_reason TEXT,
    bound_at      REAL NOT NULL,
    updated_at    REAL NOT NULL,
    PRIMARY KEY (server, mailbox)
);

CREATE TABLE IF NOT EXISTS queue_attempts (
    id                  TEXT PRIMARY KEY,
    server              TEXT NOT NULL,
    mailbox             TEXT NOT NULL,
    runtime             TEXT NOT NULL,
    session             TEXT NOT NULL,
    directory           TEXT NOT NULL,
    marker              TEXT NOT NULL,
    message_ids         TEXT NOT NULL,
    state               TEXT NOT NULL DEFAULT 'prepared',
    started_at          REAL NOT NULL,
    finished_at         REAL,
    opencode_message_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_queue_attempts_open
    ON queue_attempts(mailbox, state, started_at);

-- AN ACKNOWLEDGEMENT ASKED FOR BY THE AGENT ITSELF. `collab queue ack` runs in
-- the agent's own shell and holds no reservation — taking one for a command
-- that lives half a second would take the mailbox off whoever is consuming it.
-- So the ask is written here and performed by that consumer.
CREATE TABLE IF NOT EXISTS queue_acks (
    mailbox      TEXT NOT NULL,
    sender       TEXT NOT NULL,
    message_id   TEXT NOT NULL,
    requested_at REAL NOT NULL,
    done_at      REAL,
    PRIMARY KEY (mailbox, sender, message_id)
);
"""


def local_store_path() -> Path:
    """Beside the global config, like every other thing a profile owns.

    `COLLAB_CONFIG` moves it, which is how a second profile — and every test —
    gets an outbox of its own rather than the machine's.
    """
    from ..config import global_config_path

    return global_config_path().parent / "queue" / "local.db"


def new_attempt_id() -> str:
    return "at_" + uuid.uuid4().hex[:12]


def marker_for(attempt_id: str) -> str:
    """What a delivery carries so the session's history can be asked about it.

    The correlation has to be IN the message that reaches OpenCode: an id we
    only hold on this side answers nothing after a crash, and the reply id the
    SDK returns is evidence the call returned, not evidence the message landed.
    """
    return f"[collab-delivery:{attempt_id}]"


class LocalStore:
    """One agent's outbox, bindings and attempt journal."""

    def __init__(self, path: Path | str,
                 clock: Callable[[], float] = time.time) -> None:
        self.path = str(path)
        self._clock = clock
        if self.path != ":memory:":
            folder = Path(self.path).parent
            folder.mkdir(parents=True, exist_ok=True)
            # An outbox holds what agents said to each other before it was
            # delivered. It is nobody else's on this machine.
            os.chmod(folder, 0o700)
        self._lock = threading.Lock()
        self._db = sqlite3.connect(self.path, check_same_thread=False,
                                   isolation_level=None, timeout=5.0)
        self._db.row_factory = sqlite3.Row
        with self._lock:
            self._db.executescript(SCHEMA)
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.execute("PRAGMA synchronous=FULL")
        if self.path != ":memory:":
            os.chmod(self.path, 0o600)

    def close(self) -> None:
        with self._lock:
            self._db.close()

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                yield self._db
            except QueueError:
                self._db.execute("ROLLBACK")
                raise
            except sqlite3.Error as exc:
                self._db.execute("ROLLBACK")
                raise QueueError("unavailable",
                                 f"the outbox refused the write: {exc}") from exc
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
            else:
                try:
                    self._db.execute("COMMIT")
                except sqlite3.Error as exc:
                    self._db.execute("ROLLBACK")
                    raise QueueError("unavailable",
                                     f"the outbox refused the commit: {exc}") from exc

    @contextmanager
    def _read(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            try:
                yield self._db
            except sqlite3.Error as exc:
                raise QueueError("unavailable",
                                 f"the outbox refused the read: {exc}") from exc

    # ------------------------------------------------------------- the outbox

    def enqueue(self, server: str, message: Message,
                now: float | None = None, room: str | None = None) -> str:
        """Take responsibility for a message, and answer with the id we kept.

        Enqueuing the identical message again is the same enqueue: a client
        that lost its own answer must be able to ask twice. The same id
        carrying anything else — different text, a different server — is a
        conflict, because the record already written is the one the server has
        been, or will be, told about.
        """
        validated(message, recipients_required=room is None)
        identifier(server, "server", 512)
        if room is not None:
            identifier(room, "room")
        # The room is part of what makes this message this message: the same id
        # sent to a room and to a list of mailboxes are two different messages,
        # and the second one must not pass as a retry of the first.
        mark = fingerprint(message) + ("" if room is None else f"@{room}")
        when = self._clock() if now is None else now
        with self._write() as db:
            row = db.execute("SELECT * FROM queue_outbox WHERE id=?",
                             (message.id,)).fetchone()
            if row is not None:
                if row["fingerprint"] == mark and row["server"] == server:
                    return message.id
                raise QueueError(
                    "conflict",
                    f"{message.id} is already in the outbox for {row['server']}"
                    " with different content")
            db.execute(
                "INSERT INTO queue_outbox (id, server, payload, room,"
                " fingerprint, state, attempts, created_at, next_at)"
                " VALUES (?,?,?,?,?,'queued',0,?,?)",
                (message.id, server, json.dumps(message.to_json()), room, mark,
                 when, when))
        return message.id

    def due(self, now: float) -> list[dict[str, Any]]:
        """Queued messages whose wait is over, oldest first.

        Ordered by when they were written down and not by when their backoff
        expires: a message that has failed four times is still older than one
        written a minute ago, and reordering the outbox by failure would let a
        stuck message push everything behind it out of order.
        """
        with self._read() as db:
            rows = db.execute(
                "SELECT * FROM queue_outbox WHERE state='queued' AND next_at<=?"
                " ORDER BY created_at, id", (now,)).fetchall()
        return [self._record(r) for r in rows]

    def record(self, message_id: str) -> dict[str, Any] | None:
        with self._read() as db:
            row = db.execute("SELECT * FROM queue_outbox WHERE id=?",
                             (message_id,)).fetchone()
        return self._record(row) if row is not None else None

    def history(self, limit: int = 1000) -> list[dict[str, Any]]:
        """Everything the outbox has held. Nothing here is deleted to save room."""
        with self._read() as db:
            rows = db.execute(
                "SELECT * FROM queue_outbox ORDER BY created_at, id LIMIT ?",
                (limit,)).fetchall()
        return [self._record(r) for r in rows]

    @staticmethod
    def _record(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"], "server": row["server"], "state": row["state"],
            "message": json.loads(row["payload"]), "room": row["room"],
            "attempts": row["attempts"], "created_at": row["created_at"],
            "next_at": row["next_at"], "last_error": row["last_error"],
            "accepted_at": row["accepted_at"],
            "receipt": json.loads(row["receipt"]) if row["receipt"] else None,
        }

    def _touch(self, db: sqlite3.Connection, message_id: str) -> sqlite3.Row:
        row = db.execute("SELECT * FROM queue_outbox WHERE id=?",
                         (message_id,)).fetchone()
        if row is None:
            raise QueueError("not_found", f"{message_id} is not in the outbox")
        return row

    def accepted(self, message_id: str, receipt: dict[str, Any]) -> None:
        """The server has it. Our retry stops; the record stays."""
        with self._write() as db:
            self._touch(db, message_id)
            db.execute(
                "UPDATE queue_outbox SET state='accepted', accepted_at=?,"
                " receipt=?, last_error=NULL WHERE id=?",
                (self._clock(), json.dumps(receipt), message_id))

    def retry(self, message_id: str, error: str, next_at: float) -> None:
        """A transport failure: count it, remember why, and come back later."""
        with self._write() as db:
            row = self._touch(db, message_id)
            if row["state"] == "accepted":
                raise QueueError("conflict",
                                 f"{message_id} was already accepted by the server")
            db.execute(
                "UPDATE queue_outbox SET state='queued', attempts=attempts+1,"
                " next_at=?, last_error=?, last_error_at=? WHERE id=?",
                (next_at, str(error)[:1000], self._clock(), message_id))

    def block(self, message_id: str, error: str) -> None:
        """A fault retrying cannot fix. It waits for a person, visibly."""
        with self._write() as db:
            row = self._touch(db, message_id)
            if row["state"] == "accepted":
                raise QueueError("conflict",
                                 f"{message_id} was already accepted by the server")
            db.execute(
                "UPDATE queue_outbox SET state='blocked', last_error=?,"
                " last_error_at=? WHERE id=?",
                (str(error)[:1000], self._clock(), message_id))

    def unblock(self, message_id: str, next_at: float | None = None) -> None:
        """Try it again, once whatever was wrong has been dealt with."""
        with self._write() as db:
            row = self._touch(db, message_id)
            if row["state"] != "blocked":
                raise QueueError("conflict", f"{message_id} is not blocked")
            db.execute("UPDATE queue_outbox SET state='queued', next_at=? WHERE id=?",
                       (self._clock() if next_at is None else next_at, message_id))

    # ------------------------------------------------------------- the binding

    def bind(self, server: str, binding: Binding) -> None:
        """Remember which session consumes this mailbox, and how.

        Rebinding the same mailbox to the same session keeps its turn counter:
        a plugin that restarted is the same run of automatic deliveries as far
        as a loop is concerned. Binding it to a DIFFERENT session starts that
        counter again, because it is a different session's turns being counted.
        """
        bound(binding)
        identifier(server, "server", 512)
        now = self._clock()
        with self._write() as db:
            row = db.execute(
                "SELECT * FROM queue_bindings WHERE server=? AND mailbox=?",
                (server, binding.mailbox)).fetchone()
            same = (row is not None and row["session"] == binding.session
                    and row["runtime"] == binding.runtime
                    and row["directory"] == binding.directory)
            db.execute(
                "INSERT INTO queue_bindings (server, mailbox, runtime, session,"
                " directory, mode, turns, paused, paused_reason, bound_at,"
                " updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)"
                " ON CONFLICT(server, mailbox) DO UPDATE SET"
                " runtime=excluded.runtime, session=excluded.session,"
                " directory=excluded.directory, mode=excluded.mode,"
                " turns=excluded.turns, paused=excluded.paused,"
                " paused_reason=excluded.paused_reason,"
                " updated_at=excluded.updated_at",
                (server, binding.mailbox, binding.runtime, binding.session,
                 binding.directory, binding.mode,
                 row["turns"] if same else 0,
                 row["paused"] if same else 0,
                 row["paused_reason"] if same else None,
                 row["bound_at"] if row is not None else now, now))

    def binding(self, server: str, mailbox: str) -> dict[str, Any] | None:
        with self._read() as db:
            row = db.execute(
                "SELECT * FROM queue_bindings WHERE server=? AND mailbox=?",
                (server, mailbox)).fetchone()
        return self._binding(row) if row is not None else None

    def bindings(self) -> list[dict[str, Any]]:
        with self._read() as db:
            rows = db.execute(
                "SELECT * FROM queue_bindings ORDER BY bound_at, mailbox").fetchall()
        return [self._binding(r) for r in rows]

    @staticmethod
    def _binding(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "server": row["server"], "mailbox": row["mailbox"],
            "runtime": row["runtime"], "session": row["session"],
            "directory": row["directory"], "mode": row["mode"],
            "turns": row["turns"], "paused": bool(row["paused"]),
            "paused_reason": row["paused_reason"],
            "bound_at": row["bound_at"], "updated_at": row["updated_at"],
        }

    def _held(self, db: sqlite3.Connection, server: str, mailbox: str) -> sqlite3.Row:
        row = db.execute("SELECT * FROM queue_bindings WHERE server=? AND mailbox=?",
                         (server, mailbox)).fetchone()
        if row is None:
            raise QueueError("not_found", f"{mailbox} is not bound to a session here")
        return row

    def turn_taken(self, server: str, mailbox: str) -> int:
        """Count one automatic activation, and say how many that makes in a row.

        Written before the turn is scheduled, not after it succeeds: a counter
        that only counts turns that went well does not stop the loop that keeps
        failing.
        """
        with self._write() as db:
            row = self._held(db, server, mailbox)
            turns = row["turns"] + 1
            db.execute(
                "UPDATE queue_bindings SET turns=?, updated_at=?"
                " WHERE server=? AND mailbox=?",
                (turns, self._clock(), server, mailbox))
            return turns

    def pause(self, server: str, mailbox: str, reason: str) -> None:
        with self._write() as db:
            self._held(db, server, mailbox)
            db.execute(
                "UPDATE queue_bindings SET paused=1, paused_reason=?, updated_at=?"
                " WHERE server=? AND mailbox=?",
                (str(reason)[:500], self._clock(), server, mailbox))

    def resume(self, server: str, mailbox: str) -> None:
        """A person said carry on: the pause lifts and the run starts again."""
        with self._write() as db:
            self._held(db, server, mailbox)
            db.execute(
                "UPDATE queue_bindings SET paused=0, paused_reason=NULL, turns=0,"
                " updated_at=? WHERE server=? AND mailbox=?",
                (self._clock(), server, mailbox))

    # ------------------------------------------------------------- the journal

    def begin_attempt(self, binding: Binding,
                      message_ids: list[str]) -> dict[str, Any]:
        """Write down that we are ABOUT to deliver, and what we will carry.

        This row exists so that a crash one instruction later is answerable.
        Its marker goes into the message the session receives, which is what
        makes the question «did this land?» a question about the session's own
        history rather than about our optimism.
        """
        bound(binding)
        if not message_ids:
            raise QueueError("invalid", "an attempt carries at least one message")
        for mid in message_ids:
            identifier(mid, "message id")
        attempt = new_attempt_id()
        with self._write() as db:
            row = db.execute(
                "SELECT server FROM queue_bindings WHERE mailbox=?"
                " ORDER BY updated_at DESC", (binding.mailbox,)).fetchone()
            if row is None:
                raise QueueError("not_found",
                                 f"{binding.mailbox} is not bound to a session here")
            db.execute(
                "INSERT INTO queue_attempts (id, server, mailbox, runtime, session,"
                " directory, marker, message_ids, state, started_at)"
                " VALUES (?,?,?,?,?,?,?,?,'prepared',?)",
                (attempt, row["server"], binding.mailbox, binding.runtime,
                 binding.session, binding.directory, marker_for(attempt),
                 json.dumps(list(message_ids)), self._clock()))
            return self._attempt(db.execute(
                "SELECT * FROM queue_attempts WHERE id=?", (attempt,)).fetchone())

    def finish_attempt(self, attempt_id: str, state: str,
                       opencode_message_id: str | None) -> None:
        """Close an attempt, once and in one direction.

        A terminal state is not revised in place. If reconciliation later shows
        that an `uncertain` attempt did land, that is a new attempt row and a
        receipt against the message — the record of what this run could
        actually see at the time stays as it was.
        """
        if state not in ("delivered", "uncertain", "cancelled"):
            raise QueueError("invalid",
                             "an attempt finishes delivered, uncertain or cancelled")
        with self._write() as db:
            row = db.execute("SELECT * FROM queue_attempts WHERE id=?",
                             (attempt_id,)).fetchone()
            if row is None:
                raise QueueError("not_found", f"no attempt {attempt_id}")
            if row["state"] != "prepared":
                raise QueueError("conflict",
                                 f"attempt {attempt_id} is already {row['state']}")
            db.execute(
                "UPDATE queue_attempts SET state=?, finished_at=?,"
                " opencode_message_id=? WHERE id=?",
                (state, self._clock(), opencode_message_id, attempt_id))

    def attempt(self, attempt_id: str) -> dict[str, Any] | None:
        with self._read() as db:
            row = db.execute("SELECT * FROM queue_attempts WHERE id=?",
                             (attempt_id,)).fetchone()
        return self._attempt(row) if row is not None else None

    def unfinished_attempts(self, mailbox: str) -> list[dict[str, Any]]:
        """What was prepared and never concluded — the crash boundary."""
        with self._read() as db:
            rows = db.execute(
                "SELECT * FROM queue_attempts WHERE mailbox=? AND state='prepared'"
                " ORDER BY started_at", (mailbox,)).fetchall()
        return [self._attempt(r) for r in rows]

    @staticmethod
    def _attempt(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"], "server": row["server"], "mailbox": row["mailbox"],
            "runtime": row["runtime"], "session": row["session"],
            "directory": row["directory"], "marker": row["marker"],
            "message_ids": json.loads(row["message_ids"]), "state": row["state"],
            "started_at": row["started_at"], "finished_at": row["finished_at"],
            "opencode_message_id": row["opencode_message_id"],
        }

    # ------------------------------------------------- acknowledgements asked for

    def request_ack(self, mailbox: str, message_id: str, sender: str) -> None:
        """Record that the agent says it has this message.

        Refused for a message this machine never delivered: an acknowledgement
        is the only evidence the sender gets that anybody read anything, and a
        session must not be able to answer for a message it was never given.
        """
        identifier(mailbox, "mailbox")
        identifier(message_id, "message_id")
        # The sender may be unknown to the agent typing the command: it names
        # ids, and the server resolves the rest unless two mailboxes have used
        # the same id for this one, which is the case `--sender` is for.
        if sender:
            identifier(sender, "sender")
        with self._write() as db:
            delivered = db.execute(
                "SELECT 1 FROM queue_attempts WHERE mailbox=? AND"
                " message_ids LIKE ? LIMIT 1",
                (mailbox, f'%"{message_id}"%')).fetchone()
            if delivered is None:
                raise QueueError(
                    "not_found",
                    f"{message_id} was not delivered to {mailbox} from here")
            db.execute(
                "INSERT INTO queue_acks (mailbox, sender, message_id, requested_at)"
                " VALUES (?,?,?,?) ON CONFLICT(mailbox, sender, message_id)"
                " DO UPDATE SET requested_at=excluded.requested_at, done_at=NULL",
                (mailbox, sender, message_id, self._clock()))

    def pending_acks(self, mailbox: str) -> list[dict[str, Any]]:
        with self._read() as db:
            rows = db.execute(
                "SELECT mailbox, sender, message_id, requested_at FROM queue_acks"
                " WHERE mailbox=? AND done_at IS NULL ORDER BY requested_at",
                (mailbox,)).fetchall()
        return [dict(r) for r in rows]

    def ack_done(self, mailbox: str, message_id: str, sender: str) -> None:
        with self._write() as db:
            db.execute(
                "UPDATE queue_acks SET done_at=? WHERE mailbox=? AND sender=?"
                " AND message_id=?",
                (self._clock(), mailbox, sender, message_id))

    # ------------------------------------------------------------ what it says

    def status(self) -> dict[str, Any]:
        """Counted from the rows, never from what the client remembers.

        Carries no credential: the server is named by its URL, and the token it
        is reached with lives in the session profile this never copies.
        """
        with self._read() as db:
            outbox = {r["state"]: r["n"] for r in db.execute(
                "SELECT state, COUNT(*) AS n FROM queue_outbox GROUP BY state")}
            attempts = {r["state"]: r["n"] for r in db.execute(
                "SELECT state, COUNT(*) AS n FROM queue_attempts GROUP BY state")}
            oldest = db.execute(
                "SELECT MIN(created_at) AS at FROM queue_outbox"
                " WHERE state != 'accepted'").fetchone()["at"]
            waiting = db.execute(
                "SELECT MIN(next_at) AS at FROM queue_outbox"
                " WHERE state != 'accepted' AND attempts > 0").fetchone()["at"]
            latest = db.execute(
                "SELECT last_error FROM queue_outbox WHERE last_error IS NOT NULL"
                " ORDER BY last_error_at DESC LIMIT 1").fetchone()
            servers = [r["server"] for r in db.execute(
                "SELECT DISTINCT server FROM queue_outbox ORDER BY server")]
            bindings = [self._binding(r) for r in db.execute(
                "SELECT * FROM queue_bindings ORDER BY bound_at, mailbox")]
        return {
            "outbox": outbox, "attempts": attempts, "servers": servers,
            "oldest_unsent_at": oldest, "next_retry_at": waiting,
            "last_error": latest["last_error"] if latest is not None else None,
            "bindings": bindings,
        }
