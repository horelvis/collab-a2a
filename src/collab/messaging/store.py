"""The mailbox side of the queue: what the server writes down and stands by.

Everything here is one SQLite transaction per public call, opened with `BEGIN
IMMEDIATE` so two processes accepting the same message serialise instead of
racing, and rolled back whole on any failure. **Nothing is answered for before
it is committed**: `accept` returns a receipt only once the message and every
recipient's delivery row are on the disk, because a sender that is told
«accepted» stops retrying, and a recipient that never sees the message has no
way to ask for it again.

The tables are additive. This store is opened on the hub's own database, next
to the event log, so a session's backup and its queue travel together — and an
older database gains the queue tables the first time a newer collab opens it,
without any of its history being touched.

Synchronous sqlite3, called through `asyncio.to_thread` by the routes, exactly
as `collab.server.store` is.
"""

from __future__ import annotations

import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any, Callable, Iterator

from .model import (Binding, Message, QueueError, identifier, bound,
                    fingerprint, mailbox_name, stamp, validated)

#: A delivery is `pending` until a session has demonstrably been given it, and
#: `acknowledged` only when the agent named its id in an explicit tool call.
#: Neither means the work was done: that is the task's state, not this one's.
STATES = ("pending", "delivered", "acknowledged")

SCHEMA = """
CREATE TABLE IF NOT EXISTS queue_mailboxes (
    id         TEXT PRIMARY KEY,
    owner      TEXT NOT NULL,
    name       TEXT NOT NULL,
    created_at REAL NOT NULL,
    UNIQUE (owner, name)
);

-- THE MESSAGE AS ITS SENDER WROTE IT, immutable once accepted. Uniqueness is
-- scoped to the sending mailbox: two agents may both call their first message
-- `m_1`, and a retry of one of them must not collide with the other's.
CREATE TABLE IF NOT EXISTS queue_messages (
    sender       TEXT NOT NULL,
    id           TEXT NOT NULL,
    kind         TEXT NOT NULL,
    text         TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    conversation TEXT NOT NULL,
    reply_to     TEXT,
    accepted_at  REAL NOT NULL,
    fingerprint  TEXT NOT NULL,
    PRIMARY KEY (sender, id)
);

-- ONE ROW PER RECIPIENT, and the per-mailbox `seq` is the order that mailbox
-- reads its messages in. It is assigned on acceptance, like the event log's,
-- so it is the server's order and not any sender's clock.
CREATE TABLE IF NOT EXISTS queue_deliveries (
    mailbox         TEXT NOT NULL,
    sender          TEXT NOT NULL,
    message_id      TEXT NOT NULL,
    seq             INTEGER NOT NULL,
    state           TEXT NOT NULL DEFAULT 'pending',
    accepted_at     REAL NOT NULL,
    delivered_at    REAL,
    acknowledged_at REAL,
    PRIMARY KEY (mailbox, sender, message_id),
    UNIQUE (mailbox, seq)
);

-- EVERY STATE A DELIVERY PASSED THROUGH, appended and never rewritten. This is
-- what somebody reads when a message is claimed to have been delivered and the
-- agent says it never arrived.
CREATE TABLE IF NOT EXISTS queue_transitions (
    seq        INTEGER PRIMARY KEY AUTOINCREMENT,
    mailbox    TEXT NOT NULL,
    sender     TEXT NOT NULL,
    message_id TEXT NOT NULL,
    state      TEXT NOT NULL,
    at         REAL NOT NULL,
    attempt    TEXT
);
CREATE INDEX IF NOT EXISTS idx_queue_transitions_message
    ON queue_transitions(mailbox, sender, message_id, seq);

-- WHO CONSUMES A MAILBOX RIGHT NOW. One row per mailbox: a reservation with a
-- generation, so a consumer that was replaced cannot acknowledge on top of the
-- one that replaced it.
CREATE TABLE IF NOT EXISTS queue_leases (
    mailbox     TEXT PRIMARY KEY,
    owner       TEXT NOT NULL,
    token       TEXT NOT NULL,
    generation  INTEGER NOT NULL,
    runtime     TEXT NOT NULL,
    session     TEXT NOT NULL,
    directory   TEXT NOT NULL,
    mode        TEXT NOT NULL,
    acquired_at REAL NOT NULL,
    expires_at  REAL NOT NULL
);
"""

#: How long a reservation stands without being renewed. Sixty seconds, renewed
#: every twenty by a live consumer: long enough that an ordinary pause in the
#: plugin does not hand the mailbox to somebody else, short enough that a
#: session that died does not hold it for a working day.
LEASE_TTL_SECONDS = 60.0

#: How long a caller waits for another writer before the write is reported as
#: unavailable. Bounded, because a queue call is made from inside a request.
LOCK_WAIT_SECONDS = 5.0


def new_mailbox_id() -> str:
    return "mb_" + uuid.uuid4().hex[:12]


class QueueStore:
    """The server's mailboxes, messages, receipts and reservations."""

    def __init__(self, path: Path | str,
                 clock: Callable[[], float] = time.time) -> None:
        self.path = str(path)
        self._clock = clock
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        # `isolation_level=None` hands transaction control to us: every write
        # below opens `BEGIN IMMEDIATE` itself, which is the only way to make
        # two processes queue up rather than discover the conflict at COMMIT.
        self._db = sqlite3.connect(self.path, check_same_thread=False,
                                   isolation_level=None,
                                   timeout=LOCK_WAIT_SECONDS)
        self._db.row_factory = sqlite3.Row
        with self._lock:
            self._db.executescript(SCHEMA)
            self._db.execute("PRAGMA journal_mode=WAL")
            # FULL, not NORMAL: acceptance is answered for the moment it
            # returns, so it must survive the machine losing power, not just
            # the process going away.
            self._db.execute("PRAGMA synchronous=FULL")
            self._db.execute("PRAGMA foreign_keys=ON")

    def close(self) -> None:
        with self._lock:
            self._db.close()

    # ---------------------------------------------------------------- writing

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        """One transaction, committed whole or rolled back whole.

        A `QueueError` raised inside is the caller's answer and rolls back with
        it. Anything else the database says — a full disk, a lock we waited out,
        a constraint we did not anticipate — becomes `unavailable`, because the
        one thing we must never do is report acceptance for a write that failed.
        """
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
                                 f"the queue database refused the write: {exc}") from exc
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
            else:
                try:
                    self._db.execute("COMMIT")
                except sqlite3.Error as exc:
                    self._db.execute("ROLLBACK")
                    raise QueueError(
                        "unavailable",
                        f"the queue database refused the commit: {exc}") from exc

    @contextmanager
    def _read(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            try:
                yield self._db
            except sqlite3.Error as exc:
                raise QueueError("unavailable",
                                 f"the queue database refused the read: {exc}") from exc

    # --------------------------------------------------------------- mailboxes

    def mailbox(self, owner: str, name: str) -> dict[str, Any]:
        """This owner's mailbox by that name, created if it is not there yet.

        Idempotent by (owner, name) so a client that has lost its record of the
        id can ask again and get the same mailbox rather than a second one that
        nobody is sending to. Two owners may hold the same name; the id, not
        the name, is what anything routes on.
        """
        wanted = mailbox_name(name)
        with self._write() as db:
            row = db.execute(
                "SELECT id, owner, name, created_at FROM queue_mailboxes"
                " WHERE owner=? AND name=?", (owner, wanted)).fetchone()
            if row is None:
                now = self._clock()
                box = new_mailbox_id()
                db.execute(
                    "INSERT INTO queue_mailboxes (id, owner, name, created_at)"
                    " VALUES (?,?,?,?)", (box, owner, wanted, now))
                return {"id": box, "owner": owner, "name": wanted,
                        "created_at": stamp(now)}
            return {"id": row["id"], "owner": row["owner"], "name": row["name"],
                    "created_at": stamp(row["created_at"])}

    def mailboxes(self, owner: str) -> list[dict[str, Any]]:
        with self._read() as db:
            rows = db.execute(
                "SELECT id, owner, name, created_at FROM queue_mailboxes"
                " WHERE owner=? ORDER BY created_at, id", (owner,)).fetchall()
        return [{"id": r["id"], "owner": r["owner"], "name": r["name"],
                 "created_at": stamp(r["created_at"])} for r in rows]

    def _owned(self, db: sqlite3.Connection, owner: str, box: str) -> sqlite3.Row:
        row = db.execute("SELECT id, owner, name FROM queue_mailboxes WHERE id=?",
                         (box,)).fetchone()
        if row is None:
            raise QueueError("not_found", f"no mailbox {box}")
        if row["owner"] != owner:
            # Deliberately the same wording whoever asks: which mailboxes exist
            # and who holds them is not something an unauthorised caller learns
            # by asking about each in turn.
            raise QueueError("forbidden", f"{box} is not yours")
        return row

    # ---------------------------------------------------------------- accepting

    def accept(self, owner: str, message: Message,
               room_owners: tuple[str, ...] | None = None) -> dict[str, Any]:
        """Persist a message and one delivery per recipient, or persist nothing.

        Returns `{id, accepted_at, recipients: [{mailbox, seq}]}`. Sending the
        same message again returns that same receipt — the transport losing our
        reply is the ordinary case, and the sender's retry must not become a
        second message. Sending a DIFFERENT message under an id already used is
        a conflict the sender is told about, not an overwrite.

        `room_owners` addresses a conversation rather than a list of mailboxes.
        Their mailboxes are read INSIDE this transaction and fixed onto the
        message, so who was sent to is decided once, at acceptance — and a
        retry is answered from the row that is already there, without the
        membership being read again: somebody who joined in between neither
        receives a copy nor turns the retry into a conflict.
        """
        validated(message, recipients_required=room_owners is None)
        mark = fingerprint(message) if room_owners is None else None
        with self._write() as db:
            self._owned(db, owner, message.sender)
            existing = db.execute(
                "SELECT accepted_at, fingerprint FROM queue_messages"
                " WHERE sender=? AND id=?", (message.sender, message.id)).fetchone()
            if existing is not None:
                if room_owners is not None:
                    return self._receipt(db, message.sender, message.id,
                                         existing["accepted_at"])
                if existing["fingerprint"] != mark:
                    raise QueueError(
                        "conflict",
                        f"{message.id} was already accepted from this mailbox with"
                        " different content or recipients")
                return self._receipt(db, message.sender, message.id,
                                     existing["accepted_at"])
            if room_owners is not None:
                message = replace(message, recipients=self._room_mailboxes(
                    db, room_owners, exclude=message.sender))
                validated(message)
                mark = fingerprint(message)
            for box in message.recipients:
                if db.execute("SELECT 1 FROM queue_mailboxes WHERE id=?",
                              (box,)).fetchone() is None:
                    raise QueueError("not_found", f"no mailbox {box}")
            now = self._clock()
            db.execute(
                "INSERT INTO queue_messages (sender, id, kind, text, created_at,"
                " conversation, reply_to, accepted_at, fingerprint)"
                " VALUES (?,?,?,?,?,?,?,?,?)",
                (message.sender, message.id, message.kind, message.text,
                 message.created_at, message.conversation, message.reply_to,
                 now, mark))
            for box in sorted(set(message.recipients)):
                row = db.execute(
                    "SELECT MAX(seq) AS top FROM queue_deliveries WHERE mailbox=?",
                    (box,)).fetchone()
                seq = (row["top"] or 0) + 1
                db.execute(
                    "INSERT INTO queue_deliveries (mailbox, sender, message_id,"
                    " seq, state, accepted_at) VALUES (?,?,?,?,'pending',?)",
                    (box, message.sender, message.id, seq, now))
                db.execute(
                    "INSERT INTO queue_transitions (mailbox, sender, message_id,"
                    " state, at, attempt) VALUES (?,?,?,'pending',?,NULL)",
                    (box, message.sender, message.id, now))
            return self._receipt(db, message.sender, message.id, now)

    def _room_mailboxes(self, db: sqlite3.Connection, owners: tuple[str, ...],
                        exclude: str) -> tuple[str, ...]:
        """Every mailbox held by the people in a room, bar the sender's own.

        A room is addressed by who is in it, and a participant with no mailbox
        is simply not part of this conversation — but a room in which NOBODY
        has one is a message with no destination, and is refused rather than
        accepted into nothing.
        """
        found: list[str] = []
        for who in owners:
            rows = db.execute(
                "SELECT id FROM queue_mailboxes WHERE owner=? ORDER BY created_at, id",
                (who,)).fetchall()
            found.extend(r["id"] for r in rows if r["id"] != exclude)
        if not found:
            raise QueueError("invalid",
                             "nobody in that room has a mailbox to send to")
        return tuple(dict.fromkeys(found))

    def existing_receipt(self, owner: str, sender: str,
                         message_id: str) -> dict[str, Any] | None:
        """The receipt this message already has, if it has one."""
        identifier(sender, "sender")
        identifier(message_id, "id")
        with self._read() as db:
            self._owned(db, owner, sender)
            row = db.execute(
                "SELECT accepted_at FROM queue_messages WHERE sender=? AND id=?",
                (sender, message_id)).fetchone()
            if row is None:
                return None
            return self._receipt(db, sender, message_id, row["accepted_at"])

    def status(self, owner: str, mailbox: str) -> dict[str, Any]:
        """What this mailbox holds, for a person deciding whether to worry.

        Counted from the delivery rows themselves rather than from anything the
        client remembers, and carrying no token: this is a diagnostic, and a
        diagnostic that leaks the reservation's credential is a way of taking
        somebody else's mailbox.
        """
        with self._read() as db:
            row = self._owned(db, owner, mailbox)
            counts = {state: 0 for state in STATES}
            for r in db.execute(
                    "SELECT state, COUNT(*) AS n FROM queue_deliveries"
                    " WHERE mailbox=? GROUP BY state", (mailbox,)).fetchall():
                counts[r["state"]] = r["n"]
            oldest = db.execute(
                "SELECT MIN(accepted_at) AS at FROM queue_deliveries"
                " WHERE mailbox=? AND state != 'acknowledged'",
                (mailbox,)).fetchone()["at"]
            # Read here rather than through `lease`: this connection's lock is
            # already held, and taking it twice is a deadlock, not a slow call.
            held = db.execute("SELECT * FROM queue_leases WHERE mailbox=?",
                              (mailbox,)).fetchone()
            live = held is not None and held["expires_at"] > self._clock()
        return {
            "mailbox": mailbox, "name": row["name"], "counts": counts,
            "oldest_pending_at": stamp(oldest) if oldest is not None else None,
            "lease": self._visible_lease(held) if live else None,
        }

    @staticmethod
    def _visible_lease(row: sqlite3.Row) -> dict[str, Any]:
        """A reservation as anyone may see it: everything except the token."""
        return {"mailbox": row["mailbox"], "generation": row["generation"],
                "runtime": row["runtime"], "session": row["session"],
                "directory": row["directory"], "mode": row["mode"],
                "acquired_at": stamp(row["acquired_at"]),
                "expires_at": stamp(row["expires_at"])}

    def _receipt(self, db: sqlite3.Connection, sender: str, message_id: str,
                 accepted_at: float) -> dict[str, Any]:
        rows = db.execute(
            "SELECT mailbox, seq FROM queue_deliveries"
            " WHERE sender=? AND message_id=? ORDER BY mailbox",
            (sender, message_id)).fetchall()
        return {"id": message_id, "accepted_at": stamp(accepted_at),
                "recipients": [{"mailbox": r["mailbox"], "seq": r["seq"]}
                               for r in rows]}

    # ----------------------------------------------------------------- reading

    def pending(self, owner: str, mailbox: str, limit: int = 100) -> list[dict[str, Any]]:
        """What this mailbox has not acknowledged, oldest first.

        A delivered message is still pending: delivery says a session was given
        it, acknowledgement says the agent named it back. Only the second one
        takes it off this list.
        """
        if not isinstance(limit, int) or limit < 1 or limit > 1000:
            raise QueueError("invalid", "limit is between 1 and 1000")
        with self._read() as db:
            self._owned(db, owner, mailbox)
            rows = db.execute(
                "SELECT d.seq, d.state, d.sender, d.message_id, d.accepted_at,"
                " d.delivered_at, m.kind, m.text, m.created_at, m.conversation,"
                " m.reply_to FROM queue_deliveries d"
                " JOIN queue_messages m ON m.sender=d.sender AND m.id=d.message_id"
                " WHERE d.mailbox=? AND d.state != 'acknowledged'"
                " ORDER BY d.seq LIMIT ?", (mailbox, limit)).fetchall()
        return [self._record(r) for r in rows]

    @staticmethod
    def _record(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["message_id"], "sender": row["sender"],
            "kind": row["kind"], "text": row["text"],
            "created_at": row["created_at"], "conversation": row["conversation"],
            "reply_to": row["reply_to"], "seq": row["seq"], "state": row["state"],
            "accepted_at": stamp(row["accepted_at"]),
            "delivered_at": (stamp(row["delivered_at"])
                             if row["delivered_at"] is not None else None),
        }

    # ------------------------------------------------------------ reservations

    def acquire(self, owner: str, binding: Binding) -> dict[str, Any]:
        """Reserve this mailbox for one session, or say who holds it.

        A live reservation held by another session is a conflict reported to
        the caller, never a second consumer: two sessions taking turns out of
        the same mailbox is how the same request gets worked on twice. The same
        session may take its own reservation again — a plugin that restarted
        against the session it was bound to should not have to wait out a
        minute of its own expiry — and doing so rotates the generation, which
        retires the token the previous run of it was holding.
        """
        bound(binding)
        with self._write() as db:
            self._owned(db, owner, binding.mailbox)
            now = self._clock()
            row = db.execute("SELECT * FROM queue_leases WHERE mailbox=?",
                             (binding.mailbox,)).fetchone()
            if row is not None and row["expires_at"] > now:
                same = (row["runtime"] == binding.runtime
                        and row["session"] == binding.session
                        and row["directory"] == binding.directory)
                if not same:
                    raise QueueError(
                        "conflict",
                        f"{binding.mailbox} is being consumed by session "
                        f"{row['session']} until {stamp(row['expires_at'])}")
            generation = (row["generation"] if row is not None else 0) + 1
            token = uuid.uuid4().hex
            expires = now + LEASE_TTL_SECONDS
            db.execute(
                "INSERT INTO queue_leases (mailbox, owner, token, generation,"
                " runtime, session, directory, mode, acquired_at, expires_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?)"
                " ON CONFLICT(mailbox) DO UPDATE SET owner=excluded.owner,"
                " token=excluded.token, generation=excluded.generation,"
                " runtime=excluded.runtime, session=excluded.session,"
                " directory=excluded.directory, mode=excluded.mode,"
                " acquired_at=excluded.acquired_at, expires_at=excluded.expires_at",
                (binding.mailbox, owner, token, generation, binding.runtime,
                 binding.session, binding.directory, binding.mode, now, expires))
            return {"mailbox": binding.mailbox, "token": token,
                    "generation": generation, "expires_at": stamp(expires),
                    "mode": binding.mode}

    def _held(self, db: sqlite3.Connection, owner: str, mailbox: str,
              token: str, generation: Any, now: float) -> sqlite3.Row:
        """The caller's own live reservation, or `stale_lease` with the reason.

        Every check is made inside the caller's transaction and against the
        row as it is now: a consumer that was replaced while its request was in
        flight must be refused by this call, not by the one after it.
        """
        row = db.execute("SELECT * FROM queue_leases WHERE mailbox=?",
                         (mailbox,)).fetchone()
        if row is None:
            raise QueueError("stale_lease", f"nobody holds {mailbox}")
        if row["owner"] != owner or row["token"] != token:
            raise QueueError("stale_lease",
                             f"this is not the reservation {mailbox} is held by")
        if row["generation"] != generation:
            raise QueueError("stale_lease",
                             f"{mailbox} is on generation {row['generation']}, "
                             f"and this is generation {generation}")
        if row["expires_at"] <= now:
            raise QueueError("stale_lease",
                             f"this reservation on {mailbox} expired at "
                             f"{stamp(row['expires_at'])}")
        return row

    def renew(self, owner: str, mailbox: str, token: str,
              generation: Any) -> dict[str, Any]:
        with self._write() as db:
            self._owned(db, owner, mailbox)
            now = self._clock()
            self._held(db, owner, mailbox, token, generation, now)
            expires = now + LEASE_TTL_SECONDS
            db.execute("UPDATE queue_leases SET expires_at=? WHERE mailbox=?",
                       (expires, mailbox))
            return {"mailbox": mailbox, "token": token, "generation": generation,
                    "expires_at": stamp(expires)}

    def release(self, owner: str, mailbox: str, token: str,
                generation: Any) -> None:
        """Give the mailbox back, keeping the generation that was reached.

        The row stays: it is what the next `acquire` counts from, so a token
        from before the release cannot be made valid again by a later one
        landing on the same number.
        """
        with self._write() as db:
            self._owned(db, owner, mailbox)
            now = self._clock()
            self._held(db, owner, mailbox, token, generation, now)
            db.execute("UPDATE queue_leases SET expires_at=0 WHERE mailbox=?",
                       (mailbox,))

    def invalidate_leases(self) -> int:
        """Retire every reservation, once, when a server lifetime begins.

        Expiry alone would leave a window in which a consumer from before the
        restart could renew and carry on as though nothing had happened. The
        generations are kept, so tokens issued by the previous run are refused
        by number as well as by clock.
        """
        with self._write() as db:
            cursor = db.execute(
                "UPDATE queue_leases SET expires_at=0 WHERE expires_at > 0")
            return cursor.rowcount or 0

    def lease(self, owner: str, mailbox: str) -> dict[str, Any] | None:
        """Who consumes this mailbox now, without the token that proves it."""
        with self._read() as db:
            self._owned(db, owner, mailbox)
            now = self._clock()
            row = db.execute("SELECT * FROM queue_leases WHERE mailbox=?",
                             (mailbox,)).fetchone()
        if row is None or row["expires_at"] <= now:
            return None
        return self._visible_lease(row)

    # -------------------------------------------------------------- receipting

    def receipt(self, owner: str, mailbox: str, message_id: str, token: str,
                generation: Any, state: str, attempt_id: str | None,
                sender: str | None = None) -> dict[str, Any]:
        """Record that a delivery reached a session, or that the agent named it.

        States only advance. `delivered` is evidence that the bound session was
        given the message; `acknowledged` is an explicit call by the agent that
        received it, and is refused before a delivery, because acknowledging
        something nobody was given is the one claim this record exists to make
        impossible. Repeating either returns the state as it stands.

        `sender` disambiguates when two mailboxes have both used the same id
        for a message to this one. Left out where that is the case, the call is
        refused rather than guessed at.
        """
        if state not in ("delivered", "acknowledged"):
            raise QueueError("invalid",
                             "a receipt is 'delivered' or 'acknowledged', "
                             f"not {state!r}")
        identifier(message_id, "message_id")
        if sender is not None:
            identifier(sender, "sender")
        if attempt_id is not None:
            identifier(attempt_id, "attempt_id")
        with self._write() as db:
            self._owned(db, owner, mailbox)
            now = self._clock()
            self._held(db, owner, mailbox, token, generation, now)
            if sender is not None:
                rows = db.execute(
                    "SELECT * FROM queue_deliveries WHERE mailbox=? AND"
                    " message_id=? AND sender=?",
                    (mailbox, message_id, sender)).fetchall()
            else:
                rows = db.execute(
                    "SELECT * FROM queue_deliveries WHERE mailbox=? AND message_id=?",
                    (mailbox, message_id)).fetchall()
            if not rows:
                raise QueueError("not_found",
                                 f"{mailbox} was never sent a message {message_id}")
            if len(rows) > 1:
                raise QueueError(
                    "conflict",
                    f"{len(rows)} mailboxes have sent {mailbox} a message "
                    f"{message_id}; name the sender")
            row = rows[0]
            current = row["state"]
            if current == "acknowledged" or (state == "delivered"
                                             and current == "delivered"):
                return self._delivery(db, mailbox, row["sender"], message_id)
            if state == "acknowledged" and current == "pending":
                raise QueueError(
                    "conflict",
                    f"{message_id} has not been delivered to {mailbox} yet")
            column = "delivered_at" if state == "delivered" else "acknowledged_at"
            db.execute(
                f"UPDATE queue_deliveries SET state=?, {column}=?"
                " WHERE mailbox=? AND sender=? AND message_id=?",
                (state, now, mailbox, row["sender"], message_id))
            db.execute(
                "INSERT INTO queue_transitions (mailbox, sender, message_id,"
                " state, at, attempt) VALUES (?,?,?,?,?,?)",
                (mailbox, row["sender"], message_id, state, now, attempt_id))
            return self._delivery(db, mailbox, row["sender"], message_id)

    def _delivery(self, db: sqlite3.Connection, mailbox: str, sender: str,
                  message_id: str) -> dict[str, Any]:
        row = db.execute(
            "SELECT * FROM queue_deliveries WHERE mailbox=? AND sender=?"
            " AND message_id=?", (mailbox, sender, message_id)).fetchone()
        return {
            "id": message_id, "mailbox": mailbox, "sender": sender,
            "seq": row["seq"], "state": row["state"],
            "delivered_at": (stamp(row["delivered_at"])
                             if row["delivered_at"] is not None else None),
            "acknowledged_at": (stamp(row["acknowledged_at"])
                                if row["acknowledged_at"] is not None else None),
        }

    def transitions(self, owner: str, mailbox: str, message_id: str,
                    sender: str | None = None) -> list[dict[str, Any]]:
        """Everything this mailbox's copy of a message has been, in order."""
        with self._read() as db:
            self._owned(db, owner, mailbox)
            if sender is not None:
                rows = db.execute(
                    "SELECT state, at, attempt, sender FROM queue_transitions"
                    " WHERE mailbox=? AND message_id=? AND sender=? ORDER BY seq",
                    (mailbox, message_id, sender)).fetchall()
            else:
                rows = db.execute(
                    "SELECT state, at, attempt, sender FROM queue_transitions"
                    " WHERE mailbox=? AND message_id=? ORDER BY seq",
                    (mailbox, message_id)).fetchall()
        return [{"state": r["state"], "at": stamp(r["at"]),
                 "attempt": r["attempt"], "sender": r["sender"]} for r in rows]
