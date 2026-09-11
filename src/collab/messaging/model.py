"""What a queued message is, and what it may never be.

Validation lives here rather than in the store or the routes because all three
speak about the same record and only one of them may decide what a legal one
looks like. A message that fails here has not been accepted, has not been
written down, and must not be answered for.

**Bounds are checked before the disk is touched.** Everything in a `Message`
arrives from another machine, and the store's job is to persist what it is
given, not to argue with it — so the argument happens first, and the store may
assume its input is already the right shape.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from ..protocol import MAX_MESSAGE

#: A request or a response may activate a turn in the receiving session; an
#: informational record is filed and never activates one on its own. The
#: distinction is the spec's, and the scheduler in the plugin depends on it.
KINDS = ("request", "response", "informational")

#: Modes a binding may consume in. `automatic` groups pending work into a turn
#: of its own; `notification` shows it and waits for a person to say so.
MODES = ("automatic", "notification")

#: Identifiers travel in URLs, JSON and SQL parameters. Sixty-four characters
#: is longer than anything we mint (`mb_` and twelve hex) and short enough that
#: a forged one cannot be used to push a payload around.
MAX_ID = 64

#: A mailbox name is what a person reads in a status line: `mac/ios-jarvis`.
MAX_MAILBOX_NAME = 128

#: The origin timestamp is the sender's own ISO-8601 string, kept verbatim so
#: it survives a round trip unchanged. The server never orders by it.
MAX_TIMESTAMP = 64


class QueueError(Exception):
    """A refusal in the caller's terms: why, and what kind of why.

    `code` is the vocabulary the HTTP layer maps to a status and the client
    maps to a decision — `invalid` and `conflict` are the sender's to fix,
    `forbidden` and `not_found` are addressing faults, `stale_lease` means
    somebody else consumes this mailbox now, and `unavailable` means the
    database refused the write, so nothing was accepted.
    """

    CODES = ("invalid", "conflict", "forbidden", "not_found", "stale_lease",
             "unavailable")

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail


@dataclass(frozen=True)
class Message:
    id: str
    sender: str  # stable mailbox ID, owner checked against authentication
    recipients: tuple[str, ...]
    kind: str  # request | response | informational
    text: str
    created_at: str
    conversation: str
    reply_to: str | None = None

    @classmethod
    def from_json(cls, raw: Any, recipients_required: bool = True) -> "Message":
        """A message as it arrived over the wire, or `invalid`.

        Unknown fields are refused rather than ignored: a client sending a
        field this version does not implement is not sending the message it
        thinks it is, and silence would let it believe otherwise.
        """
        if not isinstance(raw, dict):
            raise QueueError("invalid", "a message is a JSON object")
        known = {f for f in cls.__dataclass_fields__}
        unknown = sorted(set(raw) - known)
        if unknown:
            raise QueueError("invalid", f"unknown fields: {', '.join(unknown)}")
        missing = sorted(known - {"reply_to"} - set(raw))
        if missing:
            raise QueueError("invalid", f"missing fields: {', '.join(missing)}")
        recipients = raw.get("recipients")
        if not isinstance(recipients, list):
            raise QueueError("invalid", "recipients is a list of mailbox ids")
        return validated(cls(
            id=raw["id"], sender=raw["sender"], recipients=tuple(recipients),
            kind=raw["kind"], text=raw["text"], created_at=raw["created_at"],
            conversation=raw["conversation"], reply_to=raw.get("reply_to"),
        ), recipients_required=recipients_required)

    def to_json(self) -> dict[str, Any]:
        return {
            "id": self.id, "sender": self.sender,
            "recipients": list(self.recipients), "kind": self.kind,
            "text": self.text, "created_at": self.created_at,
            "conversation": self.conversation, "reply_to": self.reply_to,
        }


@dataclass(frozen=True)
class Binding:
    mailbox: str
    runtime: str
    session: str
    directory: str
    mode: str  # automatic | notification

    @classmethod
    def from_json(cls, raw: Any, mailbox: str | None = None) -> "Binding":
        if not isinstance(raw, dict):
            raise QueueError("invalid", "a binding is a JSON object")
        known = {f for f in cls.__dataclass_fields__}
        unknown = sorted(set(raw) - known)
        if unknown:
            raise QueueError("invalid", f"unknown fields: {', '.join(unknown)}")
        box = mailbox if mailbox is not None else raw.get("mailbox")
        if mailbox is not None and raw.get("mailbox", mailbox) != mailbox:
            raise QueueError("invalid", "the binding names another mailbox")
        return bound(cls(
            mailbox=box, runtime=raw.get("runtime"), session=raw.get("session"),
            directory=raw.get("directory"), mode=raw.get("mode"),
        ))

    def to_json(self) -> dict[str, Any]:
        return {"mailbox": self.mailbox, "runtime": self.runtime,
                "session": self.session, "directory": self.directory,
                "mode": self.mode}


def identifier(value: Any, field: str, limit: int = MAX_ID) -> str:
    if not isinstance(value, str):
        raise QueueError("invalid", f"{field} is text")
    if not value or len(value) > limit:
        raise QueueError("invalid",
                         f"{field} is 1 to {limit} characters, not {len(value)}")
    if any(ch.isspace() or ord(ch) < 0x20 for ch in value):
        raise QueueError("invalid", f"{field} carries whitespace or control characters")
    return value


def validated(message: Message, recipients_required: bool = True) -> Message:
    """The same message, once it is known to be one.

    `recipients_required` is off for the moment between a room being named as
    the destination and the server resolving who was in it: the draft is
    checked in every other respect first, so a malformed message is refused
    before anybody's membership is read.
    """
    identifier(message.id, "id")
    identifier(message.sender, "sender")
    if message.kind not in KINDS:
        raise QueueError("invalid",
                         f"kind is one of {', '.join(KINDS)}, not {message.kind!r}")
    if not isinstance(message.text, str) or not message.text.strip():
        raise QueueError("invalid", "a message carries text")
    if len(message.text) > MAX_MESSAGE:
        # Refused at its full length, never cut: see protocol.MAX_MESSAGE.
        raise QueueError("invalid", f"this message is {len(message.text):,} "
                                    f"characters and the limit is {MAX_MESSAGE:,}")
    identifier(message.created_at, "created_at", MAX_TIMESTAMP)
    identifier(message.conversation, "conversation")
    if message.reply_to is not None:
        identifier(message.reply_to, "reply_to")
    if recipients_required and not message.recipients:
        raise QueueError("invalid", "a message has at least one recipient")
    seen: set[str] = set()
    for box in message.recipients:
        identifier(box, "recipient")
        if box in seen:
            raise QueueError("invalid", f"{box} is named twice as a recipient")
        seen.add(box)
    return message


def bound(binding: Binding) -> Binding:
    """The same binding, once it is known to be one."""
    identifier(binding.mailbox, "mailbox")
    identifier(binding.runtime, "runtime")
    identifier(binding.session, "session")
    if not isinstance(binding.directory, str) or not binding.directory.strip():
        raise QueueError("invalid", "a binding names the project directory")
    if len(binding.directory) > 4096:
        raise QueueError("invalid", "the directory path is longer than 4096 characters")
    if binding.mode not in MODES:
        raise QueueError("invalid",
                         f"mode is one of {', '.join(MODES)}, not {binding.mode!r}")
    return binding


def mailbox_name(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise QueueError("invalid", "a mailbox has a name")
    name = value.strip()
    if len(name) > MAX_MAILBOX_NAME:
        raise QueueError("invalid",
                         f"a mailbox name is at most {MAX_MAILBOX_NAME} characters")
    if any(ord(ch) < 0x20 for ch in name):
        raise QueueError("invalid", "a mailbox name carries control characters")
    return name


def fingerprint(message: Message) -> str:
    """What makes this message THIS message, for deduplication.

    Recipients are sorted: the same message addressed to the same people is
    the same message whichever order the sender listed them in, and a retry
    that reorders them must return the original receipt rather than a
    conflict. Everything else is compared exactly, so reusing an id for
    different content is visible instead of silently overwriting.
    """
    canonical = {
        "id": message.id,
        "sender": message.sender,
        "recipients": sorted(message.recipients),
        "kind": message.kind,
        "text": message.text,
        "created_at": message.created_at,
        "conversation": message.conversation,
        "reply_to": message.reply_to,
    }
    raw = json.dumps(canonical, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def stamp(when: float) -> str:
    """A server timestamp as it is reported, derived only from what was stored.

    A receipt returned twice must be equal twice — the second one is rebuilt
    from the row — so the text form is a pure function of the stored number.
    """
    return (datetime.fromtimestamp(when, timezone.utc)
            .isoformat(timespec="milliseconds").replace("+00:00", "Z"))
