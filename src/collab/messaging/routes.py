"""The queue over HTTP, on the hub's own authentication.

These routes hold no authority of their own. Who is calling comes from the
bearer token the existing middleware already resolved — `request.user.id` —
and never from a field in the body, for the reason `from` is never
client-supplied on the messaging path: anything that lets a caller name its
own identity is a way of speaking as somebody else.

Everything the store may refuse arrives here as a `QueueError`, and its code is
the only thing that decides the status. A client reads `invalid` as «my
payload», `conflict` as «that id is taken, or somebody else holds the mailbox»,
`forbidden`/`not_found` as «wrong address», and `unavailable` as «nothing was
accepted, come back».
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Callable, Iterable

from fastapi import HTTPException, Request

from ..protocol import EXT_PREFIX
from .model import Binding, Message, QueueError, identifier

QUEUE_PREFIX = f"{EXT_PREFIX}/queue"

#: A queue request is one message and its addressing, and `protocol.MAX_MESSAGE`
#: caps the text at eight thousand characters — four bytes each at worst, so
#: 128 KiB is room for the largest legal body several times over. Anything past
#: it is refused on the content length, before the body is read into memory,
#: because a client that can make the hub buffer a megabyte per request can
#: make it buffer a hundred.
MAX_BODY_BYTES = 128 * 1024

#: The longest the server will hold a pending request open, and how often it
#: looks while it does. A quarter of a second is far below what a person or a
#: session notices and far above what SQLite minds being asked.
MAX_HOLD_SECONDS = 30.0
HOLD_POLL_SECONDS = 0.25

STATUS = {
    "invalid": 400,
    "conflict": 409,
    "stale_lease": 409,
    "forbidden": 403,
    "not_found": 404,
    "unavailable": 503,
}


def _http(exc: QueueError) -> HTTPException:
    return HTTPException(status_code=STATUS.get(exc.code, 400),
                         detail=exc.detail, headers={"X-Collab-Queue": exc.code})


async def _body(request: Request) -> dict[str, Any]:
    declared = request.headers.get("content-length")
    if declared and declared.isdigit() and int(declared) > MAX_BODY_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"a queue request is at most {MAX_BODY_BYTES:,} bytes")
    raw = await request.body()
    if len(raw) > MAX_BODY_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"a queue request is at most {MAX_BODY_BYTES:,} bytes")
    try:
        body = json.loads(raw or b"{}")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"malformed JSON: {exc}") from exc
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="a queue request is a JSON object")
    return body


def _token(body: dict[str, Any]) -> tuple[str, int]:
    """The reservation a caller claims to hold, as far as its shape goes."""
    try:
        token = identifier(body.get("token"), "token", 128)
    except QueueError as exc:
        raise _http(exc) from exc
    generation = body.get("generation")
    if not isinstance(generation, int) or isinstance(generation, bool):
        raise HTTPException(status_code=400, detail="generation is a whole number")
    return token, generation


def register_queue_routes(
    app,
    queue,
    require: Callable[[Request], Any],
    participants: Callable[[str], Iterable[str] | None],
) -> None:
    """Install `/ext/collab/v1/queue/*` on an existing hub app.

    `require` is the hub's own `_require`, so an unauthenticated call is
    refused with the same 401 as every other extension route. `participants`
    resolves a room name to the ids of the people in it, or `None` when there
    is no such room — the queue never reads the room tables itself, and a room
    that is later closed takes nothing already accepted with it.
    """

    async def _call(fn, *args, **kwargs):
        try:
            return await asyncio.to_thread(fn, *args, **kwargs)
        except QueueError as exc:
            raise _http(exc) from exc

    @app.post(f"{QUEUE_PREFIX}/mailboxes", tags=["collab"])
    async def create_mailbox(request: Request) -> dict[str, Any]:
        user = require(request)
        body = await _body(request)
        return await _call(queue.mailbox, user.id, body.get("name"))

    @app.get(f"{QUEUE_PREFIX}/mailboxes", tags=["collab"])
    async def list_mailboxes(request: Request) -> dict[str, Any]:
        user = require(request)
        return {"mailboxes": await _call(queue.mailboxes, user.id)}

    @app.post(f"{QUEUE_PREFIX}/messages", tags=["collab"])
    async def send_message(request: Request) -> dict[str, Any]:
        """Accept a message for a list of mailboxes, or for a room's members.

        One form or the other, never both and never neither: a message that
        names a room AND recipients is two different messages depending on who
        reads it, and is refused rather than resolved in the hub's favour.
        """
        user = require(request)
        body = await _body(request)
        room = body.pop("room", None)
        if room is not None and "recipients" in body:
            raise HTTPException(
                status_code=400,
                detail="a message names recipients or a room, not both")
        if room is None:
            try:
                message = Message.from_json(body)
            except QueueError as exc:
                raise _http(exc) from exc
            return await _call(queue.accept, user.id, message)

        try:
            message = Message.from_json({**body, "recipients": []},
                                        recipients_required=False)
        except QueueError as exc:
            raise _http(exc) from exc
        # The retry of a room message is answered from what was written down
        # the first time, so nobody who joined since is added to it and nobody
        # who left is dropped from it.
        prior = await _call(queue.existing_receipt, user.id, message.sender,
                            message.id)
        if prior is not None:
            return prior
        owners = participants(str(room))
        if owners is None:
            raise HTTPException(status_code=404, detail=f"no room {room}")
        return await _call(queue.accept, user.id, message, tuple(owners))

    @app.get(f"{QUEUE_PREFIX}/mailboxes/{{mailbox}}/pending", tags=["collab"])
    async def pending(request: Request, mailbox: str, limit: int = 100,
                      wait: float = 0.0) -> dict[str, Any]:
        """What the mailbox holds, optionally held open until it holds something.

        `wait` exists so that a consumer waiting for work costs one open
        request rather than a request every second — and, further up, so that
        nothing has to wake an agent's model to ask. It is bounded here as well
        as at the client: a hold nobody bounded is a socket a proxy will cut at
        a time of its own choosing, and a client that cannot tell «nothing yet»
        from «cut off» retries blindly.
        """
        user = require(request)
        held = min(max(float(wait), 0.0), MAX_HOLD_SECONDS)
        deadline = time.monotonic() + held
        while True:
            records = await _call(queue.pending, user.id, mailbox, limit)
            if records or time.monotonic() >= deadline:
                return {"messages": records}
            if await request.is_disconnected():
                return {"messages": []}
            await asyncio.sleep(HOLD_POLL_SECONDS)

    @app.get(f"{QUEUE_PREFIX}/mailboxes/{{mailbox}}/status", tags=["collab"])
    async def status(request: Request, mailbox: str) -> dict[str, Any]:
        user = require(request)
        return await _call(queue.status, user.id, mailbox)

    @app.post(f"{QUEUE_PREFIX}/mailboxes/{{mailbox}}/lease", tags=["collab"])
    async def lease(request: Request, mailbox: str) -> dict[str, Any]:
        user = require(request)
        body = await _body(request)
        try:
            binding = Binding.from_json(body, mailbox=mailbox)
        except QueueError as exc:
            raise _http(exc) from exc
        return await _call(queue.acquire, user.id, binding)

    @app.post(f"{QUEUE_PREFIX}/mailboxes/{{mailbox}}/renew", tags=["collab"])
    async def renew(request: Request, mailbox: str) -> dict[str, Any]:
        user = require(request)
        token, generation = _token(await _body(request))
        return await _call(queue.renew, user.id, mailbox, token, generation)

    @app.post(f"{QUEUE_PREFIX}/mailboxes/{{mailbox}}/release", tags=["collab"])
    async def release(request: Request, mailbox: str) -> dict[str, Any]:
        user = require(request)
        token, generation = _token(await _body(request))
        await _call(queue.release, user.id, mailbox, token, generation)
        return {"mailbox": mailbox, "released": True}

    @app.post(f"{QUEUE_PREFIX}/mailboxes/{{mailbox}}/receipts", tags=["collab"])
    async def receipts(request: Request, mailbox: str) -> dict[str, Any]:
        user = require(request)
        body = await _body(request)
        token, generation = _token(body)
        message_id = body.get("message_id")
        sender = body.get("sender")
        return await _call(queue.receipt, user.id, mailbox, message_id, token,
                           generation, body.get("state"), body.get("attempt_id"),
                           sender=sender)

    @app.get(f"{QUEUE_PREFIX}/mailboxes/{{mailbox}}/transitions/{{message_id}}",
             tags=["collab"])
    async def transitions(request: Request, mailbox: str, message_id: str,
                          sender: str | None = None) -> dict[str, Any]:
        user = require(request)
        return {"transitions": await _call(queue.transitions, user.id, mailbox,
                                           message_id, sender)}
