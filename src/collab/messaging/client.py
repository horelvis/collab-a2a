"""Talking to the queue server, on top of a local record that survives it.

Every method here is written around one rule: **the disk decides, not the
reply**. A message is in the outbox before the first request; an acceptance is
recorded against the record that was already there; and a failure whose meaning
we cannot establish leaves the record exactly as it was, because the one thing
worse than sending a message twice is deciding it is gone.

Failures are sorted into three, and the sorting is the interesting part:

- **transient** — the connection never opened, or the hub answered 5xx/429.
  Ours to retry, with a growing wait so a hub that is down does not meet a
  client that has turned into a load test.
- **credentials** — 401 or 403. Retrying cannot fix it and repeating it can
  make things worse, so the record is blocked and says why, for a person.
- **payload** — 400, 409, 413. The server has looked at this message and will
  not take it. Retrying it unchanged is a loop; it waits, visibly.

httpx is imported here and nowhere the CLI reaches on a cold path: see
`tests/test_cli_imports_no_httpx.py`.
"""

from __future__ import annotations

import random
from typing import Any, Callable

import httpx

from .local import LocalStore
from .model import Binding, Message, QueueError, bound, validated
from .routes import QUEUE_PREFIX

#: The first wait after a failure, and the longest one. Doubling in between: a
#: hub restarting is back within a second or two, and a hub that is off for the
#: night is asked for once a minute rather than a thousand times.
BACKOFF_BASE = 1.0
BACKOFF_CAP = 60.0

#: How long one request may take. Long enough for a hub under load, short
#: enough that a flush cannot sit on a dead socket for the rest of the day.
REQUEST_TIMEOUT = 15.0

#: The longest a held «anything pending?» request may stay open. The server
#: caps it too; this is the client saying what it will wait for, so a proxy
#: that silently drops idle connections is met by a reconnect and not a hang.
HOLD_SECONDS = 25.0

TRANSIENT = {408, 425, 429, 500, 502, 503, 504}
CREDENTIALS = {401, 403}
PAYLOAD = {400, 409, 413, 422}


def backoff_seconds(attempts: int,
                    jitter: Callable[[], float] | None = None) -> float:
    """How long to wait before attempt number `attempts + 1`.

    Jittered so that a hub coming back does not meet every client on the
    machine at the same instant, and capped after the jitter so that the jitter
    cannot carry it past the cap.
    """
    spread = jitter() if jitter is not None else random.uniform(0.5, 1.5)
    return min(BACKOFF_CAP, BACKOFF_BASE * (2 ** min(attempts, 16)) * spread)


class QueueClient:
    """The local outbox, and the server it is trying to reach."""

    def __init__(self, url: str, token: str, local: LocalStore,
                 transport: Any | None = None,
                 timeout: float = REQUEST_TIMEOUT) -> None:
        self.url = url.rstrip("/")
        self.local = local
        self._http = httpx.Client(
            base_url=f"{self.url}{QUEUE_PREFIX}",
            headers={"Authorization": f"Bearer {token}"},
            transport=transport, timeout=timeout)

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "QueueClient":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ------------------------------------------------------------- the wire

    def _request(self, method: str, path: str, **kw: Any) -> Any:
        """One call, with the server's own refusal code preserved.

        The routes stamp `X-Collab-Queue` with the code the store raised, so a
        `stale_lease` arrives here as a `stale_lease` rather than as «409, work
        out which kind».
        """
        try:
            reply = self._http.request(method, path, **kw)
        except httpx.HTTPError as exc:
            raise QueueError("unavailable", f"{self.url} could not be reached: {exc}") from exc
        if reply.status_code == 200:
            return reply.json()
        raise QueueError(self._code(reply), self._detail(reply))

    @staticmethod
    def _code(reply: httpx.Response) -> str:
        stamped = reply.headers.get("X-Collab-Queue")
        if stamped in QueueError.CODES:
            return stamped
        if reply.status_code in CREDENTIALS:
            return "forbidden"
        if reply.status_code == 404:
            return "not_found"
        if reply.status_code in TRANSIENT:
            return "unavailable"
        return "invalid"

    @staticmethod
    def _detail(reply: httpx.Response) -> str:
        try:
            body = reply.json()
        except ValueError:
            return f"HTTP {reply.status_code}"
        detail = body.get("detail") if isinstance(body, dict) else None
        return str(detail or f"HTTP {reply.status_code}")

    # ------------------------------------------------------------- sending

    def mailbox(self, name: str) -> dict[str, Any]:
        return self._request("POST", "/mailboxes", json={"name": name})

    def send(self, message: Message, room: str | None = None,
             now: float | None = None) -> dict[str, Any]:
        """Take the message on, then try to hand it over.

        The order is the point: the enqueue is what makes «queued» true, and
        the attempt that follows is allowed to fail without changing that. The
        answer says which of the two happened, so a caller never has to guess
        whether «sent» meant «written down» or «the server has it».
        """
        validated(message, recipients_required=room is None)
        self.local.enqueue(self.url, message, now=now, room=room)
        self.flush(now=now)
        record = self.local.record(message.id)
        return {"id": message.id, "state": record["state"],
                "receipt": record["receipt"], "error": record["last_error"]}

    def flush(self, now: float | None = None) -> dict[str, list[str]]:
        """Try every message whose wait is over, and record what happened."""
        import time as _time

        when = _time.time() if now is None else now
        out: dict[str, list[str]] = {"accepted": [], "retried": [], "blocked": []}
        for record in self.local.due(when):
            body = dict(record["message"])
            if record.get("room"):
                body.pop("recipients", None)
                body["room"] = record["room"]
            try:
                receipt = self._request("POST", "/messages", json=body)
            except QueueError as exc:
                self._failed(record, exc, when, out)
                continue
            self.local.accepted(record["id"], receipt)
            out["accepted"].append(record["id"])
        return out

    def _failed(self, record: dict[str, Any], exc: QueueError, when: float,
                out: dict[str, list[str]]) -> None:
        if exc.code in ("unavailable",):
            self.local.retry(record["id"], exc.detail,
                             when + backoff_seconds(record["attempts"]))
            out["retried"].append(record["id"])
            return
        # `forbidden` covers a rejected token as well as a mailbox that is not
        # ours; `invalid`, `conflict` and `not_found` are all things the server
        # has looked at and refused. None of them is fixed by asking again.
        self.local.block(record["id"], exc.detail)
        out["blocked"].append(record["id"])

    # ------------------------------------------------------------ consuming

    def poll(self, binding: Binding, wait: float = 0.0,
             limit: int = 100) -> list[dict[str, Any]]:
        """What the bound mailbox is holding, optionally waiting for it.

        `wait` holds the request open on the server rather than asking again in
        a loop here — and, more to the point, rather than the agent's model
        being woken to ask. Nothing about waiting changes what is returned.
        """
        bound(binding)
        params: dict[str, Any] = {"limit": limit}
        if wait:
            params["wait"] = min(wait, HOLD_SECONDS)
        body = self._request("GET", f"/mailboxes/{binding.mailbox}/pending",
                             params=params,
                             timeout=REQUEST_TIMEOUT + (params.get("wait") or 0))
        return body["messages"]

    def acquire(self, binding: Binding) -> dict[str, Any]:
        """Take the reservation, and write the binding down on this side too."""
        bound(binding)
        hold = self._request("POST", f"/mailboxes/{binding.mailbox}/lease",
                             json=binding.to_json())
        self.local.bind(self.url, binding)
        return hold

    def renew(self, mailbox: str, hold: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", f"/mailboxes/{mailbox}/renew",
                             json={"token": hold["token"],
                                   "generation": hold["generation"]})

    def release(self, mailbox: str, hold: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", f"/mailboxes/{mailbox}/release",
                             json={"token": hold["token"],
                                   "generation": hold["generation"]})

    def receipt(self, mailbox: str, message_id: str, hold: dict[str, Any],
                state: str, attempt_id: str | None,
                sender: str | None = None) -> dict[str, Any]:
        return self._request(
            "POST", f"/mailboxes/{mailbox}/receipts",
            json={"message_id": message_id, "sender": sender, "state": state,
                  "attempt_id": attempt_id, "token": hold["token"],
                  "generation": hold["generation"]})

    def status(self, mailbox: str) -> dict[str, Any]:
        return self._request("GET", f"/mailboxes/{mailbox}/status")
