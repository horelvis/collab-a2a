"""What the client does when the server is not there, and when it comes back.

The client's job is to make «queued» mean something. So these tests are mostly
about failure: a connection that never opens, a hub that answers 500, a token
that has been revoked — and in each case what is left on the disk afterwards,
because that is what the next process has to work from.
"""

from __future__ import annotations

import json

import httpx
import pytest

from collab.messaging.client import QueueClient, backoff_seconds
from collab.messaging.local import LocalStore
from collab.messaging.model import Binding, Message, QueueError


def _message(mid='m1', text='diagnose'):
    return Message(mid, 'mb_a', ('mb_b',), 'request', text, 'now', 'c_1')


def _receipt(mid='m1'):
    return {'id': mid, 'accepted_at': '2026-09-10T00:00:00.000Z',
            'recipients': [{'mailbox': 'mb_b', 'seq': 1}]}


def _client(tmp_path, handler, path='local.db', **kw):
    local = LocalStore(tmp_path / path)
    transport = httpx.MockTransport(handler)
    return QueueClient('http://queue:9920', 'tok', local, transport=transport, **kw)


def test_the_same_message_is_sent_again_after_the_connection_failed(tmp_path):
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if len(seen) == 1:
            raise httpx.ConnectError('connection refused', request=request)
        return httpx.Response(200, json=_receipt())

    path = tmp_path / 'local.db'
    local = LocalStore(path)
    transport = httpx.MockTransport(handler)
    client = QueueClient('http://queue:9920', 'tok', local, transport=transport)
    queued = client.send(_message(), now=0.0)
    assert queued['state'] == 'queued'
    assert client.local.record('m1')['attempts'] == 1
    client.close()
    local.close()

    local = LocalStore(path)
    client = QueueClient('http://queue:9920', 'tok', local, transport=transport)
    assert client.flush(now=1_000.0)['accepted'] == ['m1']
    assert local.record('m1')['state'] == 'accepted'
    assert local.record('m1')['receipt']['recipients'][0]['seq'] == 1

    bodies = [json.loads(request.content) for request in seen]
    assert len(bodies) == 2
    assert bodies[0]['id'] == bodies[1]['id']
    client.close()
    local.close()


def test_a_rejected_credential_stops_retrying_and_says_so(tmp_path):
    def handler(request):
        return httpx.Response(401, json={'detail': 'a participant token is required'})

    client = _client(tmp_path, handler)
    result = client.send(_message(), now=0.0)
    assert result['state'] == 'blocked' 
    record = client.local.record('m1')
    assert record['state'] == 'blocked'
    assert 'token' in record['last_error']
    # A blocked record is not tried again on its own.
    assert client.flush(now=10_000.0) == {'accepted': [], 'retried': [],
                                          'blocked': []}
    client.close()


def test_a_refused_payload_is_blocked_and_a_server_fault_is_retried(tmp_path):
    answers = {'m1': httpx.Response(409, json={'detail': 'already accepted'}),
               'm2': httpx.Response(503, json={'detail': 'try later'})}

    def handler(request):
        return answers[json.loads(request.content)['id']]

    client = _client(tmp_path, handler)
    assert client.send(_message('m1'), now=0.0)['state'] == 'blocked'
    assert client.send(_message('m2'), now=0.0)['state'] == 'queued'
    out = client.flush(now=1_000.0)
    assert out == {'accepted': [], 'retried': ['m2'], 'blocked': []}
    assert client.local.record('m2')['next_at'] > 0.0
    client.close()


def test_an_ambiguous_failure_never_erases_the_record(tmp_path):
    """A reply we did not understand is not permission to forget the message."""
    def handler(request):
        return httpx.Response(418, content=b'not json at all')

    client = _client(tmp_path, handler)
    client.send(_message(), now=0.0)
    assert client.local.record('m1') is not None
    assert client.local.record('m1')['state'] in ('queued', 'blocked')
    client.close()


def test_the_wait_between_attempts_grows_and_stops_growing():
    waits = [backoff_seconds(n, jitter=lambda: 1.0) for n in range(0, 10)]
    assert waits[0] == 1.0
    assert waits == sorted(waits)
    assert max(waits) <= 60.0
    assert waits[-1] == 60.0
    # Jitter moves it, and never past the cap.
    assert backoff_seconds(9, jitter=lambda: 1.5) <= 60.0
    assert backoff_seconds(0, jitter=lambda: 0.5) == 0.5


def test_polling_asks_for_the_bound_mailbox_and_carries_the_token(tmp_path):
    asked: list[httpx.Request] = []

    def handler(request):
        asked.append(request)
        return httpx.Response(200, json={'messages': [
            {'id': 'm9', 'sender': 'mb_a', 'kind': 'request', 'text': 'hello',
             'seq': 1, 'state': 'pending'}]})

    client = _client(tmp_path, handler)
    binding = Binding('mb_b', 'opencode', 'ses_1', '/project', 'automatic')
    records = client.poll(binding)
    assert [r['id'] for r in records] == ['m9']
    assert asked[0].headers['authorization'] == 'Bearer tok'
    assert '/queue/mailboxes/mb_b/pending' in str(asked[0].url)
    client.close()


def test_a_reservation_is_taken_renewed_and_handed_back(tmp_path):
    calls: list[str] = []

    def handler(request):
        calls.append(request.url.path)
        if request.url.path.endswith('/lease'):
            return httpx.Response(200, json={'mailbox': 'mb_b', 'token': 't1',
                                             'generation': 3,
                                             'expires_at': 'later',
                                             'mode': 'automatic'})
        if request.url.path.endswith('/renew'):
            return httpx.Response(200, json={'mailbox': 'mb_b', 'token': 't1',
                                             'generation': 3,
                                             'expires_at': 'later still'})
        return httpx.Response(200, json={'mailbox': 'mb_b', 'released': True})

    client = _client(tmp_path, handler)
    binding = Binding('mb_b', 'opencode', 'ses_1', '/project', 'automatic')
    hold = client.acquire(binding)
    assert hold['generation'] == 3
    assert client.renew(binding.mailbox, hold)['expires_at'] == 'later still'
    client.release(binding.mailbox, hold)
    assert [c.rsplit('/', 1)[-1] for c in calls] == ['lease', 'renew', 'release']
    # Binding a mailbox is remembered locally, so a restart knows where it was.
    assert client.local.binding('http://queue:9920', 'mb_b')['session'] == 'ses_1'
    client.close()


def test_a_stale_reservation_reaches_the_caller_as_a_stale_reservation(tmp_path):
    def handler(request):
        return httpx.Response(409, json={'detail': 'expired'},
                              headers={'X-Collab-Queue': 'stale_lease'})

    client = _client(tmp_path, handler)
    with pytest.raises(QueueError) as caught:
        client.receipt('mb_b', 'm1', {'token': 't', 'generation': 1},
                       'delivered', 'at_1')
    assert caught.value.code == 'stale_lease'
    client.close()


def test_sending_writes_the_message_down_before_it_tries_anything(tmp_path):
    tried: list[str] = []

    def handler(request):
        tried.append(str(request.url))
        raise httpx.ConnectError('nobody there', request=request)

    client = _client(tmp_path, handler)
    outcome = client.send(_message(), now=0.0)
    assert outcome['state'] == 'queued'
    assert client.local.record('m1')['message']['text'] == 'diagnose'
    assert tried, 'it did try'
    client.close()


def test_a_message_written_before_we_had_an_identity_still_goes_out_as_one(tmp_path):
    """The outbox may be older than this agent's mailbox.

    A message can be queued on a machine that has never reached the server —
    that is the promise — so it is written under the name a person configured,
    and the id the server assigns is put in its place on the way out. The
    record keeps the id it was written with: a retry is the same record.
    """
    seen: list[dict] = []
    minted: list[str] = []

    def handler(request):
        body = json.loads(request.content)
        seen.append({'path': request.url.path, 'body': body})
        if request.url.path.endswith('/mailboxes'):
            return httpx.Response(200, json={'id': 'mb_assigned', 'owner': 'p_a',
                                             'name': body['name'],
                                             'created_at': 'now'})
        return httpx.Response(200, json=_receipt())

    local = LocalStore(tmp_path / 'local.db')
    client = QueueClient('http://queue:9920', 'tok', local,
                         transport=httpx.MockTransport(handler),
                         identity='mac/ios', on_mailbox=minted.append)
    waiting = Message('m1', 'mac/ios', ('mb_b',), 'request', 'diagnose', 'now', 'c_1')
    local.enqueue('http://queue:9920', waiting, now=0.0)
    assert client.flush(now=1000.0)['accepted'] == ['m1']

    assert [call['path'] for call in seen] == [
        '/ext/collab/v1/queue/mailboxes', '/ext/collab/v1/queue/messages']
    assert seen[1]['body']['sender'] == 'mb_assigned'
    assert minted == ['mb_assigned'], 'and the id is kept, so it is asked for once'
    assert local.record('m1')['message']['sender'] == 'mac/ios'
    client.close()
    local.close()


def test_an_identity_the_server_already_knows_is_not_asked_for_again(tmp_path):
    asked: list[str] = []

    def handler(request):
        asked.append(request.url.path)
        return httpx.Response(200, json=_receipt())

    local = LocalStore(tmp_path / 'local.db')
    client = QueueClient('http://queue:9920', 'tok', local,
                         transport=httpx.MockTransport(handler),
                         identity='mac/ios', mailbox='mb_known')
    local.enqueue('http://queue:9920',
                  Message('m1', 'mac/ios', ('mb_b',), 'request', 'x', 'now', 'c_1'),
                  now=0.0)
    client.flush(now=1000.0)
    assert asked == ['/ext/collab/v1/queue/messages']
    client.close()
    local.close()
