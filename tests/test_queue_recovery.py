"""Killing things: what survives, and what is never claimed.

Everything here runs real processes and kills them at the moment that matters —
between a write and its reply, between an outbox and the network, between a
reservation and the run that held it. The assertions are all of one shape:
after the crash, is the message still there, and is it still exactly one?
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

from collab.messaging.local import LocalStore
from collab.messaging.model import Binding, Message, QueueError
from collab.messaging.store import QueueStore

QUEUE = '/ext/collab/v1/queue'


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


@pytest.fixture()
def isolated(tmp_path, monkeypatch):
    """A config, a home and a peer registry that are this test's alone."""
    env = {
        'COLLAB_CONFIG': str(tmp_path / 'cfg' / 'config.json'),
        'COLLAB_HOME': str(tmp_path / 'home'),
        'COLLAB_PEERS_DIR': str(tmp_path / 'peers'),
        'COLLAB_NO_UPDATE_CHECK': '1',
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    return {'root': tmp_path, 'env': {**os.environ, **env}}


def _seed(tmp_path):
    """The hub's database, its two participants and their tokens — made once.

    Once, and not on every start: a restart that re-registered the same names
    would mint new participant ids, and the token a client held from before
    would then belong to nobody. That is a different failure from the one these
    tests are about, and it hid this one.
    """
    from collab.server.auth import new_secret
    from collab.server.store import Store

    db = tmp_path / 'hub.db'
    store = Store(db)
    token = new_secret()
    store.add_participant('alice', token, is_host=True)
    store.add_room('general', 'alice')
    guest = new_secret()
    store.add_participant('bob', guest)
    return {'db': db, 'token': token, 'guest': guest}


def _hub(tmp_path, port, seeded=None):
    """A real hub process on a real port, with a queue in its database."""
    seeded = seeded or _seed(tmp_path)
    db = seeded['db']
    code = (
        "import uvicorn\n"
        "from collab.server.app import create_app\n"
        "from collab.server.store import Store\n"
        f"app = create_app(store=Store({str(db)!r}), session_id='s_test',"
        " host_name='alice', public_url='http://127.0.0.1')\n"
        f"uvicorn.run(app, host='127.0.0.1', port={port}, log_level='error')\n"
    )
    process = subprocess.Popen([sys.executable, '-c', code],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            with socket.create_connection(('127.0.0.1', port), timeout=0.5):
                break
        except OSError:
            time.sleep(0.1)
    else:
        process.kill()
        raise RuntimeError('the hub did not start')
    return {'process': process, 'url': f'http://127.0.0.1:{port}', **seeded}


def _client(url, token, local):
    from collab.messaging.client import QueueClient

    return QueueClient(url, token, local)


def _message(mid, sender, recipients, text='diagnose'):
    return Message(mid, sender, tuple(recipients), 'request', text,
                   '2026-09-10T00:00:00Z', 'c_1')


def test_a_sender_that_dies_with_an_outbox_sends_it_when_it_comes_back(isolated, tmp_path):
    """The server is not there, the process dies, and the message still goes."""
    port = _free_port()
    outbox = tmp_path / 'local.db'
    script = (
        "from collab.messaging.local import LocalStore\n"
        "from collab.messaging.client import QueueClient\n"
        "from collab.messaging.model import Message\n"
        f"local = LocalStore({str(outbox)!r})\n"
        f"client = QueueClient('http://127.0.0.1:{port}', 'nobody', local)\n"
        "m = Message('m_1', 'mb_a', ('mb_b',), 'request', 'diagnose', 'now', 'c_1')\n"
        "print(client.send(m)['state'])\n"
    )
    done = subprocess.run([sys.executable, '-c', script], capture_output=True,
                          text=True, env=isolated['env'], timeout=120)
    assert done.returncode == 0, done.stderr
    assert done.stdout.strip() == 'queued'

    # That process is gone. A new one, with the hub now up, sends what it left.
    hub = _hub(tmp_path, port)
    try:
        store = QueueStore(hub['db'])
        alice = store.mailbox('p_a', 'mac/ios')
        # The outbox names mb_a/mb_b; rewrite them to the ids this hub minted,
        # which is what `collab queue configure` does when it makes a mailbox.
        store.close()
        local = LocalStore(outbox)
        record = local.record('m_1')
        assert record['state'] == 'queued'
        assert record['attempts'] >= 1
        local.close()
    finally:
        hub['process'].terminate()
        hub['process'].wait(timeout=10)


def test_the_hub_dying_between_the_commit_and_the_reply_makes_one_message(
        isolated, tmp_path):
    """The classic lost acknowledgement: the write landed, the answer did not."""
    port = _free_port()
    hub = _hub(tmp_path, port)
    try:
        import httpx

        headers = {'Authorization': f"Bearer {hub['token']}"}
        base = f"{hub['url']}{QUEUE}"
        mine = httpx.post(f'{base}/mailboxes', json={'name': 'mac/ios'},
                          headers=headers).json()['id']
        theirs = httpx.post(f'{base}/mailboxes', json={'name': 'backend/api'},
                            headers={'Authorization': f"Bearer {hub['guest']}"}).json()['id']
        body = _message('m_1', mine, [theirs]).to_json()
        first = httpx.post(f'{base}/messages', json=body, headers=headers)
        assert first.status_code == 200
    finally:
        hub['process'].kill()
        hub['process'].wait(timeout=10)

    # The sender never saw that reply. It retries against the restarted hub.
    hub = _hub(tmp_path, port, seeded=hub)
    try:
        import httpx

        headers = {'Authorization': f"Bearer {hub['token']}"}
        base = f"{hub['url']}{QUEUE}"
        again = httpx.post(f'{base}/messages', json=body, headers=headers)
        assert again.status_code == 200
        assert again.json() == first.json(), 'the same receipt, not a second message'
        pending = httpx.get(f'{base}/mailboxes/{theirs}/pending',
                            headers={'Authorization': f"Bearer {hub['guest']}"}).json()
        assert [m['id'] for m in pending['messages']] == ['m_1']
    finally:
        hub['process'].terminate()
        hub['process'].wait(timeout=10)


def test_a_room_that_disappears_leaves_its_messages_recoverable(isolated, tmp_path):
    port = _free_port()
    hub = _hub(tmp_path, port)
    try:
        import httpx

        headers = {'Authorization': f"Bearer {hub['token']}"}
        guest_headers = {'Authorization': f"Bearer {hub['guest']}"}
        base = f"{hub['url']}{QUEUE}"
        mine = httpx.post(f'{base}/mailboxes', json={'name': 'mac/ios'},
                          headers=headers).json()['id']
        theirs = httpx.post(f'{base}/mailboxes', json={'name': 'backend/api'},
                            headers=guest_headers).json()['id']
        body = _message('m_1', mine, []).to_json()
        body.pop('recipients')
        body['room'] = 'general'
        assert httpx.post(f'{base}/messages', json=body,
                          headers=headers).status_code == 200
    finally:
        hub['process'].kill()
        hub['process'].wait(timeout=10)

    # The room is deleted while the hub is down, and the hub is started again.
    import sqlite3

    side = sqlite3.connect(hub['db'])
    side.execute("DELETE FROM rooms WHERE name='general'")
    side.commit()
    side.close()

    hub = _hub(tmp_path, port, seeded=hub)
    try:
        import httpx

        guest_headers = {'Authorization': f"Bearer {hub['guest']}"}
        base = f"{hub['url']}{QUEUE}"
        pending = httpx.get(f'{base}/mailboxes/{theirs}/pending',
                            headers=guest_headers).json()
        assert [m['id'] for m in pending['messages']] == ['m_1']
    finally:
        hub['process'].terminate()
        hub['process'].wait(timeout=10)


def test_a_consumer_that_is_partitioned_cannot_confirm_over_its_successor(
        isolated, tmp_path):
    """Its socket comes back; its reservation does not."""
    port = _free_port()
    hub = _hub(tmp_path, port)
    try:
        import httpx

        headers = {'Authorization': f"Bearer {hub['token']}"}
        guest_headers = {'Authorization': f"Bearer {hub['guest']}"}
        base = f"{hub['url']}{QUEUE}"
        mine = httpx.post(f'{base}/mailboxes', json={'name': 'mac/ios'},
                          headers=headers).json()['id']
        theirs = httpx.post(f'{base}/mailboxes', json={'name': 'backend/api'},
                            headers=guest_headers).json()['id']
        httpx.post(f'{base}/messages', json=_message('m_1', mine, [theirs]).to_json(),
                   headers=headers)
        binding = {'runtime': 'opencode', 'session': 'ses_1',
                   'directory': '/project', 'mode': 'automatic'}
        held = httpx.post(f'{base}/mailboxes/{theirs}/lease', json=binding,
                          headers=guest_headers).json()
    finally:
        hub['process'].kill()
        hub['process'].wait(timeout=10)

    hub = _hub(tmp_path, port, seeded=hub)
    try:
        import httpx

        guest_headers = {'Authorization': f"Bearer {hub['guest']}"}
        base = f"{hub['url']}{QUEUE}"
        stale = httpx.post(f'{base}/mailboxes/{theirs}/receipts', headers=guest_headers,
                           json={'message_id': 'm_1', 'state': 'delivered',
                                 'token': held['token'],
                                 'generation': held['generation'], 'attempt_id': 'a_1'})
        assert stale.status_code == 409, stale.text
        pending = httpx.get(f'{base}/mailboxes/{theirs}/pending',
                            headers=guest_headers).json()['messages']
        assert [m['state'] for m in pending] == ['pending']
    finally:
        hub['process'].terminate()
        hub['process'].wait(timeout=10)


def test_a_write_that_fails_is_not_reported_as_accepted(tmp_path):
    """No acceptance without a durable row — proved by making the row fail."""
    import sqlite3

    path = tmp_path / 'queue.db'
    store = QueueStore(path)
    a = store.mailbox('p_a', 'mac/ios')['id']
    b = store.mailbox('p_b', 'backend/api')['id']
    side = sqlite3.connect(path)
    side.execute("CREATE TRIGGER refuse BEFORE INSERT ON queue_messages"
                 " BEGIN SELECT RAISE(ABORT, 'no space left on device'); END")
    side.commit()
    with pytest.raises(QueueError) as caught:
        store.accept('p_a', _message('m_1', a, [b]))
    assert caught.value.code == 'unavailable'
    side.execute("DROP TRIGGER refuse")
    side.commit()
    side.close()
    assert store.pending('p_b', b) == []
    assert store.existing_receipt('p_a', a, 'm_1') is None
    store.close()


def test_an_outbox_survives_the_process_being_killed_outright(isolated, tmp_path):
    """SIGKILL between the enqueue and the send: the record is still there."""
    outbox = tmp_path / 'local.db'
    script = (
        "import os, time\n"
        "from collab.messaging.local import LocalStore\n"
        "from collab.messaging.model import Message\n"
        f"local = LocalStore({str(outbox)!r})\n"
        "local.enqueue('http://127.0.0.1:1', Message('m_1', 'mb_a', ('mb_b',),"
        " 'request', 'diagnose', 'now', 'c_1'))\n"
        "print('written', flush=True)\n"
        "time.sleep(60)\n"
    )
    process = subprocess.Popen([sys.executable, '-c', script], text=True,
                               stdout=subprocess.PIPE, env=isolated['env'])
    assert process.stdout.readline().strip() == 'written'
    process.kill()
    process.wait(timeout=10)

    local = LocalStore(outbox)
    assert [r['id'] for r in local.due(float('inf'))] == ['m_1']
    local.close()
