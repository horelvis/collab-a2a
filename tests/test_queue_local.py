"""The sender's own record, which is what «queued» means.

Nothing here talks to a server. The point of this store is the interval in
which there IS no server: a message the client has taken responsibility for
before the first request goes out, and still has after the process that made
that request has gone. Every test reopens the database for the same reason the
server-side ones do — the value returned by a call proves nothing about what
survives the call.
"""

from __future__ import annotations

import os
import sqlite3
import stat

import pytest

from collab.messaging.local import LocalStore, local_store_path
from collab.messaging.model import Binding, Message, QueueError


def _message(mid='m1', text='hello', sender='a', recipients=('b',)):
    return Message(mid, sender, tuple(recipients), 'request', text, 'now', 'c')


def test_offline_outbox_survives_process_exit(tmp_path):
    path = tmp_path / 'local.db'
    s = LocalStore(path)
    m = Message('m1', 'a', ('b',), 'request', 'hello', 'now', 'c')
    assert s.enqueue('http://queue:9920', m) == 'm1'
    s.close()
    s = LocalStore(path)
    rows = s.due(float('inf'))
    assert len(rows) == 1
    assert rows[0]['message']['id'] == 'm1'
    s.close()


def test_an_accepted_message_stops_being_due_and_keeps_its_receipt(tmp_path):
    path = tmp_path / 'local.db'
    s = LocalStore(path)
    s.enqueue('http://queue:9920', _message())
    s.accepted('m1', {'id': 'm1', 'accepted_at': '2026-09-10T00:00:00.000Z',
                      'recipients': [{'mailbox': 'b', 'seq': 1}]})
    s.close()
    s = LocalStore(path)
    assert s.due(float('inf')) == []
    record = s.record('m1')
    assert record['state'] == 'accepted'
    assert record['receipt']['recipients'] == [{'mailbox': 'b', 'seq': 1}]
    s.close()


def test_a_retry_keeps_the_message_id_and_the_wait_it_was_given(tmp_path):
    path = tmp_path / 'local.db'
    s = LocalStore(path)
    s.enqueue('http://queue:9920', _message())
    s.retry('m1', 'connection refused', next_at=500.0)
    s.close()
    s = LocalStore(path)
    assert s.due(now=499.0) == []
    due = s.due(now=501.0)
    assert [r['id'] for r in due] == ['m1']
    assert due[0]['attempts'] == 1
    assert due[0]['last_error'] == 'connection refused'
    assert due[0]['message']['id'] == 'm1'
    s.close()


def test_a_waiting_message_holds_nothing_up_and_keeps_its_place(tmp_path):
    """A backoff takes one message out of the queue, not the ones behind it.

    And when its wait is over it goes back where it was written, so the order
    the sender wrote them in survives a failure that resolves itself.
    """
    s = LocalStore(tmp_path / 'local.db')
    s.enqueue('http://queue:9920', _message('m1'), now=10.0)
    s.enqueue('http://queue:9920', _message('m2'), now=20.0)
    s.enqueue('http://queue:9920', _message('m3'), now=30.0)
    s.retry('m1', 'nope', next_at=100.0)
    assert [r['id'] for r in s.due(now=50.0)] == ['m2', 'm3']
    assert [r['id'] for r in s.due(now=150.0)] == ['m1', 'm2', 'm3']
    s.close()


def test_the_same_id_twice_is_refused_rather_than_replaced(tmp_path):
    s = LocalStore(tmp_path / 'local.db')
    s.enqueue('http://queue:9920', _message(text='first'))
    # The identical message again is the same enqueue, not a second one.
    assert s.enqueue('http://queue:9920', _message(text='first')) == 'm1'
    with pytest.raises(QueueError) as caught:
        s.enqueue('http://queue:9920', _message(text='second'))
    assert caught.value.code == 'conflict'
    assert s.record('m1')['message']['text'] == 'first'
    s.close()


def test_one_outbox_does_not_send_another_servers_messages(tmp_path):
    s = LocalStore(tmp_path / 'local.db')
    s.enqueue('http://one:9920', _message('m1'))
    s.enqueue('http://two:9920', _message('m2'))
    assert {r['id']: r['server'] for r in s.due(float('inf'))} == {
        'm1': 'http://one:9920', 'm2': 'http://two:9920'}
    # Changing server does not carry a message over: that id belongs to the
    # outbox it was written into.
    with pytest.raises(QueueError) as caught:
        s.enqueue('http://two:9920', _message('m1'))
    assert caught.value.code == 'conflict'
    s.close()


def test_a_binding_is_remembered_with_its_session_and_its_mode(tmp_path):
    path = tmp_path / 'local.db'
    s = LocalStore(path)
    s.bind('http://queue:9920', Binding('mb_1', 'opencode', 'ses_1',
                                        '/project', 'automatic'))
    s.close()
    s = LocalStore(path)
    held = s.binding('http://queue:9920', 'mb_1')
    assert held['session'] == 'ses_1'
    assert held['directory'] == '/project'
    assert held['mode'] == 'automatic'
    assert held['turns'] == 0
    assert held['paused'] is False
    s.close()


def test_a_prepared_attempt_is_still_there_after_a_crash(tmp_path):
    """Written BEFORE the host is called, which is the whole point of it.

    A process that dies between the write and the call has a record saying it
    might have delivered; a process that dies without one has no way to tell
    that from never having tried.
    """
    path = tmp_path / 'local.db'
    s = LocalStore(path)
    binding = Binding('mb_1', 'opencode', 'ses_1', '/project', 'automatic')
    s.bind('http://queue:9920', binding)
    attempt = s.begin_attempt(binding, ['m1', 'm2'])
    assert attempt['state'] == 'prepared'
    assert attempt['id'] in attempt['marker']
    s.close()

    s = LocalStore(path)
    unfinished = s.unfinished_attempts('mb_1')
    assert [a['id'] for a in unfinished] == [attempt['id']]
    assert unfinished[0]['message_ids'] == ['m1', 'm2']
    assert unfinished[0]['session'] == 'ses_1'
    s.finish_attempt(attempt['id'], 'delivered', 'msg_from_opencode')
    assert s.unfinished_attempts('mb_1') == []
    assert s.attempt(attempt['id'])['opencode_message_id'] == 'msg_from_opencode'
    s.close()


def test_an_uncertain_attempt_stays_uncertain_until_somebody_says_otherwise(tmp_path):
    s = LocalStore(tmp_path / 'local.db')
    binding = Binding('mb_1', 'opencode', 'ses_1', '/project', 'automatic')
    s.bind('http://queue:9920', binding)
    attempt = s.begin_attempt(binding, ['m1'])
    s.finish_attempt(attempt['id'], 'uncertain', None)
    assert s.attempt(attempt['id'])['state'] == 'uncertain'
    # Uncertain is a terminal state for the attempt; the reconciliation writes
    # a new one rather than quietly overwriting the evidence of this one.
    with pytest.raises(QueueError):
        s.finish_attempt(attempt['id'], 'delivered', 'msg_1')
    assert s.attempt(attempt['id'])['state'] == 'uncertain'
    s.close()


def test_a_cancelled_attempt_is_recorded_as_cancelled(tmp_path):
    s = LocalStore(tmp_path / 'local.db')
    binding = Binding('mb_1', 'opencode', 'ses_1', '/project', 'automatic')
    s.bind('http://queue:9920', binding)
    attempt = s.begin_attempt(binding, ['m1'])
    s.finish_attempt(attempt['id'], 'cancelled', None)
    assert s.attempt(attempt['id'])['state'] == 'cancelled'
    with pytest.raises(QueueError):
        s.finish_attempt(attempt['id'], 'nonsense', None)
    s.close()


def test_the_turn_counter_and_the_pause_outlive_the_process(tmp_path):
    path = tmp_path / 'local.db'
    s = LocalStore(path)
    binding = Binding('mb_1', 'opencode', 'ses_1', '/project', 'automatic')
    s.bind('http://queue:9920', binding)
    for expected in (1, 2, 3):
        assert s.turn_taken('http://queue:9920', 'mb_1') == expected
    s.pause('http://queue:9920', 'mb_1', 'too many turns in a row')
    s.close()

    s = LocalStore(path)
    held = s.binding('http://queue:9920', 'mb_1')
    assert held['turns'] == 3
    assert held['paused'] is True
    assert held['paused_reason'] == 'too many turns in a row'
    s.resume('http://queue:9920', 'mb_1')
    held = s.binding('http://queue:9920', 'mb_1')
    assert held['paused'] is False
    assert held['turns'] == 0
    s.close()


def test_a_write_that_fails_leaves_the_record_as_it_was(tmp_path):
    path = tmp_path / 'local.db'
    s = LocalStore(path)
    s.enqueue('http://queue:9920', _message())
    side = sqlite3.connect(path)
    side.execute("CREATE TRIGGER refuse BEFORE UPDATE ON queue_outbox"
                 " BEGIN SELECT RAISE(ABORT, 'disk full'); END")
    side.commit()
    with pytest.raises(QueueError) as caught:
        s.accepted('m1', {'id': 'm1', 'accepted_at': 'now', 'recipients': []})
    assert caught.value.code == 'unavailable'
    side.execute("DROP TRIGGER refuse")
    side.commit()
    side.close()
    assert s.record('m1')['state'] == 'queued'
    assert [r['id'] for r in s.due(float('inf'))] == ['m1']
    s.close()


def test_status_counts_what_is_in_the_database_and_names_no_credential(tmp_path):
    s = LocalStore(tmp_path / 'local.db')
    s.enqueue('http://queue:9920', _message('m1'), now=10.0)
    s.enqueue('http://queue:9920', _message('m2'), now=20.0)
    s.enqueue('http://queue:9920', _message('m3'), now=30.0)
    s.accepted('m2', {'id': 'm2', 'accepted_at': 'now', 'recipients': []})
    s.retry('m3', 'credentials rejected', next_at=900.0)
    s.block('m3', 'credentials rejected')
    binding = Binding('mb_1', 'opencode', 'ses_1', '/project', 'automatic')
    s.bind('http://queue:9920', binding)
    s.begin_attempt(binding, ['m1'])
    state = s.status()
    assert state['outbox'] == {'queued': 1, 'accepted': 1, 'blocked': 1}
    assert state['oldest_unsent_at'] == 10.0
    assert state['last_error'] == 'credentials rejected'
    assert state['next_retry_at'] == 900.0
    assert state['attempts'] == {'prepared': 1}
    assert state['bindings'][0]['session'] == 'ses_1'
    assert 'token' not in repr(state)
    s.close()


def test_nothing_is_deleted_to_make_room(tmp_path):
    s = LocalStore(tmp_path / 'local.db')
    for n in range(50):
        s.enqueue('http://queue:9920', _message(f'm{n}'))
        s.accepted(f'm{n}', {'id': f'm{n}', 'accepted_at': 'now', 'recipients': []})
    assert len(s.history()) == 50
    s.close()


def test_the_outbox_lives_beside_the_global_config_and_is_private(tmp_path, monkeypatch):
    monkeypatch.setenv('COLLAB_CONFIG', str(tmp_path / 'cfg' / 'config.json'))
    where = local_store_path()
    assert where.parent.parent == tmp_path / 'cfg'
    s = LocalStore(where)
    s.enqueue('http://queue:9920', _message())
    s.close()
    assert where.exists()
    assert stat.S_IMODE(os.stat(where.parent).st_mode) == 0o700
    assert stat.S_IMODE(os.stat(where).st_mode) == 0o600
