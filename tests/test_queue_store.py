"""Acceptance is a durable fact, or it did not happen.

The queue exists because a message that a sender believes was accepted, and
that no recipient can find after a restart, is worse than a refusal: the
sender has moved on. So every assertion here reopens the database and asks it
again, rather than trusting what the call returned.
"""

from __future__ import annotations

import sqlite3

import pytest

from collab.messaging.model import Message, QueueError
from collab.messaging.store import QueueStore


def _message(mid, sender, recipients, text='diagnose', conversation='c_1'):
    return Message(mid, sender, tuple(recipients), 'request', text,
                   '2026-09-10T00:00:00Z', conversation)


def test_acceptance_survives_reopen_and_lost_reply(tmp_path):
    path = tmp_path / 'queue.db'
    q = QueueStore(path)
    a = q.mailbox('p_a', 'mac/ios')['id']
    b = q.mailbox('p_b', 'backend/api')['id']
    m = Message('m_1', a, (b,), 'request', 'diagnose',
                '2026-09-10T00:00:00Z', 'c_1')
    receipt = q.accept('p_a', m)
    q.close()
    q = QueueStore(path)
    assert q.accept('p_a', m) == receipt
    assert [x['id'] for x in q.pending('p_b', b)] == ['m_1']
    q.close()


def test_the_same_id_with_a_different_body_is_a_visible_conflict(tmp_path):
    q = QueueStore(tmp_path / 'queue.db')
    a = q.mailbox('p_a', 'mac/ios')['id']
    b = q.mailbox('p_b', 'backend/api')['id']
    q.accept('p_a', _message('m_1', a, [b], text='diagnose'))
    with pytest.raises(QueueError) as caught:
        q.accept('p_a', _message('m_1', a, [b], text='deploy'))
    assert caught.value.code == 'conflict'
    # The original survives the attempt to overwrite it.
    assert [x['text'] for x in q.pending('p_b', b)] == ['diagnose']
    q.close()


def test_the_same_id_addressed_elsewhere_is_a_conflict_too(tmp_path):
    q = QueueStore(tmp_path / 'queue.db')
    a = q.mailbox('p_a', 'mac/ios')['id']
    b = q.mailbox('p_b', 'backend/api')['id']
    c = q.mailbox('p_c', 'linux/api')['id']
    q.accept('p_a', _message('m_1', a, [b]))
    with pytest.raises(QueueError) as caught:
        q.accept('p_a', _message('m_1', a, [c]))
    assert caught.value.code == 'conflict'
    assert q.pending('p_c', c) == []
    q.close()


def test_nobody_sends_as_a_mailbox_that_is_not_theirs(tmp_path):
    q = QueueStore(tmp_path / 'queue.db')
    a = q.mailbox('p_a', 'mac/ios')['id']
    b = q.mailbox('p_b', 'backend/api')['id']
    with pytest.raises(QueueError) as caught:
        q.accept('p_b', _message('m_1', a, [b]))
    assert caught.value.code == 'forbidden'
    assert q.pending('p_b', b) == []
    q.close()


def test_nobody_reads_a_mailbox_that_is_not_theirs(tmp_path):
    q = QueueStore(tmp_path / 'queue.db')
    a = q.mailbox('p_a', 'mac/ios')['id']
    b = q.mailbox('p_b', 'backend/api')['id']
    q.accept('p_a', _message('m_1', a, [b]))
    with pytest.raises(QueueError) as caught:
        q.pending('p_a', b)
    assert caught.value.code == 'forbidden'
    q.close()


def test_two_connections_accepting_the_same_message_agree_on_one(tmp_path):
    path = tmp_path / 'queue.db'
    one = QueueStore(path)
    a = one.mailbox('p_a', 'mac/ios')['id']
    b = one.mailbox('p_b', 'backend/api')['id']
    two = QueueStore(path)
    m = _message('m_1', a, [b])
    first = one.accept('p_a', m)
    second = two.accept('p_a', m)
    assert first == second
    assert len(one.pending('p_b', b)) == 1
    one.close()
    two.close()


def test_one_unknown_recipient_leaves_nothing_behind(tmp_path):
    path = tmp_path / 'queue.db'
    q = QueueStore(path)
    a = q.mailbox('p_a', 'mac/ios')['id']
    b = q.mailbox('p_b', 'backend/api')['id']
    with pytest.raises(QueueError) as caught:
        q.accept('p_a', _message('m_1', a, [b, 'mb_nobody']))
    assert caught.value.code == 'not_found'
    q.close()
    q = QueueStore(path)
    assert q.pending('p_b', b) == []
    # Nor may the id be claimed as accepted by a later, identical send.
    assert q.accept('p_a', _message('m_1', a, [b]))['recipients'] == [
        {'mailbox': b, 'seq': 1}]
    q.close()


def test_a_mailbox_reads_its_own_order_not_the_senders_clock(tmp_path):
    q = QueueStore(tmp_path / 'queue.db')
    a = q.mailbox('p_a', 'mac/ios')['id']
    b = q.mailbox('p_b', 'backend/api')['id']
    c = q.mailbox('p_c', 'linux/api')['id']
    q.accept('p_a', _message('m_1', a, [b]))
    q.accept('p_a', _message('m_2', a, [c]))
    q.accept('p_a', _message('m_3', a, [b, c]))
    assert [(x['id'], x['seq']) for x in q.pending('p_b', b)] == [
        ('m_1', 1), ('m_3', 2)]
    assert [(x['id'], x['seq']) for x in q.pending('p_c', c)] == [
        ('m_2', 1), ('m_3', 2)]
    q.close()


def test_a_failed_write_accepts_nothing(tmp_path):
    """A disk that refuses the delivery row must refuse the message with it.

    Standing in for the full disk: a trigger that raises on the second write
    of the transaction, so the message row is already there when it fails.
    """
    path = tmp_path / 'queue.db'
    q = QueueStore(path)
    a = q.mailbox('p_a', 'mac/ios')['id']
    b = q.mailbox('p_b', 'backend/api')['id']
    side = sqlite3.connect(path)
    side.execute("CREATE TRIGGER refuse BEFORE INSERT ON queue_deliveries"
                 " BEGIN SELECT RAISE(ABORT, 'disk full'); END")
    side.commit()
    with pytest.raises(QueueError) as caught:
        q.accept('p_a', _message('m_1', a, [b]))
    assert caught.value.code == 'unavailable'
    side.execute("DROP TRIGGER refuse")
    side.commit()
    side.close()
    q.close()

    q = QueueStore(path)
    assert q.pending('p_b', b) == []
    assert q.accept('p_a', _message('m_1', a, [b]))['id'] == 'm_1'
    assert [x['id'] for x in q.pending('p_b', b)] == ['m_1']
    q.close()


def test_a_mailbox_name_belongs_to_one_owner(tmp_path):
    q = QueueStore(tmp_path / 'queue.db')
    mine = q.mailbox('p_a', 'mac/ios')
    assert q.mailbox('p_a', 'mac/ios') == mine
    other = q.mailbox('p_b', 'mac/ios')
    assert other['id'] != mine['id']
    q.close()


def test_a_message_the_protocol_refuses_never_reaches_the_disk(tmp_path):
    q = QueueStore(tmp_path / 'queue.db')
    a = q.mailbox('p_a', 'mac/ios')['id']
    b = q.mailbox('p_b', 'backend/api')['id']
    with pytest.raises(QueueError) as caught:
        q.accept('p_a', Message('m_1', a, (b,), 'shout', 'hello',
                                '2026-09-10T00:00:00Z', 'c_1'))
    assert caught.value.code == 'invalid'
    with pytest.raises(QueueError):
        q.accept('p_a', _message('m_2', a, []))
    with pytest.raises(QueueError):
        q.accept('p_a', _message('m_3', a, [b], text=''))
    assert q.pending('p_b', b) == []
    q.close()


def test_the_database_it_writes_is_the_one_it_was_given(tmp_path):
    path = tmp_path / 'nested' / 'queue.db'
    q = QueueStore(path)
    q.mailbox('p_a', 'mac/ios')
    q.close()
    rows = sqlite3.connect(path).execute(
        "SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    assert 'queue_mailboxes' in {r[0] for r in rows}
