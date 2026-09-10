"""One session consumes a mailbox, and the one it replaced cannot answer for it.

A reservation is not a lock on the work: it is a way of telling two consumers
apart. The session that lost its reservation may still be running, still
holding a batch it was given, and still able to reach the server — so every
call it makes has to be refused by name rather than by hope.
"""

from __future__ import annotations

import pytest

from collab.messaging.model import Binding, Message, QueueError
from collab.messaging.store import QueueStore


def _fake_clock(start=100.0):
    now = [start]
    return now, (lambda: now[0])


def _store(tmp_path, now=None):
    if now is None:
        _, clock = _fake_clock()
        return QueueStore(tmp_path / 'q.db', clock=clock)
    return QueueStore(tmp_path / 'q.db', clock=lambda: now[0])


def _sent(q, sender_owner, sender, box, mid='m_1', text='diagnose'):
    q.accept(sender_owner, Message(mid, sender, (box,), 'request', text,
                                   '2026-09-10T00:00:00Z', 'c_1'))
    return mid


def test_expired_consumer_cannot_renew_after_takeover(tmp_path):
    now = [100.0]
    q = QueueStore(tmp_path / 'q.db', clock=lambda: now[0])
    box = q.mailbox('p_a', 'mac/ios')['id']
    one = Binding(box, 'r1', 's1', '/project', 'automatic')
    two = Binding(box, 'r2', 's2', '/project', 'automatic')
    old = q.acquire('p_a', one)
    with pytest.raises(QueueError):
        q.acquire('p_a', two)
    now[0] += 61
    fresh = q.acquire('p_a', two)
    assert fresh['generation'] > old['generation']
    with pytest.raises(QueueError):
        q.renew('p_a', box, old['token'], old['generation'])
    q.close()


def test_a_reservation_belongs_to_the_owner_of_the_mailbox(tmp_path):
    q = _store(tmp_path)
    box = q.mailbox('p_a', 'mac/ios')['id']
    with pytest.raises(QueueError) as caught:
        q.acquire('p_b', Binding(box, 'r1', 's1', '/project', 'automatic'))
    assert caught.value.code == 'forbidden'
    q.close()


def test_holding_a_reservation_acknowledges_nothing(tmp_path):
    q = _store(tmp_path)
    a = q.mailbox('p_a', 'mac/ios')['id']
    b = q.mailbox('p_b', 'backend/api')['id']
    _sent(q, 'p_a', a, b)
    q.acquire('p_b', Binding(b, 'r1', 's1', '/project', 'automatic'))
    assert [x['state'] for x in q.pending('p_b', b)] == ['pending']
    q.close()


def test_a_delivered_message_is_still_pending_until_it_is_acknowledged(tmp_path):
    q = _store(tmp_path)
    a = q.mailbox('p_a', 'mac/ios')['id']
    b = q.mailbox('p_b', 'backend/api')['id']
    _sent(q, 'p_a', a, b)
    lease = q.acquire('p_b', Binding(b, 'r1', 's1', '/project', 'automatic'))
    q.receipt('p_b', b, 'm_1', lease['token'], lease['generation'],
              'delivered', 'a_1')
    rows = q.pending('p_b', b)
    assert [x['state'] for x in rows] == ['delivered']
    assert rows[0]['delivered_at']
    q.receipt('p_b', b, 'm_1', lease['token'], lease['generation'],
              'acknowledged', 'a_1')
    assert q.pending('p_b', b) == []
    q.close()


def test_an_acknowledgement_before_a_delivery_is_refused(tmp_path):
    q = _store(tmp_path)
    a = q.mailbox('p_a', 'mac/ios')['id']
    b = q.mailbox('p_b', 'backend/api')['id']
    _sent(q, 'p_a', a, b)
    lease = q.acquire('p_b', Binding(b, 'r1', 's1', '/project', 'automatic'))
    with pytest.raises(QueueError) as caught:
        q.receipt('p_b', b, 'm_1', lease['token'], lease['generation'],
                  'acknowledged', 'a_1')
    assert caught.value.code == 'conflict'
    assert [x['state'] for x in q.pending('p_b', b)] == ['pending']
    q.close()


def test_the_same_receipt_twice_says_the_same_thing(tmp_path):
    q = _store(tmp_path)
    a = q.mailbox('p_a', 'mac/ios')['id']
    b = q.mailbox('p_b', 'backend/api')['id']
    _sent(q, 'p_a', a, b)
    lease = q.acquire('p_b', Binding(b, 'r1', 's1', '/project', 'automatic'))
    first = q.receipt('p_b', b, 'm_1', lease['token'], lease['generation'],
                      'delivered', 'a_1')
    assert q.receipt('p_b', b, 'm_1', lease['token'], lease['generation'],
                     'delivered', 'a_1') == first
    q.close()


def test_a_receipt_names_a_message_of_its_own_mailbox(tmp_path):
    q = _store(tmp_path)
    a = q.mailbox('p_a', 'mac/ios')['id']
    b = q.mailbox('p_b', 'backend/api')['id']
    c = q.mailbox('p_c', 'linux/api')['id']
    q.accept('p_a', Message('m_1', a, (c,), 'request', 'for c',
                            '2026-09-10T00:00:00Z', 'c_1'))
    lease = q.acquire('p_b', Binding(b, 'r1', 's1', '/project', 'automatic'))
    with pytest.raises(QueueError) as caught:
        q.receipt('p_b', b, 'm_1', lease['token'], lease['generation'],
                  'delivered', 'a_1')
    assert caught.value.code == 'not_found'
    assert [x['state'] for x in q.pending('p_c', c)] == ['pending']
    q.close()


def test_a_replaced_consumer_cannot_acknowledge_over_the_new_one(tmp_path):
    now = [100.0]
    q = _store(tmp_path, now)
    a = q.mailbox('p_a', 'mac/ios')['id']
    b = q.mailbox('p_b', 'backend/api')['id']
    _sent(q, 'p_a', a, b)
    old = q.acquire('p_b', Binding(b, 'r1', 's1', '/project', 'automatic'))
    now[0] += 61
    q.acquire('p_b', Binding(b, 'r2', 's2', '/project', 'automatic'))
    with pytest.raises(QueueError) as caught:
        q.receipt('p_b', b, 'm_1', old['token'], old['generation'],
                  'delivered', 'a_1')
    assert caught.value.code == 'stale_lease'
    assert [x['state'] for x in q.pending('p_b', b)] == ['pending']
    q.close()


def test_a_reservation_from_a_previous_server_run_authorises_nothing(tmp_path):
    """A restart does not hand the mailbox back to whoever held it before.

    The plugin that held this reservation may have died with the server, or may
    still be running against a socket that no longer answers; either way its
    token is from a lifetime that ended, and it has to come back and say so.
    """
    now = [100.0]
    path = tmp_path / 'q.db'
    q = QueueStore(path, clock=lambda: now[0])
    a = q.mailbox('p_a', 'mac/ios')['id']
    b = q.mailbox('p_b', 'backend/api')['id']
    _sent(q, 'p_a', a, b)
    old = q.acquire('p_b', Binding(b, 'r1', 's1', '/project', 'automatic'))
    q.close()

    q = QueueStore(path, clock=lambda: now[0])
    q.invalidate_leases()
    with pytest.raises(QueueError) as caught:
        q.renew('p_b', b, old['token'], old['generation'])
    assert caught.value.code == 'stale_lease'
    with pytest.raises(QueueError):
        q.receipt('p_b', b, 'm_1', old['token'], old['generation'],
                  'delivered', 'a_1')
    # And the mailbox is free at once, without waiting out the old expiry.
    fresh = q.acquire('p_b', Binding(b, 'r1', 's1', '/project', 'automatic'))
    assert fresh['generation'] > old['generation']
    assert [x['id'] for x in q.pending('p_b', b)] == ['m_1']
    q.close()


def test_renewing_holds_the_mailbox_and_releasing_frees_it(tmp_path):
    now = [100.0]
    q = _store(tmp_path, now)
    box = q.mailbox('p_a', 'mac/ios')['id']
    mine = Binding(box, 'r1', 's1', '/project', 'automatic')
    lease = q.acquire('p_a', mine)
    now[0] += 40
    renewed = q.renew('p_a', box, lease['token'], lease['generation'])
    assert renewed['generation'] == lease['generation']
    now[0] += 40
    # Renewal moved the expiry, so the reservation is still live here.
    with pytest.raises(QueueError):
        q.acquire('p_a', Binding(box, 'r2', 's2', '/project', 'automatic'))
    q.release('p_a', box, renewed['token'], renewed['generation'])
    taken = q.acquire('p_a', Binding(box, 'r2', 's2', '/project', 'automatic'))
    assert taken['generation'] > lease['generation']
    with pytest.raises(QueueError):
        q.renew('p_a', box, renewed['token'], renewed['generation'])
    q.close()


def test_an_acknowledged_message_keeps_its_history(tmp_path):
    now = [100.0]
    path = tmp_path / 'q.db'
    q = QueueStore(path, clock=lambda: now[0])
    a = q.mailbox('p_a', 'mac/ios')['id']
    b = q.mailbox('p_b', 'backend/api')['id']
    _sent(q, 'p_a', a, b)
    lease = q.acquire('p_b', Binding(b, 'r1', 's1', '/project', 'automatic'))
    q.receipt('p_b', b, 'm_1', lease['token'], lease['generation'],
              'delivered', 'a_1')
    now[0] += 1
    q.receipt('p_b', b, 'm_1', lease['token'], lease['generation'],
              'acknowledged', 'a_1')
    q.close()

    q = QueueStore(path, clock=lambda: now[0])
    seen = q.transitions('p_b', b, 'm_1')
    assert [t['state'] for t in seen] == ['pending', 'delivered', 'acknowledged']
    assert [t['attempt'] for t in seen] == [None, 'a_1', 'a_1']
    assert q.pending('p_b', b) == []
    q.close()


def test_a_receipt_needs_a_state_the_queue_knows(tmp_path):
    q = _store(tmp_path)
    a = q.mailbox('p_a', 'mac/ios')['id']
    b = q.mailbox('p_b', 'backend/api')['id']
    _sent(q, 'p_a', a, b)
    lease = q.acquire('p_b', Binding(b, 'r1', 's1', '/project', 'automatic'))
    with pytest.raises(QueueError) as caught:
        q.receipt('p_b', b, 'm_1', lease['token'], lease['generation'],
                  'understood', 'a_1')
    assert caught.value.code == 'invalid'
    q.close()


def test_two_senders_may_use_the_same_id_and_a_receipt_says_which(tmp_path):
    q = _store(tmp_path)
    a = q.mailbox('p_a', 'mac/ios')['id']
    c = q.mailbox('p_c', 'linux/api')['id']
    b = q.mailbox('p_b', 'backend/api')['id']
    _sent(q, 'p_a', a, b, text='from a')
    _sent(q, 'p_c', c, b, text='from c')
    lease = q.acquire('p_b', Binding(b, 'r1', 's1', '/project', 'automatic'))
    with pytest.raises(QueueError) as caught:
        q.receipt('p_b', b, 'm_1', lease['token'], lease['generation'],
                  'delivered', 'a_1')
    assert caught.value.code == 'conflict'
    q.receipt('p_b', b, 'm_1', lease['token'], lease['generation'],
              'delivered', 'a_1', sender=a)
    states = {(x['sender'], x['state']) for x in q.pending('p_b', b)}
    assert states == {(a, 'delivered'), (c, 'pending')}
    q.close()
