"""The queue over HTTP: the hub's own authentication, and nobody else's word.

The store already refuses what it should. What these tests are about is the
layer above it: that the identity acted on is the one the bearer token proves,
never one named in the JSON; that a refusal keeps the shape the client can act
on; and that a room used as an address is resolved once, at acceptance, so a
retry cannot quietly widen or narrow who was sent to.
"""

from __future__ import annotations

import pytest

QUEUE = '/ext/collab/v1/queue'


def _join(client, session, name='bob'):
    r = client.post('/ext/collab/v1/join', json={
        'invite': session['invite'], 'name': name, 'hello': {'focus': 'queues'}})
    assert r.status_code == 200, r.text
    return r.json()


def _headers(token):
    return {'Authorization': f'Bearer {token}'}


def _mailbox(client, headers, name):
    r = client.post(f'{QUEUE}/mailboxes', json={'name': name}, headers=headers)
    assert r.status_code == 200, r.text
    return r.json()['id']


def _message(sender, recipients, mid='m_1', text='diagnose', kind='request'):
    return {'id': mid, 'sender': sender, 'recipients': list(recipients),
            'kind': kind, 'text': text, 'created_at': '2026-09-10T00:00:00Z',
            'conversation': 'c_1'}


def _binding(session_id='ses_1', runtime='opencode', directory='/project',
             mode='automatic'):
    return {'runtime': runtime, 'session': session_id, 'directory': directory,
            'mode': mode}


def test_queue_requires_existing_collab_auth(client, host_headers):
    path = '/ext/collab/v1/queue/mailboxes'
    assert client.post(path, json={'name': 'mac/ios'}).status_code == 401
    r = client.post(path, json={'name': 'mac/ios'}, headers=host_headers)
    assert r.status_code == 200
    assert r.json()['name'] == 'mac/ios'


def test_a_message_travels_from_one_participant_to_the_other_and_back(
        client, session, host_headers):
    guest = _join(client, session)
    guest_headers = _headers(guest['token'])
    mine = _mailbox(client, host_headers, 'mac/ios')
    theirs = _mailbox(client, guest_headers, 'backend/api')

    sent = client.post(f'{QUEUE}/messages', json=_message(mine, [theirs]),
                       headers=host_headers)
    assert sent.status_code == 200, sent.text
    assert sent.json()['recipients'] == [{'mailbox': theirs, 'seq': 1}]

    lease = client.post(f'{QUEUE}/mailboxes/{theirs}/lease', json=_binding(),
                        headers=guest_headers)
    assert lease.status_code == 200, lease.text
    hold = lease.json()

    pending = client.get(f'{QUEUE}/mailboxes/{theirs}/pending',
                         headers=guest_headers).json()['messages']
    assert [m['id'] for m in pending] == ['m_1']
    assert pending[0]['text'] == 'diagnose'

    for state in ('delivered', 'acknowledged'):
        r = client.post(f'{QUEUE}/mailboxes/{theirs}/receipts', headers=guest_headers,
                        json={'message_id': 'm_1', 'sender': mine, 'state': state,
                              'token': hold['token'], 'generation': hold['generation'],
                              'attempt_id': 'a_1'})
        assert r.status_code == 200, r.text
        assert r.json()['state'] == state

    assert client.get(f'{QUEUE}/mailboxes/{theirs}/pending',
                      headers=guest_headers).json()['messages'] == []


def test_the_sender_is_the_token_and_not_the_json(client, session, host_headers):
    guest = _headers(_join(client, session)['token'])
    mine = _mailbox(client, host_headers, 'mac/ios')
    theirs = _mailbox(client, guest, 'backend/api')
    # The guest signs a message as the host's mailbox.
    r = client.post(f'{QUEUE}/messages', json=_message(mine, [theirs]), headers=guest)
    assert r.status_code == 403, r.text
    assert client.get(f'{QUEUE}/mailboxes/{theirs}/pending',
                      headers=guest).json()['messages'] == []


def test_one_participant_cannot_read_or_reserve_anothers_mailbox(
        client, session, host_headers):
    guest = _headers(_join(client, session)['token'])
    theirs = _mailbox(client, guest, 'backend/api')
    assert client.get(f'{QUEUE}/mailboxes/{theirs}/pending',
                      headers=host_headers).status_code == 403
    assert client.post(f'{QUEUE}/mailboxes/{theirs}/lease', json=_binding(),
                       headers=host_headers).status_code == 403


def test_a_refusal_keeps_the_code_the_client_acts_on(client, session, host_headers):
    guest = _headers(_join(client, session)['token'])
    mine = _mailbox(client, host_headers, 'mac/ios')
    theirs = _mailbox(client, guest, 'backend/api')
    client.post(f'{QUEUE}/messages', json=_message(mine, [theirs]),
                headers=host_headers)

    same_id = _message(mine, [theirs], text='deploy')
    assert client.post(f'{QUEUE}/messages', json=same_id,
                       headers=host_headers).status_code == 409
    unknown = _message(mine, ['mb_nobody'], mid='m_2')
    assert client.post(f'{QUEUE}/messages', json=unknown,
                       headers=host_headers).status_code == 404
    shouted = _message(mine, [theirs], mid='m_3')
    shouted['kind'] = 'shout'
    assert client.post(f'{QUEUE}/messages', json=shouted,
                       headers=host_headers).status_code == 400
    assert client.post(f'{QUEUE}/messages', json={'nonsense': True},
                       headers=host_headers).status_code == 400
    assert client.get(f'{QUEUE}/mailboxes/mb_nobody/pending',
                      headers=host_headers).status_code == 404


def test_a_second_session_is_told_who_holds_the_mailbox(client, session, host_headers):
    mine = _mailbox(client, host_headers, 'mac/ios')
    first = client.post(f'{QUEUE}/mailboxes/{mine}/lease', json=_binding('ses_1'),
                        headers=host_headers)
    assert first.status_code == 200
    second = client.post(f'{QUEUE}/mailboxes/{mine}/lease', json=_binding('ses_2'),
                         headers=host_headers)
    assert second.status_code == 409, second.text
    hold = first.json()
    renewed = client.post(f'{QUEUE}/mailboxes/{mine}/renew', headers=host_headers,
                          json={'token': hold['token'],
                                'generation': hold['generation']})
    assert renewed.status_code == 200
    stale = client.post(f'{QUEUE}/mailboxes/{mine}/renew', headers=host_headers,
                        json={'token': 'not-the-token', 'generation': 1})
    assert stale.status_code == 409
    freed = client.post(f'{QUEUE}/mailboxes/{mine}/release', headers=host_headers,
                        json={'token': hold['token'],
                              'generation': hold['generation']})
    assert freed.status_code == 200
    assert client.post(f'{QUEUE}/mailboxes/{mine}/lease', json=_binding('ses_2'),
                       headers=host_headers).status_code == 200


def test_a_room_addresses_the_people_in_it_at_the_moment_it_is_accepted(
        client, session, host_headers):
    guest = _headers(_join(client, session)['token'])
    mine = _mailbox(client, host_headers, 'mac/ios')
    theirs = _mailbox(client, guest, 'backend/api')
    body = _message(mine, [])
    body.pop('recipients')
    body['room'] = 'general'
    sent = client.post(f'{QUEUE}/messages', json=body, headers=host_headers)
    assert sent.status_code == 200, sent.text
    # The sender's own mailbox is not one of its recipients.
    assert [r['mailbox'] for r in sent.json()['recipients']] == [theirs]

    # A third participant arrives with a mailbox of their own. The retry is the
    # same message, so it is answered with the snapshot taken the first time.
    late = _headers(_join(client, session, name='carol')['token'])
    _mailbox(client, late, 'linux/api')
    again = client.post(f'{QUEUE}/messages', json=body, headers=host_headers)
    assert again.status_code == 200
    assert again.json() == sent.json()


def test_a_message_names_one_destination_or_none(client, session, host_headers):
    guest = _headers(_join(client, session)['token'])
    mine = _mailbox(client, host_headers, 'mac/ios')
    theirs = _mailbox(client, guest, 'backend/api')
    both = _message(mine, [theirs])
    both['room'] = 'general'
    assert client.post(f'{QUEUE}/messages', json=both,
                       headers=host_headers).status_code == 400
    empty = _message(mine, [], mid='m_2')
    assert client.post(f'{QUEUE}/messages', json=empty,
                       headers=host_headers).status_code == 400
    missing_room = _message(mine, [], mid='m_3')
    missing_room.pop('recipients')
    missing_room['room'] = 'nowhere'
    assert client.post(f'{QUEUE}/messages', json=missing_room,
                       headers=host_headers).status_code == 404


def test_closing_a_room_leaves_its_messages_where_they_are(
        client, session, host_headers):
    guest = _headers(_join(client, session)['token'])
    mine = _mailbox(client, host_headers, 'mac/ios')
    theirs = _mailbox(client, guest, 'backend/api')
    body = _message(mine, [])
    body.pop('recipients')
    body['room'] = 'general'
    assert client.post(f'{QUEUE}/messages', json=body,
                       headers=host_headers).status_code == 200
    session['store']._db.execute("DELETE FROM rooms WHERE name='general'")
    session['store']._db.commit()
    pending = client.get(f'{QUEUE}/mailboxes/{theirs}/pending',
                         headers=guest).json()['messages']
    assert [m['id'] for m in pending] == ['m_1']


def test_a_reservation_from_before_the_restart_is_not_honoured(
        client, session, host_headers, tmp_path):
    """The hub comes back; the plugin that held the mailbox has to say so again."""
    from fastapi.testclient import TestClient

    mine = _mailbox(client, host_headers, 'mac/ios')
    hold = client.post(f'{QUEUE}/mailboxes/{mine}/lease', json=_binding(),
                       headers=host_headers).json()
    with TestClient(session['app']) as restarted:
        stale = restarted.post(f'{QUEUE}/mailboxes/{mine}/renew',
                               headers=host_headers,
                               json={'token': hold['token'],
                                     'generation': hold['generation']})
        assert stale.status_code == 409, stale.text
        again = restarted.post(f'{QUEUE}/mailboxes/{mine}/lease', json=_binding(),
                               headers=host_headers)
        assert again.status_code == 200
        assert again.json()['generation'] > hold['generation']


def test_the_status_of_a_mailbox_counts_what_is_on_the_disk(
        client, session, host_headers):
    guest = _headers(_join(client, session)['token'])
    mine = _mailbox(client, host_headers, 'mac/ios')
    theirs = _mailbox(client, guest, 'backend/api')
    client.post(f'{QUEUE}/messages', json=_message(mine, [theirs]),
                headers=host_headers)
    client.post(f'{QUEUE}/messages', json=_message(mine, [theirs], mid='m_2'),
                headers=host_headers)
    hold = client.post(f'{QUEUE}/mailboxes/{theirs}/lease', json=_binding(),
                       headers=guest).json()
    client.post(f'{QUEUE}/mailboxes/{theirs}/receipts', headers=guest,
                json={'message_id': 'm_1', 'sender': mine, 'state': 'delivered',
                      'token': hold['token'], 'generation': hold['generation'],
                      'attempt_id': 'a_1'})
    status = client.get(f'{QUEUE}/mailboxes/{theirs}/status', headers=guest)
    assert status.status_code == 200, status.text
    body = status.json()
    assert body['counts'] == {'pending': 1, 'delivered': 1, 'acknowledged': 0}
    assert body['lease']['session'] == 'ses_1'
    assert body['oldest_pending_at']
    assert 'token' not in status.text


def test_a_body_larger_than_the_queue_carries_is_refused_not_read(
        client, session, host_headers):
    mine = _mailbox(client, host_headers, 'mac/ios')
    huge = _message(mine, [mine], text='x' * 300_000)
    r = client.post(f'{QUEUE}/messages', json=huge, headers=host_headers)
    assert r.status_code == 413, r.status_code


def test_the_queue_outlives_a_reopened_database(client, session, host_headers,
                                                tmp_path):
    from collab.messaging.store import QueueStore

    mine = _mailbox(client, host_headers, 'mac/ios')
    client.post(f'{QUEUE}/messages', json=_message(mine, [mine]),
                headers=host_headers)
    owner = session['store'].participants()[0].id
    reopened = QueueStore(session['store'].path)
    assert [m['id'] for m in reopened.pending(owner, mine)] == ['m_1']
    reopened.close()


def test_waiting_for_work_is_one_held_request_and_not_a_loop(client, session,
                                                             host_headers):
    """Nothing arrives: the request waits, and comes back empty rather than cut."""
    import time

    mine = _mailbox(client, host_headers, 'mac/ios')
    started = time.monotonic()
    empty = client.get(f'{QUEUE}/mailboxes/{mine}/pending?wait=0.6',
                       headers=host_headers)
    waited = time.monotonic() - started
    assert empty.status_code == 200
    assert empty.json()['messages'] == []
    assert waited >= 0.5, waited

    client.post(f'{QUEUE}/messages', json=_message(mine, [mine]),
                headers=host_headers)
    started = time.monotonic()
    full = client.get(f'{QUEUE}/mailboxes/{mine}/pending?wait=10',
                      headers=host_headers)
    assert [m['id'] for m in full.json()['messages']] == ['m_1']
    assert time.monotonic() - started < 2.0


def test_a_hold_longer_than_the_server_offers_is_shortened_not_refused(
        client, session, host_headers):
    import time

    from collab.messaging.routes import MAX_HOLD_SECONDS

    assert MAX_HOLD_SECONDS <= 60
    mine = _mailbox(client, host_headers, 'mac/ios')
    started = time.monotonic()
    r = client.get(f'{QUEUE}/mailboxes/{mine}/pending?wait=0.4',
                   headers=host_headers)
    assert r.status_code == 200 and time.monotonic() - started < 5
