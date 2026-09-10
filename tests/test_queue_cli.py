"""`collab queue` from the outside, and the bridge the plugin speaks through.

The subprocess test is the one that matters most: it is the only one here that
proves the promise a person actually relies on — that `queue send` with the
server switched off says «queued» and means it, in a real process, with a real
exit code.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from collab.cli import build_parser
from collab.config import SessionProfile
from collab.messaging import cli as queue_cli
from collab.messaging.local import LocalStore
from collab.messaging.model import QueueError


@pytest.fixture()
def home(tmp_path, monkeypatch):
    """An isolated config, home and peer registry, with a profile in it."""
    monkeypatch.setenv('COLLAB_CONFIG', str(tmp_path / 'cfg' / 'config.json'))
    monkeypatch.setenv('COLLAB_PEERS_DIR', str(tmp_path / 'peers'))
    monkeypatch.setenv('COLLAB_HOME', str(tmp_path / 'home'))
    (tmp_path / 'home' / 'sessions' / 's_1').mkdir(parents=True)
    profile = SessionProfile(session_id='s_1', url='http://127.0.0.1:1', name='mac',
                             host_name='mac', token='tok', is_host=True,
                             home=str(tmp_path / 'home'))
    profile.save()
    return {'root': tmp_path, 'profile': profile,
            'env': {'COLLAB_CONFIG': str(tmp_path / 'cfg' / 'config.json'),
                    'COLLAB_PEERS_DIR': str(tmp_path / 'peers'),
                    'COLLAB_HOME': str(tmp_path / 'home'),
                    'COLLAB_NO_UPDATE_CHECK': '1'}}


def _run(*argv):
    args = build_parser().parse_args(['queue', *argv])
    return args.func(args)


def test_the_parser_knows_the_queue_and_its_actions():
    parser = build_parser()
    for action in ('configure', 'send', 'status', 'bridge', 'bind', 'pause',
                   'resume', 'retry'):
        parsed = parser.parse_args(['queue', action])
        assert parsed.action == action


def test_configure_is_explicit_about_where_and_as_whom(home, capsys):
    assert _run('configure') == 1
    assert 'server' in capsys.readouterr().err

    assert _run('configure', '--server', 'http://127.0.0.1:1', '--profile', 's_1',
                '--identity', 'mac/ios', '--mode', 'automatic') == 0
    saved = json.loads((home['root'] / 'cfg' / 'queue' / 'config.json').read_text())
    assert saved['server'] == 'http://127.0.0.1:1'
    assert saved['identity'] == 'mac/ios'
    assert saved['mode'] == 'automatic'
    # The credential is a REFERENCE to the session profile, never a copy of it.
    assert 'tok' not in json.dumps(saved)
    assert saved['profile'] == 's_1'


def test_configure_refuses_a_profile_that_is_not_there(home, capsys):
    assert _run('configure', '--server', 'http://h', '--profile', 'nope',
                '--identity', 'mac/ios', '--mode', 'automatic') == 1
    assert 'nope' in capsys.readouterr().err


def test_configure_refuses_a_mode_it_does_not_have(home):
    with pytest.raises(SystemExit):
        build_parser().parse_args(['queue', 'configure', '--mode', 'telepathy'])


def test_send_with_the_server_down_says_queued_and_means_it(home):
    """The subprocess case: a real exit code, and a real file afterwards."""
    _run('configure', '--server', 'http://127.0.0.1:1', '--profile', 's_1',
         '--identity', 'mac/ios', '--mode', 'automatic')
    env = {**os.environ, **home['env']}
    done = subprocess.run(
        [sys.executable, '-m', 'collab.cli', 'queue', 'send', '--to', 'mb_target',
         'diagnose the second turn'],
        capture_output=True, text=True, env=env, timeout=120)
    assert done.returncode == 0, done.stderr
    assert 'queued' in done.stdout.lower()

    outbox = LocalStore(home['root'] / 'cfg' / 'queue' / 'local.db')
    rows = outbox.history()
    assert [r['state'] for r in rows] == ['queued']
    assert rows[0]['message']['text'] == 'diagnose the second turn'
    assert rows[0]['message']['recipients'] == ['mb_target']
    assert rows[0]['attempts'] == 1, 'it tried, and kept the message'
    outbox.close()


def test_send_needs_somewhere_to_send_it(home, capsys):
    _run('configure', '--server', 'http://127.0.0.1:1', '--profile', 's_1',
         '--identity', 'mac/ios', '--mode', 'automatic')
    assert _run('send', 'text with no destination') == 1
    assert '--to' in capsys.readouterr().err


def test_send_before_configure_says_what_to_do(home, capsys):
    assert _run('send', '--to', 'mb_x', 'hello') == 1
    assert 'collab queue configure' in capsys.readouterr().err


def test_status_reads_the_database_and_prints_no_credential(home, capsys):
    _run('configure', '--server', 'http://127.0.0.1:1', '--profile', 's_1',
         '--identity', 'mac/ios', '--mode', 'automatic')
    _run('send', '--to', 'mb_target', 'one')
    capsys.readouterr()          # what `send` said is not what is under test
    assert _run('status', '--json') == 0
    printed = json.loads(capsys.readouterr().out)
    assert printed['outbox']['queued'] == 1
    assert printed['server'] == 'http://127.0.0.1:1'
    assert 'tok' not in json.dumps(printed)
    assert _run('status') == 0
    assert 'queued' in capsys.readouterr().out


def test_a_message_that_looks_like_a_command_is_carried_as_text(home):
    _run('configure', '--server', 'http://127.0.0.1:1', '--profile', 's_1',
         '--identity', 'mac/ios', '--mode', 'automatic')
    hostile = '$(rm -rf ~); `whoami`; "; DROP TABLE queue_outbox; --'
    _run('send', '--to', 'mb_target', hostile)
    outbox = LocalStore(home['root'] / 'cfg' / 'queue' / 'local.db')
    assert outbox.history()[0]['message']['text'] == hostile
    outbox.close()


# --- the bridge ---------------------------------------------------------------

def _bridge(home, lines, monkeypatch=None):
    """Run the bridge over a canned stdin and return the replies it wrote."""
    import io

    stdin = io.BytesIO(b''.join(json.dumps(line).encode() + b'\n' for line in lines))
    stdout = io.BytesIO()
    stderr = io.StringIO()
    queue_cli.run_bridge(stdin=stdin, stdout=stdout, stderr=stderr,
                         notifications=False)
    replies = [json.loads(l) for l in stdout.getvalue().splitlines() if l.strip()]
    return replies, stderr.getvalue()


def _configured(home):
    _run('configure', '--server', 'http://127.0.0.1:1', '--profile', 's_1',
         '--identity', 'mac/ios', '--mode', 'automatic')


def test_the_bridge_answers_each_request_by_its_id(home):
    _configured(home)
    replies, _ = _bridge(home, [
        {'id': '1', 'method': 'status', 'params': {}},
        {'id': '2', 'method': 'send',
         'params': {'to': 'mb_target', 'kind': 'request', 'text': 'hello'}},
    ])
    assert [r['id'] for r in replies] == ['1', '2']
    assert replies[0]['result']['outbox'] == {}
    assert replies[1]['result']['state'] == 'queued'


def test_the_bridge_refuses_what_it_does_not_understand(home):
    _configured(home)
    replies, noise = _bridge(home, [
        {'id': '1', 'method': 'levitate', 'params': {}},
        {'id': '2', 'method': 'send', 'params': {'text': 'nowhere'}},
    ])
    assert replies[0]['error']['code'] == 'invalid'
    assert 'levitate' in replies[0]['error']['detail']
    assert replies[1]['error']['code'] == 'invalid'
    assert 'tok' not in noise


def test_the_bridge_survives_a_line_that_is_not_json(home):
    _configured(home)
    import io
    stdin = io.BytesIO(b'{not json at all\n' +
                       json.dumps({'id': '2', 'method': 'status',
                                   'params': {}}).encode() + b'\n')
    stdout = io.BytesIO()
    queue_cli.run_bridge(stdin=stdin, stdout=stdout, stderr=io.StringIO(),
                         notifications=False)
    replies = [json.loads(l) for l in stdout.getvalue().splitlines() if l.strip()]
    assert replies[0]['error']['code'] == 'invalid'
    assert replies[1]['id'] == '2' and 'result' in replies[1]


def test_the_bridge_refuses_a_line_longer_than_it_will_read(home):
    _configured(home)
    import io
    huge = json.dumps({'id': '1', 'method': 'send',
                       'params': {'to': 'mb_t', 'text': 'x' * (300 * 1024)}})
    stdin = io.BytesIO(huge.encode() + b'\n' +
                       json.dumps({'id': '2', 'method': 'status',
                                   'params': {}}).encode() + b'\n')
    stdout = io.BytesIO()
    queue_cli.run_bridge(stdin=stdin, stdout=stdout, stderr=io.StringIO(),
                         notifications=False)
    replies = [json.loads(l) for l in stdout.getvalue().splitlines() if l.strip()]
    assert replies[0]['error']['code'] == 'invalid'
    # And the line after the oversized one is still read as its own request.
    assert replies[-1]['id'] == '2'


def test_the_bridge_leaves_the_outbox_behind_when_its_reader_goes_away(home):
    _configured(home)
    replies, _ = _bridge(home, [
        {'id': '1', 'method': 'send',
         'params': {'to': 'mb_target', 'kind': 'request', 'text': 'still here'}},
    ])
    assert replies[0]['result']['state'] == 'queued'
    outbox = LocalStore(home['root'] / 'cfg' / 'queue' / 'local.db')
    assert [r['message']['text'] for r in outbox.history()] == ['still here']
    outbox.close()


def test_the_bridge_records_an_attempt_before_it_is_delivered(home):
    _configured(home)
    binding = {'mailbox': 'mb_mine', 'runtime': 'opencode', 'session': 'ses_1',
               'directory': '/project', 'mode': 'automatic'}
    replies, _ = _bridge(home, [
        {'id': '1', 'method': 'bind_local', 'params': binding},
        {'id': '2', 'method': 'begin_attempt',
         'params': {**binding, 'message_ids': ['m1', 'm2']}},
        {'id': '3', 'method': 'finish_attempt',
         'params': {'attempt_id': '<from 2>', 'state': 'delivered',
                    'opencode_message_id': 'msg_1'}},
    ])
    assert replies[1]['result']['state'] == 'prepared'
    assert replies[1]['result']['marker'].startswith('[collab-delivery:')
    # The third asked about an attempt that does not exist, and said so rather
    # than closing something at random.
    assert replies[2]['error']['code'] == 'not_found'


def test_pause_and_resume_are_recorded_where_a_restart_will_read_them(home):
    _configured(home)
    binding = {'mailbox': 'mb_mine', 'runtime': 'opencode', 'session': 'ses_1',
               'directory': '/project', 'mode': 'automatic'}
    replies, _ = _bridge(home, [
        {'id': '1', 'method': 'bind_local', 'params': binding},
        {'id': '2', 'method': 'pause', 'params': {'mailbox': 'mb_mine',
                                                  'reason': 'ten turns'}},
        {'id': '3', 'method': 'status', 'params': {}},
        {'id': '4', 'method': 'resume', 'params': {'mailbox': 'mb_mine'}},
        {'id': '5', 'method': 'status', 'params': {}},
    ])
    assert replies[2]['result']['bindings'][0]['paused'] is True
    assert replies[4]['result']['bindings'][0]['paused'] is False


# --- the loop that pushes pending work ----------------------------------------

class _Stopper:
    """A stop flag that ends the loop after a fixed number of waits."""

    def __init__(self, rounds=1):
        self.rounds = rounds
        self.waits = []
        self._set = False

    def is_set(self):
        return self._set

    def set(self):
        self._set = True

    def wait(self, seconds):
        self.waits.append(seconds)
        self.rounds -= 1
        if self.rounds <= 0:
            self._set = True
        return self._set


def _bound(home, mailbox='mb_mine', paused=False):
    from collab.messaging.model import Binding

    local = LocalStore(home['root'] / 'cfg' / 'queue' / 'local.db')
    local.bind('http://127.0.0.1:1',
               Binding(mailbox, 'opencode', 'ses_1', '/project', 'automatic'))
    if paused:
        local.pause('http://127.0.0.1:1', mailbox, 'by hand')
    return local


def test_pending_work_is_pushed_as_it_arrives_without_anybody_polling(home):
    _configured(home)
    local = _bound(home)
    written = []

    class Client:
        def __init__(self):
            self.waits = []

        def poll(self, binding, wait=0.0):
            self.waits.append(wait)
            return [{'id': 'm1', 'sender': 'mb_them', 'kind': 'request',
                     'text': 'hello', 'seq': 1, 'state': 'pending'}]

    client = Client()
    stop = _Stopper(rounds=1)
    queue_cli.notify_loop(client, local, written.append, stop, hold_seconds=25.0)
    assert written[0]['method'] == 'pending'
    assert [r['id'] for r in written[0]['params']['records']] == ['m1']
    assert client.waits[0] == 25.0, 'the server held the request rather than being asked again'
    local.close()


def test_a_paused_binding_is_left_alone(home):
    _configured(home)
    local = _bound(home, paused=True)
    written = []

    class Client:
        def poll(self, binding, wait=0.0):
            raise AssertionError('a paused binding must not be consumed')

    stop = _Stopper(rounds=1)
    queue_cli.notify_loop(Client(), local, written.append, stop)
    assert written == []
    local.close()


def test_a_server_that_is_down_is_asked_for_less_and_less_often(home):
    _configured(home)
    local = _bound(home)
    seen = []

    class Client:
        def poll(self, binding, wait=0.0):
            raise QueueError('unavailable', 'connection refused')

    stop = _Stopper(rounds=4)
    queue_cli.notify_loop(Client(), local, lambda payload: None, stop,
                          diagnostic=seen.append)
    assert seen == ['unavailable'] * 4
    assert stop.waits == sorted(stop.waits), stop.waits
    assert max(stop.waits) <= 60.0
    local.close()


def test_a_credential_or_a_lost_reservation_is_told_to_the_plugin(home):
    _configured(home)
    local = _bound(home)
    written = []

    class Client:
        def poll(self, binding, wait=0.0):
            raise QueueError('stale_lease', 'somebody else holds it')

    stop = _Stopper(rounds=1)
    queue_cli.notify_loop(Client(), local, written.append, stop)
    assert written[0]['method'] == 'blocked'
    assert written[0]['params']['code'] == 'stale_lease'
    local.close()


def test_the_reservation_is_renewed_while_the_loop_runs(home):
    _configured(home)
    local = _bound(home)
    renewals = []

    class Client:
        def renew(self, mailbox, hold):
            renewals.append((mailbox, hold['token']))
            return hold

        def poll(self, binding, wait=0.0):
            return []

    stop = _Stopper(rounds=1)
    queue_cli.notify_loop(Client(), local, lambda payload: None, stop,
                          holds={'mb_mine': {'token': 't1', 'generation': 1}})
    assert renewals == [('mb_mine', 't1')]
    local.close()


def test_the_config_says_where_the_profile_lives_not_only_its_id(home, monkeypatch,
                                                                tmp_path):
    """The bridge runs wherever the editor is, not where you configured it.

    `SessionProfile.load` walks up from the current directory to find a repo's
    `.collab`. The plugin starts `collab queue bridge` in the project directory
    of the OpenCode session, which is somebody else's repository or none at
    all — so the profile is found by the path written down here, or not at all.
    """
    _run('configure', '--server', 'http://127.0.0.1:1', '--profile', 's_1',
         '--identity', 'mac/ios', '--mode', 'automatic')
    saved = json.loads((home['root'] / 'cfg' / 'queue' / 'config.json').read_text())
    assert saved['home'] == str(home['root'] / 'home')
    assert 'tok' not in json.dumps(saved), 'the path, never the credential'

    elsewhere = tmp_path / 'some' / 'other' / 'project'
    elsewhere.mkdir(parents=True)
    monkeypatch.chdir(elsewhere)
    monkeypatch.delenv('COLLAB_HOME', raising=False)
    found = queue_cli._profile(saved['profile'], saved.get('home'))
    assert found is not None and found.token == 'tok'
    # And without the path, from here, it is not found at all — which is the
    # failure this test exists to keep fixed.
    assert queue_cli._profile(saved['profile']) is None


def test_resuming_says_so_and_announces_the_pending_again(home):
    """A person resumes in a terminal; the plugin is a different process.

    It holds its own «paused», and nothing in this loop can reach into it — so
    the lifting of the pause is said out loud, and what was announced before it
    is forgotten so that the same pending work is news again.
    """
    written = []
    state = {'paused': True}

    class Bindings:
        """The outbox as this loop reads it, with a pause lifted mid-run."""

        def bindings(self):
            return [{'server': 'http://127.0.0.1:1', 'mailbox': 'mb_mine',
                     'runtime': 'opencode', 'session': 'ses_1',
                     'directory': '/project', 'mode': 'automatic',
                     'turns': 0, 'paused': state['paused'],
                     'paused_reason': None}]

    class Client:
        def poll(self, binding, wait=0.0):
            return [{'id': 'm1', 'sender': 'mb_them', 'kind': 'request',
                     'text': 'hello', 'seq': 1, 'state': 'pending'}]

    class Waking(_Stopper):
        """Lifts the pause after the first idle wait, and stops after the next."""

        def wait(self, seconds):
            state['paused'] = False
            return super().wait(seconds)

    queue_cli.notify_loop(Client(), Bindings(), written.append, Waking(rounds=2))
    assert [w['method'] for w in written] == ['resumed', 'pending']
    assert written[0]['params']['mailbox'] == 'mb_mine'
    assert [r['id'] for r in written[1]['params']['records']] == ['m1']


# --- acknowledging from the agent's own shell ---------------------------------

def test_ack_refuses_an_id_this_machine_never_delivered(home, capsys):
    from collab.messaging.model import Binding

    _configured(home)
    local = LocalStore(home['root'] / 'cfg' / 'queue' / 'local.db')
    local.bind('http://127.0.0.1:1',
               Binding('mb_mine', 'claude', 'ses-1', '/project', 'automatic'))
    local.close()
    assert _run('ack', '--id', 'm_never') == 1
    assert 'not delivered' in capsys.readouterr().err


def test_ack_records_what_the_agent_says_it_has(home, capsys):
    from collab.messaging.model import Binding

    _configured(home)
    local = LocalStore(home['root'] / 'cfg' / 'queue' / 'local.db')
    binding = Binding('mb_mine', 'claude', 'ses-1', '/project', 'automatic')
    local.bind('http://127.0.0.1:1', binding)
    local.begin_attempt(binding, ['m1', 'm2'])
    local.close()

    assert _run('ack', '--id', 'm1', '--id', 'm2') == 0
    assert 'm1' in capsys.readouterr().out
    local = LocalStore(home['root'] / 'cfg' / 'queue' / 'local.db')
    assert [a['message_id'] for a in local.pending_acks('mb_mine')] == ['m1', 'm2']
    local.close()


def test_ack_with_nothing_named_says_what_to_name(home, capsys):
    _configured(home)
    assert _run('ack') == 1
    assert '--id' in capsys.readouterr().err


def test_deliver_needs_a_pane_and_a_binding(home, capsys):
    _configured(home)
    assert _run('deliver') == 1
    assert '--pane' in capsys.readouterr().err
    assert _run('deliver', '--pane', '%3') == 1
    assert 'bound' in capsys.readouterr().err


def test_ack_is_noted_even_when_the_server_cannot_be_told(home, capsys):
    """The ask outlives the attempt to deliver it."""
    from collab.messaging.model import Binding

    _configured(home)
    local = LocalStore(home['root'] / 'cfg' / 'queue' / 'local.db')
    binding = Binding('mb_mine', 'claude', 'ses-1', '/project', 'automatic')
    local.bind('http://127.0.0.1:1', binding)
    local.begin_attempt(binding, ['m1'])
    local.close()

    assert _run('ack', '--id', 'm1') == 0
    assert 'noted' in capsys.readouterr().out
    local = LocalStore(home['root'] / 'cfg' / 'queue' / 'local.db')
    assert [a['message_id'] for a in local.pending_acks('mb_mine')] == ['m1']
    local.close()
