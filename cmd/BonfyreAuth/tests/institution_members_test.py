#!/usr/bin/env python3
"""Native authority and concurrency checks in a disposable Auth database."""
import json
import pathlib
import sqlite3
import subprocess
import tempfile
import time

binary = pathlib.Path(__file__).resolve().parents[1] / 'bonfyre-auth'
with tempfile.TemporaryDirectory(prefix='bonfyre-members-') as home:
    db = str(pathlib.Path(home) / 'auth.db')

    def call(command, *args, token='', ok=True):
        result = subprocess.run([str(binary), '--db', db, command, *args], input=token, text=True, capture_output=True)
        assert (result.returncode == 0) == ok, (command, result.stderr, result.stdout)
        return json.loads(result.stdout)

    for name in ['admin', 'member', 'reader']:
        call('http-signup', token=json.dumps(dict(email=name+'@example.test', password='team-test-password', name=name)))
    admin = call('http-login', token=json.dumps(dict(email='admin@example.test', password='team-test-password')))['session']['token']
    member = call('http-login', token=json.dumps(dict(email='member@example.test', password='team-test-password')))['session']['token']
    call('institution-grant', '--user-id', '1', '--institution', 'upper90', '--role', 'admin', '--expires-at', '2090-01-01T00:00:00Z')
    call('institution-grant', '--user-id', '3', '--institution', 'jrss', '--role', 'read')
    command = ['--institution', 'jrss', '--email', 'member@example.test']
    call('institution-member-set-stdin', *command, '--role', 'write', token=member, ok=False)
    call('institution-member-set-stdin', *command, '--role', 'write', token=admin)
    call('institution-authorize-stdin', '--institution', 'jrss', '--role', 'write', token=member)
    call('institution-authorize-stdin', '--institution', 'ai-loss', '--role', 'read', token=member, ok=False)
    rows = call('institution-members-stdin', '--institution', 'jrss', token=admin)['members']
    assert next(row for row in rows if row['user_id'] == 2)['expires_at'] == '2090-01-01T00:00:00Z'
    assert len(rows) == 2  # Parent grants are not misrepresented as editable direct grants.
    # Reviewed mutations compare the direct grant inside the write transaction.
    expected = ['--expected-role', 'write', '--expected-expiry', '2090-01-01T00:00:00Z']
    call('institution-member-set-stdin', *command, '--role', 'read', *expected, token=admin)
    stale = call('institution-member-set-stdin', *command, '--role', 'admin', *expected, token=admin, ok=False)
    assert stale['status'] == 409
    absent = call('institution-member-set-stdin', *command, '--role', 'write', '--expected-role', 'none', token=admin, ok=False)
    assert absent['status'] == 409
    call('institution-member-set-stdin', *command, '--role', 'write', '--expected-role', 'read', '--expected-expiry', '2090-01-01T00:00:00Z', token=admin)
    call('institution-member-set-stdin', '--institution', 'upper90', '--email', 'admin@example.test', '--role', 'none', token=admin, ok=False)
    call('institution-member-set-stdin', '--institution', 'bonfyre', '--email', 'member@example.test', '--role', 'admin', token=admin, ok=False)
    call('institution-member-set-stdin', '--institution', 'jrss-lookalike', '--email', 'member@example.test', '--role', 'admin', token=admin, ok=False)
    call('institution-member-set-stdin', *command, '--role', 'root', token=admin, ok=False)
    call('institution-member-set-stdin', '--institution', 'jrss', '--email', 'missing@example.test', '--role', 'read', token=admin, ok=False)
    connection = sqlite3.connect(db)
    # Role editing preserves existing expiry, including an already expired grant.
    connection.execute("UPDATE institution_grants SET expires_at='2020-01-01T00:00:00Z' WHERE user_id=2 AND institution='jrss'")
    connection.commit()
    stale = call('institution-member-set-stdin', *command, '--role', 'read', *expected, token=admin, ok=False)
    assert stale['status'] == 409
    call('institution-member-set-stdin', *command, '--role', 'read', '--expected-role', 'write', '--expected-expiry', '2020-01-01T00:00:00Z', token=admin)
    rows = call('institution-members-stdin', '--institution', 'jrss', token=admin)['members']
    current = next(row for row in rows if row['user_id'] == 2)
    assert current['expires_at'] == '2020-01-01T00:00:00Z' and not current['effective']
    # A role change on an unlimited grant remains bounded by temporary admin authority.
    connection.execute("UPDATE institution_grants SET expires_at=NULL WHERE user_id=2 AND institution='jrss'")
    connection.commit()
    call('institution-member-set-stdin', *command, '--role', 'write', '--expected-role', 'read', '--expected-expiry', '', token=admin)
    assert connection.execute("SELECT expires_at FROM institution_grants WHERE user_id=2 AND institution='jrss'").fetchone()[0] == '2090-01-01T00:00:00Z'
    # A queued review must see a competing committed change after acquiring the lock.
    connection.execute('BEGIN IMMEDIATE')
    pending_review = subprocess.Popen([str(binary), '--db', db, 'institution-member-set-stdin', *command, '--role', 'admin', *expected], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    pending_review.stdin.write(admin)
    pending_review.stdin.close()
    time.sleep(.15)
    assert pending_review.poll() is None
    connection.execute("UPDATE institution_grants SET role='read' WHERE user_id=2 AND institution='jrss'")
    connection.commit()
    pending_review.wait(timeout=5)
    assert pending_review.returncode != 0 and json.loads(pending_review.stdout.read())['status'] == 409
    assert connection.execute("SELECT role FROM institution_grants WHERE user_id=2 AND institution='jrss'").fetchone()[0] == 'read'
    connection.execute('UPDATE users SET name=? WHERE id=2', ('A "quoted" name\nwith a line',))
    connection.commit()
    rows = call('institution-members-stdin', '--institution', 'jrss', token=admin)['members']
    assert rows[0]['name'] == 'A "quoted" name\nwith a line'
    profile = call('account-profile-stdin', token=member)
    assert profile == dict(id=2, name=rows[0]['name'], email='member@example.test')
    call('institution-member-set-stdin', *command, '--role', 'none', token=admin)
    call('institution-authorize-stdin', '--institution', 'jrss', '--role', 'read', token=member, ok=False)
    # Revoked inactive rows count as absent and can be deliberately granted again.
    call('institution-member-set-stdin', *command, '--role', 'write', '--expected-role', 'none', token=admin)
    call('institution-member-set-stdin', *command, '--role', 'none', *expected, token=admin)
    # Hold the writer lock, queue an operation, then revoke the administrator.
    # Authorization must run after the lock is acquired, against the new state.
    connection.execute('BEGIN IMMEDIATE')
    pending = subprocess.Popen([str(binary), '--db', db, 'institution-member-set-stdin', *command, '--role', 'admin'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    pending.stdin.write(admin)
    pending.stdin.close()
    time.sleep(.15)
    assert pending.poll() is None, "Mutation did not wait for the writer lock"
    connection.execute("UPDATE institution_grants SET active=0 WHERE user_id=1 AND institution='upper90'")
    connection.commit()
    pending.wait(timeout=5)
    assert pending.returncode != 0
    assert connection.execute("SELECT active FROM institution_grants WHERE user_id=2 AND institution='jrss'").fetchone()[0] == 0
    call('institution-members-stdin', '--institution', 'jrss', token=admin, ok=False)
    call('institution-grant', '--user-id', '1', '--institution', 'upper90', '--role', 'admin')
    # Pagination stays inside the exact requested institution and escapes data.
    for i in range(55):
        cursor = connection.execute("INSERT INTO users(email,name,password_hash,salt,created_at) VALUES(?,?,'fixture','fixture','2026-01-01T00:00:00Z')", (f'page-{i}@example.test', f'Member {i}'))
        connection.execute("INSERT INTO institution_grants(user_id,institution,role,granted_at,active) VALUES(?,'jrss','read','2026-01-01T00:00:00Z',1)", (cursor.lastrowid,))
    connection.commit()
    first = call('institution-members-stdin', '--institution', 'jrss', token=admin)
    second = call('institution-members-stdin', '--institution', 'jrss', '--after-id', str(first['next_user_id']), token=admin)
    assert len(first['members']) == 50 and len(second['members']) == 6
    assert second['next_user_id'] is None
    assert not set(m['user_id'] for m in first['members']) & set(m['user_id'] for m in second['members'])
    call('institution-members-stdin', '--institution', 'jrss', '--after-id', 'oops', token=admin, ok=False)
    call('logout-stdin', token=admin)
    call('account-profile-stdin', token=admin, ok=False)
    call('institution-members-stdin', '--institution', 'jrss', token=admin, ok=False)
    connection.close()
print('institution members: PASS (scope, expiry preservation, stale reviews, revocation, pagination, locked mutation)')
