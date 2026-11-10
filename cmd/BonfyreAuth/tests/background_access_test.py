#!/usr/bin/env python3
"""Fresh background projections use the same Auth and CMS policy as sessions."""
import os
import sqlite3
import struct
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

BINARY = Path(os.environ.get('BONFYRE_AUTH_TEST_BINARY', Path(__file__).resolve().parents[1] / 'bonfyre-auth'))


def unpack(data):
    offset = 0

    def take(size):
        nonlocal offset
        value = data[offset:offset + size]
        if len(value) != size:
            raise ValueError('Truncated native packet')
        offset += size
        return value

    def value():
        tag = take(1)[0]
        if tag < 128:
            return tag
        if tag in (0xc2, 0xc3):
            return tag == 0xc3
        if 0xa0 <= tag <= 0xbf:
            return take(tag & 31).decode()
        if 0x90 <= tag <= 0x9f:
            return [value() for _ in range(tag & 15)]
        if 0x80 <= tag <= 0x8f:
            return {value(): value() for _ in range(tag & 15)}
        if tag in (0xcc, 0xcd, 0xce, 0xcf):
            return int.from_bytes(take(1 << (tag - 0xcc)), 'big')
        if tag in (0xd0, 0xd1, 0xd2, 0xd3):
            return int.from_bytes(take(1 << (tag - 0xd0)), 'big', signed=True)
        if tag in (0xd9, 0xda, 0xdb):
            return take(int.from_bytes(take(1 << (tag - 0xd9)), 'big')).decode()
        raise ValueError('Unexpected native packet tag')

    result = value()
    if offset != len(data):
        raise ValueError('Trailing native data')
    return result


class BackgroundAccess(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='bonfyre-background-test-')
        root = Path(self.directory.name)
        self.auth, self.cms = root / 'auth.db', root / 'cms.db'
        with sqlite3.connect(self.auth) as db:
            db.executescript('''
                CREATE TABLE users(id INTEGER PRIMARY KEY,active INTEGER NOT NULL);
                INSERT INTO users VALUES(1,1),(2,1),(3,0);
                CREATE TABLE sessions(token TEXT PRIMARY KEY,user_id INTEGER,expires_at TEXT,active INTEGER);
                CREATE TABLE institution_grants(user_id INTEGER,institution TEXT,role TEXT,expires_at TEXT,active INTEGER,UNIQUE(user_id,institution));
                INSERT INTO institution_grants VALUES(1,'upper90','write',NULL,1),(2,'ai-loss','read',NULL,1),(3,'bonfyre','admin',NULL,1);
            ''')
        with sqlite3.connect(self.cms) as db:
            db.executescript('''
                CREATE TABLE household(id INTEGER PRIMARY KEY,namespace TEXT,household_id TEXT,status TEXT,steward TEXT);
                INSERT INTO household VALUES(1,'root','home','published','user:1');
                CREATE TABLE _household_authority(household_row INTEGER PRIMARY KEY,epoch INTEGER);
                INSERT INTO _household_authority VALUES(1,7);
                CREATE TABLE _household_membership(household_row INTEGER,member TEXT,authority_epoch INTEGER,role TEXT,expires_at INTEGER);
                INSERT INTO _household_membership VALUES(1,'user:2',7,'read',0);
            ''')

    def tearDown(self):
        self.directory.cleanup()

    def call(self, actor='user:1', subject=None, scope='self', permission='read', auth=None, cms=None):
        subject = actor if subject is None else subject
        process = subprocess.run([str(BINARY), '--db', str(auth or self.auth), 'background-access',
                                  '--actor', actor, '--subject', subject, '--scope', scope,
                                  '--permission', permission, '--cms-db', str(cms or self.cms)],
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=6)
        result = unpack(process.stdout)
        self.assertEqual(result[:1], [1])
        self.assertEqual(process.returncode == 0, result[1])
        if result[1]:
            self.assertEqual({k: result[2][k] for k in ('actor', 'subject', 'scope', 'permission')},
                             dict(actor=actor, subject=subject, scope=scope, permission=permission))
            self.assertLess(abs(time.time() - result[2]['checked_at']), 3)
        return result

    def allowed(self, **kwargs):
        result = self.call(**kwargs)
        self.assertTrue(result[1], result)
        return result[2]['allowed']

    def test_active_self_has_read_and_write_without_retained_session(self):
        self.assertTrue(self.allowed())
        self.assertTrue(self.allowed(permission='write'))
        with sqlite3.connect(self.auth) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM sessions').fetchone()[0], 0)

    def test_inactive_or_unknown_user_never_authorized(self):
        self.assertFalse(self.allowed(actor='user:3'))
        self.assertFalse(self.allowed(actor='user:42'))
        self.assertFalse(self.allowed(actor='user:3', subject='institution:ai-loss', scope='institution'))

    def test_institution_ancestry_and_role_use_generated_policy(self):
        self.assertTrue(self.allowed(subject='institution:ai-loss', scope='institution', permission='write'))
        self.assertTrue(self.allowed(subject='institution:jrss', scope='institution', permission='read'))
        self.assertFalse(self.allowed(subject='institution:bonfyre', scope='institution'))
        self.assertFalse(self.allowed(subject='institution:nightgrid', scope='institution'))
        self.assertTrue(self.allowed(actor='user:2', subject='institution:ai-loss', scope='institution'))
        self.assertFalse(self.allowed(actor='user:2', subject='institution:ai-loss', scope='institution', permission='write'))
        self.assertFalse(self.allowed(actor='user:2', subject='institution:upper90', scope='institution'))
        self.assertFalse(self.allowed(actor='user:2', subject='institution:jrss', scope='institution'))

    def test_revocation_and_expiry_are_fresh(self):
        self.assertTrue(self.allowed(subject='institution:ai-loss', scope='institution'))
        with sqlite3.connect(self.auth) as db:
            db.execute("UPDATE institution_grants SET active=0 WHERE user_id=1")
        self.assertFalse(self.allowed(subject='institution:ai-loss', scope='institution'))
        with sqlite3.connect(self.auth) as db:
            db.execute("UPDATE institution_grants SET active=1,expires_at='2000-01-01T00:00:00Z' WHERE user_id=1")
        self.assertFalse(self.allowed(subject='institution:ai-loss', scope='institution'))
        with sqlite3.connect(self.auth) as db:
            db.execute('UPDATE users SET active=0 WHERE id=1')
        self.assertFalse(self.allowed())

    def test_household_steward_and_current_member_role(self):
        self.assertTrue(self.allowed(subject='household:home', scope='household', permission='write'))
        self.assertTrue(self.allowed(actor='user:2', subject='household:home', scope='household'))
        self.assertFalse(self.allowed(actor='user:2', subject='household:home', scope='household', permission='write'))
        with sqlite3.connect(self.cms) as db:
            db.execute("UPDATE _household_membership SET role='write'")
        self.assertTrue(self.allowed(actor='user:2', subject='household:home', scope='household', permission='write'))

    def test_household_epoch_and_member_expiry(self):
        with sqlite3.connect(self.cms) as db:
            db.execute('UPDATE _household_authority SET epoch=8')
        self.assertFalse(self.allowed(actor='user:2', subject='household:home', scope='household'))
        with sqlite3.connect(self.cms) as db:
            db.execute('UPDATE _household_membership SET authority_epoch=8,expires_at=1')
        self.assertFalse(self.allowed(actor='user:2', subject='household:home', scope='household'))

    def test_household_cannot_borrow_institution_membership(self):
        with sqlite3.connect(self.auth) as db:
            db.execute("INSERT INTO institution_grants VALUES(2,'home','admin',NULL,1)")
        with sqlite3.connect(self.cms) as db:
            db.execute('DELETE FROM _household_membership')
        self.assertFalse(self.allowed(actor='user:2', subject='household:home', scope='household'))
        self.assertFalse(self.allowed(subject='institution:home', scope='institution'))

    def test_invalid_bindings_are_refused(self):
        for fields in [dict(actor='user:01'), dict(actor='user:2147483648'), dict(subject='user:2'),
                       dict(permission='admin'), dict(scope='public'),
                       dict(scope='institution', subject='household:home')]:
            with self.subTest(fields=fields):
                self.assertEqual(self.call(**fields)[1:], [False, {'code': 'invalid', 'message': 'The native background access check could not complete.'}])

    def test_unavailable_database_is_not_deny_or_created_database(self):
        missing = Path(self.directory.name) / 'missing.db'
        self.assertFalse(self.call(auth=missing)[1])
        self.assertFalse(missing.exists())
        self.assertFalse(self.call(subject='household:home', scope='household', cms=missing)[1])
        self.assertFalse(missing.exists())

    def test_read_projection_leaves_schema_and_rows_unchanged(self):
        before = self.auth.read_bytes(), self.cms.read_bytes()
        self.assertTrue(self.allowed(subject='institution:ai-loss', scope='institution'))
        self.assertTrue(self.allowed(subject='household:home', scope='household'))
        self.assertEqual(before, (self.auth.read_bytes(), self.cms.read_bytes()))


if __name__ == '__main__':
    unittest.main()
