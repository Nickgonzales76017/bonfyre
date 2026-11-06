#!/usr/bin/env python3
"""Verify synthetic cached bytes without downloading or executing a model."""
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile

binary = str(Path(sys.argv[1]).resolve())
checks = 0
with tempfile.TemporaryDirectory(prefix='bonfyre-model-hash-') as tmp:
    root = Path(tmp)
    cache = root / 'cache'
    cache.mkdir()
    env = dict(os.environ, BONFYRE_MODEL_DB=str(root / 'models.db'),
               BONFYRE_MODEL_CACHE=str(cache), BONFYRE_CATALOG_DB=str(root / 'catalog.db'))
    manifest = root / 'manifest.json'
    body = cache / 'synthetic-bytes.bin'

    def register(digest, model_id='synthetic-bytes'):
        manifest.write_text(json.dumps(dict(id=model_id, name='Synthetic hash fixture',
                                           format='bin', sha256=digest, sources=[])))
        result = subprocess.run([binary, 'add', str(manifest)], env=env, capture_output=True)
        assert result.returncode == 0, result.stderr

    def verify(expected, text=None, model_id='synthetic-bytes'):
        global checks
        result = subprocess.run([binary, 'verify', model_id], env=env, capture_output=True)
        assert (result.returncode == 0) == expected, (result.stdout, result.stderr)
        if text:
            assert text.encode() in result.stdout + result.stderr
        checks += 1

    data = bytes(range(256)) * 65 + b'partial-final-block'
    digest = hashlib.sha256(data).hexdigest()
    body.write_bytes(data)
    register(digest)
    verify(True, digest)
    assert body.read_bytes() == data
    body.write_bytes(data[:-1] + b'X')
    verify(False, 'FAIL')
    body.unlink()
    verify(False, 'not in cache')
    body.write_bytes(b'')
    empty = hashlib.sha256(b'').hexdigest()
    register(empty)
    verify(True, empty)
    body.unlink()
    body.mkdir()
    verify(False, 'cannot hash')
    register('pending')
    verify(False, 'cannot hash')
    with sqlite3.connect(root / 'models.db') as db:
        assert db.execute("select sha256 from models where id='synthetic-bytes'").fetchone() == (None,)
    body.rmdir()
    body.write_bytes(data)
    verify(True, digest)
    register('pending', 'duplicate-body')
    (cache / 'duplicate-body.bin').write_bytes(data)
    verify(False, 'cannot store hash', 'duplicate-body')
    with sqlite3.connect(root / 'models.db') as db:
        assert db.execute("select sha256 from models where id='duplicate-body'").fetchone() == (None,)
    with sqlite3.connect(root / 'models.db') as db:
        assert db.execute("select sha256 from models where id='synthetic-bytes'").fetchone() == (digest,)
    verify(True, digest)
print(f'BonfyreModel file verification: PASS ({checks} CLI checks)')
