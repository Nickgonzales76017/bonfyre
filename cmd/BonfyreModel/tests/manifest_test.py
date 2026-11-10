#!/usr/bin/env python3
"""Exercise manifest parsing and atomic replacement through the native CLI."""
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile

binary = str(Path(sys.argv[1]).resolve())
with tempfile.TemporaryDirectory() as tmp:
    root = Path(tmp)
    env = dict(os.environ, BONFYRE_MODEL_DB=str(root / 'models.db'),
               BONFYRE_MODEL_CACHE=str(root / 'cache'), BONFYRE_CATALOG_DB=str(root / 'catalog.db'))
    manifest = root / 'manifest.json'
    front = subprocess.run([binary], env=env, capture_output=True, check=True).stdout.decode()
    assert 'START HERE' in front and 'setup <manifest.json>' in front
    for internal in ['representation-record', 'representation-gate', 'source add', 'rm --purge', 'push <id>']:
        assert internal not in front, (internal, front)
    advanced = subprocess.run([binary, 'help', '--all'], env=env, capture_output=True, check=True).stdout.decode()
    assert 'representation-record <passport.json>' in advanced
    assert 'source add <id> <url>' in advanced
    assert not (root / 'models.db').exists(), 'help must not open the model store'

    def run(value, good=True):
        manifest.write_text(json.dumps(value) if not isinstance(value, str) else value)
        result = subprocess.run([binary, 'add', str(manifest)], env=env, capture_output=True)
        assert (result.returncode == 0) == good, (value, result.stdout.decode(), result.stderr.decode())
    def rows(query):
        with sqlite3.connect(root / 'models.db') as db:
            return db.execute(query).fetchall()
    base = dict(id='real-model', name='A "quoted" model', format='safetensors',
                description='Nested id: "wrong"', sources=[
                    {'url': 'https://example.org/weights?version=1', 'scheme': 'https'},
                    'hf://owner/model/weights'])
    run(base)
    assert rows("select url from sources where model_id='real-model' order by priority") == [
        ('https://example.org/weights?version=1',), ('hf://owner/model/weights',)]
    assert rows("select name from models where id='real-model'") == [('A "quoted" model',)]
    with sqlite3.connect(root / 'models.db') as db:
        db.execute("insert into recipe_models values('test','real-model','embed')")
    run(dict(base, sources=[], description='following fields are not sources'))
    assert rows("select url from sources where model_id='real-model'") == []
    assert rows("select count(*) from recipe_models where model_id='real-model'") == [(1,)]
    for bad in [dict(base, sources=[{'scheme': 'https'}]), dict(base, sources='url'),
                dict(base, sources=[7]), dict(base, sources=['a\0b']),
                dict(base, name='x' * 256), '{"id":', '[]']:
        run(bad, False)
        assert rows("select url from sources where model_id='real-model'") == []
        assert rows("select name from models where id='real-model'") == [('A "quoted" model',)]
    run(dict(base, sources=['https://example.org/new']))
    assert rows("select url from sources where model_id='real-model'") == [('https://example.org/new',)]
    # A later constraint failure must not remove the previous source set.
    run(dict(base, id='second', sha256='a' * 64, sources=[]))
    run(dict(base, sha256='a' * 64), False)
    assert rows("select url from sources where model_id='real-model'") == [('https://example.org/new',)]

    body = root / 'setup-model.gguf'
    body.write_bytes(b'bonfyre-model-setup-fixture\n')
    setup = dict(id='setup-model', name='Setup model', format='gguf',
                 sha256=hashlib.sha256(body.read_bytes()).hexdigest(),
                 size_mb=body.stat().st_size / (1024 * 1024),
                 sources=[body.resolve().as_uri()])
    manifest.write_text(json.dumps(setup))
    result = subprocess.run([binary, 'setup', str(manifest)], env=env, capture_output=True)
    assert result.returncode == 0, (result.stdout.decode(), result.stderr.decode())
    stdout = result.stdout.decode()
    assert 'body: ready and verified' in stdout
    assert 'representation: unmeasured (0 measured, 0 admitted)' in stdout
    assert 'model prove setup-model <run.json>' in stdout
    path = subprocess.run([binary, 'path', 'setup-model'], env=env,
                          capture_output=True, check=True).stdout.decode().strip()
    assert Path(path).is_file(), path

    run_manifest = root / 'run.json'
    qwen_run = {
        'kind': 'qwen_generation',
        'recipe_id': 'bonfyre-qwen-fpq',
        'status': 'ok',
        'created_at': '2026-10-23T16:00:00Z',
        'model_id': 'setup-model',
        'model_hash': setup['sha256'],
        'representation': 'native-fp16',
        'finite_model_state': True,
        'generated_tokens': 2,
    }
    run_manifest.write_text(json.dumps(qwen_run))
    evidence = subprocess.run([binary, 'prove', 'setup-model', str(run_manifest)],
                              env=env, capture_output=True)
    assert evidence.returncode == 0, (evidence.stdout.decode(), evidence.stderr.decode())
    assert '"measurement":"measured"' in evidence.stdout.decode()
    listed = subprocess.run([binary, 'representations', 'setup-model'], env=env,
                            capture_output=True, check=True).stdout.decode()
    assert '"measurement":"measured"' in listed
    assert '"admission":"DEFER"' in listed
    assert '"scope":"generation"' in listed
    representation_id = rows(
        "select representation_id from model_representation_passport "
        "where model_id='setup-model' and measurement_state='measured'"
    )[0][0]
    gate = subprocess.run([binary, 'representation-gate', representation_id,
                           '--scope', 'generation'], env=env, capture_output=True)
    assert gate.returncode == 0, (gate.stdout.decode(), gate.stderr.decode())
    assert '"decision":"DEFER"' in gate.stdout.decode()
    wrong_scope = subprocess.run([binary, 'representation-gate', representation_id,
                                  '--scope', 'rerank'], env=env, capture_output=True)
    assert wrong_scope.returncode != 0
    setup_again = subprocess.run([binary, 'setup', str(manifest)], env=env,
                                 capture_output=True, check=True).stdout.decode()
    assert 'representation: evidence recorded (1 measured, 0 admitted)' in setup_again

    qwen_run['model_hash'] = '0' * 64
    run_manifest.write_text(json.dumps(qwen_run))
    bad = subprocess.run([binary, 'prove', 'setup-model', str(run_manifest)],
                         env=env, capture_output=True)
    assert bad.returncode != 0
    assert rows("select count(*) from model_representation_passport where model_id='setup-model'") == [(1,)]

print('BonfyreModel manifest: PASS')
