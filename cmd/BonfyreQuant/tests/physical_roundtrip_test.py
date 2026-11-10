#!/usr/bin/env python3
"""The benchmark must agree with independently saved and decoded weights."""
import pathlib
import re
import subprocess
import sys
import tempfile

quant, sli, model, tensor = sys.argv[1:]
def run(*args):
    p = subprocess.run(args, capture_output=True, text=True)
    assert p.returncode in (0, 2), p.stderr
    return p.stdout
with tempfile.TemporaryDirectory() as tmp:
    artifact = pathlib.Path(tmp) / 'tensor.bqfp'
    decoded = pathlib.Path(tmp) / 'tensor.f32'
    run(quant, 'encode', model, '--tensor', tensor, '--bits', '3', '--out', str(artifact))
    run(sli, 'decode', '--model', str(artifact), '--out', str(decoded))
    comparison = run(quant, 'compare', model, '--tensor', tensor, '--weights', str(decoded))
    benchmark = run(quant, 'roundtrip', model, '--tensor', tensor, '--bits', '3')
    for metric in ('cos', 'rmse'):
        pattern = metric + r'=([0-9.]+)'
        assert re.search(pattern, comparison)[1] == re.search(pattern, benchmark)[1], (comparison, benchmark)
    bpw = float(re.search(r'bits per weight: ([0-9.]+)', benchmark)[1])
    actual = artifact.stat().st_size * 8 / (decoded.stat().st_size / 4)
    assert abs(bpw - actual) <= 0.00051, (bpw, actual)
print('BQFP benchmark matches physical bytes and independent decode: PASS')
