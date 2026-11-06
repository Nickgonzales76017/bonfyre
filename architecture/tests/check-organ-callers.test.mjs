import { test, expect } from 'bun:test';
import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

const program = resolve(import.meta.dir, '..', 'check-organ-callers.mjs');

test('a native organ test is not an external caller', async () => {
  const root = await mkdtemp(join(tmpdir(), 'bonfyre-organ-callers-'));
  try {
    for (const name of ['cmd', 'components', 'integrations', 'scripts', 'lib',
      'engine/core/include', 'engine/core/src', 'engine/core/tests'])
      await mkdir(join(root, name), { recursive: true });
    await writeFile(join(root, 'engine/core/include/bf_demo.h'), 'int bf_demo_run(void);\n');
    await writeFile(join(root, 'engine/core/src/demo.c'), 'int bf_demo_run(void) { return 0; }\n');
    await writeFile(join(root, 'engine/core/tests/demo_test.c'), 'int main(void) { return bf_demo_run(); }\n');
    const run = () => Bun.spawnSync(['bun', program, '--json'], {
      env: { ...process.env, BONFYRE_REPO_ROOT: root }, stdout: 'pipe', stderr: 'pipe',
    });
    let result = run();
    expect(result.exitCode).toBe(0);
    expect(JSON.parse(result.stdout.toString()).uncalled).toEqual(['bf_demo']);
    await writeFile(join(root, 'cmd/caller.c'), 'int call(void) { return bf_demo_run(); }\n');
    result = run();
    expect(result.exitCode).toBe(0);
    expect(JSON.parse(result.stdout.toString()).organs[0].callers).toEqual(['cmd/caller.c']);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
