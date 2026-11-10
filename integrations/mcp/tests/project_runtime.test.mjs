import { test, expect } from 'bun:test';
import { projectRun } from '../project_runtime.mjs';

test('project process owner keeps literal argv and drains both streams', async () => {
  const argv = [process.execPath, '-e', 'console.log(JSON.stringify(process.argv.slice(1)));console.error("diagnostic")', 'a ; $(false) [x]'];
  const result = await projectRun(argv);
  expect(result.exit_code).toBe(0);
  expect(JSON.parse(result.stdout)).toEqual(['a ; $(false) [x]']);
  expect(result.stderr).toContain('diagnostic');
  expect(result.argv).toEqual(argv);
});

test('project process owner refuses timeout and overflow rather than truncated success', async () => {
  const timeout = await projectRun([process.execPath, '-e', 'await Bun.sleep(10000)'], { timeoutMs: 40 });
  expect(timeout.exit_code).toBe(124);
  expect(timeout.timeout).toBe(true);
  const overflow = await projectRun([process.execPath, '-e', 'process.stdout.write("x".repeat(100000));await Bun.sleep(10000)'], { maxOutputBytes: 1024 });
  expect(overflow.exit_code).toBe(125);
  expect(overflow.overflow).toBe(true);
  expect(Buffer.byteLength(overflow.stdout)).toBeLessThanOrEqual(1024);
});

test('project process owner fails closed on missing executable, binary output and retired interpreter', async () => {
  expect((await projectRun(['/missing/bonfyre-fixture-command'])).exit_code).toBe(127);
  const badUtf8 = await projectRun([process.execPath, '-e', 'process.stdout.write(Buffer.from([255]))']);
  expect(badUtf8.exit_code).toBe(126);
  expect(badUtf8.invalid_utf8).toBe(true);
  await expect(projectRun(['python3', '-c', 'raise Exception()'])).rejects.toThrow('retired');
  await expect(projectRun(['git', 'a\0b'])).rejects.toThrow('NUL-free');
});
