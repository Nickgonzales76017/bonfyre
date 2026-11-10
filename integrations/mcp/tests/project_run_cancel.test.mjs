import { test, expect } from 'bun:test';
import { mkdtempSync, readFileSync, existsSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { projectRun } from '../project_runtime.mjs';

test('pre-aborted execution does not spawn or create its marker', async () => {
  const root = mkdtempSync(join(tmpdir(), 'fabric-cancel-')), marker = join(root, 'marker');
  try {
    const controller = new AbortController(); controller.abort();
    const result = await projectRun([process.execPath, '-e', 'await Bun.write(process.argv[1],"started")', marker], { signal: controller.signal });
    expect(result.exit_code).toBe(130); expect(result.cancelled).toBe(true); expect(existsSync(marker)).toBe(false);
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test('abort kills a TERM-resistant grandchild even when the parent closes all pipes', async () => {
  const root = mkdtempSync(join(tmpdir(), 'fabric-cancel-')), marker = join(root, 'ready'), heartbeat = join(root, 'heartbeat');
  let childPid;
  try {
    const controller = new AbortController();
    const grandchild = 'const fs=require("node:fs");process.on("SIGTERM",()=>{});fs.writeFileSync(process.argv[1],String(process.pid));setInterval(()=>fs.appendFileSync(process.argv[2],"."),20)';
    const parent = 'const {spawn}=require("node:child_process");spawn(process.execPath,["-e",process.argv[1],process.argv[2],process.argv[3]],{stdio:"ignore"});setInterval(()=>{},1000)';
    const pending = projectRun(['node', '-e', parent, grandchild, marker, heartbeat], { signal: controller.signal, timeoutMs: 5000 });
    const deadline = Date.now() + 3000;
    while (!existsSync(marker) && Date.now() < deadline) await Bun.sleep(10);
    expect(existsSync(marker)).toBe(true); childPid = Number(readFileSync(marker, 'utf8'));
    controller.abort(); const result = await pending;
    expect(result.cancelled).toBe(true); expect(result.exit_code).toBe(130);
    await Bun.sleep(100);
    const stoppedAt = existsSync(heartbeat) ? readFileSync(heartbeat).length : 0;
    await Bun.sleep(100);
    expect(existsSync(heartbeat) ? readFileSync(heartbeat).length : 0).toBe(stoppedAt);
  } finally {
    if (childPid) { try { process.kill(childPid, 'SIGKILL'); } catch { /* Already gone. */ } }
    rmSync(root, { recursive: true, force: true });
  }
}, 10_000);
