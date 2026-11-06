#!/usr/bin/env bun
import { resolve } from 'node:path';

const policy = resolve(import.meta.dir, 'live-tree-guard.cue');
function loadRules() {
  const result = Bun.spawnSync(['cue', 'export', policy, '-e', 'liveTreeRules', '--out', 'json']);
  if (result.exitCode !== 0) throw new Error(`live tree guard law invalid: ${result.stderr}`);
  return JSON.parse(result.stdout.toString()).map(({ pattern, reason }) => ({ test: new RegExp(pattern, 'i'), reason }));
}
export const rules = loadRules();

export function decide(event) {
  const command = event?.tool_input?.command;
  if (typeof command !== 'string') return null;
  const match = rules.find(rule => rule.test.test(command));
  return match ? { hookSpecificOutput: {
    hookEventName: 'PreToolUse', permissionDecision: 'deny', permissionDecisionReason: match.reason,
  } } : null;
}

if (import.meta.main) {
  let event;
  try { event = JSON.parse(await Bun.stdin.text()); } catch { process.exit(0); }
  const output = decide(event);
  if (output) process.stdout.write(`${JSON.stringify(output)}\n`);
}
