#!/usr/bin/env bun
/** Host-origin AgentSession observations; never producer-fidelity receipts. */
import { createHash } from 'node:crypto';
import { appendFileSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { resolve, join } from 'node:path';

const repo = resolve(import.meta.dir,'../..');
const output = process.env.BONFYRE_AGENT_SESSIONS_DIR || join(repo,'.bonfyre-runtime','agent-sessions');
const kinds = new Map([['Stop','model_turn'],['SubagentStop','model_turn'],['PreCompact','context_compaction']]);

export function sessionIdentity(providerRef) {
  const digest = createHash('sha256').update(`aurekai-agent-session:${providerRef}`).digest('hex').slice(0,32);
  return `agent-session:${digest}`;
}
export function nextSequence(path) {
  let count = 0;
  try { count = Number.parseInt(readFileSync(path,'utf8').trim(),10) || 0; } catch {}
  count++;
  try { writeFileSync(path,String(count)); } catch {}
  return count;
}
export function observation(event,sequence,now = new Date()) {
  const name = event.hook_event_name || '';
  const providerRef = String(event.session_id || 'unknown');
  const sessionId = sessionIdentity(providerRef);
  const observed = now.toISOString().replace('Z','+00:00');
  const kind = kinds.get(name);
  if (!kind) return {
    schema:'aurekai-agent-session-observation.v1',session_id:sessionId,
    provider_session_ref:providerRef,boundary:name.toLowerCase() || 'unknown',
    observed_at:observed,cwd:event.cwd || '',truth:'host_origin_observation_not_receipt_evidence',
  };
  const record = {
    schema:'aurekai-agent-session-observation.v1',session_id:sessionId,
    sequence,kind,evidence_origin:'host',work_node:`work:${sessionId}`,
    observed_at:observed,provider_session_ref:providerRef,
    truth:'host_origin_observation_not_receipt_evidence',
    cannot_infer:[
      'provider_session_ref -> AgentSession identity',
      'host evidence -> subject provider fidelity',
      'provider attribution -> authority, effect safety, quality, ownership or evidence sufficiency',
      'this observation -> verified producer fidelity (no producer_verifier_ref exists)',
    ],
  };
  if (kind === 'model_turn') {
    record.provider_identity = 'provider:anthropic';
    record.model_identity = event.model?.id || event.model?.display_name || process.env.ANTHROPIC_MODEL || 'model:unreported';
    record.subagent = name === 'SubagentStop';
  } else {
    record.context_before_ref = `context:${providerRef}:${sequence - 1}`;
    record.context_after_ref = `context:${providerRef}:${sequence}`;
    record.reason_ref = `reason:compaction:${event.trigger || 'unknown'}`;
  }
  return record;
}
export function recordEvent(event, directory = output) {
  const name = event.hook_event_name || '';
  const sessionId = sessionIdentity(String(event.session_id || 'unknown'));
  const stem = sessionId.split(':',2)[1];
  try { mkdirSync(directory,{recursive:true}); } catch { return null; }
  const sequence = kinds.has(name) ? nextSequence(join(directory,`${stem}.seq`)) : undefined;
  const record = observation(event,sequence);
  try { appendFileSync(join(directory,`${stem}.jsonl`),`${JSON.stringify(record)}\n`); } catch {}
  return record;
}
if (import.meta.main) {
  try { recordEvent(JSON.parse(await Bun.stdin.text())); } catch {}
}
