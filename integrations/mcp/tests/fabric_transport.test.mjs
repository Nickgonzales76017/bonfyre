// What this does not prove:
// - the network call is a function supplied by this test, so no request leaves the process and no remote behaviour is observed.
import { test, expect } from 'bun:test';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createMcpTransport, validateArguments } from '../http_boundary.mjs';
import { createFabricServer } from '../fabric_server.mjs';

const catalog = JSON.parse(readFileSync(new URL('../fabric_catalog.json', import.meta.url), 'utf8'));
const BASE = 'http://127.0.0.1:3053';
const taskCreators = ['dispatch_claude', 'dispatch_codex', 'duel', 'implementation_relay', 'cross_review',
  'dual_verify', 'pr_converge', 'resume_session', 'batch_plan', 'estate_mission', 'task_cancel'];
function fixture(overrides = {}) {
  const calls = [], events = [];
  const dispatch = Object.fromEntries(catalog.tools.map(tool => [tool.name, async args => {
    calls.push({ name: tool.name, args });
    return { owner: tool.name, args };
  }]));
  Object.assign(dispatch, overrides);
  const transport = createMcpTransport({ token: 'test-token', label: 'fabric', service: 'bonfyre-dual-agent-fabric',
    version: '4.0.0', instructions: 'mock', tools: catalog.tools, dispatch, statusTool: 'fabric_status',
    prompts: catalog.prompts, getPrompt: (name, args) => ({ description: name, messages: [{ role: 'user', content: { type: 'text', text: args.mission || name } }] }),
    serialize: taskCreators,
    routes: [
      { method: 'GET', path: '/events', run: () => ({ events }) },
      { method: 'POST', path: '/hook', status: 202, serialize: true,
        schema: { type: 'object', properties: { source: { type: 'string' }, kind: { type: 'string' } }, additionalProperties: true },
        run: args => { events.push(args); return { accepted: true, seq: events.length }; } },
    ], maxBytes: 20_000, maxConcurrent: 2 });
  const req = (path, { method = 'GET', body, authorized = true } = {}) => transport.handle(new Request(`${BASE}${path}`, {
    method, headers: { ...(authorized ? { authorization: 'Bearer test-token' } : {}), ...(body ? { 'content-type': 'application/json' } : {}) },
    ...(body ? { body: JSON.stringify(body) } : {}),
  }));
  const rpc = (method, params = {}, id = 'request-1') => req('/mcp', { method: 'POST', body: { jsonrpc: '2.0', id, method, params } });
  return { calls, events, transport, req, rpc };
}

test('captured installed fabric catalog has exactly fourteen tools and four prompts', () => {
  expect(catalog.tools).toHaveLength(14);
  expect(catalog.prompts).toHaveLength(4);
  expect(new Set(catalog.tools.map(tool => tool.name)).size).toBe(14);
  expect(catalog.tools.find(tool => tool.name === 'dispatch_claude').annotations.readOnlyHint).toBe(true);
});

test('health is public; events/status/hook require bearer and events are finite JSON, not SSE', async () => {
  const { req } = fixture();
  expect((await req('/health', { authorized: false })).status).toBe(200);
  expect((await req('/events', { authorized: false })).status).toBe(401);
  expect((await req('/status', { authorized: false })).status).toBe(401);
  expect((await req('/hook', { method: 'POST', body: { kind: 'test' }, authorized: false })).status).toBe(401);
  const hook = await req('/hook', { method: 'POST', body: { source: 'claude', kind: 'PostToolUse' } });
  expect(hook.status).toBe(202);
  expect(await hook.json()).toEqual({ accepted: true, seq: 1 });
  const events = await req('/events');
  expect(events.headers.get('content-type')).toContain('application/json');
  expect(await events.json()).toEqual({ events: [{ source: 'claude', kind: 'PostToolUse' }] });
  expect((await req('/events-stream')).status).toBe(404);
});

test('MCP catalog, prompts, admission and unknown-resource refusal use one shared boundary', async () => {
  const { rpc, calls } = fixture();
  expect((await (await rpc('tools/list')).json()).result.tools).toEqual(catalog.tools);
  expect((await (await rpc('prompts/list')).json()).result.prompts).toEqual(catalog.prompts);
  expect((await (await rpc('prompts/get', { name: 'duel', arguments: { mission: 'test' } })).json()).result.messages[0].content.text).toBe('test');
  const invocation = await (await rpc('tools/call', { name: 'dispatch_claude', arguments: { prompt: 'inspect' } })).json();
  expect(invocation.result.isError).toBe(false);
  expect(calls.at(-1)).toEqual({ name: 'dispatch_claude', args: { prompt: 'inspect' } });
  const invalid = await (await rpc('tools/call', { name: 'dispatch_claude', arguments: { prompt: 7 } })).json();
  expect(invalid.result.isError).toBe(true);
  expect(calls).toHaveLength(1);
  expect((await (await rpc('resources/list')).json()).error).toBeDefined();
});

test('hook input is bounded and malformed JSON fails without recording an event', async () => {
  const { req, events } = fixture();
  expect((await req('/hook', { method: 'POST', body: { source: 10 } })).status).toBe(400);
  expect(events).toEqual([]);
  const malformed = await req('/hook', { method: 'POST', body: { payload: 'x'.repeat(20_001) } });
  expect(malformed.status).toBe(400);
  expect(events).toEqual([]);
});

test('task creation is serialized despite inherited readOnlyHint annotation', async () => {
  let running = 0, maxRunning = 0, release;
  const gate = new Promise(resolve => { release = resolve; });
  const { transport } = fixture({ dispatch_claude: async () => { running++; maxRunning = Math.max(maxRunning, running); await gate; running--; return { state: 'queued' }; } });
  const first = transport.call('dispatch_claude', { prompt: 'one' });
  const second = transport.call('dispatch_claude', { prompt: 'two' });
  await new Promise(resolve => setTimeout(resolve, 10));
  expect(maxRunning).toBe(1);
  release();
  await Promise.all([first, second]);
  expect(maxRunning).toBe(1);
});

async function actualOwner({ stopPaths = [], maxQueued = 64, maxParallel = 2, execute } = {}) {
  const taskRows = [], updates = [], workCalls = [], cancelled = [], events = [], estateAdds = [];
  const store = {
    createTask: args => { const id = `task-${taskRows.length + 1}`; taskRows.push({ id, ...args }); return id; },
    updateTask: (id, data) => updates.push({ id, ...data }), taskStatus: id => ({ id, state: 'queued', runs: [], receipts: [] }),
    taskSummary: id => ({ id, state: 'running', detail: 'summary', recent_runs: [], recent_receipts: [] }),
    recentReceipts: limit => ({ receipts: [{ id: 'receipt-one', limit }] }),
    summary: () => ({ tasks: { queued: taskRows.length }, runs: {}, locks: [], recent_tasks: taskRows }),
    events: limit => ({ events: events.slice(-limit) }),
    safeEvents: limit => ({ events: events.slice(-limit) }),
    recordEvent: (source, kind, payload, session) => { events.push({ source, kind, payload, session }); return events.length; },
    close: () => {},
  };
  const work = { execute: id => { workCalls.push(id); return execute?.(id); }, cancel: async id => { cancelled.push(id); return { task_id: id, cancelled_runs: [] }; }, close: async () => {} };
  const estate = { status: () => ({ world: {} }), add: args => { estateAdds.push(args); return { mission_id: 'mission-one', dedupe_key: 'manual:one' }; } };
  const run = async () => ({ exit_code: 0, stdout: 'agent 1', stderr: '' });
  const request = async (_url, options) => Response.json({ jsonrpc: '2.0', id: JSON.parse(options.body).id,
    result: { isError: false, structuredContent: { healthy: true } } });
  const node = await createFabricServer({ token: 'test-token', home: '/unused', projectRoot: '/unused', worktreeRoot: '/unused',
    policy: { max_parallel_tasks: maxParallel, allow_background_tasks: true, agents: { claude: { command: 'claude' }, codex: { command: 'codex' } } },
    store, work, estate, run, request, env: {}, stopPaths, maxQueued,
    pushDelivery:{dispatch:async()=>{throw Error('Recipient delivery not part of this operator transport fixture');},start(){},async close(){}},
    modelWork:{ownsTask:()=>false,dispatch:async()=>{throw Error('Model work not part of this operator transport fixture');},async close(){}} });
  return { node, taskRows, updates, workCalls, cancelled, events, estateAdds };
}

test('real fabric constructor binds all fourteen catalog tools to injected owners', async () => {
  const state = await actualOwner();
  const { node, taskRows, workCalls, cancelled, estateAdds } = state;
  try {
    const cases = [
      ['dispatch_claude', { prompt: 'a' }, 'claude'], ['dispatch_codex', { prompt: 'b' }, 'codex'],
      ['duel', { prompt: 'c' }, 'duel'], ['implementation_relay', { prompt: 'd', apply_to_project: true }, 'relay'],
      ['cross_review', {}, 'review'], ['dual_verify', { claim: 'e' }, 'verify'],
      ['pr_converge', {}, 'pr'], ['resume_session', { agent: 'claude', session_id: 'session', prompt: 'f' }, 'resume-claude'],
      ['batch_plan', { mission: 'g' }, 'batch'],
    ];
    for (const [name, args, kind] of cases) {
      const result = await node.call(name, args);
      expect(result.kind).toBe(kind);
      expect(result.state).toBe('queued');
    }
    await node.drain();
    expect(taskRows.map(task => task.kind)).toEqual(cases.map(item => item[2]));
    expect(workCalls).toHaveLength(cases.length);
    expect((await node.call('fabric_status', {})).bonfyre_mcp).toEqual({ healthy: true });
    expect((await node.call('task_status', { task_id: 'task-1' })).id).toBe('task-1');
    expect((await node.call('recent_receipts', { limit: 3 })).receipts[0].limit).toBe(3);
    expect((await node.call('task_cancel', { task_id: 'task-1' })).task_id).toBe('task-1');
    expect(cancelled).toEqual(['task-1']);
    const mission = await node.call('estate_mission', { title: 'Institutional task' });
    expect(mission.mission_id).toBe('mission-one');
    expect(mission.exit_code).toBe(0);
    expect(JSON.parse(mission.stdout)).toEqual({ mission_id: 'mission-one', dedupe_key: 'manual:one' });
    expect(mission.backend).toBe('bun:sqlite');
    expect(estateAdds).toEqual([{ title: 'Institutional task', details: '', family: 'manual', priority: 90, locks: 'general' }]);
  } finally { await node.close(); }
});

test('real broker hook route retains allowlisted evidence, never private payloads', async () => {
  const { node, events } = await actualOwner();
  try {
    const headers = { authorization: 'Bearer test-token', 'content-type': 'application/json' };
    const send = payload => node.handle(new Request(BASE + '/hook', {
      method: 'POST', headers, body: JSON.stringify(payload) }));
    const body = { source: 'claude', kind: 'PostToolUse', session_id: 'session42',
      tool_name: 'Bash', usage: { input_tokens: 11, cache_read_input_tokens: 8,
        cache_creation_input_tokens: -4, output_tokens: 2 },
      prompt: 'SECRET_PROMPT', tool_input: { command: 'SECRET_COMMAND' },
      tool_response: 'SECRET_OUTPUT', transcript_path: '/Users/private' };
    const result = await send(body);
    expect(result.status).toBe(202);
    expect((await result.json()).accepted).toBe(true);
    expect(events).toEqual([{ source: 'claude', kind: 'PostToolUse', session: 'session42',
      payload: { source: 'claude', kind: 'PostToolUse', session_id: 'session42',
        tool_name: 'Bash', usage: { input_tokens: 11, cache_read_input_tokens: 8,
          output_tokens: 2 } } }]);
    expect(JSON.stringify(events)).not.toContain('SECRET_');
    const ignored = await send({ source: 'claude', kind: 'Unsafe', prompt: 'SECRET_PROMPT' });
    expect((await ignored.json()).accepted).toBe(false);
    expect(events).toHaveLength(1);
  } finally { await node.close(); }
});

test('historical hook bodies stay redacted on authenticated event reads', async () => {
  const {node,events} = await actualOwner();
  try {
    events.push({seq:1,source:'claude',kind:'PostToolUse',session:'s-old',
      payload:{source:'claude',kind:'PostToolUse',session_id:'s-old',
        tool_input:{command:'PRIVATE_COMMAND'},prompt:'PRIVATE_PROMPT',
        usage:{input_tokens:17,output_tokens:3}}});
    events.push({seq:2,source:'codex',kind:'unknown-old',session:'s2',
      payload:{raw:'PRIVATE_UNKNOWN'}});
    const res=await node.handle(new Request(BASE + '/events',
      {headers:{authorization:'Bearer test-token'}}));
    expect(res.status).toBe(200);
    const value=await res.json();
    expect(value.events[0].payload).toEqual({source:'claude',kind:'PostToolUse',
      session_id:'s-old',usage:{input_tokens:17,output_tokens:3}});
    expect(value.events[1].payload).toEqual({source:'codex',kind:'unknown-old',legacy_redacted:true});
    expect(JSON.stringify(value)).not.toContain('PRIVATE_');
  }finally{await node.close();}
});

test('real fabric routes bind hook/events while STOP refuses new task and estate effects', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'fabric-transport-test-'));
  const stop = join(dir, 'STOP');
  const { node, events } = await actualOwner({ stopPaths: [stop] });
  try {
    const auth = { authorization: 'Bearer test-token', 'content-type': 'application/json' };
    const hook = await node.handle(new Request(`${BASE}/hook`, { method: 'POST', headers: auth,
      body: JSON.stringify({ hook_event_name: 'PostToolUse', source: 'claude', session_id: 's1' }) }));
    expect(hook.status).toBe(202);
    expect((await hook.json()).seq).toBe(1);
    expect(events[0]).toMatchObject({ source: 'claude', kind: 'PostToolUse', session: 's1' });
    expect((await (await node.handle(new Request(`${BASE}/events`, { headers: auth }))).json()).events).toHaveLength(1);
    writeFileSync(stop, 'stop');
    await expect(node.call('dispatch_claude', { prompt: 'no' })).rejects.toThrow('stopped');
    await expect(node.call('estate_mission', { title: 'no' })).rejects.toThrow('stopped');
    expect((await node.handle(new Request(`${BASE}/health`))).status).toBe(200);
    expect((await (await node.handle(new Request(`${BASE}/health`))).json()).paused).toBe(true);
    const status = await node.call('fabric_status', {});
    expect(status.admission).toMatchObject({ paused: true, accepting_tasks: false });
    expect(status.execution.active_tasks).toEqual([]);
    const historical = await node.call('task_status', { task_id: 'historical', detail: 'summary' });
    expect(historical.state).toBe('running');
    expect(historical.execution).toEqual({ owned_by_this_process: false, queue_position: null, reconciliation_required: true });
  } finally { await node.close(); rmSync(dir, { recursive: true, force: true }); }
});

test('real fabric queue applies active and queued bounds before creating additional tasks', async () => {
  let release;
  const gate = new Promise(resolve => { release = resolve; });
  const { node, taskRows, workCalls } = await actualOwner({ maxQueued: 1, maxParallel: 1, execute: () => gate });
  try {
    await node.call('dispatch_claude', { prompt: 'running' });
    await new Promise(resolve => setTimeout(resolve, 0));
    await node.call('dispatch_codex', { prompt: 'queued' });
    await expect(node.call('duel', { prompt: 'rejected' })).rejects.toThrow('queue full');
    expect(taskRows).toHaveLength(2);
    expect(workCalls).toHaveLength(1);
    const active = await node.call('task_status', { task_id: 'task-1', detail: 'summary' });
    const queued = await node.call('task_status', { task_id: 'task-2', detail: 'summary' });
    expect(active.execution).toEqual({ owned_by_this_process: true, queue_position: null, reconciliation_required: false });
    expect(queued.execution).toEqual({ owned_by_this_process: false, queue_position: 0, reconciliation_required: false });
    release(); await node.drain();
    expect(workCalls).toHaveLength(2);
  } finally { release(); await node.close(); }
});

test('source selectors carry nullable hash preconditions through MCP without accepting other JSON types',async()=>{
  const {rpc,calls}=fixture();
  const valid={prompt:'inspect',source_files:[{path:'deleted',sha256:null},{path:'current',sha256:'a'.repeat(64)}]};
  const answer=await (await rpc('tools/call',{name:'implementation_relay',arguments:valid})).json();
  expect(answer.result.isError).toBe(false);expect(calls[0].args.source_files).toEqual(valid.source_files);
  for(const sha256 of [false,0,[],{}]){
    const answer=await (await rpc('tools/call',{name:'implementation_relay',arguments:{prompt:'inspect',source_files:[{path:'x',sha256}]}})).json();
    expect(answer.result.isError).toBe(true);
  }
  const schema={type:['object','null'],properties:{value:{type:'integer'}},required:['value'],additionalProperties:false};
  expect(()=>validateArguments(null,schema)).not.toThrow();expect(()=>validateArguments({value:1},schema)).not.toThrow();
  for(const value of [{},{value:'1'},{value:1,extra:true},[]])expect(()=>validateArguments(value,schema)).toThrow();
  expect(calls).toHaveLength(1);
});
