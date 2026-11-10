// What this does not prove:
// - a server started in this test answers in place of the outside service, so nothing here is evidence about that service.
// - the network call is a function supplied by this test, so no request leaves the process and no remote behaviour is observed.
// Offline protocol fixture: deliberately not a model or a trained passport.
import { readFileSync } from 'node:fs';
import nativeLock from '../../native/clef-runtime.lock.json' with { type: 'json' };

const option = name => {
  const i = process.argv.indexOf(name);
  return i < 0 ? undefined : process.argv[i + 1];
};
const port = Number(option('--port'));
const host = option('--host');
const alias = option('--alias');
const model = option('--model');
const secret = readFileSync(option('--api-key-file'), 'utf8').trim();
if (!Number.isSafeInteger(port) || host !== '127.0.0.1' || !alias || !model || !secret)
  throw new Error('invalid mock SystemOne launch');
if (Number(option('--ubatch-size')) !== nativeLock.execution.microbatch_tokens ||
    Number(option('--batch-size')) !== nativeLock.execution.batch_tokens)
  throw new Error('mock SystemOne execution settings differ from the locked protocol');
if (option('--device') !== nativeLock.execution.device ||
    Number(option('--gpu-layers')) !== nativeLock.execution.gpu_layers ||
    !process.argv.includes('--no-op-offload') || !process.argv.includes('--no-kv-offload') ||
    !process.argv.includes('--repack') || process.argv.includes('--no-repack'))
  throw new Error('mock SystemOne launch omitted the locked CPU placement or repack mode');

Bun.serve({ hostname: host, port, async fetch(request) {
  if (request.headers.get('authorization') !== `Bearer ${secret}`)
    return new Response('unauthorized', { status: 401 });
  const path = new URL(request.url).pathname;
  if (path === '/health') return Response.json({ status: 'ok' });
  if (path === '/props') return Response.json({ model_path: model, model_alias: alias,
    total_slots: Number(option('--parallel')),
    default_generation_settings: { n_ctx: Number(option('--ctx-size')) },
    modalities: { vision: false } });
  if (path !== '/v1/systemone') return new Response('not found', { status: 404 });
  const body = await request.json();
  if (body.state?.backend_error) return Response.json({ error: { code: 503,
    type: 'native_failure', message: 'Compute error.' }, private_state: 'must never be reported' }, { status: 500 });
  if (body.state?.backend_private_error) return Response.json({ error: { code: 503,
    type: 'native_failure', message: 'rejected private state: secret phrase' } }, { status: 500 });
  if (body.state?.redirect) return Response.redirect('https://example.invalid/forbidden', 302);
  if (body.state?.slow) await Bun.sleep(2000);
  if (body.state?.oversized) return Response.json({ padding: 'x'.repeat(5000) });
  const answers = Object.fromEntries(Object.entries(body.questions).map(([id, question]) => {
    if (question.type === 'noul') return [id, { type: 'noul', noul: 1 }];
    const keys = question.type === 'choice' ? Object.keys(question.criteria) : question.criteria.map((_, index) => String(index));
    const probabilities = Object.fromEntries(keys.map((key, index) => [key, index ? 0 : 1]));
    const answer = { type: question.type, probabilities, confidence: 0.5 };
    if (question.type === 'choice') answer.choice = keys[0];
    else { answer.score = 0; answer.legend = Object.fromEntries(question.criteria.map((v, i) => [String(i), v])); }
    return [id, answer];
  }));
  if (body.state?.invalid) answers[Object.keys(answers)[0]].probabilities = { not_an_option: 1 };
  return Response.json({ model: body.state?.wrong_model ? 'wrong-model' : alias, answers, usage: { prompt_tokens: 9 } });
} });
