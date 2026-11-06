// Invoked from the Bonfyre runtime clone by the reusable Pages workflow.
// The working directory remains the consumer app repository.
import { mkdirSync, readFileSync, statSync, writeFileSync } from 'node:fs';

const MAX_CHANGED_FILES_BYTES = 1024 * 1024;
const DECIMAL = /^(0|[1-9][0-9]*)$/;
const POSITIVE_DECIMAL = /^[1-9][0-9]*$/;

function refuse(message) { throw new Error(`Pages runtime metadata: ${message}`); }
function required(env, name) {
  const value = env[name];
  if (typeof value !== 'string' || !value.trim() || value.includes('\0'))
    refuse(`${name} is missing or invalid`);
  return value;
}
function positiveRunNumber(env, name) {
  const value = required(env, name);
  if (!POSITIVE_DECIMAL.test(value)) refuse(`${name} must be a positive decimal string`);
  return value;
}
function readChangedFiles(filename) {
  let bytes;
  try {
    const source = statSync(filename);
    if (!source.isFile() || source.size > MAX_CHANGED_FILES_BYTES)
      refuse('CHANGED_FILES_FILE must be a bounded regular file');
    bytes = readFileSync(filename);
    if (bytes.length > MAX_CHANGED_FILES_BYTES)
      refuse('CHANGED_FILES_FILE must be a bounded regular file');
  } catch (error) {
    if (error.message.startsWith('Pages runtime metadata:')) throw error;
    refuse(`cannot read CHANGED_FILES_FILE: ${error.message}`);
  }
  let contents;
  try { contents = new TextDecoder('utf-8', { fatal: true }).decode(bytes); }
  catch { refuse('CHANGED_FILES_FILE must be valid UTF-8'); }
  if (contents.includes('\0')) refuse('CHANGED_FILES_FILE contains NUL');
  const lines = contents.split(/\r\n|\n|\r/);
  if (lines.at(-1) === '') lines.pop();
  if (lines.some(line => !line)) refuse('CHANGED_FILES_FILE contains an empty path');
  return lines;
}
function pythonAsciiJson(value) {
  // Match json.dumps(..., indent=2) defaults used by the retired Python step:
  // ensure_ascii=True, two-space indentation, and a terminal newline.
  return JSON.stringify(value, null, 2).replace(/[\u007f-\uffff]/g,
    unit => `\\u${unit.charCodeAt(0).toString(16).padStart(4, '0')}`) + '\n';
}
function pythonPath(value) {
  // pathlib.Path removes empty and '.' components, but preserves '..' in its
  // printable path (unlike path.normalize). The workflow records that string.
  const absolute = value.startsWith('/');
  const parts = value.split('/').filter(part => part && part !== '.');
  return (absolute ? '/' : '') + parts.join('/') || (absolute ? '/' : '.');
}
function pathChild(parent, leaf) {
  return parent === '/' ? `/${leaf}` : parent === '.' ? leaf : `${parent}/${leaf}`;
}

export function emitPagesRuntimeMetadata(env = process.env, now = new Date()) {
  const appName = required(env, 'APP_NAME');
  const repository = required(env, 'GITHUB_REPOSITORY');
  if (!/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(repository))
    refuse('GITHUB_REPOSITORY must be owner/repository');
  const ref = required(env, 'GITHUB_REF_NAME');
  const sha = required(env, 'GITHUB_SHA');
  if (!/^(?:[a-fA-F0-9]{40}|[a-fA-F0-9]{64})$/.test(sha))
    refuse('GITHUB_SHA must be a full commit digest');
  const runId = positiveRunNumber(env, 'GITHUB_RUN_ID');
  const runAttempt = positiveRunNumber(env, 'GITHUB_RUN_ATTEMPT');
  const countText = required(env, 'CHANGED_COUNT');
  if (!DECIMAL.test(countText) || !Number.isSafeInteger(Number(countText)))
    refuse('CHANGED_COUNT must be a nonnegative safe integer');
  const changedCount = Number(countText);
  const changedFiles = readChangedFiles(required(env, 'CHANGED_FILES_FILE'));
  if (changedCount !== changedFiles.length)
    refuse(`CHANGED_COUNT ${changedCount} differs from changed file count ${changedFiles.length}`);
  const rawDeployPath = required(env, 'DEPLOY_PATH');
  const deployPath = pythonPath(rawDeployPath);
  if (!Number.isFinite(now.getTime())) refuse('clock must produce a valid UTC timestamp');
  const generatedAt = now.toISOString().replace(/\.(\d{3})Z$/, (_, millis) => `.${millis}000Z`);

  const job = {
    app_name: appName,
    repository,
    ref,
    sha,
    run_id: runId,
    run_attempt: runAttempt,
    changed_count: changedCount,
    changed_files: changedFiles,
    generated_at: generatedAt,
  };
  const summary = {
    app_name: appName,
    status: 'processed',
    changed_count: changedCount,
    changed_files: changedFiles,
    deploy_path: deployPath,
    artifact_manifest: pathChild(deployPath, 'artifact.json'),
  };
  mkdirSync(deployPath, { recursive: true });
  writeFileSync(pathChild(deployPath, 'job.json'), pythonAsciiJson(job));
  writeFileSync(pathChild(deployPath, 'summary.json'), pythonAsciiJson(summary));
  return { job, summary };
}

if (import.meta.main) {
  try {
    if (process.argv.length !== 2) refuse('usage: bun scripts/pages-runtime-metadata.mjs');
    emitPagesRuntimeMetadata();
  } catch (error) {
    console.error(error.message);
    process.exitCode = 2;
  }
}
