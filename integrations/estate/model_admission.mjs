// A local model may load only after ResourceAdmission grants it the memory.
// The grant lives on the shared ledger's "memory" volume, so a one-shot
// llama-cli run, the Clef decision runtime and the on-demand MoE server
// (bonfyre-moe serve) all count against the same gigabytes. A load that does
// not fit is refused with the ledger's reason instead of pushing this 16 GiB
// machine into swap, which is how it used to hang.
import { execFileSync } from 'node:child_process';
import { existsSync, statSync } from 'node:fs';
import { homedir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const repository = fileURLToPath(new URL('../../', import.meta.url));

export class ModelNotAdmittedError extends Error {
  constructor(decision, modelPath) {
    super(`${modelPath} was not loaded: memory admission said ${decision.verdict} (${decision.reason}); ` +
      `${Math.floor(decision.spendable_bytes / 2 ** 20)} MiB spendable now`);
    this.name = 'ModelNotAdmittedError';
    this.code = 'MODEL_NOT_ADMITTED';
    this.decision = decision;
  }
}

// Measured on Qwen1.5-MoE Q2_K at -c 2048 under Metal: weights + 384 MiB KV +
// 304 MiB compute. The same shape is used by bonfyre-moe's residency front.
export function estimateModelBytes(modelPath, contextSize = 2048) {
  return statSync(modelPath).size + 320 * 2 ** 20 + Number(contextSize) * 192 * 2 ** 10;
}

export function defaultModelAdmission(env = process.env) {
  const binary = [env.BONFYRE_ADMISSION_BIN, resolve(repository, 'cmd/BonfyreAdmission/bonfyre-admission'),
    join(homedir(), '.bonfyre/bin/bonfyre-admission')].find(path => path && existsSync(path));
  if (!binary) throw new Error('bonfyre-admission is not built; run make -C cmd/BonfyreAdmission. Models do not load unadmitted.');
  return { binary, db: env.BONFYRE_ADMISSION_DB || join(homedir(), '.bonfyre/admission.db') };
}

export function acquireModelMemory({ modelPath, contextSize = 2048, plane, admission = defaultModelAdmission(),
  freeBytes, exec = execFileSync } = {}) {
  const bytes = estimateModelBytes(modelPath, contextSize);
  const args = ['request', '--db', admission.db, '--plane', plane, '--kind', 'model-run', '--bytes', String(bytes),
    '--volume', 'memory', '--detail', modelPath, ...(freeBytes ? ['--free-bytes', String(freeBytes)] : [])];
  const decision = JSON.parse(exec(admission.binary, args, { encoding: 'utf8' }));
  if (decision.verdict !== 'admit') throw new ModelNotAdmittedError(decision, modelPath);
  let released = false;
  return {
    grantId: decision.grant_id,
    bytes,
    release() {
      if (released) return;
      released = true;
      exec(admission.binary, ['release', '--db', admission.db, '--grant', String(decision.grant_id)], { encoding: 'utf8' });
    },
  };
}
