// What this does not prove:
// - a script written in this test stands where a program would run, so it shows how the caller handles that program's interface and nothing about the program.
import { afterEach, expect, test } from 'bun:test';
import { mkdtempSync, readFileSync, rmSync, writeFileSync, existsSync, mkdirSync, copyFileSync, chmodSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

const script = resolve(import.meta.dir, '../native/build-clef.sh');
const patchPath = resolve(import.meta.dir, '../native/patches/clef-output-embedding.patch');
// Minimal compilable loader slice preserves the exact upstream patch context;
// this tests operator classification, not a full model/backend calculation.
const loaderSlice = `#include <stdexcept>
#include <string>
enum { LLM_ARCH_CLEF, LLM_ARCH_OTHER, LLM_TENSOR_OUTPUT, LLM_TENSOR_OTHER,
       GGML_OP_NONE, GGML_OP_MUL_MAT, GGML_OP_GET_ROWS, TENSOR_SKIP = 128 };
struct tensor_info { int op; };
static tensor_info llm_tensor_info_for(int) { return { GGML_OP_MUL_MAT }; }
static std::string format(const char *, const char * s) { return s; }
static size_t ggml_nbytes(void *) { return 0; }
static int choose(int arch, int tn_tensor) {
    auto get_arch = [arch] { return arch; };
    struct { std::string str() { return "output.weight"; } } tn;
    const int flags = 0;
    void * t_meta = nullptr;
    tensor_info info;
    try {
        info = llm_tensor_info_for(tn_tensor);
    } catch (const std::out_of_range &) {
            throw std::runtime_error(format("missing tensor info mapping for %s", tn.str().c_str()));
        }

        // skip unused tensors
        if (info.op == GGML_OP_NONE || (flags & TENSOR_SKIP)) {
            const size_t nbytes = ggml_nbytes(t_meta);
            (void)nbytes;
            return GGML_OP_NONE;
        }
    return info.op;
}
int main() {
    return choose(LLM_ARCH_CLEF, LLM_TENSOR_OUTPUT) != GGML_OP_GET_ROWS ||
           choose(LLM_ARCH_CLEF, LLM_TENSOR_OTHER) != GGML_OP_MUL_MAT ||
           choose(LLM_ARCH_OTHER, LLM_TENSOR_OUTPUT) != GGML_OP_MUL_MAT ||
           choose(LLM_ARCH_OTHER, LLM_TENSOR_OTHER) != GGML_OP_MUL_MAT;
}
`;
const roots = [];
afterEach(() => { for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true }); });
function fixture() { const root = mkdtempSync(join(tmpdir(), 'clef-build-guard-')); roots.push(root); return root; }
function run(args) { return Bun.spawnSync(['sh', script, ...args], { stdout: 'pipe', stderr: 'pipe' }); }

test('source-only builder refuses missing arguments and relative outputs before doing work', () => {
  expect(run([]).exitCode).toBe(2);
  const result = run(['relative-build']);
  expect(result.exitCode).toBe(2);
  expect(result.stderr.toString()).toContain('must be absolute');
});

test('existing output is never overwritten, including a failed build', () => {
  const root = fixture();
  writeFileSync(join(root, 'owner'), 'existing owner');
  const result = run([root]);
  expect(result.exitCode).toBe(2);
  expect(result.stderr.toString()).toContain('refusing to overwrite');
  expect(readFileSync(join(root, 'owner'), 'utf8')).toBe('existing owner');
});

test('offline archive digest mismatch is refused before extraction or CMake execution', () => {
  const root = fixture();
  const archive = join(root, 'untrusted.tar.gz');
  const out = join(root, 'runtime');
  writeFileSync(archive, 'not the pinned source archive');
  const result = run([out, archive]);
  expect(result.exitCode).toBe(1);
  expect(result.stderr.toString()).toContain('digest mismatch');
  expect(existsSync(join(out, 'source'))).toBe(false);
  expect(existsSync(join(out, 'build'))).toBe(false);
});

test('runtime passport distinguishes trained source, lossy representation, and bounded joint execution', () => {
  const lock = JSON.parse(readFileSync(resolve(import.meta.dir, '../native/clef-runtime.lock.json'), 'utf8'));
  expect(lock.runtime.revision).toMatch(/^[a-f0-9]{40}$/);
  expect(lock.runtime.source_archive_sha256).toMatch(/^[a-f0-9]{64}$/);
  expect(lock.runtime.source_patches).toHaveLength(1);
  expect(lock.runtime.source_patches[0].sha256).toBe(createHash('sha256').update(readFileSync(patchPath)).digest('hex'));
  expect(lock.model.sha256).toMatch(/^[a-f0-9]{64}$/);
  expect(lock.model.bytes).toBe(6486448192);
  expect(lock.model.source_revision).toBe('17f0b0ad64efb65d273590632833508766b2aae6');
  expect(lock.execution.joint_questions).toBe(true);
  expect(lock.execution.parallel_sequences).toBe(1);
  expect(lock.execution.microbatch_tokens).toBe(lock.execution.max_context_tokens);
  expect(lock.execution.prefix_cache).toBe(false);
  expect(lock.execution.trained_reference_parity).toContain('not-measured');
});

test('verified offline source reaches packaged native artifact and honest build receipt', () => {
  const root = fixture();
  const source = join(root, 'fixture-source');
  const mockbin = join(root, 'mockbin');
  const out = join(root, 'runtime');
  const archive = join(root, 'source.tar.gz');
  mkdirSync(source);
  mkdirSync(join(source, 'src'));
  mkdirSync(mockbin);
  writeFileSync(join(source, 'LICENSE'), 'fixture license, not upstream');
  writeFileSync(join(source, 'src/llama-model-loader.cpp'), loaderSlice);
  expect(Bun.spawnSync(['tar', '-czf', archive, '-C', root, 'fixture-source']).exitCode).toBe(0);
  const lock = JSON.parse(readFileSync(resolve(import.meta.dir, '../native/clef-runtime.lock.json'), 'utf8'));
  lock.runtime.source_archive_sha256 = createHash('sha256').update(readFileSync(archive)).digest('hex');
  writeFileSync(join(root, 'clef-runtime.lock.json'), JSON.stringify(lock));
  const localScript = join(root, 'build-clef.sh');
  copyFileSync(script, localScript);
  mkdirSync(join(root, 'patches'));
  copyFileSync(patchPath, join(root, 'patches/clef-output-embedding.patch'));
  const mockServer = join(root, 'mock-server');
  writeFileSync(mockServer, '#!/bin/sh\nprintf "fixture native version\\n"\n');
  chmodSync(mockServer, 0o700);
  writeFileSync(join(mockbin, 'cmake'), `#!/bin/sh
set -eu
if [ "$1" = --build ]; then
  mkdir -p "$2/bin"
  cp "$MOCK_CLEF_SERVER" "$2/bin/llama-server"
else
  mkdir -p "$4"
  printf 'fixture cache\\n' > "$4/CMakeCache.txt"
fi
`);
  chmodSync(join(mockbin, 'cmake'), 0o700);
  const result = Bun.spawnSync(['sh', localScript, out, archive], {
    env: { ...process.env, PATH: `${mockbin}:${process.env.PATH}`, MOCK_CLEF_SERVER: mockServer },
    stdout: 'pipe', stderr: 'pipe',
  });
  expect(result.stderr.toString()).toBe('');
  expect(result.exitCode).toBe(0);
  const receipt = JSON.parse(readFileSync(join(out, 'build-receipt.json'), 'utf8'));
  expect(receipt.source_archive_sha256).toBe(lock.runtime.source_archive_sha256);
  expect(receipt.source_patches).toEqual(lock.runtime.source_patches.map(({ file, sha256 }) => ({ file, sha256 })));
  expect(receipt.binary_sha256).toBe(createHash('sha256').update(readFileSync(mockServer)).digest('hex'));
  expect(receipt.inference_verified).toBe(false);
  expect(readFileSync(join(out, 'LICENSE.llama.cpp'), 'utf8')).toBe('fixture license, not upstream');
  const regression = join(root, 'operator-regression');
  const compile = Bun.spawnSync(['c++', '-std=c++17', join(out, 'source/src/llama-model-loader.cpp'), '-o', regression]);
  expect(compile.stderr.toString()).toBe('');
  expect(compile.exitCode).toBe(0);
  expect(Bun.spawnSync([regression]).exitCode).toBe(0);
});

test('a modified source patch is refused before creating output or extracting source', () => {
  const root = fixture(), out = join(root, 'runtime');
  copyFileSync(script, join(root, 'build-clef.sh'));
  copyFileSync(resolve(import.meta.dir, '../native/clef-runtime.lock.json'), join(root, 'clef-runtime.lock.json'));
  mkdirSync(join(root, 'patches'));
  writeFileSync(join(root, 'patches/clef-output-embedding.patch'), readFileSync(patchPath, 'utf8') + '\n');
  const result = Bun.spawnSync(['sh', join(root, 'build-clef.sh'), out, join(root, 'unused-archive')]);
  expect(result.exitCode).toBe(1);
  expect(result.stderr.toString()).toContain('source patch digest mismatch');
  expect(existsSync(out)).toBe(false);
});

test('source context drift refuses patch application before invoking the compiler', () => {
  const root = fixture(), source = join(root, 'fixture-source'), out = join(root, 'runtime');
  mkdirSync(join(source, 'src'), { recursive: true });
  writeFileSync(join(source, 'src/llama-model-loader.cpp'), '// incompatible loader revision\n');
  const archive = join(root, 'source.tar.gz');
  expect(Bun.spawnSync(['tar', '-czf', archive, '-C', root, 'fixture-source']).exitCode).toBe(0);
  const lock = JSON.parse(readFileSync(resolve(import.meta.dir, '../native/clef-runtime.lock.json'), 'utf8'));
  lock.runtime.source_archive_sha256 = createHash('sha256').update(readFileSync(archive)).digest('hex');
  writeFileSync(join(root, 'clef-runtime.lock.json'), JSON.stringify(lock));
  copyFileSync(script, join(root, 'build-clef.sh'));
  mkdirSync(join(root, 'patches'));
  copyFileSync(patchPath, join(root, 'patches/clef-output-embedding.patch'));
  const result = Bun.spawnSync(['sh', join(root, 'build-clef.sh'), out, archive]);
  expect(result.exitCode).not.toBe(0);
  expect(result.stdout.toString().toLowerCase()).toContain('failed');
  expect(existsSync(join(out, 'build'))).toBe(false);
});
