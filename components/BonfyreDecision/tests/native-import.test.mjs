import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { importNativeClefHead } from '../native-import.mjs';
import { compileNativeDecisionHead, nativeDecisionBinding } from '../native-head.mjs';
import { headFixture, packedSpans } from './native-head-fixture.mjs';

const binding = nativeDecisionBinding(), temporary = [];
const digest = bytes => createHash('sha256').update(bytes).digest('hex');
afterEach(() => { for (const dir of temporary.splice(0)) rmSync(dir, { recursive: true, force: true }); });

function fixture({ dtype = 'F32', exhaustive = false } = {}) {
  const dir = mkdtempSync(join(tmpdir(), 'bonfyre-head-import-')); temporary.push(dir);
  const sample = headFixture(binding, exhaustive ? { hidden_size: 256, width: 256, heads: 8,
    routing_layers: 0, layers: 0 } : {});
  const header = { __metadata__: { format: 'pt', fixture: 'synthetic-not-released-weights' } }, chunks = [];
  let cursor = 0, ordinal = 0;
  // Reverse native layout order: name-bound import must not depend on ordering.
  for (const [name, tensor] of Object.entries(sample.tensors).reverse()) {
    const bytes = Buffer.alloc(tensor.data.length * (dtype === 'F32' ? 4 : 2));
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    for (let i = 0; i < tensor.data.length; i++) {
      if (dtype === 'F32') view.setFloat32(i * 4, tensor.data[i], true);
      else {
        if (exhaustive) {
          // Every finite signed half pattern, including subnormals and -0.
          let bits = ordinal++ % 65536;
          const exponentMask = dtype === 'F16' ? 0x7c00 : 0x7f80;
          if ((bits & exponentMask) === exponentMask) bits = 0;
          view.setUint16(i * 2, bits, true);
        } else if (dtype === 'F16') view.setFloat16(i * 2, tensor.data[i], true);
        else {
          const f = new DataView(new ArrayBuffer(4)); f.setFloat32(0, tensor.data[i], true);
          view.setUint16(i * 2, f.getUint32(0, true) >>> 16, true);
        }
        if (dtype === 'F16') tensor.data[i] = view.getFloat16(i * 2, true);
        else {
          // Independent reference: standard F32 little-endian byte expansion.
          const f = Buffer.from([0, 0, bytes[i * 2], bytes[i * 2 + 1]]);
          tensor.data[i] = f.readFloatLE(0);
        }
      }
    }
    header[name] = { dtype, shape: tensor.shape, data_offsets: [cursor, cursor + bytes.length] };
    cursor += bytes.length; chunks.push(bytes);
  }
  const body = Buffer.concat(chunks), configBytes = Buffer.from(JSON.stringify(sample.config));
  const configPath = join(dir, 'joint_head_config.json'), weightsPath = join(dir, 'joint_head.safetensors');
  writeFileSync(configPath, configBytes);
  const save = (changedHeader = header, changedBody = body) => {
    const raw = Buffer.from(typeof changedHeader === 'string' ? changedHeader : JSON.stringify(changedHeader));
    const padded = Buffer.concat([raw, Buffer.alloc((8 - raw.length % 8) % 8, 0x20)]);
    const prefix = Buffer.alloc(8); prefix.writeBigUInt64LE(BigInt(padded.length));
    const bytes = Buffer.concat([prefix, padded, changedBody]); writeFileSync(weightsPath, bytes); return bytes;
  };
  const bytes = save();
  return { ...sample, dir, header, body, save, bytes, configBytes, configPath, weightsPath,
    options: { binding, configPath, weightsPath } };
}

describe('bounded native Clef safetensors import', () => {
  test('imports shuffled real-layout F32 tensors and executes the actual native head', () => {
    const f = fixture();
    const head = importNativeClefHead({ ...f.options, expectedConfigDigest: digest(f.configBytes),
      expectedWeightDigest: digest(f.bytes) });
    const direct = compileNativeDecisionHead(f.config, f.tensors, { binding });
    expect(head.artifact_digest).toBe(direct.artifact_digest);
    expect(head.source).toEqual({ format: 'safetensors', config_digest: digest(f.configBytes),
      weights_digest: digest(f.bytes), weight_bytes: f.bytes.length });
    expect(Object.isFrozen(head)).toBe(true); expect(Object.isFrozen(head.source)).toBe(true);
    const spans = packedSpans(f.fields);
    const arena = () => new Float32Array(binding.workspace(head.handle,
      { tokens: f.hidden.length / f.config.hidden_size, fields: f.fields.length, options: 7 }));
    expect([...binding.run(head.handle, f.hidden, spans.fields, spans.options, f.lexical_options, arena())])
      .toEqual([...binding.run(direct.handle, f.hidden, spans.fields, spans.options, f.lexical_options, arena())]);
  });

  for (const dtype of ['F16', 'BF16']) test(`${dtype}: all finite 16-bit patterns match independent reference conversion`, () => {
    const f = fixture({ dtype, exhaustive: true });
    const head = importNativeClefHead(f.options);
    expect(head.artifact_digest).toBe(compileNativeDecisionHead(f.config, f.tensors, { binding }).artifact_digest);
    expect(head.source.weights_digest).toBe(digest(f.bytes));
  });

  test('refuses missing/extra tensors, wrong rank, wrong shape and unsupported dtypes', () => {
    const f = fixture(), name = Object.keys(f.header)[1];
    for (const mutate of [
      h => { delete h[name]; }, h => { h.extra = h[name]; },
      h => { h[name].shape.push(1); }, h => { h[name].shape[0] += 1; },
      h => { h[name].dtype = 'F64'; }, h => { h[name].dtype = '__proto__'; },
      h => { h[name].unexpected = true; }, h => { h.__metadata__.invalid = 3; },
    ]) {
      const changed = structuredClone(f.header); mutate(changed); f.save(changed);
      expect(() => importNativeClefHead(f.options)).toThrow();
    }
  });

  test('refuses holes, overlap, truncated data, trailing data and unsafe/negative/fractional offsets', () => {
    const f = fixture(), names = Object.keys(f.header).slice(1);
    for (const mutate of [
      h => { h[names[0]].data_offsets = [1, 5]; },
      h => { h[names[1]].data_offsets = h[names[0]].data_offsets; },
      h => { h[names[0]].data_offsets[0] = -1; },
      h => { h[names[0]].data_offsets[1] = 1.5; },
      h => { h[names[0]].data_offsets[1] = Number.MAX_SAFE_INTEGER + 1; },
    ]) {
      const changed = structuredClone(f.header); mutate(changed); f.save(changed);
      expect(() => importNativeClefHead(f.options)).toThrow();
    }
    f.save(f.header, f.body.subarray(0, -1)); expect(() => importNativeClefHead(f.options)).toThrow();
    f.save(f.header, Buffer.concat([f.body, Buffer.from([0])])); expect(() => importNativeClefHead(f.options)).toThrow();
  });

  test('rejects NaN and infinities for each supported source dtype before native compile', () => {
    for (const [dtype, patterns] of [
      ['F32', [0x7fc00000, 0x7f800000, 0xff800000]],
      ['F16', [0x7e00, 0x7c00, 0xfc00]], ['BF16', [0x7fc0, 0x7f80, 0xff80]],
    ]) {
      const f = fixture({ dtype });
      for (const bits of patterns) {
        const body = Buffer.from(f.body);
        if (dtype === 'F32') body.writeUInt32LE(bits); else body.writeUInt16LE(bits);
        f.save(f.header, body);
        expect(() => importNativeClefHead(f.options)).toThrow('nonfinite weight');
      }
    }
  });

  test('rejects duplicate decoded JSON keys in config, tensor descriptors and metadata', () => {
    const f = fixture();
    writeFileSync(f.configPath, JSON.stringify(f.config).replace('{', '{"hidden_\\u0073ize":6,'));
    expect(() => importNativeClefHead(f.options)).toThrow('duplicate key');
    writeFileSync(f.configPath, f.configBytes);
    for (const header of [
      JSON.stringify(f.header).replace('"dtype":"F32"', '"dtype":"F32","dtyp\\u0065":"F32"'),
      JSON.stringify(f.header).replace('"format":"pt"', '"format":"pt","format":"pt"'),
    ]) { f.save(header); expect(() => importNativeClefHead(f.options)).toThrow('duplicate key'); }
  });

  test('rejects invalid config and non-UTF8 / oversized / malformed headers', () => {
    const f = fixture();
    for (const config of [{ ...f.config, width: 3 }, { ...f.config, layers: -1 },
      { ...f.config, layers: 0.5 }, { ...f.config, hidden_size: true }, { ...f.config, unknown: 1 }]) {
      writeFileSync(f.configPath, JSON.stringify(config)); expect(() => importNativeClefHead(f.options)).toThrow();
    }
    writeFileSync(f.configPath, f.configBytes);
    const huge = Buffer.from(f.bytes); huge.writeBigUInt64LE(2n ** 63n);
    writeFileSync(f.weightsPath, huge); expect(() => importNativeClefHead(f.options)).toThrow('header length');
    const utf8 = Buffer.from(f.bytes); utf8[10] = 0xff;
    writeFileSync(f.weightsPath, utf8); expect(() => importNativeClefHead(f.options)).toThrow('UTF-8 JSON');
    f.save('[]'); expect(() => importNativeClefHead(f.options)).toThrow('begin with an object');
    f.save('{'); expect(() => importNativeClefHead(f.options)).toThrow('JSON');
  });

  test('budget and source identity refusal cannot invoke native compilation', () => {
    const f = fixture(); let compiled = false;
    const guarded = { ...binding, compile() { compiled = true; throw new Error('must not compile'); } };
    for (const options of [
      { maxWeightBytes: 4 }, { maxHeaderBytes: 4 }, { maxImportBytes: 4 },
      { maxWeightBytes: NaN }, { maxHeaderBytes: -1 }, { maxImportBytes: 0.5 },
      { expectedWeightDigest: '0'.repeat(64) }, { expectedConfigDigest: '0'.repeat(64) },
      { expectedWeightDigest: 'not-a-digest' },
    ]) expect(() => importNativeClefHead({ ...f.options, binding: guarded, ...options })).toThrow();
    expect(compiled).toBe(false);
    const link = join(f.dir, 'link'); symlinkSync(f.weightsPath, link);
    expect(() => importNativeClefHead({ ...f.options, weightsPath: link })).toThrow();
    expect(() => importNativeClefHead({ ...f.options, weightsPath: f.dir })).toThrow('regular file');
  });
});
