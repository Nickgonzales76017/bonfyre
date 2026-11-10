const MAX_INPUT_BYTES = 1024 * 1024;
const OPS = Object.freeze({ rfq: 1, quote: 2, compare: 3, allocate: 4, approvalRequest: 5, orderIntent: 6, contractPreflight: 7 });
const enc = new TextEncoder(), dec = new TextDecoder('utf-8', { fatal: true });

export async function createProcurementKernel(bytes, { random = crypto } = {}) {
  if (!(bytes instanceof ArrayBuffer) && !ArrayBuffer.isView(bytes)) throw new TypeError('procurement wasm bytes required');
  const module = await WebAssembly.compile(bytes instanceof ArrayBuffer ? bytes : bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength));
  const imports = WebAssembly.Module.imports(module);
  const allowed = new Set(['fd_prestat_get','fd_prestat_dir_name','proc_exit','random_get']);
  if (imports.length !== 4 || imports.some(i => i.module !== 'wasi_snapshot_preview1' || !allowed.has(i.name))) throw new Error('unexpected procurement kernel imports');
  let instance;
  const wasi = {
    fd_prestat_get: () => 8,
    fd_prestat_dir_name: () => 8,
    proc_exit: code => { throw new Error('procurement wasm exited '+code); },
    random_get: (ptr, len) => {
      if (!instance || !Number.isSafeInteger(ptr) || !Number.isSafeInteger(len) || ptr < 0 || len < 0 || ptr + len > instance.exports.memory.buffer.byteLength) return 21;
      const view = new Uint8Array(instance.exports.memory.buffer, ptr, len);
      if (!random?.getRandomValues) return 29;
      random.getRandomValues(view);
      return 0;
    }
  };
  instance = await WebAssembly.instantiate(module, { wasi_snapshot_preview1: wasi });
  const e = instance.exports;
  for (const name of ['memory','malloc','free','bf_procurement_wasm_run','bf_procurement_wasm_reset','bf_procurement_wasm_output','bf_procurement_wasm_output_len','bf_procurement_wasm_error','bf_procurement_wasm_error_len']) {
    if (!(name in e)) throw new Error('unexpected procurement kernel ABI: missing '+name);
  }
  if (!(e.memory instanceof WebAssembly.Memory)) throw new Error('unexpected procurement kernel memory');
  if (typeof e._initialize === 'function') e._initialize();
  function read(ptr, len) {
    if (!Number.isSafeInteger(ptr) || !Number.isSafeInteger(len) || ptr < 0 || len < 0 || ptr + len > e.memory.buffer.byteLength) throw new Error('procurement kernel returned invalid memory');
    return dec.decode(new Uint8Array(e.memory.buffer, ptr, len));
  }
  function call(op, value, asOf = '') {
    if (!Object.hasOwn(OPS, op)) throw new RangeError('unknown procurement operation');
    const input = enc.encode(JSON.stringify(value)), stamp = enc.encode(asOf || '');
    if (!input.length || input.length > MAX_INPUT_BYTES || stamp.length > 64) throw new RangeError('procurement input exceeds bounds');
    const ip = e.malloc(input.length), ap = stamp.length ? e.malloc(stamp.length) : 0;
    if (!ip || (stamp.length && !ap)) { if (ip) e.free(ip); if (ap) e.free(ap); throw new Error('procurement kernel allocation failed'); }
    try {
      new Uint8Array(e.memory.buffer, ip, input.length).set(input);
      if (stamp.length) new Uint8Array(e.memory.buffer, ap, stamp.length).set(stamp);
      const ok = e.bf_procurement_wasm_run(OPS[op], ip, input.length, ap, stamp.length);
      if (!ok) throw new Error(read(e.bf_procurement_wasm_error(), e.bf_procurement_wasm_error_len()) || 'procurement input refused');
      return JSON.parse(read(e.bf_procurement_wasm_output(), e.bf_procurement_wasm_output_len()));
    } finally {
      e.free(ip); if (ap) e.free(ap); e.bf_procurement_wasm_reset();
    }
  }
  return Object.freeze({
    prepareRFQ: value => call('rfq', value),
    observeQuote: value => call('quote', value),
    compareQuotes: (value, { asOf = '' } = {}) => call('compare', value, asOf),
    allocateQuotes: value => call('allocate', value),
    prepareApprovalRequest: value => call('approvalRequest', value),
    prepareOrderIntent: value => call('orderIntent', value),
    prepareProtocolPreflight: value => call('contractPreflight', value)
  });
}
