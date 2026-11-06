// Bounded MessagePack interchange with the native CMS bf_msgpack owner.
// Candidate values remain plain data; unsupported JavaScript objects are refused.
const text = new TextEncoder(), utf8 = new TextDecoder('utf-8', {fatal: true, ignoreBOM: true});
export const maxPacket = 512 * 1024;
const fail = message => { throw new Error(message); };
export function encode(value, {maxBytes = maxPacket, maxDepth = 32} = {}) {
  const bytes = new Uint8Array(maxBytes), view = new DataView(bytes.buffer); let at = 0, nodes = 0;
  const need = n => { if (at + n > maxBytes) fail('This draft batch is too large.'); };
  const byte = n => { need(1); bytes[at++] = n; };
  const unsigned = (n, size) => { need(size); for (let i = size - 1; i >= 0; i--) bytes[at++] = Number((BigInt(n) >> BigInt(i * 8)) & 255n); };
  const header = (n, fix, small, medium, large) => {
    if (fix && n < (fix === 0xa0 ? 32 : 16)) byte(fix | n);
    else if (small && n < 256) { byte(small); unsigned(n, 1); }
    else if (n < 65536) { byte(medium); unsigned(n, 2); }
    else { byte(large); unsigned(n, 4); }
  };
  const raw = body => { need(body.length); bytes.set(body, at); at += body.length; };
  function put(v, depth) {
    if (depth > maxDepth || ++nodes > 50000) fail('This draft is too deeply nested.');
    if (v === null) return byte(0xc0);
    if (typeof v === 'boolean') return byte(v ? 0xc3 : 0xc2);
    if (typeof v === 'number') {
      if (!Number.isFinite(v)) fail('Draft numbers must be finite.');
      if (!Number.isInteger(v) || Object.is(v, -0)) { byte(0xcb); need(8); view.setFloat64(at, v); at += 8; return; }
      if (!Number.isSafeInteger(v)) fail('Draft integers must be exactly representable.');
      if (v >= 0 && v < 128 || v >= -32 && v < 0) return byte(v & 255);
      if (v >= 0) { const size = v < 256 ? 1 : v < 65536 ? 2 : v < 4294967296 ? 4 : 8; byte(({1:0xcc,2:0xcd,4:0xce,8:0xcf})[size]); return unsigned(v, size); }
      const size = v >= -128 ? 1 : v >= -32768 ? 2 : v >= -2147483648 ? 4 : 8;
      byte(({1:0xd0,2:0xd1,4:0xd2,8:0xd3})[size]); return unsigned(v, size);
    }
    if (typeof v === 'string') {
      const body = text.encode(v); if (utf8.decode(body) !== v) fail('Draft text contains an invalid Unicode character.');
      header(body.length, 0xa0, 0xd9, 0xda, 0xdb); return raw(body);
    }
    if (v instanceof Uint8Array) { header(v.byteLength, 0, 0xc4, 0xc5, 0xc6); return raw(v); }
    if (Array.isArray(v)) { if (v.length > 20000) fail('This draft has too many items.'); header(v.length, 0x90, 0, 0xdc, 0xdd); for (const x of v) put(x, depth + 1); return; }
    if (v && (Object.getPrototypeOf(v) === Object.prototype || Object.getPrototypeOf(v) === null)) {
      const keys = Object.keys(v), descriptors = Object.getOwnPropertyDescriptors(v);
      if (keys.length > 4096 || Reflect.ownKeys(v).length !== keys.length) fail('This draft contains unsupported object properties.');
      header(keys.length, 0x80, 0, 0xde, 0xdf);
      for (const key of keys) {
        if (text.encode(key).length > 512 || !Object.hasOwn(descriptors[key], 'value')) fail('This draft contains an unsupported property.');
        put(key, depth + 1); put(descriptors[key].value, depth + 1);
      }
      return;
    }
    fail('This draft contains a value the native record owner cannot preserve.');
  }
  put(value, 0); return bytes.slice(0, at);
}
export function decode(input, {maxBytes = maxPacket, maxDepth = 32} = {}) {
  const bytes = input instanceof Uint8Array ? input : new Uint8Array(input);
  if (!bytes.length || bytes.length > maxBytes) fail('The native draft response has an invalid size.');
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength); let at = 0, nodes = 0;
  const need = n => { if (!Number.isInteger(n) || n < 0 || at + n > bytes.length) fail('The native draft response is truncated.'); };
  const uint = size => { need(size); let n = 0n; for (let i = 0; i < size; i++) n = (n << 8n) | BigInt(bytes[at++]); return n; };
  const integer = (size, signed = false) => {
    let n = uint(size); if (signed && n >> BigInt(size * 8 - 1)) n -= 1n << BigInt(size * 8);
    const result = Number(n); if (!Number.isSafeInteger(result)) fail('The native integer cannot be preserved exactly.'); return result;
  };
  const raw = size => { need(size); const result = bytes.slice(at, at + size); at += size; return result; };
  const string = size => utf8.decode(raw(size));
  const array = (n, depth) => { if (n > 20000) fail('The native array is too large.'); return Array.from({length: n}, () => get(depth + 1)); };
  const map = (n, depth) => {
    if (n > 4096) fail('The native map is too large.'); const result = {};
    for (let i = 0; i < n; i++) {
      const key = get(depth + 1);
      if (typeof key !== 'string' || text.encode(key).length > 512 || Object.hasOwn(result, key)) fail('The native map has an invalid or duplicate key.');
      Object.defineProperty(result, key, {value: get(depth + 1), enumerable: true, writable: true, configurable: true});
    }
    return result;
  };
  function get(depth) {
    if (depth > maxDepth || ++nodes > 50000) fail('The native draft response is too deeply nested.');
    need(1); const tag = bytes[at++];
    if (tag < 128) return tag;
    if (tag >= 0xe0) return tag - 256;
    if (tag >= 0xa0 && tag <= 0xbf) return string(tag & 31);
    if (tag >= 0x90 && tag <= 0x9f) return array(tag & 15, depth);
    if (tag >= 0x80 && tag <= 0x8f) return map(tag & 15, depth);
    if (tag === 0xc0) return null;
    if (tag === 0xc2 || tag === 0xc3) return tag === 0xc3;
    if (tag === 0xca || tag === 0xcb) { const size = tag === 0xca ? 4 : 8; need(size); const result = size === 4 ? view.getFloat32(at) : view.getFloat64(at); at += size; if (!Number.isFinite(result)) fail('The native number is not finite.'); return result; }
    if (tag >= 0xcc && tag <= 0xcf) return integer(2 ** (tag - 0xcc));
    if (tag >= 0xd0 && tag <= 0xd3) return integer(2 ** (tag - 0xd0), true);
    if (tag >= 0xc4 && tag <= 0xc6) return raw(integer(2 ** (tag - 0xc4)));
    if (tag >= 0xd9 && tag <= 0xdb) return string(integer(2 ** (tag - 0xd9)));
    if (tag === 0xdc || tag === 0xdd) return array(integer(tag === 0xdc ? 2 : 4), depth);
    if (tag === 0xde || tag === 0xdf) return map(integer(tag === 0xde ? 2 : 4), depth);
    fail('The native draft contains an unsupported MessagePack type.');
  }
  const result = get(0); if (at !== bytes.length) fail('The native draft response contains trailing data.'); return result;
}
