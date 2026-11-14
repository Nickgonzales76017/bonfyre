/** In-memory interaction history. Domain adapters select the editable slice.
 * No persistence, source copying, effect execution, or authority is owned here.
 */
export function sameEdit(a, b) {
  if (Object.is(a, b)) return true;
  if (!a || !b || typeof a !== 'object' || typeof b !== 'object') return false;
  if (Array.isArray(a) || Array.isArray(b)) {
    return Array.isArray(a) && Array.isArray(b) && a.length === b.length &&
      a.every((value, index) => sameEdit(value, b[index]));
  }
  if (a instanceof Date || b instanceof Date) return a instanceof Date && b instanceof Date && Object.is(a.getTime(), b.getTime());
  if (ArrayBuffer.isView(a) || ArrayBuffer.isView(b)) {
    if (!ArrayBuffer.isView(a) || !ArrayBuffer.isView(b) || a.constructor !== b.constructor || a.byteLength !== b.byteLength) return false;
    const av = new Uint8Array(a.buffer, a.byteOffset, a.byteLength), bv = new Uint8Array(b.buffer, b.byteOffset, b.byteLength);
    return av.every((n, i) => n === bv[i]);
  }
  const plain = value => Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null;
  if (!plain(a) || !plain(b)) throw new TypeError('Select plain editable records, arrays, dates or typed arrays for history.');
  const ak = Object.keys(a), bk = Object.keys(b);
  return Object.getPrototypeOf(a) === Object.getPrototypeOf(b) &&
    ak.length === bk.length && ak.every(key => Object.hasOwn(b, key) && sameEdit(a[key], b[key]));
}

export function createEditHistory({capture, restore, limit = 60, equal = sameEdit}) {
  if (typeof capture !== 'function' || typeof restore !== 'function' ||
      !Number.isSafeInteger(limit) || limit < 1 || limit > 1000) {
    throw new TypeError('History requires a bounded editable-slice adapter.');
  }
  let past = [], future = [];
  const read = () => structuredClone(capture());
  const put = value => restore(structuredClone(value));
  const push = value => { past.push(value); if (past.length > limit) past.shift(); };
  return {
    get canUndo() { return past.length > 0; },
    get canRedo() { return future.length > 0; },
    clear() { past = []; future = []; },
    transact(change) {
      if (typeof change !== 'function' || change.constructor?.name === 'AsyncFunction') throw new TypeError('History transactions require a synchronous edit.');
      const before = read();
      try {
        const result = change();
        if (result && typeof result.then === 'function') {
          throw new TypeError('History transactions must be synchronous; persist separately.');
        }
        if (equal(before, capture())) return false;
        push(before); future = []; return true;
      } catch (error) { put(before); throw error; }
    },
    undo() {
      if (!past.length) return false;
      const now = read(), prior = past.at(-1);
      put(prior); past.pop(); future.push(now); return true;
    },
    redo() {
      if (!future.length) return false;
      const now = read(), next = future.at(-1);
      put(next); future.pop(); push(now); return true;
    }
  };
}
