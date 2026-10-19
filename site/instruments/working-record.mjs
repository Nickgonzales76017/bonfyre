// Continuation of the four existing instruments in their existing record store.
// This module owns transient save coordination, not another database or account.
export const instrumentKinds = Object.freeze({traces:'trace-view.mjs', research:'research-view.mjs', schema:'schema-view.mjs', product:'product-view.mjs'});
const validRevision = n => Number.isSafeInteger(n) && n >= 0;
export function checkRecordKey(kind, key) {
  if (!Object.hasOwn(instrumentKinds, kind) || typeof key !== 'string' || !key.startsWith(`draft:instruments:${kind}:`) || !/^[A-Za-z0-9:._-]{1,240}$/.test(key) || key === `draft:instruments:${kind}:`) throw Error('Choose a saved record belonging to this instrument.');
  return key;
}
export async function readExactRecord(store, kind, key, revision = null) {
  checkRecordKey(kind, key);
  if (revision !== null && (!validRevision(revision) || revision < 1)) throw Error('Choose a valid saved revision.');
  const rows = (await store.readLocal(key)).filter(row => row.key === key);
  if (rows.length !== 1 || rows[0].value?.kind !== kind || !validRevision(rows[0].revision) || rows[0].revision < 1) throw Error('That saved work is not available in this browser. No example was substituted.');
  if (revision !== null && rows[0].revision !== revision) throw Error(`This link names revision ${revision}; the available saved record is revision ${rows[0].revision}. Choose Open saved to review the current version.`);
  return structuredClone(rows[0]);
}
function receipt(rows, key, revision) {
  const matches = rows.filter(row => row.key === key);
  if (matches.length !== 1 || matches[0].revision !== revision) throw Error('The save response did not identify the expected record revision. Reopen saved work before repeating a write.');
  return matches[0];
}
export class WorkingRecord {
  #kind; #store; #capture; #load; #notify; #key; #revision = 0; #epoch = 0; #dirty = false; #busy = false; #loading = false; #closed = false;
  constructor({kind, store, capture, load, onChange = () => {}, key = `draft:instruments:${kind}:${crypto.randomUUID()}`}) {
    checkRecordKey(kind, key);
    if (typeof capture !== 'function' || typeof load !== 'function' || typeof store?.saveLocalBatch !== 'function' || typeof store?.readLocal !== 'function') throw Error('Use the existing instrument and record store.');
    this.#kind = kind; this.#store = store; this.#capture = capture; this.#load = load; this.#notify = onChange; this.#key = key;
  }
  get kind() { return this.#kind; }
  get key() { return this.#key; }
  get revision() { return this.#revision; }
  get dirty() { return this.#dirty; }
  get busy() { return this.#busy; }
  get loading() { return this.#loading; }
  #announce() { try { this.#notify(this); } catch { /* A view failure cannot turn a completed save into a failed write. */ } }
  changed() {
    if (this.#closed || this.#loading) throw Error('Finish opening the selected work before editing it.');
    this.#epoch++; this.#dirty = true; this.#announce();
  }
  #begin() {
    if (this.#closed || this.#busy) throw Error('Finish the current record operation first.');
    const value = structuredClone(this.#capture());
    if (value?.kind !== this.#kind) throw Error('The instrument returned a different record kind.');
    const prepared = {key:this.#key, value, expectedRevision:this.#revision, epoch:this.#epoch};
    this.#busy = true; this.#announce(); return prepared;
  }
  #accepted(prepared, row) {
    this.#revision = row.revision;
    this.#dirty = prepared.epoch !== this.#epoch;
  }
  #release() { this.#busy = false; this.#loading = false; this.#announce(); }
  static async saveTogether(records) {
    if (!Array.isArray(records) || !records.length || records.length > 4 || new Set(records).size !== records.length || records.some(r => !(r instanceof WorkingRecord))) throw Error('Choose one to four distinct working instruments.');
    const store = records[0].#store;
    if (records.some(r => r.#store !== store) || new Set(records.map(r => r.key)).size !== records.length) throw Error('A joint save requires distinct records in the same existing destination.');
    const prepared = [];
    try {
      for (const record of records) prepared.push(record.#begin());
      const rows = await store.saveLocalBatch(prepared.map(({key,value,expectedRevision}) => ({key,value,expectedRevision})));
      if (!Array.isArray(rows) || rows.length !== prepared.length) throw Error('The joint save response is incomplete. Reopen saved work before repeating it.');
      const confirmed = prepared.map(p => receipt(rows, p.key, p.expectedRevision + 1));
      records.forEach((record,i) => record.#accepted(prepared[i],confirmed[i]));
      return confirmed;
    } finally { prepared.forEach((_,i) => records[i].#release()); }
  }
  async save() { return (await WorkingRecord.saveTogether([this]))[0]; }
  async handoff(candidate) {
    if (this.#kind !== 'research' || !candidate || typeof candidate.id !== 'string' || !/^[A-Za-z0-9._-]{1,180}$/.test(candidate.id)) throw Error('Use a sourced research opportunity.');
    const prepared = this.#begin();
    try {
      const candidateKey = 'draft:instruments:incoming:' + candidate.id;
      const kept = structuredClone(candidate);
      const rows = await this.#store.saveLocalBatch([
        {key:prepared.key,value:prepared.value,expectedRevision:prepared.expectedRevision},
        {key:candidateKey,value:{candidate:kept,sourceRecord:prepared.key,sourceRevision:prepared.expectedRevision+1},expectedRevision:0}
      ]);
      if (!Array.isArray(rows) || rows.length !== 2) throw Error('The source-to-opportunity receipt is incomplete. Reopen saved work before repeating it.');
      const source = receipt(rows,prepared.key,prepared.expectedRevision+1), incoming = receipt(rows,candidateKey,1);
      this.#accepted(prepared,source);
      return structuredClone(incoming);
    } finally { this.#release(); }
  }
  async open(key, revision = null) {
    if (this.#closed || this.#busy) throw Error('Finish the current record operation first.');
    this.#busy = true; this.#loading = true; this.#announce();
    try {
      const row = await readExactRecord(this.#store,this.#kind,key,revision);
      await this.#load(structuredClone(row.value));
      this.#key = row.key; this.#revision = row.revision; this.#epoch++; this.#dirty = false;
      return row;
    } finally { this.#release(); }
  }
  dispose() {
    if (this.#busy) throw Error('Finish the current save or opening operation before closing this instrument.');
    this.#closed = true;
  }
}
