/**
 * Small dependency-free async cache for hot read paths.
 *
 * It bounds memory, expires stale values, and coalesces concurrent misses so
 * one expensive loader cannot be started repeatedly by a request burst.
 */
export class AsyncTTLCache {
  constructor({ maxEntries = 256, ttlMs = 30_000, now = () => Date.now() } = {}) {
    if (!Number.isInteger(maxEntries) || maxEntries < 1) throw new RangeError('maxEntries must be positive');
    if (!Number.isFinite(ttlMs) || ttlMs < 0) throw new RangeError('ttlMs must be non-negative');
    this.maxEntries = maxEntries;
    this.ttlMs = ttlMs;
    this.now = now;
    this.entries = new Map();
    this.inflight = new Map();
    this.hits = 0;
    this.misses = 0;
    this.coalesced = 0;
  }

  _fresh(entry) {
    return entry && (this.ttlMs === 0 || entry.expiresAt > this.now());
  }

  get(key) {
    const entry = this.entries.get(key);
    if (!this._fresh(entry)) {
      if (entry) this.entries.delete(key);
      this.misses++;
      return undefined;
    }
    this.entries.delete(key);
    this.entries.set(key, entry);
    this.hits++;
    return entry.value;
  }

  set(key, value) {
    this.entries.delete(key);
    this.entries.set(key, { value, expiresAt: this.now() + this.ttlMs });
    while (this.entries.size > this.maxEntries) this.entries.delete(this.entries.keys().next().value);
    return value;
  }

  async getOrLoad(key, loader) {
    const cached = this.get(key);
    if (cached !== undefined) return cached;
    if (this.inflight.has(key)) {
      this.coalesced++;
      return this.inflight.get(key);
    }
    const pending = Promise.resolve().then(loader).then((value) => {
      // An invalidated request must not repopulate the cache after a refresh.
      return this.inflight.get(key) === pending ? this.set(key, value) : value;
    });
    this.inflight.set(key, pending);
    try {
      return await pending;
    } finally {
      this.inflight.delete(key);
    }
  }

  delete(key) {
    this.inflight.delete(key);
    return this.entries.delete(key);
  }

  clear() {
    this.entries.clear();
    this.inflight.clear();
  }

  stats() {
    return {
      entries: this.entries.size,
      inflight: this.inflight.size,
      hits: this.hits,
      misses: this.misses,
      coalesced: this.coalesced,
    };
  }
}
