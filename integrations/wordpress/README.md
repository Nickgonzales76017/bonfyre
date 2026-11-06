# Bonfyre ↔ WordPress bridge

`bridge.mjs` is a framework-neutral boundary for WordPress adapters. It keeps
WordPress transport concerns separate from Bonfyre jobs and exposes three
stable operations:

- `fetchPost(id)`: reads a post through the WordPress REST API, with bounded
  TTL/LRU caching and concurrent-request coalescing.
- `discoverPublicCapabilities()`: reads only the public REST route index and
  reports the exact post/page/media/taxonomy/Woo/ActivityPub routes it found.
  It does not treat a WordPress fingerprint as write access or site authority.
- `normalizeWebhook(...)`: verifies an HMAC-SHA256 webhook and converts it to a
  stable Bonfyre event envelope with an idempotency key.
- `toNativeCmsEntry(event)`: projects that envelope into the native CMS entry
  contract without permitting a publish or other mutation.
- `createNativeCmsClient(...)`: connects to the native CMS CRUD surface and
  performs an external-ID-based create/update sync for replay-safe delivery.
- `createNativeGenerationClient(...)`: executes the verified native Qwen FPQ
  binary and returns a hashed `bonfyre.native.generation.result.v1` receipt.
- `syncGenerationResult(...)`: projects that receipt into the namespaced
  `cms_generation` content type for CMS, WordPress, Ledger, and analytics
  consumers.
- `invalidate(id)`: removes a post after a webhook or explicit refresh.

WordPress plugins can call this contract from REST, WP-Cron, Action Scheduler,
or a queue worker without coupling Bonfyre to WordPress internals. The bridge
does not publish, mutate, or delete WordPress content.

Example webhook flow:

```js
const event = bridge.normalizeWebhook({ body, rawBody, signature, secret });
bridge.invalidate(event.source.post_id);
const entry = toNativeCmsEntry(event);
const cms = createNativeCmsClient({ baseUrl: 'http://127.0.0.1:8800', token });
await cms.syncWordPressEvent(event);

const result = await generation.generate({ prompt: 'def fibonacci(n):\n', maxNewTokens: 20, greedy: true });
await cms.syncGenerationResult(result);
```
