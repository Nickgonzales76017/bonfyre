# Bonfyre ↔ WordPress bridge

`bridge.mjs` is a framework-neutral boundary for WordPress adapters. It keeps
WordPress transport concerns separate from Bonfyre jobs and exposes three
stable operations:

- `fetchPost(id)`: reads a post through the WordPress REST API, with bounded
  TTL/LRU caching and concurrent-request coalescing.
- `normalizeWebhook(...)`: verifies an HMAC-SHA256 webhook and converts it to a
  stable Bonfyre event envelope with an idempotency key.
- `invalidate(id)`: removes a post after a webhook or explicit refresh.

WordPress plugins can call this contract from REST, WP-Cron, Action Scheduler,
or a queue worker without coupling Bonfyre to WordPress internals. The bridge
does not publish, mutate, or delete WordPress content.

Example webhook flow:

```js
const event = bridge.normalizeWebhook({ body, rawBody, signature, secret });
bridge.invalidate(event.source.post_id);
```
