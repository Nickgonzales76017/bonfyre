# WordPress ↔ Bonfÿre: observed posts, signed change delivery, CMS exchange

**This is not a generic WordPress "capability" and not a remote-site migration.** The repository has three related but separately verified paths: an HTTP bridge for reading a WordPress site and normalizing signed updates, a PHP plugin/control-proof boundary, and `bonfyre site`'s **WordPress WXR export/import format**. A completed website migration requires additional customer- and server-level work.

[Full workflow map](../../docs/real-workflows-and-interoperability.md#1-wordpress-a-bridge-an-export-format-and-a-migration-are-three-different-jobs) · [Bridge implementation](bridge.mjs) · [Bridge tests](bridge.test.mjs) · [Plugin](plugin/aurekai-bridge.php) · [Native site export](../../cmd/BonfyreWebAddress/src/site_product.c)

## What actually happens

~~~mermaid
flowchart LR
  WP["WordPress /wp-json/ routes"] --> R["discoverPublicCapabilities / fetchPost"]
  HOOK["WordPress plugin signed event"] --> SIG["HMAC SHA-256 raw-body verification"]
  SIG --> EN["site / post ID / modified version → idempotency key"]
  EN --> N["toNativeCmsEntry: external ID + replay metadata"]
  N --> STORE["createNativeCmsClient: native CMS sync"]
  STORE --> REVIEW["Separate review / publish / application continuation"]
  OWN["Native authored site"] --> OUT["bonfyre site export"]
  OUT --> WXR["WordPress WXR 1.2"]
  OUT --> FRAPPE["Frappe records"]
  OUT --> HTML["Script-free HTML snapshot"]
~~~

### Public WordPress discovery

`createWordPressBridge({baseUrl, token, fetchImpl})` exposes:

- `discoverPublicCapabilities()`: reads `/wp-json/` and returns *only advertised routes* for posts, pages, media, taxonomies, WooCommerce Store API and ActivityPub. Public route presence **does not prove** authenticated access, plugin installation, site control or an allowed write.
- `fetchPost(id)`: loads `/wp-json/wp/v2/posts/{id}` with bounded TTL/LRU cache (default 15 seconds and 512 entries) and coalescing of concurrent reads.
- `invalidate(id)`: removes a cached post after an update event.

### Signed webhook and site claim

`normalizeWebhook({body,rawBody,signature,secret})` verifies HMAC-SHA256 on the raw request bytes. The resulting `bonfyre.wordpress.bridge.v1` event preserves `source.site`, `source.post_id`, `source.post_type`, title, URL, publication status, timestamp and an idempotency key of `site:postId:version`.

`issueSiteClaimChallenge` and `verifySiteClaimResponse` prove control of the *installed bridge secret* for one clean HTTPS site origin. They do **not** prove ownership of a human account, institution, brand, article rights or publishing authorization.

### Native CMS sync, not WordPress publication

`toNativeCmsEntry(event)` yields `bonfyre.native.cms.entry.v1` with `external_id=site:postId`, `idempotency_key` and source metadata. `createNativeCmsClient().syncWordPressEvent(event)` uses those coordinates for a replay-safe native CMS create/update. The independent native Qwen generation client can return a hashed generation receipt; `syncGenerationResult` writes it to a namespaced `cms_generation` record. Neither route grants the right to mutate WordPress.

Example of the safe projection boundary:

~~~js
import {createWordPressBridge,toNativeCmsEntry,normalizeWebhook} from './bridge.mjs';

// webhookBody is the already-parsed body, rawBody the original request bytes.
// Signature and secret come from the verified, configured endpoint.
const event=normalizeWebhook({
  body:webhookBody,rawBody,signature,secret,event:'post.updated'
});
const nativeDraft=toNativeCmsEntry(event,{contentType:'cms_article',namespace:'wordpress'});
// Review nativeDraft or send it to the configured native CMS client.
// This operation alone performs no WordPress publish.
~~~

## Separately: export a native Bonfÿre site *as* WordPress content

~~~bash
bonfyre site export notes --format wordpress --out notes.xml
bonfyre site export notes --format frappe --out notes.json
bonfyre site export notes --out notes.html
~~~

The native [site-product test](../../cmd/BonfyreWebAddress/tests/site_product_test.sh) verifies **WXR version 1.2** output, Frappe document output, re-import back into Bonfÿre, post-title/body equality and escaping of markup characters. Offline HTML export inlines available site stylesheets and removes scripts/remote runtime dependence. **These are local format and rendering checks; the test does not connect to or upload content into an actual WordPress/Frappe host.**

## Migration from WordPress or older PHP hosting

A real portfolio migration also needs the customer's current backups, WordPress/plugins/theme/PHP and database compatibility, domain/TLS/mail and redirects, media, forms, user/accounts, deployment window and reversal plan. [BonfyreBrief's read-only hosting inventory](../../cmd/BonfyreBrief/src/inventory.c) recognizes exported cPanel, InterWorx and Plesk listings. [BonfyreBridge](../../cmd/BonfyreBridge/src/main.c) can compare source/destination database values and type changes; it is not a server migration guarantee.

**Status and tests:** [`bridge.test.mjs`](bridge.test.mjs) and the installed plugin contract's static tests are recorded. The registry explicitly states that the plugin contract has **not been exercised inside a real WordPress runtime** and that bridge-control proof is not an institutional publication claim. Remote import, actual PHP upgrade, live application behavior, and migration cutover each need their own witnesses.
