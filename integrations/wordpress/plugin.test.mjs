import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
const plugin = readFileSync(new URL('./plugin/aurekai-bridge.php', import.meta.url), 'utf8');
assert.match(plugin, /register_rest_route\('aurekai-bridge\/v1', '\/claim'/);
assert.match(plugin, /hash_hmac\('sha256'/);
assert.match(plugin, /permission_callback' => '__return_true'/);
assert.match(plugin, /cannot_infer/);
assert.doesNotMatch(plugin, /wp_insert_post|wp_update_post|wp_delete_post/);
console.log('wordpress plugin contract tests passed');
