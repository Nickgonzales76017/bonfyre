// Local-only WebSub witness hub; no production network or authority.
import { appendFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { createServer } from 'node:net';

const root = process.argv[2];
if (!root) throw new Error('fixture root required');
const probe = createServer();
await new Promise(resolve => probe.listen(0, '127.0.0.1', resolve));
const port = probe.address().port;
await new Promise(resolve => probe.close(resolve));
const server = Bun.serve({
  hostname: '127.0.0.1', port,
  async fetch(request) {
    const path = new URL(request.url).pathname;
    if (request.method === 'GET' && path === '/topic')
      return new Response('<feed/>', { headers: { Link:
        `<http://127.0.0.1:${server.port}/hub>; rel="hub", <http://127.0.0.1:${server.port}/topic>; rel="self"` } });
    if (request.method === 'POST') {
      appendFileSync(join(root, 'forms'), `${await request.text()}\n`);
      return new Response('', { status: 202 });
    }
    return new Response('', { status: 404 });
  },
});
writeFileSync(join(root, 'port'), String(server.port));
