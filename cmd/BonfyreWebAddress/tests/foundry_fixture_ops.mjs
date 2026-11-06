#!/usr/bin/env bun
import { readFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { createServer } from 'node:net';

const [operation, ...args] = process.argv.slice(2);
switch (operation) {
  case 'seed-contracts': {
    const [cms, db, schemas, catalog] = args;
    if (![cms, db, schemas, catalog].every(Boolean)) throw new Error('seed-contracts CMS DB SCHEMAS CATALOG');
    for (const contract of JSON.parse(readFileSync(catalog, 'utf8')).contracts) {
      const result = spawnSync(cms, ['entry', 'create', 'object_contract', JSON.stringify(contract),
        '--ns', 'root', '--db', db, '--schemas', schemas], { encoding: 'utf8' });
      if (result.status !== 0) throw new Error(`object contract refused: ${contract.operation || contract.id}`);
    }
    break;
  }
  case 'port': {
    const server = createServer();
    server.listen(0, '127.0.0.1', () => {
      console.log(server.address().port);
      server.close();
    });
    break;
  }
  case 'session-token': {
    const response = JSON.parse(await Bun.stdin.text());
    if (!response?.session?.token) throw new Error('login response has no session token');
    console.log(response.session.token);
    break;
  }
  default: throw new Error('unknown Foundry fixture operation');
}
