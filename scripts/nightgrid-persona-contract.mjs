// Read the authored DEMO lattice; never invent roles or perform signup/grants.
import { closeSync, constants, fstatSync, openSync, readSync } from 'node:fs';

const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const fail = message => { throw new Error(`Night Grid contract: ${message}`); };
const atom = value => typeof value === 'string' && /^[a-z][a-z0-9_-]{0,62}$/.test(value);
function parseJSON(bytes, label) {
  try { return JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes)); }
  catch { fail(`${label} is not valid UTF-8 JSON`); }
}

export function nightgridPersonaRows(contract) {
  const rows = contract?.persona_lattice?.nightgrid;
  if (contract?.worlds?.nightgrid !== 'DEMO' || !object(contract.role_ranks) ||
      !Array.isArray(rows) || !rows.length || rows.length > 1024)
    fail('a bounded, nonempty DEMO lattice and role ranks are required');
  const seen = new Set();
  // Validate the entire lattice before emitting any account-producing input.
  return rows.map(row => {
    if (!object(row) || !atom(row.persona) || seen.has(row.persona) || !atom(row.rank) ||
        !Object.hasOwn(contract.role_ranks, row.rank) || !Number.isSafeInteger(contract.role_ranks[row.rank]) ||
        contract.role_ranks[row.rank] < 0 || typeof row.capabilities !== 'string' || !row.capabilities.trim() ||
        [...row.capabilities].some(char => char.codePointAt(0) < 0x20 || char.codePointAt(0) === 0x7f))
      fail('invalid persona, declared rank, or capability label');
    seen.add(row.persona);
    return [row.persona, row.rank, row.capabilities].join('\t');
  }).join('\n') + '\n';
}

export function nightgridSignupId(response) {
  const id = response?.user?.id;
  // BonfyreAuth's user ID and institution-grant argument are positive C ints.
  // This result enters the remote shell command, so strings are not coerced.
  if (!object(response) || !object(response.user) || !Number.isSafeInteger(id) || id < 1 || id > 0x7fffffff)
    fail('signup did not return a positive native user ID');
  return String(id);
}

function contractFile(path) {
  const fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const stat = fstatSync(fd, { bigint: true });
    if (!stat.isFile() || stat.size > 1048576n) fail('contract must be a regular file under 1 MiB');
    const bytes = Buffer.alloc(Number(stat.size) + 1);
    let size = 0;
    while (size < bytes.length) {
      const count = readSync(fd, bytes, size, bytes.length - size, size);
      if (!count) break;
      size += count;
    }
    const after = fstatSync(fd, { bigint: true });
    if (size !== Number(stat.size) || ['size', 'mtimeNs', 'ctimeNs'].some(key => stat[key] !== after[key]))
      fail('contract changed during read');
    return parseJSON(bytes.subarray(0, size), 'contract');
  } finally { closeSync(fd); }
}

if (import.meta.main) {
  try {
    const commands = {
      rows: async args => {
        if (args.length !== 1) fail('rows requires a contract path');
        process.stdout.write(nightgridPersonaRows(contractFile(args[0])));
      },
      'signup-id': async args => {
        if (args.length) fail('signup-id takes JSON only on stdin');
        let size = 0; const parts = [];
        for await (const part of Bun.stdin.stream()) {
          size += part.byteLength;
          if (size > 64 * 1024) fail('signup response exceeds 64 KiB');
          parts.push(part);
        }
        const response = parseJSON(Buffer.concat(parts), 'signup response');
        console.log(nightgridSignupId(response));
      },
    };
    const [verb, ...args] = process.argv.slice(2);
    if (!Object.hasOwn(commands, verb)) fail('usage: nightgrid-persona-contract.mjs rows CONTRACT | signup-id');
    await commands[verb](args);
  } catch (error) { console.error(error.message); process.exitCode = 1; }
}
