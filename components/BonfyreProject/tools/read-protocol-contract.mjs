// Glue over BonfyreFacet's native semantic owner, never an authority issuer.
import { openSync, readSync, closeSync, fstatSync, constants } from 'node:fs';

const MAX_COMPILE_BYTES = 8 * 1024 * 1024;
const READ_RELATIONS = ['DESCRIBE', 'QUERY', 'DISCOVER'];
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);

export function selectReadFacet(doc) {
  if (!object(doc) || !Array.isArray(doc.facets)) throw new Error('compiler result needs a facets array');
  if (doc.facets.length > 100000) throw new Error('compiler facet budget exceeded');
  for (const facet of doc.facets) {
    if (!object(facet)) throw new Error('compiler facet is not an object');
    if (facet.phenotype_derivation === 'shape' && READ_RELATIONS.includes(facet.relation_kind) &&
      (typeof facet.protocol_occurrence !== 'string' || facet.protocol_occurrence.length > 255 ||
       !/^[A-Za-z0-9][A-Za-z0-9:._/-]*$/.test(facet.protocol_occurrence)))
      throw new Error('read facet has an invalid protocol occurrence');
  }
  for (const relation of READ_RELATIONS) {
    const facet = doc.facets.find(facet => facet.phenotype_derivation === 'shape' && facet.relation_kind === relation);
    if (facet) return { protocol: facet.protocol_occurrence, relation };
  }
  return null;
}

function readCompiler(path) {
  const fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const before = fstatSync(fd);
    if (!before.isFile() || before.size > MAX_COMPILE_BYTES) throw new Error('compiler output exceeds regular-file budget');
    const bytes = Buffer.alloc(before.size);
    let offset = 0;
    while (offset < bytes.length) {
      const n = readSync(fd, bytes, offset, bytes.length - offset, offset);
      if (!n) throw new Error('compiler output changed during read');
      offset += n;
    }
    const after = fstatSync(fd);
    if (before.size !== after.size || before.mtimeMs !== after.mtimeMs || before.ctimeMs !== after.ctimeMs)
      throw new Error('compiler output changed during read');
    return JSON.parse(bytes.toString('utf8'));
  } finally { closeSync(fd); }
}

function count(value) {
  if (!/^(0|[1-9][0-9]*)$/.test(value) || !Number.isSafeInteger(Number(value))) throw new Error('invalid summary count');
  return Number(value);
}

// The current native CLI searches argv for flags, including values, and some
// evidence rows interpolate values into JSON. Refuse ambiguous values at this
// boundary; quoting the shell argument does not repair either native behavior.
function ownerValues(values) {
  const unsafeCharacter = value => [...value].some(char => char.codePointAt(0) < 32 || char === '\x7f' || char === '"' || char === '\\');
  if (!values.length || values.some(value => !value || value.startsWith('-') || unsafeCharacter(value)))
    throw new Error('native owner argument must be non-option, nonempty and JSON-safe');
}

if (import.meta.main) {
  try {
    const [mode, ...args] = process.argv.slice(2);
    if (mode === 'owner-values') {
      ownerValues(args);
    } else if (mode === 'pick' && args.length === 1) {
      const pick = selectReadFacet(readCompiler(args[0]));
      if (!pick) process.exitCode = 4;
      else console.log(`${pick.protocol} ${pick.relation}`);
    } else if (mode === 'contract-summary' && args.length === 5) {
      const [considered, compiled, refused, compileFailed, invalidCompiler] = args.map(count);
      console.log(JSON.stringify({ schema: 'bonfyre-contract-drive.v1', considered, contracts_compiled: compiled, refused,
        compiler_failed: compileFailed, invalid_compiler_output: invalidCompiler,
        truth: 'contract_compiled_not_enacted_not_closed_not_authorized',
        cannot_infer: ['a compiled contract -> an enacted request',
          'a refusal -> the source being unusable; it may also mean compiler failure or malformed output'] }));
    } else if (mode === 'enact-summary' && args.length === 8) {
      const [considered, enacted, refused, unpermitted, drifted, compileFailed, invalidCompiler] = args.slice(0, 7).map(count);
      console.log(JSON.stringify({ schema: 'bonfyre-read-enactment-drive.v1', considered, enacted, refused,
        not_permitted_or_not_fetched: unpermitted, body_drifted_from_receipt: drifted, actor: args[7], spend: 0,
        compiler_failed: compileFailed, invalid_compiler_output: invalidCompiler,
        actor_basis: 'caller assertion; retained ForeignTwin receipt does not bind an observer identity',
        authority_basis: 'retained receipt records policy allow; no new authority is granted or current permission verified',
        truth: 'a_read_that_was_performed_under_a_published_permission_recorded_as_one',
        cannot_infer: ['an enacted read -> a licence to republish or derive',
          'an enacted read -> authority over anything else this source offers',
          'obligations_closed -> the source agreeing that they were',
          'caller actor -> verified identity of the original observer',
          'failed enact command -> no local database writes; the native owner controls transaction behavior'] }));
    } else throw new Error('invalid read protocol contract command or arguments');
  } catch (error) { console.error(`read protocol contract: ${error.message}`); process.exitCode = 1; }
}
