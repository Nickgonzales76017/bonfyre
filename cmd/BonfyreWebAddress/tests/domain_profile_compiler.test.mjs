// Actual CUE -> Zig -> C, with temporary build fixtures and no estate writes.
import {after, before, test} from 'node:test';
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {chmodSync, copyFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, utimesSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {dirname, join, resolve} from 'node:path';
import {fileURLToPath} from 'node:url';

const here = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const root = resolve(here, '../..');
let scratch, compiler, serial = 0;
const run = (command, args, cwd = here) => {
  const result = spawnSync(command, args, {cwd, encoding: 'utf8', timeout: 90000, maxBuffer: 4 * 1024 * 1024, env: scratch ? {...process.env, ZIG_GLOBAL_CACHE_DIR: join(scratch, 'zig-global-cache')} : process.env});
  if (result.error) throw result.error;
  return result;
};
const ok = result => {
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  return result.stdout;
};
const directory = () => {
  const path = join(scratch, String(++serial));
  mkdirSync(path); return path;
};
const fixture = (name = 'lot-review') => ({
  name, evidence_requirements: ['source_receipt'],
  fields: [{name: 'amount', audiences: ['public', 'institution']},
    {name: 'private_note', audiences: ['self'], withhold_reason: 'private'}],
  projections: [{carrier: 'api-v1', standard: 'Fixture', fields: [{semantic: 'amount', carrier: 'amount'}]}],
});
const generate = (profiles, dir, output = join(dir, 'profiles.h')) => {
  const input = join(dir, 'input.json');
  writeFileSync(input, JSON.stringify(profiles));
  return run(compiler, [input, output]);
};

// Length-prefixed byte comparison avoids assuming strings contain no newlines.
const expected = profiles => {
  let output = '';
  const number = value => {output += `${value}\n`;};
  const text = value => {output += value == null ? 'null\n' : `${Buffer.byteLength(value)}:${value}\n`;};
  number(profiles.length);
  for (const p of profiles) {
    text(p.name); text(p.evidence_requirements[0]); number(p.fields.length);
    for (const f of p.fields) {
      text(f.name);
      number(f.audiences.reduce((mask, a) => mask | (1 << ['self','household','group','community','public','market','institution'].indexOf(a)), 0));
      text(f.withhold_reason);
    }
    number(p.projections.length);
    for (const c of p.projections) {
      text(c.carrier); text(c.standard); text(c.container); number(c.fields.length);
      for (const f of c.fields) {text(f.semantic); text(f.carrier);}
    }
  }
  return output;
};
const dumpSource = `#include <stdio.h>
#include <string.h>
#include "profiles.h"
static void text(const char *s) {
  if (!s) { puts("null"); return; }
  printf("%zu:", strlen(s)); fwrite(s, 1, strlen(s), stdout); putchar('\\n');
}
int main(void) {
  printf("%zu\\n", (size_t)BF_NATIVE_DOMAIN_PROFILE_COUNT);
  for (size_t i=0; i<BF_NATIVE_DOMAIN_PROFILE_COUNT; i++) {
    const BfNativeDomainProfile *p = &bf_native_domain_profiles[i];
    text(p->name); text(p->evidence_requirement); printf("%zu\\n", p->field_count);
    for (size_t j=0; j<p->field_count; j++) {
      text(p->fields[j].name); printf("%u\\n", p->fields[j].audience_mask); text(p->fields[j].withhold_reason);
    }
    printf("%zu\\n", p->projection_count);
    for (size_t j=0; j<p->projection_count; j++) {
      const BfDomainCarrierProjection *c = &p->projections[j];
      text(c->carrier); text(c->standard); text(c->container); printf("%zu\\n", c->field_count);
      for (size_t k=0; k<c->field_count; k++) {text(c->fields[k].semantic); text(c->fields[k].carrier);}
    }
  }
  return 0;
}
`;
function compileAndRead(dir) {
  writeFileSync(join(dir, 'dump.c'), dumpSource);
  ok(run('cc', ['-std=c11','-Wall','-Wextra','-Werror','-pedantic', join(dir,'dump.c'), '-o',join(dir,'dump')]));
  return ok(run(join(dir,'dump'), []));
}
before(() => {
  scratch = mkdtempSync(join(tmpdir(), 'bonfyre-domain-compiler-'));
  compiler = join(scratch, 'compiler');
  ok(run('zig', ['build-exe', join(here,'tools/compile_domain_profiles.zig'), '-O','ReleaseSafe', `-femit-bin=${compiler}`]));
});
after(() => {if (scratch) rmSync(scratch, {recursive:true,force:true});});

test('all current CUE profiles survive real C compilation and field-by-field readback', () => {
  const profiles = JSON.parse(ok(run('cue', ['export', '.', '-e','nativeDomainProfiles','--out','json'], join(root,'schemas/control'))));
  assert.ok(profiles.some(p => p.name === 'parcel'));
  assert.ok(profiles.some(p => p.name === 'live-moment'));
  assert.ok(profiles.some(p => p.name === 'social-clip'));
  const dir = directory(); ok(generate(profiles, dir));
  assert.equal(compileAndRead(dir), expected(profiles));
  const first = readFileSync(join(dir,'profiles.h'));
  ok(generate(profiles, dir)); assert.deepEqual(readFileSync(join(dir,'profiles.h')), first);
});

test('private C symbols do not rename or collide with punctuation, digits or Unicode identities', () => {
  const profiles = ['a-b','a_b','a.b','1profile','a/b','café','a"b','a\\b'].map(fixture);
  for (const p of profiles) p.projections.push({...p.projections[0], carrier:'api_v1'});
  const dir = directory(); ok(generate(profiles,dir));
  assert.equal(compileAndRead(dir), expected(profiles));
});

test('C string literals preserve controls, UTF-8, trigraphs and following digits', () => {
  const p = fixture('escaped');
  p.projections[0].standard = 'quote " backslash \\ newline\n\t\u0001a9 café ??/';
  const dir = directory(); ok(generate([p],dir));
  assert.equal(compileAndRead(dir), expected([p]));
});

const refusals = [
  ['duplicate profile', ps => ps.push(structuredClone(ps[0])), 'DuplicateProfile'],
  ['duplicate field', ps => ps[0].fields.push(structuredClone(ps[0].fields[0])), 'DuplicateField'],
  ['duplicate carrier', ps => ps[0].projections.push(structuredClone(ps[0].projections[0])), 'DuplicateCarrier'],
  ['undeclared field', ps => {ps[0].projections[0].fields[0].semantic='absent';}, 'UndeclaredSemanticField'],
  ['duplicate output field', ps => {ps[0].projections[0].fields.push({semantic:'private_note',carrier:'amount'});}, 'DuplicateCarrierMapping'],
  ['unknown audience', ps => {ps[0].fields[0].audiences=['nobody'];}, 'UnknownAudience'],
  ['empty audiences', ps => {ps[0].fields[0].audiences=[];}, 'EmptyAudience'],
  ['empty requirements', ps => {ps[0].evidence_requirements=[];}, 'EmptyProfile'],
  ['empty projection', ps => {ps[0].projections[0].fields=[];}, 'EmptyProjection'],
  ['NUL identity', ps => {ps[0].name='before\0after';}, 'InvalidText'],
  ['unknown descriptor field', ps => {ps[0].discarded='must not disappear';}, 'UnknownField'],
];
for (const [name, change, reason] of refusals) test(`${name} refuses before replacing the previous output`, () => {
  const dir = directory(), profiles = [fixture()], output = join(dir,'profiles.h');
  writeFileSync(output,'previous retained header\n'); change(profiles);
  const result = generate(profiles,dir);
  assert.notEqual(result.status,0); assert.ok(result.stderr.includes(reason),result.stderr);
  assert.equal(readFileSync(output,'utf8'),'previous retained header\n');
});

function buildFixture() {
  const dir = directory(), schema = join(dir,'schemas'); mkdirSync(schema);
  const profiles = [fixture()];
  const source = join(schema,'profile.cue');
  const write = () => writeFileSync(source, `package control\nnativeDomainProfiles: ${JSON.stringify(profiles)}\n`);
  write();
  copyFileSync(join(here,'tools/compile_domain_profiles.zig'),join(dir,'compiler.zig'));
  writeFileSync(join(dir,'Makefile'), `.DEFAULT_GOAL := probe\nCC := cc\nCFLAGS := -O0\nDOMAIN_PROFILE_SCHEMAS := ${schema}\nDOMAIN_PROFILE_COMPILER := ${join(dir,'compiler.zig')}\ninclude ${join(here,'tools/domain_profiles.mk')}\nprobe: probe.c $(DOMAIN_PROFILE_HEADER)\n\t$(CC) -std=c11 -Wall -Wextra -Werror -o $@ probe.c\n`);
  writeFileSync(join(dir,'probe.c'), '#include <stdio.h>\n#include "generated/native_domain_profiles.h"\nint main(void) { puts(bf_native_domain_profiles[0].name); return 0; }\n');
  const make = (args=[]) => run('make',['--no-print-directory',...args],dir);
  const stale = () => {for (const path of ['probe','generated/native_domain_profiles.h']) utimesSync(join(dir,path),new Date('2000-01-01'),new Date('2000-01-01'));};
  return {dir, profiles, source, write, make, stale, header:join(dir,'generated/native_domain_profiles.h')};
}

test('the included production make rule regenerates missing/stale input and recompiles its consumer', () => {
  const f = buildFixture(); ok(f.make());
  assert.equal(ok(run(join(f.dir,'probe'),[])), 'lot-review\n');
  f.profiles[0].name='changed-plan'; f.write(); f.stale(); ok(f.make());
  assert.equal(ok(run(join(f.dir,'probe'),[])), 'changed-plan\n');
  rmSync(f.header); ok(f.make());
  assert.equal(ok(run(join(f.dir,'probe'),[])), 'changed-plan\n');
});

test('an identical projection preserves header and downstream binary on a forced refresh', () => {
  const f=buildFixture(); ok(f.make());
  const header=statSync(f.header,{bigint:true}), probe=statSync(join(f.dir,'probe'),{bigint:true});
  ok(f.make(['domain-profiles-generate'])); ok(f.make());
  assert.equal(statSync(f.header,{bigint:true}).mtimeNs,header.mtimeNs);
  assert.equal(statSync(f.header,{bigint:true}).ino,header.ino);
  assert.equal(statSync(join(f.dir,'probe'),{bigint:true}).mtimeNs,probe.mtimeNs);
});

test('failed CUE, failed Zig and invalid generated C preserve the last usable header', () => {
  const f=buildFixture(); ok(f.make()); const before=readFileSync(f.header);
  for (const argument of ['CUE=false','ZIG=false']) {
    const result=f.make(['-B','generated/native_domain_profiles.h',argument]);
    assert.notEqual(result.status,0); assert.deepEqual(readFileSync(f.header),before);
  }
  const fake=join(f.dir,'bad-zig');
  writeFileSync(fake,'#!/bin/sh\nfor arg do output=$arg; done\nprintf "%s\\n" "this is not C" > "$output"\n'); chmodSync(fake,0o755);
  assert.notEqual(f.make(['-B','generated/native_domain_profiles.h',`ZIG=${fake}`]).status,0);
  assert.deepEqual(readFileSync(f.header),before);
  ok(f.make(['-B','generated/native_domain_profiles.h']));
});

test('normal owner targets include generation and the real compiler regression', () => {
  const makefile=readFileSync(join(here,'Makefile'),'utf8');
  assert.match(makefile,/include tools\/domain_profiles\.mk/);
  assert.match(makefile,/build\/web_domain_profile_projection\.o:\s*\$\(DOMAIN_PROFILE_HEADER\)/);
  assert.match(makefile,/all:\s*domain-profile-compiler-witness domain-profile-witness/);
  assert.match(makefile,/node --test tests\/domain_profile_compiler\.test\.mjs/);
});
