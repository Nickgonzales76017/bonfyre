#!/bin/sh
# A grammar that is only names carries nothing.
#
# #ApplicationKind declared fifty-nine names and nothing else. It could say a
# slot held a Checkout and not that a checkout quotes, commits, pays and
# settles, so fifty-nine site applications sat outside the twenty-five relation
# grammar entirely -- the facet compiler types four thousand external
# occurrences and could not see a single site application.
#
# Binding each kind to the phenotypes it actually is makes the relations follow
# rather than be restated per capability program. This asserts the binding stays
# total and stays honest: every declared kind bound, no binding for a kind that
# does not exist, and the phenotype and extent grammars fully exercised rather
# than a handful of favourites carrying everything.
set -eu
bin=${1:?binary required}
bin=$(CDPATH= cd -- "$(dirname -- "$bin")" && pwd -P)/$(basename -- "$bin")
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd -P)
cd "$root"
corpus="$root/estate/generated/external-estate-v4/observed-external-estate.jsonl"

"$bin" vocabulary --corpus "$corpus" --json > /tmp/bf-vocab.json

python3 - /tmp/bf-vocab.json <<'PY'
import json, re, subprocess, sys, collections
vocab = json.load(open(sys.argv[1]))
facts = json.loads(subprocess.run(
    ['cue', 'export', './schemas/control', '-e', 'applicationKindFacts', '--out', 'json'],
    capture_output=True, text=True).stdout)
decl = open('schemas/control/site-presentation.cue').read()
kinds = re.findall(r'"([^"]+)"', re.search(r'^#ApplicationKind:(.*)$', decl, re.M).group(1))

missing = [k for k in kinds if k not in facts]
assert not missing, f'application kinds with no declared facts: {missing}'
spurious = [k for k in facts if k not in kinds]
assert not spurious, f'facts for kinds that are not declared: {spurious}'

# Every phenotype named must be one the compiler knows; a grammar that binds to
# a phenotype the compiler has never heard of is two vocabularies again.
known = set(vocab['phenotypes'])
for kind, f in facts.items():
    unknown = [p for p in f['phenotypes'] if p not in known]
    assert not unknown, f'{kind} names phenotypes the compiler does not have: {unknown}'
    assert f['phenotypes'], f'{kind} carries no phenotype'

# The binding must exercise the grammars, not lean on a few.
used_ph = {p for f in facts.values() for p in f['phenotypes']}
assert len(used_ph) == len(known), \
    f'{len(known) - len(used_ph)} phenotypes unused by any application kind: {sorted(known - used_ph)}'
used_ex = {f['extent'] for f in facts.values()}
assert len(used_ex) >= 6, f'extents unused: {used_ex}'

# And the point of it: relations must actually follow.
admits = {p: set(v['admits']) for p, v in vocab['phenotypes'].items()}
total = 0
reached = set()
for kind, f in facts.items():
    rel = set()
    for p in f['phenotypes']:
        rel |= admits[p]
    assert rel, f'{kind} admits no relation, so binding it bought nothing'
    total += len(rel)
    reached |= rel
assert len(reached) == len(vocab['relations']), \
    f'only {len(reached)} of {len(vocab["relations"])} relations reachable from site applications'
# The estate's own rows are bound too. The facet compiler typed 4,891 external
# occurrences and none of the 54 content types that carry the institution's own
# data, so it could propose relations for the outside world and nothing for the
# inside.
import glob, os
ctf = json.loads(subprocess.run(
    ['cue', 'export', './schemas/control', '-e', 'contentTypeFacts', '--out', 'json'],
    capture_output=True, text=True).stdout)
types = {os.path.basename(f)[:-5] for f in glob.glob('cmd/BonfyreCMS/content-types/*.json')}
unbound = sorted(types - set(ctf))
assert not unbound, f'content types with no declared facts: {unbound}'
phantom = sorted(set(ctf) - types)
assert not phantom, f'facts for content types that do not exist: {phantom}'

ct_total = 0
ct_reached = set()
domains = set()
for name, f in ctf.items():
    unknown = [p for p in f['phenotypes'] if p not in known]
    assert not unknown, f'{name} names phenotypes the compiler does not have: {unknown}'
    rel = set()
    for p in f['phenotypes']:
        rel |= admits[p]
    assert rel, f'{name} admits no relation'
    ct_total += len(rel)
    ct_reached |= rel
    domains.add(f['domain'])

# COMMAND is admitted only by COMMANDABLE_RESOURCE and nothing the CMS holds is
# a device, so content types alone must not reach it -- and the two grammars
# together must. If a content type ever becomes commandable this fails and asks
# whether that is true.
assert 'COMMAND' not in ct_reached, \
    'a content type now claims COMMAND; confirm the estate really holds a commandable row'
assert reached | ct_reached == set(vocab['relations']), \
    'the two grammars together no longer reach every relation'
assert len(domains) >= 12, f'estate domains collapsed to {len(domains)}'

# Three vocabularies describe one thing and nothing checked they agreed: the
# affordances #ExternalAffordance declares, the ones the compiler maps to
# relations, and the ones the estate's data actually carries. Six were declared
# and mapped to nothing -- an occurrence arriving with SHIP would have
# contributed nothing to its own facet set and no one would have been told.
import sqlite3, os
cue = open('schemas/control/external-estate.cue').read()
declared = set(re.findall(r'"([A-Z_]+)"',
    re.search(r'#ExternalAffordance:(.*?)\n\n', cue, re.S).group(1)))
csrc = open('cmd/BonfyreFacet/src/main.c').read()
mapped = set(re.findall(r'\{\s*"([A-Z_]+)"',
    re.search(r'AFFORDANCE_RELATIONS\[\]\s*=\s*\{(.*?)\n\};', csrc, re.S).group(1)))

unmapped = sorted(declared - mapped)
assert not unmapped, f'affordances declared but deriving no relation: {unmapped}'
undeclared = sorted(mapped - declared)
assert not undeclared, f'affordances mapped but not declared: {undeclared}'

# And the data must stay inside the grammar. This is checked only when a live
# estate exists, because its absence is not a failure of the vocabulary.
estate = os.path.expanduser('~/.bonfyre/estate/estate.db')
if os.path.exists(estate):
    db = sqlite3.connect(estate)
    used = {r[0].upper() for r in
            db.execute("SELECT DISTINCT term FROM occurrence_term WHERE axis='affordance'")}
    rogue = sorted(used - declared)
    assert not rogue, f'data carries affordances the grammar does not declare: {rogue}'

# The fourth law, made mechanical. #StandingAction names what a person does and
# was read by nothing at all -- the only entirely unread grammar in the estate.
# Binding it says which actions computation may carry to completion and which
# are a commitment no engine may close on someone's behalf.
saf = json.loads(subprocess.run(
    ['cue', 'export', './schemas/control', '-e', 'standingActionFacts', '--out', 'json'],
    capture_output=True, text=True).stdout)
sa_decl = set(re.findall(r'"([A-Z_]+)"', re.search(
    r'#StandingAction:(.*?)\n\n', open('schemas/control/standing-address.cue').read(), re.S).group(1)))
sa_missing = sorted(sa_decl - set(saf))
assert not sa_missing, f'standing actions with no declared facts: {sa_missing}'
assert not sorted(set(saf) - sa_decl), 'facts for undeclared standing actions'

relset = set(vocab['relations'])
for name, f in saf.items():
    bad = [r for r in f['requires'] if r not in relset]
    assert not bad, f'{name} requires relations that are not declared: {bad}'
    bad = [p for p in f['acts_on'] if p not in known]
    assert not bad, f'{name} acts on phenotypes the compiler does not have: {bad}'

# Anything that moves money, grants authority or discloses is a commitment. An
# engine that could close these on a person's behalf is the thing the fourth law
# exists to prevent, so the boundary is asserted and not left to drafting care.
for name in ('BUY', 'SELL', 'FUND', 'APPROVE', 'APPLY', 'SHARE', 'ESCALATE', 'JOIN'):
    assert saf[name]['commits'], f'{name} was marked reachable without a human commitment'
reachable = [n for n, f in saf.items() if not f['commits']]
assert reachable, 'no standing action completes in computation, which cannot be right'

# A bound grammar is a description until some protocol can carry it. Every
# member is measured against the 739-protocol population: a member no protocol
# can serve is a name that cannot be executed, however well it is typed.
import subprocess as sp
def reach(phenos):
    return json.loads(sp.run(
        ['./cmd/BonfyreFacet/bonfyre-facet', 'reach', '--corpus',
         'estate/generated/external-estate-v4/observed-external-estate.jsonl',
         '--phenotypes', ','.join(phenos), '--json'],
        capture_output=True, text=True).stdout)

seen_reach = {}
orphans = []
for label, table, field in (('application_kind', facts, 'phenotypes'),
                            ('content_type', ctf, 'phenotypes'),
                            ('standing_action', saf, 'acts_on')):
    for name, f in table.items():
        key = ','.join(sorted(f[field]))
        if key not in seen_reach:
            seen_reach[key] = reach(f[field])
        if seen_reach[key]['protocols_offering_any'] == 0:
            orphans.append(f'{label}:{name}')
assert not orphans, f'grammar members no protocol can serve: {orphans}'

# The loop-closing relations are scarce and that scarcity is a fact about the
# machine-readable internet, not a defect here. It is asserted so that a change
# which appears to make proof abundant gets looked at rather than believed.
census = reach(list(known))['by_relation']
for lean in ('PROVE', 'ROUND_TRIP', 'SETTLE'):
    assert census[lean] < census['QUERY'] / 4, \
        f'{lean} is no longer scarce against QUERY; confirm the protocol corpus really changed'

print(f'grammar density: PASS ({len(kinds)} kinds + {len(ctf)} content types across '
      f'{len(domains)} domains, {total + ct_total} relations derivable, '
      f'all {len(vocab["relations"])} reached, {len(declared)} affordances mapped, '
      f'{len(saf)} standing actions with {len(reachable)} reachable and '
      f'{len(saf)-len(reachable)} at the human line, every member served by '
      f'at least one of 739 protocols)')
PY
