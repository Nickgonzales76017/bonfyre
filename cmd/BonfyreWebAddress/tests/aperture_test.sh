#!/usr/bin/env bash
# Action apertures: the bridge between what may exist and what is addressed.
#
# Three questions get confused constantly. DEFINE asks what kinds of thing may
# exist (ObjectContract). ADDRESS asks how an existing thing is operated on
# (AddressPlane). DECLARE -- how something enters the world at all -- has no
# home unless apertures are computed, because the thing being created cannot be
# addressed: Friday's event is imaginary, and only the institution, its lens and
# the admitted grammar are real.
#
# The assertions here are that the operation exists before any instance does,
# that the aperture list differs by actor rather than showing locked entries,
# and that an AtomicForm asks only what the closure could not already answer.
set -uo pipefail
BASE="${BASE:-https://aurekai.life}"
FILE="${PERSONA_FILE:-$HOME/Documents/Bonfyre/NIGHTGRID-PERSONAS.md}"
SUBJ="institution:nightgrid"
pass=0; fail=0
ok()  { pass=$((pass+1)); printf '  ok   %s\n' "$1"; }
bad() { fail=$((fail+1)); printf '  FAIL %s\n' "$1"; }
check(){ if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (want '$3', got '$2')"; fi; }

[ -f "$FILE" ] || { echo "no persona file at $FILE" >&2; exit 1; }
command -v jq >/dev/null || { echo "jq is required for JSON assertions" >&2; exit 1; }
cred() { awk -F'|' -v p="$1" '{gsub(/[ `]/,"",$2); gsub(/[ `]/,"",$3); gsub(/[ `]/,"",$4); if ($2==p) {print $3" "$4; exit}}' "$FILE"; }
login() {
  local line email password
  line=$(cred "$1"); email=${line%% *}; password=${line#* }
  [ -z "$line" ] && return 1
  printf '{"email":"%s","password":"%s"}' "$email" "$password" \
    | curl -s --max-time 20 -X POST "$BASE/login" -H 'Content-Type: application/json' -H 'Accept: application/json' -d @- \
    | jq -r '.session.token' 2>/dev/null
}
field() { jq -r --arg key "$1" '.[$key] | if type == "boolean" then (if . then "True" else "False" end) else . end' 2>/dev/null; }

ORG=$(login organizer); MEM=$(login member)

echo "== the aperture list is a statement about this actor =="
anon=$(curl -s --max-time 20 -H 'Accept: application/json' "$BASE/_aperture?subject=$SUBJ")
check "anonymous is offered nothing"       "$(printf '%s' "$anon" | field offered)" "0"
if [ "$(printf '%s' "$anon" | field withheld_for_authority)" -gt 0 ] 2>/dev/null; then
  ok "operations exist here and are withheld, not absent"
else
  bad "nothing was withheld; the grammar may not be applicable at all"
fi
org=$(curl -s --max-time 20 -H "Authorization: Bearer $ORG" -H 'Accept: application/json' "$BASE/_aperture?subject=$SUBJ")
if [ "$(printf '%s' "$org" | field offered)" -gt 0 ] 2>/dev/null; then ok "an admin actor is offered operations"; else bad "admin offered none"; fi
mem=$(curl -s --max-time 20 -H "Authorization: Bearer $MEM" -H 'Accept: application/json' "$BASE/_aperture?subject=$SUBJ")
check "a read actor is offered nothing"    "$(printf '%s' "$mem" | field offered)" "0"

echo "== the operation exists before any instance does =="
if printf '%s' "$org" | grep -q '"operation":"Event.declare"'; then
  ok "Event.declare is offered on the institution, with no Event in existence"
else
  bad "Event.declare is not offered from the parent closure"
fi

echo "== AtomicForm asks only what the closure cannot answer =="
form=$(curl -s --max-time 20 -H "Authorization: Bearer $ORG" -H 'Accept: application/json' \
        "$BASE/_aperture/form?subject=$SUBJ&operation=Event.declare")
closed=$(printf '%s' "$form" | field closed_by_context)
human=$(printf '%s' "$form" | field human_must_supply)
if [ -n "$closed" ] && [ -n "$human" ]; then ok "the form separates resolved context from human input"; else bad "form did not partially evaluate"; fi
for f in institution organizer timezone; do
  if printf '%s' "$closed" | grep -q "$f"; then ok "$f is closed by context, not asked"; else bad "$f was not resolved"; fi
done
for f in title venue; do
  if printf '%s' "$human" | grep -q "$f"; then ok "$f remains a human choice"; else bad "$f was wrongly resolved"; fi
done
if printf '%s' "$human" | grep -q 'institution'; then bad "the form re-asks something the closure knows"; else ok "nothing already known is re-asked"; fi

echo "== birth modes are distinguished =="
check "DECLARE allocates identity" "$(printf '%s' "$form" | field birth_mode)" "DECLARE"
derive=$(curl -s --max-time 20 -H "Authorization: Bearer $ORG" -H 'Accept: application/json' \
          "$BASE/_aperture/form?subject=$SUBJ&operation=Economics.compute")
check "DERIVE creates no identity"  "$(printf '%s' "$derive" | field identity_allocation)" "none"
check "DERIVE asks nothing"         "$(printf '%s' "$derive" | field human_must_supply)" ""

echo "== an aperture form is gated, not merely hidden =="
check "a read actor is refused the form" \
  "$(curl -s --max-time 20 -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $MEM" "$BASE/_aperture/form?subject=$SUBJ&operation=Event.declare")" "403"
check "an admitted actor receives it" \
  "$(curl -s --max-time 20 -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $ORG" "$BASE/_aperture/form?subject=$SUBJ&operation=Event.declare")" "200"
check "an undeclared operation is not invented" \
  "$(curl -s --max-time 20 -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $ORG" "$BASE/_aperture/form?subject=$SUBJ&operation=Event.teleport")" "404"

echo "== birth mode is enforced, not described =="
sub() { curl -s --max-time 25 -X POST "$BASE/_aperture/submit" -H "Authorization: Bearer $ORG" -d "$1"; }
# A DERIVE that only refuses to store is a refusal wearing an operation's name.
# It must compute and return, every time, from the owner of the observations.
d=$(sub 'subject=foreign_twin:src-dallas-tax-parcels&operation=Property.economics')
check "DERIVE returns a computed result" "$(printf '%s' "$d" | field derived)" "True"
check "and stores nothing"               "$(printf '%s' "$d" | field stored)"  "False"
obs=$(printf '%s' "$d" | jq -r '.result[0].observations' 2>/dev/null)
if [ "${obs:-0}" -gt 1000 ] 2>/dev/null; then
  ok "it summarises the real observation corpus ($obs observations)"
else
  bad "derivation returned no corpus ($obs)"
fi
# Two sources must not derive the same numbers, or the subject is being ignored.
d2=$(sub 'subject=foreign_twin:src-arlington-tax-parcel&operation=Property.economics')
obs2=$(printf '%s' "$d2" | jq -r '.result[0].observations' 2>/dev/null)
if [ -n "$obs2" ] && [ "$obs2" != "$obs" ]; then
  ok "a different source derives different numbers ($obs2)"
else
  bad "the addressed subject did not change the result"
fi
# A DERIVE addressed at something with no corpus says so rather than inventing one.
d3=$(sub 'subject=institution:nightgrid&operation=Economics.compute&title=x')
check "a derivation with no corpus is refused" "$(printf '%s' "$d3" | field error)" "nothing_to_derive_from"
d=$(sub 'subject=commitment:demo&operation=Commitment.settle&title=x')
check "INVOKE is owned elsewhere"  "$(printf '%s' "$d" | field error)" "invoke_is_owned_elsewhere"

echo "== a DECLARE birth allocates a permanent identity =="
# A birth allocates a real Matrix subject. A suite that performs one on every
# run accumulates permanent semantic identities, and deleting a subject to tidy
# up is worse than not creating it: an identity that existed and was removed is
# not the same as one that never existed. So this runs only when asked.
if [ -n "${APERTURE_BIRTH:-}" ]; then
  born=$(sub "subject=institution:nightgrid&operation=Event.declare&title=aperture-witness-$$&venue=witness")
  check "the birth reports itself born"   "$(printf '%s' "$born" | field born)" "True"
  check "it begins in its declared state" "$(printf '%s' "$born" | field lifecycle_state)" "draft"
  sid=$(printf '%s' "$born" | field subject_id)
  if [ "${sid#event:}" != "$sid" ]; then ok "a subject id was allocated"; else bad "no subject id"; fi
else
  echo "  skip (set APERTURE_BIRTH=1; a birth allocates a permanent subject)"
fi

echo "== identity precedes the body it describes =="
# Read-only over whatever births have happened. Identity is allocated before the
# authored body is written, so a body describing no subject is the failure here.
if [ -n "${APERTURE_REMOTE:-}" ]; then
  q() { $APERTURE_REMOTE "sudo sqlite3 $1 \"$2\"" 2>/dev/null | tr -d '\r\n '; }
  CMSDB=/var/lib/bonfyre/cms/bonfyre_cms.db
  orphans=$(q "$CMSDB" "SELECT COUNT(*) FROM night_event WHERE subject_id IS NULL OR subject_id='';")
  check "no authored body lacks a subject" "$orphans" "0"
  bodies=$(q "$CMSDB" "SELECT COUNT(*) FROM night_event;")
  if [ "${bodies:-0}" -ge 1 ] 2>/dev/null; then
    one=$(q "$CMSDB" "SELECT subject_id FROM night_event LIMIT 1;")
    check "its subject exists in the Matrix" \
      "$(q /var/lib/bonfyre/matrix.db "SELECT COUNT(*) FROM matrix_subjects WHERE subject_id='$one';")" "1"
  else
    echo "  skip (no Event has been born yet)"
  fi
else
  echo "  skip (set APERTURE_REMOTE to verify the binding)"
fi

echo "== a born subject is reachable, not merely recorded =="
# A subject with an address and no presence is unreachable by construction:
# resolution joins address to presence, so the address is a row nobody reads.
# The invariant is that birth leaves none of the three halves missing.
if [ -n "${APERTURE_REMOTE:-}" ]; then
  q() { $APERTURE_REMOTE "sudo sqlite3 $1 \"$2\"" 2>/dev/null | tr -d '\r\n '; }
  MDB=/var/lib/bonfyre/matrix.db
  # Every born object, not only Events: the invariant belongs to birth, and a
  # grammar wired later must not be able to opt out of it.
  KINDS=$(q "$MDB" "SELECT DISTINCT object_name FROM matrix_subjects;" 2>/dev/null)
  # "Born" means born through the birth transaction, identified by the aperture
  # receipt it was bound under -- not every subject that is not foreign. Subjects
  # that predate the compiler never had a birth to bind these halves, and
  # counting them measured the wrong population rather than a broken invariant.
  born=$(q "$MDB" "SELECT COUNT(*) FROM matrix_subjects s WHERE s.evidence_ref LIKE 'aperture:%';")
  addr=$(q "$MDB" "SELECT COUNT(DISTINCT a.subject_id) FROM matrix_addresses a JOIN matrix_subjects s ON s.subject_id=a.subject_id WHERE s.evidence_ref LIKE 'aperture:%';")
  pres=$(q "$MDB" "SELECT COUNT(DISTINCT p.subject_id) FROM matrix_presences p JOIN matrix_subjects s ON s.subject_id=p.subject_id WHERE s.evidence_ref LIKE 'aperture:%';")
  check "every born object has an address"  "$addr" "$born"
  check "every born object has a presence"  "$pres" "$born"
  kinds=$(q "$MDB" "SELECT COUNT(DISTINCT subject_kind) FROM matrix_subjects s WHERE s.evidence_ref LIKE 'aperture:%';")
  if [ "${kinds:-0}" -ge 2 ] 2>/dev/null; then
    ok "more than one object grammar has actually been born ($kinds kinds)"
  else
    bad "only one grammar has been exercised; the compiler claim is untested"
  fi
  # The world is inherited from the institution, never defaulted: a demo
  # institution projecting into LIVE would put synthetic state in the real world.
  live=$(q "$MDB" "SELECT COUNT(*) FROM matrix_presences WHERE (subject_id LIKE 'event:%' OR subject_id LIKE 'budgetrequest:%') AND world != 'DEMO';")
  check "no demo object is present in a non-DEMO world" "$live" "0"
fi

echo "== a new object grammar needs authoring, not a redeploy =="
# The claim behind the whole compiler: the organ holds no knowledge of what an
# Event or a BudgetRequest is. Body type, fields, identity prefix and route
# segment all come from the contract, so a second grammar was wired by writing
# a schema and a contract row.
prop=$(sub 'subject=institution:nightgrid&operation=Budget.request&rationale=witness&amount=1.00&horizon=test')
check "a PROPOSE birth reports its own mode" "$(printf '%s' "$prop" | field birth_mode)" "PROPOSE"
check "it begins proposed, not draft"        "$(printf '%s' "$prop" | field lifecycle_state)" "proposed"
psid=$(printf '%s' "$prop" | field subject_id)
if [ "${psid#budgetrequest:}" != "$psid" ]; then
  ok "its identity follows its own grammar, not Event's"
else
  bad "identity prefix did not follow the contract: $psid"
fi
paddr=$(printf '%s' "$prop" | field address)
case "$paddr" in
  */finance/*) ok "its route follows its declared collection lens" ;;
  *) bad "route ignored the lens: $paddr" ;;
esac
# Which field names a thing is the contract's answer. Defaulting every object to
# a title gave every budget request the same route.
case "$paddr" in
  */finance/witness*) ok "its slug comes from its own declared field, not a title" ;;
  */finance/budgetrequest*) bad "the slug fell back to the object name" ;;
  *) ok "slug derived from a contract field ($paddr)" ;;
esac
check "the birth wrote its own receipt" "$(printf '%s' "$prop" | field receipt_written)" "True"

echo "== a birth records itself as its own evidence =="
# The receipt id is already the evidence_ref of the subject, the address and the
# presence. The receipt file is what those three references point at, in the
# same never-overwritten shape foreign observations use.
if [ -n "${APERTURE_REMOTE:-}" ]; then
  rc=$($APERTURE_REMOTE "sudo ls /var/lib/bonfyre/births/receipts/ 2>/dev/null | wc -l" 2>/dev/null | tr -d '\r\n ')
  if [ "${rc:-0}" -ge 1 ] 2>/dev/null; then ok "birth receipts are on disk ($rc)"; else bad "no birth receipt was written"; fi
  sch=$($APERTURE_REMOTE "sudo sh -c 'ls -t /var/lib/bonfyre/births/receipts/*.json 2>/dev/null | head -1 | xargs grep -o \"bonfyre-birth-receipt.v1\"'" 2>/dev/null | tr -d '\r\n ')
  check "the receipt declares its schema" "$sch" "bonfyre-birth-receipt.v1"
fi

echo "== a birth binds every half, or the subject is partly born =="
check "the relation to its closure is bound" "$(printf '%s' "$prop" | field relation_bound)" "True"
check "the write set is emitted as a delta"  "$(printf '%s' "$prop" | field fact_delta_written)" "True"
if [ -n "${APERTURE_REMOTE:-}" ]; then
  q2() { $APERTURE_REMOTE "sudo sqlite3 $1 \"$2\"" 2>/dev/null | tr -d '\r\n '; }
  MDB2=/var/lib/bonfyre/matrix.db
  bornn=$(q2 "$MDB2" "SELECT COUNT(*) FROM matrix_subjects s WHERE s.evidence_ref LIKE 'aperture:%';")
  # Scoped to declared_in: a claimed_by edge from a ForeignTwin is a legitimate
  # relation whose source is a subject but not a born object.
  rel=$(q2 "$MDB2" "SELECT COUNT(DISTINCT r.from_subject) FROM matrix_subject_relations r JOIN matrix_subjects s ON s.subject_id=r.from_subject WHERE r.relation='declared_in' AND s.evidence_ref LIKE 'aperture:%';")
  check "every born object knows the closure it was declared in" "$rel" "$bornn"
  # A delta names what one transaction wrote. It is only knowable at that
  # moment; reconstructing it later from three stores is the archaeology it
  # exists to prevent.
  dc=$($APERTURE_REMOTE "sudo ls /var/lib/bonfyre/births/deltas/ 2>/dev/null | wc -l" 2>/dev/null | tr -d '\r\n ')
  if [ "${dc:-0}" -ge 1 ] 2>/dev/null; then ok "fact deltas are on disk ($dc)"; else bad "no delta was written"; fi
  wc5=$($APERTURE_REMOTE "sudo sh -c 'ls -t /var/lib/bonfyre/births/deltas/*.json | head -1 | xargs grep -o \"write_count.:5\"'" 2>/dev/null | tr -d '\r\n ')
  if [ -n "$wc5" ]; then ok "the delta names all five writes"; else bad "delta write set is short"; fi
fi

echo "== a delta is consumed, and recomputes only what it named =="
# A write set that changes nothing is indistinguishable from one nobody emitted.
# The consumer must also recompute *only* what the delta named: rebuilding every
# index on every birth is exactly the whole-answer recomputation a delta avoids.
if [ -n "${APERTURE_REMOTE:-}" ]; then
  REMOTE_BUN=$($APERTURE_REMOTE "command -v bun" 2>/dev/null | tr -d '\r\n')
  [ -n "$REMOTE_BUN" ] || { echo "Bun is required on the remote estate" >&2; exit 1; }
  APPLY="sudo $REMOTE_BUN /opt/bonfyre/scripts/apply_fact_deltas.mjs"
  $APERTURE_REMOTE "$APPLY" >/dev/null 2>&1
  born=$(sub "subject=institution:nightgrid&operation=Event.declare&title=delta-witness-$$&venue=witness")
  check "the birth emitted a delta" "$(printf '%s' "$born" | field fact_delta_written)" "True"
  dry=$($APERTURE_REMOTE "$APPLY --dry-run --json" 2>/dev/null)
  pend=$(printf '%s' "$dry" | jq -r '.pending_deltas' 2>/dev/null)
  inv=$(printf '%s' "$dry" | jq -r '.invalidated_types | join(",")' 2>/dev/null)
  check "one birth leaves one delta pending" "$pend" "1"
  check "it invalidates only the type it wrote" "$inv" "night_event"
  app=$($APERTURE_REMOTE "$APPLY --json" 2>/dev/null)
  rb=$(printf '%s' "$app" | jq -r '.rebuilt | keys | join(",")' 2>/dev/null)
  check "only that index is rebuilt" "$rb" "night_event"
  again=$($APERTURE_REMOTE "$APPLY --dry-run --json" 2>/dev/null | jq -r '.pending_deltas' 2>/dev/null)
  check "applying twice recomputes nothing" "$again" "0"
  # Applied is marked, not deleted: a delta records what happened, and deleting
  # it to store one bit would destroy the history.
  kept=$($APERTURE_REMOTE "sudo ls /var/lib/bonfyre/births/deltas/*.json | wc -l" 2>/dev/null | tr -d '\r\n ')
  if [ "${kept:-0}" -ge 1 ] 2>/dev/null; then ok "applied deltas are kept, not deleted ($kept)"; else bad "deltas were deleted on apply"; fi
fi

echo "== a schedule is a schedule, not a table =="
# An events surface listing last month's sets beside tonight's is not a
# schedule. The pattern declares which field carries time and which direction
# matters; the comparison is lexical on ISO-8601, so a malformed value fails to
# match rather than being silently treated as now.
ev=$(curl -s --max-time 20 -H "Authorization: Bearer $ORG" -H 'Accept: application/json' "$BASE/_surface/night-events")
shown=$(printf '%s' "$ev" | field count)
if [ -n "${APERTURE_REMOTE:-}" ]; then
  total=$($APERTURE_REMOTE "sudo sqlite3 /var/lib/bonfyre/cms/bonfyre_cms.db 'SELECT COUNT(*) FROM night_event;'" 2>/dev/null | tr -d '\r\n ')
  if [ "${shown:-0}" -lt "${total:-0}" ] 2>/dev/null; then
    ok "the surface shows upcoming events only ($shown of $total)"
  else
    bad "every event is shown regardless of time ($shown of $total)"
  fi
fi
if printf '%s' "$ev" | grep -q '"start"'; then ok "each shown event carries its time"; else bad "no time on the records"; fi

echo "== ATTACH binds what exists and creates nothing =="
# The failure this prevents: claiming a merchant a ForeignTwin already
# represents produces a rival record, and the institution then holds two of one
# thing and has to decide later which is real.
# The claim contract applies to a foreign_twin, so it is invoked from the twin's
# own surface: the thing being claimed is the thing being looked at.
d=$(sub 'subject=foreign_twin:not-real&operation=Merchant.claim&claimant=institution:nightgrid&relationship=owner')
check "it refuses to create its own target"  "$(printf '%s' "$d" | field error)" "subject_does_not_exist"
d=$(sub 'subject=institution:nightgrid&operation=Merchant.claim&relationship=owner')
check "the contract does not apply to an unrelated kind" "$(printf '%s' "$d" | field error)" "no_such_aperture"

if [ -n "${APERTURE_REMOTE:-}" ]; then
  cnt() { $APERTURE_REMOTE "sudo sqlite3 /var/lib/bonfyre/matrix.db 'SELECT COUNT(*) FROM matrix_subjects;'" 2>/dev/null | tr -d '\r\n '; }
  before=$(cnt)
  a=$(sub 'subject=foreign_twin:aurekai-work-auth-md&operation=Merchant.claim&claimant=institution:nightgrid&relationship=operator&identity_evidence=witness')
  after=$(cnt)
  check "an attachment reports itself attached" "$(printf '%s' "$a" | field attached)" "True"
  check "it allocates no identity"              "$(printf '%s' "$a" | field identity_allocated)" "False"
  check "it binds the claim as an edge"         "$(printf '%s' "$a" | field relation_bound)" "True"
  check "no second object was created"          "$after" "$before"
  check "the claim names the existing subject"  "$(printf '%s' "$a" | field subject_id)" "foreign_twin:aurekai-work-auth-md"
fi

echo "== a born subject becomes addressable, still gated =="
check "an admitted actor reads the events surface" \
  "$(curl -s --max-time 20 -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $ORG" "$BASE/_surface/night-events")" "200"
check "an anonymous caller does not" \
  "$(curl -s --max-time 20 -o /dev/null -w '%{http_code}' "$BASE/_surface/night-events")" "403"

echo "== authority is checked at submit, not only at render =="
check "a read actor cannot declare" \
  "$(curl -s --max-time 20 -o /dev/null -w '%{http_code}' -X POST "$BASE/_aperture/submit" -H "Authorization: Bearer $MEM" -d 'subject=institution:nightgrid&operation=Event.declare&title=nope')" "403"
check "an anonymous caller cannot declare" \
  "$(curl -s --max-time 20 -o /dev/null -w '%{http_code}' -X POST "$BASE/_aperture/submit" -d 'subject=institution:nightgrid&operation=Event.declare&title=nope')" "403"

echo "== signing in is an operation on the identity root, not on any institution =="
# Session.open was first declared as applying to `institution`, which quietly
# offered "sign in" and "create account" on every estate in the lattice --
# nightgrid included. An institution is a thing you hold a grant in; it is not
# the thing that issues sessions. The kind is the scope.
n=$(curl -s --max-time 20 -H 'Accept: application/json' "$BASE/_aperture?subject=institution:nightgrid" | jq -r '.offered')
check "an arbitrary institution offers no identity operation" "$n" "0"
ops=$(curl -s --max-time 20 -H 'Accept: application/json' "$BASE/_aperture?subject=identity_root:bonfyre" | jq -r '[.apertures[].operation] | sort | join(",")')
check "the identity root offers exactly the identity operations" "$ops" "Account.declare,Session.open"

echo
echo "apertures: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
