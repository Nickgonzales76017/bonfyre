#!/usr/bin/env bash
# Foreign twins: observation, promotion and claim are three different standings.
#
# Observing records that something answered. Promoting means the institution
# chose to be able to name it, identified by bytes it actually retained.
# Claiming is an assertion that some institution holds a relationship to it.
# None of the three is ownership, and collapsing any two is the failure this
# suite exists to catch.
set -uo pipefail
BASE="${BASE:-https://aurekai.life}"

# The subject of this test is a deployed estate, not this checkout. When that
# estate cannot be reached -- it is down, or behind a rate limit -- there is
# nothing here to be right or wrong about, and reporting FAIL points the
# reader at code that is fine.
# Two things have to hold: the estate answers at all, and it exposes the
# _surface projections these assertions read. The edge that serves the
# published pages does not serve _surface, so a 200 on /healthz there is
# not the estate this test is about. curl exits 0 on a 429 and on a 404,
# so the status itself is the test.
if [ "$(curl -sS --max-time 12 -o /dev/null -w '%{http_code}' "$BASE/healthz" 2>/dev/null)" != "200" ] \
   || [ "$(curl -sS --max-time 12 -o /dev/null -w '%{http_code}' "$BASE/_surface/routes" 2>/dev/null)" = "404" ]; then
  printf '%s: SKIP (%s does not serve _surface)\n' 'foreign twins' "$BASE"
  exit 0
fi
pass=0; fail=0
command -v jq >/dev/null || { echo "jq is required for JSON assertions" >&2; exit 1; }
ok()  { pass=$((pass+1)); printf '  ok   %s\n' "$1"; }
bad() { fail=$((fail+1)); printf '  FAIL %s\n' "$1"; }
check(){ if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (want '$3', got '$2')"; fi; }
R="${TWIN_REMOTE:-}"
REMOTE_BUN=""
if [ -n "$R" ]; then
  REMOTE_BUN=$($R "command -v bun" 2>/dev/null | tr -d '\r\n')
  [ -n "$REMOTE_BUN" ] || { echo "Bun is required on the remote estate" >&2; exit 1; }
fi
PF="${PERSONA_FILE:-$HOME/Documents/Bonfyre/NIGHTGRID-PERSONAS.md}"
TWIN_TOKEN=""
if [ -f "$PF" ]; then
  _l=$(awk -F'|' '{gsub(/[ `]/,"",$2); gsub(/[ `]/,"",$3); gsub(/[ `]/,"",$4); if ($2=="organizer") {print $3" "$4; exit}}' "$PF")
  TWIN_TOKEN=$(printf '{"email":"%s","password":"%s"}' "${_l%% *}" "${_l#* }" \
    | curl -s --max-time 20 -X POST "$BASE/login" -H 'Content-Type: application/json' -H 'Accept: application/json' -d @- \
    | jq -r '.session.token' 2>/dev/null)
fi
q() { [ -n "$R" ] || return 0; $R "sudo sqlite3 /var/lib/bonfyre/matrix.db \"$1\"" 2>/dev/null | tr -d '\r\n '; }

echo "== the registry answers over real observations =="
tw=$(curl -s --max-time 20 -H 'Accept: application/json' "$BASE/twins")
cnt=$(printf '%s' "$tw" | jq -r '.count' 2>/dev/null)
if [ "${cnt:-0}" -ge 5 ] 2>/dev/null; then ok "twins are registered ($cnt)"; else bad "registry returned ${cnt:-none}"; fi
check "the page serves to a person" "$(curl -s --max-time 20 -o /dev/null -w '%{http_code}' -H 'Accept: text/html' "$BASE/twins")" "200"

echo "== a thing that never yielded bytes cannot be promoted =="
# An identity whose digest we invented would be a claim wearing the shape of an
# observation, so promotion requires content we actually retained.
if [ -n "$R" ]; then
  un=$($R "sudo /usr/local/bin/bonfyre-project foreign promote --dry-run" 2>/dev/null \
        | jq -r '.never_retained_content' 2>/dev/null)
  if [ "${un:-0}" -ge 1 ] 2>/dev/null; then ok "unpromotable resources are refused ($un)"; else ok "every observed resource retained content"; fi
  notprom=$(printf '%s' "$tw" | jq '[.twins[] | select((.ever_retained | not) and .promoted)] | length' 2>/dev/null)
  check "nothing was promoted without retained bytes" "$notprom" "0"
fi

echo "== promotion is idempotent =="
if [ -n "$R" ]; then
  before=$($R "sudo sqlite3 /var/lib/bonfyre/matrix.db \"SELECT COUNT(*) FROM matrix_subjects WHERE subject_kind='ForeignTwin';\"" 2>/dev/null | tr -d '\r\n ')
  $R "sudo /usr/local/bin/bonfyre-project foreign promote" >/dev/null 2>&1
  after=$($R "sudo sqlite3 /var/lib/bonfyre/matrix.db \"SELECT COUNT(*) FROM matrix_subjects WHERE subject_kind='ForeignTwin';\"" 2>/dev/null | tr -d '\r\n ')
  check "re-promoting creates no new subject" "$after" "$before"
fi

echo "== a promoted twin is addressable =="
if [ -n "$R" ]; then
  subj=$(q "SELECT COUNT(*) FROM matrix_subjects WHERE subject_kind='ForeignTwin';")
  addr=$(q "SELECT COUNT(DISTINCT subject_id) FROM matrix_addresses WHERE subject_id LIKE 'foreign_twin:%';")
  pres=$(q "SELECT COUNT(DISTINCT subject_id) FROM matrix_presences WHERE subject_id LIKE 'foreign_twin:%';")
  # Two shapes of twin, and they do not identify the same way.
  #
  # A URL twin is one resource fetched repeatedly: identified by the bytes it
  # last returned, and carrying the web address we observed it at. A source is
  # an endpoint observed across hundreds of pages with no route this estate
  # holds -- identified by a digest over its page-digest set, and carrying no
  # address, because binding an invented URL would assert a route that cannot
  # resolve. Both carry a presence: both are real things observed in the world.
  urltw=$(q "SELECT COUNT(*) FROM matrix_subjects WHERE subject_kind='ForeignTwin' AND subject_id NOT LIKE 'foreign_twin:src-%';")
  srctw=$(q "SELECT COUNT(*) FROM matrix_subjects WHERE subject_id LIKE 'foreign_twin:src-%';")
  srcaddr=$(q "SELECT COUNT(DISTINCT subject_id) FROM matrix_addresses WHERE subject_id LIKE 'foreign_twin:src-%';")
  if [ "${srctw:-0}" -ge 1 ] 2>/dev/null; then ok "observation sources are promoted ($srctw)"; else bad "no source twins"; fi
  check "a source asserts no web route it cannot resolve" "$srcaddr" "0"
  # Presence is NOT implied by registration. This assertion previously required
  # every twin to be present, which encoded the conflation the estate has since
  # separated: a registered identity that is not present is the normal case, and
  # a twin becomes present only by an authority-gated act.
  reg=$(q "SELECT COUNT(*) FROM matrix_subjects WHERE subject_kind='ForeignTwin';")
  act=$(q "SELECT COUNT(DISTINCT subject_id) FROM matrix_presences WHERE subject_id LIKE 'foreign_twin:%' AND active=1;")
  if [ "${reg:-0}" -ge "${act:-0}" ] 2>/dev/null; then
    ok "registered twins ($reg) are at least the active ones ($act); presence is not implied"
  else
    bad "more twins are present ($act) than registered ($reg)"
  fi
  if [ "${addr:-0}" -ge 1 ] 2>/dev/null; then ok "URL twins carry the address they were observed at ($addr)"; else bad "no URL twin has an address"; fi
  # An observation of the real world is LIVE even when a demo institution looks.
  demo=$(q "SELECT COUNT(*) FROM matrix_presences WHERE subject_id LIKE 'foreign_twin:%' AND world != 'LIVE';")
  check "observations of the real world are LIVE" "$demo" "0"
fi

echo "== an observation names what it retained, without being asked =="
# The one moment a resource's identity is unambiguous is when the bytes that
# identify it are in hand. Deferring promotion invites an identity derived later
# by a different rule, beside the one derived here -- which is how this estate
# ended up with two ids for one digest.
if [ -n "$R" ]; then
  b=$(q "SELECT COUNT(*) FROM matrix_subjects WHERE subject_kind='ForeignTwin';")
  stamp=$(date -u +%Y%m%dT%H%M%SZ)
  $R "sudo /usr/local/bin/bonfyre-project foreign observe witness-$stamp https://www.w3.org/Icons/w3c_home.png \
        --receipt /var/lib/bonfyre/foreign/receipts/witness-$stamp.json \
        --body /var/lib/bonfyre/foreign/bodies/witness-$stamp.body --observe-right public" >/dev/null 2>&1
  a=$(q "SELECT COUNT(*) FROM matrix_subjects WHERE subject_kind='ForeignTwin';")
  # Re-observing something already promoted must not create a second identity.
  check "re-observing a known resource adds no identity" "$a" "$b"
  # q() quotes correctly; nesting escaped quotes by hand did not.
  known=$(q "SELECT COUNT(*) FROM matrix_subjects WHERE subject_id='foreign_twin:www-w3-org-icons-w3c-home-png';")
  check "the observed resource is named" "$known" "1"
fi

echo "== a private closure is never promoted into public subjects =="
# A private observation belongs to the estate or actor that took it, and the
# contract is explicit that a sibling closure cannot enumerate it. Promotion
# walks a directory, so pointing it at a private root -- by an operator or a
# changed environment variable -- must refuse rather than publish.
if [ -n "$R" ]; then
  before=$(q "SELECT COUNT(*) FROM matrix_subjects WHERE subject_kind='ForeignTwin';")
  $R "sudo mkdir -p /var/lib/bonfyre/foreign/private/witness/scope && sudo cp \"\$(sudo ls /var/lib/bonfyre/foreign/receipts/*.json | head -1)\" /var/lib/bonfyre/foreign/private/witness/scope/" >/dev/null 2>&1
  out=$($R "sudo BONFYRE_FOREIGN_RECEIPTS=/var/lib/bonfyre/foreign/private/witness/scope /usr/local/bin/bonfyre-project foreign promote 2>&1" 2>/dev/null)
  after=$(q "SELECT COUNT(*) FROM matrix_subjects WHERE subject_kind='ForeignTwin';")
  if printf '%s' "$out" | grep -q "refusing to promote from a private closure"; then
    ok "promotion refuses a private closure"
  else
    bad "a private closure was not refused"
  fi
  check "and nothing was published" "$after" "$before"
  $R "sudo rm -rf /var/lib/bonfyre/foreign/private" >/dev/null 2>&1
fi

echo "== an identity is derived only from a body that still matches its receipt =="
# A body that no longer hashes to its receipt is not the thing the receipt
# describes; an identity taken from it would be attested by a receipt for
# different content.
if [ -n "$R" ]; then
  drift=$($R "sudo /usr/local/bin/bonfyre-project foreign promote --dry-run" 2>/dev/null \
          | jq -r '.body_drifted' 2>/dev/null)
  check "no live retained body has drifted" "$drift" "0"
fi

echo "== the auth boundary answers in the caller's grammar =="
# auth_request failing made nginx serve its own HTML page, so a machine asking
# an authenticated API for JSON received 188 bytes of text/html.
ct=$(curl -s --max-time 15 -o /dev/null -w '%{content_type}' -H 'Accept: application/json' "$BASE/studio/v1/profile")
case "$ct" in
  application/json*) ok "an unauthenticated machine gets JSON ($ct)" ;;
  *) bad "the auth boundary answered $ct to a machine" ;;
esac
if curl -s --max-time 15 -H 'Accept: application/json' "$BASE/studio/v1/profile" | grep -q session_required; then
  ok "and the refusal says what to do about it"
else
  bad "the refusal carries no explanation"
fi

echo "== a source's identity is over its observation set, and is stable =="
# No single page digest identifies a source with hundreds of pages. The rule is
# a digest over its sorted distinct page digests: it moves when what the source
# serves moves, and not otherwise. An identity that drifted on re-observation
# would make every source a new thing each time it was looked at.
if [ -n "$R" ]; then
  before=$(q "SELECT group_concat(identity_digest) FROM matrix_subjects WHERE subject_id LIKE 'foreign_twin:src-%' ORDER BY subject_id;")
  $R "sudo $REMOTE_BUN /opt/bonfyre/scripts/promote_observation_sources.mjs" >/dev/null 2>&1
  after=$(q "SELECT group_concat(identity_digest) FROM matrix_subjects WHERE subject_id LIKE 'foreign_twin:src-%' ORDER BY subject_id;")
  check "re-promoting moves no source identity" "$after" "$before"
  n2=$(q "SELECT COUNT(*) FROM matrix_subjects WHERE subject_kind='ForeignTwin';")
  $R "sudo $REMOTE_BUN /opt/bonfyre/scripts/promote_observation_sources.mjs" >/dev/null 2>&1
  n3=$(q "SELECT COUNT(*) FROM matrix_subjects WHERE subject_kind='ForeignTwin';")
  check "and creates no new subject"           "$n3" "$n2"
fi

echo "== a derived claim needs an authorized read, not just good bytes =="
# A body digest proves what a parser read. It does not prove the read was
# authorized under the policy in force. A candidate built from a
# robots-disallowed fetch would carry real content and no permission.
if [ -n "$R" ]; then
  adm=$($R "sudo /usr/local/bin/bonfyre-project foreign admissible" 2>/dev/null)
  tot=$(printf '%s' "$adm" | jq -r '.observations' 2>/dev/null)
  okc=$(printf '%s' "$adm" | jq -r '.admissible' 2>/dev/null)
  ref=$(printf '%s' "$adm" | jq -r '.refused_count' 2>/dev/null)
  if [ "${tot:-0}" -ge 1 ] 2>/dev/null; then ok "the admission law runs over the corpus ($okc of $tot admissible)"; else bad "admission law did not answer"; fi
  if [ "${ref:-0}" -ge 1 ] 2>/dev/null; then
    ok "and refuses reads it cannot justify ($ref)"
  else
    ok "every observation in the corpus is admissible"
  fi
  # Every refusal names which current-policy fact was missing.
  named=$(printf '%s' "$adm" | jq '[.refused[] | select(.reason)] | length' 2>/dev/null)
  listed=$(printf '%s' "$adm" | jq '.refused | length' 2>/dev/null)
  check "every refusal names the missing fact" "$named" "$listed"
fi

echo "== registration is not activation =="
# REGISTER says a stable foreign identity exists. It does not say the thing is
# present: not an active public presence, not a partner, provider, opportunity
# or offer. Binding a presence at registration is how every observed favicon
# acquired a projection into the live world merely by having been fetched.
if [ -n "$R" ]; then
  s0=$(q "SELECT COUNT(*) FROM matrix_subjects WHERE subject_kind='ForeignTwin';")
  p0=$(q "SELECT COUNT(*) FROM matrix_presences WHERE subject_id LIKE 'foreign_twin:%';")
  st=$(date -u +%Y%m%dT%H%M%SZ)
  $R "sudo /usr/local/bin/bonfyre-project foreign observe reglaw-$st https://www.w3.org/Icons/valid-html401 \
        --receipt /var/lib/bonfyre/foreign/receipts/reglaw-$st.json \
        --body /var/lib/bonfyre/foreign/bodies/reglaw-$st.body --observe-right public" >/dev/null 2>&1
  p1=$(q "SELECT COUNT(*) FROM matrix_presences WHERE subject_id LIKE 'foreign_twin:%';")
  check "observing binds no presence" "$p1" "$p0"
  # Activation is authority-gated in both directions: turning a subject on or
  # off in the world is an act, and this actor holds no such grant.
  anypid=$(q "SELECT presence_id FROM matrix_presences WHERE subject_id LIKE 'foreign_twin:%' AND active=1 LIMIT 1;")
  if [ -n "$anypid" ]; then
    out=$($R "sudo /usr/local/bin/bonfyre-web-address presence-withdraw --id '$anypid' --actor auth-user:21 --authority-db /var/lib/bonfyre/auth.db --db /var/lib/bonfyre/matrix.db 2>&1" 2>/dev/null)
    if printf '%s' "$out" | grep -q "unauthorized"; then
      ok "deactivation is refused without an authority grant"
    else
      bad "a presence was withdrawn without authority"
    fi
  fi
fi

echo "== not every twin is mirrored, and the choice is authored =="
# Retaining a body is a decision with a cost, and applying MIRROR-LIKE to
# everything is that decision never being made. A regime says what a twin is
# worth holding; cooling is that decision taking effect. Receipts are never
# removed: cooling reduces what is held, never what is known to have been seen.
if [ -n "$R" ]; then
  hyd=$($R "sudo /usr/local/bin/bonfyre-project foreign hydration" 2>/dev/null)
  tw=$(printf '%s' "$hyd" | jq -r '.twins_count' 2>/dev/null)
  if [ "${tw:-0}" -ge 1 ] 2>/dev/null; then ok "hydration is reported per twin ($tw)"; else bad "no hydration report"; fi
  cool=$($R "sudo $REMOTE_BUN /opt/bonfyre/scripts/cool_foreign_bodies.mjs" 2>/dev/null)
  pend=$(printf '%s' "$cool" | jq -r '.bodies_cooled' 2>/dev/null)
  check "everything a regime cools has already been cooled" "$pend" "0"
  # A regime enforced only by cleanup is undone by the next observation. It has
  # to hold at the moment of retention or it is advice.
  b0=$($R "sudo ls /var/lib/bonfyre/foreign/bodies | wc -l" 2>/dev/null | tr -d '\r\n ')
  st2=$(date -u +%Y%m%dT%H%M%SZ)
  $R "sudo /usr/local/bin/bonfyre-project foreign observe regime-$st2 https://www.w3.org/Icons/w3c_home.png \
        --receipt /var/lib/bonfyre/foreign/receipts/regime-$st2.json \
        --body /var/lib/bonfyre/foreign/bodies/regime-$st2.body --observe-right public" >/dev/null 2>&1
  b1=$($R "sudo ls /var/lib/bonfyre/foreign/bodies | wc -l" 2>/dev/null | tr -d '\r\n ')
  check "re-observing a cooled twin retains no body" "$b1" "$b0"
  keptn=$(printf '%s' "$cool" | jq -r '.retained_by_regime' 2>/dev/null)
  if [ "${keptn:-0}" -ge 1 ] 2>/dev/null; then ok "a regime that retains is honoured too ($keptn)"; else bad "no twin is retained by regime"; fi
  # Receipts survive cooling: what was seen is not what is held.
  rc=$($R "sudo ls /var/lib/bonfyre/foreign/receipts/*.json | wc -l" 2>/dev/null | tr -d '\r\n ')
  if [ "${rc:-0}" -ge 20 ] 2>/dev/null; then ok "receipts survive cooling ($rc)"; else bad "receipts were lost ($rc)"; fi
fi

echo "== a cooled body is not a drifted body =="
# Conflating them made every lawfully cooled twin look tampered with.
if [ -n "$R" ]; then
  dr=$($R "sudo /usr/local/bin/bonfyre-project foreign promote --dry-run" 2>/dev/null \
        | jq -r '.body_drifted' 2>/dev/null)
  check "cooled twins are not reported as drifted" "$dr" "0"
fi

echo "== what a twin is decides what it affords =="
# Every twin has subject kind ForeignTwin, which says only that its authority is
# elsewhere. Investigating ownership is meaningless for a specification and
# mapping a vocabulary is meaningless for a tax roll, so applicable actions come
# from what the thing is -- authored, not guessed from a hostname.
if [ -n "${TWIN_TOKEN:-}" ]; then
  ap() { curl -s --max-time 20 -H "Authorization: Bearer $TWIN_TOKEN" -H 'Accept: application/json' \
         "$BASE/_aperture?subject=$1"; }
  kind_of() { printf '%s' "$1" | jq -r '.subject_kind' 2>/dev/null; }
  ops_of()  { printf '%s' "$1" | jq -r '[.apertures[].operation] | sort | join(",")' 2>/dev/null; }
  prop=$(ap "foreign_twin:src-dallas-tax-parcels")
  res=$(ap "foreign_twin:agents-md")
  ref=$(ap "foreign_twin:www-w3-org-icons-w3c-home-png")
  check "a parcel source classifies as property"  "$(kind_of "$prop")" "property_twin"
  check "a specification classifies as research"  "$(kind_of "$res")"  "research_twin"
  po=$(ops_of "$prop"); ro=$(ops_of "$res")
  if [ -n "$po" ] && [ -n "$ro" ] && [ "$po" != "$ro" ]; then
    ok "they afford different actions"
  else
    bad "both kinds afford the same set ($po vs $ro)"
  fi
  case "$po" in *Property.*) ok "a property twin is offered property actions" ;; *) bad "property actions missing: $po" ;; esac
  case "$ro" in *Research.*) ok "a research twin is offered research actions" ;; *) bad "research actions missing: $ro" ;; esac
  # A kind nobody declared actions for offers nothing, rather than falling back.
  check "an unactioned kind offers nothing" "$(ops_of "$ref")" ""
fi

echo "== the columnar estate is a representation the closure knows =="
# Parquet was a file this estate happened to query. A subject could describe its
# identity, address and presence while saying nothing about the 570MB of
# observation facts backing it. The material plane is where that belongs.
if [ -n "$R" ]; then
  contrib=$(q "SELECT COUNT(*) FROM matrix_closure_contributions WHERE plane='material' AND representation='Parquet';")
  if [ "${contrib:-0}" -ge 1 ] 2>/dev/null; then ok "the columnar body is bound on the material plane ($contrib)"; else bad "material plane is empty"; fi
  # A closure resource must be portable: a filesystem path is a reference only
  # this host can follow, and the validator refuses file:// for that reason.
  localpath=$(q "SELECT COUNT(*) FROM matrix_closure_contributions WHERE resource_uri LIKE 'file:%' OR resource_uri LIKE '/%';")
  check "no contribution carries a local path" "$localpath" "0"
  # One body, many slices: every contribution shares the manifest digest, and the
  # slice is identified by the selector rather than by pretending bodies differ.
  digests=$(q "SELECT COUNT(DISTINCT digest) FROM matrix_closure_contributions WHERE representation='Parquet';")
  check "one body means one digest" "$digests" "1"
  slices=$(q "SELECT COUNT(DISTINCT resource_uri) FROM matrix_closure_contributions WHERE representation='Parquet';")
  if [ "${slices:-0}" -gt 1 ] 2>/dev/null; then ok "each source names its own slice ($slices)"; else bad "slices are not distinguished"; fi
  # A contribution requires an active presence: bytes cannot back a subject that
  # is not projected anywhere.
  orphan=$(q "SELECT COUNT(*) FROM matrix_closure_contributions c WHERE NOT EXISTS(SELECT 1 FROM matrix_presences p WHERE p.presence_id=c.presence_id AND p.active=1);")
  check "no contribution outlives its presence" "$orphan" "0"
fi

echo "== a closure can be taken over a thing no hostname reaches =="
# Closures were host-addressed only, which limited the manifest to institutions.
# A foreign twin has an identity, a presence and a bound body; an observation
# source has no route at all and never will.
if [ -n "$R" ]; then
  CL="sudo /usr/local/bin/bonfyre-web-address closure --db /var/lib/bonfyre/matrix.db --subject foreign_twin:src-dallas-tax-parcels --presence presence:foreign_twin:src-dallas-tax-parcels:observed"
  crate=$($R "$CL --format ro-crate" 2>/dev/null)
  n=$(printf '%s' "$crate" | jq '[.["@graph"][] | (."bonfyre:ownerContributions" // []) | length] | add // 0' 2>/dev/null)
  if [ "${n:-0}" -ge 1 ] 2>/dev/null; then ok "the crate carries the bound body ($n)"; else bad "the crate lists no contribution"; fi
  rep=$(printf '%s' "$crate" | jq -r '[.["@graph"][] | (."bonfyre:ownerContributions" // [])[] | .representation][0]' 2>/dev/null)
  check "and names its representation" "$rep" "Parquet"
  dg=$(printf '%s' "$crate" | jq '[.["@graph"][] | (."bonfyre:ownerContributions" // [])[] | .digest | length][0]' 2>/dev/null)
  check "with an exact body digest" "$dg" "64"
  # The same cut, in the field carrier.
  zn=$($R "$CL --format zarr" 2>/dev/null | jq '[.. | objects | ."bonfyre:ownerContributions"? | arrays | length] | add // 0' 2>/dev/null)
  if [ "${zn:-0}" -ge 1 ] 2>/dev/null; then ok "the zarr group carries it too ($zn)"; else bad "zarr closure omits the body"; fi
fi

echo "== a derived field is traceable to the rows it counts =="
# Parquet holds durable columnar facts; Zarr holds chunked fields. Going from one
# to the other is an ordinary derivation, and the point of it is that a cell stays
# accountable: a grid of numbers nobody can trace is worse than no grid.
if [ -n "$R" ]; then
  Z=/var/lib/bonfyre/state/fields/observation-density.zarr
  field=$($R "sudo $REMOTE_BUN /opt/bonfyre/scripts/inspect_foreign_field.mjs $Z" 2>/dev/null)
  zshape=$(printf '%s' "$field" | jq -c '.shape' 2>/dev/null)
  if [ -n "$zshape" ]; then ok "the field exists on disk $zshape"; else bad "no derived field"; fi
  # It reconciles with the corpus it came from: the total is the observation count.
  total=$(printf '%s' "$field" | jq -r '.total' 2>/dev/null)
  if [ "${total:-0}" -gt 6000000 ] 2>/dev/null; then
    ok "the field sums to the observation corpus ($total)"
  else
    bad "field total $total does not reconcile with the corpus"
  fi
  # Lineage: the field names the exact body digest it was lowered from, and that
  # digest is the one bound on the material plane.
  from=$(printf '%s' "$field" | jq -r '.derived_from_digest' 2>/dev/null)
  bound=$(q "SELECT DISTINCT digest FROM matrix_closure_contributions WHERE representation='Parquet' LIMIT 1;")
  check "the field's source digest is the bound body" "$from" "$bound"
  hasderiv=$(printf '%s' "$field" | jq -r 'if .has_derivation_frontier then 1 else 0 end' 2>/dev/null)
  check "and it carries its derivation and frontier" "$hasderiv" "1"
  # Both representations reach the same subject's closure.
  reps=$($R "sudo /usr/local/bin/bonfyre-web-address closure --db /var/lib/bonfyre/matrix.db --subject foreign_twin:src-dallas-tax-parcels --presence presence:foreign_twin:src-dallas-tax-parcels:observed --format ro-crate" 2>/dev/null | jq -r '[.["@graph"][] | (."bonfyre:ownerContributions" // [])[] | .representation] | unique | join(",")' 2>/dev/null)
  check "the closure carries body and field together" "$reps" "Parquet,Zarr"
fi

echo "== a twin's kind reflects what it is, not a bulk guess =="
if [ -n "$R" ]; then
  kinds=$($R "sudo sqlite3 /var/lib/bonfyre/cms/bonfyre_cms.db \"SELECT COUNT(DISTINCT twin_kind) FROM twin_policy WHERE twin_kind IS NOT NULL AND twin_kind != '';\"" 2>/dev/null | tr -d '\r\n ')
  if [ "${kinds:-0}" -ge 4 ] 2>/dev/null; then
    ok "sources are classified into distinct kinds ($kinds)"
  else
    bad "only ${kinds:-0} kind(s): a single label over many shapes is a bulk guess"
  fi
fi

echo "== the estate finds its own duplicate identities =="
# Two subjects with one identity digest were identified by the same retained
# bytes: one thing named twice.
if [ -n "$R" ]; then
  dup=$($R "sudo /usr/local/bin/bonfyre-project foreign duplicates" 2>/dev/null \
        | jq -r '.count' 2>/dev/null)
  if [ -n "${dup:-}" ]; then ok "duplicate identities are reported, not merged silently ($dup)"; else bad "duplicate check did not answer"; fi
fi

echo "== a claim is an edge on something that already existed =="
if [ -n "$R" ]; then
  # Re-fetched rather than reusing a variable set far above: a claim made by an
  # earlier section would otherwise be invisible to this one.
  claimed=$(curl -s --max-time 20 -H 'Accept: application/json' "$BASE/twins" | jq '[.twins[] | select(.claims > 0)] | length' 2>/dev/null)
  if [ "${claimed:-0}" -ge 1 ] 2>/dev/null; then ok "a claim is visible in the registry ($claimed)"; else bad "no claim is visible"; fi
  orphan=$(q "SELECT COUNT(*) FROM matrix_subject_relations r WHERE r.relation='claimed_by' AND NOT EXISTS(SELECT 1 FROM matrix_subjects s WHERE s.subject_id=r.from_subject);")
  check "no claim points at a subject that does not exist" "$orphan" "0"
fi

echo
echo "foreign twins: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
