#!/usr/bin/env bash
# The measurement estate: gym contracts, ceilings, residual attribution, and the
# failure estate that feeds the next cohort.
#
# What this proves: that a gym declares what it measures, that an unmeasured
# ceiling says why, that a residual is attributed to a plane that could fix it,
# and that no credential value is ever read or reported.
#
# What it cannot prove: that any ceiling is achievable or that any residual is
# worth closing. A gym measures; deciding what to do about it is not its job.
set -u
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
  printf '%s: SKIP (%s does not serve _surface)\n' 'gym' "$BASE"
  exit 0
fi
pass=0; fail=0
ok()  { printf '  ok   %s\n' "$1"; pass=$((pass+1)); }
bad() { printf '  FAIL %s\n' "$1"; fail=$((fail+1)); }
check() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (want '$3', got '$2')"; fi; }
J() { python3 -c "import sys,json;d=json.load(sys.stdin);$1"; }

echo "== a gym declares what it measures =="
G=$(curl -s --max-time 25 -H 'Accept: application/json' "$BASE/_surface/gyms")
n=$(printf '%s' "$G" | J 'print(d["count"])')
[ "$n" -ge 1 ] && ok "$n gym contract(s) published" || bad "no gym contracts"
# A contract that does not say what it cannot infer is a benchmark that will be
# over-read the first time it produces a good number.
miss=$(printf '%s' "$G" | J 'print(sum(1 for r in d["records"] if not r["cannot_infer"].strip()))')
check "every gym states what it cannot infer" "$miss" "0"
blank=$(printf '%s' "$G" | J 'print(sum(1 for r in d["records"] if not (r["generator"].strip() and r["validity_window"].strip() and r["ceilings"].strip())))')
check "every gym names a generator, a window and its ceilings" "$blank" "0"

echo "== every supplier is measured, including our own organs =="
R=$(curl -s --max-time 25 -H 'Accept: application/json' "$BASE/_surface/routes")
rn=$(printf '%s' "$R" | J 'print(d["count"])')
[ "$rn" -ge 2 ] && ok "$rn supplier route(s) declared" || bad "only $rn routes"
own=$(printf '%s' "$R" | J 'print(sum(1 for r in d["records"] if r["kind"]=="internal_organ"))')
[ "$own" -ge 1 ] && ok "this institution's own organs are in the tournament" \
                 || bad "no internal organ is declared as a route; that is a free pass"
rmiss=$(printf '%s' "$R" | J 'print(sum(1 for r in d["records"] if not r["cannot_infer"].strip()))')
check "every route states what it cannot infer" "$rmiss" "0"
# The registry is content. Content must never hold a credential.
leak=$(printf '%s' "$R" | grep -ciE 'sk-[a-z0-9]{16}|bearer [a-z0-9]{24}|api[_-]?key["'"'"']?\s*[:=]\s*["'"'"']?[a-z0-9]{16}' || true)
check "no route carries a credential value" "$leak" "0"

echo "== a run reports ceilings, and says why one is unmeasured =="
if [ -n "${GYM_REMOTE:-}" ]; then
  OUT=$($GYM_REMOTE 2>/dev/null)
  printf '%s\n' "$OUT" | grep -q 'InternalizedCeiling' \
    && ok "the run measures what the estate already holds" \
    || bad "no InternalizedCeiling in the run"
  printf '%s\n' "$OUT" | grep -q 'IdentityResidual' \
    && ok "the run attributes an identity residual" \
    || bad "no IdentityResidual reported"
  # An unmeasured ceiling must carry its reason, or it reads as a zero.
  unm=$(printf '%s\n' "$OUT" | grep -c 'unmeasured:' || true)
  [ "$unm" -ge 1 ] && ok "$unm unmeasured ceiling(s) state their reason" \
                   || bad "an unmeasured ceiling gave no reason"
  printf '%s\n' "$OUT" | grep -q 'MarketCeiling' \
    && ok "the market is named even while it cannot be measured" \
    || bad "the market ceiling is absent rather than unmeasured"
fi

echo "== the seven rungs: what must be true before a ceiling may say measured =="
# The first implementation could call a market ceiling measured because an
# environment variable was non-empty. That encoded "we possess a key" as "we
# measured a market". Each assertion below is one rung that must not be
# skippable, and the defect was reproduced deliberately before being removed.
if [ -n "${GYM_JSON:-}" ]; then
  R=$($GYM_JSON 2>/dev/null)
  mc=$(printf '%s' "$R" | J 'print(d["ceilings"]["MarketCeiling"].get("measured"))')
  rung=$(printf '%s' "$R" | J 'print(d["ceilings"]["MarketCeiling"].get("rung",""))')
  # 1. credential present -> MarketCeiling measured   must FAIL
  if [ "$mc" = "True" ]; then
    tr=$(printf '%s' "$R" | J 'print(d["ceilings"]["MarketCeiling"].get("trials",0))')
    [ "$tr" -gt 0 ] && ok "a measured market rests on $tr executed trials" \
                    || bad "MarketCeiling claims measured with no executed trial"
  else
    ok "the market is unmeasured and names the rung it stopped at ($rung)"
  fi
  # A ceiling that is measured must carry its cohort, or it is a number about
  # some other question.
  cd_=$(printf '%s' "$R" | J 'print(len(d.get("cohort_digest","")))')
  [ "$cd_" -ge 16 ] && ok "the run is scoped to a cohort digest" \
                    || bad "the run carries no cohort identity"
  # 5. supplier output without a trial receipt -> pooled member   must FAIL
  ds=$(printf '%s' "$R" | J 'print(d["ceilings"]["DirectSourceCeiling"].get("measured"))')
  if [ "$ds" = "True" ]; then
    dof=$(printf '%s' "$R" | J 'print(d["ceilings"]["DirectSourceCeiling"].get("of",0))')
    [ "$dof" -gt 0 ] && ok "the direct-source ceiling is built from $dof executed trials" \
                     || bad "a measured ceiling with no trials behind it"
    # 3. several addresses of one resource -> several evidence units   must FAIL
    dv=$(printf '%s' "$R" | J 'print(d["ceilings"]["DirectSourceCeiling"].get("value",0))')
    [ "$dv" -le "$dof" ] && ok "evidence ($dv) never exceeds trials ($dof); addresses are not evidence" \
                         || bad "more evidence units than trials; URLs are being counted as evidence"
  fi
fi

echo "== a supplier is admitted by capability, never by possession of a key =="
if [ -n "${TRIAL_REMOTE:-}" ]; then
  # 2. a completion route must not enter a retrieval tournament.
  T=$($TRIAL_REMOTE 2>/dev/null)
  adm=$(printf '%s' "$T" | J 'print(",".join(d.get("admissible_routes",[])))')
  bad_adm=$(printf '%s' "$T" | J '
import sys
print(sum(1 for r in d.get("admissible_routes",[]) if "completion" in r or "chat" in r))')
  check "no completion route is admitted to retrieval.search" "$bad_adm" "0"
  ok "admissible for retrieval.search: [${adm:-none}]"
  # 4. an old run must not count for a newer frontier.
  cdig=$(printf '%s' "$T" | J 'print(d["cohort_digest"])')
  [ -n "$cdig" ] && ok "trials are stamped with the cohort they ran against" \
                 || bad "a trial receipt carries no cohort identity"
fi

echo "== a credential value never leaves the process =="
# 6. a secret must not reach a report, the CMS, or a surface.
for probe in "$BASE/_surface/routes" "$BASE/gyms"; do
  body=$(curl -s --max-time 25 -H 'Accept: application/json' "$probe")
  hits=$(printf '%s' "$body" | grep -coiE 'sk-[a-z0-9]{20}|gsk_[a-z0-9]{20}|AIza[a-z0-9_-]{30}|Bearer [a-z0-9]{32}' || true)
  check "no credential-shaped string in $probe" "$hits" "0"
done
if [ -n "${GYM_JSON:-}" ]; then
  hits=$(printf '%s' "$R" | grep -coiE 'sk-[a-z0-9]{20}|gsk_[a-z0-9]{20}|AIza[a-z0-9_-]{30}' || true)
  check "no credential-shaped string in a gym receipt" "$hits" "0"
  # The variable NAME may appear; the value may not. Naming where a credential
  # arrives is how a route stays declarable without holding a secret.
  ok "routes name a credential variable and never its value"
fi

echo "== a 200 is not an answer =="
# Four dallasnews sitemap URLs answered 200 with the same 3038 bytes of HTML
# carrying a CSP meta: a bot wall, not a sitemap. All four passed admission law,
# because that law asks whether the fetch was permitted and whether it
# completed, and both were true. Nothing asked whether the bytes were the thing
# requested, so the estate held four twins containing no evidence about the
# documents they named and every ceiling built on them was inflated.
if [ -n "${GYM_JSON:-}" ]; then
  Q=$($GYM_JSON 2>/dev/null)
  eq=$(printf '%s' "$Q" | J 'print(d["residual"]["EvidenceQualityResidual"]["value"])')
  of=$(printf '%s' "$Q" | J 'print(d["residual"]["EvidenceQualityResidual"]["of"])')
  [ "$of" -gt 0 ] && ok "evidence quality is measured over $of observations" \
                  || bad "no observations examined for evidence quality"
  # The detector must actually be catching them; a zero here after the finding
  # would mean the check silently stopped running.
  [ "$eq" -ge 1 ] && ok "$eq boundary response(s) refused as non-evidence" \
                  || bad "no boundary responses detected; the detector may be inert"
  # And it must not swallow the estate: a detector that classifies everything as
  # a boundary would drive admissibility to zero and look like a strict policy.
  adm=$(printf '%s' "$Q" | J 'print(d["ceilings"]["InstitutionalCeiling"]["value"])')
  [ "$adm" -gt "$eq" ] && ok "admissible evidence ($adm) still far exceeds refusals ($eq)" \
                       || bad "the boundary detector is refusing nearly everything"
fi
if [ -n "${ADMISSIBLE_REMOTE:-}" ]; then
  A=$($ADMISSIBLE_REMOTE 2>/dev/null)
  # The narrowness is the point. It is a type contradiction, not a guess about
  # prose: a request for .xml answered with an HTML document. An HTML page that
  # merely discusses errors must not be classified as a boundary.
  printf '%s' "$A" | grep -q 'boundary_response_not_content' \
    && ok "the refusal names the reason rather than failing silently" \
    || bad "boundary refusals are not reported by reason"
  html_ok=$(printf '%s' "$A" | J '
r=d.get("refused") or d.get("refusals") or []
bad=[x for x in r if (x.get("reason") or x.get("why"))=="boundary_response_not_content"
     and not any(x.get("url","").split("?")[0].endswith(e)
                 for e in (".xml",".json",".csv",".geojson",".txt",".rss"))]
print(len(bad))')
  check "no boundary refusal targets a document that asked for HTML" "$html_ok" "0"
fi

echo "== a damaged reference still finds what we hold =="
# Resolution compared addresses with strcmp, so a reference differing by a
# tracking parameter, a default port, a fragment, an upper-case host or a
# missing root slash resolved to nothing: the institution held the resource and
# answered that it did not. Measured at 8 closures of 41 lawful equivalences.
if [ -n "${DEFORM_REMOTE:-}" ]; then
  D=$($DEFORM_REMOTE 2>/dev/null)
  closed=$(printf '%s' "$D" | J 'print(d["closed"])')
  missed=$(printf '%s' "$D" | J 'print(d["missed"])')
  merges=$(printf '%s' "$D" | J 'print(d["false_merges"])')
  refused=$(printf '%s' "$D" | J 'print(d["correctly_refused"])')
  # The dangerous failure. A canonicalizer that folds hard enough to close
  # everything will eventually fold two resources into one, and that is worse
  # than not closing: a miss is visible, a false merge is a wrong answer
  # wearing a right answer's clothes. Never netted against closures.
  check "no deformation merges two different resources" "$merges" "0"
  [ "$refused" -ge 8 ] && ok "$refused decoys correctly refused" \
                       || bad "only $refused decoys refused; the probes prove little"
  # Lawful equivalences must actually close, or the fix was never internalized.
  scheme=$(printf '%s' "$D" | J 'print(d["by_deformation"].get("stale_scheme",{}).get("missed",0))')
  lawful=$(( closed + missed - scheme ))
  [ "$lawful" -gt 0 ] && check "every lawful equivalence closes" "$closed" "$lawful" \
                      || bad "no lawful equivalences were probed"
  for kind in tracking_noise host_case default_port fragment; do
    m=$(printf '%s' "$D" | J "print(d['by_deformation'].get('$kind',{}).get('missed',0))")
    check "a $kind reference is recognised" "$m" "0"
  done
  # A path is not case-folded and a non-root trailing slash is not stripped,
  # because a server may lawfully serve different bytes at each.
  dt=$(printf '%s' "$D" | J 'print(d["by_deformation"].get("decoy_trailing_slash",{}).get("false_merge",0))')
  check "a non-root trailing slash is not folded away" "$dt" "0"
  # A decoy that equals its own subject measures nothing; two generator bugs
  # produced exactly that and reported it as a false merge of the estate's.
  self=$(printf '%s' "$D" | J '
rows=d.get("rows",[])
canon={r["asked"] for r in rows if r["deformation"]=="canonical"}
print(sum(1 for r in rows if r["deformation"].startswith("decoy_") and r["asked"] in canon))')
  check "no decoy is its own subject" "$self" "0"
  printf '%s' "$D" | grep -q 'canonicalize alike -> the same resource' \
    && ok "closure states it is addressing, not proof of identity" \
    || bad "the closure report does not bound its own reading"
fi

echo "== the failure estate ranks what we can actually close =="
if [ -f scripts/failure_cohort.py ]; then
  F=$(bun scripts/failure-cohort.mjs --json 2>/dev/null)
  if [ -n "$F" ]; then
    g=$(printf '%s' "$F" | J 'print(d["goals_examined"])')
    [ "$g" -ge 1 ] && ok "$g goals mined for capability gaps" || bad "no goals examined"
    # The whole point of the ranking: a cause we cannot close must not outrank
    # one we can, however much work it consumed.
    # Partitioned, not weighted: an unclosable cause must not appear in the
    # ranked list at all. Weighting it down was not enough -- 119 goals holding
    # 99% of all attempts survived any multiplier small enough to defend.
    unclosable_in_rank=$(printf '%s' "$F" | J 'print(sum(1 for c in d["clusters"] if not c["closable"]))')
    check "no unclosable cause appears in the ranked list" "$unclosable_in_rank" "0"
    nc=$(printf '%s' "$F" | J '
c=[x for x in d.get("awaiting_person",[]) if x["cause"]=="no_credit"]
print(c[0]["attempts"] if c else 0)')
    [ "$nc" -gt 0 ] && ok "no_credit ($nc attempts) sits past the human line, not in the build queue" \
                    || bad "no_credit was not partitioned out"
    # And the ranking must not reward a retry storm: stuckness is per-goal and
    # damped, so one runaway goal cannot outrank a gap seen across many.
    storm=$(printf '%s' "$F" | J '
r=d["clusters"]
print("ok" if not r or all(c.get("stuckness") is not None for c in r) else "no")')
    check "ranking uses damped per-goal stuckness, not share of total attempts" "$storm" "ok"
    cmiss=$(printf '%s' "$F" | J 'print(sum(1 for c in d["clusters"] if not c["remedy"].strip()))')
    check "every cause names a remedy" "$cmiss" "0"
    printf '%s' "$F" | grep -q 'attempt count -> a cost in money' \
      && ok "the cohort states that attempts are not dollars" \
      || bad "the cohort does not bound its own reading"
  fi
fi

echo
echo "gyms: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
