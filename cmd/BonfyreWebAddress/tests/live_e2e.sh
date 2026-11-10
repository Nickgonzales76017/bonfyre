#!/bin/bash
HB='text/html,application/xhtml+xml,*/*;q=0.8'; JB='application/json'
pass=0; fail=0
ok(){ printf "  PASS  %-42s %s\n" "$1" "$2"; pass=$((pass+1)); }
no(){ printf "  FAIL  %-42s %s\n" "$1" "$2"; fail=$((fail+1)); }
chk(){ [ "$2" = "$3" ] && ok "$1" "$2" || no "$1" "got $2 want $3"; }
code(){ curl -s -o /dev/null -w '%{http_code}' --max-time 40 "$@"; }
body(){ curl -s --max-time 40 "$@"; }

echo "FACE A - anonymous human"
for h in aurekai.life aurekai.live aurekai.shop aurekai.work; do chk "$h renders" "$(code -H "Accept: $HB" https://$h/)" 200; done
chk "notes readable"        "$(code -H "Accept: $HB" https://aurekai.life/notes/)" 200
chk "protocols door"        "$(code -H "Accept: $HB" https://aurekai.life/protocols)" 200
chk "capability page"       "$(code -H "Accept: $HB" https://aurekai.life/capability)" 200
chk "private estate gated"  "$(code -H "Accept: $HB" https://jrss.aurekai.work/)" 302
chk "institution read shut" "$(code -H "Accept: $JB" https://aurekai.life/studio/v1/economy/status)" 401
chk "drafts stay unpublished" "$(code https://aurekai.life/research/hqq-claim-invalidation)" 404

echo "FACE B - machine / agent"
chk "agent card"            "$(code -H "Accept: $JB" https://aurekai.life/.well-known/agent.json)" 200
chk "a2a descriptor"        "$(code -H "Accept: $JB" https://aurekai.life/a2a)" 200
rpc=$(body -X POST -d '{"jsonrpc":"2.0","id":"e2e","method":"message/send","params":{"skill":"parcel-underwrite"}}' https://aurekai.life/a2a)
echo "$rpc" | grep -q '"code":-32003' && ok "a2a quotes payment" "$(echo "$rpc"|grep -o '"priceUsd":"[0-9.]*"')" || no "a2a quotes payment" "-"
x=$(body -H "Accept: $JB" https://aurekai.life/capability/parcel-underwrite|grep -o '"priceUsd":"[0-9.]*"'); a=$(echo "$rpc"|grep -o '"priceUsd":"[0-9.]*"')
[ "$x" = "$a" ] && ok "one price two carriers" "$x" || no "one price two carriers" "x=$x a=$a"
chk "unknown path anon is 401" "$(code -H "Accept: $JB" https://aurekai.life/studio/v1/models)" 401
chk "root json to machine"  "$(code -H "Accept: $JB" https://aurekai.life/)" 200
body -H "Accept: $JB" https://aurekai.life/|grep -q bonfyre-address-v1 && ok "root json is descriptor" ok || no "root json is descriptor" wrong
chk "webfinger"             "$(code "https://aurekai.life/.well-known/webfinger?resource=acct:bonfyre@aurekai.life")" 200
chk "activitypub actor"     "$(code -H "Accept: $JB" https://aurekai.life/ap/actor)" 200

echo "FACE C - new person, signup then login"
EM="e2e-$(date +%s)@aurekai.test"; PW="E2e-pass-$(date +%s)-Xq"
chk "login page"  "$(code -H "Accept: $HB" https://aurekai.life/login)" 200
chk "signup page" "$(code -H "Accept: $HB" https://aurekai.life/signup)" 200
body -X POST -H 'Content-Type: application/json' -d "{\"email\":\"$EM\",\"password\":\"$PW\",\"name\":\"E2E\"}" https://aurekai.life/signup | grep -q '"created":true' && ok "signup creates account" "$EM" || no "signup creates account" "-"
T=$(body -X POST -H 'Content-Type: application/json' -d "{\"email\":\"$EM\",\"password\":\"$PW\"}" https://aurekai.life/login | python3 -c 'import sys,json;print(json.load(sys.stdin)["session"]["token"])' 2>/dev/null)
[ -n "$T" ] && ok "login issues session" "scope=self" || no "login issues session" "-"
if [ -n "$T" ]; then
  chk "own profile readable"   "$(code -H "Authorization: Bearer $T" -H "Accept: $JB" https://aurekai.life/studio/v1/profile)" 200
  chk "own closure readable"   "$(code -H "Authorization: Bearer $T" -H "Accept: $JB" https://aurekai.life/studio/v1/closure)" 200
  chk "unknown path with session" "$(code -H "Authorization: Bearer $T" -H "Accept: $JB" https://aurekai.life/studio/v1/models)" 404
  for p in economy/status models/list ledger/export frappe-lineages; do
    chk "institution read: $p" "$(code -H "Authorization: Bearer $T" -H "Accept: $JB" https://aurekai.life/studio/v1/$p)" 403
  done
fi
echo "FACE D - the estate"
chk "healthz"               "$(code https://aurekai.life/healthz)" 200
chk "mcp protected"         "$(code -X POST -H 'Content-Type: application/json' -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' https://aurekai.work/mcp)" 401
chk "oauth metadata"        "$(code https://aurekai.work/.well-known/oauth-protected-resource/mcp)" 200
echo; echo "  $pass passed, $fail failed"
# Exit on the count. Thirty-three checks against the live estate ended on the
# summary echo, so this returned 0 whatever it had just printed: the suite that
# says whether the site is serving could not say no to anything reading its
# status. The numbers were always right and nothing consumed them.
[ "$fail" -eq 0 ]
