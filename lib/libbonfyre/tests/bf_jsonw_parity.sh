#!/bin/sh
# Byte parity between bf_jsonw and JavaScript's JSON.stringify.
set -eu
here=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
cc -O2 -std=c11 -I"$here/../include" "$here/bf_jsonw_parity.c" "$here/../libbonfyre.a" -lm -o "$tmp/parity"
cat > "$tmp/cases.jsonl" <<'J'
[123456789012345680000,99999999999999990000,-123456789012345680000,9223372036854775807,9007199254740993,18446744073709551616]
{"b":1,"a":[1,2.5,-0.000001,1e21,123456789012,0.1,1e-7,-3,100,1.5e300,5e-324],"c":{},"d":[],"e":null,"f":true}
{"s":"quote \" back \\ slash \/ tab \t nl \n ctl \u0001 \u001f uni é é astral 😀 😀"}
{"10":0,"2":1,"b":2,"01":3,"1":4,"z":{"y":[{"x":[]},{}],"w":"v"},"é":1,"E":2,"a b":3,"":4}
[0,-0,1.0,2.50,1e2,1E-3,9007199254740991,0.30000000000000004,123e-20,4.35,-1.7976931348623157e308]
"top"
{"offer_id":"beans","cash_price_usd":"1.79","merchant":"Target","source_url":"https://example.test/beans"}
J
"$tmp/parity" < "$tmp/cases.jsonl" > "$tmp/c.out"
bun -e '
const { createHash } = require("node:crypto");
const canonical = v => v === null || typeof v !== "object" ? JSON.stringify(v)
  : Array.isArray(v) ? `[${v.map(canonical).join(",")}]`
  : `{${Object.keys(v).sort().map(k => `${JSON.stringify(k)}:${canonical(v[k])}`).join(",")}}`;
for (const line of require("node:fs").readFileSync(process.argv[1], "utf8").split("\n")) {
  if (!line) continue; const v = JSON.parse(line); const c = canonical(v);
  console.log(JSON.stringify(v, null, 2)); console.log(c);
  console.log(createHash("sha256").update(c, "utf8").digest("hex"));
}' "$tmp/cases.jsonl" > "$tmp/js.out"
diff "$tmp/js.out" "$tmp/c.out" && echo "bf_jsonw parity: PASS"
