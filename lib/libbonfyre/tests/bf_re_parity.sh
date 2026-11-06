#!/bin/sh
# Parity between bf_re and Bun's RegExp on tests/bf_re_cases.jsonl.
set -eu
here=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
cc -O2 -std=c11 -D_DEFAULT_SOURCE -D_DARWIN_C_SOURCE -I"$here/../include" "$here/bf_re_parity.c" "$here/../libbonfyre.a" -lm -o "$tmp/re"
"$tmp/re" < "$here/bf_re_cases.jsonl" > "$tmp/c.out"
bun -e '
const bytes = (s, i) => Buffer.byteLength(s.slice(0, i));
for (const line of require("node:fs").readFileSync(process.argv[1], "utf8").split("\n")) {
  if (!line) continue;
  const [pat, flags, input, rep] = JSON.parse(line);
  let re; try { re = new RegExp(pat, flags); } catch { console.log("compile error\n"); continue; }
  const once = new RegExp(pat, flags.replace("g", "")).exec(input);
  if (once) {
    const parts = ["match"];
    let at = once.index;
    parts.push(`${bytes(input, at)}:${bytes(input, at + once[0].length)}`);
    // group offsets via the d flag
    const d = new RegExp(pat, flags.replace("g", "") + "d").exec(input);
    for (let g = 1; g < d.length; g++) parts.push(d.indices[g] ? `${bytes(input, d.indices[g][0])}:${bytes(input, d.indices[g][1])}` : "-1:-1");
    console.log(parts.join(" "));
  } else console.log("no match");
  console.log(JSON.stringify(input.replace(re, rep ?? "<$&>")));
}' "$here/bf_re_cases.jsonl" > "$tmp/js.out"
diff "$tmp/js.out" "$tmp/c.out" && echo "bf_re parity: $(grep -c . "$here/bf_re_cases.jsonl") cases PASS"
