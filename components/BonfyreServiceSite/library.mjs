// A large collection, published so a phone can search and read all of it while
// fetching almost none of it. The collection is cut into small compressed
// pieces at build: entries in runs of 150, each kind's titles in pages, and
// title words by their first three letters. A visit fetches the one or two
// pieces it needs. Nothing here owns the collection; it is handed entries.
//
//   entry: { id, title, kinds: [..], serves, parts: [..lines], method, notes: [..], from, ways: [[name, [..adds], method, serves]] }
export const COMMON = new Set(["and", "the", "with", "for", "from"]);
export async function libraryFiles(entries, { run = 150, page = 300 } = {}) {
  // `entries` may be any iterable, sync or async; only titles and kinds are held while it is read.
  const files = {}, gz = value => Bun.gzipSync(Buffer.from(JSON.stringify(value)), { level: 9 });
  // Words nearly every title has are no way to find one; they are left out of the word pieces (a search still matches on them).
  const word = text => text.toLowerCase().normalize("NFKD").replace(/[^a-z0-9]+/g, " ").trim().split(" ").filter(w => w.length >= 3 && !COMMON.has(w));
  const titles = [], kinds = new Map(), words = new Map();
  let held = [];
  const flush = () => { if (held.length) { files[`e/${Math.floor((titles.length - 1) / run)}.json.gz`] = gz(held); held = []; } };
  for await (const e of entries) {
    const n = titles.length; titles.push(e.title);
    // An entry may be a family: `ways` lists its variations, each as [name, what it adds, method, serves].
    held.push([e.id, e.title, e.kinds ?? [], e.serves ?? "", e.parts ?? [], e.method ?? "", e.notes ?? [], e.from ?? "", e.ways ?? []]);
    for (const kind of new Set(e.kinds ?? [])) { if (!kinds.has(kind)) kinds.set(kind, []); kinds.get(kind).push(n); }
    for (const prefix of new Set(word(e.title).map(w => w.slice(0, 3)))) { if (!words.has(prefix)) words.set(prefix, []); words.get(prefix).push(n); }
    if (held.length === run) flush();
  }
  flush();
  const byTitle = (a, b) => (titles[a] < titles[b] ? -1 : titles[a] > titles[b] ? 1 : a - b), slug = text => text.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "") || "other";
  // A kind held by a single entry is a stray word, not a way to browse; such entries are still found by search.
  const groups = [], taken = new Set();
  // In a very large collection the bar rises, so the list of kinds stays something a person can read.
  const least = Math.max(2, Math.floor(titles.length / 5000));
  for (const [kind, members] of [...kinds].filter(([, m]) => m.length >= least).sort((a, b) => b[1].length - a[1].length || (a[0] < b[0] ? -1 : 1))) {
    let name = slug(kind).slice(0, 48); while (taken.has(name)) name += "-"; taken.add(name);
    members.sort(byTitle);
    for (let at = 0; at < members.length; at += page) files[`k/${name}/${at / page}.json.gz`] = gz(members.slice(at, at + page).map(n => [n, titles[n]]));
    groups.push([name, kind, members.length]);
  }
  for (const [prefix, members] of words) files[`w/${prefix}.json.gz`] = gz(members.sort(byTitle).map(n => [n, titles[n]]));
  files["about.json"] = Buffer.from(JSON.stringify({ count: titles.length, run, page, kinds: groups, common: [...COMMON], words: [...words.keys()].sort().join(" ") }));
  return { files, count: titles.length, kinds: groups.length, shipped_bytes: Object.values(files).reduce((sum, f) => sum + f.length, 0), pieces: Object.keys(files).length };
}
