#!/usr/bin/env bun
import { spawnSync } from "node:child_process";

const port = process.argv[2];
if (!/^[0-9]+$/.test(port ?? "")) throw new Error("page commons checker requires a port");
const base = `http://127.0.0.1:${port}`;
const problems = [];
const check = (ok, why) => { if (!ok) problems.push(why); };
function get(path) {
  const child = spawnSync("curl", ["-sS", "-i", "-H", "Host: aurekai.life", "-H", "Accept: text/html", base + path],
    { encoding: "utf8", timeout: 10_000, maxBuffer: 8 * 1024 * 1024 });
  if (child.error || child.status !== 0) throw new Error(`page commons request failed: ${path}`);
  const split = child.stdout.indexOf("\r\n\r\n");
  const head = child.stdout.slice(0, split), body = child.stdout.slice(split + 4);
  return { status: head.match(/^HTTP\/\S+\s+(\d+)/)?.[1] ?? "", body };
}
function decode(value) {
  return value.replace(/&(#x[0-9a-f]+|#[0-9]+|[a-z]+);/gi, (entity, code) => {
    const names = { amp: "&", lt: "<", gt: ">", quot: '"', apos: "'", nbsp: " " };
    if (code.startsWith("#")) {
      const point = code[1]?.toLowerCase() === "x" ? Number.parseInt(code.slice(2), 16) : Number.parseInt(code.slice(1), 10);
      return Number.isInteger(point) && point <= 0x10ffff ? String.fromCodePoint(point) : entity;
    }
    return names[code.toLowerCase()] ?? entity;
  });
}
function words(page) {
  return decode(page.split("</head>").slice(1).join("</head>")
    .replace(/<(script|style|template)[^>]*>[\s\S]*?<\/\1>/gi, " ")
    .replace(/<[^>]+>/g, " ").replace(/\s+/g, " "));
}
function firstItem(page, source) {
  return page.match(new RegExp(`href="(\\/o\\/${source}\\/[0-9a-f]{24})"`))?.[1] ?? null;
}
const leak = /\b(World|Surface|Scene|Closure|Runtime|Semantic|Occurrence|Projection|Aperture|Frontier|ForeignTwin|AddressPlane|SPARQL|QLever|Wikidata entity|bindings)\b/g;

const day = get("/on-this-day");
check(day.status === "200", `on this day answers (${day.status})`);
const lead = day.body.match(/<section class="[^"]*region-lead"[^>]*>([\s\S]*?)<\/section>/)?.[1];
check(Boolean(lead?.includes("Eternal Blue (album)") && lead.includes("debut studio album")),
  "today's article leads by its own words though its picture is withheld");
check(!day.body.includes("wikipedia/en/"), "an English Wikipedia upload is never shown");
check(day.body.includes("Eurasian coot (Fulica atra) with chicks"), "a file name is read as words");
check(!words(day.body).includes("File:"), "no file namespace reaches a person");
check(day.body.includes("Charles J. Sharp") && !day.body.split("</head>").at(-1).includes("<bdi"),
  "the picture's author is read from markup as words");
check(day.body.includes('<span class="terms">CC BY-SA 4.0</span>'), "a photograph states its terms beside its byline");
check(!leak.test(words(day.body)), "no machine vocabulary on the almanac");
check(day.body.includes("<span>2013</span>"), "an event is dated by its year");

const potd = firstItem(day.body, "wikimedia-picture-of-the-day");
check(potd !== null, "the picture of the day has its own page");
if (potd) {
  const page = get(potd);
  check(page.status === "200" && page.body.includes('class="hero-picture"'), "the picture opens large");
  check(page.body.includes("Special:FilePath/Eurasian_coot_%28Fulica_atra%29_with_chicks.jpg?width=1280"),
    "the commons is asked for the picture at the width the page needs");
  check(page.body.includes('Picture from <a rel="noopener" target="_blank" href="https://commons.wikimedia.org/wiki/File:Eurasian_coot_%28Fulica_atra%29_with_chicks.jpg">Wikimedia Commons</a>'),
    "the picture credits the commons that keeps it, linked to its page");
  check(page.body.includes('href="https://creativecommons.org/licenses/by-sa/4.0">CC BY-SA 4.0</a>'),
    "its terms link to where they are written");
  check(page.body.includes("<summary>Use this picture</summary>") && page.body.includes("&ldquo;Eurasian coot (Fulica atra) with chicks&rdquo; by Charles J. Sharp, CC BY-SA 4.0, via Wikimedia Commons"),
    "using it offers the credit line its terms ask for");
}

const shooting = firstItem(day.body, "wikipedia-on-this-day");
check(shooting !== null, "an anniversary has its own page");
if (shooting) {
  const page = get(shooting);
  check(page.body.includes("Also about this") && page.body.split("Also about this").at(-1).includes("/o/wikipedia-most-read/"),
    "the same identity reported elsewhere is linked as the same thing");
  check(page.body.includes("Read at Wikipedia"), "the link out is named by where it goes");
}
check((day.body.match(/<b>Washington Navy Yard shooting<\/b>|>Washington Navy Yard shooting</g) ?? []).length <= 1,
  "one identity reported twice appears once on a page");

const board = get("/board");
check(board.body.includes("Hunter baby chameleon(young)"), "an encoded file name becomes plain words");
check(board.body.includes("Mkrc85"), "a contributor named only by account is still credited");

const listen = get("/listen");
check(listen.status === "200" && listen.body.includes('<ol class="tracks">'), "listen is a track list");
check(listen.body.includes('data-play="https://commons.wikimedia.org/wiki/Special:FilePath/Ludwig_van_Beethoven_-_symphony_no._5.ogg"'),
  "a recording names the file it plays, from where it is kept");
check(listen.body.includes("data-deck") && (listen.body.match(/<audio/g) ?? []).length === 1,
  "and one deck plays them in turn");
const track = firstItem(listen.body, "wikidata-recorded-music");
check(track !== null, "a recording opens its own page");
if (track) {
  const page = get(track);
  check(page.status === "200" && page.body.includes("<audio controls") && page.body.includes("Recording from"),
    "a recording's page plays it and says where it is kept");
  check(page.body.includes("<summary>Use this recording</summary>"), "a recording can be used with its page's terms");
}

const watch = get("/watch");
const film = firstItem(watch.body, "wikidata-public-domain-films");
check(film !== null, "a film is on watch");
if (film) {
  const page = get(film);
  check(page.body.includes("<video controls") && page.body.includes("Night%20of%20the%20Living%20Dead%20(1968).webm"),
    "a film plays in place");
  check(page.body.includes("Video from") && page.body.includes("Public domain"), "a film says where it is kept and its standing");
  check(page.body.includes("Read about it at Wikipedia"), "reading about a film goes to its article");
  check(page.body.includes("1968 American independent horror film"), "a film is described in words");
}

const near = get("/nearby");
check(near.status === "200" && near.body.includes('class="plate streets"'), "places close together are drawn on their streets");
check(near.body.includes("tile.openstreetmap.org/") && near.body.includes("OpenStreetMap contributors"),
  "the street tiles carry their credit");
check(/<text x="[0-9.]+" y="[0-9.]+">1<\/text>/.test(near.body), "the map's points are numbered like its list");
check(near.body.includes('<span class="terms">CC BY-SA 4.0</span>') && near.body.includes("Fkbowen"),
  "a rated photograph states its author and terms");
const place = firstItem(near.body, "wikidata-places-dfw");
check(place !== null, "a place has its own page");
if (place) {
  const page = get(place);
  check(page.body.includes('class="map where"') && page.body.includes('class="plate streets"'),
    "a place with a picture also shows where it is");
  check(page.body.includes("Read at Wikipedia"), "a place's article is read, not seen");
  check(page.body.includes("width=1280"), "the place's picture is asked for large");
}

if (problems.length) {
  console.error("page commons: FAIL");
  for (const problem of problems) console.error(`  - ${problem}`);
  process.exitCode = 1;
} else console.log("page commons: PASS");
