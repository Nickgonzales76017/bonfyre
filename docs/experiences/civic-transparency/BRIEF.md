# Who Watches the Road: civic transparency brief

Operator goal 7614596e215848ea95b44745b404dcca. The owner pointed at three
public projects about license plate reader (ALPR) cameras as inspiration: a
camera map, a resident and council research toolkit, and an audit-log lookup.
This is an original Aurekai site built on existing owners. It borrows no code,
branding or text from those projects.

## What it is

A WebAddress site program (`civic-watch`) with six standing pages and a dated
front page:

| Page | Pattern it answers | Built from |
|---|---|---|
| Cameras (`/cameras`) | public infrastructure awareness | OpenStreetMap counts by town and county, through `civic-ledger towns` |
| Council votes (`/council-votes`) | who contracts the cameras | city Legistar agenda items, through `civic-ledger council` |
| Ask your city (`/ask`) | resident and council research kit | authored: a Texas Public Information Act letter, what to ask for, deadlines, council questions, a note to pass on |
| Search logs (`/search-logs`) | privacy-preserving audit-log explainer | authored: what a log line holds, what is redacted, what counts a redacted log still answers |
| Sources (`/sources`) | source and licensing review, confidence model | authored, plus `civic-ledger reads` listing every read tried |
| Fix something (`/fix`) | correction workflow | authored; corrections arrive through the site's contact page |

Readers are residents who just noticed a camera, council members and reporters.
The site ends in an action a resident can take that day: send the letter, or
ask one question at public comment.

## Source and licensing review

| Source | Access | Robots | Licence | Use |
|---|---|---|---|---|
| OpenStreetMap through a public Overpass server (`maps.mail.ru/osm/tools/overpass`) | query that returns only totals (`make ... count(nodes)`) | allowed | ODbL: attribution, share-alike on the database | counts per town and county, credited on every page that shows them |
| `overpass-api.de` | not used | robots disallows `/api/` | | ForeignTwin recorded `robots_disallow`; we stay out |
| Legistar web API (`webapi.legistar.com/v1/<client>/matters`) | OData title filter | no robots file (404), law allows | public records published by each city | title line, file number, status, date, link back to the city |
| The three inspiration projects | not read by any tool | | their own terms | nothing taken |

Every read goes through `bonfyre-project foreign observe` (robots preflight,
retained bytes, `content_sha256`). `civic-ledger` reads a body only when robots
allowed it, the source answered 200 and the bytes still hash to the receipt.

## Provenance and confidence model

Ranked on the Sources page, highest first:

1. A city's own record (contract, vote, policy).
2. Records a city released under the Public Information Act.
3. The volunteer map. Not confirmed by any city; may lag or lead the street.
4. News, linked, for context only.

Never used: guesses, single-sighting posts, anything that tracks a person or a
car. Each generated page states its source and the day it was read. A read that
failed (five Legistar clients answered 500 on 2026-10-30) is listed as failed,
and a failed re-read never replaces the last usable one (`build.sh observe`).

## Safety boundary, as enforced

- `civic-ledger towns` refuses a whole body if any element carries `lat`, `lon`,
  `center`, `geometry`, `bounds`, `nodes` or `members`, or is not a town or
  county count. No camera position enters the build.
- Council items are kept only when the title names a reader system or its vendor
  ("flock", "plate read", "plate camera", "plate recogn", "alpr", "lpr"); a
  registration ordinance that says "license plate" is dropped. Only the first
  title line is kept, which drops presenter lines.
- No lookup of a plate, and none will be added: answering it needs the plate log,
  which is a record of where people drove. The Search logs page says so.
- Logs a resident sends in are published as monthly counts and agency names only.
- No sender names are published; corrections land in the owner's inbox
  (`site inbox`), not on a page.

## Correction workflow

1. Contact page message (stored by the site program; nothing auto-publishes).
2. Owner checks it against a public record: the city's page, a released document
   or the map.
3. If supported: map fixes go to OpenStreetMap itself; council fixes come from a
   fresh read; `build.sh --observe` rebuilds; a dated post goes on the front page
   (`site post`).
4. If not supported: no change, and the reply says why.
5. Anything that could point to a person is taken down first and checked after.

## Distribution loop

- Each page is a plain address that reads well when pasted (`/cameras`,
  `/council-votes`, `/ask`), and every council section has its own anchor.
- The Ask page ends with a fill-in note to neighbors (town count, vote date, link
  back) in a copy block. A resident who sends a letter is asked, on Search logs
  and Fix something, to send back what the city released, which becomes the next
  dated post and the next set of counts.
- New council items appear on the next `--observe` build; each change is a dated
  post, which gives a returning reader a reason to come back and share again.
- Nothing is posted to social networks or sent to third parties by the system.
  Placing the site on a public host, and any outreach, is the owner's decision.

## Build and test

```
make -C cmd/BonfyreSourceFrontier tools/civic-ledger civic-ledger-test
sh docs/experiences/civic-transparency/build.sh --home DIR --obs DIR [--observe]
BONFYRE_TEST_WEB_BINARY=... sh cmd/BonfyreWebAddress/tests/civic_watch_site_test.sh
```

The default host is `watch.example.test`. Moving the site onto a live host
(`bonfyre-web-address site new ... --home ~/.bonfyre/estate-local`, then the
edge publish path) is a human step and has not been done.

## Open items

- Five Legistar clients (Arlington, Irving, Garland, Frisco, Denton) answered 500
  on 2026-10-30; their councils are absent until a later read succeeds. Cities on
  other agenda systems (Granicus, CivicClerk) need their own reader.
- Town counts come from one Overpass mirror; when it times out (504 seen on a
  retry), the previous read is kept.
- The kit's post header carries the author subject in hidden markup
  (`institution:civic-watch`); visible text is clean.
