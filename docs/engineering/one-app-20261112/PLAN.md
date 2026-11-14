# One app: the Stitch screens are the layout

2026-11-12. Direction from Nick: the estate reads as disconnected
micro-sites. Agents keep building new methods beside existing ones. Pages
keep their original demo body and get a "workflow" section added under
it. The whole app should take a visitor from first arrival to running
their own work, and the 91 Stitch screens already show the layout and
controls for nearly every page.

The rule is now in `CLAUDE.md` and `AGENTS.md` ("One app").

## What is wrong (measured 2026-11-12)

- **Many shells.** In `cmd/BonfyreWebAddress/src`, 27 files emit their own
  `<!doctype>`, 34 their own `<header>`, 57 an inline `<style>`. The
  journal and profile family (`profile_home.h`,
  `profile_journal_index.h`) uses a light shell with a different nav from
  the dark estate shell (`page_core_rendering.h`). An article's header
  ("Aurekai / Journal") does not match its own index ("Nick · Aurekai").
- **Stitch was applied as a re-skin.** The earlier pass reduced 91 screens
  to 12 patterns (`~/.bonfyre/design-references/stitch-20261104/review.csv`).
  95 `route_presentation` rows each wear a pattern, which sets only the
  colors and spacing in `composition.css`. The old demo bodies stayed.
- **Appended blocks.** The apex fronts end with "Explore the rest of
  Aurekai" and "Conversations to take part in" under the old body,
  standing in for navigation the shell should provide.
- **Live is behind.** The live binary was built 2026-11-10 14:24, before
  `/blog/` landed (`5e203602`, 14:58). The 90 canonical journal posts
  (`person-nick-work`) and their 327 sections are all `draft`. 77 older
  copies sit in `aurekai-journal`.
- **Post models are split.** Posts live in `site_page` (two namespaces),
  `community_post`, `wiki_page`, `community_body`, plus an empty
  `cms_article` type.

## The shell every Stitch screen shares

Read from `aurekai_discover` and `aurekai_editorial_discover_hub`:

1. Top bar: wordmark, territory switch (Live / Life / Shop / Work), search,
   account.
2. Section bar: Discover, What's on, Watch, Listen, Boards, Needs doing,
   What things cost, People.
3. Status strip: edition, freshness, counts.
4. `<main>`: the route's own screen.
5. Persistent player: one mini player across pages. This is where
   narration of every article and every media item plays.
6. Footer: the territories, colophon, account.

## Tracks, in order

### 1. One shell

Make the estate shell in `page_core_rendering.h` match the Stitch shell
above. Then move each of the 27 document emitters onto it so they emit
`<main>` only. Start with the families visitors reach first: the apex
fronts, then journal/profile, then account and workspace. Each move
deletes that file's own head, header, footer and page style.

Done when one grep finds a single doctype emitter for HTML routes.

### 2. One screen per route

Replace the pattern column with an exact screen: each of the 95 routes
(plus `/blog/`, `/blog/:slug`, profile, onboarding) gets one Stitch
screen, picked by looking at `screen.png` and `code.html`. Write it as a
ledger: `route-screens.tsv` (route, screen, archive, owner file, status).
Then rebuild each route's body to that screen with real records, and
remove the old body and any appended sections at the same time.

First family: editorial. `aurekai_editorial_discover_hub` becomes the
journal index and the Work front. `aurekai_living_feeds_distraction_free_zen_reading_mode`
becomes the article page.

Done per route when a desktop and a 390px screenshot sit next to
`screen.png` and match its layout and controls.

### 3. Zero to hero

Lay out the path a person walks and make every step a link the shell or
the screen already carries:

arrival (Discover) → read / watch / listen → follow a source or person →
sign in → own profile (`aurekai_member_profile_living_feeds_agent_hub`) →
choose tools (`aurekai_tool_packs_workspace_setup`) → workspace and
studio → publish (journal, atelier CMS) → their own page served at their
address.

Each step names its screen, its owner and the record it writes.

### 4. Narration everywhere

Narration plays through the persistent player. Every article and media
page offers it; the Listen screen (`aurekai_listen`) is its queue.
Existing article narration (`article-narration.test.mjs`) is the starting
owner.

### 5. One post model and live

- Retire the `aurekai-journal` duplicates (archive, not delete).
- Fold `community_post` and `wiki_page` reading into the same article
  screen, so a post is one thing however it was written.
- Release the current tree and promote the 90 posts. Both need Nick's yes.

## New since the 91

`~/Downloads/stitch_aurekai_item_detail_page.zip` (2026-11-11): six dating
screens and an editorial `DESIGN.md`. Index them with the 91 before
track 2 reaches people and profile.
