# Addressed depth: every reference opens a composed page

2026-10-18. Scope: every detail page and every reference to a person, organization, publisher, source, channel, item, owner, foreign observation or fixture, on the live apexes, the 1,715 subject hosts and the demo applications. A label opens a page about the thing. That page is composed by the estate's own scene compiler and dressed by its presentation system, not hand-written.

## What was measured

| Finding | Evidence |
|---|---|
| Item pages are thin | 920 item pages crawled on the apexes, 54 words on average, every one under 120 |
| The reader drops what the estate retained | 19,666 of 41,213 matched feed items (48%) hold more than 200 characters in the retained body and none on the page; `main.c` skips `<description>` for publications and cuts the rest at 600 characters |
| A publisher is shown as a hostname | the SIB item is credited to `sib.swiss`; frontier holds the institution, its Wikidata identity, official site and channels |
| Publisher addresses show other publishers | `sib-swiss.aurekai.life` opens on ODVA, Jidai Matsuri and a Valencia half marathon |
| Why | the live service environment omits `BONFYRE_SUBJECT_FABRIC_DB`, `BONFYRE_ENTITY_DB`, `BONFYRE_ITEM_ARCHIVE_DB` and nine others, so every depth section renders empty and publisher hosts fall back to the generic LIVE world |
| 637 publisher subjects were never compiled | 1,703 publisher subjects have readers; 1,066 have material |
| The shared link opener links almost nothing internal | `page_emit_item_open` links an item page, `/s/` paths and outbound URLs; every other estate path is a span |
| The scene compiler cannot see relations | atlas `scene-compiler` known gap: it counts material, not relationships |
| Demo references have no destination | `foreign:houston-35` in the ITAD desk and Brief Studio is plain text (browser audit 2026-10-18) |

## Owners this lands in

- **Composition:** `schemas/control/page-composition.cue` (regions, materials, scene grammars), lowered by `tools/compile_page_composition.zig`, executed by `page_compile_scene` and `page_emit_regions`.
- **Look:** `page_emit_profile_for` over the Ghost-catalogue profiles, `site_presentation` (1,708 hosts), 1,394 presentation templates, 238 design fingerprints, presentation cells and template slots.
- **Addresses:** `matrix_domains` (1,715 hosts), URL grammars for 26 subject kinds (`cmd/BonfyreExperienceMap/data/url_grammars.tsv`), canonical subject address `/s/<kind>/<id>`, item `/o/<source>/<key>`.
- **Material:** retained bodies (`foreign/bodies`), item archive, source frontier (29,261 roots, 24,806 with Wikidata identity), subject fabric, entity store.
- **Demo receivers:** ServiceSite builds and pack records (`source_ref`, `receipt_ref`, owner fields).

## Work, in order

1. **Wiring.** The live service reads every store its pages need. Stage the environment on a canary, compare routes, then cut over.
2. **Projection completeness.** Compile the 637 publisher subjects with no material through `compile_subject_projection.mjs`; a host with readers and no material is a defect, not a thin page.
3. **One resolver for references.** Any reference becomes the canonical estate URL: a publisher root or host to its subject host, a Wikidata identity to the subject that holds it, a source or channel to its publisher's address, an item to `/o/`, a `foreign:` observation to its source profile, a `fixture:` to a visible sample page. Used by `page_emit_item_open` and every region, by ServiceSite builds and by demo bindings. An unbound reference stays visibly unresolved.
4. **Relations as material.** The scene compiler gains relation materials (the parts of a piece, the organizations it names, a publisher's channels, then people, places, events and topics). Each has scene grammars with thresholds and donor rhythms, so a page rich in relations composes richly.
5. **Every detail family composes.** Item pages, publisher host fronts, institution pages, `/about/` identities, source and channel pages and subject rooms run collect, then compile, then dress through the profile, then emit regions. No family keeps a hand-written body.
6. **Reader depth at intake.** The census keeps each piece's lead, parts and linked names when it reads them (no per-request reparse), and keeps a mention index so an organization's page lists the pieces that name it.
7. **Demos bind to the live estate.** Pack and fixture references in the demo applications resolve through the same resolver to live addresses or visible sample pages.
8. **Witness.** The reference census runs before and after on the apexes, a host sample and the demo builds: words per page, linked versus plain references, thin pages and long internal strings. Browser checks at desktop and phone width are compared against neighbouring estate pages.

## Boundaries

A publisher's syndicated lead may be shown; the rest of a piece shows section headings and an opening line, and the original is one link away. A reference resolves only to an address the estate already answers for; nothing invents a profile. Presentation never changes which material a page holds.
