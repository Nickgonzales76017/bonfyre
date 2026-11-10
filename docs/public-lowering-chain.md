# Public lowering chain (as deployed 2026-09-17)

How a human request becomes a page on the four territory hosts, traced through
`cmd/BonfyreWebAddress/src/main.c`. Each line is `address → resolution → owner
reads → selection → emitted HTML → actions`. The last column names where richer
owner meaning is flattened today; those are the Pass 1 migration targets.

| Address | Resolution | Owner reads | Presentation selection | Emitted | Actions | Flattening today |
|---|---|---|---|---|---|---|
| `aurekai.{live,life,shop,work}/` | territory apex (`page_territory_apex`) ahead of `serve_site_program` | `internet_source` + retained twins per section program | `nativePagePrograms` `/` sections → each section's first region | `world.html` head + `page_emit_section` previews | links, search form, sign in | retained items become `PageItem`; no owner beyond retained material |
| `/discover` `/watch` `/channels` `/community` `/board` `/parcels` `/releases` `/advisories` `/research` | `emit_page_program_route` inside `serve_native_donor_route` | families → sources (`page_program_sources`) → `page_collect_retained` (census reader) | CUE regions (feed, grid, panes, communities, wall, map, table, index) | `page_emit_document` | item links, `?from=`, `?s=`, search, sign in | all material is `PageItem`; SurfaceScene/SurfaceRuntime not in the path |
| `/search?q=` | page program | families + observed Tarrant roll (`fleming_query`) when `q` has a digit | results + Properties | `page_emit_document` | open item / property | retained items flattened; properties are real owner rows linked, not flattened |
| `/o/<source>/<key>` | `emit_item_page` | re-reads that source's retained body; `saved_item` for the signed-in keeper | verbs by item kind | item page | play (PeerTube embed), view, map, read at, share, Save/Unsave, more from | the item has no canonical subject; it is recomputed from the feed |
| `/s/parcel/<account>` (HTML) | `emit_parcel_page` before `serve_subject` | observed roll (`observation`, `src:tad-parcel-view`); `saved_item` | facts + same-street ledger | property page | See this market; Save; Watch/Open opportunity with write scope; share | none: facts are read from the owner each open |
| `/s/parcel/<account>` (JSON, HX) and other `/s/<kind>/` | `serve_subject` | kind owner | aperture list (`aperture_push`) | legacy subject door | aperture verbs | legacy chrome, not composed |
| `/saved` | `emit_saved_page` | `saved_item` for the session actor | feed | `page_emit_document` (prefilled) | open, unsave on item page | snapshots are `PageItem`s by design (private description only) |
| `/w/<world>` | `emit_native_world_page` | `world`, `world_fragment`, sources, community/media/recording readers | `nativePageWorldRegions` | `page_emit_document` | item links | retained + subject readers flattened to `PageItem` |
| `/w/<world>/<grammar>`, `/stage`, `/listings`, `/inbox`, `/territory`, `/packs`, `/link`, `/console`, `/studio` | `serve_native_donor_route` → `emit_native_donor_surface` | `night_subject` (synthetic), publications, capability programs | CUE surface programs (slots/native) | donor or native document + `aurekai-native.css` | links | synthetic `night_subject` still lowered on `/stage`; owners with no rows show empty states |
| `/login` | authored page → `/_aperture/form?...Session.open` → POST `/login` | BonfyreAuth | ObjectGrammar AtomicForm | form | sign in; browser form returns to validated `next` | none |
| `POST /o/save`, `/o/unsave` | `serve_page_save` | BonfyreAuth verify; retained source or roll; CMS binary write | — | 303 back | — | none |

## Where the mature stack is not yet in the path

- **SurfaceScene / SurfaceRuntime** (`cmd/BonfyreSurfaceScene`) compile and run,
  but no public route reads a compiled scene. Routes are emitted by
  `page_emit_document` straight from CUE page programs.
- **ObjectGrammar apertures** govern sign-in and the legacy subject doors; Save
  is a native owner handler and not yet an aperture contract.
- **Event / Place / Offer owners** have no production rows (`night_event` and
  `night_subject` are synthetic; `spatial_observation` is unregistered;
  `service_offer` is empty), so no route can yet show an owner-backed event,
  place or offer.
