# fputs-to-components migration, wave 1 (2026-11-11)

## Direction

Surfaces compose ui:* data through shared builders and the one native
renderer; control markup never appears in C. Component styles live in
exactly one place (authored-pattern-styles.mjs → authoring-reader.css);
JSON carriers use bf_jsonw, never hand printing.

## Wave 1: dating + profile surfaces (done)

- New vocabulary: ui:choice (radio-pill group), ui:field (text, search,
  multiline, required, inputmode), ui:action methods (post/get) and
  child containment; suite at 46 with search-admitted/file-refused
  regression checks.
- form_tree.h: ft_card/ft_badge/ft_action/ft_choice/ft_field/ft_render
  builders; all payloads validated by safe_material (safe_type now
  admits every safe HTML input type; file/image stay refused).
- Converted: companions rows/filters/theme picker, clubs, events,
  notices, attention, plans form, report form, dateplan history,
  profile quiz + theme picker. Dead cp-*/bf-program-* CSS deleted;
  duplicate esc/jes escapers deleted.
- Reader CSS linked from companions + profile heads with dark-surface
  overrides; Chrome-verified pixels on all three pages.
- fputs-family call sites in webAddress: 6482 → 6293. The remaining
  ~6.3k are other surfaces, same pattern, later waves.

## Deliberately not done

- `if` statements are not migration targets: dispatch ladders already
  collapse into tables where they fit (command dispatch), but guards,
  bounds checks and error paths are required for memory-safe C.
  Removing them would trade a style complaint for vulnerabilities.
- Hand-printed JSON outside the dating carriers stays until its wave;
  nothing new may hand-print (bf_jsonw rule).
