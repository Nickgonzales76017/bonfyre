Upstream: https://github.com/mity/md4c
License: MIT
Pin: f9e5eb232b95b737373ae706648cb75bd259ed1a (verified = upstream HEAD on 2026-11-10)
Do not treat declaration-only records as an installed parser.

## Pin review 2026-11-10

Vendored blob identities (git hash-object):
- md4c.c: 73a34be4d6e79167c304fce41c932c70fa4dd46c (matches upstream)
- md4c.h: b6f28153a9cb6e3b746e0d691f4263177c4dfdd (whitespace-only delta, see below)
- md4c-html.c: d1e1c7cbb1fbd10e1083e1071708ee55170db1b3 (matches upstream)
- md4c-html.h: 9cd93afe38baf7e8d9d3294aa5f1c662488d4ac6 (matches upstream)
- entity.c: c72b81f8b06253f8cfebae4e36395c343de11a07 (whitespace-only delta, see below)
- entity.h: 2ee99c8f1e54a0672cca0a9413c584892986d6cf (matches upstream)

Deltas vs upstream at the pin (both trailing-whitespace-only, zero functional
change; likely editor-trimmed during vendoring):
- md4c.h:223 comment `&nbsp;` vs upstream `&nbsp; ` (one trailing space)
- entity.c:27 blank line empty vs upstream two spaces
