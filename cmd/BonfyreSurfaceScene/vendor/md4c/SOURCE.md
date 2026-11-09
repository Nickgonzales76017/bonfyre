# Native MD4C — pinned original source (MIT)

Upstream: https://github.com/mity/md4c
Imported: 2026-11-09; source blobs below are immutable Git identities.
- md4c.h: `796fd677961872143b29594a0e21fd3d5f6564d8`
- md4c.c: `73a34be4d6e79167c304fce41c932c70fa4dd46c`
- md4c-html.h: `9cd93afe38baf7e8d9d3294aa5f1c662488d4ac6`
- md4c-html.c: `d1e1c7cbb1fbd10e1083e1071708ee55170db1b3`
- entity.h: `2ee99c8f1e54a0672cca0a9413c584892986d6cf`
- entity.c: `c048db6caf9a1875d53985403577287913dee202`
- LICENSE.md: `3baef6a38a10ed88ca55bb149907296ab7ff3462`

C renderer uses NOHTML and bounded source input. This is a vendored native parser, **not** a new HTML-authority or publication owner. Its render result must pass URL and unsafe-content checks before a draft preview can be emitted. No network access at runtime.
