# Native model verification evidence

`benchmark.json` records five alternating runs of the old and rebuilt model verifier on 128 MiB plus 17 synthetic bytes. Median wall time falls from 0.718 to 0.127 seconds. All output digests match independent SHA-256, and source bytes remain unchanged. No model download or inference occurred.

`old-file-verification.log` retains the old binary accepting a directory as an empty file. `unknown-hash-before-after.json` shows the old NULL-hash failure and the corrected installed behavior. `model-tests.log` and `installed-file-verification.log` record manifest, representation and nine file-verification checks. Database uniqueness failure is included.

`installation.json` retains binary hashes and the rollback path; `mcp-readback.json` independently hashes the installed binary through native MCP. `atlas.log`, `experience-map.log` and `corpus-installation.json` record registry validation and installation. `verification.json` pins changed sources and records remaining disk space.
