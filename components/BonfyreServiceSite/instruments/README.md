# Working instruments

Four implementations under the existing ServiceSite owner: observed process traces, source-anchored research authoring, a dependency-aware product story map, and actual SQLite schema inspection/migration. These are an initial implementation cut, not completion of the 388-skill conversion.

`traces.mjs` calculates empirical case spans, interval-union coverage, overlapping work and per-case percentiles from imported timestamps. It does not simulate outcomes or infer that every unrecorded gap is waiting. The initial case is a retained public GitHub deployment; timestamps and links are recorded in `examples.mjs`.

`research.mjs` retains exact selected imported-text spans and SHA-256 identity. The analyst supplies findings and inclusion decisions. Synthesis organizes those authored findings with their supporting/challenging/context passages and bibliography. It does not fabricate research claims. Europe PMC search is an explicit, bounded public read, not a background harvester or a claim of completed literature review.

`product.mjs` validates dependencies and release capacities and computes an inspectable RICE-first allocation proposal. Estimates are not measurements or a delivery forecast. Research handoff uses the existing browser store's atomic multi-record save, preserving exact source context and refusing duplicate candidate insertion.

`schema.mjs` generates four bounded schema operations; `sqlite-worker.js` executes them in SQLite WASM on a disposable in-memory copy. Integrity and foreign keys are checked before the user can adopt the result. Export is a real SQLite database, independently reopened by native Node SQLite in browser tests. No external or production database is touched.

The entry point calls the existing SurfaceScene `WorkspaceSession` and `createEditHistory`. Persistence is the existing public Spaces IndexedDB store with revision comparisons. Saves are explicitly browser-local; no native account binding or authority is implied. The build checks the pinned shared owner hashes and copies only the required existing implementations. sql.js 1.13.0 is a pinned build dependency, accompanied by its license and emitted hashes. No model or skill-prompt runtime is used.

## Connected working editors

The existing entry point now mounts a second instrument alongside the first. Each mounted instrument keeps its own domain controller, inspector, selection, dirty state, history and teardown. The original single-instrument routes use this same host, not an alternate demo implementation. Heavy workers are created only when their instrument is opened.

Offering a research opportunity retains the source and candidate through the existing atomic batch operation, then opens the actual product editor with the candidate ready for inspection. Adoption checks the retained source text and exact passages; it does not invent reach, confidence or availability. The current map keeps its existing stories. A receiving-view failure does not replay an already completed source save.

`working-record.mjs` coordinates saves through the existing store. Save connected work checks both record revisions in one transaction. Concurrent edits made while saving stay dirty. Exact continuation URLs name each saved record and revision; missing or stale requested records are not replaced by examples. No historical record-version store, grant, account migration or new database is introduced.

This closes a browser-local receiving interaction, not the entire complete-production contract. Native destination adoption, the separate Mac ServiceSite instrument-store/composer repair, arbitrary product authoring, maintained source correction across every receiver, and the remaining skill/API/platform work still require implementation and release checks. The GitHub runtime remains pinned to its reviewed public release rather than silently replacing it with different Mac bytes.

Build and verification:

    node --test scripts/tests/instruments-core.test.mjs scripts/tests/instruments-working-record.test.mjs
    npm install --prefix .spaces-tools --ignore-scripts --no-audit --no-fund playwright@1.56.1 sql.js@1.13.0
    INSTRUMENT_REQUIRE_SQLITE=1 node scripts/build-instruments.mjs
    PLAYWRIGHT_MODULE="$PWD/.spaces-tools/node_modules/playwright/index.mjs" CHROME_PATH="$(command -v google-chrome)" node scripts/tests/instruments-browser.mjs

The original browser journey remains intact. The composition journey adds unfamiliar research-to-map work, independent edits, exact reload, real two-tab revision conflict, and schema/trace companions. Tests use actual DOM interaction, IndexedDB, workers and SQLite execution. Screenshots and assertions remain run artifacts, not stored business data. Current external API access and native account continuation remain separate verification work.
