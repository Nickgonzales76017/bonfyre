# Checking a Stitch design against Aurekai

A procedure, not a program. Stitch is how designs arrive today; the workflow will
change, so nothing here is built into Aurekai.

Each design folder in the Downloads zips holds `code.html` and `screen.png`.
`screen.png` is the design as drawn, so a check never needs to render `code.html`.

1. Find the design's routes and owner in `COVERAGE.tsv` (`existing_entry_routes`,
   `native_owner_candidate`).
2. Unzip the archive named in `archive_sources` into a scratch folder:
   `unzip -o -q ~/Downloads/'stitch_aurekai_item_detail_page (2).zip' -d "$SCRATCH"`
3. Start the preview of the current tree: `make -C cmd/BonfyreWebAddress preview`.
   For concurrent isolated lanes use a distinct port and home, e.g.
   `BONFYRE_PREVIEW_PORT=28921 BONFYRE_PREVIEW_HOME="${TMPDIR:-/tmp}/stitch-preview-identity" make -C cmd/BonfyreWebAddress preview`.
   The variable is `BONFYRE_PREVIEW_PORT`, **not** `BONFYRE_PREVIEW_28921`.
   Give the agent read access to the design files before asking it to compare
   images, and use the lane's actual port in the browser resolver below.
4. Screenshot the route at the design's width (read it with `file screen.png`):

   ```
   "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" --headless=new \
     --user-data-dir="$SCRATCH/chrome" --no-first-run --hide-scrollbars \
     --window-size=507,1600 --virtual-time-budget=10000 \
     --host-resolver-rules="MAP aurekai.work:80 127.0.0.1:28911" \
     --screenshot="$SCRATCH/route.png" http://aurekai.work/organisations
   ```
   Wrap it in `perl -e 'alarm 40; exec @ARGV' …`; headless Chrome can hang.
5. Look at `screen.png` and `route.png` side by side. Use the design's example
   people, prices and metrics as layout only; the page shows real owner data.
6. Record what you saw in the design's `COVERAGE.tsv` row (`control_parity`,
   `source_record_parity`, `browser_acceptance`). After a release, repeat step 4
   against the live host (https, no resolver rule) for `live_release_readback`.

Example, 2026-11-05: `aurekai_organization_profile_workspaces_2` draws a member and
org profile with feeds, circles, nested workspaces and collaborators. `/organisations`
served a plain publisher list, and names showed raw escapes (`™`, `–`).
