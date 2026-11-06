# Bonfÿre Pages runtime

Pages is a public `SurfaceIR` projection, not a second source of truth. It may
present products, recipes, evidence, and operator paths; it may not invent
capability maturity, authorize effects, or turn a benchmark into a universal
claim.

The live Pages sources are:

- `site/index.html` — concise whole-machine front door
- `site/explore.html` — product paths and institutional use
- `site/docs.html` — technical map and runnable checks
- `site/runtime/` — bounded client-side runtime helpers
- `.github/workflows/pages-runtime.yml` — reusable build/process/deploy workflow

## Projection contract

Pages copy should derive from the same owners as the rest of Bonfÿre:

```text
Atlas maturity + live command evidence + source-backed measurements
                              │
                              ▼
                    Pages / SurfaceIR view
                              │
                    action trace and links
                              │
                              ▼
             Authority and EffectKernel still decide effects
```

Required fences:

- Distinguish the frozen Generation-10 target from live implementation
  maturity.
- Present the 91 Powers as identities with maturity, not 91 guaranteed callable
  tools.
- Preserve the nine Frappe lineages, Model Commons, agentic execution, Partner
  Commons, evidence/replay, and operational/financial metabolism.
- Treat Claude, Codex, MCP, A2A, GitHub, and Pages as carriers or surfaces, not
  semantic owners.
- Never imply that reachability, a rendered button, or a generated recipe is
  authority to perform an effect.

## Reusable workflow

`bonfyre-oss` provides `.github/workflows/pages-runtime.yml` to:

1. build the repository on a runner;
2. detect changed `input/**` files;
3. call an app-local processing script;
4. upload generated artifacts;
5. commit authorized outputs when configured; and
6. deploy the selected Pages path.

Inputs:

- `app_name`
- `input_glob`
- `process_script`
- `deploy_path`
- `artifact_name`
- `bonfyre_repo`
- `bonfyre_ref`

The app-local script receives:

- `BONFYRE_APP_NAME`
- `CHANGED_FILES_FILE`
- `DEPLOY_PATH`

It should read changed paths, write replayable artifacts under `artifacts/`, and
write the public projection under `$DEPLOY_PATH`. The workflow adds
`site/job.json` and `site/summary.json` as execution evidence.

## Local verification

Serve the static projection:

```bash
python3 -m http.server 8000 --directory site
```

Then open `http://localhost:8000`, follow every local link, and verify narrow
and wide layouts. Before publishing architecture claims, also run:

```bash
bun architecture/atlas.mjs validate
cd components/BonfyreControlPlane
python3 -m pytest tests/ -q
```

Pages deployment is an external effect. Use repository authority and workflow
policy; a valid local render does not itself authorize publication.
