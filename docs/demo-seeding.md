# Demo Seeding

Bonfyre demo apps should graduate from one-off showcase records to larger seeded datasets that make:

- search feel real
- memory surfaces feel useful
- clustering and compression become visible
- orchestration value obvious from outcomes, not claims

## Rules

- Keep raw copyrighted source material out of the repo unless you have rights.
- Prefer storing derived demo artifacts and compact record summaries over raw media.
- For YouTube or famous-podcast inspired demos, use:
  - public-domain or Creative Commons source where available
  - permissioned material
  - or abstracted/derived seed records instead of reposting raw transcripts/audio

YouTube’s official help says reuse depends on permission or the applicable license, and CC BY videos are a special case rather than a blanket rule:

- License types on YouTube: https://support.google.com/youtube/answer/2797468/creative-commons
- YouTube Terms: https://www.youtube.com/t/terms

## Target

Aim for `100` compact seeded records per flagship demo family.

That does **not** mean hosting 100 raw podcast episodes or 100 full town-hall videos in the repo.

It means hosting 100 normalized demo records that preserve the behavior we want to show:

- topic clustering
- cross-record search
- timeline reuse
- artifact fanout
- orchestration-driven branching

## Generator

Use:

```bash
node scripts/generate_demo_seed_sets.mjs path/to/manifest.json
```

The generator writes `demo-items.json` files for each app from a compact source manifest.

It also passes through provenance fields when they exist:

- `sourceUrl`
- `sourceLinks`
- `sourceTitle`
- `sourceLabel`
- `sourceCopy`
- `publisher`
- `license`

Those fields should point to the public origin page or channel, not to mirrored source media hosted inside the app repo.

## Reviewed-Source Generator

For corpora that need to pass strict provenance, use:

```bash
node scripts/generate_reference_corpora_from_sources.mjs path/to/reviewed-source-manifest.json
```

Start from:

```bash
cp scripts/reviewed_source_manifest.template.json path/to/reviewed-source-manifest.json
```

That manifest is intentionally source-first:

- one reviewed public origin per record
- public URL attached up front
- publisher and license note attached up front
- no mirrored source audio/video in `site/demos`

This is the path that should replace synthetic placeholder corpora for the flagship apps.

## Public Source Queue

Before a source becomes `approved`, keep it in a review queue:

```bash
scripts/public_source_queue.sample.json
```

That queue separates three steps that were previously getting blurred together:

1. find candidate public sources
2. review provenance, license posture, and fit
3. promote only approved sources into a reviewed-source manifest

Convert a reviewed queue into a reviewed-source manifest with:

```bash
node scripts/build_reviewed_manifest_from_queue.mjs path/to/public-source-queue.json path/to/reviewed-source-manifest.json
```

Only sources with:

- `review_status: "approved"`

are promoted.

## Validator

Before publishing a corpus, run:

```bash
node scripts/validate_reference_corpora.mjs --strict-provenance --min-count 100 --min-distinct-sources 10
```

What it catches:

- mirrored audio/video files inside `site/demos`
- corpora below the target size
- missing public provenance links
- local-path provenance masquerading as origin
- corpora that rely on too few distinct public sources
- stale staged-demo copy like `seeded` or `demo dataset`
- excessively repeated record templates that make the corpus feel fake

Use the validator as the release gate:

- synthetic exploration can fail locally
- public demo publication should not pass until provenance is real

## Sample Reviewed Manifest

For local stress testing, there is a starter manifest at:

```bash
scripts/reviewed_source_manifest.sample.json
```

It uses real public-source examples for:

- `pages-town-box`
- `pages-shift-handoff`

Generate into `/tmp` with:

```bash
node scripts/generate_reference_corpora_from_sources.mjs scripts/reviewed_source_manifest.sample.json
```

## Manifest Shape

```json
{
  "default_count": 100,
  "apps": [
    {
      "repo": "pages-podcast-plant",
      "slug": "podcast-plant",
      "title": "Podcast Plant",
      "out_path": "/abs/path/to/pages-podcast-plant/site/demos/podcast-plant/demo-items.json",
      "default_outputs": ["show notes", "clip list", "article draft", "RSS-ready bundle"],
      "default_tags": ["podcast", "clips", "publish-ready"],
      "why_it_matters": "A larger seed set makes episode search and repurposing obvious inside the demo.",
      "search_intro": "Search across many seeded episodes to compare topics, clips, and publish surfaces.",
      "seeds": [
        {
          "file": "City Budget Special",
          "brief": "Episode covering budget pressure, staffing, and service tradeoffs.",
          "tags": ["budget", "city", "policy"],
          "searchSummary": "budget pressure, staffing, service tradeoffs"
        }
      ]
    }
  ]
}
```

## Sourcing Strategy

Use different source modes by demo type:

- `podcast`: permissioned, CC BY, public domain, or derived metadata-only records
- `town halls`: public-government meetings are strong candidates, but still store normalized records rather than raw repo copies when possible
- `customer voice` / `sales`: synthetic or permissioned only
- `legal` / `evidence`: synthetic or sanitized only

## Recommendation

For the next pass, build `100`-record manifests first for:

- `pages-podcast-plant`
- `pages-town-box`
- `pages-customer-voice`
- `pages-oss-cockpit`
- `pages-memory-atlas`

Those are the apps where larger seeded datasets will most clearly demonstrate:

- search
- clustering
- memory
- orchestration-driven fanout
