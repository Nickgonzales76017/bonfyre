# S2Vec In Bonfyre

S2Vec is integrated as a metadata-first Bonfyre import pack, separate from the Recursive DisCIPL runtime upgrade.

## Why It Matters

S2Vec is useful as a future domain-expansion primitive for:

- city-level reasoning
- spatial memory
- urban planning loops
- environmental scoring
- graph-to-map flow planning
- non-AI civic and economic workflows

## Bonfyre Shape

- recipe: `recipes/community/geospatial/s2vec_geospatial_embeddings.yaml`
- metadata: `docs/metadata_s2vec.json`
- source model reference: `google/s2vec`
- import mode: metadata-first, no network or weights required at runtime

## Key Family Contracts

- `T_S2_CELL -> T_RASTER_FEATURE` as `rasterizes_to`
- `T_BUILT_ENVIRONMENT -> T_RASTER_FEATURE` as `counted_into`
- `T_RASTER_FEATURE -> T_MASKED_AUTOENCODER` as `encoded_by`
- `T_MASKED_AUTOENCODER -> T_GEOSPATIAL_EMBED` as `emits_embedding`
- `T_GEOSPATIAL_EMBED -> T_SOCIOECONOMIC_HEAD` as `predicts_socioeconomic`
- `T_GEOSPATIAL_EMBED -> T_ENVIRONMENTAL_HEAD` as `predicts_environmental`
- `T_GEOSPATIAL_EMBED -> T_MULTIMODAL_FUSION` as `fuses_with_imagery`

## DisCIPL And S2Vec

Recursive DisCIPL and S2Vec are intentionally separate:

- DisCIPL provides actor, contract, chain, and loop substrate for Bonfyre OS planning.
- S2Vec provides a geospatial embedding domain pack that can later participate in DisCIPL chains and civic/economic loops.

## Sanity Commands

```bash
make -C lib/libbonfyre clean all && make -C cmd/BonfyreCLI clean all

cmd/BonfyreCLI/bonfyre discipl init --root layeros/state
cmd/BonfyreCLI/bonfyre discipl actors import --root layeros/state
cmd/BonfyreCLI/bonfyre discipl contracts import --root layeros/state

cmd/BonfyreCLI/bonfyre discipl chain-plan --root layeros/state \
  T_AUDIO_MODEL T_AUDIO_GENERATOR T_SAMPLE_OUTPUT T_LATENT_SPACE T_DIFFUSION_UNET T_VIDEO_OUTPUT

cmd/BonfyreCLI/bonfyre layer import-recipe recipes/community/geospatial/s2vec_geospatial_embeddings.yaml --root layeros/state
cmd/BonfyreCLI/bonfyre index layers --root layeros/state
cmd/BonfyreCLI/bonfyre query layers --family T_GEOSPATIAL_EMBED --root layeros/state
```
