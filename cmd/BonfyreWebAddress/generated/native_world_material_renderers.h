/* Generated from world-material-renderers.cue. Do not edit. */
#ifndef BONFYRE_NATIVE_WORLD_MATERIAL_RENDERERS_H
#define BONFYRE_NATIVE_WORLD_MATERIAL_RENDERERS_H
typedef enum { BF_WORLD_MATERIAL_FLYER, BF_WORLD_MATERIAL_PIN, BF_WORLD_MATERIAL_CASE, BF_WORLD_MATERIAL_TICK, BF_WORLD_MATERIAL_READING, BF_WORLD_MATERIAL_ARTICLE, BF_WORLD_MATERIAL_ROW } BfWorldMaterialItemVariant;
typedef struct { const char *character; const char *container_class; BfWorldMaterialItemVariant item_variant; } BfWorldMaterialRenderer;
static const BfWorldMaterialRenderer bf_world_material_renderers[] = {
  {"zine","wall",BF_WORLD_MATERIAL_FLYER},
  {"scrapbook","pins",BF_WORLD_MATERIAL_PIN},
  {"dossier","file",BF_WORLD_MATERIAL_CASE},
  {"ticker","tape",BF_WORLD_MATERIAL_TICK},
  {"almanac","read",BF_WORLD_MATERIAL_READING},
  {"journal","cols",BF_WORLD_MATERIAL_ARTICLE},
  {"default","rows",BF_WORLD_MATERIAL_ROW},
};
#define BF_WORLD_MATERIAL_RENDERER_COUNT (sizeof(bf_world_material_renderers)/sizeof(bf_world_material_renderers[0]))
#endif
