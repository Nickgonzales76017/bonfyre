/* Generated from schemas/control/native-surface-programs.cue. Do not edit. */
#ifndef BONFYRE_NATIVE_MACHINE_SURFACES_H
#define BONFYRE_NATIVE_MACHINE_SURFACES_H

typedef struct { const char *name; const char *schema; } BfNativeMachineSurface;
static const BfNativeMachineSurface bf_native_machine_surfaces[] = {
  {"api", "bonfyre-api-v1"},
  {"mcp", "bonfyre-mcp-resources-v1"},
  {"a2a", "bonfyre-a2a-resources-v1"},
};
#define BF_NATIVE_MACHINE_SURFACE_COUNT (sizeof(bf_native_machine_surfaces) / sizeof(bf_native_machine_surfaces[0]))
#endif
