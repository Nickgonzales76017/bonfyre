/* Generated from schemas/control/native-surface-programs.cue. Do not edit. */
#ifndef BONFYRE_NATIVE_TEMPLATE_SLOTS_H
#define BONFYRE_NATIVE_TEMPLATE_SLOTS_H

typedef enum { BF_TEMPLATE_RESOLVED_IDENTITY, BF_TEMPLATE_AUTHORED_NAVIGATION, BF_TEMPLATE_MATRIX_CONTEXT } BfTemplateOpcode;
typedef struct { const char *template_name; unsigned short slot; BfTemplateOpcode opcode; } BfNativeTemplateSlot;
static const BfNativeTemplateSlot bf_native_template_slots[] = {
  {"home", 0, BF_TEMPLATE_RESOLVED_IDENTITY},
  {"home", 1, BF_TEMPLATE_AUTHORED_NAVIGATION},
  {"matrix_index", 0, BF_TEMPLATE_MATRIX_CONTEXT},
};
#define BF_NATIVE_TEMPLATE_SLOT_COUNT (sizeof(bf_native_template_slots) / sizeof(bf_native_template_slots[0]))
#endif
