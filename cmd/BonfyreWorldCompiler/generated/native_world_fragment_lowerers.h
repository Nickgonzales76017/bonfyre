/* Generated from world-fragment-lowering.cue. Do not edit. */
#ifndef BONFYRE_WORLD_FRAGMENT_LOWERERS_H
#define BONFYRE_WORLD_FRAGMENT_LOWERERS_H
#include <stddef.h>
typedef struct { const char *input_grammar,*surface_grammar,*renderer_family,*renderer_variant,*extent; } BfWorldFragmentLowerer;
static const BfWorldFragmentLowerer bf_world_fragment_lowerers[] = {
  {"discover","discover","vertical","faceted_stream","page"},
  {"stage","stage","stage","calendar_timeline","page"},
  {"channels","channels","thread","stream_topics","section"},
  {"community","community","community","activity_body","section"},
  {"board","board","board","masonry_collection","page"},
  {"spatial","spatial","spatial","map_layers","page"},
  {"listings","listings","market","availability_compare","page"},
  {"search","discover","search","query_results","page"},
  {"inbox","inbox","work","obligation_timeline","workspace"},
  {"territory","territory","inspector","address_inspector","page"},
  {"observations","observations","thread","observation_timeline","section"},
  {"worlds","worlds","board","world_shelf","page"},
  {"packs","packs","work","dependency_kanban","workspace"},
  {"link","link","inspector","relationship_inspector","page"},
  {"studio","studio","work","composition_kanban","workspace"},
  {"console","console","inspector","observatory","workspace"},
  {"vertical","discover","vertical","faceted_stream","page"},
  {"thread","channels","thread","stream_topics","section"},
  {"market","listings","market","availability_compare","page"},
  {"work","inbox","work","obligation_timeline","workspace"},
  {"inspector","territory","inspector","address_inspector","page"},
};
#define BF_WORLD_FRAGMENT_LOWERER_COUNT (sizeof(bf_world_fragment_lowerers)/sizeof(bf_world_fragment_lowerers[0]))
#endif
