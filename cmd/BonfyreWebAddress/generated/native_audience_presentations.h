/* Generated from audience-presentation.cue. Do not edit. */
#ifndef BONFYRE_NATIVE_AUDIENCE_PRESENTATIONS_H
#define BONFYRE_NATIVE_AUDIENCE_PRESENTATIONS_H
typedef struct { const char *scope; const char *title; const char *description; } BfNativeAudiencePresentation;
static const BfNativeAudiencePresentation bf_native_audience_presentations[] = {
  {"self","Bonfÿre — the workshop","The self-scoped territory of the Bonfÿre matrix: work addressed and receipted before it is published."},
  {"public","Bonfÿre — in the open","The public territory of the Bonfÿre matrix: receipted public-record observation, legible to people and to machines."},
  {"market","Bonfÿre — the market","The market territory of the Bonfÿre matrix: priced capability, quoted from one service book."},
  {"institution","Bonfÿre — the institution","The institutional territory of the Bonfÿre matrix: the work graph, the desk, and what is open for decision."},
};
#define BF_NATIVE_AUDIENCE_PRESENTATION_COUNT (sizeof(bf_native_audience_presentations)/sizeof(bf_native_audience_presentations[0]))
#endif
