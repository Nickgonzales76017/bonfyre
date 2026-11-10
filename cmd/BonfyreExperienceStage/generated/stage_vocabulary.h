/* Generated from stage-vocabulary.cue. Do not edit. */
#ifndef BONFYRE_EXPERIENCE_STAGE_VOCABULARY_H
#define BONFYRE_EXPERIENCE_STAGE_VOCABULARY_H
#include <stddef.h>
typedef struct { const char *key; const char *terms; } BfStageHint;
typedef struct { const char *contains; const char *const *hints; size_t hint_count; } BfStageNameRule;

static const BfStageHint bf_stage_affordance_hints[] = {
  {"LOCATE", "place map route origin destination directions territory nearby location venue"},
  {"IDENTIFY", "identity person owner account subject party asset profile actor"},
  {"OBSERVE", "observe read status track monitor activity signal detail context history now"},
  {"MEASURE", "measure meter stats analytics value equity comps risk progress score"},
  {"VERIFY", "verify evidence assessment review proof quality inspection"},
  {"PREDICT", "forecast predict estimate arv value risk compare"},
  {"COMPARE", "compare contrast alternative option variant value comps risk"},
  {"SEARCH", "discover search filter catalog find query related universe browse"},
  {"MATCH", "match compare select provider candidate related fit"},
  {"ROUTE", "route delivery trip transit in-transit directions dispatch"},
  {"SCHEDULE", "schedule calendar book reserve appointment upcoming hold"},
  {"QUOTE", "quote price offer bid term"},
  {"PURCHASE", "cart checkout buy order product purchase"},
  {"PAY", "pay payment settlement invoice receipt"},
  {"INSURE", "insurance coverage claim policy"},
  {"COMMUNICATE", "message conversation discussion reply contact communication thread"},
  {"TRANSLATE", "translate language transcript captions"},
  {"PUBLISH", "publish post syndicate distribution share article release"},
  {"STREAM", "stream live play watch listen media"},
  {"AUTHENTICATE", "identity authenticate session login credential"},
  {"AUTHORIZE", "authorize consent approval permission authority"},
  {"SIGN", "sign signature mandate agreement"},
  {"CERTIFY", "certificate credential verify compliance"},
  {"FILE", "file filing form submission permit tax"},
  {"REPORT", "report alert notification analytics status"},
  {"ALERT", "alert emergency warning incident"},
  {"SIMULATE", "simulate model plan forecast"},
  {"OPTIMIZE", "optimize plan route schedule rank"},
  {"REPAIR", "repair maintenance service work"},
  {"INSPECT", "inspect inspection review audit evidence"},
  {"CLASSIFY", "classify category topic tag ontology"},
  {"NEGOTIATE", "negotiate proposal quote terms conversation"},
  {"CONTRACT", "contract agreement commitment checkout approval"},
  {"HIRE", "hire candidate job interview offer onboarding"},
  {"TEACH", "learn lesson course module feedback assessment"},
  {"CARE", "care provider appointment plan follow-up service"},
};
#define BF_STAGE_AFFORDANCE_HINT_COUNT (sizeof(bf_stage_affordance_hints)/sizeof(bf_stage_affordance_hints[0]))

static const BfStageHint bf_stage_role_hints[] = {
  {"material_source", "entry hero media detail read source content release track episode thread event place product discovery"},
  {"media_service", "play media watch listen stream chapter transcript captions queue"},
  {"action_adapter", "select reserve book checkout payment quote contact apply submit order confirm"},
  {"interaction_reference", "entry body thread board timeline map workspace detail"},
  {"design_reference", "hero layout preview editorial gallery document"},
  {"authoring_adapter", "draft edit compose preview publish update"},
  {"compiler_toolchain", "plan build test run analysis edit deploy"},
  {"transport_carrier", "request send receive deliver execute stream status"},
  {"publication_carrier", "publish syndicate distribution share post update"},
  {"verification_tool", "verify evidence review audit proof quality"},
  {"external_service_candidate", "search select request execute result status"},
  {"reference_standard", "discover resolve validate execute response"},
};
#define BF_STAGE_ROLE_HINT_COUNT (sizeof(bf_stage_role_hints)/sizeof(bf_stage_role_hints[0]))

static const char *bf_stage_presentation_terms = "entry hero media detail read sequence discussion community related collection identity profile article source release track episode place product preview creator people";

static const char *const bf_stage_name_rule_0_hints[] = {
  "TRANSLATE",
};
static const char *const bf_stage_name_rule_1_hints[] = {
  "STREAM",
};
static const char *const bf_stage_name_rule_2_hints[] = {
  "QUOTE",
  "PURCHASE",
};
static const char *const bf_stage_name_rule_3_hints[] = {
  "PAY",
};
static const char *const bf_stage_name_rule_4_hints[] = {
  "PUBLISH",
};
static const char *const bf_stage_name_rule_5_hints[] = {
  "AUTHENTICATE",
};
static const char *const bf_stage_name_rule_6_hints[] = {
  "OPTIMIZE",
};
static const BfStageNameRule bf_stage_name_rules[] = {
  {"transcrib caption subtitle", bf_stage_name_rule_0_hints, sizeof(bf_stage_name_rule_0_hints)/sizeof(bf_stage_name_rule_0_hints[0])},
  {"clip video audio media stream", bf_stage_name_rule_1_hints, sizeof(bf_stage_name_rule_1_hints)/sizeof(bf_stage_name_rule_1_hints[0])},
  {"offer commerce shop", bf_stage_name_rule_2_hints, sizeof(bf_stage_name_rule_2_hints)/sizeof(bf_stage_name_rule_2_hints[0])},
  {"finance pay ledger settle", bf_stage_name_rule_3_hints, sizeof(bf_stage_name_rule_3_hints)/sizeof(bf_stage_name_rule_3_hints[0])},
  {"publish cms distribute repurpose", bf_stage_name_rule_4_hints, sizeof(bf_stage_name_rule_4_hints)/sizeof(bf_stage_name_rule_4_hints[0])},
  {"auth identity", bf_stage_name_rule_5_hints, sizeof(bf_stage_name_rule_5_hints)/sizeof(bf_stage_name_rule_5_hints[0])},
  {"project queue workflow pipeline", bf_stage_name_rule_6_hints, sizeof(bf_stage_name_rule_6_hints)/sizeof(bf_stage_name_rule_6_hints[0])},
};
#define BF_STAGE_NAME_RULE_COUNT (sizeof(bf_stage_name_rules)/sizeof(bf_stage_name_rules[0]))
#endif
