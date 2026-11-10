/* Generated from boundary-contract.cue. Do not edit. */
#ifndef BONFYRE_OBSERVATION_BOUNDARY_CONTRACT_H
#define BONFYRE_OBSERVATION_BOUNDARY_CONTRACT_H
#include <stddef.h>
typedef struct { const char *name; const char *policy_decision; int require_fetch; int require_transport_success; int require_http_success; int require_body; int require_content_hash; } BfObservationState;
static const char *const bf_observation_boundary_schema = "bonfyre.observation-boundary.v1";
static const char *const bf_observation_receipt_schema = "bonfyre-foreign-observation.v1";
static const BfObservationState bf_observation_states[] = {
  {"policy_excluded", "robots_disallow", 0, 0, 0, 0, 0},
  {"policy_indeterminate", "robots_preflight_unavailable", 0, 0, 0, 0, 0},
  {"source_observed", "allow", 1, 1, 1, 1, 1},
};
#define BF_OBSERVATION_STATE_COUNT (sizeof(bf_observation_states)/sizeof(bf_observation_states[0]))
static const char *const bf_observation_cannot_infer[] = {
  "documented public feed -> lawful observation by this agent",
  "public observe_right -> robots or terms permission",
  "successful transport -> source authority",
  "content hash -> retained body availability",
};
#define BF_OBSERVATION_CANNOT_INFER_COUNT (sizeof(bf_observation_cannot_infer)/sizeof(bf_observation_cannot_infer[0]))
#endif
