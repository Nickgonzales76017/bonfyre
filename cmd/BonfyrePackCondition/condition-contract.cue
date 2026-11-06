package packcondition

#ConditionState: "satisfied" | "unsatisfied"

#ConditionAssertion: {
  schema: "bonfyre-pack-condition-assertion.v1"
  condition_ref: string & =~"^[a-z0-9][a-z0-9_-]*$"
  subject: string & =~"^[a-z0-9][a-z0-9_-]*$"
  scope: string & =~"^[a-z0-9][a-z0-9_-]*$"
  state: #ConditionState
  generation: int & >=1
  basis: string & !=""
}

#ConditionResolution: {
  schema: "bonfyre-pack-condition-resolution.v1"
  condition_ref: string
  subject: string
  scope: string
  state: #ConditionState | "unknown"
  generation: int & >=0
  basis: string
  truth: "condition_truth_only_no_authority_or_observation"
}

#ConditionDelta: {
  schema: "bonfyre-pack-condition-delta.v1"
  condition_ref: string & =~"^[a-z0-9][a-z0-9_-]*$"
  subject: string & =~"^[a-z0-9][a-z0-9_-]*$"
  scope: string & =~"^[a-z0-9][a-z0-9_-]*$"
  state: #ConditionState
  generation: int & >=1
  basis: string & !=""
  truth: "accepted_condition_change_notification_not_authority"
}

#CannotInfer: ["condition satisfied -> authority", "condition satisfied -> observation", "condition satisfied -> pack installed", "condition satisfied -> body retained"]
