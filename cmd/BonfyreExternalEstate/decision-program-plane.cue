decisionProgramPlane: {
  schema: "bonfyre-decision-program-plane.v3"
  default_provider: "local"
  default_request_provider: "clef-flash"
  local_request_timeout_ms: 240000
  agent: "Bonfyre-DecisionProgramPlane/1"
  max_batch_size: 1
  max_execute_sources: 21
  request_timeout_ms: 45000
  retry_statuses: [429, 500, 502, 503, 504]
  retry_attempts: 3
  lane_probability_floor: 0.75
  exact_review_floor: 0.35
  boundary: "This is an observed admitted source descriptor. It is not a claim of authority, correctness, availability, rights, payment, or a command to publish."
  programs: {
    read: "A current reader-facing written program is the clearest fit."
    listen: "A current audio or podcast program is the clearest fit."
    watch: "A current video program is the clearest fit."
    events: "A current calendar or event program is the clearest fit."
    images: "A current image-led program is the clearest fit."
    review: "The descriptor is insufficient or ambiguous; do not select a program."
  }
  attention: [
    "defer: an admitted descriptor with no retained material or no clear present program fit",
    "inspect: retained material suggests a possible program fit that needs an operator check",
    "prioritize: retained material and descriptor make a current program review especially useful",
  ]
  questions: {
    program: "Which declared public program, if any, is the clearest diagnostic review lane for `source`? Choose review unless the descriptor and retained-material state support one lane. This is not permission to publish or alter the source."
    attention: "How soon should an operator inspect the program fit of `source`? Judge only the stated descriptor and retained-material state; unknown material must not be treated as absent."
    needs_exact_review: "Does the descriptor leave an ambiguity that requires an exact owner or operator review before any program change?"
  }
  question_specs: [
    {id: "program", type: "choice", criteria_field: "programs"},
    {id: "attention", type: "score", criteria_field: "attention"},
    {id: "needs_exact_review", type: "noul", criteria_field: "exact_review_criteria"},
  ]
  exact_review_criteria: {
    "true": "Any ambiguity, unknown material state, rights, or currentness needs exact review."
    "false": "The descriptor itself is enough for a diagnostic review label only, not for an effect."
  }
  planned_cannot_infer: "a plan, source descriptor, or decision-model question does not alter admission, public publication, authority, rights, evidence, payment, or effects"
  diagnostic_cannot_infer: "typed diagnostic output is not source truth, admission, publication, authority, payment, or an effect"
}
