package control

// Authored protocol shapes migrated from the Python reference library.
#LibraryEntry: #ProtocolContract & {family: string & != ""}
protocolLibrary: [...#LibraryEntry] & [
  {
    "family": "authority",
    "name": "authority-challenge",
    "keys": [
      "challenge_id"
    ],
    "given": [
      "asserted_act",
      "subject"
    ],
    "evidence_required": [],
    "roles": [
      {
        "name": "challenger",
        "requires": [
          "asserted_act",
          "subject"
        ],
        "produces": [
          "authority_challenge"
        ],
        "authority": "challenge:act"
      },
      {
        "name": "asserter",
        "requires": [
          "authority_challenge"
        ],
        "produces": [
          "authority_showing"
        ],
        "authority": "answer:challenge"
      },
      {
        "name": "adjudicator",
        "requires": [
          "authority_showing"
        ],
        "produces": [
          "verdict"
        ],
        "authority": "adjudicate:authority"
      }
    ],
    "commitments": [
      {
        "key": "show_authority",
        "committer": "asserter",
        "beneficiary": "challenger",
        "resource": "a showing of the authority relied on",
        "discharged_by": "authority_showing"
      }
    ]
  },
  {
    "family": "authority",
    "name": "scoped-delegation",
    "keys": [
      "delegation_id"
    ],
    "given": [
      "expiry",
      "principal_authority",
      "purpose",
      "scope"
    ],
    "evidence_required": [],
    "roles": [
      {
        "name": "principal",
        "requires": [
          "expiry",
          "principal_authority",
          "purpose",
          "scope"
        ],
        "produces": [
          "delegation_envelope"
        ],
        "authority": "delegate:scope"
      },
      {
        "name": "delegate",
        "requires": [
          "delegation_envelope"
        ],
        "produces": [
          "acceptance"
        ],
        "authority": "accept:delegation"
      },
      {
        "name": "performer",
        "requires": [
          "acceptance"
        ],
        "produces": [
          "performance"
        ],
        "authority": "perform:within-scope"
      }
    ],
    "commitments": [
      {
        "key": "perform_within_scope",
        "committer": "delegate",
        "beneficiary": "principal",
        "resource": "performance bounded by the delegated scope and purpose",
        "discharged_by": "performance"
      }
    ]
  },
  {
    "family": "data_compute",
    "name": "compute-placement",
    "keys": [
      "placement_id"
    ],
    "given": [
      "resource_ceiling",
      "workload"
    ],
    "evidence_required": [],
    "roles": [
      {
        "name": "planner",
        "requires": [
          "resource_ceiling",
          "workload"
        ],
        "produces": [
          "placement_plan"
        ],
        "authority": "plan:compute"
      },
      {
        "name": "admitter",
        "requires": [
          "placement_plan"
        ],
        "produces": [
          "admission"
        ],
        "authority": "admit:resource"
      },
      {
        "name": "executor",
        "requires": [
          "admission"
        ],
        "produces": [
          "result"
        ],
        "authority": "execute:compute"
      }
    ],
    "commitments": [
      {
        "key": "stay_within_the_admitted_ceiling",
        "committer": "executor",
        "beneficiary": "admitter",
        "resource": "execution within the admitted resource ceiling",
        "discharged_by": "result"
      }
    ]
  },
  {
    "family": "data_compute",
    "name": "minimum-dataset-slice",
    "keys": [
      "request_id"
    ],
    "given": [
      "question",
      "requester_purpose"
    ],
    "evidence_required": [],
    "roles": [
      {
        "name": "requester",
        "requires": [
          "question",
          "requester_purpose"
        ],
        "produces": [
          "slice_request"
        ],
        "authority": "request:data"
      },
      {
        "name": "bounder",
        "requires": [
          "slice_request"
        ],
        "produces": [
          "slice_bound"
        ],
        "authority": "bound:disclosure",
        "reads": [
          "slice_request"
        ]
      },
      {
        "name": "holder",
        "requires": [
          "slice_bound"
        ],
        "produces": [
          "slice"
        ],
        "authority": "release:data",
        "reads": [
          "slice_bound"
        ]
      }
    ],
    "commitments": [
      {
        "key": "release_only_within_the_bound",
        "committer": "holder",
        "beneficiary": "bounder",
        "resource": "data no wider than the bound allows",
        "discharged_by": "slice"
      }
    ]
  },
  {
    "family": "economic",
    "name": "collective-purchase",
    "keys": [
      "group_buy_id"
    ],
    "given": [
      "offer",
      "threshold"
    ],
    "evidence_required": [],
    "roles": [
      {
        "name": "initiator",
        "requires": [
          "offer",
          "threshold"
        ],
        "produces": [
          "group_offer"
        ],
        "authority": "open:collective"
      },
      {
        "name": "participant",
        "requires": [
          "group_offer"
        ],
        "produces": [
          "pledge"
        ],
        "authority": "pledge:collective"
      },
      {
        "name": "threshold_check",
        "requires": [
          "pledge"
        ],
        "produces": [
          "threshold_met"
        ],
        "authority": "evaluate:collective",
        "reads": [
          "pledge",
          "threshold"
        ]
      },
      {
        "name": "seller",
        "requires": [
          "threshold_met"
        ],
        "produces": [
          "acceptance"
        ],
        "authority": "accept:collective"
      }
    ],
    "commitments": [
      {
        "key": "honour_the_group_price",
        "committer": "seller",
        "beneficiary": "initiator",
        "resource": "the offered terms once the threshold is met",
        "discharged_by": "acceptance"
      },
      {
        "key": "pay_the_pledged_amount",
        "committer": "participant",
        "beneficiary": "seller",
        "resource": "each participant's pledged amount",
        "discharged_by": "threshold_met"
      }
    ]
  },
  {
    "family": "economic",
    "name": "escrow",
    "keys": [
      "escrow_id"
    ],
    "given": [
      "agreement",
      "amount"
    ],
    "evidence_required": [],
    "roles": [
      {
        "name": "payer",
        "requires": [
          "agreement",
          "amount"
        ],
        "produces": [
          "deposit"
        ],
        "authority": "deposit:escrow"
      },
      {
        "name": "agent",
        "requires": [
          "deposit"
        ],
        "produces": [
          "hold_confirmation"
        ],
        "authority": "hold:escrow"
      },
      {
        "name": "deliverer",
        "requires": [
          "hold_confirmation"
        ],
        "produces": [
          "delivery"
        ],
        "authority": "deliver:agreement"
      },
      {
        "name": "releaser",
        "requires": [
          "delivery"
        ],
        "produces": [
          "release"
        ],
        "authority": "release:escrow"
      }
    ],
    "commitments": [
      {
        "key": "deliver_the_agreement",
        "committer": "deliverer",
        "beneficiary": "payer",
        "resource": "the agreed goods or capability",
        "discharged_by": "delivery"
      },
      {
        "key": "release_the_held_funds",
        "committer": "agent",
        "beneficiary": "deliverer",
        "resource": "the held amount, on observed delivery",
        "discharged_by": "release"
      }
    ]
  },
  {
    "family": "economic",
    "name": "ownership-transfer",
    "keys": [
      "transfer_id"
    ],
    "given": [
      "asset",
      "from_party",
      "to_party"
    ],
    "evidence_required": [],
    "roles": [
      {
        "name": "transferor",
        "requires": [
          "asset",
          "from_party",
          "to_party"
        ],
        "produces": [
          "transfer_intent"
        ],
        "authority": "transfer:ownership"
      },
      {
        "name": "registry",
        "requires": [
          "transfer_intent"
        ],
        "produces": [
          "record"
        ],
        "authority": "record:ownership"
      },
      {
        "name": "transferee",
        "requires": [
          "record"
        ],
        "produces": [
          "acceptance"
        ],
        "authority": "accept:ownership"
      }
    ],
    "commitments": [
      {
        "key": "record_the_transfer",
        "committer": "registry",
        "beneficiary": "transferee",
        "resource": "a durable record that title passed",
        "discharged_by": "record"
      }
    ]
  },
  {
    "family": "economic",
    "name": "sponsor-obligation",
    "keys": [
      "sponsor_contract_id"
    ],
    "given": [
      "live_occurrence",
      "sponsor_contract"
    ],
    "evidence_required": [],
    "roles": [
      {
        "name": "creator",
        "requires": [
          "live_occurrence",
          "sponsor_contract"
        ],
        "produces": [
          "spoken_acknowledgement"
        ],
        "authority": "perform:sponsorship"
      },
      {
        "name": "publisher",
        "requires": [
          "spoken_acknowledgement"
        ],
        "produces": [
          "published_clip"
        ],
        "authority": "publish:clip"
      },
      {
        "name": "observer",
        "requires": [
          "published_clip"
        ],
        "produces": [
          "external_observation"
        ],
        "authority": "observe:foreign"
      },
      {
        "name": "settler",
        "requires": [
          "external_observation"
        ],
        "produces": [
          "settlement_evidence"
        ],
        "authority": "settle:sponsorship",
        "reads": [
          "external_observation"
        ]
      }
    ],
    "commitments": [
      {
        "key": "speak_the_acknowledgements",
        "committer": "creator",
        "beneficiary": "settler",
        "resource": "the contracted spoken acknowledgements, on air",
        "discharged_by": "spoken_acknowledgement"
      },
      {
        "key": "publish_the_clip",
        "committer": "publisher",
        "beneficiary": "settler",
        "resource": "one clip published to the agreed platforms",
        "discharged_by": "published_clip"
      },
      {
        "key": "pay_on_evidence",
        "committer": "settler",
        "beneficiary": "creator",
        "resource": "the sponsorship fee, on observed performance",
        "discharged_by": "settlement_evidence"
      }
    ]
  },
  {
    "family": "evidence",
    "name": "evidence-challenge",
    "keys": [
      "challenge_id"
    ],
    "given": [
      "claim"
    ],
    "evidence_required": [],
    "roles": [
      {
        "name": "challenger",
        "requires": [
          "claim"
        ],
        "produces": [
          "challenge"
        ],
        "authority": "challenge:claim"
      },
      {
        "name": "respondent",
        "requires": [
          "challenge"
        ],
        "produces": [
          "evidence"
        ],
        "authority": "respond:challenge"
      },
      {
        "name": "adjudicator",
        "requires": [
          "evidence"
        ],
        "produces": [
          "verdict"
        ],
        "authority": "adjudicate:evidence",
        "reads": [
          "evidence"
        ]
      }
    ],
    "commitments": [
      {
        "key": "answer_the_challenge",
        "committer": "respondent",
        "beneficiary": "challenger",
        "resource": "evidence sufficient to cross the proof frontier",
        "discharged_by": "evidence"
      }
    ]
  },
  {
    "family": "foreign_world",
    "name": "carrier-upgrade",
    "keys": [
      "source_id"
    ],
    "given": [
      "candidate_carrier",
      "current_carrier"
    ],
    "evidence_required": [],
    "roles": [
      {
        "name": "prober",
        "requires": [
          "candidate_carrier",
          "current_carrier"
        ],
        "produces": [
          "probe_result"
        ],
        "authority": "probe:foreign"
      },
      {
        "name": "verifier",
        "requires": [
          "probe_result"
        ],
        "produces": [
          "upgrade_evidence"
        ],
        "authority": "verify:carrier",
        "reads": [
          "probe_result"
        ]
      },
      {
        "name": "adopter",
        "requires": [
          "upgrade_evidence"
        ],
        "produces": [
          "adoption"
        ],
        "authority": "adopt:carrier"
      }
    ],
    "commitments": [
      {
        "key": "read_through_the_upgraded_carrier",
        "committer": "adopter",
        "beneficiary": "verifier",
        "resource": "subsequent reads through the verified carrier",
        "discharged_by": "adoption"
      }
    ]
  },
  {
    "family": "foreign_world",
    "name": "foreign-observation",
    "keys": [
      "twin_id"
    ],
    "given": [
      "observe_right",
      "source_url"
    ],
    "evidence_required": [],
    "roles": [
      {
        "name": "preflight",
        "requires": [
          "observe_right",
          "source_url"
        ],
        "produces": [
          "policy_decision"
        ],
        "authority": "preflight:foreign"
      },
      {
        "name": "observer",
        "requires": [
          "policy_decision"
        ],
        "produces": [
          "retained_body"
        ],
        "authority": "observe:foreign",
        "reads": [
          "policy_decision",
          "source_url"
        ]
      },
      {
        "name": "receipting",
        "requires": [
          "retained_body"
        ],
        "produces": [
          "receipt"
        ],
        "authority": "receipt:observation"
      }
    ],
    "commitments": [
      {
        "key": "retain_a_re_examinable_receipt",
        "committer": "receipting",
        "beneficiary": "observer",
        "resource": "a receipt naming what was fetched, when and from where",
        "discharged_by": "receipt"
      }
    ]
  },
  {
    "family": "identity",
    "name": "relationship-admission",
    "keys": [
      "relationship_id"
    ],
    "given": [
      "counterparty",
      "proposer",
      "relationship_kind"
    ],
    "evidence_required": [],
    "roles": [
      {
        "name": "proposer",
        "requires": [
          "counterparty",
          "proposer",
          "relationship_kind"
        ],
        "produces": [
          "proposal"
        ],
        "authority": "propose:relationship"
      },
      {
        "name": "admitter",
        "requires": [
          "proposal"
        ],
        "produces": [
          "admission"
        ],
        "authority": "admit:relationship"
      }
    ],
    "commitments": [
      {
        "key": "honour_the_relationship",
        "committer": "admitter",
        "beneficiary": "proposer",
        "resource": "what this kind of relationship obliges",
        "discharged_by": "admission"
      }
    ]
  },
  {
    "family": "identity",
    "name": "subject-introduction",
    "keys": [
      "introduction_id"
    ],
    "given": [
      "introducer",
      "subject_a",
      "subject_b"
    ],
    "evidence_required": [],
    "roles": [
      {
        "name": "introducer",
        "requires": [
          "introducer",
          "subject_a",
          "subject_b"
        ],
        "produces": [
          "introduction"
        ],
        "authority": "introduce:subject"
      },
      {
        "name": "recipient",
        "requires": [
          "introduction"
        ],
        "produces": [
          "acknowledgement"
        ],
        "authority": "receive:introduction"
      }
    ],
    "commitments": [
      {
        "key": "stand_behind_the_introduction",
        "committer": "introducer",
        "beneficiary": "recipient",
        "resource": "the introducer's account of the party introduced",
        "discharged_by": "acknowledgement"
      }
    ]
  },
  {
    "family": "meta",
    "name": "agent-delegation",
    "keys": [
      "delegation_id"
    ],
    "given": [
      "ceiling",
      "operation",
      "principal",
      "purpose"
    ],
    "evidence_required": [],
    "roles": [
      {
        "name": "principal",
        "requires": [
          "ceiling",
          "operation",
          "principal",
          "purpose"
        ],
        "produces": [
          "envelope"
        ],
        "authority": "delegate:operation"
      },
      {
        "name": "agent",
        "requires": [
          "envelope"
        ],
        "produces": [
          "acceptance"
        ],
        "authority": "accept:delegation"
      },
      {
        "name": "actor",
        "requires": [
          "acceptance"
        ],
        "produces": [
          "agent_result"
        ],
        "authority": "act:within-envelope"
      },
      {
        "name": "auditor",
        "requires": [
          "agent_result"
        ],
        "produces": [
          "conformance_report"
        ],
        "authority": "audit:delegation",
        "reads": [
          "agent_result",
          "envelope"
        ]
      }
    ],
    "commitments": [
      {
        "key": "act_only_within_the_envelope",
        "committer": "agent",
        "beneficiary": "principal",
        "resource": "action bounded by the delegated purpose and ceiling",
        "discharged_by": "conformance_report"
      }
    ]
  },
  {
    "family": "physical",
    "name": "reservation",
    "keys": [
      "reservation_id"
    ],
    "given": [
      "resource",
      "window"
    ],
    "evidence_required": [],
    "roles": [
      {
        "name": "requester",
        "requires": [
          "resource",
          "window"
        ],
        "produces": [
          "reservation_request"
        ],
        "authority": "request:reservation"
      },
      {
        "name": "holder",
        "requires": [
          "reservation_request"
        ],
        "produces": [
          "hold"
        ],
        "authority": "hold:resource"
      },
      {
        "name": "confirmer",
        "requires": [
          "hold"
        ],
        "produces": [
          "confirmation"
        ],
        "authority": "confirm:reservation"
      }
    ],
    "commitments": [
      {
        "key": "keep_the_resource_available",
        "committer": "holder",
        "beneficiary": "requester",
        "resource": "the resource, available through the reserved window",
        "discharged_by": "confirmation"
      }
    ]
  },
  {
    "family": "social",
    "name": "event-formation",
    "keys": [
      "claim_id"
    ],
    "given": [
      "ephemeral_signal"
    ],
    "evidence_required": [],
    "roles": [
      {
        "name": "asserter",
        "requires": [
          "ephemeral_signal"
        ],
        "produces": [
          "claim"
        ],
        "authority": "assert:claim"
      },
      {
        "name": "corroborator",
        "requires": [
          "claim"
        ],
        "produces": [
          "corroboration"
        ],
        "authority": "observe:foreign"
      },
      {
        "name": "admitter",
        "requires": [
          "corroboration"
        ],
        "produces": [
          "admitted_event"
        ],
        "authority": "admit:event",
        "reads": [
          "corroboration"
        ]
      }
    ],
    "commitments": [
      {
        "key": "perform_as_announced",
        "committer": "asserter",
        "beneficiary": "admitter",
        "resource": "the performance the claim announced",
        "discharged_by": "performance"
      }
    ]
  },
  {
    "family": "social",
    "name": "mutual-aid",
    "keys": [
      "request_id"
    ],
    "given": [
      "locality",
      "need"
    ],
    "evidence_required": [],
    "roles": [
      {
        "name": "asker",
        "requires": [
          "locality",
          "need"
        ],
        "produces": [
          "aid_request"
        ],
        "authority": "ask:aid"
      },
      {
        "name": "helper",
        "requires": [
          "aid_request"
        ],
        "produces": [
          "aid_offer"
        ],
        "authority": "offer:aid"
      },
      {
        "name": "acceptor",
        "requires": [
          "aid_offer"
        ],
        "produces": [
          "aid_acceptance"
        ],
        "authority": "accept:aid"
      }
    ],
    "commitments": [
      {
        "key": "show_up_as_offered",
        "committer": "helper",
        "beneficiary": "asker",
        "resource": "the help that was offered, at the time offered",
        "discharged_by": "aid_acceptance"
      }
    ]
  },
  {
    "family": "work",
    "name": "contract-net",
    "keys": [
      "cfp_id"
    ],
    "given": [
      "deadline",
      "task"
    ],
    "evidence_required": [],
    "roles": [
      {
        "name": "caller",
        "requires": [
          "deadline",
          "task"
        ],
        "produces": [
          "call_for_proposals"
        ],
        "authority": "announce:task"
      },
      {
        "name": "participant",
        "requires": [
          "call_for_proposals"
        ],
        "produces": [
          "proposal"
        ],
        "authority": "propose:task"
      },
      {
        "name": "awarder",
        "requires": [
          "proposal"
        ],
        "produces": [
          "award"
        ],
        "authority": "award:task",
        "reads": [
          "proposal",
          "task"
        ]
      },
      {
        "name": "awardee",
        "requires": [
          "award"
        ],
        "produces": [
          "result"
        ],
        "authority": "perform:task"
      }
    ],
    "commitments": [
      {
        "key": "perform_awarded_task",
        "committer": "awardee",
        "beneficiary": "caller",
        "resource": "the awarded task, performed by the deadline",
        "discharged_by": "result"
      },
      {
        "key": "honor_the_award",
        "committer": "caller",
        "beneficiary": "awardee",
        "resource": "the terms proposed and accepted",
        "discharged_by": "award"
      }
    ]
  },
  {
    "family": "work",
    "name": "job-application",
    "keys": [
      "application_id"
    ],
    "given": [
      "applicant",
      "job_posting"
    ],
    "evidence_required": [],
    "roles": [
      {
        "name": "publisher",
        "requires": [
          "job_posting"
        ],
        "produces": [
          "published_role"
        ],
        "authority": "publish:role"
      },
      {
        "name": "applicant",
        "requires": [
          "applicant",
          "published_role"
        ],
        "produces": [
          "application"
        ],
        "authority": "apply:role"
      },
      {
        "name": "reviewer",
        "requires": [
          "application"
        ],
        "produces": [
          "assessment"
        ],
        "authority": "review:application",
        "reads": [
          "application"
        ]
      },
      {
        "name": "decider",
        "requires": [
          "assessment"
        ],
        "produces": [
          "decision"
        ],
        "authority": "decide:hiring",
        "reads": [
          "assessment"
        ]
      }
    ],
    "commitments": [
      {
        "key": "answer_the_application",
        "committer": "decider",
        "beneficiary": "applicant",
        "resource": "a decision communicated to the applicant",
        "discharged_by": "decision"
      }
    ]
  },
  {
    "family": "work",
    "name": "mission-lease",
    "keys": [
      "mission_id"
    ],
    "given": [
      "lease_window",
      "mission"
    ],
    "evidence_required": [],
    "roles": [
      {
        "name": "offerer",
        "requires": [
          "lease_window",
          "mission"
        ],
        "produces": [
          "offer"
        ],
        "authority": "offer:mission"
      },
      {
        "name": "claimant",
        "requires": [
          "offer"
        ],
        "produces": [
          "lease"
        ],
        "authority": "claim:mission"
      },
      {
        "name": "worker",
        "requires": [
          "lease"
        ],
        "produces": [
          "completion_report"
        ],
        "authority": "work:mission"
      },
      {
        "name": "reviewer",
        "requires": [
          "completion_report"
        ],
        "produces": [
          "disposition"
        ],
        "authority": "dispose:mission"
      }
    ],
    "commitments": [
      {
        "key": "complete_or_escalate",
        "committer": "claimant",
        "beneficiary": "offerer",
        "resource": "the leased mission, completed or escalated before the lease expires",
        "discharged_by": "disposition"
      }
    ]
  },
  {
    "family": "work",
    "name": "quorum-decision",
    "keys": [
      "decision_id"
    ],
    "given": [
      "proposal",
      "quorum_rule"
    ],
    "evidence_required": [],
    "roles": [
      {
        "name": "proposer",
        "requires": [
          "proposal",
          "quorum_rule"
        ],
        "produces": [
          "tabled_proposal"
        ],
        "authority": "propose:decision"
      },
      {
        "name": "voter",
        "requires": [
          "tabled_proposal"
        ],
        "produces": [
          "vote"
        ],
        "authority": "vote:decision"
      },
      {
        "name": "teller",
        "requires": [
          "vote"
        ],
        "produces": [
          "quorum_result"
        ],
        "authority": "count:votes",
        "reads": [
          "quorum_rule",
          "vote"
        ]
      },
      {
        "name": "recorder",
        "requires": [
          "quorum_result"
        ],
        "produces": [
          "decision_snapshot"
        ],
        "authority": "record:decision"
      }
    ],
    "commitments": [
      {
        "key": "record_what_was_decided",
        "committer": "recorder",
        "beneficiary": "proposer",
        "resource": "a snapshot of the decision and the quorum that made it",
        "discharged_by": "decision_snapshot"
      }
    ]
  }
]
