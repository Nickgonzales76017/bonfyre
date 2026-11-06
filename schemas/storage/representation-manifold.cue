package storage
#Temperature: "absent" | "reconstructable" | "cold" | "warm" | "hot" | "pinned"
#Authority: "semantic-owner" | "evidence" | "derived" | "cache" | "candidate"
#Representation: {
  kind: string
  authority: #Authority
  required: bool
  reconstructable: bool
  temperature: #Temperature
  unlocks: [...string]
}
#RepresentationPortfolio: {
  semantic_kind: string
  representations: [...#Representation]
}
#Invariant: {
  one_semantic_identity: true
  physical_representation_never_mints_authority: true
  transformations_are_receipted: true
  frontier_equivalence_is_provable: true
  evicted_derived_forms_remain_reconstructable: true
}
