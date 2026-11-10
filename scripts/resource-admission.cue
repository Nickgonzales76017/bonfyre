// A measured approximation may serve discovery, but never an institutional effect.
resourceAdmission: {
  exact_only_classes: ["authority", "money", "identity", "publication", "effect", "legal", "privacy", "fulfillment"]
  disclosure_scopes: ["public", "partner-shared", "institution-private", "self-private", "restricted"]
  default_work_class: "discovery"
  default_data_scope: "public"
  default_backend: "reference"
  default_axis: "latent_geometry"
  default_k: 5
  default_min_recall: 0.95
  fallback_representation: "exact-f32"
  schema: "bonfyre-resource-admission.v1"
}
