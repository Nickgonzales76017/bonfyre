package representation

// A row's co-present coordinates are only a candidate projection. They do not
// establish a semantic relation, authority, adoption, or an external effect.
graphProjection: {
  schema: "bonfyre-graph-projection-policy.v1"
  // Unlike the old maximal anchor expansion, these pairs have an intelligible
  // direction. Even these remain observational candidate edges until an owner
  // interprets them.
  directedPairs: [
    {source: "source", target: "target"},
    {source: "src", target: "dst"},
    {source: "from", target: "to"},
    {source: "parent", target: "child"},
    {source: "subject", target: "object"},
  ]
  batchCompression: "ZSTD"
}
