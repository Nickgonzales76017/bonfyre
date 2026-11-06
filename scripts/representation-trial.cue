// Trial order is authored; the exact reference is always measured first.
representationTrial: {
  schema: "bonfyre-representation-trial.v1"
  representations: [
    {name: "exact-f32", note: "the reference; every other envelope is relative to it"},
    {name: "int8", note: "one byte per dimension, per-body scale"},
    {name: "sign-1bit", note: "one bit per dimension"},
    {name: "halved-f32", note: "first half of the dimensions, kept as a control"},
  ]
}
