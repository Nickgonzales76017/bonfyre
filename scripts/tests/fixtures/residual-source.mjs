console.log(JSON.stringify({
  schema: "bonfyre-test-residual.v1",
  residual: {
    TemporalResidual: { value: 3, attribution: "stale public source" },
    CapitalResidual: { value: 9, attribution: "requires a funding decision" },
  },
}));
