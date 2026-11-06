package roundtrip

roundtripLaw: {
  origin: "https://aurekai.life"
  projections: [
    {carrier: "activitypub", address: "/ap/actor", required: ["id", "type", "inbox", "preferredUsername"]},
    {carrier: "activitypub", address: "/.well-known/webfinger?resource=acct:bonfyre@aurekai.life", required: ["subject", "links"]},
    {carrier: "a2a", address: "/.well-known/agent.json", required: ["name", "url", "skills"]},
    {carrier: "mcp", address: "/mcp/resources/", required: ["resources"]},
    {carrier: "beckn", address: "/beckn/search", required: ["context"]},
    {carrier: "machine-discovery", address: "/.well-known/bonfyre", required: ["version", "host"]},
  ]
  agreement: [
    {name: "actor id vs webfinger", kind: "actor-webfinger", left: "/ap/actor", right: "/.well-known/webfinger?resource=acct:bonfyre@aurekai.life"},
    {name: "actor host vs agent card", kind: "host", left: "/ap/actor", right: "/.well-known/agent.json"},
  ]
  forbidden_inference: [
    "fidelity -> reach: nothing here says anyone fetched it",
    "fidelity -> acceptance: a federation may serve the document and refuse the actor",
    "a present field -> a correct value",
  ]
}
