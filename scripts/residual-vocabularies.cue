package residualvocabularies

voices: [
  {kind: "evidence", capability: "retrieval.record", phrasings: ["{c} record", "{c} statement", "verified {c}", "{c} documentation"]},
  {kind: "partner", capability: "discovery.entity", phrasings: ["{c} partner", "{c} operator", "experienced {c} principal", "{c} joint venture"]},
  {kind: "capital", capability: "capital.route", phrasings: ["{c} financing", "{c} facility", "{c} funding route"]},
  {kind: "authority", capability: "authority.lookup", phrasings: ["{c} permit", "{c} authorisation", "{c} approval process"]},
  {kind: "contract", capability: "quote.request", phrasings: ["{c} quote", "{c} coverage", "{c} terms"]},
  {kind: "work", capability: "work.plan", phrasings: ["{c} plan", "{c} scheduling", "{c} staffing"]},
  {kind: "space", capability: "availability.lookup", phrasings: ["{c} availability", "{c} booking", "{c} hold"]},
  {kind: "people", capability: "availability.lookup", phrasings: ["{c} confirmation", "{c} booking", "{c} availability"]},
  {kind: "economics", capability: "computation.internal", phrasings: ["{c} model", "{c} projection"]},
]
fallbackCapability: "retrieval.search"
rungs: ["named", "capability declared", "route admissible", "trial executed", "result retrieved", "requirement closed"]
