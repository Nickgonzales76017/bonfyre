rateDiscovery: {
  user_agent: "Aurekai/1.0 (price discovery; honours robots.txt; contact via aurekai.live)"
  amount_fields: ["maxAmountRequired", "amount", "price", "cost"]
  currency_fields: ["asset", "currency"]
  admitted_currencies: ["USD", "EUR", "GBP"]
  front_protocols: ["x402", "json", "api", "openapi", "graphql", "mcp"]
  op: "http_request"
  unit: "request"
  cannot_infer: "a rate admitted here is what a server said about itself in an HTTP 402 on the date recorded. It is not a rate that still holds, not a price anyone agreed to pay, and not evidence that this estate has paid it. A provider that stated no price is left unpriced, and usage of it is counted and left unpriced rather than counted as free."
}
