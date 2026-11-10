package gym

// Ordered classification law. A cause beyond institutional control stays on
// the human side of the ranking boundary, regardless of its attempt count.
causes: [
  {name: "no_credit", pattern: "insufficient_quota|credit_balance_exhausted|no credits remaining", closable: false, remedy: "the account has no credit; a person adds credit or the work stops"},
  {name: "bad_credential", pattern: "invalid_api_key|incorrect api key|authenticationerror", closable: false, remedy: "a credential is wrong or revoked; a person replaces it"},
  {name: "rate_limited", pattern: "rate.?limit(?!.*insufficient)|429(?!.*insufficient)", closable: true, remedy: "backoff and route to an independent supplier"},
  {name: "mcp_unreachable", pattern: "connection refused|errno 111|timed out|timeouterror|http 0", closable: true, remedy: "supervise the transport and fail the goal rather than the tool call"},
  {name: "tool_missing", pattern: "unknown tool|no such tool|not registered|tool_not_found", closable: true, remedy: "register the capability or stop advertising it"},
  {name: "context_exceeded", pattern: "context.?length|too many tokens|maximum context", closable: true, remedy: "cut the context; this is what the compression estate is for"},
  {name: "no_finish", pattern: "ended without operator_finish|model ended without", closable: true, remedy: "the run produced no disposition; the default retry hid it"},
]
