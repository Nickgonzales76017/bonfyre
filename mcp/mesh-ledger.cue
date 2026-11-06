package meshledger

// A bounded, read-only slice of the legacy mesh. This is not the rich gateway.
registry: {
    tools: [{
        name: "mcp_call_history"
        description: "Return recent MCP tool calls with trace IDs, durations, and states."
        inputSchema: {
            type: "object"
            properties: {
                tool: {type: "string", maxLength: 256}
                state: {type: "string", maxLength: 256}
                limit: {type: "integer", minimum: 1, maximum: 2000, default: 200}
            }
            additionalProperties: false
        }
        annotations: {readOnlyHint: true, destructiveHint: false}
    }]
    resources: [{
        uri: "bonfyre://mcp/calls"
        name: "MCP call ledger"
        description: "Recent MCP calls with trace IDs and durations."
        mimeType: "application/json"
    }]
}
