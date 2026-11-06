package procurement

#Hex64: =~"^[a-f0-9]{64}$"
#Id: string & !=""
#Timestamp: =~"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"

recordPrefixes: {
	purchaseList: "procurement-draft:"
	supplierRequest: "procurement-rfq:"
	supplierQuote: "procurement-quote:"
	quoteComparison: "procurement-comparison:"
	allocationPlan: "procurement-allocation:"
	approvalRequest: "procurement-approval:"
	orderIntent: "procurement-order-intent:"
}

Supplier: null | {
	id?: string & !=""
	name?: string & !=""
}

SupplierRequest: {
	schema: "bonfyre.procurement.supplier-request.v1"
	id: #Id
	status: "prepared"
	requested_at: #Timestamp
	supplier: Supplier
	supplier_contacted: false
	quote_received: false
	purchase_approved: false
	order_committed: false
	digest: #Hex64
}

SupplierQuoteObservation: {
	schema: "bonfyre.procurement.supplier-quote-observation.v1"
	id: #Id
	status: "observed"
	currency: =~"^[A-Z]{3}$"
	quote_accepted: false
	reserved: false
	order_authorized: false
	digest: #Hex64
}

QuoteComparison: {
	schema: "bonfyre.procurement.quote-comparison.v1"
	status: "current" | "partial" | "unpriced" | "mixed_currency" | "stale"
	approved: false
	committed: false
	ordered: false
	digest: #Hex64
}

AllocationPlan: {
	schema: "bonfyre.procurement.allocation-plan.v1"
	id: #Id
	status: "prepared"
	prepared_at: #Timestamp
	policy: "lowest_known_merchandise"
	currency: =~"^[A-Z]{3}$"
	fees_attribution: "unallocated"
	known_total_cents: null
	approval_required: true
	approved: false
	cart_created: false
	commitment_opened: false
	order_created: false
	commerce_chain: {
		owner: "CommerceChain"
		status: "unmaterialized"
		next_required_stage: "product"
	}
	digest: #Hex64
}


ApprovalRequest: {
	schema: "bonfyre.procurement.approval-request.v1"
	id: #Id
	status: "pending"
	requested_at: #Timestamp
	requester: #Id
	known_total_cents: null
	fees_attribution: "unallocated"
	authority: {
		owner: "AuthorityGraph"
		required_permissions: ["approve", "spend"]
		decision: "pending"
		decided_by: null
		decision_ref: null
	}
	approved: false
	spend_authorized: false
	quote_accepted: false
	commitment_opened: false
	order_created: false
	payment_presented: false
	digest: #Hex64
}

OrderIntent: {
	schema: "bonfyre.procurement.order-intent.v1"
	id: #Id
	status: "prepared"
	prepared_at: #Timestamp
	known_total_cents: null
	fees_attribution: "unallocated"
	authority_eligible: true
	quote_accepted: false
	reservation_created: false
	cart_created: false
	commitment_opened: false
	order_created: false
	purchase_order_issued: false
	supplier_accepted: false
	payable_created: false
	payment_presented: false
	settled: false
	fulfilled: false
	shipped: false
	received: false
	commitment_handoff: {
		owner: "CommitmentGraph"
		status: "unmaterialized"
		requires_quote_acceptance: true
		requires_supplier_counterparty_terms: true
	}
	digest: #Hex64
}
