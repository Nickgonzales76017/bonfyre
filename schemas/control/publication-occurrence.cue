package control

#PublicationOccurrence: {
    occurrence_id: string & != ""
    source_twin: string & != ""
    source_ref: string & != ""
    canonical_href?: string
    title: string & != ""
    summary?: string
    published_at?: string
    observed_at: string & != ""
    receipt_ref: string & != ""
    rights_basis: string & != ""
    retention: "ephemeral" | "retained" | "archival"
    visibility: "public" | "scoped" | "withheld"
    assertion_state: "OBSERVED"
    cannot_infer: string & != ""
    status: "draft" | "published" | "retired"
}

// Coordinates for the existing native retained-input reader. The fingerprint
// covers its complete exported input; it does not grant publication authority.
#RetainedPublicSelection: {
    source: string & =~"^[a-z0-9._-]{1,95}$"
    item: string & =~"^[a-f0-9]{24}$"
    snapshot: string & =~"^[a-f0-9]{64}$"
    cursor?: 0
}
#RetainedPublicContinuation: {
    #RetainedPublicSelection
    source: string
    item: string
    return?: "/o/\(source)/\(item)"
}

// A private source-set handoff names the native record that must be reread.
// It carries no passage text or inferred destination permission.
#SavedSourceResearchSelection: {
    endpoint: string & =~"^/apps/ws-[a-f0-9]{40}/_records$"
    key: string & =~"^connections:source-set:[a-f0-9]{64}$"
    revision: int & >0 & <2147483647
    context: "self" | string & =~"^(institution|household):[A-Za-z0-9][A-Za-z0-9._-]{0,95}$"
}
