package webaddress

#RelationshipObservationReview: {
    item: string & =~"^[a-z0-9_.-]+/[a-f0-9]{24}$"
    snapshot: string & =~"^[a-f0-9]{64}$"
    actor: string
    release: string & =~"^[a-f0-9]{64}$"
}
#RelationshipObservation: {
    schema: "bonfyre.relationship.v1"
    id: string & =~"^[a-f0-9]{64}$"
    state: "observed"
    label: string
    actor: {state: "unresolved", subject: null}
    source: {
        itemRef: #RelationshipObservationReview.item
        authorName: string
        snapshot: #RelationshipObservationReview.snapshot
        evidenceKind: "retained-source-credit" | "retained-source-item"
        source: string
        family: "jobs" | "people"
        kind: string
        publicPath: string
        title: string
        summary: string
        canonicalHref: string
    }
    opportunity: {kind: "job-opening" | "observed-mention", stage: "observed"}
    createdAt: string
}
