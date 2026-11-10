package control

// The declarative input boundary for the native RetentionGC owner.  A record
// cannot choose its own tier: the C owner derives it from `kind` so a caller
// cannot lower a record's retention responsibility by changing a payload.
#RetentionKind: "reaction" | "presence" | "chat" | "utterance" | "speaker" |
    "topic" | "entity" | "place" | "chapter" | "clip_candidate" | "cta" |
    "claim" | "sponsor_mention" | "commerce"

#RetentionHold: "open_obligation" | "evidence_reference" | "legal_hold" | "user_saved"

#RetentionRecord: {
    record_id: string & !=""
    occurrence: string & !=""
    kind: #RetentionKind
    written_at_ms: int
    body?: string
}

#RetentionHoldPlacement: {
    record_id: string & !=""
    hold: #RetentionHold
    holder: string & !=""
}
