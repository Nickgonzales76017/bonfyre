package control

// Closed declaration boundary for the native CommitmentGraph owner.  An
// authored record cannot select an arbitrary state transition: only the C
// state machine can append events after admission.
#CommitmentState: "open" | "discharged" | "delegated" | "violated" | "compensated"

#CommitmentRecord: {
    commitment_id: string & != ""
    committer: string & != ""
    beneficiary: string & != ""
    resource: string & != ""
    conditions?: string
    expires_at?: string
    authority?: string
    origin?: string
    enactment?: string
}

#CommitmentEvent: {
    commitment_id: string & != ""
    state: #CommitmentState
    actor?: string
    detail?: string
    at?: string
}
