package control

#Territory: "life" | "live" | "shop" | "work"
#StandingAction: "WATCH" | "JOIN" | "GO" | "FOLLOW" | "SAVE" | "BOOK" |
    "BUY" | "CONTRIBUTE" | "MEET" | "APPLY" | "HELP" | "OFFER" | "APPROVE" |
    "CLAIM" | "FUND" | "CONTACT" | "PLAN" | "REMEMBER" | "SHARE" | "MOVE" |
    "COMPARE" | "REQUEST" | "SELL" | "DO" | "REVIEW" | "ASSIGN" | "RESPOND" | "ESCALATE"

#StandingOpportunity: {
    subject: string & != ""
    action: #StandingAction
    territory: #Territory
    relevance?: number
    available?: bool
}

// One authored verb vocabulary for all territory-facing compilers.
territoryActions: {
    life: ["MEET", "JOIN", "GO", "SAVE", "HELP", "FOLLOW", "PLAN", "REMEMBER", "SHARE", "MOVE", "ASSIGN"]
    live: ["WATCH", "JOIN", "GO", "FOLLOW", "SAVE", "SHARE"]
    shop: ["BUY", "OFFER", "JOIN", "SAVE", "COMPARE", "REQUEST", "SELL", "FUND", "CONTRIBUTE"]
    work: ["APPLY", "APPROVE", "CLAIM", "FUND", "CONTACT", "BOOK", "DO", "REVIEW", "ASSIGN", "RESPOND", "ESCALATE"]
}

territoryAudience: {
    life: "self"
    live: "public"
    shop: "market"
    work: "institution"
}

attentionSignals: ["watched", "swiped_past", "reacted", "dwelled", "followed_back", "muted"]
