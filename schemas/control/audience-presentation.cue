package control

// Matrix resolves scope. This declaration only supplies the public metadata
// phrasing for that already-resolved scope; it neither grants an audience nor
// says any offer, provider, price, or effect exists.
#AudiencePresentation: {
    scope: "self" | "public" | "market" | "institution"
    title: string & != ""
    description: string & != ""
}

nativeAudiencePresentations: [
    {
        scope: "self"
        title: "Bonfÿre — the workshop"
        description: "The self-scoped territory of the Bonfÿre matrix: work addressed and receipted before it is published."
    },
    {
        scope: "public"
        title: "Bonfÿre — in the open"
        description: "The public territory of the Bonfÿre matrix: receipted public-record observation, legible to people and to machines."
    },
    {
        scope: "market"
        title: "Bonfÿre — the market"
        description: "The market territory of the Bonfÿre matrix: priced capability, quoted from one service book."
    },
    {
        scope: "institution"
        title: "Bonfÿre — the institution"
        description: "The institutional territory of the Bonfÿre matrix: the work graph, the desk, and what is open for decision."
    },
]
