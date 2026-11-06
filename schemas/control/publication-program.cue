package control

#PublicationTarget: {
    platform: string & != ""
    carrier: string & != ""
    audience: "self" | "household" | "group" | "community" | "public" | "market" | "institution"
    operation: string & != ""
}

publicationBinding: {
    defaultOperation: "publish"
    defaultAudience: "public"
    stages: ["delegation", "disclosure", "projection", "adapter", "observation"]
}
