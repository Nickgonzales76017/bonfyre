package control

// A later right never follows from an earlier grant. Every grant is explicit.
#ForeignLiveRight: "observable" | "retainable" | "derivable" | "republishable"

#ForeignLiveSource: {
    twin_id: string & != ""
    platform: string & != ""
    url: string & != ""
    granted: [...#ForeignLiveRight]
    basis?: string
}

foreignLiveBinding: {
    rights: ["observable", "retainable", "derivable", "republishable"]
    kindMap: {
        caption: "utterance"
        chat: "chat"
        title: "topic"
        category: "topic"
        reaction: "reaction"
        presence: "presence"
    }
}
