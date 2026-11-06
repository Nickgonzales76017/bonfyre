package control

#OccurrenceOrigin: "observed" | "derived" | "effect"

#OccurrenceOriginRecord: {
    occurrence_id: string & != ""
    origin: #OccurrenceOrigin
    subject: string & != ""
    at?: string
    source?: string
    authority?: string
    derived_from?: [...string & != ""]
    if origin == "observed" {
        source: string & != ""
        authority?: ""
    }
    if origin == "derived" {
        derived_from: [string & != "", ...string & != ""]
        authority?: ""
    }
    if origin == "effect" {
        authority: string & != ""
    }
}
