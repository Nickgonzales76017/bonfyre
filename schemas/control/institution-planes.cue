package control

// How an institution can actually be reached, and what each way can tell you.
//
// A parent should never have to ask whether their district has an API. They
// name the school; the estate works out which surfaces exist. But the surfaces
// are not equivalent, and pretending they are is how a calendar feed ends up
// presented as though it knew something about a child.
//
// So each plane declares two things: whether reaching it needs a relationship,
// and what it is incapable of telling you however well it is read. A public
// calendar knows when things happen and nothing about who attends. A roster
// plane knows who is enrolled and only because someone authorised it.
#InstitutionPlane: {
    plane: string & =~"^[a-z][a-z0-9-]*$"
    label: string & != ""
    // public: anyone may read it. granted: it answers only to a relationship
    // the institution itself established. Nothing moves between the two by
    // being wanted.
    reach: "public" | "granted"
    // What arrives when it is read.
    carries: string & != ""
    cannot_infer: string & != ""
}

nativeInstitutionPlanes: [...#InstitutionPlane] & [
    {plane: "calendar-ics", label: "Published calendar", reach: "public",
     carries: "when things are scheduled to happen, as the institution published them",
     cannot_infer: "a calendar entry says an event was published, never that it happened, that it still stands, or that any particular person is expected at it"},
    {plane: "feed", label: "News or notices feed", reach: "public",
     carries: "what the institution chose to announce, in the order it announced it",
     cannot_infer: "an announcement is what was published, not what was decided, and a silent feed is not an absence of news"},
    {plane: "web-page", label: "Public page", reach: "public",
     carries: "whatever the institution put on its own site",
     cannot_infer: "a page read once is not a page that still says that, and a staff listing is not a directory anyone consented to be in"},
    {plane: "document", label: "Published document", reach: "public",
     carries: "agendas, minutes, menus, handbooks and notices as filed",
     cannot_infer: "a document being public is not permission to republish it whole, and an agenda item is not a decision"},
    {plane: "open-data", label: "Open data catalogue", reach: "public",
     carries: "tabular public records the body publishes deliberately",
     cannot_infer: "an open dataset describes a population, never an individual in it, however few rows it has"},
    {plane: "roster", label: "Roster exchange", reach: "granted",
     carries: "enrolment, courses and sections, where a relationship has been established",
     cannot_infer: "a roster names who is enrolled; it is never evidence of attendance, performance, or anything about a person beyond enrolment"},
    {plane: "student-record", label: "Student information system", reach: "granted",
     carries: "attendance, assignments and notices for a person whose guardian connected it",
     cannot_infer: "connecting a record is permission for one household to read its own, never for this estate to hold, compare or publish it"},
    {plane: "messaging", label: "Family messaging", reach: "granted",
     carries: "what the institution sent to a family that connected it",
     cannot_infer: "a message delivered here is a copy shown to its recipient, never correspondence this estate is party to"},
    {plane: "payment", label: "Fees and payments", reach: "granted",
     carries: "what is owed and what was paid, where a household connected it",
     cannot_infer: "a balance shown is what the institution stated; this estate holds no money and settles nothing"},
]
