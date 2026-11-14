package control

// FormGraph: the reusable shape of a form.  lib/libbonfyre/src/bf_form.c
// compiles it against values and state into an AtomicForm and names every
// blocker; nothing here submits anything.

#FormField: {
    name: string & != ""
    kind: *"text" | string
    base_required: *true | bool
    // required only when another field holds this value
    required_when?: {field: string & != "", equals: string}
    // an actor whose verified display name may fill the field
    autofill_actor?: string & != ""
}

#FormEvidence: {
    name: string & != ""
    required: *true | bool
    match_kind?: string
    match_name?: [...string]
    match_tags?: [...string]
}

#FormGraph: {
    form_id: string & != ""
    title: string
    fields: [...#FormField]
    evidence: [...#FormEvidence] | *[]
    authority_required: [...string] | *[]
    review_gate?: string & != ""
}
