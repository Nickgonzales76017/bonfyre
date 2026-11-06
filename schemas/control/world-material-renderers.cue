package control

// World characters are presentation programs.  They decide only the visual
// shape of already-admitted material; a character does not choose a source,
// parse a body, change a subject, or authorize a route.
#WorldMaterialRenderer: {
    character: string & != ""
    container_class: "wall" | "pins" | "file" | "tape" | "read" | "cols" | "rows"
    item_variant: "flyer" | "pin" | "case" | "tick" | "reading" | "article" | "row"
}

nativeWorldMaterialRenderers: [
    {character: "zine", container_class: "wall", item_variant: "flyer"},
    {character: "scrapbook", container_class: "pins", item_variant: "pin"},
    {character: "dossier", container_class: "file", item_variant: "case"},
    {character: "ticker", container_class: "tape", item_variant: "tick"},
    {character: "almanac", container_class: "read", item_variant: "reading"},
    {character: "journal", container_class: "cols", item_variant: "article"},
    // This explicit resilience presentation applies only when a legacy World
    // has not yet selected a studied character. It is not an inferred family.
    {character: "default", container_class: "rows", item_variant: "row"},
]
