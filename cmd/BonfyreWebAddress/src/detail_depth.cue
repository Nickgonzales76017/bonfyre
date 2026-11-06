package detaildepth

// Which record kind an application would file a thing like this as.
// Mirrors shapes[] in detail_depth.c; a row is shown only when the compiled
// shape store (BONFYRE_FRAPPE_SURFACE_DB frappe_program.slug) holds the slug.
// A shape is not a record: nothing is imported, held or permitted by it.

#What: "story" | "event" | "video" | "photo" | "audio" | "family:jobs" | "maker" | "publisher" | "topic"
#Slug: =~"^(frappe|erpnext|hrms|lms|crm|helpdesk)-[a-z0-9-]+$"

#Shape: {
	what: #What
	slug: #Slug
}

shapes: [...#Shape] & [
	{what: "story", slug:       "frappe-blog-post"},
	{what: "story", slug:       "frappe-web-page"},
	{what: "event", slug:       "frappe-event"},
	{what: "video", slug:       "erpnext-video"},
	{what: "photo", slug:       "frappe-file"},
	{what: "audio", slug:       "frappe-file"},
	{what: "family:jobs", slug: "hrms-job-opening"},
	{what: "family:jobs", slug: "lms-job-opportunity"},
	{what: "maker", slug:       "frappe-blogger"},
	{what: "maker", slug:       "frappe-contact"},
	{what: "maker", slug:       "crm-crm-contacts"},
	{what: "publisher", slug:   "crm-crm-organization"},
	{what: "topic", slug:       "frappe-tag"},
	{what: "topic", slug:       "frappe-blog-category"},
]

// The facets the offline derivation may write, and nothing else.
#Facet: "topic" | "maker" | "mention" | "linked" | "linked_root" | "month" | "media"
