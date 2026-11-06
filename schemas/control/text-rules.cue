package control

// Extraction declared once, so it is not re-derived per call site.
//
// This estate had 163 hand-rolled field extractors in C and 49 grep, sed and
// awk extractions across its witnesses, each one a small parser written from
// scratch at the moment it was needed. Two of them were wrong in ways nothing
// could notice:
//
//   a receipt reader stopped at the first quote in a string value, so an HTTP
//   ETag -- which is `W/"abc"` and arrives escaped -- came back as `W/\`. Every
//   source then ignored the validator and conditional re-observation had never
//   worked while appearing to.
//
//   the atlas parser accepted any key and stored unknown ones as scalars it
//   never read, so 77 `cannot_infer` lines written by three families were
//   silently discarded and the loss report went on saying those architectures
//   had stated nothing.
//
// Both are the same failure: an undeclared text rule that fails quietly. A
// declared rule cannot, because the vocabulary is checked and the escaping is
// implemented once rather than remembered every time.

// How a value is found in its source text. Each kind has exactly one
// implementation; adding a call site does not add a parser.
#FieldKind:
	"json_string" |     // escaping handled, including escaped quotes
	"json_number" |
	"json_bool" |
	"json_string_array" |
	"http_header" |     // case-insensitive name, first value wins
	"http_link_rel" |   // one relation out of a multi-relation Link header
	"tsv_column" |      // 1-indexed
	"yaff_field" |      // an indented `key value` line in a .yaff block
	"url_query" |       // a query parameter, percent-decoded
	"argv_option" |     // a command-line flag's value
	"text_column"       // a whitespace-separated column of a report table

#ExtractionRule: {
	name: string & =~"^[a-z][a-z0-9_]*$"
	kind: #FieldKind
	// What to look for: a JSON key, a header name, a link relation, a column
	// index, or a yaff field name.
	key: string & != ""
	// A rule may say a value must be present. Absence then fails loudly rather
	// than yielding an empty string that reads like a legitimate answer.
	required?: bool
	// What this field is, in one line, so a reader of the declaration does not
	// have to open the code that consumes it.
	means: string & != ""
}

#RuleSet: {
	name: string & =~"^[a-z][a-z0-9_-]*$"
	// The shape this ruleset reads. Naming it stops a ruleset for one document
	// being pointed at another that happens to parse.
	over: string & != ""
	rules: [...#ExtractionRule]
	// Unknown keys in the source are reported rather than dropped. A parser
	// that silently ignores what it does not recognise is a parser that hides
	// the author's intent from the author.
	report_unknown?: bool
}

#RuleFile: {
	rulesets: [...#RuleSet]
}

// The live rulesets.
rulesets: #RuleFile & {
	rulesets: [
		{
			name: "foreign-observation"
			over: "bonfyre-foreign-observation.v1"
			report_unknown: true
			rules: [
				{name: "twin_id",            kind: "json_string", key: "twin_id",            required: true,  means: "the ExternalEstate occurrence this observation is keyed to"},
				{name: "source_url",         kind: "json_string", key: "source_url",         required: true,  means: "the address that was requested"},
				{name: "observed_at",        kind: "json_string", key: "observed_at",        required: true,  means: "when this estate observed it"},
				{name: "content_sha256",     kind: "json_string", key: "content_sha256",     means: "this estate's digest of the bytes it retained"},
				{name: "body_path",          kind: "json_string", key: "body_path",          means: "where those bytes are held"},
				{name: "observation_kind",   kind: "json_string", key: "observation_kind",   required: true,  means: "which rung of the observation axis this reached"},
				{name: "policy_decision",    kind: "json_string", key: "policy_decision",    means: "what robots preflight decided"},
				{name: "http_status",        kind: "json_number", key: "http_status",        means: "what the transport answered"},
				{name: "body_bytes",         kind: "json_number", key: "body_bytes",         means: "how many bytes were retained, zero for a 304"},
				{name: "etag",               kind: "json_string", key: "etag",               means: "the validator the source issued, quotes included"},
				{name: "unchanged_from",     kind: "json_string", key: "unchanged_from_content_sha256", means: "the prior body a 304 refers to"},
				{name: "conditional",        kind: "json_bool",   key: "conditional",        means: "whether the request carried a validator"},
			]
		},
		{
			name: "http-response"
			over: "an HTTP response header block"
			rules: [
				{name: "etag",             kind: "http_header",   key: "ETag",             means: "the validator, verbatim and with its quotes"},
				{name: "last_modified",    kind: "http_header",   key: "Last-Modified",    means: "when the source says the representation changed"},
				{name: "content_type",     kind: "http_header",   key: "Content-Type",     means: "the media type served"},
				{name: "content_location", kind: "http_header",   key: "Content-Location", means: "the address of the representation actually served"},
				{name: "content_digest",   kind: "http_header",   key: "Content-Digest",   means: "RFC 9530: the source's own digest, never this estate's"},
				{name: "repr_digest",      kind: "http_header",   key: "Repr-Digest",      means: "RFC 9530: the source's digest of the selected representation"},
				{name: "signature_input",  kind: "http_header",   key: "Signature-Input",  means: "RFC 9421: which components a signature covers"},
				{name: "link_license",     kind: "http_link_rel", key: "license",          means: "the only machine-readable rights statement a source ever makes"},
				{name: "link_canonical",   kind: "http_link_rel", key: "canonical",        means: "the address the source considers authoritative"},
			]
		},
		{
			name: "observe-plan"
			over: "a bonfyre-facet observe-plan row"
			rules: [
				{name: "twin_id",   kind: "tsv_column", key: "1", required: true, means: "the occurrence to observe"},
				{name: "url",       kind: "tsv_column", key: "2", required: true, means: "the address to request"},
				{name: "host",      kind: "tsv_column", key: "3", required: true, means: "the host, so a driver can pace itself"},
				{name: "facets",    kind: "tsv_column", key: "4", means: "how many candidate facets this would unlock"},
				{name: "phenotype", kind: "tsv_column", key: "5", means: "the shape this occurrence was typed as"},
			]
		},
		{
			name: "spatial-frontier-query"
			over: "the query string of /w/<world>/spatial"
			rules: [
				{name: "at",      kind: "url_query", key: "at",      required: true, means: "the valid instant being asked about"},
				{name: "as_of",   kind: "url_query", key: "as-of",   means: "the knowledge instant to answer it with"},
				{name: "extent",  kind: "url_query", key: "extent",  means: "how wide a cut the caller wants"},
				{name: "region",  kind: "url_query", key: "region",  means: "a coarse region coordinate"},
				{name: "cell",    kind: "url_query", key: "cell",    means: "a coarse cell coordinate"},
				{name: "subject", kind: "url_query", key: "subject", means: "one subject of interest inside the world"},
			]
		},
	]
}
