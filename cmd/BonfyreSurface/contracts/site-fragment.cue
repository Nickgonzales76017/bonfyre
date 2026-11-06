// SiteFragment is authored presentation state. It may be projected by SurfaceIR,
// but it does not own the Matrix subject, publication authority, capability,
// payment state, or any effect rendered beside it.
package surface

#SiteFragment: {
	coordinate: =~"^[A-Za-z0-9][A-Za-z0-9._:-]{2,191}$"
	page:       =~"^[A-Za-z0-9][A-Za-z0-9._:-]{0,95}$"
	slot:       =~"^[A-Za-z0-9][A-Za-z0-9._:-]{0,95}$"
	audience: "self" | "household" | "group" | "community" | "public" | "market" | "institution"
	kind: "copy" | "hero" | "notice" | "card" | "cta" | "door" | "nav" | "section" | "empty-state" | "protocol"
	position: int & >=0 & <=1000000
	status: "draft" | "published" | "archived"

	eyebrow?: string
	heading?: string
	body?: string
	label?: string
	href?: string
	variantKey?: string
	semanticTags?: [...string]

	projection: {
		carriers: [...("html" | "htmx" | "mobile" | "mcp" | "a2a" | "json-ld" | "ro-crate" | "feed" | "open-graph")]
		fallback: "compiled" | "none"
	}

	provenance: {
		sourceRef: string
		sourceDigest?: =~"^[A-Fa-f0-9]{64}$"
		evidenceRef?: string
	}

	// Published fragments must have a source identity and at least one projection.
	// "manually authored" is a valid source identity; provenance does not require
	// pretending that human copy was derived from an external evidence object.
	if status == "published" {
		provenance: sourceRef: !=""
		projection: carriers: [_, ...]
	}

	// A link is presentation only. URLs that cause an effect are still interpreted
	// and gated by the carrier/Authority owner at request time.
	if href != _|_ {
		href: =~"^(https?://|/|#)"
	}
}

#SurfaceSelectionLaw: {
	exactCoordinate: bool
	approximationMayOverrideExact: false
	publicationRequiredForPublicProjection: true
