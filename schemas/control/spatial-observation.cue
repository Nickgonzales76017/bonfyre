package control

// SpatialObservation says where an existing subject was observed or projected.
// It is not an identity for that subject and never becomes one: identity,
// authority, proof, permission, provider capability and canonical truth stay
// with the owners that already hold them. A coordinate arriving here cannot
// create a subject, and a subject having a coordinate says nothing about
// whether anyone may be told it.
//
// The discipline that makes this worth owning is `observation_mode`. A display
// position computed between two fixes is not a third fix. Interpolated,
// propagated and simulated positions are legitimate and useful, and they are
// typed differently from what a source actually reported, so a derived state
// moving at 12 Hz never writes history that nobody observed.

#GeometryKind: "point" | "line" | "polygon" | "volume" | "trajectory"

// What produced this position. Ordered from what a source stated to what this
// estate computed; the boundary between `reported` and everything below it is
// the boundary between observation and derivation.
#ObservationMode:
	"observed" |     // the source stated this position at this time
	"reported" |     // a party reported it; the source did not measure it
	"propagated" |   // carried forward from a prior fix under a stated model
	"interpolated" | // computed between two real fixes for display
	"inferred" |     // derived from non-positional evidence
	"simulated"      // modelled; no observation stands behind it

#Accuracy: {
	horizontal?: float & >=0   // metres
	vertical?:   float & >=0   // metres
	confidence?: float & >=0 & <=1
}

#Motion: {
	altitude?:        float           // metres, against vertical_datum
	heading?:         float & >=0 & <360
	speed?:           float & >=0     // metres/second
	velocity_vector?: [float, float, float]
}

#SpatialObservation: {
	subject: string & != ""            // an existing addressed subject
	geometry_kind: #GeometryKind
	geometry: string & != ""           // exact geometry, opaque here

	crs: string & != ""
	horizontal_datum: string & != ""
	vertical_datum?: string & != ""

	// Three separate times. `observed_at` is when the source saw it,
	// `effective_at` is when this position is claimed to hold, `valid_until`
	// is when the claim expires rather than silently persisting.
	observed_at: string & != ""
	effective_at?: string & != ""
	valid_until?: string & != ""

	source_occurrence: string & != ""
	source_digest?: string & != ""

	observation_mode: #ObservationMode
	// A derived position names the observations it was computed from. Without
	// this an interpolated fix is indistinguishable from a reported one after
	// the fact, which is the failure this whole record exists to prevent.
	derived_from?: [...string & != ""]

	accuracy?: #Accuracy
	motion?: #Motion

	provenance: string & != ""
	license_profile?: string & != ""
}

// A CoverageVolume is what an owner can see, reach, serve or carry from a
// place. Generalised deliberately: a camera frustum, a delivery radius, a
// transit service region, a stage's audible area and a radar cell are the same
// shape of claim. It describes reach, never entitlement -- a subject falling
// inside a coverage volume is not thereby observable, disclosable or servable.
#CoverageVolume: {
	owner_subject: string & != ""
	origin: string & != ""             // exact position, opaque here
	orientation?: {
		heading?: float & >=0 & <360
		pitch?:   float & >= -90 & <=90
		roll?:    float
	}
	horizontal_extent?: float & >=0 & <=360   // degrees of field
	vertical_extent?:   float & >=0 & <=180
	range?: float & >=0                        // metres
	valid_frontier: string & != ""
	quality?: string & != ""
	// How this volume was arrived at. A published specification and a measured
	// footprint are not the same claim and must not be read as one.
	derivation: "specified" | "measured" | "estimated" | "modelled"
}
