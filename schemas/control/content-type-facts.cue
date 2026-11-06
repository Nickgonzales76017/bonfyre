package control

// What each content type is, so the estate's own rows enter the relation
// grammar the way external occurrences already do.
//
// The facet compiler types 4,891 external occurrences by phenotype and derives
// 25 relations from them. The estate's own 54 content types -- the vocabularies
// that carry real rows -- declared none, so the compiler could propose facets
// for the outside world and nothing for the institution's own data. A
// media_program could not be known to be a media asset; a pack_demand could not
// be known to be an obligation.
//
// The domain axis is here because the estate is no longer one thing. Packs,
// music, community, site, external observation, spatial, governance, contract,
// frappe and cms are separate bodies of meaning that share one relation spine,
// and saying so is what lets a question be asked across them: which obligations
// exist anywhere, which media assets, which live entities.
//
// Cannot infer: a declared phenotype -> a row that exists. This says what a
// type is, not that anything has been written into it, and the estate holds
// several types with no rows at all.

#EstateDomain:
	"pack" |        // addressed behaviour: versions, overlays, activation
	"music" |       // works, recordings, releases, rights
	"media" |       // playable programs and physical carriers
	"community" |   // human social bodies and their occurrences
	"site" |        // authored presentation and navigation
	"surface" |     // interaction grammars over presentation
	"external" |    // what the outside world publishes into the estate
	"spatial" |     // where a subject was observed or can be reached
	"governance" |  // what is held back, screened or classified
	"contract" |    // what may be born, referenced or executed
	"frappe" |      // real installed Frappe lineages
	"household" |   // one person's or household's own records, seen by them
	"cms"           // authored articles and generation receipts

#ContentTypeFacts: {
	phenotypes: [#Phenotype, ...#Phenotype]
	domain:     #EstateDomain
	audience:   #DomainAudience
}

contentTypeFacts: [Name=string]: #ContentTypeFacts

contentTypeFacts: {
	// ---- pack: addressed behaviour, versioned and installed
	pack_artifact:           {phenotypes: ["VERSIONED_RECORD", "SERVICE_CAPABILITY"], domain: "pack", audience: "institution"}
	pack_version:            {phenotypes: ["VERSIONED_RECORD", "SUPPLY_CHAIN_OBJECT"], domain: "pack", audience: "institution"}
	pack_head:               {phenotypes: ["LIVE_ENTITY", "VERSIONED_RECORD"], domain: "pack", audience: "institution"}
	pack_overlay:            {phenotypes: ["VERSIONED_RECORD", "DATASET"], domain: "pack", audience: "institution"}
	pack_dependency:         {phenotypes: ["SUPPLY_CHAIN_OBJECT", "VERSIONED_RECORD"], domain: "pack", audience: "institution"}
	pack_installation:       {phenotypes: ["OBLIGATION", "VERSIONED_RECORD"], domain: "pack", audience: "institution"}
	pack_activation:         {phenotypes: ["EVENT", "SERVICE_CAPABILITY"], domain: "pack", audience: "institution"}
	pack_demand:             {phenotypes: ["OBLIGATION", "ECONOMIC_OFFER"], domain: "pack", audience: "institution"}

	// ---- music: the rights-bearing chain
	music_work:              {phenotypes: ["VERSIONED_RECORD", "DOCUMENT"], domain: "music", audience: "public"}
	music_recording:         {phenotypes: ["MEDIA_ASSET", "VERSIONED_RECORD"], domain: "music", audience: "public"}
	music_release:           {phenotypes: ["MEDIA_ASSET", "ECONOMIC_OFFER"], domain: "music", audience: "market"}
	music_release_track:     {phenotypes: ["MEDIA_ASSET", "VERSIONED_RECORD"], domain: "music", audience: "public"}
	music_credit:            {phenotypes: ["IDENTITY_CREDENTIAL", "VERSIONED_RECORD"], domain: "music", audience: "public"}
	music_right:             {phenotypes: ["OBLIGATION", "IDENTITY_CREDENTIAL"], domain: "music", audience: "institution"}

	// ---- media: what can actually be played or carried
	media_program:           {phenotypes: ["MEDIA_ASSET"], domain: "media", audience: "community"}
	physical_carrier:        {phenotypes: ["SPATIAL_RESOURCE", "MEDIA_ASSET"], domain: "media", audience: "self"}

	// ---- community: human social bodies
	community_body:          {phenotypes: ["IDENTITY_CREDENTIAL", "DATASET"], domain: "community", audience: "community"}
	community_membership:    {phenotypes: ["IDENTITY_CREDENTIAL", "OBLIGATION"], domain: "community", audience: "community"}
	community_post:          {phenotypes: ["EVENT", "DOCUMENT"], domain: "community", audience: "community"}
	community_reaction:      {phenotypes: ["EVENT"], domain: "community", audience: "community"}

	// ---- site: authored presentation
	site_presentation:       {phenotypes: ["DOCUMENT", "VERSIONED_RECORD"], domain: "site", audience: "public"}
	site_program:            {phenotypes: ["SERVICE_CAPABILITY", "DOCUMENT"], domain: "site", audience: "public"}
	site_page:               {phenotypes: ["DOCUMENT", "VERSIONED_RECORD"], domain: "site", audience: "public"}
	site_nav:                {phenotypes: ["DOCUMENT", "DATASET"], domain: "site", audience: "public"}
	site_door:               {phenotypes: ["SERVICE_CAPABILITY", "DOCUMENT"], domain: "site", audience: "public"}
	site_fragment:           {phenotypes: ["DOCUMENT", "MEDIA_ASSET"], domain: "site", audience: "public"}
	site_fragment_migration: {phenotypes: ["VERSIONED_RECORD", "EVENT"], domain: "site", audience: "institution"}
	semantic_slot:           {phenotypes: ["SERVICE_CAPABILITY", "DOCUMENT"], domain: "site", audience: "public"}
	territory_presentation:  {phenotypes: ["DOCUMENT", "SPATIAL_RESOURCE"], domain: "site", audience: "public"}
	world:                   {phenotypes: ["LIVE_ENTITY", "EVENT"], domain: "site", audience: "public"}
	world_fragment:          {phenotypes: ["DOCUMENT", "MEDIA_ASSET"], domain: "site", audience: "public"}
	application_binding:     {phenotypes: ["SERVICE_CAPABILITY", "DOCUMENT"], domain: "site", audience: "public"}
	experience_stage:        {phenotypes: ["SERVICE_CAPABILITY", "EVENT"], domain: "site", audience: "public"}
	presentation_template:   {phenotypes: ["DOCUMENT", "DATASET"], domain: "site", audience: "public"}

	// ---- surface: interaction grammar over presentation
	surface_pattern:         {phenotypes: ["SERVICE_CAPABILITY", "DOCUMENT"], domain: "surface", audience: "public"}
	surface_renderer:        {phenotypes: ["SERVICE_CAPABILITY", "DOCUMENT"], domain: "surface", audience: "public"}
	capability_program:      {phenotypes: ["SERVICE_CAPABILITY", "OBLIGATION"], domain: "surface", audience: "public"}

	// ---- external: what the outside world publishes in
	external_resource:       {phenotypes: ["SERVICE_CAPABILITY", "DATASET"], domain: "external", audience: "public"}
	internet_source:         {phenotypes: ["SERVICE_CAPABILITY", "DATASET"], domain: "external", audience: "public"}
	publication_occurrence:  {phenotypes: ["EVENT", "DOCUMENT"], domain: "external", audience: "public"}
	source_collection:       {phenotypes: ["DATASET", "DOCUMENT"], domain: "external", audience: "public"}

	// ---- spatial
	spatial_observation:     {phenotypes: ["SPATIAL_RESOURCE", "MEASUREMENT_STREAM"], domain: "spatial", audience: "institution"}
	coverage_volume:         {phenotypes: ["FIELD_COVERAGE", "SPATIAL_RESOURCE"], domain: "spatial", audience: "institution"}

	// ---- governance: what is held back and why
	content_flag:            {phenotypes: ["OBLIGATION", "EVENT"], domain: "governance", audience: "institution"}
	content_screen:          {phenotypes: ["OBLIGATION", "DOCUMENT"], domain: "governance", audience: "institution"}
	twin_policy:             {phenotypes: ["OBLIGATION", "IDENTITY_CREDENTIAL"], domain: "governance", audience: "institution"}

	// ---- governance: where pure work may run, and on what
	//
	// These three are the pure-execution tier's own declarations, and they are
	// governance rather than substrate for a reason: each one states what the
	// estate permits rather than what a machine can do. A compute_site is a
	// clearance before it is a capability -- having every reducer installed
	// does not make a machine allowed to see institution-private state -- and
	// an execution_profile is the estate's claim about a backend, kept
	// deliberately apart from whether that binary is on PATH in any one run.
	execution_profile:       {phenotypes: ["SERVICE_CAPABILITY", "VERSIONED_RECORD"], domain: "governance", audience: "institution"}
	compute_site:            {phenotypes: ["SERVICE_CAPABILITY", "OBLIGATION"], domain: "governance", audience: "institution"}
	representation_profile:  {phenotypes: ["DATASET", "VERSIONED_RECORD"], domain: "governance", audience: "institution"}

	// ---- contract: what may be born or referenced
	object_contract:         {phenotypes: ["OBLIGATION", "DOCUMENT"], domain: "contract", audience: "institution"}
	reference_contract:      {phenotypes: ["OBLIGATION", "DOCUMENT"], domain: "contract", audience: "institution"}

	// ---- frappe lineages
	frappe_app:              {phenotypes: ["SERVICE_CAPABILITY", "VERSIONED_RECORD"], domain: "frappe", audience: "institution"}
	course_lesson:           {phenotypes: ["DOCUMENT", "MEDIA_ASSET"], domain: "frappe", audience: "group"}
	wiki_page:               {phenotypes: ["DOCUMENT", "VERSIONED_RECORD"], domain: "frappe", audience: "group"}

	// ---- cms
	cms_article:             {phenotypes: ["DOCUMENT", "VERSIONED_RECORD"], domain: "cms", audience: "public"}
	cms_generation:          {phenotypes: ["VERSIONED_RECORD", "EVENT"], domain: "cms", audience: "institution"}
	design_fingerprint:      {phenotypes: ["DATASET", "VERSIONED_RECORD"], domain: "cms", audience: "institution"}

	// ---- added 2026-10-10: the 42 content types that carried rows and no
	// declared facts. Each says what the record is and nothing about what it
	// permits. Where a type records something observed, declared or held, it is
	// given the narrower phenotype on purpose: an observed offer is filed under
	// "external" and is not the estate's offer; a role, a need and a requirement
	// are documents and not obligations anyone accepted or capabilities anyone
	// can run; a session or a presence is an event that was recorded and not a
	// thing that is live.

	// ---- governance: decisions, proposals, held messages, programmes
	governance_decision:     {phenotypes: ["EVENT", "VERSIONED_RECORD"], domain: "governance", audience: "community"}
	governance_proposal:     {phenotypes: ["DOCUMENT", "VERSIONED_RECORD"], domain: "governance", audience: "community"}
	governance_vote:         {phenotypes: ["EVENT", "DOCUMENT"], domain: "governance", audience: "community"}
	outbound_message:        {phenotypes: ["DOCUMENT", "VERSIONED_RECORD"], domain: "governance", audience: "institution"}
	program_instance:        {phenotypes: ["DOCUMENT", "VERSIONED_RECORD"], domain: "governance", audience: "institution"}
	program_requirement:     {phenotypes: ["DOCUMENT", "VERSIONED_RECORD"], domain: "governance", audience: "institution"}
	program_template:        {phenotypes: ["DOCUMENT", "VERSIONED_RECORD"], domain: "governance", audience: "institution"}
	project_support:         {phenotypes: ["EVENT", "DOCUMENT"], domain: "governance", audience: "community"}
	role_assignment:         {phenotypes: ["VERSIONED_RECORD", "DOCUMENT"], domain: "governance", audience: "institution"}

	// ---- contract: what the estate itself declares or has been asked
	assent_signature:        {phenotypes: ["DOCUMENT", "VERSIONED_RECORD"], domain: "contract", audience: "institution"}
	bounty:                  {phenotypes: ["DOCUMENT", "VERSIONED_RECORD"], domain: "contract", audience: "market"}
	destination_release:     {phenotypes: ["EVENT", "VERSIONED_RECORD"], domain: "contract", audience: "institution"}
	funding_work_package:    {phenotypes: ["DOCUMENT", "VERSIONED_RECORD"], domain: "contract", audience: "institution"}
	product_identity:        {phenotypes: ["VERSIONED_RECORD", "DATASET"], domain: "contract", audience: "market"}
	project:                 {phenotypes: ["DOCUMENT", "VERSIONED_RECORD"], domain: "contract", audience: "community"}
	proposal_contract:       {phenotypes: ["DOCUMENT", "VERSIONED_RECORD"], domain: "contract", audience: "institution"}
	role_program:            {phenotypes: ["DOCUMENT", "VERSIONED_RECORD"], domain: "contract", audience: "institution"}
	service_lead:            {phenotypes: ["EVENT", "DOCUMENT"], domain: "contract", audience: "institution"}
	team_need:               {phenotypes: ["DOCUMENT", "VERSIONED_RECORD"], domain: "contract", audience: "community"}

	// ---- external: what was observed of somebody else
	availability_observation: {phenotypes: ["MEASUREMENT_STREAM", "VERSIONED_RECORD"], domain: "external", audience: "market"}
	discovery_candidate:     {phenotypes: ["DATASET", "DOCUMENT"], domain: "external", audience: "institution"}
	discovery_set:           {phenotypes: ["DATASET", "DOCUMENT"], domain: "external", audience: "institution"}
	external_participation_carrier: {phenotypes: ["DATASET", "DOCUMENT"], domain: "external", audience: "institution"}
	external_provider_connection: {phenotypes: ["VERSIONED_RECORD", "DOCUMENT"], domain: "external", audience: "institution"}
	merchant_offer:          {phenotypes: ["ECONOMIC_OFFER", "VERSIONED_RECORD"], domain: "external", audience: "market"}
	price_observation:       {phenotypes: ["MEASUREMENT_STREAM", "VERSIONED_RECORD"], domain: "external", audience: "market"}
	service_offer:           {phenotypes: ["ECONOMIC_OFFER", "DOCUMENT"], domain: "external", audience: "market"}
	source_release_delta:    {phenotypes: ["VERSIONED_RECORD", "EVENT"], domain: "external", audience: "institution"}

	// ---- household: one person's or household's own records
	household:               {phenotypes: ["VERSIONED_RECORD", "DATASET"], domain: "household", audience: "self"}
	household_opportunity:   {phenotypes: ["DOCUMENT", "VERSIONED_RECORD"], domain: "household", audience: "self"}
	household_opportunity_save: {phenotypes: ["EVENT", "VERSIONED_RECORD"], domain: "household", audience: "self"}
	institution_connection:  {phenotypes: ["VERSIONED_RECORD", "DOCUMENT"], domain: "household", audience: "self"}
	saved_item:              {phenotypes: ["VERSIONED_RECORD", "DOCUMENT"], domain: "household", audience: "self"}

	// ---- community, media, site, surface, pack
	community_presence:      {phenotypes: ["EVENT", "VERSIONED_RECORD"], domain: "community", audience: "community"}
	occurrence_follow:       {phenotypes: ["EVENT", "VERSIONED_RECORD"], domain: "community", audience: "self"}
	live_session:            {phenotypes: ["EVENT", "VERSIONED_RECORD"], domain: "media", audience: "community"}
	program_binding:         {phenotypes: ["DOCUMENT", "VERSIONED_RECORD"], domain: "site", audience: "institution"}
	service_site:            {phenotypes: ["DOCUMENT", "VERSIONED_RECORD"], domain: "site", audience: "public"}
	surface_interaction_profile: {phenotypes: ["DOCUMENT", "VERSIONED_RECORD"], domain: "surface", audience: "public"}
	surface_scene:           {phenotypes: ["DOCUMENT", "VERSIONED_RECORD"], domain: "surface", audience: "public"}
	estate_version:          {phenotypes: ["VERSIONED_RECORD", "DATASET"], domain: "pack", audience: "institution"}
	version_path:            {phenotypes: ["VERSIONED_RECORD", "DATASET"], domain: "pack", audience: "institution"}
}
