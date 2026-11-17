package control

// Where each fact an organ produces reaches a person, declared once.
//
// The atlas counted facts that organs produce and nobody reads
// (fact_orphan_produced). A row here names one such fact, the organ that
// produces it, how it is read, who may see it and where. Two kinds of row:
//
//   page   the fact has no page yet. One renderer in WebAddress
//          (src/fact_surface_web.h) draws it at /x/<slug> from a declared
//          store with one read-only SELECT, bounded in rows, bytes and time.
//   route  the fact already shows on a declared web route; the row records
//          that contract so the atlas stops calling the fact unread.
//
// cmd/BonfyreWebAddress/tools/compile_fact_surfaces.sh compiles this file to
// generated/native_fact_surfaces.h (the renderer's table) and to
// architecture/generated/fact-surfaces.yaff (the consumer and its
// interactions), so a row changes the page and the atlas together.
//
// Authority: public rows only for facts already public (a CMS type whose
// content-type-facts audience is public or market, or a page the estate
// already serves to anyone). member rows need a signed-in session; admin
// rows need the bonfyre admin grant, the same check /studio/admin makes.
// Every row is a read. Nothing here submits, publishes, pays or changes a
// record, and a fact shown is not a claim that it is current or verified.

#FactAudience: "public" | "member" | "admin"

// The ui_tree shape the renderer draws: ui_table over the declared columns.
// A record has no page of its own here, so no shape links rows anywhere.
#FactPresentation: "table"

// Plain words a person reads: no machine vocabulary.
#FactWords: =~"^[A-Z][A-Za-z0-9 ,.'()&/-]*$"

#FactStoreRead: {
	// A store declared in schemas/storage/stores and opened by
	// cmd/BonfyreWebAddress (the compiler refuses any other).
	store: =~"^[a-z][a-z0-9_]*$"
	// Fallback file under $HOME when the store's variable is unset.
	home_file?: =~"^\\.[A-Za-z0-9._/-]+$"
	// One read-only SELECT; ?1 is bound to the row limit.
	sql: =~"^SELECT [^;]*LIMIT \\?1$"
	// How many records there are, for the figure above the table. A bare
	// count(*) is one SQLite opcode the deadline cannot interrupt, so a large
	// table counts through a WHERE and shows "Too many to count quickly".
	count_sql: =~"^SELECT count\\(\\*\\) FROM [^;]*$"
	// Column labels, one per selected column, in order.
	columns: [#FactWords, ...#FactWords]
	limit: int & >=1 & <=200 | *50
}

#FactSurface: {
	fact: =~"^[A-Z][A-Za-z0-9]*$"
	// The architecture id (architecture/**.yaff) that produces the fact.
	owner:        =~"^[a-z0-9][a-z0-9-]*$"
	audience:     #FactAudience
	title:        #FactWords
	lede:         #FactWords
	empty:        #FactWords
	presentation: #FactPresentation | *"table"
	// Columns of the figure grid per viewport (numeric presentation).
	layout: {desktop: int & >=1 & <=4 | *3, phone: int & >=1 & <=2 | *1}
	{
		slug: =~"^[a-z][a-z0-9-]*$"
		read: #FactStoreRead
	} | {
		// An existing path in web-routes.cue whose owner shows the fact.
		route: =~"^/[A-Za-z0-9._/-]*$"
		// The part of that page that shows it.
		slot: #FactWords
		// estate: a path of web-routes.cue; site: a path every CMS site
		// answers through its own dispatcher (bf_site_mcp_serve).
		on: *"estate" | "site"
	}
}

factSurfaces: [...#FactSurface] & [
	// ---- commerce observations: market-audience CMS types, public
	{
		fact: "PriceObservation", owner: "commerce-observation-provider", audience: "public"
		title: "Prices seen", lede: "Prices the estate has read from sellers, with when each was seen."
		empty: "No prices have been recorded yet."
		slug:  "prices"
		read: {
			store:     "cms"
			sql:       "SELECT COALESCE(i.title, p.product_id), p.amount, p.currency, p.kind, p.observed_at FROM price_observation p LEFT JOIN product_identity i ON i.product_id = p.product_id AND i.status = 'published' AND i.namespace = 'root' WHERE p.status = 'published' AND p.namespace = 'root' ORDER BY p.observed_at DESC LIMIT ?1"
			count_sql: "SELECT count(*) FROM price_observation WHERE status = 'published' AND namespace = 'root'"
			columns: ["Product", "Amount", "Currency", "Kind", "Seen"]
		}
	},
	{
		fact: "MerchantOffer", owner: "commerce-observation-provider", audience: "public"
		title: "Offers from sellers", lede: "Who offers each product, in what condition and how it is delivered."
		empty: "No offers have been recorded yet."
		slug:  "offers"
		read: {
			store:     "cms"
			sql:       "SELECT COALESCE(i.title, o.product_id), o.merchant, o.seller, o.condition, o.fulfillment, o.observed_at FROM merchant_offer o LEFT JOIN product_identity i ON i.product_id = o.product_id AND i.status = 'published' AND i.namespace = 'root' WHERE o.status = 'published' AND o.namespace = 'root' ORDER BY o.observed_at DESC LIMIT ?1"
			count_sql: "SELECT count(*) FROM merchant_offer WHERE status = 'published' AND namespace = 'root'"
			columns: ["Product", "Merchant", "Seller", "Condition", "Delivery", "Seen"]
		}
	},
	{
		fact: "ProductIdentity", owner: "commerce-observation-provider", audience: "public"
		title: "Products", lede: "Products the estate can tell apart, by brand and the codes printed on them."
		empty: "No products have been recorded yet."
		slug:  "products"
		read: {
			store:     "cms"
			sql:       "SELECT title, brand, gtin, sku, observed_at FROM product_identity WHERE status = 'published' AND namespace = 'root' ORDER BY observed_at DESC LIMIT ?1"
			count_sql: "SELECT count(*) FROM product_identity WHERE status = 'published' AND namespace = 'root'"
			columns: ["Product", "Brand", "Barcode", "Seller code", "Seen"]
		}
	},
	// ---- item depth: the facets item pages already show anyone
	{
		fact: "ItemFacet", owner: "detail-depth", audience: "public"
		title: "Item details", lede: "Details read from items in the reading archive, in item order."
		empty: "No item details have been gathered yet."
		slug:  "item-details"
		read: {
			store:     "detail_depth"
			home_file: ".bonfyre/estate-local/detail-depth.db"
			sql:       "SELECT key, facet, value FROM item_facet LIMIT ?1"
			count_sql: "SELECT count(*) FROM item_facet WHERE facet IS NOT NULL"
			columns: ["Item", "Detail", "Value"]
		}
	},
	// ---- institution records: admin only
	{
		fact: "RoleProgram", owner: "role-program-compiler", audience: "admin"
		title: "Roles", lede: "The roles the estate has written down and when each last changed."
		empty: "No roles have been written down yet."
		slug:  "roles"
		read: {
			store:     "cms"
			sql:       "SELECT label, role_id, updated_at FROM role_program WHERE status = 'published' AND namespace = 'root' ORDER BY updated_at DESC LIMIT ?1"
			count_sql: "SELECT count(*) FROM role_program WHERE status = 'published' AND namespace = 'root'"
			columns: ["Role", "Name", "Changed"]
		}
	},
	{
		fact: "SourceChannel", owner: "source-frontier", audience: "admin"
		title: "Source channels", lede: "Feeds and pages the estate has found, most recently found first."
		empty: "No source channels have been found yet."
		slug:  "source-channels"
		read: {
			store:     "source_frontier"
			home_file: ".bonfyre/estate-local/frontier.db"
			sql:       "SELECT COALESCE(NULLIF(title, ''), address), address, protocol, state, last_seen FROM channel ORDER BY rowid DESC LIMIT ?1"
			count_sql: "SELECT count(*) FROM channel"
			columns: ["Channel", "Address", "Protocol", "State", "Last seen"]
		}
	},
	{
		fact: "SourceGap", owner: "source-frontier", audience: "admin"
		title: "Source gaps", lede: "What the source frontier could not read yet, most recently found first."
		empty: "No gaps are open."
		slug:  "source-gaps"
		read: {
			store:     "source_frontier"
			home_file: ".bonfyre/estate-local/frontier.db"
			sql:       "SELECT kind, state, channel_id, resolutions, last_seen FROM gap ORDER BY rowid DESC LIMIT ?1"
			count_sql: "SELECT count(*) FROM gap"
			columns: ["Kind", "State", "Channel", "Tries", "Last seen"]
		}
	},
	{
		fact: "SourceContributionReceipt", owner: "source-frontier", audience: "admin"
		title: "Items brought in", lede: "How many items each channel brought in, by day."
		empty: "No channel has brought anything in yet."
		slug:  "source-contributions"
		read: {
			store:     "source_frontier"
			home_file: ".bonfyre/estate-local/frontier.db"
			sql:       "SELECT day, route, channel_id, items FROM contribution ORDER BY day DESC LIMIT ?1"
			count_sql: "SELECT count(*) FROM contribution"
			columns: ["Day", "Route", "Channel", "Items"]
		}
	},
	{
		fact: "MatrixSubject", owner: "bonfyre-matrix", audience: "admin"
		title: "Subjects", lede: "The people, places and things the estate has an identity for."
		empty: "No subjects are known yet."
		slug:  "subjects"
		read: {
			store:     "matrix"
			sql:       "SELECT subject_id, subject_kind, evidence_ref FROM matrix_subjects ORDER BY rowid DESC LIMIT ?1"
			count_sql: "SELECT count(*) FROM matrix_subjects"
			columns: ["Subject", "Kind", "Evidence"]
		}
	},
	{
		fact: "CanonicalDomain", owner: "bonfyre-matrix", audience: "admin"
		title: "Domains", lede: "Each host name the estate answers for, the institution it belongs to and its territory."
		empty: "No domains are known yet."
		slug:  "domains"
		read: {
			store:     "matrix"
			sql:       "SELECT hostname, institution_id, territory, default_scope, COALESCE(canonical_hostname, '') FROM matrix_domains ORDER BY rowid DESC LIMIT ?1"
			count_sql: "SELECT count(*) FROM matrix_domains"
			columns: ["Host", "Institution", "Territory", "Scope", "Main host"]
		}
	},
	{
		fact: "LatentChart", owner: "latent-coordinate-atlas", audience: "admin"
		title: "Similarity charts", lede: "The charts the similarity atlas places things on."
		empty: "No charts have been built yet."
		slug:  "similarity-charts"
		read: {
			store:     "latent_atlas"
			home_file: ".bonfyre/estate-local/atlas.db"
			sql:       "SELECT chart_id, family, dims, version, encoder FROM chart ORDER BY rowid DESC LIMIT ?1"
			count_sql: "SELECT count(*) FROM chart"
			columns: ["Chart", "Family", "Dimensions", "Version", "Encoder"]
		}
	},
	{
		fact: "NeighbourhoodSnapshot", owner: "latent-coordinate-atlas", audience: "admin"
		title: "Neighbourhood snapshots", lede: "Saved views of what sits near what on each chart."
		empty: "No snapshots have been taken yet."
		slug:  "neighbourhoods"
		read: {
			store:     "latent_atlas"
			home_file: ".bonfyre/estate-local/atlas.db"
			sql:       "SELECT snapshot_id, logical, lens, chart_id, created_at FROM snapshot ORDER BY created_at DESC LIMIT ?1"
			count_sql: "SELECT count(*) FROM snapshot"
			columns: ["Snapshot", "Subject", "Lens", "Chart", "Taken"]
		}
	},
	{
		fact: "CommerceStage", owner: "commerce-chain", audience: "admin"
		title: "Exchange stages", lede: "Where each exchange has got to, and who moved it there."
		empty: "No exchange has started yet."
		slug:  "exchange-stages"
		read: {
			store:     "commerce"
			home_file: ".bonfyre/estate-local/commerce.db"
			sql:       "SELECT exchange, stage, owner, at FROM commerce_progress ORDER BY at DESC LIMIT ?1"
			count_sql: "SELECT count(*) FROM commerce_progress"
			columns: ["Exchange", "Stage", "Moved by", "When"]
		}
	},
	{
		fact: "ResourceGrant", owner: "resource-admission", audience: "admin"
		title: "Space and memory grants", lede: "What each job was allowed to use, and when it gave it back."
		empty: "Nothing has been granted yet."
		slug:  "resource-grants"
		read: {
			store:     "admission"
			home_file: ".bonfyre/admission.db"
			sql:       "SELECT plane, kind, estimated_bytes, granted_at, COALESCE(released_at, '') FROM resource_grants ORDER BY granted_at DESC LIMIT ?1"
			count_sql: "SELECT count(*) FROM resource_grants"
			columns: ["Job", "Kind", "Bytes", "Granted", "Released"]
		}
	},

	// ---- commerce observations, continued: market-audience CMS types, public
	{
		fact: "AvailabilityObservation", owner: "commerce-observation-provider", audience: "public"
		title: "Offer watch", lede: "What shops said was on the shelf, and when they said it."
		empty: "No shop notes yet."
		slug:  "availability"
		read: {
			store:     "cms"
			sql:       "SELECT observation_id, offer_id, state, quantity_stated, source_ref, observed_at FROM availability_observation WHERE status = 'published' AND namespace = 'root' ORDER BY observed_at DESC LIMIT ?1"
			count_sql: "SELECT count(*) FROM availability_observation WHERE status = 'published' AND namespace = 'root'"
			columns: ["Note", "Offer", "State", "Amount said", "Source", "Seen"]
		}
	},
	// ---- commons reading: already read while composing public pages
	{
		fact: "CommonsReadingRow", owner: "commons-knowledge-reading", audience: "public"
		title: "Sources", lede: "The outside feeds this place reads, and what each one says."
		empty: "No feeds listed yet."
		slug:  "commons-reading"
		read: {
			store:     "cms"
			sql:       "SELECT label, says, url, state, last_seen FROM internet_source WHERE status = 'published' AND namespace = 'root' ORDER BY id DESC LIMIT ?1"
			count_sql: "SELECT count(*) FROM internet_source WHERE status = 'published' AND namespace = 'root'"
			columns: ["Source", "Says", "Link", "State", "Last seen"]
		}
	},
	// ---- broadcast: published shows are public, drafts are admin only
	{
		fact: "BroadcastProgramme", owner: "broadcast", audience: "public"
		title: "Shows", lede: "Ready audio and video with words to read along."
		empty: "No shows ready yet."
		slug:  "broadcast-programmes"
		read: {
			store:     "cms"
			sql:       "SELECT program_id, media_kind, duration_ms, transcript_ref, updated_at FROM media_program WHERE status = 'published' AND namespace = 'root' AND publication_state = 'published' ORDER BY updated_at DESC LIMIT ?1"
			count_sql: "SELECT count(*) FROM media_program WHERE status = 'published' AND namespace = 'root' AND publication_state = 'published'"
			columns: ["Show", "Kind", "Length", "Words", "Changed"]
		}
	},
	{
		fact: "BroadcastRender", owner: "broadcast", audience: "admin"
		title: "Renders", lede: "Fresh renders and drafts on the way to a show."
		empty: "No renders yet."
		slug:  "broadcast-renders"
		read: {
			store:     "cms"
			sql:       "SELECT program_id, media_kind, status, publication_state, duration_ms, updated_at FROM media_program WHERE namespace = 'root' ORDER BY updated_at DESC LIMIT ?1"
			count_sql: "SELECT count(*) FROM media_program WHERE namespace = 'root'"
			columns: ["Show", "Kind", "Row state", "Release stage", "Length", "Changed"]
		}
	},
	// ---- institution records, continued: admin only
	{
		fact: "RoleAssignment", owner: "role-program-compiler", audience: "admin"
		title: "Roles taken", lede: "Who has taken on which role, where, and since when."
		empty: "No roles taken on yet."
		slug:  "role-assignments"
		read: {
			store:     "cms"
			sql:       "SELECT assignment_id, actor_subject, role_program, context_scope, state, assumed_at FROM role_assignment WHERE status = 'published' AND namespace = 'root' ORDER BY assumed_at DESC LIMIT ?1"
			count_sql: "SELECT count(*) FROM role_assignment WHERE status = 'published' AND namespace = 'root'"
			columns: ["Name", "Who", "Role", "Scope", "State", "Since"]
		}
	},
	{
		fact: "SpatialObservation", owner: "spatial-observation", audience: "admin"
		title: "Spotted", lede: "Where people and things were seen, and how the fix was made."
		empty: "Nothing spotted yet."
		slug:  "spatial-observations"
		read: {
			store:     "cms"
			sql:       "SELECT observation_id, subject_address, geometry_kind, observation_mode, mobility, scale, region, cell, observed_at FROM spatial_observation WHERE status = 'published' AND namespace = 'root' ORDER BY observed_at DESC LIMIT ?1"
			count_sql: "SELECT count(*) FROM spatial_observation WHERE status = 'published' AND namespace = 'root'"
			columns: ["Note", "Subject", "Shape", "Mode", "Mobility", "Scale", "Region", "Cell", "Seen"]
		}
	},
	{
		fact: "CoverageVolume", owner: "coverage-volume", audience: "admin"
		title: "Reach", lede: "What each place can see or serve from where it stands."
		empty: "No reach drawn yet."
		slug:  "coverage"
		read: {
			store:     "cms"
			sql:       "SELECT coverage_id, owner_subject, scale, region, cell, quality, derivation, valid_frontier FROM coverage_volume WHERE status = 'published' AND namespace = 'root' ORDER BY updated_at DESC LIMIT ?1"
			count_sql: "SELECT count(*) FROM coverage_volume WHERE status = 'published' AND namespace = 'root'"
			columns: ["Reach", "Owner", "Scale", "Region", "Cell", "Quality", "Drawn from", "Good until"]
		}
	},
	// ---- meter: live usage and prices, per-row detail is admin only.
	// UsageOccurrence, EconomicMeasure and DerivationEconomics read the live
	// meter successor (events joined to rate), not the archived finance
	// tables; priced rows only where a measure exists.
	{
		fact: "UsageOccurrence", owner: "financial-metabolism", audience: "admin"
		title: "Metered use", lede: "Each thing the estate counted, with when it happened and what it cost."
		empty: "Nothing has been metered yet."
		slug:  "metered-use"
		read: {
			store:     "meter"
			sql:       "SELECT timestamp, provider, op, bytes, duration_ms, cost FROM events ORDER BY timestamp DESC LIMIT ?1"
			count_sql: "SELECT count(*) FROM events"
			columns: ["When", "Provider", "Action", "Bytes", "Time (ms)", "Cost"]
		}
	},
	{
		fact: "EconomicMeasure", owner: "financial-metabolism", audience: "admin"
		title: "Priced use", lede: "Metered use joined to the listed price used for it."
		empty: "No priced use is recorded yet."
		slug:  "priced-use"
		read: {
			store:     "meter"
			sql:       "SELECT e.timestamp, e.provider, e.op, e.cost, r.amount, r.unit, r.currency, r.effective_at, r.evidence_ref FROM events e LEFT JOIN rate r ON r.id = e.rate_id WHERE e.cost IS NOT NULL ORDER BY e.timestamp DESC LIMIT ?1"
			count_sql: "SELECT count(*) FROM events WHERE cost IS NOT NULL"
			columns: ["When", "Provider", "Action", "Cost", "Rate amount", "Per", "Money", "Rate from", "Backing note"]
		}
	},
	{
		fact: "DerivationEconomics", owner: "financial-metabolism", audience: "admin"
		title: "Accrued costs", lede: "Use that has a listed price, with what each item came to."
		empty: "No use has a listed price yet."
		slug:  "accrued-costs"
		read: {
			store:     "meter"
			sql:       "SELECT e.timestamp, e.provider, e.op, e.cost, r.currency, r.evidence_ref, e.rate_id FROM events e JOIN rate r ON r.id = e.rate_id WHERE e.cost IS NOT NULL ORDER BY e.timestamp DESC LIMIT ?1"
			count_sql: "SELECT count(*) FROM events WHERE cost IS NOT NULL"
			columns: ["When", "Provider", "Action", "Cost", "Money", "Backing note", "Rate number"]
		}
	},
	// ---- admitted rates are published on every public territory already
	{
		fact: "RateAdmissionAttempt", owner: "rate-discovery", audience: "public"
		title: "Listed prices", lede: "Prices providers stated that this estate uses, with the note that backs each one."
		empty: "No provider price has been listed yet."
		slug:  "listed-prices"
		read: {
			store:     "meter"
			sql:       "SELECT provider, op, unit, amount, currency, effective_at, evidence_ref, observed_at FROM rate ORDER BY effective_at DESC, id DESC LIMIT ?1"
			count_sql: "SELECT count(*) FROM rate"
			columns: ["Provider", "Action", "Per", "Amount", "Money", "In force from", "Backing note", "Noted when"]
		}
	},
	{
		fact: "ProviderModelUsageObservation", owner: "physical-derivation", audience: "admin"
		title: "Model use rows", lede: "Token counts the meter took in from agent exports, one row per usage line."
		empty: "No agent usage has been taken in yet."
		slug:  "model-usage-receipts"
		read: {
			store:     "meter"
			sql:       "SELECT timestamp, provider, model, session_ref, uncached_input_tokens, cached_input_tokens, cache_write_tokens, output_tokens, reasoning_tokens, counter_basis FROM events WHERE op = 'model.tokens' ORDER BY timestamp DESC LIMIT ?1"
			count_sql: "SELECT count(*) FROM events WHERE op = 'model.tokens'"
			columns: ["When", "Provider", "Model", "Session", "Fresh input", "Cached input", "Cache writes", "Output", "Reasoning", "Count basis"]
		}
	},
	// ---- source frontier, similarity atlas and matrix: admin only
	{
		fact: "SourceAttentionPlan", owner: "source-frontier", audience: "admin"
		title: "Source attention", lede: "How closely each source is being watched, and why."
		empty: "No source has a watch level yet."
		slug:  "source-attention"
		read: {
			store:     "source_frontier"
			home_file: ".bonfyre/estate-local/frontier.db"
			sql:       "SELECT root_id, level, reason, updated FROM attention ORDER BY updated DESC LIMIT ?1"
			count_sql: "SELECT count(*) FROM attention"
			columns: ["Source", "Watch level", "Why", "Updated"]
		}
	},
	{
		fact: "TypedAnchor", owner: "latent-coordinate-atlas", audience: "admin"
		title: "Typed links", lede: "What each thing is tied to, the tie used and the proof behind it."
		empty: "No typed links have been written yet."
		slug:  "typed-anchors"
		read: {
			store:     "latent_atlas"
			home_file: ".bonfyre/estate-local/atlas.db"
			sql:       "SELECT src, relation, dst, evidence FROM relation WHERE relation <> 'member_of' ORDER BY src, relation, dst LIMIT ?1"
			count_sql: "SELECT count(*) FROM relation WHERE relation <> 'member_of'"
			columns: ["From", "Tie", "To", "Evidence"]
		}
	},
	{
		fact: "StandingAddress", owner: "bonfyre-matrix", audience: "admin"
		title: "Standing addresses", lede: "Saved address filters, and the subjects each one covers."
		empty: "No standing addresses have been saved yet."
		slug:  "standing-addresses"
		read: {
			store:     "matrix"
			sql:       "SELECT address, COALESCE(subject_kind, ''), COALESCE(territory, ''), COALESCE(scope, ''), COALESCE(world, ''), COALESCE(representation, '') FROM matrix_standing_addresses ORDER BY rowid DESC LIMIT ?1"
			count_sql: "SELECT count(*) FROM matrix_standing_addresses"
			columns: ["Address", "Kind", "Territory", "Scope", "World", "Format"]
		}
	},
	{
		fact: "InstitutionDelegation", owner: "bonfyre-matrix", audience: "admin"
		title: "Institution delegations", lede: "Which institution answers to which, who allowed it and why."
		empty: "No institution answers to another yet."
		slug:  "delegations"
		read: {
			store:     "matrix"
			sql:       "SELECT parent_institution_id, child_institution_id, authority_actor, authority_ref, purpose FROM matrix_delegations ORDER BY rowid DESC LIMIT ?1"
			count_sql: "SELECT count(*) FROM matrix_delegations"
			columns: ["Parent", "Child", "Decided by", "Reference", "Why"]
		}
	},
	{
		fact: "CanonicalHost", owner: "web-address-edge", audience: "admin"
		title: "Main hosts", lede: "Each host name that points at another host as its main one."
		empty: "No host points at another yet."
		slug:  "canonical-hosts"
		read: {
			store:     "matrix"
			sql:       "SELECT hostname, canonical_hostname FROM matrix_domains WHERE canonical_hostname IS NOT NULL ORDER BY rowid DESC LIMIT ?1"
			count_sql: "SELECT count(*) FROM matrix_domains WHERE canonical_hostname IS NOT NULL"
			columns: ["Host", "Main host"]
		}
	},
	{
		fact: "WorldPolicy", owner: "institutional-worlds", audience: "admin"
		title: "Institution worlds", lede: "Which world each institution stands in, and what kind each one is."
		empty: "No institutions have been declared yet."
		slug:  "worlds"
		read: {
			store:     "matrix"
			sql:       "SELECT institution_id, institution_kind, world, evidence_ref FROM matrix_institutions ORDER BY rowid DESC LIMIT ?1"
			count_sql: "SELECT count(*) FROM matrix_institutions"
			columns: ["Institution", "Kind", "World", "Evidence"]
		}
	},
	// ---- facts already shown on a declared route
	{fact: "DeviceKey", owner: "account-device-keys", audience: "member", title: "Your devices", lede: "The device keys you added to this account.", empty: "No devices yet.", route: "/account/devices", slot: "Device list"},
	{fact: "DevicePairing", owner: "account-device-move", audience: "member", title: "Move to a new device", lede: "One-use codes you offered to move this account.", empty: "No move codes yet.", route: "/account/move", slot: "Move codes"},
	{fact: "FeedAgentPolicy", owner: "feed-agent-policy", audience: "member", title: "Agents and feeds", lede: "The rules you set for agents on feeds you keep.", empty: "No feed rules yet.", route: "/account/agents", slot: "Feed rules"},
	{fact: "CarrierSyncRequest", owner: "bluesky-sync-request", audience: "member", title: "Bluesky sync", lede: "Sync requests you asked for and whether they ran.", empty: "No sync requests yet.", route: "/connections/bluesky/sync", slot: "Sync requests"},
	{fact: "FieldDispatch", owner: "field-dispatch", audience: "member", title: "Field visits", lede: "Visits you asked for and visits asked of you.", empty: "No field requests yet.", route: "/dispatch", slot: "Dispatch queue"},
	{fact: "AccountActivityPresentation", owner: "aurekai-activity", audience: "member", title: "Activity", lede: "Work running for your account.", empty: "Nothing is running.", route: "/apps/_activity", slot: "Activity panel"},
	{fact: "GitHubAppConnection", owner: "github-app-connection", audience: "member", title: "GitHub connections", lede: "The GitHub installations connected for your customers.", empty: "No GitHub installation is connected.", route: "/connect/github-app/customers", slot: "Installations"},
	{fact: "ProviderCustomerAccount", owner: "provider-customer-accounts", audience: "member", title: "Provider accounts", lede: "The provider accounts held for your organization.", empty: "No provider account is held yet.", route: "/organizations", slot: "Provider accounts"},
	{fact: "ConnectionSourceCandidate", owner: "connection-source-catalog", audience: "member", title: "Connections you could add", lede: "Sources the catalogue lists as possible connections, not yet checked.", empty: "The catalogue lists nothing yet.", route: "/apps/connections", slot: "Connection catalogue"},
	{fact: "ReleaseApproval", owner: "release-approval", audience: "admin", title: "Release approvals", lede: "Releases waiting for a person to approve them.", empty: "No release is waiting.", route: "/work-board", slot: "Open approvals"},
	{fact: "ForeignOccurrence", owner: "foreign-link-desk", audience: "public", title: "Links and sources", lede: "What the estate has read from other sites.", empty: "Nothing has been read yet.", route: "/sources", slot: "Source desk"},
	{fact: "SiteSearchIndex", owner: "static-semantic-search", audience: "public", title: "Site search", lede: "Each site's own search, built from its published sections.", empty: "This site has no published sections.", route: "/_search/index.bfsi", slot: "Search index", on: "site"},
	{fact: "SearchIndex", owner: "static-semantic-search", audience: "public", title: "Site search", lede: "Each site's own search, built from its published sections.", empty: "This site has no published sections.", route: "/_search/index.bfsi", slot: "Search index", on: "site"},
	{fact: "SearchPassage", owner: "static-semantic-search", audience: "public", title: "Search passages", lede: "The sections a site's search can answer with.", empty: "This site has no published sections.", route: "/_search/passages.json", slot: "Passages", on: "site"},
	{fact: "StaticEmbeddingModel", owner: "static-semantic-search", audience: "public", title: "Search model", lede: "The small model a page uses to read a question.", empty: "The search model is not installed.", route: "/_search/model.bfse", slot: "Model file", on: "site"},
	// ---- action apertures: what this place offers you, already on /_aperture
	{fact: "ActionAperture", owner: "object-grammar", audience: "public", title: "Actions you can take", lede: "The operations this place offers for the thing you are looking at.", empty: "No actions are offered here.", route: "/_aperture", slot: "Aperture list"},
	{fact: "AtomicForm", owner: "object-grammar", audience: "public", title: "One operation form", lede: "The questions one operation asks before it runs.", empty: "No form is open.", route: "/_aperture/form", slot: "Operation form"},
	{fact: "SemanticOperation", owner: "object-grammar", audience: "public", title: "Operation names", lede: "The operations the estate knows by name.", empty: "No operations are named yet.", route: "/_aperture", slot: "Operation names"},
	// ---- front page, identity and item depth: already on /, /about/ and /o/
	{fact: "EventItem", owner: "event-material", audience: "public", title: "What is on", lede: "Events and gatherings the estate has listed.", empty: "Nothing is listed yet.", route: "/", slot: "What is on"},
	{fact: "ComposedPublicPage", owner: "page-composition", audience: "public", title: "Front page", lede: "The front page as composed for anyone visiting.", empty: "The front page is empty.", route: "/", slot: "Composed front page"},
	{fact: "IdentityPage", owner: "identity-page", audience: "public", title: "About this place", lede: "Who runs this place and what it is for.", empty: "No description has been written yet.", route: "/about/", slot: "Identity page"},
	{fact: "PageHistory", owner: "page-history", audience: "public", title: "How this page changed", lede: "Earlier versions of this page and when each changed.", empty: "This page has no earlier versions.", route: "/o/", slot: "How this page changed"},
	{fact: "DetailContinuation", owner: "detail-depth", audience: "public", title: "Where next", lede: "What to read next from this item.", empty: "There is nowhere further to go.", route: "/o/", slot: "Where next"},
	{fact: "EstateAddress", owner: "estate-reference", audience: "public", title: "Reference links", lede: "Where each reference on this page points.", empty: "This page carries no references.", route: "/o/", slot: "Reference links"},
	{fact: "CompiledCalendar", owner: "event-material", audience: "public", title: "Calendar file", lede: "An event as a calendar file for your own calendar.", empty: "No calendar file is offered.", route: "/o/", slot: "Calendar file"},
]
