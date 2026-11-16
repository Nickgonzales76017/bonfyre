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
]
