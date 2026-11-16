/* Generated from schemas/control/fact-surfaces.cue by tools/compile_fact_surfaces.sh. Do not edit. */
#ifndef BONFYRE_NATIVE_FACT_SURFACES_H
#define BONFYRE_NATIVE_FACT_SURFACES_H
#include "store_schema.h"
typedef struct {
    const char *fact, *owner, *audience, *title, *lede, *empty, *presentation;
    const char *slug, *route, *slot, *on;                 /* page rows: slug; route rows: route, slot, on */
    const char *store, *env, *home_file, *sql, *count_sql;
    const char *const *columns; unsigned ncolumns; int limit, cols_desktop, cols_phone;
    BfStoreDurability policy;
} BfFactSurface;
static const char *const bf_fact_columns_0[] = {"Product", "Amount", "Currency", "Kind", "Seen"};
static const char *const bf_fact_columns_1[] = {"Product", "Merchant", "Seller", "Condition", "Delivery", "Seen"};
static const char *const bf_fact_columns_2[] = {"Product", "Brand", "Barcode", "Seller code", "Seen"};
static const char *const bf_fact_columns_3[] = {"Item", "Detail", "Value"};
static const char *const bf_fact_columns_4[] = {"Role", "Name", "Changed"};
static const char *const bf_fact_columns_5[] = {"Channel", "Address", "Protocol", "State", "Last seen"};
static const char *const bf_fact_columns_6[] = {"Kind", "State", "Channel", "Tries", "Last seen"};
static const char *const bf_fact_columns_7[] = {"Day", "Route", "Channel", "Items"};
static const char *const bf_fact_columns_8[] = {"Subject", "Kind", "Evidence"};
static const char *const bf_fact_columns_9[] = {"Host", "Institution", "Territory", "Scope", "Main host"};
static const char *const bf_fact_columns_10[] = {"Chart", "Family", "Dimensions", "Version", "Encoder"};
static const char *const bf_fact_columns_11[] = {"Snapshot", "Subject", "Lens", "Chart", "Taken"};
static const char *const bf_fact_columns_12[] = {"Exchange", "Stage", "Moved by", "When"};
static const char *const bf_fact_columns_13[] = {"Job", "Kind", "Bytes", "Granted", "Released"};
static const BfFactSurface bf_fact_surfaces[] = {
    {"PriceObservation", "commerce-observation-provider", "public", "Prices seen", "Prices the estate has read from sellers, with when each was seen.", "No prices have been recorded yet.", "table",
     "prices", NULL, NULL, NULL,
     "cms", BF_STORE_CMS_DB_ENV, NULL, "SELECT COALESCE(i.title, p.product_id), p.amount, p.currency, p.kind, p.observed_at FROM price_observation p LEFT JOIN product_identity i ON i.product_id = p.product_id AND i.status = 'published' AND i.namespace = 'root' WHERE p.status = 'published' AND p.namespace = 'root' ORDER BY p.observed_at DESC LIMIT ?1", "SELECT count(*) FROM price_observation WHERE status = 'published' AND namespace = 'root'",
     bf_fact_columns_0, 5, 50, 3, 1, BF_STORE_CMS_DB_DURABILITY},
    {"MerchantOffer", "commerce-observation-provider", "public", "Offers from sellers", "Who offers each product, in what condition and how it is delivered.", "No offers have been recorded yet.", "table",
     "offers", NULL, NULL, NULL,
     "cms", BF_STORE_CMS_DB_ENV, NULL, "SELECT COALESCE(i.title, o.product_id), o.merchant, o.seller, o.condition, o.fulfillment, o.observed_at FROM merchant_offer o LEFT JOIN product_identity i ON i.product_id = o.product_id AND i.status = 'published' AND i.namespace = 'root' WHERE o.status = 'published' AND o.namespace = 'root' ORDER BY o.observed_at DESC LIMIT ?1", "SELECT count(*) FROM merchant_offer WHERE status = 'published' AND namespace = 'root'",
     bf_fact_columns_1, 6, 50, 3, 1, BF_STORE_CMS_DB_DURABILITY},
    {"ProductIdentity", "commerce-observation-provider", "public", "Products", "Products the estate can tell apart, by brand and the codes printed on them.", "No products have been recorded yet.", "table",
     "products", NULL, NULL, NULL,
     "cms", BF_STORE_CMS_DB_ENV, NULL, "SELECT title, brand, gtin, sku, observed_at FROM product_identity WHERE status = 'published' AND namespace = 'root' ORDER BY observed_at DESC LIMIT ?1", "SELECT count(*) FROM product_identity WHERE status = 'published' AND namespace = 'root'",
     bf_fact_columns_2, 5, 50, 3, 1, BF_STORE_CMS_DB_DURABILITY},
    {"ItemFacet", "detail-depth", "public", "Item details", "Details read from items in the reading archive, in item order.", "No item details have been gathered yet.", "table",
     "item-details", NULL, NULL, NULL,
     "detail_depth", BF_STORE_WEB_DETAIL_DEPTH_ENV, ".bonfyre/estate-local/detail-depth.db", "SELECT key, facet, value FROM item_facet LIMIT ?1", "SELECT count(*) FROM item_facet WHERE facet IS NOT NULL",
     bf_fact_columns_3, 3, 50, 3, 1, BF_STORE_WEB_DETAIL_DEPTH_LEASE},
    {"RoleProgram", "role-program-compiler", "admin", "Roles", "The roles the estate has written down and when each last changed.", "No roles have been written down yet.", "table",
     "roles", NULL, NULL, NULL,
     "cms", BF_STORE_CMS_DB_ENV, NULL, "SELECT label, role_id, updated_at FROM role_program WHERE status = 'published' AND namespace = 'root' ORDER BY updated_at DESC LIMIT ?1", "SELECT count(*) FROM role_program WHERE status = 'published' AND namespace = 'root'",
     bf_fact_columns_4, 3, 50, 3, 1, BF_STORE_CMS_DB_DURABILITY},
    {"SourceChannel", "source-frontier", "admin", "Source channels", "Feeds and pages the estate has found, most recently found first.", "No source channels have been found yet.", "table",
     "source-channels", NULL, NULL, NULL,
     "source_frontier", BF_STORE_WEB_SOURCE_FRONTIER_ENV, ".bonfyre/estate-local/frontier.db", "SELECT COALESCE(NULLIF(title, ''), address), address, protocol, state, last_seen FROM channel ORDER BY rowid DESC LIMIT ?1", "SELECT count(*) FROM channel",
     bf_fact_columns_5, 5, 50, 3, 1, BF_STORE_WEB_SOURCE_FRONTIER_LEASE},
    {"SourceGap", "source-frontier", "admin", "Source gaps", "What the source frontier could not read yet, most recently found first.", "No gaps are open.", "table",
     "source-gaps", NULL, NULL, NULL,
     "source_frontier", BF_STORE_WEB_SOURCE_FRONTIER_ENV, ".bonfyre/estate-local/frontier.db", "SELECT kind, state, channel_id, resolutions, last_seen FROM gap ORDER BY rowid DESC LIMIT ?1", "SELECT count(*) FROM gap",
     bf_fact_columns_6, 5, 50, 3, 1, BF_STORE_WEB_SOURCE_FRONTIER_LEASE},
    {"SourceContributionReceipt", "source-frontier", "admin", "Items brought in", "How many items each channel brought in, by day.", "No channel has brought anything in yet.", "table",
     "source-contributions", NULL, NULL, NULL,
     "source_frontier", BF_STORE_WEB_SOURCE_FRONTIER_ENV, ".bonfyre/estate-local/frontier.db", "SELECT day, route, channel_id, items FROM contribution ORDER BY day DESC LIMIT ?1", "SELECT count(*) FROM contribution",
     bf_fact_columns_7, 4, 50, 3, 1, BF_STORE_WEB_SOURCE_FRONTIER_LEASE},
    {"MatrixSubject", "bonfyre-matrix", "admin", "Subjects", "The people, places and things the estate has an identity for.", "No subjects are known yet.", "table",
     "subjects", NULL, NULL, NULL,
     "matrix", BF_STORE_WEB_MATRIX_ENV, NULL, "SELECT subject_id, subject_kind, evidence_ref FROM matrix_subjects ORDER BY rowid DESC LIMIT ?1", "SELECT count(*) FROM matrix_subjects",
     bf_fact_columns_8, 3, 50, 3, 1, BF_STORE_WEB_MATRIX_LEASE},
    {"CanonicalDomain", "bonfyre-matrix", "admin", "Domains", "Each host name the estate answers for, the institution it belongs to and its territory.", "No domains are known yet.", "table",
     "domains", NULL, NULL, NULL,
     "matrix", BF_STORE_WEB_MATRIX_ENV, NULL, "SELECT hostname, institution_id, territory, default_scope, COALESCE(canonical_hostname, '') FROM matrix_domains ORDER BY rowid DESC LIMIT ?1", "SELECT count(*) FROM matrix_domains",
     bf_fact_columns_9, 5, 50, 3, 1, BF_STORE_WEB_MATRIX_LEASE},
    {"LatentChart", "latent-coordinate-atlas", "admin", "Similarity charts", "The charts the similarity atlas places things on.", "No charts have been built yet.", "table",
     "similarity-charts", NULL, NULL, NULL,
     "latent_atlas", BF_STORE_WEB_LATENT_ATLAS_ENV, ".bonfyre/estate-local/atlas.db", "SELECT chart_id, family, dims, version, encoder FROM chart ORDER BY rowid DESC LIMIT ?1", "SELECT count(*) FROM chart",
     bf_fact_columns_10, 5, 50, 3, 1, BF_STORE_WEB_LATENT_ATLAS_LEASE},
    {"NeighbourhoodSnapshot", "latent-coordinate-atlas", "admin", "Neighbourhood snapshots", "Saved views of what sits near what on each chart.", "No snapshots have been taken yet.", "table",
     "neighbourhoods", NULL, NULL, NULL,
     "latent_atlas", BF_STORE_WEB_LATENT_ATLAS_ENV, ".bonfyre/estate-local/atlas.db", "SELECT snapshot_id, logical, lens, chart_id, created_at FROM snapshot ORDER BY created_at DESC LIMIT ?1", "SELECT count(*) FROM snapshot",
     bf_fact_columns_11, 5, 50, 3, 1, BF_STORE_WEB_LATENT_ATLAS_LEASE},
    {"CommerceStage", "commerce-chain", "admin", "Exchange stages", "Where each exchange has got to, and who moved it there.", "No exchange has started yet.", "table",
     "exchange-stages", NULL, NULL, NULL,
     "commerce", BF_STORE_WEB_COMMERCE_ENV, ".bonfyre/estate-local/commerce.db", "SELECT exchange, stage, owner, at FROM commerce_progress ORDER BY at DESC LIMIT ?1", "SELECT count(*) FROM commerce_progress",
     bf_fact_columns_12, 4, 50, 3, 1, BF_STORE_WEB_COMMERCE_LEASE},
    {"ResourceGrant", "resource-admission", "admin", "Space and memory grants", "What each job was allowed to use, and when it gave it back.", "Nothing has been granted yet.", "table",
     "resource-grants", NULL, NULL, NULL,
     "admission", BF_STORE_ADMISSION_ENV, ".bonfyre/admission.db", "SELECT plane, kind, estimated_bytes, granted_at, COALESCE(released_at, '') FROM resource_grants ORDER BY granted_at DESC LIMIT ?1", "SELECT count(*) FROM resource_grants",
     bf_fact_columns_13, 5, 50, 3, 1, BF_STORE_ADMISSION_LEASE},
    {"DeviceKey", "account-device-keys", "member", "Your devices", "The device keys you added to this account.", "No devices yet.", "table",
     NULL, "/account/devices", "Device list", "estate",
     NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 3, 1, {0}},
    {"DevicePairing", "account-device-move", "member", "Move to a new device", "One-use codes you offered to move this account.", "No move codes yet.", "table",
     NULL, "/account/move", "Move codes", "estate",
     NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 3, 1, {0}},
    {"FeedAgentPolicy", "feed-agent-policy", "member", "Agents and feeds", "The rules you set for agents on feeds you keep.", "No feed rules yet.", "table",
     NULL, "/account/agents", "Feed rules", "estate",
     NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 3, 1, {0}},
    {"CarrierSyncRequest", "bluesky-sync-request", "member", "Bluesky sync", "Sync requests you asked for and whether they ran.", "No sync requests yet.", "table",
     NULL, "/connections/bluesky/sync", "Sync requests", "estate",
     NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 3, 1, {0}},
    {"FieldDispatch", "field-dispatch", "member", "Field visits", "Visits you asked for and visits asked of you.", "No field requests yet.", "table",
     NULL, "/dispatch", "Dispatch queue", "estate",
     NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 3, 1, {0}},
    {"AccountActivityPresentation", "aurekai-activity", "member", "Activity", "Work running for your account.", "Nothing is running.", "table",
     NULL, "/apps/_activity", "Activity panel", "estate",
     NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 3, 1, {0}},
    {"GitHubAppConnection", "github-app-connection", "member", "GitHub connections", "The GitHub installations connected for your customers.", "No GitHub installation is connected.", "table",
     NULL, "/connect/github-app/customers", "Installations", "estate",
     NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 3, 1, {0}},
    {"ProviderCustomerAccount", "provider-customer-accounts", "member", "Provider accounts", "The provider accounts held for your organization.", "No provider account is held yet.", "table",
     NULL, "/organizations", "Provider accounts", "estate",
     NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 3, 1, {0}},
    {"ConnectionSourceCandidate", "connection-source-catalog", "member", "Connections you could add", "Sources the catalogue lists as possible connections, not yet checked.", "The catalogue lists nothing yet.", "table",
     NULL, "/apps/connections", "Connection catalogue", "estate",
     NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 3, 1, {0}},
    {"ReleaseApproval", "release-approval", "admin", "Release approvals", "Releases waiting for a person to approve them.", "No release is waiting.", "table",
     NULL, "/work-board", "Open approvals", "estate",
     NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 3, 1, {0}},
    {"ForeignOccurrence", "foreign-link-desk", "public", "Links and sources", "What the estate has read from other sites.", "Nothing has been read yet.", "table",
     NULL, "/sources", "Source desk", "estate",
     NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 3, 1, {0}},
    {"SiteSearchIndex", "static-semantic-search", "public", "Site search", "Each site's own search, built from its published sections.", "This site has no published sections.", "table",
     NULL, "/_search/index.bfsi", "Search index", "site",
     NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 3, 1, {0}},
    {"SearchIndex", "static-semantic-search", "public", "Site search", "Each site's own search, built from its published sections.", "This site has no published sections.", "table",
     NULL, "/_search/index.bfsi", "Search index", "site",
     NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 3, 1, {0}},
    {"SearchPassage", "static-semantic-search", "public", "Search passages", "The sections a site's search can answer with.", "This site has no published sections.", "table",
     NULL, "/_search/passages.json", "Passages", "site",
     NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 3, 1, {0}},
    {"StaticEmbeddingModel", "static-semantic-search", "public", "Search model", "The small model a page uses to read a question.", "The search model is not installed.", "table",
     NULL, "/_search/model.bfse", "Model file", "site",
     NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 3, 1, {0}},
};
#define BF_FACT_SURFACES (sizeof(bf_fact_surfaces) / sizeof(bf_fact_surfaces[0]))
#endif
