// Which native owner answers which public path. Compiled by
// cmd/BonfyreWebAddress/tools/compile_web_routes.sh into
// cmd/BonfyreWebAddress/generated/native_web_routes.h. A new page adds a row
// here and an owner taking `const BfRequest *`; it never adds a branch to main.c.
//
// The list order is the dispatch order. The first owner that answers ends
// the request; an owner already asked is not asked again.
package control

// A request field an owner outside this program's headers is called with.
#RequestField: "method" | "target" | "query" | "query_opt" | "body" | "raw" | "host" | "bearer" | "auth_db" |
	"accept" | "m" | "root" | "origin" | "site_bearer" | "site_client" | "if_none_match" | "payment" |
	"origin_method" | "institution" | "socket"

// Facts about the request the owner is only asked under.
//   get                 the method is GET
//   site_root           the server was started with a site root
//   html                Accept names text/html
//   not_json            Accept is not JSON-only
//   not_territory_apex  the host is not a territory's own apex
//   json                Accept names JSON
#When: "get" | "site_root" | "html" | "not_json" | "not_territory_apex" | "json"

#WebRoute: {
	owner: =~"^[a-z][a-z0-9_]*$"
	// exact: the path, or the path with one trailing slash; strict: the path
	// only; prefix: any path that starts with it; any: every path.
	match: "exact" | "strict" | "prefix" | "any"
	if match != "any" {
		// The empty path is the request line that named no target.
		path: "" | =~"^/[A-Za-z0-9._/-]*$"
	}
	// The name the owner knows this path by. The owner tests
	// bf_request_is(r, "<as>"), so a path is spelled once, here.
	as?: =~"^[a-z][a-z0-9_]*$"
	when?: [...#When]
	// Only for an owner compiled outside this program's headers, which keeps
	// its own argument list: the request fields it is called with, in order.
	call?: [...#RequestField]
}

web_routes: [...#WebRoute] & [
	// Published, content-addressed media has an exact route. It answers before
	// unrelated page and account handlers can inspect or alter the request.
	{owner: "serve_asset_twin", match: "prefix", path: "/assets/twin/"},
	// An operator's own site answers assistants itself, before the estate's
	// shared surfaces see the request.
	{owner: "bf_site_mcp_serve", match: "any", call: ["method", "target", "query", "body", "host", "origin", "site_bearer", "site_client"]},
	// A delegated host gets its private dispatcher before the public machine
	// web. This ordering is a disclosure invariant: no sitemap, protocol card,
	// RO-Crate, feed or fallback resolver can observe that a client estate exists.
	{owner: "serve_vertical_private", match: "any"},
	// The Observatory owns its maintained views, ahead of generic machine-web
	// dispatch, so a host-specific closure cannot inherit another carrier page.
	{owner: "serve_internet_observatory", match: "any"},
	// /people/<person> resolves one published public profile.
	{owner: "serve_people_alias", match: "prefix", path: "/people/", when: ["get", "site_root", "not_json"]},
	// Public, server-lowered relational directories, before capability,
	// commerce, studio and protocol owners are probed.
	{owner: "serve_credit_profile", match: "any", when: ["site_root", "not_json"]},
	{owner: "serve_people_directory", match: "strict", path: "/people", when: ["get", "site_root", "not_json"]},
	{owner: "serve_people_directory", match: "strict", path: "/organisations", when: ["get", "site_root", "not_json"]},
	// The discovery tier: the same bytes for every caller.
	{owner: "serve_machine_web", match: "any"},
	{owner: "subject_artifact", match: "any"},
	{owner: "serve_indexnow_key", match: "any"},
	{owner: "serve_websub", match: "any"},
	// A /w/<world>/spatial request carrying a frontier asks what was there at
	// an instant; without `at` the spatial surface answers as before.
	{owner: "serve_spatial_frontier", match: "any", call: ["method", "target", "query_opt", "accept"]},
	// Devices, moving to a new device, Bluesky sync, feed agent rules and
	// field visits: CMS records keyed by the signed-in account.
	{owner: "serve_account_continuity", match: "exact", path: "/account/devices"},
	{owner: "serve_account_continuity", match: "exact", path: "/account/devices/revoke"},
	{owner: "serve_account_continuity", match: "prefix", path: "/account/move"},
	{owner: "serve_account_continuity", match: "prefix", path: "/account/agents"},
	{owner: "serve_account_continuity", match: "prefix", path: "/connections/bluesky/sync"},
	{owner: "serve_field_dispatch", match: "exact", path: "/dispatch"},
	{owner: "serve_dating_review", match: "exact", path: "/moderation/dating"},
	{owner: "serve_guild_records", match: "prefix", path: "/guild/"},
	{owner: "serve_household_circles", match: "prefix", path: "/care"},
	{owner: "serve_household_circles", match: "prefix", path: "/pantry"},
	{owner: "serve_charters", match: "exact", path: "/charters"},
	{owner: "serve_charters", match: "prefix", path: "/charters/"},
	{owner: "serve_work_orders", match: "strict", path: "/work-orders", as: "index"},
	{owner: "serve_work_orders", match: "prefix", path: "/work-orders/", as: "order"},
	{owner: "serve_work_orders", match: "prefix", path: "/disputes/", as: "dispute"},
	{owner: "serve_work_orders", match: "strict", path: "/vouches", as: "vouches"},
	{owner: "serve_work_orders", match: "prefix", path: "/vouches/"},
	{owner: "serve_account_activity", match: "strict", path: "/apps/_activity"},
	// The source-specific workbench answers before general SiteProgram or
	// /s/work material.
	{owner: "serve_source_web_work", match: "exact", path: "/sources/web"},
	{owner: "serve_source_web_work", match: "strict", path: "/sources/_web"},
	{owner: "serve_source_push_operations", match: "exact", path: "/sources/push"},
	{owner: "serve_source_desk", match: "exact", path: "/sources", as: "sources", when: ["get", "site_root", "not_json"]},
	{owner: "serve_source_desk", match: "exact", path: "/link", as: "link", when: ["get", "site_root", "not_json"]},
	{owner: "serve_source_desk", match: "exact", path: "/foreign/workbench", as: "legacy", when: ["get", "site_root", "not_json"]},
	{owner: "serve_source_desk", match: "exact", path: "/s/work/foreign-twins", as: "legacy", when: ["get", "site_root", "not_json"]},
	{owner: "serve_source_desk", match: "exact", path: "/connections", as: "connections", when: ["get", "site_root", "not_json"]},
	{owner: "serve_source_desk", match: "exact", path: "/connect", as: "connect", when: ["get", "site_root", "not_json"]},
	{owner: "serve_source_desk", match: "exact", path: "/channels", as: "channels", when: ["get", "site_root", "not_json"]},
	{owner: "serve_community_journey", match: "exact", path: "/guilds", as: "guild_directory"},
	{owner: "serve_community_journey", match: "strict", path: "/guild/new", as: "guild_create"},
	{owner: "serve_community_journey", match: "exact", path: "/communities", as: "directory"},
	{owner: "serve_community_journey", match: "strict", path: "/community/new", as: "creating"},
	{owner: "serve_community_journey", match: "strict", path: "/community/preview", as: "preview"},
	{owner: "serve_community_journey", match: "strict", path: "/community/action", as: "action"},
	{owner: "serve_community_journey", match: "strict", path: "/govern/community-join", as: "legacy"},
	{owner: "serve_community_journey", match: "exact", path: "/community", as: "room"},
	{owner: "serve_community_journey", match: "prefix", path: "/community/", as: "permalink"},
	{owner: "serve_account_world_workspace", match: "strict", path: "/workspace/world", as: "world"},
	{owner: "serve_account_world_workspace", match: "strict", path: "/workspace/world-literature", as: "literature"},
	{owner: "serve_account_world_workspace", match: "strict", path: "/workspace/worlds/reading", as: "reading"},
	{owner: "serve_account_world_workspace", match: "strict", path: "/workspace/world/film", as: "film"},
	{owner: "serve_account_world_workspace", match: "strict", path: "/workspace/worlds/club", as: "club"},
	{owner: "serve_account_world_workspace", match: "strict", path: "/workspace/worlds", as: "studio"},
	{owner: "serve_account_world_workspace", match: "strict", path: "/workspace/world-audio", as: "audio"},
	{owner: "serve_account_world_workspace", match: "prefix", path: "/worlds/", as: "world_page"},
	{owner: "serve_account_world_workspace", match: "strict", path: "/worlds", as: "worlds"},
	{owner: "serve_profile_home", match: "exact", path: "/profile", as: "profile"},
	{owner: "serve_profile_home", match: "strict", path: "/profile/connections/join", as: "connections_join"},
	{owner: "serve_profile_home", match: "strict", path: "/profile/connections/choice", as: "connections_choice"},
	{owner: "serve_profile_home", match: "strict", path: "/profile/connections/interests", as: "connections_interests"},
	{owner: "serve_profile_home", match: "strict", path: "/profile/connections/message", as: "connections_message"},
	{owner: "serve_profile_home", match: "prefix", path: "/profile/connections/", as: "connections_change"},
	{owner: "serve_profile_home", match: "strict", path: "/profile/dating/request", as: "dating_request"},
	{owner: "serve_profile_home", match: "strict", path: "/profile/dating/choice", as: "dating_choice"},
	{owner: "serve_profile_home", match: "strict", path: "/profile/dating/message", as: "dating_message"},
	{owner: "serve_profile_home", match: "strict", path: "/profile/dating/preferences", as: "dating_preferences"},
	{owner: "serve_profile_home", match: "strict", path: "/profile/dating/plan", as: "dating_plan"},
	{owner: "serve_profile_home", match: "strict", path: "/profile/dating/plan-approve", as: "dating_plan_approve"},
	{owner: "serve_profile_home", match: "prefix", path: "/profile/dating/", as: "dating_change"},
	{owner: "serve_profile_home", match: "strict", path: "/profile/cover/remove", as: "profile_cover_remove"},
	{owner: "serve_profile_home", match: "strict", path: "/profile/edit", as: "profile_edit"},
	{owner: "serve_profile_home", match: "strict", path: "/profile/interests/add", as: "profile_interests_add"},
	{owner: "serve_profile_home", match: "strict", path: "/profile/interests/remove", as: "profile_interests_remove"},
	{owner: "serve_profile_home", match: "strict", path: "/profile/preview", as: "profile_preview"},
	{owner: "serve_profile_home", match: "strict", path: "/profile/publish", as: "profile_publish"},
	{owner: "serve_profile_home", match: "strict", path: "/profile/quiz/answer", as: "profile_quiz_answer"},
	{owner: "serve_profile_home", match: "strict", path: "/profile/quiz/clear", as: "profile_quiz_clear"},
	{owner: "serve_profile_home", match: "strict", path: "/profile/theme", as: "profile_theme"},
	{owner: "serve_profile_home", match: "strict", path: "/profile/withdraw", as: "profile_withdraw"},
	{owner: "serve_profile_home", match: "prefix", path: "/profile/", as: "profile_change"},
	{owner: "serve_profile_home", match: "strict", path: "/", as: "apex"},
	{owner: "serve_profile_home", match: "exact", path: "/blog", as: "journal"},
	{owner: "serve_profile_home", match: "prefix", path: "/blog/", as: "post"},
	{owner: "serve_companions", match: "any"},
	{owner: "serve_participant_journey", match: "strict", path: "/participation", as: "page"},
	{owner: "serve_participant_journey", match: "strict", path: "/participation/change", as: "change"},
	// A subject's own apex opens on the program published for that subject;
	// a territory's apex opens on its composed front page.
	{owner: "serve_site_program", match: "strict", path: "/", when: ["get", "not_territory_apex"]},
	{owner: "serve_front_program", match: "strict", path: "/", when: ["get", "site_root", "not_json"]},
	{owner: "serve_page_save", match: "strict", path: "/o/save"},
	{owner: "serve_page_save", match: "strict", path: "/o/unsave"},
	// Saying where you stand, putting your name to a role, stating support.
	{owner: "govern_act", match: "any"},
	{owner: "serve_role_assume", match: "any"},
	{owner: "serve_role_workspace", match: "strict", path: "/roles/workspace"},
	{owner: "serve_role_directory", match: "strict", path: "/roles"},
	{owner: "serve_role_leave", match: "strict", path: "/roles/leave"},
	{owner: "emit_saved_page", match: "strict", path: "/saved", when: ["get", "site_root"]},
	{owner: "serve_page_changes", match: "prefix", path: "/o/", when: ["site_root"]},
	{owner: "emit_about_page", match: "prefix", path: "/about/", when: ["get", "site_root"]},
	{owner: "emit_item_page", match: "prefix", path: "/o/", when: ["get", "site_root"]},
	// The plain-words address of the price room.
	{owner: "serve_price_room_alias", match: "strict", path: "/what-things-cost", when: ["get"]},
	// Released work now appears on the account profile that published it.
	{owner: "serve_examples_retired", match: "strict", path: "/examples", when: ["get"]},
	{owner: "serve_examples_retired", match: "prefix", path: "/examples/", when: ["get"]},
	{owner: "serve_site_program", match: "any"},
	// A signed-in person's /inbox is their own; the public donor inbox keeps
	// answering everyone else.
	{owner: "serve_account_inbox", match: "strict", path: "/inbox/_summary"},
	{owner: "serve_account_inbox", match: "exact", path: "/inbox"},
	// The studio is a C workbench on the one shell; it answers before the
	// compiled donor surface that once held the same address.
	{owner: "serve_desk_external_candidate", match: "any"},
	{owner: "serve_desk", match: "any"},
	{owner: "serve_territory_map", match: "any"},
	{owner: "serve_native_donor_route", match: "any"},
	// An authored surface answers before the auth handler, which only matters
	// for GET: serve_authored_page declines anything else, so a credential
	// POST still reaches the organ that owns sessions.
	{owner: "serve_account_navigation", match: "strict", path: "/account/review"},
	{owner: "serve_account_navigation", match: "strict", path: "/account/navigation"},
	{owner: "serve_authored_page", match: "any"},
	{owner: "serve_external_candidate_fragment", match: "any"},
	// Browser sign-in across territories (session_handoff_web.h).
	{owner: "serve_auth", match: "strict", path: "/session/switch", as: "session_switch"},
	{owner: "serve_auth", match: "strict", path: "/session/continue", as: "session_continue"},
	{owner: "serve_auth", match: "strict", path: "/session/authorize", as: "session_authorize"},
	{owner: "serve_auth", match: "strict", path: "/session/complete", as: "session_complete"},
	{owner: "serve_auth", match: "any"},
	{owner: "serve_superuser_control", match: "any"},
	{owner: "serve_membrane", match: "any"},
	{owner: "emit_parcel_page", match: "prefix", path: "/s/parcel/", when: ["get", "site_root", "html"]},
	{owner: "serve_subject", match: "any"},
	{owner: "serve_lens", match: "any"},
	{owner: "serve_room", match: "any"},
	{owner: "serve_compose", match: "any"},
	{owner: "serve_surfaceir", match: "any"},
	{owner: "serve_flow", match: "any"},
	{owner: "emit_market_fronts", match: "any", when: ["site_root", "html"]},
	{owner: "serve_property_flow", match: "any"},
	{owner: "serve_market_flow", match: "any"},
	{owner: "serve_sources_flow", match: "any"},
	{owner: "serve_agents_flow", match: "any"},
	{owner: "serve_a2a", match: "any"},
	{owner: "serve_capability", match: "any"},
	{owner: "serve_aperture_submit", match: "any"},
	{owner: "serve_aperture_form", match: "any"},
	{owner: "serve_aperture", match: "any"},
	{owner: "serve_twins_page", match: "any"},
	{owner: "serve_observations_page", match: "any"},
	{owner: "serve_night_page", match: "any"},
	{owner: "serve_protocols", match: "any"},
	{owner: "serve_world_composition", match: "any"},
	{owner: "serve_world_fragments", match: "any"},
	{owner: "serve_world_material", match: "strict", path: "/_surface/world-material"},
	{owner: "serve_listen_program", match: "strict", path: "/_surface/listen-program"},
	{owner: "serve_human_community_page", match: "any"},
	{owner: "bf_serve_human_community", match: "any", call: ["method", "target", "query_opt"]},
	{owner: "serve_production_publication", match: "strict", path: "/_surface/production-home.js", as: "script"},
	{owner: "serve_production_publication", match: "exact", path: "/productions", as: "home"},
	{owner: "serve_production_publication", match: "strict", path: "/_surface/productions", as: "fragment"},
	{owner: "serve_production_publication", match: "prefix", path: "/productions/", as: "detail"},
	{owner: "serve_cms_composer", match: "strict", path: "/cms/compose", as: "home"},
	{owner: "serve_cms_composer", match: "strict", path: "/cms/compose/page", as: "page"},
	{owner: "serve_cms_composer", match: "strict", path: "/cms/compose/pages", as: "listing"},
	{owner: "serve_cms_composer", match: "strict", path: "/cms/compose/advanced", as: "visual"},
	{owner: "serve_cms_composer", match: "strict", path: "/cms/compose/narration-revision", as: "revision"},
	{owner: "serve_cms_composer", match: "strict", path: "/cms/compose/revise", as: "revising"},
	{owner: "serve_cms_composer", match: "strict", path: "/cms/compose/publish", as: "publishing"},
	{owner: "serve_cms_composer", match: "strict", path: "/cms/compose/export", as: "exporting"},
	{owner: "serve_source_publication", match: "strict", path: "/cms/source-publication"},
	{owner: "serve_cms_api", match: "any"},
	{owner: "serve_discovery_review", match: "any"},
	{owner: "serve_discovery_verification", match: "any"},
	{owner: "serve_discovery_brief", match: "any"},
	{owner: "serve_cms_partials", match: "any"},
	{owner: "serve_cms_route", match: "any"},
	{owner: "serve_studio", match: "any"},
	{owner: "serve_subject_index", match: "any"},
	{owner: "serve_pack_setup", match: "strict", path: "/packs/setup"},
	{owner: "serve_account_choices", match: "strict", path: "/account/choices/institutions"},
	{owner: "serve_organization_workspace", match: "strict", path: "/organizations"},
	{owner: "serve_organization_workspace", match: "strict", path: "/organizations/join"},
	{owner: "serve_household_members", match: "strict", path: "/households/access"},
	// Your work (application_workspace.h): each path its sub-owner answers;
	// the rest of its owners still decide their own paths.
	{owner: "serve_application_workspace", match: "exact", path: "/apps/audio", as: "audio_studio"},
	{owner: "serve_application_workspace", match: "strict", path: "/apps/_connection_catalog", as: "connection_catalog"},
	{owner: "serve_application_workspace", match: "strict", path: "/apps/connections", as: "connections_page"},
	{owner: "serve_application_workspace", match: "strict", path: "/apps/_connections", as: "connections_api"},
	{owner: "serve_application_workspace", match: "strict", path: "/work-board/act", as: "work_board_act"},
	{owner: "serve_application_workspace", match: "strict", path: "/work-board/outputs", as: "work_outputs"},
	{owner: "serve_application_workspace", match: "strict", path: "/work-board", as: "work_board"},
	{owner: "serve_application_workspace", match: "strict", path: "/relationships", as: "relationships"},
	{owner: "serve_application_workspace", match: "strict", path: "/feed-use", as: "feed_use"},
	{owner: "serve_application_workspace", match: "strict", path: "/apps/_destinations", as: "destinations"},
	{owner: "serve_application_workspace", match: "strict", path: "/apps/_bindings", as: "bindings"},
	{owner: "serve_application_workspace", match: "strict", path: "/apps/_cards", as: "cards"},
	{owner: "serve_application_workspace", match: "strict", path: "/cms/productions", as: "productions"},
	{owner: "serve_application_workspace", match: "exact", path: "/apps", as: "apps_home"},
	{owner: "serve_application_workspace", match: "prefix", path: "/apps/", as: "apps"},
	{owner: "serve_application_workspace", match: "any"},
	{owner: "serve_packs", match: "any"},
	{owner: "serve_home_weather_fragment", match: "any"},
	{owner: "serve_foreign_twins", match: "any"},
	{owner: "serve_foreign_weather", match: "any"},
	{owner: "serve_foreign_receipts", match: "any"},
	{owner: "serve_foreign_body", match: "any"},
	{owner: "serve_foreign_edges", match: "any"},
	{owner: "serve_ops", match: "strict", path: "/ops"},
	{owner: "serve_ops", match: "prefix", path: "/ops/"},
	{owner: "serve_matrix_closure", match: "any"},
	// The tail: once no page owner answered. A non-GET is refused, then the
	// discovery documents, fragments and surfaces, and the resolver answers last.
	{owner: "serve_method_refused", match: "any"},
	{owner: "serve_health", match: "strict", path: "/healthz"},
	{owner: "serve_estate_directory", match: "strict", path: "/.well-known/bonfyre/estate"},
	{owner: "serve_discovery", match: "strict", path: "/.well-known/bonfyre"},
	{owner: "serve_tail_root_discovery", match: "any"},
	{owner: "serve_tail_presentation_class", match: "any"},
	{owner: "serve_tail_surface_pattern", match: "any"},
	{owner: "serve_tail_door_fragment", match: "any"},
	{owner: "serve_tail_aperture_fragment", match: "any"},
	{owner: "serve_tail_surface", match: "any"},
	{owner: "serve_tail_matrix_asset", match: "any"},
	{owner: "serve_resolve", match: "any"},
]
