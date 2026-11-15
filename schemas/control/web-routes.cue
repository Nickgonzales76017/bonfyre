// Which native owner answers which public path. Compiled by
// cmd/BonfyreWebAddress/tools/compile_web_routes.sh into
// cmd/BonfyreWebAddress/generated/native_web_routes.h. A new page adds a row
// here and an owner taking `const BfRequest *`; it never adds a branch to main.c.
package control

#WebRoute: {
	owner: =~"^serve_[a-z_]+$"
	match: "exact" | "prefix"
	path:  =~"^/[A-Za-z0-9._/-]*$"
}

web_routes: [...#WebRoute] & [
	{owner: "serve_account_continuity", match: "exact", path: "/account/devices"},
	{owner: "serve_account_continuity", match: "exact", path: "/account/devices/revoke"},
	{owner: "serve_account_continuity", match: "prefix", path: "/account/move"},
	{owner: "serve_account_continuity", match: "prefix", path: "/account/agents"},
	{owner: "serve_account_continuity", match: "prefix", path: "/connections/bluesky/sync"},
	{owner: "serve_field_dispatch", match: "prefix", path: "/dispatch"},
	{owner: "serve_dating_review", match: "exact", path: "/moderation/dating"},
	{owner: "serve_guild_records", match: "prefix", path: "/guild/"},
	{owner: "serve_household_circles", match: "prefix", path: "/care"},
	{owner: "serve_household_circles", match: "prefix", path: "/pantry"},
	{owner: "serve_charters", match: "prefix", path: "/charters"},
	{owner: "serve_work_orders", match: "prefix", path: "/work-orders"},
	{owner: "serve_work_orders", match: "prefix", path: "/disputes"},
	{owner: "serve_work_orders", match: "prefix", path: "/vouches"},
]
