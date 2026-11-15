/* Generated from schemas/control/web-routes.cue by tools/compile_web_routes.sh. Do not edit. */
#ifndef BONFYRE_NATIVE_WEB_ROUTES_H
#define BONFYRE_NATIVE_WEB_ROUTES_H
#include "../src/web_request.h"
static const BfRoute bf_web_routes[] = {
    {"/account/devices", BF_ROUTE_EXACT, serve_account_continuity},
    {"/account/devices/revoke", BF_ROUTE_EXACT, serve_account_continuity},
    {"/account/move", BF_ROUTE_PREFIX, serve_account_continuity},
    {"/account/agents", BF_ROUTE_PREFIX, serve_account_continuity},
    {"/connections/bluesky/sync", BF_ROUTE_PREFIX, serve_account_continuity},
    {"/dispatch", BF_ROUTE_PREFIX, serve_field_dispatch},
    {"/moderation/dating", BF_ROUTE_EXACT, serve_dating_review},
    {"/guild/", BF_ROUTE_PREFIX, serve_guild_records},
    {"/care", BF_ROUTE_PREFIX, serve_household_circles},
    {"/pantry", BF_ROUTE_PREFIX, serve_household_circles},
    {"/charters", BF_ROUTE_PREFIX, serve_charters},
    {"/work-orders", BF_ROUTE_PREFIX, serve_work_orders},
    {"/disputes", BF_ROUTE_PREFIX, serve_work_orders},
    {"/vouches", BF_ROUTE_PREFIX, serve_work_orders},
};
static inline int bf_web_route_dispatch(const BfRequest *request) {
    return bf_route_dispatch(bf_web_routes, sizeof(bf_web_routes) / sizeof(bf_web_routes[0]), request);
}
#endif
