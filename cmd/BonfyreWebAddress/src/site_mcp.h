#ifndef BONFYRE_SITE_MCP_H
#define BONFYRE_SITE_MCP_H

/* One existing Aurekai asset for its native site heads. An independently
 * branded customer site should supply its own mark, not inherit ours. */
#define BF_AUREKAI_FAVICON_LINK "<link rel=\"icon\" type=\"image/png\" sizes=\"16x16\" href=\"/assets/aurekai-knight-16.png\">" \
                                "<link rel=\"icon\" type=\"image/png\" sizes=\"32x32\" href=\"/assets/aurekai-knight-32.png\">" \
                                "<link rel=\"icon\" type=\"image/png\" sizes=\"168x168\" href=\"/assets/aurekai-knight.png\">" \
                                "<link rel=\"apple-touch-icon\" sizes=\"180x180\" href=\"/assets/aurekai-knight-180.png\">"

/* Answers /mcp, /llms.txt and /.well-known/agent.json for an operator's own
 * site at this host, writing a whole HTTP response to stdout.  Returns 1 when
 * it answered and 0 when the request is not for such a site. */
int bf_site_mcp_serve(const char *method, const char *target, const char *query, const char *body, const char *host,
                      const char *origin, const char *bearer, const char *client);

/* Writes links to an operator's own standing pages to stdout, if it has any. */
struct sqlite3;
void bf_site_pages_nav(struct sqlite3 *db, const char *site_id);
void bf_site_bar(const char *title);
void bf_site_home_extras(struct sqlite3 *db, const char *site_id, const char *title);

#endif
