#ifndef BF_FRONT_RECIPE_QUERY_H
#define BF_FRONT_RECIPE_QUERY_H
/* The factored recipe owner rebuilds FTS from its record table. Pick the exact
 * search ordinal first; only that recipe's body metadata needs a table read.
 * A missing selected record yields no link, never a guessed substitute. */
#define BF_FRONT_RECIPE_PICK_SQL \
    "SELECT r.record_key,r.title FROM record r WHERE r.seq=(" \
    "SELECT rowid FROM record_search WHERE record_search MATCH ?1 " \
    "ORDER BY rowid LIMIT 1 OFFSET ?2)"
#endif
