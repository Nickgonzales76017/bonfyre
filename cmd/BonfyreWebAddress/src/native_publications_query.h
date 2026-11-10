#ifndef BONFYRE_NATIVE_PUBLICATIONS_QUERY_H
#define BONFYRE_NATIVE_PUBLICATIONS_QUERY_H

/* Domain membership belongs to the published collection set. Materialize it
 * once per statement; correlating json_each with every occurrence repeatedly
 * parses the same selectors and occupies a public worker even for empty cuts.
 * SQLite >= 3.35 supplies this optimization fence. Standing, attribution join,
 * ordering and caller's limit retain their existing meanings. */
static const char bf_native_publications_query[] =
    "WITH allowed_sources AS MATERIALIZED ("
    "SELECT source_value.value FROM source_collection sc, "
    "json_each(sc.source_selector,'$.source_refs') source_value "
    "WHERE sc.status='published' AND EXISTS ("
    "SELECT 1 FROM json_each(sc.source_selector,'$.domains') domain_value "
    "WHERE domain_value.value=?2)) "
    "SELECT p.title,COALESCE(p.canonical_href,''),COALESCE(p.published_at,p.observed_at,''),"
    "COALESCE(i.says,p.source_ref) FROM publication_occurrence p "
    "LEFT JOIN internet_source i ON i.source=p.source_ref AND i.status='published' "
    "WHERE p.status='published' AND p.visibility='public' AND p.assertion_state='OBSERVED' "
    "AND (?2='' OR p.source_ref IN (SELECT value FROM allowed_sources)) "
    "ORDER BY COALESCE(p.published_at,p.observed_at) DESC,p.id DESC LIMIT ?1";

#endif
