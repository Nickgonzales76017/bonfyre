#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "surface_maturity.h"
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

typedef struct {
    int cms_db, items_db, frontier_db, fabric_db, missing_owner_dbs;
    int account_db, meter_db, control_db, account_index_current, account_output_column;
    int evidence_relation_table, foreign_observation_table, frontier_layer_table, solved_invariant_table, known_noncause_table, evidence_sync_table, evidence_sync_current;
    int evidence_relations, foreign_observations, frontier_layers, measured_invariants, proven_invariants, challenged_invariants, known_noncauses;
    int evidence_sync_considered, evidence_sync_exact, evidence_sync_unresolved;
    int account_work_table, account_run_table, account_step_table, meter_events_table, meter_rate_table;
    int account_tasks, account_runs, account_steps, account_outputs, inbox_states, inbox_operations;
    int meter_events, meter_rates, meter_unpriced_events;
    int published_sources, sources_with_publisher_identity, retained_items, retained_sources;
    int retained_publishers, eligible_publishers, retained_source_identity_gaps;
    int addressed_publishers, addressed_publishers_without_bundle, addressed_publishers_without_reader;
    int admitted_channels, admitted_published_sources, admitted_published_sources_with_reader;
    int subject_bundles, subject_readers, subject_actions, subject_scenes;
    int program_instances, source_collections, program_subjects_without_collection, surface_scenes;
    int publisher_address_gaps, admitted_published_sources_without_reader, maturity_debt;
    int site_presentation_table, subject_bundle_table, subject_reader_table;
    int subject_action_table, subject_scene_table;
    int program_table, source_collection_table, surface_scene_table;
    /* The source lake: what the estate reads, joined to who it is from. */
    int lake_measured, lake_roots, lake_roots_with_identity, identity_gaps_open, frontier_gap_table;
    int items_with_subject, lake_subjects, lake_subjects_with_site, lake_subjects_with_reader;
    int lake_subjects_with_site_and_reader, lake_sites_without_reader;
} EstateMetrics;

static int scalar(sqlite3 *db, const char *sql, int *out) {
    sqlite3_stmt *st = NULL;
    int ok = 0;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, bf_surface_maturity_json, -1, SQLITE_STATIC);
    if (sqlite3_step(st) == SQLITE_ROW) { *out = sqlite3_column_int(st, 0); ok = 1; }
    sqlite3_finalize(st);
    return ok;
}

static int count_sql(sqlite3 *db, const char *sql, int *out) {
    sqlite3_stmt *st = NULL;
    int ok = 0;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return 0;
    if (sqlite3_step(st) == SQLITE_ROW) { *out = sqlite3_column_int(st, 0); ok = 1; }
    sqlite3_finalize(st);
    return ok;
}

static int file_exists(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) return 0;
    fclose(f);
    return 1;
}

static int estate_path(char *out, size_t cap, const char *estate, const char *name) {
    int n = snprintf(out, cap, "%s/%s", estate, name);
    return n > 0 && (size_t)n < cap;
}

static int attach_db(sqlite3 *db, const char *schema, const char *path) {
    sqlite3_stmt *st = NULL;
    char sql[96];
    if (!file_exists(path)) return 0;
    if (snprintf(sql, sizeof(sql), "ATTACH DATABASE ?1 AS %s", schema) >= (int)sizeof(sql)) return 0;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, path, -1, SQLITE_TRANSIENT);
    int ok = sqlite3_step(st) == SQLITE_DONE;
    sqlite3_finalize(st);
    return ok;
}

static int table_exists(sqlite3 *db, const char *schema, const char *table) {
    sqlite3_stmt *st = NULL;
    char sql[160];
    if (snprintf(sql, sizeof(sql),
        "SELECT 1 FROM %s.sqlite_master WHERE type='table' AND name=?1 LIMIT 1", schema) >= (int)sizeof(sql)) return 0;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, table, -1, SQLITE_TRANSIENT);
    int ok = sqlite3_step(st) == SQLITE_ROW;
    sqlite3_finalize(st);
    return ok;
}

static int audit(sqlite3 *db) {
    int count = 0, axes = 0, shallow = 0, shallow_axes = 0, unbound = 0, duplicate_routes = 0, broken = 0;
    sqlite3_stmt *st = NULL;
    const char *broken_sql =
        "SELECT count(*) FROM json_each(?1,'$.surfaces') s, json_each(s.value,'$.transitions') t "
        "WHERE NOT EXISTS (SELECT 1 FROM json_each(?1,'$.surfaces') x "
        "WHERE json_extract(x.value,'$.surface')=t.value)";
    if (!scalar(db, "SELECT count(*) FROM json_each(?1,'$.surfaces')", &count) ||
        !scalar(db, "SELECT count(*) FROM json_each(?1,'$.coverage_axes')", &axes) ||
        !scalar(db, "SELECT count(*) FROM json_each(?1,'$.surfaces') WHERE json_array_length(json_extract(value,'$.owners'))<3", &shallow) ||
        !scalar(db, "SELECT count(*) FROM json_each(?1,'$.coverage_axes') WHERE json_array_length(json_extract(value,'$.owners'))<2", &shallow_axes) ||
        !scalar(db, "SELECT count(*) FROM json_each(?1,'$.surfaces') WHERE COALESCE(json_extract(value,'$.world_bound'),0)!=1", &unbound) ||
        !scalar(db, "SELECT count(*) FROM (SELECT json_extract(value,'$.route') r,count(*) n FROM json_each(?1,'$.surfaces') GROUP BY r HAVING n>1)", &duplicate_routes))
        return 0;
    if (sqlite3_prepare_v2(db, broken_sql, -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, bf_surface_maturity_json, -1, SQLITE_STATIC);
    if (sqlite3_step(st) != SQLITE_ROW) { sqlite3_finalize(st); return 0; }
    broken = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);
    printf("{\"schema\":\"bonfyre-surface-maturity-audit.v2\",\"surfaces\":%d,\"coverage_axes\":%d,\"shallow_owner_sets\":%d,\"shallow_axis_owner_sets\":%d,\"world_unbound\":%d,\"duplicate_routes\":%d,\"broken_transitions\":%d,\"truth\":\"projection_contract_only_use_estate_audit_for_owner_state\"}\n",
           count, axes, shallow, shallow_axes, unbound, duplicate_routes, broken);
    return count == 22 && axes >= 9 && shallow == 0 && shallow_axes == 0 && unbound == 0 && duplicate_routes == 0 && broken == 0;
}

static int list(sqlite3 *db) {
    sqlite3_stmt *st = NULL;
    const char *sql =
        "SELECT json_extract(value,'$.surface'),json_extract(value,'$.route'),json_extract(value,'$.question') "
        "FROM json_each(?1,'$.surfaces') ORDER BY key";
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, bf_surface_maturity_json, -1, SQLITE_STATIC);
    while (sqlite3_step(st) == SQLITE_ROW)
        printf("%s\t%s\t%s\n", sqlite3_column_text(st,0), sqlite3_column_text(st,1), sqlite3_column_text(st,2));
    sqlite3_finalize(st);
    return 1;
}

static int axes(sqlite3 *db) {
    sqlite3_stmt *st = NULL;
    const char *sql =
        "SELECT json_extract(value,'$.axis'),json_extract(value,'$.question'),json_extract(value,'$.owners'),"
        "json_extract(value,'$.backfill') FROM json_each(?1,'$.coverage_axes') ORDER BY key";
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, bf_surface_maturity_json, -1, SQLITE_STATIC);
    while (sqlite3_step(st) == SQLITE_ROW)
        printf("%s\t%s\t%s\t%s\n", sqlite3_column_text(st,0), sqlite3_column_text(st,1),
            sqlite3_column_text(st,2), sqlite3_column_text(st,3));
    sqlite3_finalize(st);
    return 1;
}

static int closure(sqlite3 *db, const char *surface, const char *world) {
    sqlite3_stmt *st = NULL;
    const char *sql =
        "SELECT json_object("
        "'schema','bonfyre-world-surface-closure.v1',"
        "'world',?2,'surface',json_extract(value,'$.surface'),'route',json_extract(value,'$.route'),"
        "'question',json_extract(value,'$.question'),'owners',json(json_extract(value,'$.owners')),"
        "'runtime_owners',json(json_extract(value,'$.runtime_owners')),"
        "'same_world_transitions',json(json_extract(value,'$.transitions')),"
        "'anonymous_use',json_extract(value,'$.anonymous_use'),'effect_aperture',json_extract(value,'$.effect_aperture'),"
        "'truth_boundary',json_extract(value,'$.truth_boundary'),"
        "'address_rule',json_extract(?1,'$.address_rule'),'truth','projection_contract_not_owner_state') "
        "FROM json_each(?1,'$.surfaces') WHERE json_extract(value,'$.surface')=?3 OR json_extract(value,'$.route')=?3 LIMIT 1";
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, bf_surface_maturity_json, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, world, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 3, surface, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) != SQLITE_ROW || !sqlite3_column_text(st,0)) { sqlite3_finalize(st); return 0; }
    puts((const char *)sqlite3_column_text(st,0));
    sqlite3_finalize(st);
    return 1;
}

static int load_estate(sqlite3 *db, const char *estate, EstateMetrics *m) {
    char cms[4096], items[4096], frontier[4096], fabric[4096], account[4096] = "", meter[4096] = "", evidence_path[4096] = "";
    const char *account_override = getenv("BONFYRE_ACCOUNT_WORK_DB");
    const char *meter_override = getenv("BONFYRE_METER_DB");
    const char *evidence_override = getenv("BONFYRE_CONTROL_DB");
    const char *home = getenv("HOME");
    memset(m, 0, sizeof(*m));
    if (!estate_path(cms, sizeof(cms), estate, "cms.db") ||
        !estate_path(items, sizeof(items), estate, "items.db") ||
        !estate_path(frontier, sizeof(frontier), estate, "frontier.db") ||
        !estate_path(fabric, sizeof(fabric), estate, "subject_fabric.db")) return 0;
    if (meter_override && *meter_override) snprintf(meter, sizeof(meter), "%s", meter_override);
    else if (!estate_path(meter, sizeof(meter), estate, "meter.db")) return 0;
    if (account_override && *account_override) snprintf(account, sizeof(account), "%s", account_override);
    else if (home && *home) snprintf(account, sizeof(account), "%s/.bonfyre/state/account-work.db", home);
    if (evidence_override && *evidence_override) snprintf(evidence_path, sizeof(evidence_path), "%s", evidence_override);
    else if (!estate_path(evidence_path, sizeof(evidence_path), estate, "control.db")) return 0;
    m->cms_db = attach_db(db, "cms", cms);
    if (!m->cms_db) return 0;
    m->items_db = attach_db(db, "items", items);
    m->frontier_db = attach_db(db, "frontier", frontier);
    m->fabric_db = attach_db(db, "fabric", fabric);
    m->account_db = account[0] ? attach_db(db, "acct", account) : 0;
    m->meter_db = meter[0] ? attach_db(db, "meter", meter) : 0;
    m->control_db = evidence_path[0] ? attach_db(db, "control", evidence_path) : 0;
    m->missing_owner_dbs = !m->items_db + !m->frontier_db + !m->fabric_db;
    sqlite3_exec(db, "PRAGMA query_only=ON", NULL, NULL, NULL);

    if (table_exists(db, "cms", "internet_source")) {
        count_sql(db, "SELECT COUNT(*) FROM cms.internet_source WHERE status='published'", &m->published_sources);
        count_sql(db, "SELECT COUNT(*) FROM cms.internet_source WHERE status='published' AND COALESCE(publisher_ident,'')<>'' AND COALESCE(publisher_name,'')<>''", &m->sources_with_publisher_identity);
    }
    if (m->items_db && table_exists(db, "items", "item")) {
        count_sql(db, "SELECT COUNT(*) FROM items.item", &m->retained_items);
        count_sql(db, "SELECT COUNT(DISTINCT source) FROM items.item", &m->retained_sources);
        count_sql(db, "SELECT COUNT(DISTINCT publisher) FROM items.item WHERE COALESCE(publisher,'')<>''", &m->retained_publishers);
        count_sql(db, "SELECT COUNT(*) FROM (SELECT publisher FROM items.item WHERE COALESCE(publisher,'')<>'' GROUP BY publisher HAVING COUNT(*)>=3)", &m->eligible_publishers);
        if (table_exists(db, "cms", "internet_source"))
            count_sql(db, "SELECT COUNT(DISTINCT s.source) FROM cms.internet_source s JOIN items.item i ON i.source=s.source WHERE s.status='published' AND COALESCE(i.publisher,'')<>'' AND (COALESCE(s.publisher_ident,'')='' OR COALESCE(s.publisher_name,'')='')", &m->retained_source_identity_gaps);
    }
    m->site_presentation_table = table_exists(db, "cms", "site_presentation");
    if (m->site_presentation_table)
        count_sql(db, "SELECT COUNT(DISTINCT subject_address) FROM cms.site_presentation WHERE status='published' AND subject_address LIKE 'institution:publisher:%'", &m->addressed_publishers);
    m->publisher_address_gaps = m->eligible_publishers > m->addressed_publishers ? m->eligible_publishers - m->addressed_publishers : 0;

    if (m->fabric_db) {
        m->subject_bundle_table = table_exists(db, "fabric", "subject_bundle");
        m->subject_reader_table = table_exists(db, "fabric", "subject_reader");
        m->subject_action_table = table_exists(db, "fabric", "subject_action");
        m->subject_scene_table = table_exists(db, "fabric", "subject_scene");
        if (m->subject_bundle_table) count_sql(db, "SELECT COUNT(*) FROM fabric.subject_bundle", &m->subject_bundles);
        if (m->subject_reader_table) count_sql(db, "SELECT COUNT(*) FROM fabric.subject_reader WHERE state IN ('admitted','active')", &m->subject_readers);
        if (m->subject_action_table) count_sql(db, "SELECT COUNT(*) FROM fabric.subject_action", &m->subject_actions);
        if (m->subject_scene_table) count_sql(db, "SELECT COUNT(*) FROM fabric.subject_scene", &m->subject_scenes);
        if (m->site_presentation_table && m->subject_bundle_table)
            count_sql(db, "SELECT COUNT(DISTINCT p.subject_address) FROM cms.site_presentation p WHERE p.status='published' AND p.subject_address LIKE 'institution:publisher:%' AND NOT EXISTS (SELECT 1 FROM fabric.subject_bundle b WHERE b.subject=p.subject_address)", &m->addressed_publishers_without_bundle);
        if (m->site_presentation_table && m->subject_reader_table)
            count_sql(db, "SELECT COUNT(DISTINCT p.subject_address) FROM cms.site_presentation p WHERE p.status='published' AND p.subject_address LIKE 'institution:publisher:%' AND NOT EXISTS (SELECT 1 FROM fabric.subject_reader r WHERE r.subject=p.subject_address AND r.state IN ('admitted','active'))", &m->addressed_publishers_without_reader);
    }

    if (m->frontier_db && table_exists(db, "frontier", "channel")) {
        count_sql(db, "SELECT COUNT(*) FROM frontier.channel WHERE state IN ('admitted','active') AND COALESCE(census_source,'')<>''", &m->admitted_channels);
        if (table_exists(db, "cms", "internet_source"))
            count_sql(db, "SELECT COUNT(DISTINCT f.census_source) FROM frontier.channel f JOIN cms.internet_source s ON s.source=f.census_source AND s.status='published' WHERE f.state IN ('admitted','active') AND COALESCE(f.census_source,'')<>''", &m->admitted_published_sources);
        if (m->fabric_db && table_exists(db, "fabric", "subject_reader") && table_exists(db, "cms", "internet_source"))
            count_sql(db, "SELECT COUNT(DISTINCT f.census_source) FROM frontier.channel f JOIN cms.internet_source s ON s.source=f.census_source AND s.status='published' JOIN fabric.subject_reader r ON r.census_source=f.census_source AND r.state IN ('admitted','active') WHERE f.state IN ('admitted','active') AND COALESCE(f.census_source,'')<>''", &m->admitted_published_sources_with_reader);
    }
    if (m->frontier_db && table_exists(db, "frontier", "channel") && table_exists(db, "frontier", "root")) {
        count_sql(db, "SELECT COUNT(DISTINCT c.root_id) FROM frontier.channel c WHERE c.state IN ('admitted','active') AND COALESCE(c.census_source,'')<>''", &m->lake_roots);
        count_sql(db, "SELECT COUNT(DISTINCT c.root_id) FROM frontier.channel c JOIN frontier.root r ON r.root_id=c.root_id WHERE c.state IN ('admitted','active') AND COALESCE(c.census_source,'')<>'' AND (r.identity GLOB 'Q[1-9]*' OR c.identity GLOB 'Q[1-9]*')", &m->lake_roots_with_identity);
    }
    m->frontier_gap_table = m->frontier_db && table_exists(db, "frontier", "gap");
    if (m->frontier_gap_table)
        count_sql(db, "SELECT COUNT(*) FROM frontier.gap WHERE kind='IDENTITY' AND status='open'", &m->identity_gaps_open);
    if (m->fabric_db && m->items_db && table_exists(db, "items", "item") && table_exists(db, "fabric", "subject_reader") &&
        table_exists(db, "fabric", "subject_bundle")) {
        m->lake_measured = 1;
        /* An item has a subject when a subject this estate composes reads its
         * source; the estate's own territories read everything and do not count. */
        count_sql(db, "WITH read_by(source) AS (SELECT DISTINCT census_source FROM fabric.subject_reader WHERE state IN ('admitted','active') AND subject NOT LIKE 'institution:aurekai:%') "
                      "SELECT COUNT(*) FROM items.item i WHERE i.source IN (SELECT source FROM read_by)", &m->items_with_subject);
        count_sql(db, "SELECT COUNT(DISTINCT subject) FROM fabric.subject_bundle WHERE subject NOT LIKE '/%' AND subject NOT LIKE 'institution:aurekai:%'", &m->lake_subjects);
        count_sql(db, "SELECT COUNT(DISTINCT b.subject) FROM fabric.subject_bundle b WHERE b.subject NOT LIKE '/%' AND b.subject NOT LIKE 'institution:aurekai:%' AND EXISTS (SELECT 1 FROM fabric.subject_reader r WHERE r.subject=b.subject AND r.state IN ('admitted','active'))", &m->lake_subjects_with_reader);
        if (m->site_presentation_table) {
            count_sql(db, "SELECT COUNT(DISTINCT b.subject) FROM fabric.subject_bundle b WHERE b.subject NOT LIKE '/%' AND b.subject NOT LIKE 'institution:aurekai:%' AND EXISTS (SELECT 1 FROM cms.site_presentation p WHERE p.status='published' AND p.subject_address=b.subject)", &m->lake_subjects_with_site);
            count_sql(db, "SELECT COUNT(DISTINCT b.subject) FROM fabric.subject_bundle b WHERE b.subject NOT LIKE '/%' AND b.subject NOT LIKE 'institution:aurekai:%' AND EXISTS (SELECT 1 FROM cms.site_presentation p WHERE p.status='published' AND p.subject_address=b.subject) AND EXISTS (SELECT 1 FROM fabric.subject_reader r WHERE r.subject=b.subject AND r.state IN ('admitted','active'))", &m->lake_subjects_with_site_and_reader);
            count_sql(db, "SELECT COUNT(DISTINCT p.subject_address) FROM cms.site_presentation p WHERE p.status='published' AND p.subject_address LIKE 'institution:%' AND p.subject_address NOT LIKE 'institution:aurekai:%' AND NOT EXISTS (SELECT 1 FROM fabric.subject_reader r WHERE r.subject=p.subject_address AND r.state IN ('admitted','active'))", &m->lake_sites_without_reader);
        }
    }
    m->admitted_published_sources_without_reader =
        m->admitted_published_sources > m->admitted_published_sources_with_reader ?
        m->admitted_published_sources - m->admitted_published_sources_with_reader : 0;

    m->program_table = table_exists(db, "cms", "program_instance");
    m->source_collection_table = table_exists(db, "cms", "source_collection");
    m->surface_scene_table = table_exists(db, "cms", "surface_scene");
    if (m->program_table) {
        count_sql(db, "SELECT COUNT(*) FROM cms.program_instance WHERE status<>'retired'", &m->program_instances);
        if (m->source_collection_table)
            count_sql(db, "SELECT COUNT(*) FROM cms.program_instance p WHERE p.status<>'retired' AND NOT EXISTS (SELECT 1 FROM cms.source_collection c WHERE c.status<>'retired' AND c.subject_address=p.owner_subject)", &m->program_subjects_without_collection);
    }
    if (m->source_collection_table)
        count_sql(db, "SELECT COUNT(*) FROM cms.source_collection WHERE status<>'retired'", &m->source_collections);
    if (m->surface_scene_table)
        count_sql(db, "SELECT COUNT(*) FROM cms.surface_scene WHERE status='published'", &m->surface_scenes);

    if (m->account_db) {
        m->account_work_table = table_exists(db, "acct", "account_work");
        m->account_run_table = table_exists(db, "acct", "account_work_run");
        m->account_step_table = table_exists(db, "acct", "account_work_step");
        if (m->account_work_table) count_sql(db, "SELECT COUNT(*) FROM acct.account_work", &m->account_tasks);
        if (m->account_run_table) count_sql(db, "SELECT COUNT(*) FROM acct.account_work_run", &m->account_runs);
        if (m->account_step_table) {
            count_sql(db, "SELECT COUNT(*) FROM acct.account_work_step", &m->account_steps);
            m->account_output_column = count_sql(db,
                "SELECT COUNT(*) FROM acct.account_work_step WHERE COALESCE(output_ref,'')<>''", &m->account_outputs);
        }
        if (table_exists(db, "acct", "account_work_meta")) {
            int current = 0;
            if (count_sql(db, "SELECT COUNT(*) FROM acct.account_work_meta WHERE key='indexed_ms' AND value >= CAST((julianday('now')-2440587.5)*86400000 AS INTEGER)-60000", &current))
                m->account_index_current = current > 0;
        }
        if (table_exists(db, "acct", "queue_inbox_states"))
            count_sql(db, "SELECT COUNT(*) FROM acct.queue_inbox_states", &m->inbox_states);
        if (table_exists(db, "acct", "queue_inbox_operations"))
            count_sql(db, "SELECT COUNT(*) FROM acct.queue_inbox_operations", &m->inbox_operations);
    }
    if (m->meter_db) {
        m->meter_events_table = table_exists(db, "meter", "events");
        m->meter_rate_table = table_exists(db, "meter", "rate");
        if (m->meter_events_table) {
            count_sql(db, "SELECT COUNT(*) FROM meter.events", &m->meter_events);
            count_sql(db, "SELECT COUNT(*) FROM meter.events WHERE cost IS NULL", &m->meter_unpriced_events);
        }
        if (m->meter_rate_table) count_sql(db, "SELECT COUNT(*) FROM meter.rate", &m->meter_rates);
    }


    if (m->control_db) {
        m->evidence_relation_table = table_exists(db, "control", "evidence_relations");
        m->foreign_observation_table = table_exists(db, "control", "foreign_twin_observations");
        m->frontier_layer_table = table_exists(db, "control", "frontier_layers");
        m->solved_invariant_table = table_exists(db, "control", "solved_invariants");
        m->known_noncause_table = table_exists(db, "control", "known_noncauses");
        m->evidence_sync_table = table_exists(db, "control", "estate_evidence_sync_receipts");
        if (m->evidence_relation_table) count_sql(db, "SELECT COUNT(*) FROM control.evidence_relations", &m->evidence_relations);
        if (m->foreign_observation_table) count_sql(db, "SELECT COUNT(*) FROM control.foreign_twin_observations", &m->foreign_observations);
        if (m->frontier_layer_table) count_sql(db, "SELECT COUNT(*) FROM control.frontier_layers", &m->frontier_layers);
        if (m->solved_invariant_table) {
            count_sql(db, "SELECT COUNT(*) FROM control.solved_invariants WHERE truth_plane='measured'", &m->measured_invariants);
            count_sql(db, "SELECT COUNT(*) FROM control.solved_invariants WHERE truth_plane='proven' AND status NOT IN ('challenged','invalidated')", &m->proven_invariants);
            count_sql(db, "SELECT COUNT(*) FROM control.solved_invariants WHERE status IN ('challenged','invalidated')", &m->challenged_invariants);
        }
        if (m->known_noncause_table) count_sql(db, "SELECT COUNT(*) FROM control.known_noncauses", &m->known_noncauses);
        if (m->evidence_sync_table) {
            count_sql(db, "SELECT COALESCE((SELECT considered FROM control.estate_evidence_sync_receipts ORDER BY synced_at DESC,sync_id DESC LIMIT 1),0)", &m->evidence_sync_considered);
            count_sql(db, "SELECT COALESCE((SELECT exact FROM control.estate_evidence_sync_receipts ORDER BY synced_at DESC,sync_id DESC LIMIT 1),0)", &m->evidence_sync_exact);
            count_sql(db, "SELECT COALESCE((SELECT unresolved FROM control.estate_evidence_sync_receipts ORDER BY synced_at DESC,sync_id DESC LIMIT 1),0)", &m->evidence_sync_unresolved);
            count_sql(db, "SELECT COUNT(*) FROM (SELECT publication_count,publication_watermark FROM control.estate_evidence_sync_receipts ORDER BY synced_at DESC,sync_id DESC LIMIT 1) r WHERE r.publication_count=(SELECT COUNT(*) FROM cms.publication_occurrence WHERE status='published' AND assertion_state='OBSERVED') AND r.publication_watermark=(SELECT COALESCE(MAX(updated_at),'') FROM cms.publication_occurrence WHERE status='published' AND assertion_state='OBSERVED')", &m->evidence_sync_current);
        }
    }

    m->maturity_debt = m->missing_owner_dbs + m->retained_source_identity_gaps + m->publisher_address_gaps +
        m->addressed_publishers_without_bundle + m->addressed_publishers_without_reader +
        m->admitted_published_sources_without_reader + m->program_subjects_without_collection;
    return 1;
}

static int bps(int numerator, int denominator) {
    if (denominator <= 0) return 10000;
    if (numerator < 0) numerator = 0;
    if (numerator > denominator) numerator = denominator;
    return (int)((long long)numerator * 10000LL / denominator);
}

static const char *axis_identity(const EstateMetrics *m) {
    if (!m->cms_db || !m->items_db) return "unmeasured";
    return m->retained_source_identity_gaps ? "backfill" : "covered";
}
static const char *axis_address(const EstateMetrics *m) {
    if (!m->site_presentation_table) return "unmeasured";
    return m->publisher_address_gaps ? "backfill" : "covered";
}
static const char *axis_material(const EstateMetrics *m) {
    if (!m->frontier_db || !m->subject_reader_table) return "unmeasured";
    return m->admitted_published_sources_without_reader ? "backfill" : "covered";
}
static const char *axis_subject(const EstateMetrics *m) {
    if (!m->subject_bundle_table || !m->subject_reader_table || !m->subject_scene_table) return "unmeasured";
    if (m->addressed_publishers_without_bundle || m->addressed_publishers_without_reader ||
        (m->addressed_publishers > 0 && m->subject_scenes == 0)) return "backfill";
    return "covered";
}
static const char *axis_program(const EstateMetrics *m) {
    if (!m->program_table || !m->source_collection_table) return "unmeasured";
    return m->program_subjects_without_collection ? "backfill" : "covered";
}
static const char *axis_surface(const EstateMetrics *m) {
    if (!m->surface_scene_table) return "unmeasured";
    return m->surface_scenes ? "covered" : "backfill";
}
static const char *axis_action(const EstateMetrics *m) {
    if (!m->subject_action_table) return "unmeasured";
    return m->subject_actions ? "covered" : "backfill";
}
static const char *axis_account(const EstateMetrics *m) {
    if (!m->account_db || !m->meter_db || !m->account_work_table || !m->account_run_table ||
        !m->account_step_table || !m->account_output_column || !m->meter_events_table || !m->meter_rate_table)
        return "unmeasured";
    return m->account_index_current ? "covered" : "backfill";
}
static const char *axis_evidence(const EstateMetrics *m) {
    if (!m->control_db || !m->evidence_relation_table || !m->foreign_observation_table ||
        !m->frontier_layer_table || !m->solved_invariant_table || !m->known_noncause_table || !m->evidence_sync_table)
        return "unmeasured";
    return m->evidence_sync_current ? "covered" : "backfill";
}
static int axis_open_count(const EstateMetrics *m) {
    int open = 0;
    open += strcmp(axis_identity(m), "covered") != 0;
    open += strcmp(axis_address(m), "covered") != 0;
    open += strcmp(axis_material(m), "covered") != 0;
    open += strcmp(axis_subject(m), "covered") != 0;
    open += strcmp(axis_program(m), "covered") != 0;
    open += strcmp(axis_surface(m), "covered") != 0;
    open += strcmp(axis_action(m), "covered") != 0;
    open += strcmp(axis_account(m), "covered") != 0;
    open += strcmp(axis_evidence(m), "covered") != 0;
    return open;
}
#include "account_maturity_print.h"

static void print_estate_metrics(const EstateMetrics *m) {
    const int cross_axis_open = axis_open_count(m);
    printf("{\"schema\":\"bonfyre-estate-maturity-audit.v2\","
        "\"owner_databases\":{\"cms\":%s,\"items\":%s,\"frontier\":%s,\"subject_fabric\":%s,\"missing\":%d},"
        "\"sources\":{\"published\":%d,\"publisher_identity\":%d,\"retained_items\":%d,\"retained_sources\":%d,\"retained_publishers\":%d,\"retained_source_identity_gaps\":%d,\"identity_coverage_bps\":%d},"
        "\"subjects\":{\"eligible_publishers\":%d,\"addressed_publishers\":%d,\"publisher_address_gaps\":%d,\"bundles\":%d,\"readers\":%d,\"actions\":%d,\"scenes\":%d,\"addressed_without_bundle\":%d,\"addressed_without_reader\":%d,\"address_coverage_bps\":%d},"
        "\"frontier\":{\"admitted_channels\":%d,\"admitted_published_sources\":%d,\"with_reader\":%d,\"without_reader\":%d,\"reader_coverage_bps\":%d},"
        "\"programs\":{\"instances\":%d,\"source_collections\":%d,\"subjects_without_collection\":%d,\"collection_coverage_bps\":%d},"
        "\"surface_scenes\":%d,\"maturity_debt\":%d,\"core_maturity_debt\":%d,"
        "\"axes\":{\"identity\":\"%s\",\"address\":\"%s\",\"material\":\"%s\",\"subject\":\"%s\","
        "\"program\":\"%s\",\"surface\":\"%s\",\"action\":\"%s\",\"account\":\"unmeasured\",\"evidence\":\"unmeasured\"},"
        "\"cross_axis_open\":%d,\"whole_estate_ready\":%s,\"gate_scope\":\"local_estate_core\","
        "\"truth\":\"row debt and axis maturity are separate; unmeasured owners never count as covered\"}\n",
        m->cms_db ? "true" : "false", m->items_db ? "true" : "false", m->frontier_db ? "true" : "false", m->fabric_db ? "true" : "false", m->missing_owner_dbs,
        m->published_sources, m->sources_with_publisher_identity, m->retained_items, m->retained_sources, m->retained_publishers, m->retained_source_identity_gaps,
        bps(m->published_sources - m->retained_source_identity_gaps, m->published_sources),
        m->eligible_publishers, m->addressed_publishers, m->publisher_address_gaps, m->subject_bundles, m->subject_readers, m->subject_actions, m->subject_scenes,
        m->addressed_publishers_without_bundle, m->addressed_publishers_without_reader, bps(m->addressed_publishers, m->eligible_publishers),
        m->admitted_channels, m->admitted_published_sources, m->admitted_published_sources_with_reader, m->admitted_published_sources_without_reader,
        bps(m->admitted_published_sources_with_reader, m->admitted_published_sources),
        m->program_instances, m->source_collections, m->program_subjects_without_collection,
        bps(m->program_instances - m->program_subjects_without_collection, m->program_instances),
        m->surface_scenes, m->maturity_debt, m->maturity_debt,
        axis_identity(m), axis_address(m), axis_material(m), axis_subject(m),
        axis_program(m), axis_surface(m), axis_action(m),
        cross_axis_open, (m->maturity_debt == 0 && cross_axis_open == 0) ? "true" : "false");
}

static int estate_audit(sqlite3 *db, const char *estate, int gate) {
    EstateMetrics m;
    if (!load_estate(db, estate, &m)) {
        fprintf(stderr, "estate maturity: cannot open %s/cms.db\n", estate);
        return 0;
    }
    print_estate_metrics_account(&m);
    return gate ? (m.maturity_debt == 0 && axis_open_count(&m) == 0) : 1;
    print_estate_metrics(&m);
    return gate ? m.maturity_debt == 0 : 1;
}

static void plan_step(int *comma, const char *owner, const char *action, int gap) {
    if (gap <= 0) return;
    printf("%s{\"owner\":\"%s\",\"gap\":%d,\"action\":\"%s\"}", *comma ? "," : "", owner, gap, action);
    *comma = 1;
}

static int backfill_plan(sqlite3 *db, const char *estate) {
    EstateMetrics m;
    if (!load_estate(db, estate, &m)) return 0;
    printf("{\"schema\":\"bonfyre-estate-maturity-backfill-plan.v2\",\"core_maturity_debt\":%d,\"cross_axis_open\":%d,\"steps\":[",
           m.maturity_debt, axis_open_count(&m));
    int comma = 0;
    plan_step(&comma, "Estate owners", "restore missing owner projections before claiming measurable maturity", m.missing_owner_dbs);
    plan_step(&comma, "BonfyreSourceFrontier", "PUBLIC_BACKFILL observed publisher identities; route unresolved cases to DISCOVER or HUMAN_VERIFY instead of inventing identity", m.retained_source_identity_gaps);
    plan_step(&comma, "BonfyreWebAddress", "address observed publishers through the existing publisher subject and site-presentation path", m.publisher_address_gaps);
    plan_step(&comma, "SubjectFabric", "recompute dense subject bundles from exact owners before lowering any page slice", m.addressed_publishers_without_bundle);
    plan_step(&comma, "SubjectFabric", "reconcile publisher_readers and admitted territory readers from current source/frontier standing", m.addressed_publishers_without_reader + m.admitted_published_sources_without_reader);
    plan_step(&comma, "ProgramFabric", "recompile affected program instances so every retained instance keeps its owner-subject source collection", m.program_subjects_without_collection);
    plan_step(&comma, "SubjectFabric/BonfyreSurfaceScene", "backfill dense subject scenes when subject closure exists but no scene projection is measured", strcmp(axis_subject(&m), "covered") != 0);
    plan_step(&comma, "SubjectFabric/AuthorityGraph", "backfill exact owner-backed subject actions; zero actions is not mature merely because readers exist", strcmp(axis_action(&m), "covered") != 0);
    plan_step(&comma, "BonfyreSurfaceScene/BonfyreWebAddress", "join live surface-scene and browser-witness coverage; subject scenes alone do not prove live surface maturity", strcmp(axis_surface(&m), "covered") != 0);
    if (strcmp(axis_account(&m), "covered") != 0)
    plan_step(&comma, "Fabric/AurekaiActivity/BonfyreMeter", "join account-scoped agents activity outputs inbox and measured usage into the estate maturity cut; missing usage stays unreported", 1);
    if (strcmp(axis_evidence(&m), "covered") != 0)
    plan_step(&comma, "EvidenceGraph/ProofFrontierGraph/ForeignTwinGraph", "synchronize the declared control-plane evidence store from exact retained observations; measured is never upgraded to proven", 1);
    printf("],\"truth\":\"existing owners only; unmeasured axes are work, never implicit green\"}\n");
    return 1;
}

/* ------------------------------------------------------------ lake check
 *
 * The live sweep's regression reader for the source lake.  It prints one
 * tab-separated FAIL or NOTE line per regression in the form the sweep folds
 * into operator goals, and nothing for a lake that held.  A floor file keeps
 * the last good cut ("name value" per line); it is raised only by a cut with
 * no FAIL, so coverage can climb and never silently fall back.  Nothing here
 * writes an owner store or repairs anything. */
typedef struct { const char *name; int higher_is_better; int fail_on_drop; } LakeFloor;
static const LakeFloor LAKE_FLOORS[] = {
    {"sources_with_publisher_identity", 1, 1},
    {"source_roots_with_identity", 1, 1},
    {"items_with_subject", 1, 1},
    {"subjects_with_site_and_reader", 1, 1},
    {"unresolved_identity_gaps", 0, 0},
};

static int lake_value(const EstateMetrics *m, const char *name) {
    if (!strcmp(name, "sources_with_publisher_identity")) return m->sources_with_publisher_identity;
    if (!strcmp(name, "source_roots_with_identity")) return m->lake_roots_with_identity;
    if (!strcmp(name, "items_with_subject")) return m->items_with_subject;
    if (!strcmp(name, "subjects_with_site_and_reader")) return m->lake_subjects_with_site_and_reader;
    if (!strcmp(name, "unresolved_identity_gaps")) return m->identity_gaps_open;
    return 0;
}

static int lake_floor_read(const char *path, const char *name, int *out) {
    FILE *f = path && *path ? fopen(path, "r") : NULL;
    char key[96]; int value;
    if (!f) return 0;
    while (fscanf(f, "%95s %d", key, &value) == 2)
        if (!strcmp(key, name)) { *out = value; fclose(f); return 1; }
    fclose(f);
    return 0;
}

static int lake_check(sqlite3 *db, const char *estate, const char *floor_path, const char *write_path) {
    EstateMetrics m;
    size_t i;
    int fails = 0, notes = 0;
    if (!load_estate(db, estate, &m)) {
        fprintf(stderr, "lake check: cannot open %s/cms.db\n", estate);
        return 0;
    }
    if (!m.lake_measured) {
        printf("FAIL\testate\tsubject-lake\tthe item archive or subject fabric cannot be read, so subject coverage is unmeasured\n");
        return 1;
    }
    if (m.addressed_publishers_without_bundle > 0) {
        printf("FAIL\testate\tsubject-lake\t%d addressed publishers have no subject bundle\n", m.addressed_publishers_without_bundle); fails++;
    }
    if (m.addressed_publishers_without_reader > 0) {
        printf("FAIL\testate\tsubject-lake\t%d addressed publishers have no reader\n", m.addressed_publishers_without_reader); fails++;
    }
    if (m.admitted_published_sources_without_reader > 0) {
        printf("FAIL\testate\tsubject-lake\t%d admitted sources reach no reader\n", m.admitted_published_sources_without_reader); fails++;
    }
    for (i = 0; i < sizeof(LAKE_FLOORS) / sizeof(LAKE_FLOORS[0]); i++) {
        int floor = 0, now = lake_value(&m, LAKE_FLOORS[i].name);
        if (!lake_floor_read(floor_path, LAKE_FLOORS[i].name, &floor)) continue;
        if (LAKE_FLOORS[i].higher_is_better ? now < floor : now > floor) {
            printf("%s\testate\tsubject-lake\t%s %s from %d to %d\n", LAKE_FLOORS[i].fail_on_drop ? "FAIL" : "NOTE",
                   LAKE_FLOORS[i].name, LAKE_FLOORS[i].higher_is_better ? "fell" : "rose", floor, now);
            if (LAKE_FLOORS[i].fail_on_drop) fails++; else notes++;
        }
    }
    if (write_path && *write_path && !fails) {
        char tmp[4096];
        FILE *f;
        if (snprintf(tmp, sizeof(tmp), "%s.next", write_path) >= (int)sizeof(tmp) || !(f = fopen(tmp, "w"))) {
            fprintf(stderr, "lake check: cannot write floor %s\n", write_path);
            return 0;
        }
        for (i = 0; i < sizeof(LAKE_FLOORS) / sizeof(LAKE_FLOORS[0]); i++)
            fprintf(f, "%s %d\n", LAKE_FLOORS[i].name, lake_value(&m, LAKE_FLOORS[i].name));
        if (fclose(f) != 0 || rename(tmp, write_path) != 0) { remove(tmp); fprintf(stderr, "lake check: floor not moved into place\n"); return 0; }
    }
    printf("LAKE\testate\t{\"fails\":%d,\"notes\":%d,\"sources_with_publisher_identity\":%d,\"source_roots_with_identity\":%d,"
           "\"items_with_subject\":%d,\"subjects_with_site_and_reader\":%d,\"unresolved_identity_gaps\":%d,\"floor_written\":%s}\n",
           fails, notes, m.sources_with_publisher_identity, m.lake_roots_with_identity, m.items_with_subject,
           m.lake_subjects_with_site_and_reader, m.identity_gaps_open, write_path && *write_path && !fails ? "true" : "false");
    return 1;
}

static const char *opt(int argc, char **argv, const char *name) {
    for (int i = 3; i + 1 < argc; i++) if (!strcmp(argv[i], name)) return argv[i + 1];
    return NULL;
}

int main(int argc, char **argv) {
    sqlite3 *db = NULL;
    int ok = 0;
    if (argc < 2 || sqlite3_open(":memory:", &db) != SQLITE_OK) return 2;
    enum {
        SM_AUDIT, SM_AXES, SM_BACKFILL_PLAN, SM_CLOSURE,
        SM_ESTATE_AUDIT, SM_ESTATE_GATE, SM_LAKE_CHECK, SM_LIST,
    };
    static const BfCommandEntry sm_commands[] = {
        {"audit", SM_AUDIT, 0, NULL},
        {"axes", SM_AXES, 0, NULL},
        {"backfill-plan", SM_BACKFILL_PLAN, 0, NULL},
        {"closure", SM_CLOSURE, 0, NULL},
        {"estate-audit", SM_ESTATE_AUDIT, 0, NULL},
        {"estate-gate", SM_ESTATE_GATE, 0, NULL},
        {"lake-check", SM_LAKE_CHECK, 0, NULL},
        {"list", SM_LIST, 0, NULL},
    };
    const BfCommandEntry *command = bf_command_lookup(sm_commands,
        sizeof(sm_commands) / sizeof(sm_commands[0]), argv[1]);
    if (command && command->id == SM_AUDIT) ok = audit(db);
    else if (command && command->id == SM_LIST) ok = list(db);
    else if (command && command->id == SM_AXES) ok = axes(db);
    else if (command && command->id == SM_CLOSURE && argc == 4) ok = closure(db, argv[2], argv[3]);
    else if (command && command->id == SM_ESTATE_AUDIT && argc == 3) ok = estate_audit(db, argv[2], 0);
    else if (command && command->id == SM_ESTATE_GATE && argc == 3) ok = estate_audit(db, argv[2], 1);
    else if (command && command->id == SM_BACKFILL_PLAN && argc == 3) ok = backfill_plan(db, argv[2]);
    else if (command && command->id == SM_LAKE_CHECK && argc >= 3 && argc % 2 == 1)
        ok = lake_check(db, argv[2], opt(argc, argv, "--floor"), opt(argc, argv, "--write-floor"));
    else fprintf(stderr, "usage: bonfyre-surface-maturity audit | list | axes | closure SURFACE WORLD | estate-audit ESTATE | estate-gate ESTATE | backfill-plan ESTATE | lake-check ESTATE [--floor FILE] [--write-floor FILE]\n");
    sqlite3_close(db);
    return ok ? 0 : 1;
}
