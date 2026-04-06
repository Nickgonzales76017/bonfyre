/* compact_bindings.c — SQLite storage layer for compact bindings
 *
 * Compression logic has been extracted to liblambda-tensors.
 * This file contains only the SQLite persistence functions:
 *   - compact_bindings_bootstrap() — schema migration
 *   - compact_pack_family()        — pack a family into storage
 *   - compact_pack_content_type()  — pack all families of a type
 */

#include "compact_bindings.h"
#include "bench_metrics.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>


typedef struct {
    int target_id;
    char *json;
    unsigned char *plain_packed;
    size_t plain_len;
    unsigned char *interned_packed;
    size_t interned_len;
    unsigned char *delta;
    size_t delta_len;
    int use_delta;
} StoredFamilyMember;

static void free_stored_family_members(StoredFamilyMember *members, int count) {
    if (!members) return;
    for (int i = 0; i < count; i++) {
        free(members[i].json);
        free(members[i].plain_packed);
        free(members[i].interned_packed);
        free(members[i].delta);
    }
    free(members);
}

static int clear_family_compact_metadata(sqlite3 *db, int family_id) {
    sqlite3_stmt *stmt = NULL;
    int rc = -1;

    if (sqlite3_prepare_v2(db,
        "UPDATE _families "
        "SET compact_codec=NULL, compact_header=NULL, ref_binding=NULL "
        "WHERE id=?1",
        -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_int(stmt, 1, family_id);
    rc = (sqlite3_step(stmt) == SQLITE_DONE) ? 0 : -1;
    sqlite3_finalize(stmt);
    return rc;
}

int compact_pack_family(sqlite3 *db, int family_id, const char *content_type) {
    sqlite3_stmt *query = NULL, *upd_family = NULL, *upd_member = NULL;
    StoredFamilyMember *members = NULL;
    int member_count = 0, member_cap = 0;
    FamilyStringTable strtab;
    unsigned char *header = NULL;
    size_t header_len = 0;
    unsigned char *ref_packed = NULL;
    size_t ref_len = 0;
    sqlite3_int64 plain_total = 0;
    sqlite3_int64 interned_total = 0;
    sqlite3_int64 bytes_written_total = 0;
    const char *codec = COMPACT_CODEC_V2_PACKED;
    int rc = -1;

    if (!db || family_id <= 0 || !content_type) return -1;

    family_strtab_init(&strtab);

    if (sqlite3_prepare_v2(db,
        "SELECT target_id, bindings FROM _family_members "
        "WHERE family_id=?1 AND target_type=?2 "
        "ORDER BY target_id",
        -1, &query, NULL) != SQLITE_OK) {
        goto cleanup;
    }
    sqlite3_bind_int(query, 1, family_id);
    sqlite3_bind_text(query, 2, content_type, -1, SQLITE_STATIC);

    while (sqlite3_step(query) == SQLITE_ROW) {
        const char *json = (const char *)sqlite3_column_text(query, 1);
        StoredFamilyMember *next;
        if (!json) continue;
        if (member_count >= member_cap) {
            member_cap = member_cap ? member_cap * 2 : 8;
            next = realloc(members, (size_t)member_cap * sizeof(*members));
            if (!next) goto cleanup;
            members = next;
        }
        memset(&members[member_count], 0, sizeof(members[member_count]));
        members[member_count].target_id = sqlite3_column_int(query, 0);
        members[member_count].json = strdup(json);
        if (!members[member_count].json) goto cleanup;
        member_count++;
    }
    sqlite3_finalize(query);
    query = NULL;

    if (member_count == 0) {
        rc = clear_family_compact_metadata(db, family_id);
        goto cleanup;
    }

    for (int i = 0; i < member_count; i++) {
        if (compact_encode_v2(members[i].json,
                              &members[i].plain_packed,
                              &members[i].plain_len) < 0) {
            goto cleanup;
        }
        plain_total += (sqlite3_int64)members[i].plain_len;
        if (family_strtab_ingest(&strtab, members[i].json) != 0) goto cleanup;
    }

    family_strtab_finalize(&strtab);
    if (family_strtab_encode_header(&strtab, &header, &header_len) != 0) goto cleanup;
    if (compact_encode_v2_interned(members[0].json, &strtab, &ref_packed, &ref_len) < 0)
        goto cleanup;

    interned_total = (sqlite3_int64)header_len + (sqlite3_int64)ref_len;
    for (int i = 0; i < member_count; i++) {
        if (compact_encode_v2_interned(members[i].json, &strtab,
                                       &members[i].interned_packed,
                                       &members[i].interned_len) < 0) {
            goto cleanup;
        }
        if (compact_delta_encode_v2(ref_packed, ref_len,
                                    members[i].interned_packed, members[i].interned_len,
                                    &members[i].delta, &members[i].delta_len) < 0) {
            goto cleanup;
        }
        members[i].use_delta = (members[i].delta_len <= members[i].interned_len);
        interned_total += 1 + (sqlite3_int64)(members[i].use_delta
            ? members[i].delta_len
            : members[i].interned_len);
    }

    if (interned_total < plain_total)
        codec = COMPACT_CODEC_V2_INTERNED;

    if (sqlite3_prepare_v2(db,
        "UPDATE _families "
        "SET compact_codec=?1, compact_header=?2, ref_binding=?3 "
        "WHERE id=?4",
        -1, &upd_family, NULL) != SQLITE_OK) {
        goto cleanup;
    }
    sqlite3_bind_text(upd_family, 1, codec, -1, SQLITE_STATIC);
    if (strcmp(codec, COMPACT_CODEC_V2_INTERNED) == 0) {
        sqlite3_bind_blob(upd_family, 2, header, (int)header_len, SQLITE_TRANSIENT);
        sqlite3_bind_blob(upd_family, 3, ref_packed, (int)ref_len, SQLITE_TRANSIENT);
        bytes_written_total += (sqlite3_int64)header_len + (sqlite3_int64)ref_len;
    } else {
        sqlite3_bind_null(upd_family, 2);
        sqlite3_bind_null(upd_family, 3);
    }
    sqlite3_bind_int(upd_family, 4, family_id);
    if (sqlite3_step(upd_family) != SQLITE_DONE) goto cleanup;

    if (sqlite3_prepare_v2(db,
        "UPDATE _family_members SET bindings_packed=?1 "
        "WHERE family_id=?2 AND target_type=?3 AND target_id=?4",
        -1, &upd_member, NULL) != SQLITE_OK) {
        goto cleanup;
    }

    for (int i = 0; i < member_count; i++) {
        const unsigned char *payload = NULL;
        size_t payload_len = 0;
        unsigned char *stored = NULL;
        size_t stored_len = 0;

        sqlite3_reset(upd_member);
        sqlite3_clear_bindings(upd_member);

        if (strcmp(codec, COMPACT_CODEC_V2_INTERNED) == 0) {
            unsigned char mode = (unsigned char)(members[i].use_delta
                ? CB_STORED_DELTA
                : CB_STORED_PACKED);
            payload = members[i].use_delta ? members[i].delta : members[i].interned_packed;
            payload_len = members[i].use_delta ? members[i].delta_len : members[i].interned_len;
            stored_len = payload_len + 1;
            stored = malloc(stored_len);
            if (!stored) goto cleanup;
            stored[0] = mode;
            memcpy(stored + 1, payload, payload_len);
            sqlite3_bind_blob(upd_member, 1, stored, (int)stored_len, SQLITE_TRANSIENT);
            bytes_written_total += (sqlite3_int64)stored_len;
        } else {
            sqlite3_bind_blob(upd_member, 1, members[i].plain_packed, (int)members[i].plain_len,
                              SQLITE_TRANSIENT);
            bytes_written_total += (sqlite3_int64)members[i].plain_len;
        }

        sqlite3_bind_int(upd_member, 2, family_id);
        sqlite3_bind_text(upd_member, 3, content_type, -1, SQLITE_STATIC);
        sqlite3_bind_int(upd_member, 4, members[i].target_id);
        if (sqlite3_step(upd_member) != SQLITE_DONE) {
            free(stored);
            goto cleanup;
        }
        free(stored);
    }

    rc = 0;
    bench_metrics_record_family_repack(bytes_written_total);

cleanup:
    if (query) sqlite3_finalize(query);
    if (upd_family) sqlite3_finalize(upd_family);
    if (upd_member) sqlite3_finalize(upd_member);
    free(header);
    free(ref_packed);
    family_strtab_free(&strtab);
    free_stored_family_members(members, member_count);
    return rc;
}

int compact_pack_content_type(sqlite3 *db, const char *content_type) {
    sqlite3_stmt *stmt = NULL;
    int *family_ids = NULL;
    int count = 0, cap = 0;
    int rc = -1;

    if (!db || !content_type) return -1;

    if (sqlite3_prepare_v2(db,
        "SELECT id FROM _families WHERE content_type=?1 ORDER BY id",
        -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(stmt, 1, content_type, -1, SQLITE_STATIC);

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        int *next;
        if (count >= cap) {
            cap = cap ? cap * 2 : 8;
            next = realloc(family_ids, (size_t)cap * sizeof(*family_ids));
            if (!next) goto cleanup;
            family_ids = next;
        }
        family_ids[count++] = sqlite3_column_int(stmt, 0);
    }
    sqlite3_finalize(stmt);
    stmt = NULL;

    for (int i = 0; i < count; i++) {
        if (compact_pack_family(db, family_ids[i], content_type) != 0)
            goto cleanup;
    }

    rc = 0;

cleanup:
    if (stmt) sqlite3_finalize(stmt);
    free(family_ids);
    return rc;
}

/* ================================================================
 * Schema bootstrap
 * ================================================================ */

int compact_bindings_bootstrap(sqlite3 *db) {
    /* Add bindings_packed BLOB column to _family_members if missing */
    sqlite3_stmt *q;
    int has_packed = 0, has_ref = 0, has_header = 0, has_codec = 0;

    if (sqlite3_prepare_v2(db,
        "SELECT COUNT(*) FROM pragma_table_info('_family_members') WHERE name='bindings_packed'",
        -1, &q, NULL) == SQLITE_OK) {
        if (sqlite3_step(q) == SQLITE_ROW)
            has_packed = sqlite3_column_int(q, 0);
        sqlite3_finalize(q);
    }
    if (!has_packed) {
        sqlite3_exec(db,
            "ALTER TABLE _family_members ADD COLUMN bindings_packed BLOB",
            NULL, NULL, NULL);
    }

    /* Add ref_binding BLOB column to _families if missing */
    if (sqlite3_prepare_v2(db,
        "SELECT COUNT(*) FROM pragma_table_info('_families') WHERE name='ref_binding'",
        -1, &q, NULL) == SQLITE_OK) {
        if (sqlite3_step(q) == SQLITE_ROW)
            has_ref = sqlite3_column_int(q, 0);
        sqlite3_finalize(q);
    }
    if (!has_ref) {
        sqlite3_exec(db,
            "ALTER TABLE _families ADD COLUMN ref_binding BLOB",
            NULL, NULL, NULL);
    }

    if (sqlite3_prepare_v2(db,
        "SELECT COUNT(*) FROM pragma_table_info('_families') WHERE name='compact_header'",
        -1, &q, NULL) == SQLITE_OK) {
        if (sqlite3_step(q) == SQLITE_ROW)
            has_header = sqlite3_column_int(q, 0);
        sqlite3_finalize(q);
    }
    if (!has_header) {
        sqlite3_exec(db,
            "ALTER TABLE _families ADD COLUMN compact_header BLOB",
            NULL, NULL, NULL);
    }

    if (sqlite3_prepare_v2(db,
        "SELECT COUNT(*) FROM pragma_table_info('_families') WHERE name='compact_codec'",
        -1, &q, NULL) == SQLITE_OK) {
        if (sqlite3_step(q) == SQLITE_ROW)
            has_codec = sqlite3_column_int(q, 0);
        sqlite3_finalize(q);
    }
    if (!has_codec) {
        sqlite3_exec(db,
            "ALTER TABLE _families ADD COLUMN compact_codec TEXT",
            NULL, NULL, NULL);
    }

    return 0;
}
