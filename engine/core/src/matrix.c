#include "bf_matrix.h"

#include <sqlite3.h>

#include <ctype.h>
#include <stdlib.h>
#include <string.h>

struct BfMatrix {
    sqlite3 *db;
    int owns_db;
    FILE *err;
};

static const char *MATRIX_SCHEMA =
    "CREATE TABLE IF NOT EXISTS matrix_subjects("
    " subject_id TEXT PRIMARY KEY, subject_kind TEXT NOT NULL,"
    " identity_digest TEXT NOT NULL, evidence_ref TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS matrix_addresses("
    " address TEXT PRIMARY KEY, subject_id TEXT NOT NULL REFERENCES matrix_subjects(subject_id),"
    " address_kind TEXT NOT NULL, evidence_ref TEXT NOT NULL);"
    "CREATE INDEX IF NOT EXISTS idx_matrix_addresses_subject ON matrix_addresses(subject_id);"
    "CREATE TABLE IF NOT EXISTS matrix_address_aliases("
    " alias_address TEXT PRIMARY KEY REFERENCES matrix_addresses(address),"
    " canonical_address TEXT NOT NULL REFERENCES matrix_addresses(address),"
    " evidence_ref TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS matrix_domains("
    " hostname TEXT PRIMARY KEY, institution_id TEXT REFERENCES matrix_institutions(institution_id),"
    " territory TEXT NOT NULL, default_scope TEXT NOT NULL, default_world TEXT NOT NULL,"
    " default_representation TEXT NOT NULL, canonical_hostname TEXT REFERENCES matrix_domains(hostname),"
    " evidence_ref TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS matrix_standing_addresses("
    " address TEXT PRIMARY KEY REFERENCES matrix_addresses(address), subject_kind TEXT, territory TEXT,"
    " scope TEXT, world TEXT, representation TEXT, evidence_ref TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS matrix_institutions("
    " institution_id TEXT PRIMARY KEY, institution_kind TEXT NOT NULL, world TEXT NOT NULL,"
    " evidence_ref TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS matrix_delegations("
    " parent_institution_id TEXT NOT NULL REFERENCES matrix_institutions(institution_id),"
    " child_institution_id TEXT NOT NULL REFERENCES matrix_institutions(institution_id),"
    " authority_actor TEXT NOT NULL, authority_ref TEXT NOT NULL, purpose TEXT NOT NULL,"
    " PRIMARY KEY(parent_institution_id, child_institution_id));"
    /* Subject-to-subject relations.
     *
     * The Matrix owns subjects, so it owns the edges between them. Without this
     * a born Event knew the institution it was declared in only as a string in
     * an authored CMS body -- readable, but not a fact the subject layer could
     * traverse, and invisible to anything asking what an institution contains.
     *
     * to_subject deliberately carries no foreign key: an institution id is a
     * legitimate endpoint and is not itself a row in matrix_subjects. The
     * evidence_ref is required for the same reason every other binding here
     * requires one -- a relation asserted with no evidence is an opinion. */
    "CREATE TABLE IF NOT EXISTS matrix_subject_relations("
    " from_subject TEXT NOT NULL REFERENCES matrix_subjects(subject_id),"
    " relation TEXT NOT NULL, to_subject TEXT NOT NULL, evidence_ref TEXT NOT NULL,"
    " PRIMARY KEY(from_subject, relation, to_subject));"
    "CREATE INDEX IF NOT EXISTS idx_matrix_relations_to ON matrix_subject_relations(to_subject, relation);"
    "CREATE TABLE IF NOT EXISTS matrix_presences("
    " presence_id TEXT PRIMARY KEY, subject_id TEXT NOT NULL REFERENCES matrix_subjects(subject_id),"
    " institution_id TEXT REFERENCES matrix_institutions(institution_id), territory TEXT NOT NULL,"
    " scope TEXT NOT NULL, world TEXT NOT NULL, representation TEXT NOT NULL,"
    " evidence_ref TEXT NOT NULL, derived_from_presence_id TEXT REFERENCES matrix_presences(presence_id),"
    " active INTEGER NOT NULL DEFAULT 1);"
    "CREATE INDEX IF NOT EXISTS idx_matrix_presences_subject ON matrix_presences(subject_id,active);"
    "CREATE TABLE IF NOT EXISTS matrix_closure_contributions("
    " subject_id TEXT NOT NULL REFERENCES matrix_subjects(subject_id),"
    " presence_id TEXT NOT NULL REFERENCES matrix_presences(presence_id),"
    " plane TEXT NOT NULL, owner TEXT NOT NULL, resource_uri TEXT NOT NULL,"
    " representation TEXT NOT NULL, digest TEXT NOT NULL, evidence_ref TEXT NOT NULL,"
    " PRIMARY KEY(subject_id,presence_id,plane,owner,resource_uri,digest));"
    "CREATE INDEX IF NOT EXISTS idx_matrix_closure_contributions_cut "
    "ON matrix_closure_contributions(subject_id,presence_id,plane);";

static const char *ADDRESS_KINDS[] = {
    "web_url", "subdomain", "slug", "historical_alias", "fs_path", "address_expr",
    "standing_query", "api", "mcp", "a2a", "qr", "cms_route", "habitat",
    "foreign_uri", "federated_uri", NULL
};
static const char *TERRITORIES[] = {"life", "live", "shop", "work", "independent", NULL};
static const char *WORLDS[] = {"LIVE", "SHADOW", "REPLAY", "CANDIDATE", "DEMO", NULL};
static const char *SCOPES[] = {"self", "household", "group", "community", "public", "market", "institution", NULL};
static const char *CLOSURE_PLANES[] = {"material", "latent", "execution", "economic", NULL};

static int present(const char *value) { return value && value[0] != '\0'; }

static int in_set(const char *const *set, const char *value) {
    size_t i;
    if (!present(value)) return 0;
    for (i = 0; set[i]; ++i) if (strcmp(set[i], value) == 0) return 1;
    return 0;
}

static int is_hex_digest(const char *value) {
    size_t i;
    if (!value || strlen(value) != 64) return 0;
    for (i = 0; i < 64; ++i) {
        if (!((value[i] >= '0' && value[i] <= '9') ||
              (value[i] >= 'a' && value[i] <= 'f') ||
              (value[i] >= 'A' && value[i] <= 'F'))) return 0;
    }
    return 1;
}

static int bounded(const char *value, size_t max) { return present(value) && strlen(value) < max; }

static int closure_atom(const char *value, size_t max) {
    const unsigned char *p = (const unsigned char *)value;
    if (!bounded(value, max)) return 0;
    for (; *p; ++p) if (!(isalnum(*p) || strchr("._:-/", *p))) return 0;
    return 1;
}

static int closure_uri(const char *value) {
    return bounded(value, 1536) &&
        (!strncmp(value, "bonfyre://", 10) || !strncmp(value, "https://", 8) ||
         !strncmp(value, "urn:", 4));
}

const char *bf_matrix_world_name(BfMatrixWorld world) {
    return world >= BF_MATRIX_LIVE && world <= BF_MATRIX_DEMO ? WORLDS[world] : NULL;
}

const char *bf_matrix_scope_name(BfMatrixParticipationScope scope) {
    return scope >= BF_MATRIX_SELF && scope <= BF_MATRIX_INSTITUTION ? SCOPES[scope] : NULL;
}

const char *bf_matrix_status_name(BfMatrixStatus status) {
    static const char *NAMES[] = {"ok", "invalid", "invalid_world", "invalid_territory",
        "invalid_scope", "invalid_address_kind", "invalid_digest", "missing_evidence",
        "unknown_subject", "unknown_institution", "unknown_presence", "address_conflict",
        "identity_conflict", "duplicate_presence", "scope_regression", "cycle", "unauthorized",
        "storage_error"};
    return status >= BF_MATRIX_OK && status <= BF_MATRIX_STORAGE_ERROR ? NAMES[status] : "unknown";
}

static int apply_schema(sqlite3 *db, FILE *err) {
    char *message = NULL;
    /* Another process may hold the store for a moment (a publishing pass, a
     * second worker starting). Waiting is cheaper than refusing to start. */
    sqlite3_busy_timeout(db, 15000);
    if (sqlite3_exec(db, MATRIX_SCHEMA, NULL, NULL, &message) != SQLITE_OK) {
        if (err) fprintf(err, "matrix: schema failed: %s\n", message ? message : "?");
        sqlite3_free(message);
        return -1;
    }
    sqlite3_exec(db, "PRAGMA foreign_keys=ON;", NULL, NULL, NULL);
    return 0;
}

int bf_matrix_open(const char *database_path, BfMatrix **out, FILE *err) {
    sqlite3 *db = NULL;
    BfMatrix *matrix;
    if (!present(database_path) || !out || sqlite3_open(database_path, &db) != SQLITE_OK) {
        sqlite3_close(db);
        return -1;
    }
    sqlite3_exec(db, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
    if (apply_schema(db, err) != 0) { sqlite3_close(db); return -1; }
    matrix = calloc(1, sizeof(*matrix));
    if (!matrix) { sqlite3_close(db); return -1; }
    matrix->db = db;
    matrix->owns_db = 1;
    matrix->err = err;
    *out = matrix;
    return 0;
}

int bf_matrix_open_database(void *sqlite_database, BfMatrix **out, FILE *err) {
    BfMatrix *matrix;
    if (!sqlite_database || !out || apply_schema((sqlite3 *)sqlite_database, err) != 0) return -1;
    matrix = calloc(1, sizeof(*matrix));
    if (!matrix) return -1;
    matrix->db = sqlite_database;
    matrix->err = err;
    *out = matrix;
    return 0;
}

void bf_matrix_close(BfMatrix *matrix) {
    if (!matrix) return;
    if (matrix->owns_db) sqlite3_close(matrix->db);
    free(matrix);
}

static int exists(sqlite3 *db, const char *table, const char *field, const char *value) {
    char sql[192];
    sqlite3_stmt *stmt = NULL;
    int found = 0;
    if (snprintf(sql, sizeof(sql), "SELECT 1 FROM %s WHERE %s=?", table, field) >= (int)sizeof(sql)) return -1;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(stmt, 1, value, -1, SQLITE_TRANSIENT);
    found = sqlite3_step(stmt) == SQLITE_ROW;
    sqlite3_finalize(stmt);
    return found;
}

BfMatrixStatus bf_matrix_subject_bind(BfMatrix *matrix, const BfMatrixSubjectSpec *spec) {
    sqlite3_stmt *stmt = NULL;
    int step;
    if (!matrix || !spec || !present(spec->subject_id) || !present(spec->subject_kind)) return BF_MATRIX_INVALID;
    if (!present(spec->evidence_ref)) return BF_MATRIX_MISSING_EVIDENCE;
    if (!is_hex_digest(spec->identity_digest)) return BF_MATRIX_INVALID_DIGEST;
    if (sqlite3_prepare_v2(matrix->db,
            "SELECT subject_kind,identity_digest,evidence_ref FROM matrix_subjects WHERE subject_id=?",
            -1, &stmt, NULL) != SQLITE_OK) return BF_MATRIX_STORAGE_ERROR;
    sqlite3_bind_text(stmt, 1, spec->subject_id, -1, SQLITE_TRANSIENT);
    step = sqlite3_step(stmt);
    if (step == SQLITE_ROW) {
        const char *kind = (const char *)sqlite3_column_text(stmt, 0);
        const char *digest = (const char *)sqlite3_column_text(stmt, 1);
        const char *evidence = (const char *)sqlite3_column_text(stmt, 2);
        int same = kind && digest && evidence && strcmp(kind, spec->subject_kind) == 0 &&
                   strcmp(digest, spec->identity_digest) == 0 && strcmp(evidence, spec->evidence_ref) == 0;
        sqlite3_finalize(stmt);
        return same ? BF_MATRIX_OK : BF_MATRIX_IDENTITY_CONFLICT;
    }
    sqlite3_finalize(stmt);
    if (step != SQLITE_DONE) return BF_MATRIX_STORAGE_ERROR;
    if (sqlite3_prepare_v2(matrix->db,
            "INSERT INTO matrix_subjects(subject_id,subject_kind,identity_digest,evidence_ref) VALUES(?,?,?,?)",
            -1, &stmt, NULL) != SQLITE_OK) return BF_MATRIX_STORAGE_ERROR;
    sqlite3_bind_text(stmt, 1, spec->subject_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, spec->subject_kind, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, spec->identity_digest, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 4, spec->evidence_ref, -1, SQLITE_TRANSIENT);
    step = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return step == SQLITE_DONE ? BF_MATRIX_OK : BF_MATRIX_STORAGE_ERROR;
}

BfMatrixStatus bf_matrix_address_bind(BfMatrix *matrix, const char *subject_id,
                                      const char *address_kind, const char *address,
                                      const char *evidence_ref) {
    sqlite3_stmt *stmt = NULL;
    int step;
    if (!matrix || !present(subject_id) || !present(address)) return BF_MATRIX_INVALID;
    if (!in_set(ADDRESS_KINDS, address_kind)) return BF_MATRIX_INVALID_ADDRESS_KIND;
    if (!present(evidence_ref)) return BF_MATRIX_MISSING_EVIDENCE;
    if (exists(matrix->db, "matrix_subjects", "subject_id", subject_id) != 1) return BF_MATRIX_UNKNOWN_SUBJECT;
    if (sqlite3_prepare_v2(matrix->db, "SELECT subject_id FROM matrix_addresses WHERE address=?", -1, &stmt, NULL) != SQLITE_OK)
        return BF_MATRIX_STORAGE_ERROR;
    sqlite3_bind_text(stmt, 1, address, -1, SQLITE_TRANSIENT);
    step = sqlite3_step(stmt);
    if (step == SQLITE_ROW) {
        const char *existing = (const char *)sqlite3_column_text(stmt, 0);
        int same = existing && strcmp(existing, subject_id) == 0;
        sqlite3_finalize(stmt);
        return same ? BF_MATRIX_OK : BF_MATRIX_ADDRESS_CONFLICT;
    }
    sqlite3_finalize(stmt);
    if (step != SQLITE_DONE) return BF_MATRIX_STORAGE_ERROR;
    if (sqlite3_prepare_v2(matrix->db,
            "INSERT INTO matrix_addresses(address,subject_id,address_kind,evidence_ref) VALUES(?,?,?,?)",
            -1, &stmt, NULL) != SQLITE_OK) return BF_MATRIX_STORAGE_ERROR;
    sqlite3_bind_text(stmt, 1, address, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, subject_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, address_kind, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 4, evidence_ref, -1, SQLITE_TRANSIENT);
    step = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return step == SQLITE_DONE ? BF_MATRIX_OK : BF_MATRIX_STORAGE_ERROR;
}

int bf_matrix_address_subject(BfMatrix *matrix, const char *address, char *subject_id, size_t subject_id_size) {
    sqlite3_stmt *stmt = NULL;
    const char *value;
    if (!matrix || !present(address) || !subject_id || subject_id_size == 0) return -1;
    if (sqlite3_prepare_v2(matrix->db, "SELECT subject_id FROM matrix_addresses WHERE address=?", -1, &stmt, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(stmt, 1, address, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) != SQLITE_ROW) { sqlite3_finalize(stmt); return 0; }
    value = (const char *)sqlite3_column_text(stmt, 0);
    if (!value || snprintf(subject_id, subject_id_size, "%s", value) >= (int)subject_id_size) { sqlite3_finalize(stmt); return -1; }
    sqlite3_finalize(stmt);
    return 1;
}

int bf_matrix_address_resolve(BfMatrix *matrix, const char *address,
                              const char *territory, BfMatrixWorld world,
                              BfMatrixParticipationScope scope,
                              const char *representation,
                              BfMatrixAddressResolution *out) {
    sqlite3_stmt *stmt = NULL; const char *subject, *presence, *stored_territory, *stored_representation, *stored_world, *stored_scope, *kind, *canonical;
    const char *world_name = bf_matrix_world_name(world), *scope_name = bf_matrix_scope_name(scope);
    if (!matrix || !present(address) || !in_set(TERRITORIES, territory) || !world_name || !scope_name || !present(representation) || !out) return -1;
    if (sqlite3_prepare_v2(matrix->db,
        "SELECT a.subject_id,p.presence_id,p.territory,p.representation,p.world,p.scope,a.address_kind,COALESCE(x.canonical_address,a.address) "
        "FROM matrix_addresses a LEFT JOIN matrix_address_aliases x ON x.alias_address=a.address "
        "JOIN matrix_presences p ON p.subject_id=a.subject_id "
        "WHERE a.address=? AND p.territory=? AND p.world=? AND p.scope=? AND p.representation=? AND p.active=1 "
        "ORDER BY p.presence_id LIMIT 1", -1, &stmt, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(stmt, 1, address, -1, SQLITE_TRANSIENT); sqlite3_bind_text(stmt, 2, territory, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, world_name, -1, SQLITE_TRANSIENT); sqlite3_bind_text(stmt, 4, scope_name, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 5, representation, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) != SQLITE_ROW) { sqlite3_finalize(stmt); return 0; }
    subject = (const char *)sqlite3_column_text(stmt, 0); presence = (const char *)sqlite3_column_text(stmt, 1);
    stored_territory = (const char *)sqlite3_column_text(stmt, 2); stored_representation = (const char *)sqlite3_column_text(stmt, 3);
    stored_world = (const char *)sqlite3_column_text(stmt, 4); stored_scope = (const char *)sqlite3_column_text(stmt, 5);
    kind = (const char *)sqlite3_column_text(stmt, 6); canonical = (const char *)sqlite3_column_text(stmt, 7);
    if (!subject || !presence || !stored_territory || !stored_representation || !stored_world || !stored_scope ||
        snprintf(out->subject_id, sizeof(out->subject_id), "%s", subject) >= (int)sizeof(out->subject_id) ||
        snprintf(out->presence_id, sizeof(out->presence_id), "%s", presence) >= (int)sizeof(out->presence_id) ||
        snprintf(out->territory, sizeof(out->territory), "%s", stored_territory) >= (int)sizeof(out->territory) ||
        snprintf(out->representation, sizeof(out->representation), "%s", stored_representation) >= (int)sizeof(out->representation) ||
        !kind || !canonical || snprintf(out->address_kind, sizeof(out->address_kind), "%s", kind) >= (int)sizeof(out->address_kind) ||
        snprintf(out->requested_address, sizeof(out->requested_address), "%s", address) >= (int)sizeof(out->requested_address) ||
        snprintf(out->canonical_address, sizeof(out->canonical_address), "%s", canonical) >= (int)sizeof(out->canonical_address)) { sqlite3_finalize(stmt); return -1; }
    out->is_alias = strcmp(address, canonical) != 0; out->world = world; out->scope = scope; sqlite3_finalize(stmt); return 1;
}

/* Does this subject exist? Asked by callers that must bind to something real
 * and must not create it -- an attachment that silently declares the thing it
 * claims to be attaching to is a declaration wearing another name. */
/* Subjects of one kind that share an identity digest.
 *
 * Two subjects identified by the same retained bytes are one thing named twice.
 * Returned rather than resolved: which id is canonical is not this layer's
 * call, and collapsing them would strand whatever already references the loser.
 *
 * An out-array rather than the open handle, so the storage stays behind the
 * API; a caller that can reach the database can ask it anything, and this
 * question deserves a name. */
int bf_matrix_duplicate_identities(BfMatrix *matrix, const char *subject_kind,
                                   BfMatrixDuplicateIdentity *out, int max) {
    sqlite3_stmt *stmt = NULL; int n = 0;
    if (!matrix || !present(subject_kind) || !out || max <= 0) return -1;
    if (sqlite3_prepare_v2(matrix->db,
            "SELECT a.subject_id, b.subject_id, a.identity_digest "
            "FROM matrix_subjects a JOIN matrix_subjects b "
            "  ON a.identity_digest = b.identity_digest AND a.subject_id < b.subject_id "
            "WHERE a.subject_kind = ?1 AND b.subject_kind = ?1", -1, &stmt, NULL) != SQLITE_OK)
        return -1;
    sqlite3_bind_text(stmt, 1, subject_kind, -1, SQLITE_TRANSIENT);
    while (n < max && sqlite3_step(stmt) == SQLITE_ROW) {
        snprintf(out[n].a, sizeof(out[n].a), "%s", (const char *)sqlite3_column_text(stmt, 0));
        snprintf(out[n].b, sizeof(out[n].b), "%s", (const char *)sqlite3_column_text(stmt, 1));
        snprintf(out[n].identity_digest, sizeof(out[n].identity_digest), "%s",
                 (const char *)sqlite3_column_text(stmt, 2));
        n++;
    }
    sqlite3_finalize(stmt);
    return n;
}

/* A closure resolution for a subject that is not reachable at a host.
 *
 * Closures were addressable only by host and path, which suited institutions
 * and their territories and excluded everything else. A foreign twin has an
 * identity, a presence and -- now -- a bound body on the material plane, and
 * none of it could be assembled into a closure because no hostname resolves to
 * it. An observation source has no route at all and never will.
 *
 * The resolution is built from the presence, which already carries the
 * territory, world, scope and representation a closure needs. The address
 * fields describe the subject itself rather than a route, because there is no
 * route: a closure over a thing is not a claim that the thing is fetchable.
 *
 * Cannot infer: a resolvable closure -> a reachable subject. */
int bf_matrix_presence_resolution(BfMatrix *matrix, const char *subject_id,
                                  const char *presence_id,
                                  BfMatrixAddressResolution *out) {
    sqlite3_stmt *stmt = NULL;
    int ok = 0;
    if (!matrix || !present(subject_id) || !present(presence_id) || !out) return -1;
    if (sqlite3_prepare_v2(matrix->db,
            "SELECT territory, representation, world, scope FROM matrix_presences "
            "WHERE presence_id=? AND subject_id=? AND active=1", -1, &stmt, NULL) != SQLITE_OK)
        return -1;
    sqlite3_bind_text(stmt, 1, presence_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, subject_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *territory = (const char *)sqlite3_column_text(stmt, 0);
        const char *repr = (const char *)sqlite3_column_text(stmt, 1);
        const char *world = (const char *)sqlite3_column_text(stmt, 2);
        const char *scope = (const char *)sqlite3_column_text(stmt, 3);
        int w, sc;
        memset(out, 0, sizeof(*out));
        snprintf(out->subject_id, sizeof(out->subject_id), "%s", subject_id);
        snprintf(out->presence_id, sizeof(out->presence_id), "%s", presence_id);
        snprintf(out->territory, sizeof(out->territory), "%s", territory ? territory : "");
        snprintf(out->representation, sizeof(out->representation), "%s", repr ? repr : "");
        snprintf(out->address_kind, sizeof(out->address_kind), "%s", "subject");
        snprintf(out->requested_address, sizeof(out->requested_address), "%s", subject_id);
        snprintf(out->canonical_address, sizeof(out->canonical_address), "%s", subject_id);
        out->is_alias = 0;
        out->world = BF_MATRIX_LIVE;
        for (w = 0; WORLDS[w]; w++)
            if (world && !strcmp(world, WORLDS[w])) { out->world = (BfMatrixWorld)w; break; }
        out->scope = BF_MATRIX_PUBLIC;
        for (sc = 0; SCOPES[sc]; sc++)
            if (scope && !strcmp(scope, SCOPES[sc])) { out->scope = (BfMatrixParticipationScope)sc; break; }
        ok = 1;
    }
    sqlite3_finalize(stmt);
    return ok;
}

int bf_matrix_subject_exists(BfMatrix *matrix, const char *subject_id) {
    if (!matrix || !present(subject_id)) return -1;
    return exists(matrix->db, "matrix_subjects", "subject_id", subject_id);
}

BfMatrixStatus bf_matrix_subject_relate(BfMatrix *matrix, const char *from_subject,
                                        const char *relation, const char *to_subject,
                                        const char *evidence_ref) {
    sqlite3_stmt *stmt = NULL; int step;
    if (!matrix || !present(from_subject) || !present(relation) || !present(to_subject))
        return BF_MATRIX_INVALID;
    if (!present(evidence_ref)) return BF_MATRIX_MISSING_EVIDENCE;
    /* The subject the relation starts from must exist. The endpoint may be an
     * institution or another subject, so it is not checked here. */
    if (exists(matrix->db, "matrix_subjects", "subject_id", from_subject) != 1)
        return BF_MATRIX_INVALID;
    if (sqlite3_prepare_v2(matrix->db,
            "INSERT INTO matrix_subject_relations(from_subject,relation,to_subject,evidence_ref) "
            "VALUES(?,?,?,?) ON CONFLICT(from_subject,relation,to_subject) "
            "DO UPDATE SET evidence_ref=excluded.evidence_ref", -1, &stmt, NULL) != SQLITE_OK)
        return BF_MATRIX_STORAGE_ERROR;
    sqlite3_bind_text(stmt, 1, from_subject, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, relation, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, to_subject, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 4, evidence_ref, -1, SQLITE_TRANSIENT);
    step = sqlite3_step(stmt); sqlite3_finalize(stmt);
    return step == SQLITE_DONE ? BF_MATRIX_OK : BF_MATRIX_STORAGE_ERROR;
}

/* Edges leaving a subject.
 *
 * The inbound counter below answers "how many things point at this". A claim
 * points the other way -- twin claimed_by institution -- so counting claims on
 * a twin with the inbound function returns zero for a twin that is demonstrably
 * claimed. Both directions exist because both questions are real, and using the
 * wrong one fails silently by answering a different question correctly. */
int bf_matrix_subject_relation_from_count(BfMatrix *matrix, const char *from_subject,
                                          const char *relation) {
    sqlite3_stmt *stmt = NULL; int count = -1;
    if (!matrix || !present(from_subject) || !present(relation)) return -1;
    if (sqlite3_prepare_v2(matrix->db,
            "SELECT COUNT(*) FROM matrix_subject_relations WHERE from_subject=? AND relation=?",
            -1, &stmt, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(stmt, 1, from_subject, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, relation, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) == SQLITE_ROW) count = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
    return count;
}

int bf_matrix_subject_relation_count(BfMatrix *matrix, const char *to_subject,
                                     const char *relation) {
    sqlite3_stmt *stmt = NULL; int count = -1;
    if (!matrix || !present(to_subject) || !present(relation)) return -1;
    if (sqlite3_prepare_v2(matrix->db,
            "SELECT COUNT(*) FROM matrix_subject_relations WHERE to_subject=? AND relation=?",
            -1, &stmt, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(stmt, 1, to_subject, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, relation, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) == SQLITE_ROW) count = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
    return count;
}

int bf_matrix_subject_address_count(BfMatrix *matrix, const char *subject_id) {
    sqlite3_stmt *stmt = NULL; int count = -1;
    if (!matrix || !present(subject_id) || sqlite3_prepare_v2(matrix->db, "SELECT COUNT(*) FROM matrix_addresses WHERE subject_id=?", -1, &stmt, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(stmt, 1, subject_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) == SQLITE_ROW) count = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
    return count;
}

int bf_matrix_subject_read(BfMatrix *matrix, const char *subject_id,
                           BfMatrixSubjectRecord *out) {
    sqlite3_stmt *stmt = NULL;
    const char *id, *kind, *digest;
    int rc = -1;
    if (!matrix || !present(subject_id) || !out ||
        sqlite3_prepare_v2(matrix->db,
            "SELECT subject_id,subject_kind,identity_digest FROM matrix_subjects WHERE subject_id=?",
            -1, &stmt, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(stmt, 1, subject_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) != SQLITE_ROW) { sqlite3_finalize(stmt); return 0; }
    id = (const char *)sqlite3_column_text(stmt, 0);
    kind = (const char *)sqlite3_column_text(stmt, 1);
    digest = (const char *)sqlite3_column_text(stmt, 2);
    if (id && kind && digest &&
        snprintf(out->subject_id, sizeof(out->subject_id), "%s", id) < (int)sizeof(out->subject_id) &&
        snprintf(out->subject_kind, sizeof(out->subject_kind), "%s", kind) < (int)sizeof(out->subject_kind) &&
        snprintf(out->identity_digest, sizeof(out->identity_digest), "%s", digest) < (int)sizeof(out->identity_digest)) rc = 1;
    sqlite3_finalize(stmt);
    return rc;
}

BfMatrixStatus bf_matrix_closure_contribution_bind(
    BfMatrix *matrix, const BfMatrixClosureContributionSpec *spec) {
    sqlite3_stmt *stmt = NULL;
    int step;
    if (!matrix || !spec || !bounded(spec->subject_id, 256) ||
        !bounded(spec->presence_id, 256) || !in_set(CLOSURE_PLANES, spec->plane) ||
        !closure_atom(spec->owner, 128) || !closure_uri(spec->resource_uri) ||
        !closure_atom(spec->representation, 64) || !is_hex_digest(spec->digest) ||
        !bounded(spec->evidence_ref, 512)) return BF_MATRIX_INVALID;
    if (sqlite3_prepare_v2(matrix->db,
            "SELECT 1 FROM matrix_presences WHERE presence_id=? AND subject_id=? AND active=1",
            -1, &stmt, NULL) != SQLITE_OK) return BF_MATRIX_STORAGE_ERROR;
    sqlite3_bind_text(stmt, 1, spec->presence_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, spec->subject_id, -1, SQLITE_TRANSIENT);
    step = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if (step != SQLITE_ROW) return BF_MATRIX_UNKNOWN_PRESENCE;
    if (sqlite3_prepare_v2(matrix->db,
            "INSERT OR IGNORE INTO matrix_closure_contributions("
            "subject_id,presence_id,plane,owner,resource_uri,representation,digest,evidence_ref)"
            " VALUES(?,?,?,?,?,?,?,?)", -1, &stmt, NULL) != SQLITE_OK) return BF_MATRIX_STORAGE_ERROR;
    sqlite3_bind_text(stmt, 1, spec->subject_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, spec->presence_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, spec->plane, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 4, spec->owner, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 5, spec->resource_uri, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 6, spec->representation, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 7, spec->digest, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 8, spec->evidence_ref, -1, SQLITE_TRANSIENT);
    step = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return step == SQLITE_DONE ? BF_MATRIX_OK : BF_MATRIX_STORAGE_ERROR;
}

int bf_matrix_closure_contributions(
    BfMatrix *matrix, const char *subject_id, const char *presence_id,
    const char *plane, BfMatrixClosureContributionVisitor visitor,
    void *context) {
    sqlite3_stmt *stmt = NULL;
    int count = 0;
    if (!matrix || !bounded(subject_id, 256) || !bounded(presence_id, 256) ||
        !visitor || (plane && !in_set(CLOSURE_PLANES, plane))) return -1;
    if (sqlite3_prepare_v2(matrix->db,
            "SELECT plane,owner,resource_uri,representation,digest,evidence_ref "
            "FROM matrix_closure_contributions WHERE subject_id=? AND presence_id=? "
            "AND (? IS NULL OR plane=?) ORDER BY plane,owner,resource_uri,digest",
            -1, &stmt, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(stmt, 1, subject_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, presence_id, -1, SQLITE_TRANSIENT);
    if (plane) sqlite3_bind_text(stmt, 3, plane, -1, SQLITE_TRANSIENT); else sqlite3_bind_null(stmt, 3);
    if (plane) sqlite3_bind_text(stmt, 4, plane, -1, SQLITE_TRANSIENT); else sqlite3_bind_null(stmt, 4);
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        BfMatrixClosureContributionSpec value = {
            subject_id, presence_id,
            (const char *)sqlite3_column_text(stmt, 0),
            (const char *)sqlite3_column_text(stmt, 1),
            (const char *)sqlite3_column_text(stmt, 2),
            (const char *)sqlite3_column_text(stmt, 3),
            (const char *)sqlite3_column_text(stmt, 4),
            (const char *)sqlite3_column_text(stmt, 5)
        };
        if (!value.plane || !value.owner || !value.resource_uri || !value.representation ||
            !value.digest || !value.evidence_ref) { sqlite3_finalize(stmt); return -1; }
        ++count;
        if (visitor(&value, context)) break;
    }
    sqlite3_finalize(stmt);
    return count;
}

int bf_matrix_subject_addresses(BfMatrix *matrix, const char *subject_id,
                                BfMatrixAddressVisitor visitor, void *context) {
    sqlite3_stmt *stmt = NULL; int count = 0;
    if (!matrix || !present(subject_id) || !visitor) return -1;
    if (sqlite3_prepare_v2(matrix->db,
            "SELECT a.address,a.address_kind,COALESCE(x.canonical_address,a.address),a.evidence_ref "
            "FROM matrix_addresses a LEFT JOIN matrix_address_aliases x ON x.alias_address=a.address "
            "WHERE a.subject_id=? ORDER BY a.address_kind,a.address", -1, &stmt, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(stmt, 1, subject_id, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *address=(const char *)sqlite3_column_text(stmt,0), *kind=(const char *)sqlite3_column_text(stmt,1), *canonical=(const char *)sqlite3_column_text(stmt,2), *evidence=(const char *)sqlite3_column_text(stmt,3);
        if (!address || !kind || !canonical || !evidence) { sqlite3_finalize(stmt); return -1; }
        count++; if (visitor(address,kind,canonical,evidence,context)) break;
    }
    sqlite3_finalize(stmt); return count;
}

BfMatrixStatus bf_matrix_address_alias(BfMatrix *matrix, const char *alias_address,
                                       const char *canonical_address, const char *evidence_ref) {
    sqlite3_stmt *stmt = NULL; const char *alias_subject, *canonical_subject; int step;
    if (!matrix || !present(alias_address) || !present(canonical_address)) return BF_MATRIX_INVALID;
    if (!present(evidence_ref)) return BF_MATRIX_MISSING_EVIDENCE;
    if (strcmp(alias_address, canonical_address) == 0) return BF_MATRIX_INVALID;
    if (sqlite3_prepare_v2(matrix->db,
            "SELECT (SELECT subject_id FROM matrix_addresses WHERE address=?),"
            "(SELECT subject_id FROM matrix_addresses WHERE address=?)", -1, &stmt, NULL) != SQLITE_OK) return BF_MATRIX_STORAGE_ERROR;
    sqlite3_bind_text(stmt, 1, alias_address, -1, SQLITE_TRANSIENT); sqlite3_bind_text(stmt, 2, canonical_address, -1, SQLITE_TRANSIENT);
    step = sqlite3_step(stmt); alias_subject = step == SQLITE_ROW ? (const char *)sqlite3_column_text(stmt, 0) : NULL;
    canonical_subject = step == SQLITE_ROW ? (const char *)sqlite3_column_text(stmt, 1) : NULL;
    if (!alias_subject || !canonical_subject) { sqlite3_finalize(stmt); return BF_MATRIX_UNKNOWN_SUBJECT; }
    if (strcmp(alias_subject, canonical_subject) != 0) { sqlite3_finalize(stmt); return BF_MATRIX_ADDRESS_CONFLICT; }
    sqlite3_finalize(stmt);
    if (sqlite3_prepare_v2(matrix->db,
            "INSERT INTO matrix_address_aliases(alias_address,canonical_address,evidence_ref) VALUES(?,?,?) "
            "ON CONFLICT(alias_address) DO UPDATE SET canonical_address=excluded.canonical_address,evidence_ref=excluded.evidence_ref", -1, &stmt, NULL) != SQLITE_OK) return BF_MATRIX_STORAGE_ERROR;
    sqlite3_bind_text(stmt, 1, alias_address, -1, SQLITE_TRANSIENT); sqlite3_bind_text(stmt, 2, canonical_address, -1, SQLITE_TRANSIENT); sqlite3_bind_text(stmt, 3, evidence_ref, -1, SQLITE_TRANSIENT);
    step = sqlite3_step(stmt); sqlite3_finalize(stmt); return step == SQLITE_DONE ? BF_MATRIX_OK : BF_MATRIX_STORAGE_ERROR;
}

BfMatrixStatus bf_matrix_domain_bind(BfMatrix *matrix, const BfMatrixDomainSpec *spec) {
    sqlite3_stmt *stmt = NULL; const char *scope, *world; int step;
    if (!matrix || !spec || !present(spec->hostname) || !in_set(TERRITORIES, spec->territory) || !present(spec->default_representation)) return BF_MATRIX_INVALID;
    if (!present(spec->evidence_ref)) return BF_MATRIX_MISSING_EVIDENCE;
    scope = bf_matrix_scope_name(spec->default_scope); world = bf_matrix_world_name(spec->default_world);
    if (!scope) return BF_MATRIX_INVALID_SCOPE;
    if (!world) return BF_MATRIX_INVALID_WORLD;
    if (spec->institution_id && exists(matrix->db, "matrix_institutions", "institution_id", spec->institution_id) != 1) return BF_MATRIX_UNKNOWN_INSTITUTION;
    if (spec->canonical_hostname && strcmp(spec->canonical_hostname, spec->hostname) != 0 && exists(matrix->db, "matrix_domains", "hostname", spec->canonical_hostname) != 1) return BF_MATRIX_INVALID;
    if (sqlite3_prepare_v2(matrix->db,
            "INSERT INTO matrix_domains(hostname,institution_id,territory,default_scope,default_world,default_representation,canonical_hostname,evidence_ref) VALUES(?,?,?,?,?,?,?,?) "
            "ON CONFLICT(hostname) DO UPDATE SET institution_id=excluded.institution_id,territory=excluded.territory,default_scope=excluded.default_scope,default_world=excluded.default_world,default_representation=excluded.default_representation,canonical_hostname=excluded.canonical_hostname,evidence_ref=excluded.evidence_ref", -1, &stmt, NULL) != SQLITE_OK) return BF_MATRIX_STORAGE_ERROR;
    sqlite3_bind_text(stmt, 1, spec->hostname, -1, SQLITE_TRANSIENT);
    if (spec->institution_id) sqlite3_bind_text(stmt, 2, spec->institution_id, -1, SQLITE_TRANSIENT); else sqlite3_bind_null(stmt, 2);
    sqlite3_bind_text(stmt, 3, spec->territory, -1, SQLITE_TRANSIENT); sqlite3_bind_text(stmt, 4, scope, -1, SQLITE_TRANSIENT); sqlite3_bind_text(stmt, 5, world, -1, SQLITE_TRANSIENT); sqlite3_bind_text(stmt, 6, spec->default_representation, -1, SQLITE_TRANSIENT);
    if (spec->canonical_hostname && strcmp(spec->canonical_hostname, spec->hostname) != 0) sqlite3_bind_text(stmt, 7, spec->canonical_hostname, -1, SQLITE_TRANSIENT); else sqlite3_bind_null(stmt, 7);
    sqlite3_bind_text(stmt, 8, spec->evidence_ref, -1, SQLITE_TRANSIENT); step = sqlite3_step(stmt); sqlite3_finalize(stmt);
    return step == SQLITE_DONE ? BF_MATRIX_OK : BF_MATRIX_STORAGE_ERROR;
}

int bf_matrix_domain_resolve(BfMatrix *matrix, const char *hostname, BfMatrixDomainResolution *out) {
    sqlite3_stmt *stmt = NULL; const char *matched, *canonical, *institution, *territory, *scope, *world, *representation;
    if (!matrix || !present(hostname) || !out) return -1;
    if (sqlite3_prepare_v2(matrix->db,
            "SELECT d.hostname,COALESCE(c.hostname,d.hostname),COALESCE(c.institution_id,d.institution_id),COALESCE(c.territory,d.territory),COALESCE(c.default_scope,d.default_scope),COALESCE(c.default_world,d.default_world),COALESCE(c.default_representation,d.default_representation) "
            "FROM matrix_domains d LEFT JOIN matrix_domains c ON c.hostname=d.canonical_hostname "
            "WHERE ?=d.hostname OR ? LIKE '%.' || d.hostname ORDER BY length(d.hostname) DESC LIMIT 1", -1, &stmt, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(stmt, 1, hostname, -1, SQLITE_TRANSIENT); sqlite3_bind_text(stmt, 2, hostname, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) != SQLITE_ROW) { sqlite3_finalize(stmt); return 0; }
    matched=(const char *)sqlite3_column_text(stmt,0); canonical=(const char *)sqlite3_column_text(stmt,1); institution=(const char *)sqlite3_column_text(stmt,2); territory=(const char *)sqlite3_column_text(stmt,3); scope=(const char *)sqlite3_column_text(stmt,4); world=(const char *)sqlite3_column_text(stmt,5); representation=(const char *)sqlite3_column_text(stmt,6);
    if (!matched || !canonical || !territory || !scope || !world || !representation ||
        snprintf(out->requested_hostname,sizeof(out->requested_hostname),"%s",hostname)>=(int)sizeof(out->requested_hostname) || snprintf(out->matched_hostname,sizeof(out->matched_hostname),"%s",matched)>=(int)sizeof(out->matched_hostname) || snprintf(out->canonical_hostname,sizeof(out->canonical_hostname),"%s",canonical)>=(int)sizeof(out->canonical_hostname) || snprintf(out->territory,sizeof(out->territory),"%s",territory)>=(int)sizeof(out->territory) || snprintf(out->representation,sizeof(out->representation),"%s",representation)>=(int)sizeof(out->representation)) { sqlite3_finalize(stmt); return -1; }
    snprintf(out->institution_id,sizeof(out->institution_id),"%s",institution ? institution : ""); out->scope=scope[0]=='s'?BF_MATRIX_SELF:scope[0]=='h'?BF_MATRIX_HOUSEHOLD:scope[0]=='g'?BF_MATRIX_GROUP:scope[0]=='c'?BF_MATRIX_COMMUNITY:scope[0]=='p'?BF_MATRIX_PUBLIC:scope[0]=='m'?BF_MATRIX_MARKET:BF_MATRIX_INSTITUTION; out->world=world[0]=='L'?BF_MATRIX_LIVE:world[0]=='S'?BF_MATRIX_SHADOW:world[0]=='R'?BF_MATRIX_REPLAY:BF_MATRIX_CANDIDATE; sqlite3_finalize(stmt); return 1;
}

BfMatrixStatus bf_matrix_standing_address_bind(BfMatrix *matrix,
                                                const BfMatrixStandingAddressSpec *spec) {
    sqlite3_stmt *stmt = NULL; const char *scope = NULL, *world = NULL; int step;
    if (!matrix || !spec || !present(spec->address) || !present(spec->evidence_ref)) return BF_MATRIX_INVALID;
    if (exists(matrix->db, "matrix_addresses", "address", spec->address) != 1) return BF_MATRIX_UNKNOWN_SUBJECT;
    if (spec->territory && !in_set(TERRITORIES, spec->territory)) return BF_MATRIX_INVALID_TERRITORY;
    if (spec->scope != -1) { scope=bf_matrix_scope_name((BfMatrixParticipationScope)spec->scope); if(!scope)return BF_MATRIX_INVALID_SCOPE; }
    if (spec->world != -1) { world=bf_matrix_world_name((BfMatrixWorld)spec->world); if(!world)return BF_MATRIX_INVALID_WORLD; }
    if (sqlite3_prepare_v2(matrix->db,
            "INSERT INTO matrix_standing_addresses(address,subject_kind,territory,scope,world,representation,evidence_ref) VALUES(?,?,?,?,?,?,?) "
            "ON CONFLICT(address) DO UPDATE SET subject_kind=excluded.subject_kind,territory=excluded.territory,scope=excluded.scope,world=excluded.world,representation=excluded.representation,evidence_ref=excluded.evidence_ref", -1, &stmt, NULL) != SQLITE_OK) return BF_MATRIX_STORAGE_ERROR;
    sqlite3_bind_text(stmt,1,spec->address,-1,SQLITE_TRANSIENT);
    if(spec->subject_kind)sqlite3_bind_text(stmt,2,spec->subject_kind,-1,SQLITE_TRANSIENT);else sqlite3_bind_null(stmt,2);
    if(spec->territory)sqlite3_bind_text(stmt,3,spec->territory,-1,SQLITE_TRANSIENT);else sqlite3_bind_null(stmt,3);
    if(scope)sqlite3_bind_text(stmt,4,scope,-1,SQLITE_TRANSIENT);else sqlite3_bind_null(stmt,4);
    if(world)sqlite3_bind_text(stmt,5,world,-1,SQLITE_TRANSIENT);else sqlite3_bind_null(stmt,5);
    if(spec->representation)sqlite3_bind_text(stmt,6,spec->representation,-1,SQLITE_TRANSIENT);else sqlite3_bind_null(stmt,6);
    sqlite3_bind_text(stmt,7,spec->evidence_ref,-1,SQLITE_TRANSIENT); step=sqlite3_step(stmt); sqlite3_finalize(stmt);
    return step==SQLITE_DONE ? BF_MATRIX_OK : BF_MATRIX_STORAGE_ERROR;
}

int bf_matrix_standing_address_member_count(BfMatrix *matrix, const char *address,
                                            int *member_count) {
    sqlite3_stmt *stmt = NULL;
    if (!matrix || !present(address) || !member_count) return -1;
    if (sqlite3_prepare_v2(matrix->db,
            "SELECT COUNT(DISTINCT s.subject_id) FROM matrix_standing_addresses q "
            "JOIN matrix_presences p ON p.active=1 JOIN matrix_subjects s ON s.subject_id=p.subject_id "
            "WHERE q.address=? AND (q.subject_kind IS NULL OR q.subject_kind=s.subject_kind) "
            "AND (q.territory IS NULL OR q.territory=p.territory) AND (q.scope IS NULL OR q.scope=p.scope) "
            "AND (q.world IS NULL OR q.world=p.world) AND (q.representation IS NULL OR q.representation=p.representation)", -1, &stmt, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(stmt,1,address,-1,SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) != SQLITE_ROW) { sqlite3_finalize(stmt); return 0; }
    *member_count=sqlite3_column_int(stmt,0); sqlite3_finalize(stmt); return 1;
}

BfMatrixStatus bf_matrix_institution_bind(BfMatrix *matrix, const BfMatrixInstitutionSpec *spec) {
    sqlite3_stmt *stmt = NULL; const char *world; int step;
    if (!matrix || !spec || !present(spec->institution_id) || !present(spec->institution_kind)) return BF_MATRIX_INVALID;
    if (!present(spec->evidence_ref)) return BF_MATRIX_MISSING_EVIDENCE;
    world = bf_matrix_world_name(spec->world);
    if (!world) return BF_MATRIX_INVALID_WORLD;
    if (sqlite3_prepare_v2(matrix->db,
        "SELECT institution_kind,world,evidence_ref FROM matrix_institutions WHERE institution_id=?", -1, &stmt, NULL) != SQLITE_OK) return BF_MATRIX_STORAGE_ERROR;
    sqlite3_bind_text(stmt, 1, spec->institution_id, -1, SQLITE_TRANSIENT);
    step = sqlite3_step(stmt);
    if (step == SQLITE_ROW) {
        const char *kind = (const char *)sqlite3_column_text(stmt, 0); const char *old_world = (const char *)sqlite3_column_text(stmt, 1); const char *evidence = (const char *)sqlite3_column_text(stmt, 2);
        int same = kind && old_world && evidence && strcmp(kind, spec->institution_kind) == 0 && strcmp(old_world, world) == 0 && strcmp(evidence, spec->evidence_ref) == 0;
        sqlite3_finalize(stmt); return same ? BF_MATRIX_OK : BF_MATRIX_IDENTITY_CONFLICT;
    }
    sqlite3_finalize(stmt); if (step != SQLITE_DONE) return BF_MATRIX_STORAGE_ERROR;
    if (sqlite3_prepare_v2(matrix->db,
        "INSERT INTO matrix_institutions(institution_id,institution_kind,world,evidence_ref) VALUES(?,?,?,?)", -1, &stmt, NULL) != SQLITE_OK) return BF_MATRIX_STORAGE_ERROR;
    sqlite3_bind_text(stmt, 1, spec->institution_id, -1, SQLITE_TRANSIENT); sqlite3_bind_text(stmt, 2, spec->institution_kind, -1, SQLITE_TRANSIENT); sqlite3_bind_text(stmt, 3, world, -1, SQLITE_TRANSIENT); sqlite3_bind_text(stmt, 4, spec->evidence_ref, -1, SQLITE_TRANSIENT);
    step = sqlite3_step(stmt); sqlite3_finalize(stmt); return step == SQLITE_DONE ? BF_MATRIX_OK : BF_MATRIX_STORAGE_ERROR;
}

static int delegation_would_cycle(BfMatrix *matrix, const char *parent_id, const char *child_id) {
    sqlite3_stmt *stmt = NULL; int cycle = -1;
    if (strcmp(parent_id, child_id) == 0) return 1;
    if (sqlite3_prepare_v2(matrix->db,
        "WITH RECURSIVE descendants(id) AS (SELECT child_institution_id FROM matrix_delegations WHERE parent_institution_id=? UNION SELECT d.child_institution_id FROM matrix_delegations d JOIN descendants x ON d.parent_institution_id=x.id) SELECT 1 FROM descendants WHERE id=?", -1, &stmt, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(stmt, 1, child_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, parent_id, -1, SQLITE_TRANSIENT);
    cycle = sqlite3_step(stmt) == SQLITE_ROW;
    sqlite3_finalize(stmt);
    return cycle;
}

BfMatrixStatus bf_matrix_institution_delegate(BfMatrix *matrix, BfAuthority *authority,
                                               const char *actor, const char *parent_id,
                                               const char *child_id, const char *authority_ref,
                                               const char *at_iso, const char *purpose) {
    sqlite3_stmt *stmt = NULL; int step; int cycle;
    if (!matrix || !authority || !present(actor) || !present(parent_id) || !present(child_id)) return BF_MATRIX_INVALID;
    if (!present(authority_ref)) return BF_MATRIX_MISSING_EVIDENCE;
    if (exists(matrix->db, "matrix_institutions", "institution_id", parent_id) != 1 || exists(matrix->db, "matrix_institutions", "institution_id", child_id) != 1) return BF_MATRIX_UNKNOWN_INSTITUTION;
    if (bf_authority_has(authority, actor, "delegate", parent_id, at_iso, purpose) != 1) return BF_MATRIX_UNAUTHORIZED;
    cycle = delegation_would_cycle(matrix, parent_id, child_id);
    if (cycle < 0) return BF_MATRIX_STORAGE_ERROR;
    if (cycle) return BF_MATRIX_CYCLE;
    if (sqlite3_prepare_v2(matrix->db,
        "INSERT INTO matrix_delegations(parent_institution_id,child_institution_id,authority_actor,authority_ref,purpose) VALUES(?,?,?,?,?) ON CONFLICT(parent_institution_id,child_institution_id) DO UPDATE SET authority_actor=excluded.authority_actor,authority_ref=excluded.authority_ref,purpose=excluded.purpose", -1, &stmt, NULL) != SQLITE_OK) return BF_MATRIX_STORAGE_ERROR;
    sqlite3_bind_text(stmt, 1, parent_id, -1, SQLITE_TRANSIENT); sqlite3_bind_text(stmt, 2, child_id, -1, SQLITE_TRANSIENT); sqlite3_bind_text(stmt, 3, actor, -1, SQLITE_TRANSIENT); sqlite3_bind_text(stmt, 4, authority_ref, -1, SQLITE_TRANSIENT); sqlite3_bind_text(stmt, 5, purpose ? purpose : "", -1, SQLITE_TRANSIENT);
    step = sqlite3_step(stmt); sqlite3_finalize(stmt); return step == SQLITE_DONE ? BF_MATRIX_OK : BF_MATRIX_STORAGE_ERROR;
}

static BfMatrixStatus validate_presence(BfMatrix *matrix, const BfMatrixPresenceSpec *spec) {
    const char *world;
    if (!matrix || !spec || !present(spec->presence_id) || !present(spec->subject_id) || !present(spec->representation)) return BF_MATRIX_INVALID;
    if (!present(spec->evidence_ref)) return BF_MATRIX_MISSING_EVIDENCE;
    if (!in_set(TERRITORIES, spec->territory)) return BF_MATRIX_INVALID_TERRITORY;
    if (!bf_matrix_scope_name(spec->scope)) return BF_MATRIX_INVALID_SCOPE;
    world = bf_matrix_world_name(spec->world); if (!world) return BF_MATRIX_INVALID_WORLD;
    if (exists(matrix->db, "matrix_subjects", "subject_id", spec->subject_id) != 1) return BF_MATRIX_UNKNOWN_SUBJECT;
    if (present(spec->institution_id) && exists(matrix->db, "matrix_institutions", "institution_id", spec->institution_id) != 1) return BF_MATRIX_UNKNOWN_INSTITUTION;
    return BF_MATRIX_OK;
}

static BfMatrixStatus insert_presence(BfMatrix *matrix, const BfMatrixPresenceSpec *spec, const char *derived_from) {
    sqlite3_stmt *stmt = NULL; int step; const char *world = bf_matrix_world_name(spec->world); const char *scope = bf_matrix_scope_name(spec->scope);
    if (exists(matrix->db, "matrix_presences", "presence_id", spec->presence_id) == 1) return BF_MATRIX_DUPLICATE_PRESENCE;
    if (sqlite3_prepare_v2(matrix->db,
        "INSERT INTO matrix_presences(presence_id,subject_id,institution_id,territory,scope,world,representation,evidence_ref,derived_from_presence_id) VALUES(?,?,?,?,?,?,?,?,?)", -1, &stmt, NULL) != SQLITE_OK) return BF_MATRIX_STORAGE_ERROR;
    sqlite3_bind_text(stmt, 1, spec->presence_id, -1, SQLITE_TRANSIENT); sqlite3_bind_text(stmt, 2, spec->subject_id, -1, SQLITE_TRANSIENT);
    if (present(spec->institution_id)) sqlite3_bind_text(stmt, 3, spec->institution_id, -1, SQLITE_TRANSIENT); else sqlite3_bind_null(stmt, 3);
    sqlite3_bind_text(stmt, 4, spec->territory, -1, SQLITE_TRANSIENT); sqlite3_bind_text(stmt, 5, scope, -1, SQLITE_TRANSIENT); sqlite3_bind_text(stmt, 6, world, -1, SQLITE_TRANSIENT); sqlite3_bind_text(stmt, 7, spec->representation, -1, SQLITE_TRANSIENT); sqlite3_bind_text(stmt, 8, spec->evidence_ref, -1, SQLITE_TRANSIENT);
    if (derived_from) sqlite3_bind_text(stmt, 9, derived_from, -1, SQLITE_TRANSIENT); else sqlite3_bind_null(stmt, 9);
    step = sqlite3_step(stmt); sqlite3_finalize(stmt); return step == SQLITE_DONE ? BF_MATRIX_OK : BF_MATRIX_STORAGE_ERROR;
}

BfMatrixStatus bf_matrix_presence_bind(BfMatrix *matrix, const BfMatrixPresenceSpec *spec) {
    BfMatrixStatus status = validate_presence(matrix, spec);
    return status == BF_MATRIX_OK ? insert_presence(matrix, spec, NULL) : status;
}

static BfMatrixStatus source_presence(BfMatrix *matrix, const char *presence_id, char *subject_id, size_t subject_size, BfMatrixParticipationScope *scope) {
    sqlite3_stmt *stmt = NULL; const char *subject; const char *stored_scope;
    if (sqlite3_prepare_v2(matrix->db, "SELECT subject_id,scope FROM matrix_presences WHERE presence_id=? AND active=1", -1, &stmt, NULL) != SQLITE_OK) return BF_MATRIX_STORAGE_ERROR;
    sqlite3_bind_text(stmt, 1, presence_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) != SQLITE_ROW) { sqlite3_finalize(stmt); return BF_MATRIX_UNKNOWN_PRESENCE; }
    subject = (const char *)sqlite3_column_text(stmt, 0); stored_scope = (const char *)sqlite3_column_text(stmt, 1);
    if (!subject || !stored_scope || snprintf(subject_id, subject_size, "%s", subject) >= (int)subject_size) { sqlite3_finalize(stmt); return BF_MATRIX_STORAGE_ERROR; }
    for (*scope = BF_MATRIX_SELF; *scope <= BF_MATRIX_INSTITUTION; *scope = (BfMatrixParticipationScope)(*scope + 1)) if (strcmp(stored_scope, bf_matrix_scope_name(*scope)) == 0) break;
    sqlite3_finalize(stmt); return *scope <= BF_MATRIX_INSTITUTION ? BF_MATRIX_OK : BF_MATRIX_STORAGE_ERROR;
}

BfMatrixStatus bf_matrix_presence_project(BfMatrix *matrix, BfAuthority *authority,
                                           const char *actor, const char *source_presence_id,
                                           const BfMatrixPresenceSpec *target,
                                           const char *authority_ref, const char *at_iso,
                                           const char *purpose) {
    char source_subject[512]; BfMatrixParticipationScope source_scope; BfMatrixStatus status;
    if (!matrix || !authority || !present(actor) || !present(source_presence_id)) return BF_MATRIX_INVALID;
    if (!present(authority_ref)) return BF_MATRIX_MISSING_EVIDENCE;
    status = validate_presence(matrix, target); if (status != BF_MATRIX_OK) return status;
    status = source_presence(matrix, source_presence_id, source_subject, sizeof(source_subject), &source_scope); if (status != BF_MATRIX_OK) return status;
    if (strcmp(source_subject, target->subject_id) != 0) return BF_MATRIX_IDENTITY_CONFLICT;
    if (target->scope <= source_scope) return BF_MATRIX_SCOPE_REGRESSION;
    if (bf_authority_has(authority, actor, "publish", source_subject, at_iso, purpose) != 1) return BF_MATRIX_UNAUTHORIZED;
    return insert_presence(matrix, target, source_presence_id);
}

BfMatrixStatus bf_matrix_presence_withdraw(BfMatrix *matrix, BfAuthority *authority,
                                            const char *actor, const char *presence_id,
                                            const char *authority_ref, const char *at_iso,
                                            const char *purpose) {
    char subject_id[512]; BfMatrixParticipationScope scope; sqlite3_stmt *stmt = NULL; int step; BfMatrixStatus status;
    if (!matrix || !authority || !present(actor) || !present(presence_id)) return BF_MATRIX_INVALID;
    if (!present(authority_ref)) return BF_MATRIX_MISSING_EVIDENCE;
    status = source_presence(matrix, presence_id, subject_id, sizeof(subject_id), &scope); if (status != BF_MATRIX_OK) return status;
    if (bf_authority_has(authority, actor, "publish", subject_id, at_iso, purpose) != 1) return BF_MATRIX_UNAUTHORIZED;
    if (sqlite3_prepare_v2(matrix->db, "UPDATE matrix_presences SET active=0 WHERE presence_id=?", -1, &stmt, NULL) != SQLITE_OK) return BF_MATRIX_STORAGE_ERROR;
    sqlite3_bind_text(stmt, 1, presence_id, -1, SQLITE_TRANSIENT); step = sqlite3_step(stmt); sqlite3_finalize(stmt); return step == SQLITE_DONE ? BF_MATRIX_OK : BF_MATRIX_STORAGE_ERROR;
}

int bf_matrix_subject_presence_count(BfMatrix *matrix, const char *subject_id, int active_only) {
    sqlite3_stmt *stmt = NULL; int count = -1;
    if (!matrix || !present(subject_id) || sqlite3_prepare_v2(matrix->db, active_only ? "SELECT COUNT(*) FROM matrix_presences WHERE subject_id=? AND active=1" : "SELECT COUNT(*) FROM matrix_presences WHERE subject_id=?", -1, &stmt, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(stmt, 1, subject_id, -1, SQLITE_TRANSIENT); if (sqlite3_step(stmt) == SQLITE_ROW) count = sqlite3_column_int(stmt, 0); sqlite3_finalize(stmt); return count;
}
