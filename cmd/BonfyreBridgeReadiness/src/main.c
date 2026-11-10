#define _POSIX_C_SOURCE 200809L
#include "../generated/readiness_contract.h"

#include <ctype.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

typedef struct {
    char *occurrence;
    char *semantic_sha;
} DomainReceipt;

typedef struct {
    DomainReceipt *items;
    size_t len;
    size_t cap;
} DomainReceipts;

typedef struct {
    long long domain_valid;
    long long domain_rejected;
    long long protocol_valid;
    long long protocol_rejected;
    long long protocol_joined;
    long long raw_rows;
    DomainReceipts domains;
} EvidenceSummary;

typedef struct {
    int valid;
    char *contract;
    long long enactments;
    long long programs;
} WitnessSummary;

static const char *opt(int argc, char **argv, const char *name, const char *fallback) {
    int i;
    for (i = 2; i + 1 < argc; ++i) {
        if (!strcmp(argv[i], name)) return argv[i + 1];
    }
    return fallback;
}

static void usage(FILE *out) {
    fprintf(out,
        "Usage:\n"
        "  bonfyre-bridge-readiness audit --evidence FILE --protocol-witness FILE [--output FILE]\n");
}

static char *read_file(const char *path) {
    FILE *f;
    long size;
    char *buf;
    if (!path || !(f = fopen(path, "rb"))) return NULL;
    if (fseek(f, 0, SEEK_END) != 0 || (size = ftell(f)) < 0 || fseek(f, 0, SEEK_SET) != 0) {
        fclose(f);
        return NULL;
    }
    buf = malloc((size_t)size + 1);
    if (!buf) {
        fclose(f);
        return NULL;
    }
    if (size > 0 && fread(buf, 1, (size_t)size, f) != (size_t)size) {
        free(buf);
        fclose(f);
        return NULL;
    }
    buf[size] = '\0';
    fclose(f);
    return buf;
}

static char *json_text(sqlite3 *db, const char *json, const char *path) {
    sqlite3_stmt *st = NULL;
    char *out = NULL;
    if (sqlite3_prepare_v2(db, "SELECT json_extract(?1,?2)", -1, &st, NULL) != SQLITE_OK)
        return NULL;
    sqlite3_bind_text(st, 1, json, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, path, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_type(st, 0) == SQLITE_TEXT)
        out = strdup((const char *)sqlite3_column_text(st, 0));
    sqlite3_finalize(st);
    return out;
}

static long long json_int(sqlite3 *db, const char *json, const char *path, int *present) {
    sqlite3_stmt *st = NULL;
    long long out = 0;
    *present = 0;
    if (sqlite3_prepare_v2(db, "SELECT json_extract(?1,?2)", -1, &st, NULL) != SQLITE_OK)
        return 0;
    sqlite3_bind_text(st, 1, json, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, path, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_type(st, 0) == SQLITE_INTEGER) {
        out = sqlite3_column_int64(st, 0);
        *present = 1;
    }
    sqlite3_finalize(st);
    return out;
}

static int json_bool(sqlite3 *db, const char *json, const char *path, int *present) {
    int p = 0;
    long long value = json_int(db, json, path, &p);
    *present = p;
    return p ? value != 0 : 0;
}

static int valid_hex64(const char *s) {
    size_t i;
    if (!s || strlen(s) != 64) return 0;
    for (i = 0; i < 64; ++i) {
        if (!((s[i] >= '0' && s[i] <= '9') || (s[i] >= 'a' && s[i] <= 'f')))
            return 0;
    }
    return 1;
}

static int external_occurrence(const char *s) {
    return s && !strncmp(s, "external:", 9) && s[9] != '\0';
}

static int text_eq(sqlite3 *db, const char *json, const char *path, const char *expected) {
    char *value = json_text(db, json, path);
    int ok = value && !strcmp(value, expected);
    free(value);
    return ok;
}

static int required_hashes(sqlite3 *db, const char *json,
                           const char *const *fields, size_t count) {
    size_t i;
    for (i = 0; i < count; ++i) {
        char path[160];
        char *value;
        if (snprintf(path, sizeof(path), "$.%s", fields[i]) < 0) return 0;
        value = json_text(db, json, path);
        if (!valid_hex64(value)) {
            free(value);
            return 0;
        }
        free(value);
    }
    return 1;
}

static int domain_push(DomainReceipts *domains, const char *occurrence, const char *semantic_sha) {
    DomainReceipt *next;
    if (domains->len == domains->cap) {
        size_t cap = domains->cap ? domains->cap * 2 : 8;
        next = realloc(domains->items, cap * sizeof(*next));
        if (!next) return 0;
        domains->items = next;
        domains->cap = cap;
    }
    domains->items[domains->len].occurrence = strdup(occurrence);
    domains->items[domains->len].semantic_sha = strdup(semantic_sha);
    if (!domains->items[domains->len].occurrence || !domains->items[domains->len].semantic_sha)
        return 0;
    domains->len++;
    return 1;
}

static int domain_has(const DomainReceipts *domains, const char *occurrence, const char *semantic_sha) {
    size_t i;
    for (i = 0; i < domains->len; ++i) {
        if (!strcmp(domains->items[i].occurrence, occurrence) &&
            !strcmp(domains->items[i].semantic_sha, semantic_sha))
            return 1;
    }
    return 0;
}

static void domains_free(DomainReceipts *domains) {
    size_t i;
    for (i = 0; i < domains->len; ++i) {
        free(domains->items[i].occurrence);
        free(domains->items[i].semantic_sha);
    }
    free(domains->items);
    memset(domains, 0, sizeof(*domains));
}

static int validate_binding_base(sqlite3 *db, const char *json, const BfBridgeBinding *contract) {
    int present = 0;
    int validated;
    char *occurrence = NULL;
    long long programs = 0;
    if (!text_eq(db, json, "$.schema", contract->schema) ||
        !text_eq(db, json, "$.axis", contract->axis) ||
        !text_eq(db, json, "$.owner", contract->owner) ||
        !text_eq(db, json, "$.from_owner", contract->from_owner) ||
        !text_eq(db, json, "$.to_owner", contract->to_owner) ||
        !text_eq(db, json, "$.state", contract->state))
        return 0;
    validated = json_bool(db, json, "$.binding_validated", &present);
    if (contract->require_validated && (!present || !validated)) return 0;
    occurrence = json_text(db, json, "$.occurrence_id");
    if (!external_occurrence(occurrence)) {
        free(occurrence);
        return 0;
    }
    free(occurrence);
    if (contract->minimum_programs > 0) {
        programs = json_int(db, json, "$.programs", &present);
        if (!present || programs < contract->minimum_programs) return 0;
    }
    return 1;
}

static int load_evidence(sqlite3 *db, const char *path, EvidenceSummary *summary) {
    FILE *f = fopen(path, "r");
    char *line = NULL;
    size_t cap = 0;
    if (!f) return 0;

    while (getline(&line, &cap, f) >= 0) {
        char *schema;
        if (!*line || *line == '\n') continue;
        summary->raw_rows++;
        schema = json_text(db, line, "$.schema");
        if (schema && !strcmp(schema, bf_bridge_domain_binding.schema)) {
            char *occurrence = NULL;
            char *semantic = NULL;
            int valid = validate_binding_base(db, line, &bf_bridge_domain_binding) &&
                required_hashes(db, line, bf_bridge_domain_binding_hash_fields,
                                bf_bridge_domain_binding_HASH_FIELD_COUNT);
            if (valid) {
                occurrence = json_text(db, line, "$.occurrence_id");
                semantic = json_text(db, line, "$.semantic_record_sha256");
                valid = occurrence && semantic && domain_push(&summary->domains, occurrence, semantic);
            }
            if (valid) summary->domain_valid++;
            else summary->domain_rejected++;
            free(occurrence);
            free(semantic);
        }
        free(schema);
    }

    if (ferror(f)) {
        free(line);
        fclose(f);
        return 0;
    }
    rewind(f);

    while (getline(&line, &cap, f) >= 0) {
        char *schema;
        if (!*line || *line == '\n') continue;
        schema = json_text(db, line, "$.schema");
        if (schema && !strcmp(schema, bf_bridge_protocol_binding.schema)) {
            char *occurrence = NULL;
            char *semantic = NULL;
            char *contract = NULL;
            int valid = validate_binding_base(db, line, &bf_bridge_protocol_binding) &&
                required_hashes(db, line, bf_bridge_protocol_binding_hash_fields,
                                bf_bridge_protocol_binding_HASH_FIELD_COUNT);
            if (valid) {
                occurrence = json_text(db, line, "$.occurrence_id");
                semantic = json_text(db, line, "$.semantic_record_sha256");
                contract = json_text(db, line, "$.contract");
                valid = occurrence && semantic && contract && *contract;
            }
            if (valid) {
                summary->protocol_valid++;
                if (domain_has(&summary->domains, occurrence, semantic))
                    summary->protocol_joined++;
            } else {
                summary->protocol_rejected++;
            }
            free(occurrence);
            free(semantic);
            free(contract);
        }
        free(schema);
    }
    free(line);
    fclose(f);
    return 1;
}

static WitnessSummary load_witness(sqlite3 *db, const char *path) {
    WitnessSummary result = {0};
    const BfBridgeWitness *contract = &bf_bridge_derivation_witness;
    char *json = read_file(path);
    int present = 0;
    int registered, scoped, targeted, atomic;
    if (!json) return result;

    registered = json_bool(db, json, "$.registered", &present);
    if (!present) goto done;
    scoped = json_bool(db, json, "$.scoped", &present);
    if (!present) goto done;
    targeted = json_bool(db, json, "$.targeted_invalidation", &present);
    if (!present) goto done;
    atomic = json_bool(db, json, "$.atomic", &present);
    if (!present) goto done;

    result.enactments = json_int(db, json, "$.enactments", &present);
    if (!present) goto done;
    result.programs = json_int(db, json, "$.programs", &present);
    if (!present) goto done;
    result.contract = json_text(db, json, "$.contract");

    if (!text_eq(db, json, "$.schema", contract->schema) ||
        !result.contract || !*result.contract ||
        (contract->require_registered && !registered) ||
        (contract->require_scoped && !scoped) ||
        (contract->require_targeted_invalidation && !targeted) ||
        (contract->require_atomic && !atomic) ||
        result.enactments < contract->minimum_enactments ||
        result.programs < contract->minimum_programs)
        goto done;
    result.valid = 1;

done:
    free(json);
    return result;
}

static char *json_quote(sqlite3 *db, const char *value) {
    sqlite3_stmt *st = NULL;
    char *out = NULL;
    if (sqlite3_prepare_v2(db, "SELECT json_quote(?1)", -1, &st, NULL) != SQLITE_OK)
        return NULL;
    sqlite3_bind_text(st, 1, value ? value : "", -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_text(st, 0))
        out = strdup((const char *)sqlite3_column_text(st, 0));
    sqlite3_finalize(st);
    return out;
}

static int write_projection(sqlite3 *db, const char *path,
                            const EvidenceSummary *evidence, const WitnessSummary *witness) {
    FILE *out = path ? fopen(path, "w") : stdout;
    char *quoted_contract;
    int domain_ready = evidence->domain_valid > 0;
    int protocol_ready = evidence->protocol_joined > 0 && witness->valid;
    int overall = domain_ready && protocol_ready;
    if (!out) return 0;
    quoted_contract = json_quote(db, witness->contract ? witness->contract : "");

    fprintf(out,
        "{\"schema\":\"%s\",\"truth_level\":\"receipt_backed_projection\","
        "\"overall_ready\":%s,\"bridges\":{"
        "\"ForeignTwinGraph->DomainProfile\":{"
        "\"ready\":%s,\"validated_bindings\":%lld,\"rejected_bindings\":%lld,"
        "\"validated_receipt_pairs\":%zu,\"source_text_used\":false},"
        "\"ProtocolContract->SemanticDerivationGraph\":{"
        "\"ready\":%s,\"validated_protocol_bindings\":%lld,\"rejected_protocol_bindings\":%lld,"
        "\"joined_protocol_bindings\":%lld,\"registration_witness_valid\":%s,"
        "\"witness_contract\":%s,\"witness_enactments\":%lld,\"witness_programs\":%lld,"
        "\"source_text_used\":false}},"
        "\"evidence_rows\":%lld,"
        "\"cannot_infer\":["
        "\"receipt presence -> provider authority\","
        "\"validated bridge -> successful future enactment\","
        "\"source implementation text -> bridge readiness\"]}\n",
        bf_bridge_readiness_schema,
        overall ? "true" : "false",
        domain_ready ? "true" : "false",
        evidence->domain_valid,
        evidence->domain_rejected,
        evidence->domains.len,
        protocol_ready ? "true" : "false",
        evidence->protocol_valid,
        evidence->protocol_rejected,
        evidence->protocol_joined,
        witness->valid ? "true" : "false",
        quoted_contract ? quoted_contract : "\"\"",
        witness->enactments,
        witness->programs,
        evidence->raw_rows);

    free(quoted_contract);
    if (path) fclose(out);
    return 1;
}

int main(int argc, char **argv) {
    const char *evidence;
    const char *witness_path;
    const char *output;
    sqlite3 *db = NULL;
    EvidenceSummary summary = {0};
    WitnessSummary witness = {0};
    int rc = 1;

    enum { BR_HELP, BR_AUDIT };
    static const BfCommandEntry readiness_commands[] = {
        {"--help", BR_HELP, 0, NULL},
        {"audit", BR_AUDIT, 0, NULL},
        {"help", BR_HELP, 0, NULL},
    };
    const BfCommandEntry *command = argc < 2 ? NULL : bf_command_lookup(readiness_commands,
        sizeof(readiness_commands) / sizeof(readiness_commands[0]), argv[1]);
    if (argc < 2 || (command && command->id == BR_HELP)) {
        usage(stdout);
        return argc < 2 ? 1 : 0;
    }
    if (!command || command->id != BR_AUDIT) {
        usage(stderr);
        return 1;
    }

    evidence = opt(argc, argv, "--evidence", NULL);
    witness_path = opt(argc, argv, "--protocol-witness", NULL);
    output = opt(argc, argv, "--output", NULL);
    if (!evidence || !witness_path) {
        usage(stderr);
        return 1;
    }

    if (sqlite3_open(":memory:", &db) != SQLITE_OK) goto done;
    if (!load_evidence(db, evidence, &summary)) goto done;
    witness = load_witness(db, witness_path);
    if (!write_projection(db, output, &summary, &witness)) goto done;

    printf("{\"domain_valid\":%lld,\"protocol_valid\":%lld,\"protocol_joined\":%lld,"
           "\"witness_valid\":%s,\"overall_ready\":%s}\n",
           summary.domain_valid,
           summary.protocol_valid,
           summary.protocol_joined,
           witness.valid ? "true" : "false",
           (summary.domain_valid > 0 && summary.protocol_joined > 0 && witness.valid) ? "true" : "false");
    rc = 0;

done:
    free(witness.contract);
    domains_free(&summary.domains);
    if (db) sqlite3_close(db);
    return rc;
}
