/* Extraction, driven by the declaration rather than by whoever needed it.
 *
 * `bf_lex.h` holds one implementation per field kind. This holds the rulesets
 * that say which fields exist in which document, read from
 * schemas/control/text-rules.cue rather than restated here, and offers them to
 * C callers, to shell witnesses, and to a check that says what a document
 * carries that nobody declared.
 *
 * The last of those is the point. Two bugs in this estate were the same bug:
 * an undeclared text rule failing quietly. A receipt reader stopped at the
 * first quote in an ETag and every source ignored the mangled validator; the
 * atlas parser accepted any key and discarded 77 `cannot_infer` lines into a
 * scalar nothing read. Neither could be noticed because neither complained.
 * `check` complains.
 */
#define _POSIX_C_SOURCE 200809L
#include "bf_lex.h"
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_RULES 64
#define MAX_SETS 16

struct Rule { char name[64]; char kind[32]; char key[128]; char means[512]; int required; };
struct RuleSet { char name[64]; char over[128]; int report_unknown; struct Rule rules[MAX_RULES]; int rule_count; };

static struct RuleSet g_sets[MAX_SETS];
static int g_set_count;

static const char *arg(int argc, char **argv, const char *flag) {
    for (int i = 2; i + 1 < argc; i++) if (!strcmp(argv[i], flag)) return argv[i+1];
    return NULL;
}

static int has_flag(int argc, char **argv, const char *flag) {
    for (int i = 2; i < argc; i++) if (!strcmp(argv[i], flag)) return 1;
    return 0;
}

/* The rules are exported from CUE rather than duplicated here. A second copy
 * would be a second vocabulary, which is the thing being fixed. */
static int load_rules(const char *cue_path) {
    char command[2048];
    FILE *pipe;
    char *doc = NULL;
    size_t cap = 0, used = 0;
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    int ok = 0;

    snprintf(command, sizeof(command),
             "cue export %s -e rulesets --out json 2>/dev/null", cue_path);
    pipe = popen(command, "r");
    if (!pipe) return 0;
    for (;;) {
        char chunk[8192];
        size_t n = fread(chunk, 1, sizeof(chunk), pipe);
        if (!n) break;
        if (used + n + 1 > cap) {
            size_t grown = (used + n + 1) * 2;
            char *bigger = realloc(doc, grown);
            if (!bigger) { free(doc); pclose(pipe); return 0; }
            doc = bigger; cap = grown;
        }
        memcpy(doc + used, chunk, n);
        used += n;
    }
    if (pclose(pipe) != 0 || !doc || !used) { free(doc); return 0; }
    doc[used] = '\0';

    if (sqlite3_open(":memory:", &db) != SQLITE_OK) { free(doc); return 0; }
    if (sqlite3_prepare_v2(db,
        "SELECT json_extract(s.value,'$.name'), json_extract(s.value,'$.over'),"
        "       COALESCE(json_extract(s.value,'$.report_unknown'),0),"
        "       json_extract(r.value,'$.name'), json_extract(r.value,'$.kind'),"
        "       json_extract(r.value,'$.key'), json_extract(r.value,'$.means'),"
        "       COALESCE(json_extract(r.value,'$.required'),0) "
        "FROM json_each(?1,'$.rulesets') s, json_each(s.value,'$.rules') r",
        -1, &st, NULL) != SQLITE_OK) { sqlite3_close(db); free(doc); return 0; }
    sqlite3_bind_text(st, 1, doc, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *set_name = (const char *)sqlite3_column_text(st, 0);
        struct RuleSet *set = NULL;
        if (!set_name) continue;
        for (int i = 0; i < g_set_count; i++)
            if (!strcmp(g_sets[i].name, set_name)) set = &g_sets[i];
        if (!set) {
            if (g_set_count >= MAX_SETS) break;
            set = &g_sets[g_set_count++];
            snprintf(set->name, sizeof(set->name), "%s", set_name);
            snprintf(set->over, sizeof(set->over), "%s",
                     (const char *)sqlite3_column_text(st, 1));
            set->report_unknown = sqlite3_column_int(st, 2);
        }
        if (set->rule_count >= MAX_RULES) continue;
        {
            struct Rule *rule = &set->rules[set->rule_count++];
            snprintf(rule->name, sizeof(rule->name), "%s", (const char *)sqlite3_column_text(st, 3));
            snprintf(rule->kind, sizeof(rule->kind), "%s", (const char *)sqlite3_column_text(st, 4));
            snprintf(rule->key, sizeof(rule->key), "%s", (const char *)sqlite3_column_text(st, 5));
            snprintf(rule->means, sizeof(rule->means), "%s", (const char *)sqlite3_column_text(st, 6));
            rule->required = sqlite3_column_int(st, 7);
        }
        ok = 1;
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
    free(doc);
    return ok;
}

static struct RuleSet *find_set(const char *name) {
    for (int i = 0; i < g_set_count; i++)
        if (!strcmp(g_sets[i].name, name)) return &g_sets[i];
    return NULL;
}

static struct Rule *find_rule(struct RuleSet *set, const char *name) {
    for (int i = 0; i < set->rule_count; i++)
        if (!strcmp(set->rules[i].name, name)) return &set->rules[i];
    return NULL;
}

static char *read_all(const char *path, size_t *out_len) {
    FILE *fh = path ? fopen(path, "rb") : stdin;
    char *doc = NULL;
    size_t cap = 0, used = 0;
    if (!fh) return NULL;
    for (;;) {
        char chunk[8192];
        size_t n = fread(chunk, 1, sizeof(chunk), fh);
        if (!n) break;
        if (used + n + 1 > cap) {
            size_t grown = (used + n + 1) * 2;
            char *bigger = realloc(doc, grown);
            if (!bigger) { free(doc); if (path) fclose(fh); return NULL; }
            doc = bigger; cap = grown;
        }
        memcpy(doc + used, chunk, n);
        used += n;
    }
    if (path) fclose(fh);
    if (doc) doc[used] = '\0';
    if (out_len) *out_len = used;
    return doc;
}

/* One field, by declared name. A caller names the ruleset and the field and
 * never the key, the escaping or the shape -- which is the whole point. */
/* One extraction, whichever way the caller named the field. `doc` is consumed
 * (strtok), which is why it is the caller's copy. */
static int extract(const char *kind, const char *key, char *doc,
                   const char *line_select, char *out, size_t cap) {
    int found = 0;
    out[0] = '\0';
    if (!strcmp(kind, "json_string") || !strcmp(kind, "json_number") ||
        !strcmp(kind, "json_bool")) {
        /* A receipt file is a log, so `--line last` reads the newest rather
         * than the oldest -- the mistake that made a stale validator get handed
         * back to sources. */
        char *chosen = doc;
        if (line_select && !strcmp(line_select, "last")) {
            char *line = strtok(doc, "\n");
            char *newest = NULL;
            while (line) { if (line[0] == '{') newest = line; line = strtok(NULL, "\n"); }
            chosen = newest ? newest : doc;
        }
        found = !strcmp(kind, "json_string")
              ? bf_lex_json_string(chosen, key, out, cap)
              : bf_lex_json_scalar(chosen, key, out, cap);
    } else if (!strcmp(kind, "http_header") || !strcmp(kind, "http_link_rel")) {
        char *line = strtok(doc, "\n");
        while (line) {
            char *end = line + strlen(line);
            while (end > line && (end[-1] == '\r')) *--end = '\0';
            if (!strncasecmp(line, "HTTP/", 5)) { found = 0; out[0] = '\0'; }
            else if (!found)
                found = !strcmp(kind, "http_header")
                      ? bf_lex_http_header(line, key, out, cap)
                      : bf_lex_http_link_rel(line, key, out, cap);
            line = strtok(NULL, "\n");
        }
    } else if (!strcmp(kind, "tsv_column")) {
        char *line = strtok(doc, "\n");
        if (line) found = bf_lex_tsv_column(line, atoi(key), out, cap);
    } else if (!strcmp(kind, "text_column")) {
        char *line = strtok(doc, "\n");
        while (line && !found) { found = bf_lex_text_column(line, atoi(key), out, cap); line = strtok(NULL, "\n"); }
    } else if (!strcmp(kind, "yaff_field")) {
        char *line = strtok(doc, "\n");
        while (line && !found) { found = bf_lex_yaff_field(line, key, out, cap); line = strtok(NULL, "\n"); }
    } else if (!strcmp(kind, "url_query")) {
        char *line = strtok(doc, "\n");
        if (line) {
            const char *q = strchr(line, '?');
            found = bf_lex_url_query(q ? q + 1 : line, key, out, cap);
        }
    }
    return found;
}

static int cmd_get(int argc, char **argv) {
    const char *set_name = arg(argc, argv, "--set");
    const char *field = arg(argc, argv, "--field");
    const char *path = arg(argc, argv, "--file");
    const char *line_select = arg(argc, argv, "--line");
    struct RuleSet *set;
    struct Rule *rule;
    char *doc;
    char out[8192];
    int found;

    if (!set_name || !field) { fprintf(stderr, "get requires --set and --field\n"); return 2; }
    if (!(set = find_set(set_name))) { fprintf(stderr, "no such ruleset: %s\n", set_name); return 2; }
    if (!(rule = find_rule(set, field))) {
        fprintf(stderr, "ruleset %s declares no field %s; declared fields are:\n", set_name, field);
        for (int i = 0; i < set->rule_count; i++)
            fprintf(stderr, "  %-18s %-18s %s\n", set->rules[i].name, set->rules[i].kind, set->rules[i].means);
        return 2;
    }
    if (!(doc = read_all(path, NULL))) { fprintf(stderr, "cannot read input\n"); return 1; }
    found = extract(rule->kind, rule->key, doc, line_select, out, sizeof(out));
    free(doc);

    if (!found) {
        /* Absent and empty are different facts about a document, so a missing
         * required field fails rather than printing nothing and continuing. */
        if (rule->required) {
            fprintf(stderr, "%s.%s is required and absent\n", set_name, field);
            return 3;
        }
        return 1;
    }
    printf("%s\n", out);
    return 0;
}

/* The same extraction without a ruleset.
 *
 * A ruleset is right where the field set is a contract -- a receipt, an HTTP
 * response, an observation plan -- because those are worth declaring once and
 * checking against. A witness asserting one value of one reply is not a
 * contract, and making it author a ruleset would buy ceremony rather than
 * agreement. What it needs is the same reader.
 *
 * Fifty-odd witnesses hand-rolled a sed substitution that captured everything
 * up to the next quote. That is the ETag defect in shell: the capture stops at
 * the first escaped quote, and the witness then asserts on a truncated value
 * and passes. */
static int cmd_field(int argc, char **argv) {
    const char *kind = arg(argc, argv, "--kind");
    const char *key = arg(argc, argv, "--key");
    const char *path = arg(argc, argv, "--file");
    const char *line_select = arg(argc, argv, "--line");
    const char *where = arg(argc, argv, "--where");
    char *doc;
    char out[8192];
    int found;

    if (!kind || !key) { fprintf(stderr, "field requires --kind and --key\n"); return 2; }
    if (!(doc = read_all(path, NULL))) { fprintf(stderr, "cannot read input\n"); return 1; }
    if (where && !strcmp(kind, "text_column")) {
        /* The same selection over a report table: the row whose column N is
         * this value, then column M of that row. */
        char have[8192];
        const char *eq = strchr(where, '=');
        char *line;
        int wcol;
        if (!eq) { fprintf(stderr, "--where wants column=value\n"); free(doc); return 2; }
        wcol = atoi(where);
        found = 0;
        for (line = strtok(doc, "\n"); line; line = strtok(NULL, "\n")) {
            if (!bf_lex_text_column(line, wcol, have, sizeof(have))) continue;
            if (strcmp(have, eq + 1)) continue;
            if (bf_lex_text_column(line, atoi(key), out, sizeof(out))) {
                found = 1;
                if (!(line_select && !strcmp(line_select, "last"))) break;
            }
        }
        free(doc);
        if (!found) {
            if (has_flag(argc, argv, "--required")) {
                fprintf(stderr, "no row with column %s and a column %s\n", where, key);
                return 3;
            }
            return 1;
        }
        printf("%s\n", out);
        return 0;
    }
    if (where) {
        /* Select the record by one field, then read another from it. Three
         * witnesses did this with a greedy substitution across a whole line,
         * which selects by proximity rather than by record. */
        char wkey[128], wanted[512], have[8192], record[16384];
        const char *eq = strchr(where, '=');
        const char *begin, *end;
        int index = 0, want_last = line_select && !strcmp(line_select, "last");
        if (!eq) { fprintf(stderr, "--where wants key=value\n"); free(doc); return 2; }
        snprintf(wkey, sizeof(wkey), "%.*s", (int)(eq - where), where);
        snprintf(wanted, sizeof(wanted), "%s", eq + 1);
        found = 0;
        while (bf_lex_json_record(doc, index++, &begin, &end)) {
            size_t n = (size_t)(end - begin);
            if (n >= sizeof(record)) continue;
            memcpy(record, begin, n);
            record[n] = 0;
            if (!bf_lex_json_string(record, wkey, have, sizeof(have)) &&
                !bf_lex_json_scalar(record, wkey, have, sizeof(have))) continue;
            if (strcmp(have, wanted)) continue;
            if (!strcmp(kind, "json_string")
                ? bf_lex_json_string(record, key, out, sizeof(out))
                : bf_lex_json_scalar(record, key, out, sizeof(out))) {
                found = 1;
                if (!want_last) break;
            }
        }
        free(doc);
        if (!found) {
            if (has_flag(argc, argv, "--required")) {
                fprintf(stderr, "no record with %s and a %s\n", where, key);
                return 3;
            }
            return 1;
        }
        printf("%s\n", out);
        return 0;
    }
    found = extract(kind, key, doc, line_select, out, sizeof(out));
    free(doc);
    if (!found) {
        if (has_flag(argc, argv, "--required")) {
            fprintf(stderr, "%s %s is required and absent\n", kind, key);
            return 3;
        }
        return 1;
    }
    printf("%s\n", out);
    return 0;
}

/* What a document carries that nobody declared, and what it was declared to
 * carry and does not. */
static int cmd_check(int argc, char **argv) {
    const char *set_name = arg(argc, argv, "--set");
    const char *path = arg(argc, argv, "--file");
    struct RuleSet *set;
    char *doc;
    int missing = 0, undeclared = 0, first = 1;
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;

    if (!set_name) { fprintf(stderr, "check requires --set\n"); return 2; }
    if (!(set = find_set(set_name))) { fprintf(stderr, "no such ruleset: %s\n", set_name); return 2; }
    if (!(doc = read_all(path, NULL))) { fprintf(stderr, "cannot read input\n"); return 1; }

    printf("{\"ruleset\":\"%s\",\"over\":\"%s\",\"missing_required\":[", set->name, set->over);
    for (int i = 0; i < set->rule_count; i++) {
        char out[8192];
        int present;
        if (!set->rules[i].required) continue;
        present = !strcmp(set->rules[i].kind, "json_string")
                ? bf_lex_json_string(doc, set->rules[i].key, out, sizeof(out))
                : bf_lex_json_scalar(doc, set->rules[i].key, out, sizeof(out));
        if (present) continue;
        printf("%s\"%s\"", missing++ ? "," : "", set->rules[i].name);
    }
    printf("],\"undeclared\":[");

    /* Only a JSON document can be asked what keys it has. The others are line
     * shapes with no key space to enumerate, and saying so is better than
     * reporting an empty list that reads like a clean bill. */
    if (set->report_unknown && sqlite3_open(":memory:", &db) == SQLITE_OK &&
        sqlite3_prepare_v2(db, "SELECT key FROM json_each(?1) WHERE json_valid(?1)",
                           -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, doc, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *key = (const char *)sqlite3_column_text(st, 0);
            int declared = 0;
            if (!key) continue;
            for (int i = 0; i < set->rule_count; i++)
                if (!strcmp(set->rules[i].key, key)) declared = 1;
            if (declared) continue;
            printf("%s\"%s\"", first ? "" : ",", key);
            first = 0;
            undeclared++;
        }
        sqlite3_finalize(st);
    }
    if (db) sqlite3_close(db);
    free(doc);

    printf("],\"undeclared_count\":%d,\"report_unknown\":%s",
           undeclared, set->report_unknown ? "true" : "false");
    printf(",\"truth\":\"a_declared_vocabulary_checked_against_a_document_not_a_claim_the_document_is_correct\"");
    printf(",\"cannot_infer\":[\"an undeclared key -> a key that should not exist; it is a key nobody wrote a rule for\","
           "\"no missing required fields -> a well-formed document\"]}\n");
    return missing ? 4 : 0;
}

static int cmd_describe(void) {
    for (int i = 0; i < g_set_count; i++) {
        printf("ruleset %s over %s%s\n", g_sets[i].name, g_sets[i].over,
               g_sets[i].report_unknown ? " (reports undeclared keys)" : "");
        for (int r = 0; r < g_sets[i].rule_count; r++)
            printf("  %-18s %-18s %-34s %s%s\n",
                   g_sets[i].rules[r].name, g_sets[i].rules[r].kind,
                   g_sets[i].rules[r].key, g_sets[i].rules[r].required ? "[required] " : "",
                   g_sets[i].rules[r].means);
    }
    return 0;
}

static void usage(void) {
    fputs("bonfyre-lex <command> --rules <text-rules.cue>\n"
          "  describe                       every declared ruleset and field\n"
          "  get --set S --field F [--file P] [--line last]\n"
          "  check --set S [--file P]       required fields, undeclared keys\n"
          "  field --kind K --key F [--file P] [--line first|last] [--required]\n"
          "        [--where K=V]            select the record, then read the field\n"
          "\n"
          "`get` names a ruleset and a field, never a key or an escaping rule,\n"
          "because those are declared once in the rules. `field` names a kind\n"
          "and a key for the one-off case. Both read through bf_lex.h, so a\n"
          "witness and an organ cannot disagree about what a document says.\n", stderr);
}

int main(int argc, char **argv) {
    const char *rules;
    if (argc < 2) { usage(); return 2; }
    enum {
        LX_CHECK, LX_DESCRIBE, LX_FIELD, LX_GET,
    };
    static const BfCommandEntry lex_commands[] = {
        {"check", LX_CHECK, 0, NULL},
        {"describe", LX_DESCRIBE, 0, NULL},
        {"field", LX_FIELD, 0, NULL},
        {"get", LX_GET, 0, NULL},
    };
    const BfCommandEntry *command = bf_command_lookup(lex_commands,
        sizeof(lex_commands) / sizeof(lex_commands[0]), argv[1]);
    /* `field` names its own kind and key, so it needs no ruleset and no `cue`
     * on the path. A witness should be able to read one value on a host that
     * has only the binary. */
    if (command && command->id == LX_FIELD) return cmd_field(argc, argv);
    rules = arg(argc, argv, "--rules");
    if (!rules) rules = "schemas/control/text-rules.cue";
    if (!load_rules(rules)) {
        fprintf(stderr, "could not export rulesets from %s; is cue installed?\n", rules);
        return 1;
    }
    if (command && command->id == LX_DESCRIBE) return cmd_describe();
    if (command && command->id == LX_GET)      return cmd_get(argc, argv);
    if (command && command->id == LX_CHECK)    return cmd_check(argc, argv);
    if (has_flag(argc, argv, "--help")) { usage(); return 0; }
    usage();
    return 2;
}
