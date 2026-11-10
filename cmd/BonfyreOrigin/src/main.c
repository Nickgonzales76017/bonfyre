/* Where an occurrence came from, as something the estate can actually run.
 *
 * Three origins that must never be confused: this estate OBSERVED something
 * happen elsewhere, it DERIVED something from inputs it can name, or it caused
 * an EFFECT itself. Reading a derivation as an observation launders a
 * conclusion into a fact about the world; reading our own effect as an
 * observation launders it into something that happened anyway.
 *
 * The organ keeps them apart and had no caller: nothing outside its own
 * source, header and test named a symbol it exports.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "bf_occurrence_origin.h"
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

static void json_text(const char *s) {
    putchar('"');
    for (; s && *s; s++) {
        unsigned char c = (unsigned char)*s;
        if (c == '"' || c == '\\') { putchar('\\'); putchar(c); }
        else if (c < 0x20) printf("\\u%04x", c);
        else putchar(c);
    }
    putchar('"');
}

static const char *kind_name(BfOriginKind kind) {
    return kind == BF_ORIGIN_OBSERVED ? "observed"
         : kind == BF_ORIGIN_DERIVED ? "derived"
         : kind == BF_ORIGIN_EFFECT ? "effect" : "unknown";
}

static int usage(void) {
    fputs("BonfyreOrigin -- observed, derived, or something this estate did\n\n"
          "  bonfyre-origin record --db F --occurrence O --origin observed|derived|effect\n"
          "                        --subject S --at T --source SRC [--authority A]\n"
          "                        [--from ID ...]   (derived only)\n"
          "  bonfyre-origin get --db F --occurrence O\n\n"
          "A derived occurrence must name the exact inputs it came from.\n", stderr);
    return 2;
}

int main(int argc, char *argv[]) {
    const char *db = getenv("BONFYRE_ORIGIN_DB");
    const char *occurrence = NULL, *subject = NULL, *at = "", *source = "", *authority = "";
    const char *origin_name = NULL;
    const char *inputs[BF_ORIGIN_INPUT_MAX];
    size_t input_count = 0;
    BfOccurrenceOrigin *origin = NULL;
    int i;

    if (argc < 2) return usage();
    for (i = 2; i < argc - 1; i++) {
        if (!strcmp(argv[i], "--db")) db = argv[i + 1];
        else if (!strcmp(argv[i], "--occurrence")) occurrence = argv[i + 1];
        else if (!strcmp(argv[i], "--origin")) origin_name = argv[i + 1];
        else if (!strcmp(argv[i], "--subject")) subject = argv[i + 1];
        else if (!strcmp(argv[i], "--at")) at = argv[i + 1];
        else if (!strcmp(argv[i], "--source")) source = argv[i + 1];
        else if (!strcmp(argv[i], "--authority")) authority = argv[i + 1];
        else if (!strcmp(argv[i], "--from") && input_count < BF_ORIGIN_INPUT_MAX)
            inputs[input_count++] = argv[i + 1];
    }
    if (!db) {
        fputs("no origin store: pass --db or set BONFYRE_ORIGIN_DB. Refusing to create one "
              "in the current directory.\n", stderr);
        return 2;
    }
    if (bf_occurrence_origin_open(db, &origin, stderr) != 0) {
        fputs("{\"error\":\"origin store unavailable\"}\n", stdout);
        return 1;
    }

    enum { OR_GET, OR_RECORD };
    static const BfCommandEntry origin_commands[] = {
        {"get", OR_GET, 0, NULL},
        {"record", OR_RECORD, 0, NULL},
    };
    const BfCommandEntry *command = bf_command_lookup(origin_commands,
        sizeof(origin_commands) / sizeof(origin_commands[0]), argv[1]);

    if (command && command->id == OR_RECORD) {
        BfOccurrenceOriginSpec spec;
        BfOriginStatus status;
        BfOriginKind kind;
        if (!occurrence || !origin_name || !subject) { bf_occurrence_origin_close(origin); return usage(); }
        if (!strcmp(origin_name, "observed")) kind = BF_ORIGIN_OBSERVED;
        else if (!strcmp(origin_name, "derived")) kind = BF_ORIGIN_DERIVED;
        else if (!strcmp(origin_name, "effect")) kind = BF_ORIGIN_EFFECT;
        else { fprintf(stderr, "no such origin: %s\n", origin_name);
               bf_occurrence_origin_close(origin); return 2; }
        memset(&spec, 0, sizeof(spec));
        spec.occurrence_id = occurrence; spec.origin = kind; spec.subject = subject;
        spec.at = at; spec.source = source; spec.authority = authority;
        spec.derived_from = inputs; spec.derived_from_count = input_count;
        status = bf_occurrence_origin_record(origin, &spec);
        bf_occurrence_origin_close(origin);
        if (status != BF_ORIGIN_OK) {
            fputs("{\"refused\":", stdout);
            json_text(status == BF_ORIGIN_INVALID
                      ? "a derived occurrence must name the exact inputs it came from"
                      : status == BF_ORIGIN_EXISTS ? "this occurrence already has an origin"
                      : status == BF_ORIGIN_CAPACITY ? "too many inputs" : "refused");
            fputs("}\n", stdout);
            return 3;
        }
        fputs("{\"occurrence\":", stdout); json_text(occurrence);
        fputs(",\"origin\":", stdout); json_text(kind_name(kind));
        fputs(",\"cannot_infer\":", stdout);
        json_text("a derivation is not an observation and an effect this estate caused is not "
                  "something that happened anyway");
        fputs("}\n", stdout);
        return 0;
    }

    if (command && command->id == OR_GET) {
        BfOriginKind kind = BF_ORIGIN_OBSERVED;
        BfOriginIds ids;
        int found = 0;
        size_t n;
        if (!occurrence) { bf_occurrence_origin_close(origin); return usage(); }
        bf_occurrence_origin_get(origin, occurrence, &kind, &found);
        if (!found) {
            fputs("{\"found\":false}\n", stdout);
            bf_occurrence_origin_close(origin);
            return 0;
        }
        memset(&ids, 0, sizeof(ids));
        bf_occurrence_origin_inputs(origin, occurrence, &ids);
        fputs("{\"occurrence\":", stdout); json_text(occurrence);
        fputs(",\"origin\":", stdout); json_text(kind_name(kind));
        printf(",\"caused_by_us\":%s",
               bf_occurrence_origin_caused_by_us(origin, occurrence) ? "true" : "false");
        fputs(",\"derived_from\":[", stdout);
        for (n = 0; n < ids.count; n++) { if (n) putchar(','); json_text(ids.ids[n]); }
        fputs("]}\n", stdout);
        bf_occurrence_origin_close(origin);
        return 0;
    }

    bf_occurrence_origin_close(origin);
    return usage();
}
