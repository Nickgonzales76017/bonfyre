/* POSIX/GNU interfaces used below are hidden by glibc under -std=c11 without
   a feature macro. macOS exposes them regardless, so this class of break is
   invisible on the dev machine and fatal on the Linux deploy host. */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

/**
 * main.c
 *
 * BonfyreFrappeCompiler — Universal Uplift Runtime Compiler
 *
 * Compiles Frappe applications into dependent, stratified, proof-carrying
 * business computation runtime.
 */

#include "bf_app_lineages.h"
#include "bonfyre_frappe.h"
#include "bonfyre_frappe_condition.h"
#include "bf_jsonw.h"
#include "bonfyre.h"
#include <dirent.h>
#include <limits.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <getopt.h>
#include <sqlite3.h>
#include <unistd.h>

#include "bf_lex.h"
static void print_usage(const char *prog) {
    printf("BonfyreFrappeCompiler — Universal Uplift Runtime Compiler\n\n");
    printf("Usage: %s [options] <app_path>\n\n", prog);
    printf("Options:\n");
    printf("  -o, --output DIR         Output directory (default: ./generated/projections/frappe)\n");
    printf("  -r, --root DIR           Bonfyre root directory (default: $BONFYRE_ROOT or ..)\n");
    printf("  --dry-run                Parse and analyze only, don't generate output\n");
    printf("  --emit-schema-ir FILE    Emit schema intermediate representation JSON\n");
    printf("  --emit-bindings FILE     Emit binding derivations JSON\n");
    printf("  --emit-rule-universes FILE  Emit rule universes JSON\n");
    printf("  --emit-pack FILE         Emit complete app pack\n");
    printf("  --emit-cms-schema FILE   Emit one safe native CMS content-type schema\n");
    printf("  --cms-doctype NAME       DocType to project with --emit-cms-schema\n");
    printf("  --emit-lineage-catalog FILE  Emit the registered Frappe lineage catalog ('-' for stdout)\n");
    printf("  --emit-interaction-fingerprints FILE  Emit measured DocType interaction grammars JSON\n");
    printf("  --emit-surface-programs FILE  Emit the compiled layout, field presentations and organs ('-' for stdout)\n");
    printf("  --emit-surface-db FILE   Write those programs into a SQLite store the composer queries\n");
    printf("  --emit-condition-plans FILE  Emit every conditional field property as a plan, its uses and its reads ('-' for stdout)\n");
    printf("  --eval-condition EXPR    Compile one condition; with --doc JSON evaluate it (no app needed)\n");
    printf("  --doc JSON               Document for --eval-condition\n");
    printf("  --parent-doc JSON        Parent document for --eval-condition\n");
    printf("  --env JSON               What the estate binds for names that are neither doc nor parent\n");
    printf("  --emit-notification-plans FILE  Emit every Notification definition as trigger, condition plan, recipients and template\n");
    printf("  --prepare-notification FILE  With --event and --doc: say whether one definition fires and compose the held message (no app needed)\n");
    printf("  --event NAME             New, Save, Submit, Cancel, Value Change or Daily\n");
    printf("  --previous-doc JSON      The record as it was, for Value Change\n");
    printf("  --today DATE             The day to count from, for Days Before and Days After\n");
    printf("  --notify DIR             With --document-type, --event and --doc: every definition under DIR for that kind of record\n");
    printf("  --name TEXT              The record's name, when the store it came from calls it something else\n");
    printf("  --not-carried A,B        Fields that store keeps for its own use; a condition reading one is not answered\n");
    printf("  --queue-lines            Print only composed messages, as KEY<tab>MESSAGE, ready to file\n");
    printf("  --emit-metric-plans FILE Emit every Number Card and Dashboard Chart as a read plan or a named refusal\n");
    printf("  --read-metric FILE       With --store DB and --table NAME: answer one definition, read only (no app needed; '-' reads it from standard input)\n");
    printf("  --param NAME=VALUE       What the reader says where the donor would ask for the reader's default\n");
    printf("  --phase PHASE            Run up to specified phase (0-12)\n");
    printf("  -v, --verbose            Verbose output\n");
    printf("  -h, --help               Show this help\n");
    printf("\n");
    printf("Phases:\n");
    printf("  0  - Inventory\n");
    printf("  1  - Schema graph build\n");
    printf("  2  - Dependent type elaboration\n");
    printf("  3  - Signal extraction\n");
    printf("  4  - Binding derivation (Datalog)\n");
    printf("  5  - Type elaboration\n");
    printf("  6  - Proof elaboration\n");
    printf("  7  - Rule universe derivation\n");
    printf("  8  - Rule checks\n");
    printf("  9  - Route/surface generation\n");
    printf("  10 - Storage optimization\n");
    printf("  11 - Semantic substrate build\n");
    printf("  12 - Coverage and theorem audit\n");
    printf("\n");
    printf("Example:\n");
    printf("  %s -o ./pack_output ~/frappe-bench/apps/erpnext\n", prog);
    printf("  %s --phase 4 --emit-bindings bindings.json ~/frappe-bench/apps/frappe\n", prog);
    printf("\n");
}

/* BfAppLineage now carries this shape; see bf_app_lineages.h. */

/* The compiler owns this registry.  Downstream gauntlets and smoke tests read
 * it through --emit-lineage-catalog rather than carrying another nine-app list. */
/* The registry now comes from architecture/apps/app-lineages.contract.yaff via
 * the generated header. It lived here as a C table, which meant the set of
 * application grammars this institution understands could only be answered by
 * building this binary and asking it.
 *
 * The struct is unchanged in shape and order, plus a dialect column recording
 * where each grammar was first extracted from. Frappe is that provenance for
 * all nine today; it is not the type of an application lineage. */
static const BfAppLineage BF_FRAPPE_LINEAGES[] = BF_APP_LINEAGES;

static const size_t BF_FRAPPE_LINEAGE_COUNT = BF_APP_LINEAGE_N;

/* Delegates to the one implementation. Nineteen copies of this shape existed
 * with sixteen different opinions about escaping.
 * This copy was one of the four defects: it wrote nothing at all for any byte below 0x20, so an authored description
 * with two lines in it was emitted as one line with the newline simply gone --
 * a silent edit of the content, not of the encoding.
 */
static void json_string(FILE *file, const char *value) {
    bf_lex_json_emit(file, value);
}

static void json_pipe_array(FILE *file, const char *values) {
    fputc('[', file);
    const char *cursor = values ? values : "";
    int first = 1;
    while (*cursor) {
        const char *end = strchr(cursor, '|');
        size_t length = end ? (size_t)(end - cursor) : strlen(cursor);
        char value[128];
        if (length >= sizeof(value)) length = sizeof(value) - 1;
        memcpy(value, cursor, length);
        value[length] = '\0';
        if (!first) fputc(',', file);
        json_string(file, value);
        first = 0;
        if (!end) break;
        cursor = end + 1;
    }
    fputc(']', file);
}

static int emit_lineage_catalog(const char *path) {
    FILE *file = !strcmp(path, "-") ? stdout : fopen(path, "wb");
    if (!file) {
        fprintf(stderr, "Error: cannot emit lineage catalog to %s\n", path);
        return -1;
    }
    fprintf(file, "{\"schema\":\"bonfyre.frappe-lineage-catalog.v1\",\"lineages\":[");
    for (size_t i = 0; i < BF_FRAPPE_LINEAGE_COUNT; ++i) {
        const BfAppLineage *spec = &BF_FRAPPE_LINEAGES[i];
        if (i) fputc(',', file);
        fprintf(file, "{\"lineage\":"); json_string(file, spec->lineage);
        fprintf(file, ",\"app_dir\":"); json_string(file, spec->app_dir);
        fprintf(file, ",\"flow_stage\":"); json_string(file, spec->flow_stage);
        fprintf(file, ",\"representative_doctype\":"); json_string(file, spec->representative_doctype);
        fprintf(file, ",\"company_flow_after\":");
        if (spec->company_flow_after[0]) json_string(file, spec->company_flow_after); else fprintf(file, "null");
        fprintf(file, ",\"owns\":"); json_pipe_array(file, spec->owns);
        fprintf(file, ",\"consumes\":"); json_pipe_array(file, spec->consumes);
        fprintf(file, ",\"publishes\":"); json_pipe_array(file, spec->publishes);
        fputc('}', file);
    }
    fprintf(file, "]}\n");
    int ok = ferror(file) ? -1 : 0;
    if (file != stdout && fclose(file) != 0) ok = -1;
    return ok;
}

static int name_has_any(const char *value, const char *const *needles, size_t count) {
    char lowered[512];
    size_t n = value ? strlen(value) : 0;
    if (n >= sizeof(lowered)) n = sizeof(lowered) - 1;
    for (size_t i = 0; i < n; ++i) lowered[i] = (char)tolower((unsigned char)value[i]);
    lowered[n] = '\0';
    for (size_t i = 0; i < count; ++i)
        if (strstr(lowered, needles[i])) return 1;
    return 0;
}

/* This is a measured interaction signature over metadata the native compiler
 * actually parsed. It is expressly not a Frappe runtime clone: it describes
 * which generic renderer grammars a DocType can evidence, while controllers,
 * permissions, records and effects remain with their real owners. */
static void emit_interaction_roles(FILE *file, const BfDocType *doctype) {
    static const char *const state_words[] = {"status", "stage", "workflow", "state"};
    static const char *const correspondence_words[] = {"email", "message", "comment", "communication", "reply"};
    static const char *const progression_words[] = {"course", "lesson", "chapter", "progress", "completion"};
    int first = 1, lifecycle = 0, correspondence = 0, progression = 0;
    size_t links = 0, tables = 0, temporal = 0, notes = 0, media = 0, measures = 0, spatial = 0;
#define ROLE(value) do { if (!first) fputc(',', file); json_string(file, (value)); first = 0; } while (0)
    ROLE("record-form");
    ROLE("list-grid");
    for (size_t i = 0; doctype && i < doctype->field_count; ++i) {
        const BfField *field = &doctype->fields[i];
        const char *name = field->fieldname ? field->fieldname : field->label;
        lifecycle |= name_has_any(name, state_words, sizeof(state_words) / sizeof(state_words[0]));
        correspondence |= name_has_any(name, correspondence_words, sizeof(correspondence_words) / sizeof(correspondence_words[0]));
        progression |= name_has_any(name, progression_words, sizeof(progression_words) / sizeof(progression_words[0]));
        switch (field->fieldtype) {
            case BF_TYPE_LINK: case BF_TYPE_DYNAMIC_LINK: links++; break;
            case BF_TYPE_TABLE: case BF_TYPE_TABLE_MULTISELECT: tables++; break;
            case BF_TYPE_DATE: case BF_TYPE_DATETIME: case BF_TYPE_TIME: temporal++; break;
            case BF_TYPE_TEXT: case BF_TYPE_LONG_TEXT: case BF_TYPE_SMALL_TEXT:
            case BF_TYPE_MARKDOWN: case BF_TYPE_TEXT_EDITOR: notes++; break;
            case BF_TYPE_ATTACH: case BF_TYPE_ATTACH_IMAGE: media++; break;
            case BF_TYPE_INT: case BF_TYPE_FLOAT: case BF_TYPE_CURRENCY:
            case BF_TYPE_CURRENCY_DEPENDENT: case BF_TYPE_DURATION: case BF_TYPE_RATING: measures++; break;
            case BF_TYPE_GEOLOCATION: spatial++; break;
            default: break;
        }
    }
    if (lifecycle || (doctype && doctype->is_submittable)) ROLE("kanban");
    if (temporal || (doctype && (doctype->track_changes || doctype->track_seen || doctype->track_views))) ROLE("record-timeline");
    if (correspondence) ROLE("correspondence");
    if (temporal) ROLE("calendar");
    if (measures >= 2) ROLE("dashboard");
    if (links) ROLE("relationship-inspector");
    if (tables) ROLE("composition-table");
    if (notes) ROLE("longform-editor");
    if (media) ROLE("file-preview");
    if (spatial) ROLE("spatial");
    if (progression) ROLE("progression");
#undef ROLE
}

static int emit_interaction_fingerprints(const char *path, const BfAppPack *pack) {
    FILE *file = !strcmp(path, "-") ? stdout : fopen(path, "wb");
    if (!file) {
        fprintf(stderr, "Error: cannot emit interaction fingerprints to %s\n", path);
        return -1;
    }
    fprintf(file, "{\"schema\":\"bonfyre.frappe-interaction-fingerprints.v1\",\"app\":");
    json_string(file, pack && pack->schema ? pack->schema->app_name : "");
    fprintf(file, ",\"fingerprints\":[");
    for (size_t i = 0; pack && pack->schema && i < pack->schema->doctype_count; ++i) {
        const BfDocType *doctype = pack->schema->doctypes[i];
        size_t required = 0, links = 0, tables = 0, temporal = 0, measures = 0, attachments = 0;
        if (!doctype) continue;
        if (i) fputc(',', file);
        for (size_t j = 0; j < doctype->field_count; ++j) {
            const BfField *field = &doctype->fields[j];
            if (field->reqd) required++;
            if (field->fieldtype == BF_TYPE_LINK || field->fieldtype == BF_TYPE_DYNAMIC_LINK) links++;
            if (field->fieldtype == BF_TYPE_TABLE || field->fieldtype == BF_TYPE_TABLE_MULTISELECT) tables++;
            if (field->fieldtype == BF_TYPE_DATE || field->fieldtype == BF_TYPE_DATETIME || field->fieldtype == BF_TYPE_TIME) temporal++;
            if (field->fieldtype == BF_TYPE_INT || field->fieldtype == BF_TYPE_FLOAT || field->fieldtype == BF_TYPE_CURRENCY || field->fieldtype == BF_TYPE_CURRENCY_DEPENDENT || field->fieldtype == BF_TYPE_DURATION || field->fieldtype == BF_TYPE_RATING) measures++;
            if (field->fieldtype == BF_TYPE_ATTACH || field->fieldtype == BF_TYPE_ATTACH_IMAGE) attachments++;
        }
        char fingerprint[768];
        snprintf(fingerprint, sizeof(fingerprint), "frappe:%s:%s",
                 pack->schema->app_name ? pack->schema->app_name : "app",
                 doctype->name ? doctype->name : "doctype");
        fprintf(file, "{\"fingerprint_id\":"); json_string(file, fingerprint);
        fprintf(file, ",\"doctype\":"); json_string(file, doctype->name);
        fprintf(file, ",\"module\":"); json_string(file, doctype->module);
        fprintf(file, ",\"interaction_roles\":["); emit_interaction_roles(file, doctype); fprintf(file, "]");
        fprintf(file, ",\"field_counts\":{\"total\":%zu,\"required\":%zu,\"links\":%zu,\"tables\":%zu,\"temporal\":%zu,\"measures\":%zu,\"attachments\":%zu}", doctype->field_count, required, links, tables, temporal, measures, attachments);
        fprintf(file, ",\"record_posture\":{\"submittable\":%s,\"child\":%s,\"single\":%s,\"tree\":%s,\"tracks_changes\":%s}", doctype->is_submittable ? "true" : "false", doctype->is_child ? "true" : "false", doctype->is_single ? "true" : "false", doctype->is_tree ? "true" : "false", doctype->track_changes ? "true" : "false");
        fprintf(file, ",\"cannot_infer\":\"a Frappe interaction fingerprint -> a Frappe controller, permission, document, runtime, authority, effect right or a requirement to reproduce a donor interface\"}");
    }
    fprintf(file, "]}\n");
    {
        int ok = ferror(file) ? -1 : 0;
        if (file != stdout && fclose(file) != 0) ok = -1;
        return ok;
    }
}

/* ------------------------------------------------------------------ surfaces
 *
 * What a compiled DocType looks like, as opposed to what it is called.
 *
 * Until now the pack's "surfaces" were 44 strings of the form
 * "surface:crm/CRM Notification" -- a name and nothing else. Everything a
 * reader would need in order to see the thing was parsed and then dropped: the
 * tab and section and column breaks that are the entire layout, each field's
 * kind, which fields are mandatory, what a link points at, and which fields
 * must never be shown at all. A renderer given only the name has to invent all
 * of that, which is how a compiled app turns back into a hand-written page.
 *
 * So this emits the layout as the DocType states it, each field carrying the
 * generic presentation it asks for and the name of the client organ that
 * answers it -- "list" for a child table, "space" for a geolocation, and
 * nothing at all for the ordinary majority, because most fields want no
 * script. The organ names are the estate's own; nothing here is Frappe's
 * client, and none of it is a controller, a permission or an effect.
 *
 * A field this estate refuses to render is named in `withheld` with the reason,
 * rather than omitted silently. A missing field and a refused one are different
 * facts and a reader is entitled to tell them apart.
 */
static const char *field_presentation(const BfField *field) {
    switch (field->fieldtype) {
        case BF_TYPE_DATA: case BF_TYPE_READ_ONLY:       return "text";
        case BF_TYPE_SMALL_TEXT: case BF_TYPE_TEXT:      return "prose";
        case BF_TYPE_LONG_TEXT:                          return "longform";
        case BF_TYPE_MARKDOWN: case BF_TYPE_TEXT_EDITOR: return "rich-text";
        case BF_TYPE_HTML:                               return "authored-markup";
        case BF_TYPE_CODE: case BF_TYPE_JSON:            return "code";
        case BF_TYPE_SELECT:                             return "choice";
        case BF_TYPE_LINK:                               return "reference";
        case BF_TYPE_DYNAMIC_LINK:                       return "dynamic-reference";
        case BF_TYPE_TABLE: case BF_TYPE_TABLE_MULTISELECT: return "composition";
        case BF_TYPE_CHECK:                              return "boolean";
        case BF_TYPE_INT:                                return "count";
        case BF_TYPE_FLOAT:                              return "quantity";
        case BF_TYPE_CURRENCY: case BF_TYPE_CURRENCY_DEPENDENT: return "money";
        case BF_TYPE_DATE:                               return "date";
        case BF_TYPE_DATETIME:                           return "instant";
        case BF_TYPE_TIME:                               return "time-of-day";
        case BF_TYPE_DURATION:                           return "duration";
        case BF_TYPE_RATING:                             return "rating";
        case BF_TYPE_COLOR:                              return "colour";
        case BF_TYPE_ICON:                               return "icon";
        case BF_TYPE_GEOLOCATION:                        return "place";
        case BF_TYPE_IMAGE: case BF_TYPE_ATTACH_IMAGE:   return "image";
        case BF_TYPE_ATTACH:                             return "attachment";
        case BF_TYPE_SIGNATURE:                          return "signature";
        case BF_TYPE_BARCODE:                            return "barcode";
        case BF_TYPE_HEADING:                            return "heading";
        case BF_TYPE_BUTTON:                             return "stated-action";
        default:                                         return "text";
    }
}

/* The client organ a field needs, or nothing. Most fields need nothing, and
 * saying so is the point: an organ named where none is required is how a page
 * acquires a script it does not use. */
static const char *field_organ(const BfField *field) {
    switch (field->fieldtype) {
        case BF_TYPE_TABLE: case BF_TYPE_TABLE_MULTISELECT: return "list";
        case BF_TYPE_GEOLOCATION:                           return "space";
        default:                                            return "";
    }
}

/* Why this estate will not render a field. Empty means it renders. */
static const char *field_withheld(const BfField *field) {
    /* frappe's own new-doctype-1 and new-doctype-2 carry fields with a label
     * and no fieldname. There is nothing to address such a field by, so it is
     * named as refused rather than emitted with an empty name that a renderer
     * would quietly treat as a real one. */
    if (!field->fieldname || !*field->fieldname)
        return "the DocType gave this field a label and no name, so there is nothing to address it by";
    if (field->fieldtype == BF_TYPE_PASSWORD)
        return "a secret is never rendered, echoed or stored by this estate";
    if (field->fieldtype == BF_TYPE_BUTTON)
        return "a button in a donor app invokes that app's controller; here it is named as an action somebody may take, never wired to one";
    if (field->hidden)
        return "the DocType marks it hidden, and hidden in the record it came from means hidden here";
    return "";
}

static int emit_surface_programs(const char *path, const BfAppPack *pack) {
    FILE *file = !strcmp(path, "-") ? stdout : fopen(path, "wb");
    if (!file) {
        fprintf(stderr, "Error: cannot emit surface programs to %s\n", path);
        return -1;
    }
    fprintf(file, "{\"schema\":\"bonfyre.frappe-surface-programs.v1\",\"app\":");
    json_string(file, pack && pack->schema ? pack->schema->app_name : "");
    fprintf(file, ",\"programs\":[");
    for (size_t i = 0; pack && pack->schema && i < pack->schema->doctype_count; ++i) {
        const BfDocType *doctype = pack->schema->doctypes[i];
        size_t rendered = 0, withheld = 0, needs_list = 0, needs_space = 0, collapsibles = 0;
        int in_tab = 0, in_section = 0, in_column = 0, first_field = 1;
        char program_id[768];
        if (!doctype) continue;
        if (i) fputc(',', file);
        snprintf(program_id, sizeof(program_id), "frappe:%s:%s",
                 pack->schema->app_name ? pack->schema->app_name : "app",
                 doctype->name ? doctype->name : "doctype");
        fprintf(file, "{\"program_id\":"); json_string(file, program_id);
        fprintf(file, ",\"doctype\":"); json_string(file, doctype->name);
        fprintf(file, ",\"module\":"); json_string(file, doctype->module);
        fprintf(file, ",\"title_field\":"); json_string(file, doctype->title_field ? doctype->title_field : "");
        fprintf(file, ",\"description\":"); json_string(file, doctype->description ? doctype->description : "");
        fprintf(file, ",\"record_posture\":{\"submittable\":%s,\"child\":%s,\"single\":%s,\"tree\":%s,\"read_only\":%s,\"guest_readable\":%s}",
                doctype->is_submittable ? "true" : "false", doctype->is_child ? "true" : "false",
                doctype->is_single ? "true" : "false", doctype->is_tree ? "true" : "false",
                doctype->read_only ? "true" : "false",
                doctype->allow_guest_to_view ? "true" : "false");

        fprintf(file, ",\"layout\":[");
        for (size_t j = 0; j < doctype->field_count; ++j) {
            const BfField *field = &doctype->fields[j];
            if (field->fieldtype == BF_TYPE_TAB_BREAK) {
                if (in_column) { fprintf(file, "]}"); in_column = 0; }
                if (in_section) { fprintf(file, "]}"); in_section = 0; }
                if (in_tab) fprintf(file, "]}");
                fprintf(file, "%s{\"tab\":", in_tab ? "," : "");
                json_string(file, field->label ? field->label : "");
                /* crm_lead declares person_tab and details as consecutive tab
                 * breaks with no field between them: the donor's own client
                 * fills them at runtime. Empty here therefore means empty in
                 * the metadata, which a reader is owed rather than a blank. */
                fprintf(file, ",\"empty_in_metadata\":%s,\"sections\":[",
                        (j + 1 < doctype->field_count &&
                         doctype->fields[j + 1].fieldtype != BF_TYPE_TAB_BREAK)
                          ? "false" : "true");
                in_tab = 1; first_field = 1;
                continue;
            }
            if (!in_tab) {
                fprintf(file, "{\"tab\":\"\",\"empty_in_metadata\":false,\"sections\":[");
                in_tab = 1;
            }
            if (field->fieldtype == BF_TYPE_SECTION_BREAK) {
                if (in_column) { fprintf(file, "]}"); in_column = 0; }
                if (in_section) fprintf(file, "]}");
                fprintf(file, "%s{\"section\":", in_section ? "," : "");
                json_string(file, field->label ? field->label : "");
                if (field->collapsible) collapsibles++;
                /* A collapsible section is progressive disclosure, which the
                 * aperture organ already owns on every other surface here. */
                fprintf(file, ",\"collapsible\":%s,\"organ\":\"%s\",\"columns\":[",
                        field->collapsible ? "true" : "false",
                        field->collapsible ? "aperture" : "");
                in_section = 1; in_column = 0; first_field = 1;
                continue;
            }
            if (!in_section) {
                fprintf(file, "{\"section\":\"\",\"collapsible\":false,\"organ\":\"\",\"columns\":[");
                in_section = 1; in_column = 0; first_field = 1;
            }
            if (field->fieldtype == BF_TYPE_COLUMN_BREAK) {
                if (in_column) fprintf(file, "]}");
                fprintf(file, "%s{\"fields\":[", in_column ? "," : "");
                in_column = 1; first_field = 1;
                continue;
            }
            if (!in_column) { fprintf(file, "{\"fields\":["); in_column = 1; first_field = 1; }

            {
                const char *why = field_withheld(field);
                const char *organ = field_organ(field);
                if (!first_field) fputc(',', file);
                first_field = 0;
                fprintf(file, "{\"field\":"); json_string(file, field->fieldname ? field->fieldname : "");
                fprintf(file, ",\"label\":"); json_string(file, field->label ? field->label : "");
                fprintf(file, ",\"presentation\":"); json_string(file, field_presentation(field));
                fprintf(file, ",\"organ\":"); json_string(file, organ);
                fprintf(file, ",\"required\":%s", field->reqd ? "true" : "false");
                fprintf(file, ",\"read_only\":%s", field->read_only ? "true" : "false");
                fprintf(file, ",\"in_list_view\":%s", field->in_list_view ? "true" : "false");
                fprintf(file, ",\"reference\":"); json_string(file, field->options ? field->options : "");
                fprintf(file, ",\"depends_on\":"); json_string(file, field->depends_on_expr ? field->depends_on_expr : "");
                fprintf(file, ",\"withheld\":"); json_string(file, why);
                fputc('}', file);
                if (*why) withheld++; else rendered++;
                if (!strcmp(organ, "list")) needs_list++;
                if (!strcmp(organ, "space")) needs_space++;
            }
        }
        if (in_column) fprintf(file, "]}");
        if (in_section) fprintf(file, "]}");
        if (in_tab) fprintf(file, "]}");
        fprintf(file, "]");

        fprintf(file, ",\"organs\":[");
        {
            int first = 1;
            if (needs_list) { json_string(file, "list"); first = 0; }
            if (needs_space) { if (!first) fputc(',', file); json_string(file, "space"); first = 0; }
            if (collapsibles) { if (!first) fputc(',', file); json_string(file, "aperture"); }
        }
        fprintf(file, "],\"counts\":{\"fields\":%zu,\"rendered\":%zu,\"withheld\":%zu,\"layout\":%zu}",
                doctype->field_count, rendered, withheld, doctype->field_count - rendered - withheld);
        fprintf(file, ",\"cannot_infer\":\"a compiled surface program -> a Frappe runtime, controller, permission, saved document, effect right, or that any record of this DocType exists in this estate. It is the shape a reader would be shown if one did\"}");
    }
    fprintf(file, "]}\n");
    {
        int ok = ferror(file) ? -1 : 0;
        if (file != stdout && fclose(file) != 0) ok = -1;
        if (ok != 0) fprintf(stderr, "Error: failed writing surface programs to %s\n", path);
        return ok;
    }
}

/* The same layout, in a store a reader's request can query.
 *
 * The JSON projection above is the honest record and stays. It is also 1.9 MB
 * for erpnext alone, and a room that had to parse it on every request would be
 * paying for 502 DocTypes to answer about one. So the compiler also writes the
 * programs into SQLite, which is what every other reading surface in this
 * estate already queries.
 *
 * This is the compiler's own output and nothing else: it is not the CMS, which
 * holds what people authored, and a row here is never evidence that a record
 * of that DocType exists anywhere.
 */
/* A DocType is called "CRM Lead", and a subject identifier in this estate holds
 * no spaces. So each program carries the address it can be reached at, derived
 * once here rather than guessed at by every reader. */
static void surface_slug(const char *app, const char *doctype, char *out, size_t cap) {
    size_t w = 0; int dash = 0;
    const char *parts[2]; size_t i;
    parts[0] = app ? app : ""; parts[1] = doctype ? doctype : "";
    for (i = 0; i < 2; i++) {
        const char *in = parts[i];
        if (i && w && w + 1 < cap) { out[w++] = '-'; dash = 1; }
        for (; *in && w + 1 < cap; in++) {
            unsigned char c = (unsigned char)*in;
            if (isalnum(c)) { out[w++] = (char)tolower(c); dash = 0; }
            else if (!dash && w) { out[w++] = '-'; dash = 1; }
        }
    }
    while (w && out[w - 1] == '-') w--;
    out[w] = '\0';
}

static int surface_db_exec(sqlite3 *db, const char *sql) {
    char *why = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &why) != SQLITE_OK) {
        fprintf(stderr, "Error: surface store: %s\n", why ? why : "unknown");
        sqlite3_free(why);
        return -1;
    }
    return 0;
}

static int emit_surface_db(const char *path, const BfAppPack *pack) {
    sqlite3 *db = NULL;
    sqlite3_stmt *ins_program = NULL, *ins_field = NULL, *del_p = NULL, *del_f = NULL;
    const char *app = pack && pack->schema && pack->schema->app_name ? pack->schema->app_name : "app";
    int rc = -1;
    size_t programs = 0, rows = 0;

    if (sqlite3_open(path, &db) != SQLITE_OK) {
        fprintf(stderr, "Error: cannot open surface store %s\n", path);
        if (db) sqlite3_close(db);
        return -1;
    }
    if (surface_db_exec(db,
          "PRAGMA journal_mode=WAL;"
          "CREATE TABLE IF NOT EXISTS frappe_program("
          " program_id TEXT PRIMARY KEY, slug TEXT NOT NULL, app TEXT NOT NULL, doctype TEXT NOT NULL,"
          " module TEXT, title_field TEXT, description TEXT,"
          " submittable INT, child INT, single INT, tree INT, read_only INT, guest_readable INT,"
          " field_count INT, rendered INT, withheld INT, layout INT,"
          " organs TEXT, cannot_infer TEXT);"
          "CREATE TABLE IF NOT EXISTS frappe_field("
          " program_id TEXT NOT NULL, ord INT NOT NULL,"
          " tab TEXT, tab_ord INT, tab_empty INT,"
          " section TEXT, section_ord INT, collapsible INT, column_ord INT,"
          " field TEXT, label TEXT, presentation TEXT, organ TEXT,"
          " required INT, read_only INT, in_list_view INT,"
          " reference TEXT, depends_on TEXT, withheld TEXT,"
          " PRIMARY KEY(program_id, ord));"
          "CREATE INDEX IF NOT EXISTS frappe_program_app ON frappe_program(app);"
          "CREATE UNIQUE INDEX IF NOT EXISTS frappe_program_slug ON frappe_program(slug);"
          "CREATE INDEX IF NOT EXISTS frappe_field_program ON frappe_field(program_id, ord);") != 0)
        goto done;

    /* A recompile replaces this app's programs rather than accumulating them:
     * two versions of one DocType in one store is not a record of either. */
    if (sqlite3_prepare_v2(db, "DELETE FROM frappe_field WHERE program_id IN"
                               " (SELECT program_id FROM frappe_program WHERE app=?1)",
                           -1, &del_f, NULL) != SQLITE_OK) goto done;
    sqlite3_bind_text(del_f, 1, app, -1, SQLITE_STATIC);
    sqlite3_step(del_f);
    if (sqlite3_prepare_v2(db, "DELETE FROM frappe_program WHERE app=?1", -1, &del_p, NULL) != SQLITE_OK) goto done;
    sqlite3_bind_text(del_p, 1, app, -1, SQLITE_STATIC);
    sqlite3_step(del_p);

    if (sqlite3_prepare_v2(db,
          "INSERT INTO frappe_program(program_id,slug,app,doctype,module,title_field,description,"
          " submittable,child,single,tree,read_only,guest_readable,"
          " field_count,rendered,withheld,layout,organs,cannot_infer)"
          " VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19)",
          -1, &ins_program, NULL) != SQLITE_OK) goto done;
    if (sqlite3_prepare_v2(db,
          "INSERT INTO frappe_field(program_id,ord,tab,tab_ord,tab_empty,section,section_ord,"
          " collapsible,column_ord,field,label,presentation,organ,required,read_only,"
          " in_list_view,reference,depends_on,withheld)"
          " VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19)",
          -1, &ins_field, NULL) != SQLITE_OK) goto done;

    if (surface_db_exec(db, "BEGIN") != 0) goto done;
    for (size_t i = 0; pack && pack->schema && i < pack->schema->doctype_count; ++i) {
        const BfDocType *doctype = pack->schema->doctypes[i];
        char program_id[768], organs[64], slug[320];
        const char *tab = "", *section = "";
        int tab_ord = -1, section_ord = -1, column_ord = 0, tab_empty = 0;
        size_t rendered = 0, withheld = 0, laid_out = 0, ord = 0;
        size_t needs_list = 0, needs_space = 0, collapsibles = 0;
        int collapsible = 0;
        if (!doctype || !doctype->name) continue;
        snprintf(program_id, sizeof(program_id), "frappe:%s:%s", app, doctype->name);
        surface_slug(app, doctype->name, slug, sizeof(slug));

        for (size_t j = 0; j < doctype->field_count; ++j) {
            const BfField *field = &doctype->fields[j];
            if (field->fieldtype == BF_TYPE_TAB_BREAK) {
                tab = field->label ? field->label : "";
                tab_ord++;
                tab_empty = !(j + 1 < doctype->field_count &&
                              doctype->fields[j + 1].fieldtype != BF_TYPE_TAB_BREAK);
                section = ""; section_ord = -1; column_ord = 0; collapsible = 0;
                laid_out++;
                /* A tab with no field of its own would otherwise vanish, and a
                 * tab that is missing from a page and a tab that is empty in
                 * the metadata are different facts. One row says which. */
                if (tab_empty) {
                    sqlite3_reset(ins_field);
                    sqlite3_bind_text(ins_field, 1, program_id, -1, SQLITE_TRANSIENT);
                    sqlite3_bind_int64(ins_field, 2, (sqlite3_int64)ord++);
                    sqlite3_bind_text(ins_field, 3, tab, -1, SQLITE_TRANSIENT);
                    sqlite3_bind_int(ins_field, 4, tab_ord);
                    sqlite3_bind_int(ins_field, 5, 1);
                    sqlite3_bind_text(ins_field, 6, "", -1, SQLITE_STATIC);
                    sqlite3_bind_int(ins_field, 7, -1);
                    sqlite3_bind_int(ins_field, 8, 0);
                    sqlite3_bind_int(ins_field, 9, 0);
                    sqlite3_bind_text(ins_field, 10, "", -1, SQLITE_STATIC);
                    sqlite3_bind_text(ins_field, 11, "", -1, SQLITE_STATIC);
                    sqlite3_bind_text(ins_field, 12, "nothing", -1, SQLITE_STATIC);
                    sqlite3_bind_text(ins_field, 13, "", -1, SQLITE_STATIC);
                    sqlite3_bind_int(ins_field, 14, 0);
                    sqlite3_bind_int(ins_field, 15, 0);
                    sqlite3_bind_int(ins_field, 16, 0);
                    sqlite3_bind_text(ins_field, 17, "", -1, SQLITE_STATIC);
                    sqlite3_bind_text(ins_field, 18, "", -1, SQLITE_STATIC);
                    sqlite3_bind_text(ins_field, 19,
                      "this tab holds no field in the DocType's own metadata: the donor "
                      "application fills it from its client at run time, and this estate does not "
                      "run that client", -1, SQLITE_STATIC);
                    if (sqlite3_step(ins_field) != SQLITE_DONE) { surface_db_exec(db, "ROLLBACK"); goto done; }
                    rows++;
                }
                continue;
            }
            if (field->fieldtype == BF_TYPE_SECTION_BREAK) {
                section = field->label ? field->label : "";
                section_ord++; column_ord = 0;
                collapsible = field->collapsible ? 1 : 0;
                if (collapsible) collapsibles++;
                laid_out++;
                continue;
            }
            if (field->fieldtype == BF_TYPE_COLUMN_BREAK) { column_ord++; laid_out++; continue; }
            {
                const char *why = field_withheld(field);
                const char *organ = field_organ(field);
                sqlite3_reset(ins_field);
                sqlite3_bind_text(ins_field, 1, program_id, -1, SQLITE_TRANSIENT);
                sqlite3_bind_int64(ins_field, 2, (sqlite3_int64)ord++);
                sqlite3_bind_text(ins_field, 3, tab, -1, SQLITE_TRANSIENT);
                sqlite3_bind_int(ins_field, 4, tab_ord);
                sqlite3_bind_int(ins_field, 5, tab_empty);
                sqlite3_bind_text(ins_field, 6, section, -1, SQLITE_TRANSIENT);
                sqlite3_bind_int(ins_field, 7, section_ord);
                sqlite3_bind_int(ins_field, 8, collapsible);
                sqlite3_bind_int(ins_field, 9, column_ord);
                sqlite3_bind_text(ins_field, 10, field->fieldname ? field->fieldname : "", -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(ins_field, 11, field->label ? field->label : "", -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(ins_field, 12, field_presentation(field), -1, SQLITE_STATIC);
                sqlite3_bind_text(ins_field, 13, organ, -1, SQLITE_STATIC);
                sqlite3_bind_int(ins_field, 14, field->reqd ? 1 : 0);
                sqlite3_bind_int(ins_field, 15, field->read_only ? 1 : 0);
                sqlite3_bind_int(ins_field, 16, field->in_list_view ? 1 : 0);
                sqlite3_bind_text(ins_field, 17, field->options ? field->options : "", -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(ins_field, 18, field->depends_on_expr ? field->depends_on_expr : "", -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(ins_field, 19, why, -1, SQLITE_STATIC);
                if (sqlite3_step(ins_field) != SQLITE_DONE) { surface_db_exec(db, "ROLLBACK"); goto done; }
                rows++;
                if (*why) withheld++; else rendered++;
                if (!strcmp(organ, "list")) needs_list++;
                if (!strcmp(organ, "space")) needs_space++;
            }
        }

        snprintf(organs, sizeof(organs), "%s%s%s",
                 needs_list ? "list" : "",
                 (needs_list && (needs_space || collapsibles)) ? "," : "",
                 needs_space ? (collapsibles ? "space,aperture" : "space")
                             : (collapsibles ? "aperture" : ""));
        sqlite3_reset(ins_program);
        sqlite3_bind_text(ins_program, 1, program_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(ins_program, 2, slug, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(ins_program, 3, app, -1, SQLITE_STATIC);
        sqlite3_bind_text(ins_program, 4, doctype->name, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(ins_program, 5, doctype->module ? doctype->module : "", -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(ins_program, 6, doctype->title_field ? doctype->title_field : "", -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(ins_program, 7, doctype->description ? doctype->description : "", -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(ins_program, 8, doctype->is_submittable ? 1 : 0);
        sqlite3_bind_int(ins_program, 9, doctype->is_child ? 1 : 0);
        sqlite3_bind_int(ins_program, 10, doctype->is_single ? 1 : 0);
        sqlite3_bind_int(ins_program, 11, doctype->is_tree ? 1 : 0);
        sqlite3_bind_int(ins_program, 12, doctype->read_only ? 1 : 0);
        sqlite3_bind_int(ins_program, 13, doctype->allow_guest_to_view ? 1 : 0);
        sqlite3_bind_int64(ins_program, 14, (sqlite3_int64)doctype->field_count);
        sqlite3_bind_int64(ins_program, 15, (sqlite3_int64)rendered);
        sqlite3_bind_int64(ins_program, 16, (sqlite3_int64)withheld);
        sqlite3_bind_int64(ins_program, 17, (sqlite3_int64)laid_out);
        sqlite3_bind_text(ins_program, 18, organs, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(ins_program, 19,
          "a compiled surface program -> a Frappe runtime, controller, permission, saved document, "
          "effect right, or that any record of this DocType exists in this estate",
          -1, SQLITE_STATIC);
        if (sqlite3_step(ins_program) != SQLITE_DONE) { surface_db_exec(db, "ROLLBACK"); goto done; }
        programs++;
    }
    if (surface_db_exec(db, "COMMIT") != 0) goto done;
    printf("  %s: %zu surface programs, %zu field rows\n", app, programs, rows);
    rc = 0;
done:
    if (ins_program) sqlite3_finalize(ins_program);
    if (ins_field) sqlite3_finalize(ins_field);
    if (del_p) sqlite3_finalize(del_p);
    if (del_f) sqlite3_finalize(del_f);
    if (db) sqlite3_close(db);
    return rc;
}

/* ---- Condition plans ------------------------------------------------------
 * Four donor properties carry a condition. Each distinct expression becomes
 * one plan; every field that declares it is a use bound to that plan, and each
 * field a plan reads becomes an edge from that field to the uses it can
 * change. A declaration outside the supported operations is kept with the
 * operation it is missing, never dropped and never approximated. */

typedef struct {
    const char *doctype;
    const char *field;
    const char *property;
    const char *source;
    char kind[16];
    char said[600];
    size_t plan;          /* index into the plan table when compiled */
    int compiled;
    char form[8];
    char missing[96];
} CondUse;

typedef struct { char operation[96]; size_t declarations; } CondMissing;

static int surface_db_exec(sqlite3 *db, const char *sql);

static const char *const CONDITION_CANNOT_INFER[] = {
    "a plan is the donor's client-side form rule; it is not a validation this estate performs",
    "a compiled plan says the expression is inside the supported operations, not that any surface consumes it",
    "two plans of one shape share syntax; they are not thereby equivalent",
    "a plan that reads the environment is evaluated against what this estate binds, which is its own statement and not the donor client's state",
    "a result marked constant holds for documents that do not make the donor expression throw",
    NULL
};

static void json_string_array(FILE *file, char *const *items, size_t count) {
    fputc('[', file);
    for (size_t i = 0; i < count; ++i) {
        if (i) fputc(',', file);
        json_string(file, items[i]);
    }
    fputc(']', file);
}

/* What this estate binds for the names a donor expression reads from its own
 * client. It runs no donor developer mode, and an application counts as
 * installed when its lineage is registered here. Anything else stays unbound
 * and an expression reading it is refused rather than guessed. */
static char *condition_environment(void) {
    size_t cap = 256 + BF_FRAPPE_LINEAGE_COUNT * 96, len;
    char *env = malloc(cap);
    if (!env) return NULL;
    len = (size_t)snprintf(env, cap, "{\"frappe\":{\"boot\":{\"developer_mode\":0,\"versions\":{");
    for (size_t i = 0; i < BF_FRAPPE_LINEAGE_COUNT && len < cap - 96; ++i) {
        const char *dir = BF_FRAPPE_LINEAGES[i].app_dir;
        int plain = dir && *dir;
        for (const char *c = dir; plain && *c; ++c)
            if (!isalnum((unsigned char)*c) && *c != '_' && *c != '-') plain = 0;
        if (!plain || strlen(dir) > 60) continue;
        len += (size_t)snprintf(env + len, cap - len, "%s\"%s\":\"registered\"",
                                env[len - 1] == '{' ? "" : ",", dir);
    }
    snprintf(env + len, cap - len, "}}}}");
    return env;
}

static const char *condition_label(void *ctx, const char *fieldname, int *is_check) {
    const BfDocType *doctype = ctx;
    for (size_t i = 0; doctype && i < doctype->field_count; ++i) {
        const BfField *field = &doctype->fields[i];
        if (!field->fieldname || strcmp(field->fieldname, fieldname)) continue;
        if (is_check) *is_check = field->fieldtype == BF_TYPE_CHECK;
        return field->label && field->label[0] ? field->label : NULL;
    }
    return NULL;
}

/* How a use is said to a reader. kind is one of: words (said is the condition),
 * always, never, depends (said names the fields), unevaluated (said is empty). */
static void condition_said(const BfDocType *doctype, const BfCondPlan *plan,
                           const char **kind, char *said, size_t cap) {
    size_t len = 0;
    said[0] = '\0';
    if (!plan->compiled) { *kind = "unevaluated"; return; }
    if (!strcmp(plan->constant, "always_true")) { *kind = "always"; return; }
    if (!strcmp(plan->constant, "always_false")) { *kind = "never"; return; }
    if (bf_cond_words(plan, condition_label, (void *)doctype, said, cap) == 0) { *kind = "words"; return; }
    *kind = "depends";
    if (plan->env_read_count && !plan->read_count && !plan->parent_read_count) {
        snprintf(said, cap, "how the donor program itself is set up");
        return;
    }
    for (size_t i = 0; i < plan->read_count && len + 2 < cap; ++i) {
        const char *label = condition_label((void *)doctype, plan->reads[i], NULL);
        len += (size_t)snprintf(said + len, cap - len, "%s%s",
                                !i ? "" : i + 1 == plan->read_count ? " and " : ", ",
                                label ? label : plan->reads[i]);
        if (len >= cap) { len = cap - 1; break; }
    }
    if (!plan->read_count) snprintf(said, cap, "the record it belongs to");
}

/* The same plans, one row per use, beside the programs the composer reads. */
static int emit_condition_db(const char *path, const BfAppPack *pack) {
    static const char *const properties[] = {"visible", "mandatory", "read_only", "collapsed"};
    const char *app = pack && pack->schema && pack->schema->app_name ? pack->schema->app_name : "app";
    sqlite3 *db = NULL;
    sqlite3_stmt *ins = NULL, *del = NULL;
    size_t rows = 0;
    int rc = -1;
    if (sqlite3_open(path, &db) != SQLITE_OK) goto done;
    if (surface_db_exec(db,
          "CREATE TABLE IF NOT EXISTS frappe_condition("
          " program_id TEXT NOT NULL, app TEXT NOT NULL, field TEXT NOT NULL, property TEXT NOT NULL,"
          " status TEXT NOT NULL, plan_id TEXT, plan TEXT, kind TEXT NOT NULL, said TEXT,"
          " reads TEXT, missing_operation TEXT, source TEXT NOT NULL,"
          " PRIMARY KEY(program_id, field, property));"
          "CREATE INDEX IF NOT EXISTS frappe_condition_plan ON frappe_condition(plan_id);") != 0) goto done;
    if (sqlite3_prepare_v2(db, "DELETE FROM frappe_condition WHERE app=?1", -1, &del, NULL) != SQLITE_OK) goto done;
    sqlite3_bind_text(del, 1, app, -1, SQLITE_STATIC);
    sqlite3_step(del);
    if (sqlite3_prepare_v2(db,
          "INSERT OR REPLACE INTO frappe_condition(program_id,app,field,property,status,plan_id,plan,"
          " kind,said,reads,missing_operation,source) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
          -1, &ins, NULL) != SQLITE_OK) goto done;
    if (surface_db_exec(db, "BEGIN") != 0) goto done;
    for (size_t i = 0; pack && pack->schema && i < pack->schema->doctype_count; ++i) {
        const BfDocType *doctype = pack->schema->doctypes[i];
        char program_id[768];
        if (!doctype || !doctype->name) continue;
        snprintf(program_id, sizeof(program_id), "frappe:%s:%s", app, doctype->name);
        for (size_t j = 0; j < doctype->field_count; ++j) {
            const BfField *field = &doctype->fields[j];
            const char *sources[4];
            sources[0] = field->depends_on_expr;
            sources[1] = field->mandatory_depends_on_expr;
            sources[2] = field->read_only_depends_on_expr;
            sources[3] = field->collapsible_depends_on_expr;
            for (size_t k = 0; k < 4; ++k) {
                BfCondPlan plan;
                const char *kind = "";
                char said[600], reads[512];
                size_t len = 0;
                if (!sources[k] || !sources[k][0] || !field->fieldname) continue;
                bf_cond_compile(sources[k], &plan);
                condition_said(doctype, &plan, &kind, said, sizeof(said));
                reads[0] = '\0';
                for (size_t r = 0; r < plan.read_count && len + 2 < sizeof(reads); ++r) {
                    len += (size_t)snprintf(reads + len, sizeof(reads) - len, "%s%s", r ? "|" : "", plan.reads[r]);
                    if (len >= sizeof(reads)) break;
                }
                sqlite3_reset(ins);
                sqlite3_bind_text(ins, 1, program_id, -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(ins, 2, app, -1, SQLITE_STATIC);
                sqlite3_bind_text(ins, 3, field->fieldname, -1, SQLITE_STATIC);
                sqlite3_bind_text(ins, 4, properties[k], -1, SQLITE_STATIC);
                sqlite3_bind_text(ins, 5, plan.compiled ? "compiled" : "unsupported", -1, SQLITE_STATIC);
                sqlite3_bind_text(ins, 6, plan.plan_id, -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(ins, 7, plan.plan ? plan.plan : "", -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(ins, 8, kind, -1, SQLITE_STATIC);
                sqlite3_bind_text(ins, 9, said, -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(ins, 10, reads, -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(ins, 11, plan.missing_operation, -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(ins, 12, sources[k], -1, SQLITE_STATIC);
                bf_cond_plan_free(&plan);
                if (sqlite3_step(ins) != SQLITE_DONE) { surface_db_exec(db, "ROLLBACK"); goto done; }
                rows++;
            }
        }
    }
    if (surface_db_exec(db, "COMMIT") != 0) goto done;
    printf("  %s: %zu condition rows\n", app, rows);
    rc = 0;
done:
    if (rc != 0) fprintf(stderr, "Error: cannot write conditions into %s\n", path);
    if (ins) sqlite3_finalize(ins);
    if (del) sqlite3_finalize(del);
    if (db) sqlite3_close(db);
    return rc;
}

static int emit_condition_plans(const char *path, const BfAppPack *pack) {
    static const char *const properties[] = {"visible", "mandatory", "read_only", "collapsed"};
    static const char *const donor_keys[] = {
        "depends_on", "mandatory_depends_on", "read_only_depends_on", "collapsible_depends_on"
    };
    CondUse *uses = NULL;
    BfCondPlan *plans = NULL;
    size_t *plan_uses = NULL;
    CondMissing *missing = NULL;
    char (*shapes)[40] = NULL;
    size_t use_count = 0, use_cap = 0, plan_count = 0, missing_count = 0, shape_count = 0;
    size_t compiled = 0, constant = 0, by_property[4] = {0}, edges = 0;
    FILE *file = NULL;
    int rc = -1;

    for (size_t i = 0; pack && pack->schema && i < pack->schema->doctype_count; ++i) {
        const BfDocType *doctype = pack->schema->doctypes[i];
        for (size_t j = 0; doctype && j < doctype->field_count; ++j) {
            const BfField *field = &doctype->fields[j];
            const char *sources[4];
            sources[0] = field->depends_on_expr;
            sources[1] = field->mandatory_depends_on_expr;
            sources[2] = field->read_only_depends_on_expr;
            sources[3] = field->collapsible_depends_on_expr;
            for (size_t k = 0; k < 4; ++k) {
                BfCondPlan plan;
                CondUse *use;
                if (!sources[k] || !sources[k][0]) continue;
                if (use_count == use_cap) {
                    size_t cap = use_cap ? use_cap * 2 : 256;
                    CondUse *next = realloc(uses, sizeof(*uses) * cap);
                    if (!next) goto done;
                    uses = next;
                    use_cap = cap;
                }
                use = &uses[use_count++];
                memset(use, 0, sizeof(*use));
                use->doctype = doctype->name ? doctype->name : "";
                use->field = field->fieldname ? field->fieldname : "";
                use->property = properties[k];
                use->source = sources[k];
                by_property[k]++;
                bf_cond_compile(sources[k], &plan);
                snprintf(use->form, sizeof(use->form), "%s", plan.form);
                {
                    const char *kind = "";
                    condition_said(doctype, &plan, &kind, use->said, sizeof(use->said));
                    snprintf(use->kind, sizeof(use->kind), "%s", kind);
                }
                if (!plan.compiled) {
                    size_t m = 0;
                    snprintf(use->missing, sizeof(use->missing), "%s", plan.missing_operation);
                    while (m < missing_count && strcmp(missing[m].operation, use->missing)) ++m;
                    if (m == missing_count) {
                        CondMissing *next = realloc(missing, sizeof(*missing) * (missing_count + 1));
                        if (!next) { bf_cond_plan_free(&plan); goto done; }
                        missing = next;
                        memset(&missing[missing_count], 0, sizeof(*missing));
                        snprintf(missing[missing_count].operation, sizeof(missing->operation), "%s", use->missing);
                        missing_count++;
                    }
                    missing[m].declarations++;
                    bf_cond_plan_free(&plan);
                    continue;
                }
                use->compiled = 1;
                compiled++;
                {
                    size_t p = 0;
                    while (p < plan_count && strcmp(plans[p].plan, plan.plan)) ++p;
                    if (p == plan_count) {
                        BfCondPlan *next = realloc(plans, sizeof(*plans) * (plan_count + 1));
                        size_t *next_uses = next ? realloc(plan_uses, sizeof(*plan_uses) * (plan_count + 1)) : NULL;
                        size_t s = 0;
                        if (next) plans = next;
                        if (!next_uses) { bf_cond_plan_free(&plan); goto done; }
                        plan_uses = next_uses;
                        plans[plan_count] = plan;
                        plan_uses[plan_count] = 0;
                        plan_count++;
                        if (plan.constant[0]) constant++;
                        while (s < shape_count && strcmp(shapes[s], plan.shape_id)) ++s;
                        if (s == shape_count) {
                            char (*next_shapes)[40] = realloc(shapes, sizeof(*shapes) * (shape_count + 1));
                            if (!next_shapes) goto done;
                            shapes = next_shapes;
                            memcpy(shapes[shape_count++], plan.shape_id, sizeof(plan.shape_id));
                        }
                    } else {
                        bf_cond_plan_free(&plan);
                    }
                    use->plan = p;
                    plan_uses[p]++;
                    edges += plans[p].read_count;
                }
            }
        }
    }

    file = !strcmp(path, "-") ? stdout : fopen(path, "wb");
    if (!file) {
        fprintf(stderr, "Error: cannot emit condition plans to %s\n", path);
        goto done;
    }
    fprintf(file, "{\"schema\":\"bonfyre.frappe-condition-plans.v1\",\"app\":");
    json_string(file, pack && pack->schema && pack->schema->app_name ? pack->schema->app_name : "");
    fprintf(file, ",\"counts\":{\"declarations\":%zu,\"compiled\":%zu,\"unsupported\":%zu,"
                  "\"plans\":%zu,\"shapes\":%zu,\"constant_plans\":%zu,\"read_edges\":%zu,"
                  "\"visible\":%zu,\"mandatory\":%zu,\"read_only\":%zu,\"collapsed\":%zu}",
            use_count, compiled, use_count - compiled, plan_count, shape_count, constant, edges,
            by_property[0], by_property[1], by_property[2], by_property[3]);
    fprintf(file, ",\"donor_properties\":{");
    for (size_t k = 0; k < 4; ++k)
        fprintf(file, "%s\"%s\":\"%s\"", k ? "," : "", properties[k], donor_keys[k]);
    fprintf(file, "},\"plans\":[");
    for (size_t p = 0; p < plan_count; ++p) {
        if (p) fputc(',', file);
        fprintf(file, "{\"plan_id\":\"%s\",\"shape_id\":\"%s\",\"form\":\"%s\",\"uses\":%zu,\"plan\":",
                plans[p].plan_id, plans[p].shape_id, plans[p].form, plan_uses[p]);
        json_string(file, plans[p].plan);
        fprintf(file, ",\"reads\":"); json_string_array(file, plans[p].reads, plans[p].read_count);
        fprintf(file, ",\"parent_reads\":"); json_string_array(file, plans[p].parent_reads, plans[p].parent_read_count);
        fprintf(file, ",\"env_reads\":"); json_string_array(file, plans[p].env_reads, plans[p].env_read_count);
        fprintf(file, ",\"constant\":\"%s\",\"diagnostic\":", plans[p].constant);
        json_string(file, plans[p].diagnostic);
        fputc('}', file);
    }
    fprintf(file, "],\"uses\":[");
    for (size_t u = 0; u < use_count; ++u) {
        if (u) fputc(',', file);
        fprintf(file, "{\"doctype\":"); json_string(file, uses[u].doctype);
        fprintf(file, ",\"field\":"); json_string(file, uses[u].field);
        fprintf(file, ",\"property\":\"%s\",\"form\":\"%s\",\"source\":", uses[u].property, uses[u].form);
        json_string(file, uses[u].source);
        fprintf(file, ",\"status\":\"%s\",\"plan_id\":\"%s\",\"missing_operation\":",
                uses[u].compiled ? "compiled" : "unsupported",
                uses[u].compiled ? plans[uses[u].plan].plan_id : "");
        json_string(file, uses[u].missing);
        fprintf(file, ",\"kind\":\"%s\",\"said\":", uses[u].kind);
        json_string(file, uses[u].said);
        fputc('}', file);
    }
    /* The reverse index: a change to `read` on a document of `doctype` can
     * change exactly these properties, and no others. */
    fprintf(file, "],\"read_edges\":[");
    {
        int first = 1;
        for (size_t u = 0; u < use_count; ++u) {
            const BfCondPlan *plan;
            if (!uses[u].compiled) continue;
            plan = &plans[uses[u].plan];
            for (size_t r = 0; r < plan->read_count; ++r) {
                if (!first) fputc(',', file);
                first = 0;
                fprintf(file, "{\"doctype\":"); json_string(file, uses[u].doctype);
                fprintf(file, ",\"read\":"); json_string(file, plan->reads[r]);
                fprintf(file, ",\"plan_id\":\"%s\",\"field\":", plan->plan_id);
                json_string(file, uses[u].field);
                fprintf(file, ",\"property\":\"%s\"}", uses[u].property);
            }
        }
    }
    fprintf(file, "],\"missing_operations\":[");
    for (size_t m = 0; m < missing_count; ++m) {
        if (m) fputc(',', file);
        fprintf(file, "{\"operation\":"); json_string(file, missing[m].operation);
        fprintf(file, ",\"declarations\":%zu}", missing[m].declarations);
    }
    fprintf(file, "],\"cannot_infer\":[");
    for (size_t i = 0; CONDITION_CANNOT_INFER[i]; ++i) {
        if (i) fputc(',', file);
        json_string(file, CONDITION_CANNOT_INFER[i]);
    }
    {
        char *env = condition_environment();
        fprintf(file, "],\"environment\":%s}\n", env ? env : "null");
        free(env);
    }
    if (file != stdout) {
        printf("Condition plans: %zu declarations, %zu compiled, %zu unsupported, %zu plans, %zu shapes\n",
               use_count, compiled, use_count - compiled, plan_count, shape_count);
    }
    rc = ferror(file) ? -1 : 0;
done:
    if (file && file != stdout) fclose(file);
    for (size_t p = 0; p < plan_count; ++p) bf_cond_plan_free(&plans[p]);
    free(plans); free(plan_uses); free(uses); free(missing); free(shapes);
    return rc;
}

/* One expression against one document, with no application compiled: the
 * operation a surface or a test asks when a field changes. */
static int eval_condition(const char *source, const char *doc_json, const char *parent_json, const char *env_json) {
    BfCondPlan plan;
    sqlite3 *db = NULL;
    char error[200] = "";
    int result = -1, rc = 0;
    char *estate_env = env_json ? NULL : condition_environment();
    if (!env_json) env_json = estate_env;
    bf_cond_compile(source, &plan);
    printf("{\"source\":"); json_string(stdout, source);
    printf(",\"form\":\"%s\",\"status\":\"%s\",\"plan_id\":\"%s\",\"plan\":", plan.form,
           plan.compiled ? "compiled" : "unsupported", plan.plan_id);
    json_string(stdout, plan.plan ? plan.plan : "");
    printf(",\"reads\":"); json_string_array(stdout, plan.reads, plan.read_count);
    printf(",\"parent_reads\":"); json_string_array(stdout, plan.parent_reads, plan.parent_read_count);
    printf(",\"env_reads\":"); json_string_array(stdout, plan.env_reads, plan.env_read_count);
    printf(",\"constant\":\"%s\",\"missing_operation\":", plan.constant);
    json_string(stdout, plan.missing_operation);
    if (!plan.compiled) rc = 2;
    else if (doc_json) {
        if (sqlite3_open(":memory:", &db) == SQLITE_OK)
            result = bf_cond_evaluate(&plan, db, doc_json, parent_json, env_json, error, sizeof(error));
        else snprintf(error, sizeof(error), "cannot open evaluation store");
        if (db) sqlite3_close(db);
        if (result < 0) rc = 3;
        printf(",\"result\":%s,\"error\":", result < 0 ? "null" : result ? "true" : "false");
        json_string(stdout, error);
    }
    printf("}\n");
    bf_cond_plan_free(&plan);
    free(estate_env);
    return rc;
}

/* ---- Notifications --------------------------------------------------------
 * A donor Notification is a definition: when something happens to a record of
 * one kind, and a condition holds, compose a message for some recipients. It
 * is bound here to the two owners that already exist -- the condition plan
 * decides, and what comes out is a held message: composed, addressed where an
 * address can be read off the record, and transmitted by nobody. Reachability
 * stops at the human line, so there is no send to bind to. */

typedef struct { char **paths; size_t count; } NotificationFiles;

static int json_text_is_notification(sqlite3 *db, const char *json) {
    sqlite3_stmt *stmt = NULL;
    int yes = 0;
    if (sqlite3_prepare_v2(db, "SELECT json_valid(?1) AND json_extract(?1,'$.doctype')='Notification'"
                               " AND json_type(?1,'$.document_type')='text'", -1, &stmt, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(stmt, 1, json, -1, SQLITE_STATIC);
    if (sqlite3_step(stmt) == SQLITE_ROW) yes = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
    return yes;
}

static char *read_small_file(const char *path) {
    FILE *file = fopen(path, "rb");
    char *text = NULL;
    long size;
    if (!file) return NULL;
    if (fseek(file, 0, SEEK_END) == 0 && (size = ftell(file)) > 0 && size < 2 * 1024 * 1024 &&
        fseek(file, 0, SEEK_SET) == 0 && (text = malloc((size_t)size + 1))) {
        if (fread(text, 1, (size_t)size, file) != (size_t)size) { free(text); text = NULL; }
        else text[size] = '\0';
    }
    fclose(file);
    return text;
}

/* A definition handed over on standard input rather than named as a file. */
static char *read_small_stream(FILE *file) {
    size_t cap = 65536, used = 0, got;
    char *text = malloc(cap);
    while (text && (got = fread(text + used, 1, cap - used - 1, file)) > 0) {
        used += got;
        if (used + 1 >= cap) {
            char *more = cap < 2 * 1024 * 1024 ? realloc(text, cap * 2) : NULL;
            if (!more) { free(text); return NULL; }
            text = more; cap *= 2;
        }
    }
    if (text) text[used] = '\0';
    if (text && !used) { free(text); text = NULL; }
    return text;
}

static void notification_walk(sqlite3 *db, const char *directory, NotificationFiles *out, int depth) {
    DIR *dir = depth <= 24 ? opendir(directory) : NULL;
    struct dirent *entry;
    if (!dir) return;
    while ((entry = readdir(dir)) != NULL) {
        char path[PATH_MAX];
        struct stat st;
        size_t len = strlen(entry->d_name);
        if (entry->d_name[0] == '.' || !strcmp(entry->d_name, "node_modules")) continue;
        if (snprintf(path, sizeof(path), "%s/%s", directory, entry->d_name) >= (int)sizeof(path)) continue;
        if (lstat(path, &st) != 0 || S_ISLNK(st.st_mode)) continue;
        if (S_ISDIR(st.st_mode)) { notification_walk(db, path, out, depth + 1); continue; }
        if (!S_ISREG(st.st_mode) || len < 6 || strcmp(entry->d_name + len - 5, ".json")) continue;
        {
            char *json = read_small_file(path);
            if (json && json_text_is_notification(db, json)) {
                char **next = realloc(out->paths, sizeof(char *) * (out->count + 1));
                if (next) {
                    out->paths = next;
                    if ((out->paths[out->count] = strdup(path))) out->count++;
                }
            }
            free(json);
        }
    }
    closedir(dir);
}

static int path_order(const void *a, const void *b) {
    return strcmp(*(char *const *)a, *(char *const *)b);
}

/* One text value out of a JSON document; the caller frees it. */
static char *json_field(sqlite3 *db, const char *json, const char *path) {
    sqlite3_stmt *stmt = NULL;
    char *out = NULL;
    if (sqlite3_prepare_v2(db, "SELECT json_extract(?1, ?2)", -1, &stmt, NULL) != SQLITE_OK) return NULL;
    sqlite3_bind_text(stmt, 1, json, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, path, -1, SQLITE_STATIC);
    if (sqlite3_step(stmt) == SQLITE_ROW && sqlite3_column_type(stmt, 0) != SQLITE_NULL)
        out = strdup((const char *)sqlite3_column_text(stmt, 0));
    sqlite3_finalize(stmt);
    return out;
}

static char *doc_field(sqlite3 *db, const char *doc, const char *name) {
    char path[160];
    if (!doc || !name || strchr(name, '"') || strchr(name, '\\') || strlen(name) > 120) return NULL;
    snprintf(path, sizeof(path), "$.\"%s\"", name);
    return json_field(db, doc, path);
}

/* The donor's templates are Jinja, rendered with undefined names left as
 * written. What is run here: printing an expression, if / elif / else, a loop
 * over a list or a range, and set -- each over the server dialect the
 * condition plans already compile. An expression outside that dialect names
 * the operation it is missing, and then no text is claimed at all. With no
 * document this only analyses. Returns 0 when the whole template is supported. */

typedef struct { int kind; char *text; } TplToken;   /* 0 text, 1 expression, 2 statement */

typedef struct {
    sqlite3 *db;
    const char *doc;
    TplToken *tokens;
    size_t count;
    FILE *out;
    char *missing;          /* char[96] */
    char *undefined;        /* char[256], names the donor would print verbatim; may be NULL */
    char statement[240];    /* the statement a block stopped at */
} Tpl;

static void tpl_fail(Tpl *t, const char *fmt, const char *detail) {
    if (!t->missing[0]) snprintf(t->missing, 96, fmt, detail);
}

static int tpl_tokenize(Tpl *t, const char *source) {
    const char *c = source;
    int trim_next = 0;
    while (*c) {
        const char *open = c, *close;
        const char *end_mark;
        size_t len;
        TplToken *next;
        char *text;
        int kind = 0, trim_before = 0;
        while (*open && !(open[0] == '{' && (open[1] == '{' || open[1] == '%' || open[1] == '#'))) ++open;
        len = (size_t)(open - c);
        if (*open && open[2] == '-') trim_before = 1;
        if (len || !*open) {
            const char *start = c;
            if (trim_next) while (len && isspace((unsigned char)*start)) { ++start; --len; }
            if (trim_before) while (len && isspace((unsigned char)start[len - 1])) --len;
            if (len) {
                if (!(next = realloc(t->tokens, sizeof(*next) * (t->count + 1))) ) return -1;
                t->tokens = next;
                if (!(text = strndup(start, len))) return -1;
                t->tokens[t->count].kind = 0;
                t->tokens[t->count++].text = text;
            }
        }
        if (!*open) break;
        end_mark = open[1] == '{' ? "}}" : open[1] == '%' ? "%}" : "#}";
        close = strstr(open + 2, end_mark);
        if (!close) { tpl_fail(t, "jinja:%s", "unterminated"); return -1; }
        kind = open[1] == '{' ? 1 : open[1] == '%' ? 2 : -1;
        trim_next = close > open + 2 && close[-1] == '-';
        if (kind > 0) {
            const char *start = open + 2 + trim_before;
            const char *stop = close - trim_next;
            while (start < stop && isspace((unsigned char)*start)) ++start;
            while (stop > start && isspace((unsigned char)stop[-1])) --stop;
            if (!(next = realloc(t->tokens, sizeof(*next) * (t->count + 1)))) return -1;
            t->tokens = next;
            if (!(text = strndup(start, (size_t)(stop - start)))) return -1;
            t->tokens[t->count].kind = kind;
            t->tokens[t->count++].text = text;
        }
        c = close + 2;
    }
    /* Jinja drops one trailing newline of the template. */
    if (t->count && t->tokens[t->count - 1].kind == 0) {
        char *last = t->tokens[t->count - 1].text;
        size_t len = strlen(last);
        if (len && last[len - 1] == '\n') last[len - 1] = '\0';
    }
    return 0;
}

static char *tpl_env_set(sqlite3 *db, const char *env, const char *name, const char *value_json) {
    sqlite3_stmt *stmt = NULL;
    char *out = NULL;
    if (sqlite3_prepare_v2(db, "SELECT json_set(?1, '$.' || ?2, json(?3))", -1, &stmt, NULL) != SQLITE_OK) return NULL;
    sqlite3_bind_text(stmt, 1, env, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, name, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, value_json ? value_json : "null", -1, SQLITE_STATIC);
    if (sqlite3_step(stmt) == SQLITE_ROW && sqlite3_column_text(stmt, 0)) out = strdup((const char *)sqlite3_column_text(stmt, 0));
    sqlite3_finalize(stmt);
    return out;
}

/* Compiles an expression, and evaluates it when the block is live. Returns the
 * value's truth (0 when not live), or -1 once something is named as missing. */
static int tpl_expression(Tpl *t, const char *source, const char *env, int live, char **json, char **text, int *unbound) {
    BfCondPlan plan;
    char error[200];
    int truth = 0;
    if (unbound) *unbound = 0;
    if (bf_cond_compile_python(source, &plan) != 0) {
        tpl_fail(t, "%s", plan.missing_operation);
        bf_cond_plan_free(&plan);
        return -1;
    }
    if (live && t->doc) {
        truth = bf_cond_evaluate_value(&plan, t->db, t->doc, NULL, env, json, text, error, sizeof(error));
        if (truth < 0) {
            if (unbound && (!strncmp(error, "environment value is not bound", 30) || !strncmp(error, "environment is not bound", 24))) {
                *unbound = 1;
                truth = 0;
            } else tpl_fail(t, "evaluation:%.80s", error);
        }
    }
    bf_cond_plan_free(&plan);
    return truth;
}

static int is_name(const char *text, size_t len) {
    if (!len || !(isalpha((unsigned char)text[0]) || text[0] == '_')) return 0;
    for (size_t i = 0; i < len; ++i) if (!isalnum((unsigned char)text[i]) && text[i] != '_') return 0;
    return 1;
}

/* Runs tokens from *at until a statement named in stops (space-separated) or
 * the end. Returns 1 when stopped by one (left in t->statement), 0 at the end,
 * -1 on anything unsupported. *env may be replaced by set. */
static int tpl_block(Tpl *t, size_t *at, char **env, int live, const char *stops) {
    while (*at < t->count && !t->missing[0]) {
        TplToken *token = &t->tokens[(*at)++];
        char word[24];
        size_t wl;
        if (token->kind == 0) { if (live && t->out) fputs(token->text, t->out); continue; }
        if (token->kind == 1) {
            char *text = NULL;
            int unbound = 0;
            if (tpl_expression(t, token->text, *env, live, NULL, &text, &unbound) < 0) { free(text); return -1; }
            if (live && t->out) {
                if (unbound) {
                    /* The donor prints an undefined name as it was written. */
                    fprintf(t->out, "{{ %s }}", token->text);
                    if (t->undefined && strlen(t->undefined) + strlen(token->text) + 3 < 256 && !strstr(t->undefined, token->text)) {
                        if (t->undefined[0]) strcat(t->undefined, ", ");
                        strcat(t->undefined, token->text);
                    }
                } else if (text) fputs(text, t->out);
            }
            free(text);
            continue;
        }
        wl = strcspn(token->text, " \t\n");
        snprintf(word, sizeof(word), "%.*s", (int)(wl < 23 ? wl : 23), token->text);
        {
            const char *found = strstr(stops, word);
            if (found && (found == stops || found[-1] == ' ') && (found[wl] == ' ' || !found[wl])) {
                snprintf(t->statement, sizeof(t->statement), "%s", token->text);
                return 1;
            }
        }
        if (!strcmp(word, "if")) {
            int taken = 0, holds = tpl_expression(t, token->text + 2, *env, live, NULL, NULL, NULL);
            if (holds < 0) return -1;
            for (;;) {
                int stopped = tpl_block(t, at, env, live && holds && !taken, "elif else endif");
                if (stopped != 1) { tpl_fail(t, "jinja:%s", "unclosed_if"); return -1; }
                if (holds) taken = 1;
                if (!strncmp(t->statement, "endif", 5)) break;
                if (!strncmp(t->statement, "elif", 4)) {
                    char condition[240];
                    snprintf(condition, sizeof(condition), "%s", t->statement + 4);
                    holds = tpl_expression(t, condition, *env, live && !taken, NULL, NULL, NULL);
                    if (holds < 0) return -1;
                } else holds = !taken;
            }
        } else if (!strcmp(word, "for")) {
            const char *name = token->text + 3, *in, *list_source;
            char variable[64], *list = NULL, *saved;
            size_t name_len, body = *at;
            long items = 0;
            while (isspace((unsigned char)*name)) ++name;
            name_len = strcspn(name, " \t\n");
            in = name + name_len;
            while (isspace((unsigned char)*in)) ++in;
            if (!is_name(name, name_len) || name_len >= sizeof(variable) || strncmp(in, "in", 2) || !isspace((unsigned char)in[2])) {
                tpl_fail(t, "jinja:%s", "for_form");
                return -1;
            }
            snprintf(variable, sizeof(variable), "%.*s", (int)name_len, name);
            list_source = in + 3;
            if (tpl_expression(t, list_source, *env, live, &list, NULL, NULL) < 0) { free(list); return -1; }
            if (live && t->doc) {
                sqlite3_stmt *stmt = NULL;
                if (!list || sqlite3_prepare_v2(t->db, "SELECT CASE WHEN json_type(?1)='array' THEN json_array_length(?1) ELSE -1 END", -1, &stmt, NULL) != SQLITE_OK) items = -1;
                else {
                    sqlite3_bind_text(stmt, 1, list, -1, SQLITE_STATIC);
                    items = sqlite3_step(stmt) == SQLITE_ROW ? (long)sqlite3_column_int64(stmt, 0) : -1;
                }
                if (stmt) sqlite3_finalize(stmt);
                /* An absent list is a record with no rows of that kind. */
                if (items < 0 && list && !strcmp(list, "null")) items = 0;
                if (items < 0) { free(list); tpl_fail(t, "jinja:%s", "for_over_non_list"); return -1; }
            }
            saved = *env;
            for (long k = 0; k < (items > 0 ? items : 1); ++k) {
                char *scoped = NULL;
                int stopped;
                *at = body;
                if (items > 0) {
                    char path[40], *element;
                    snprintf(path, sizeof(path), "$[%ld]", k);
                    element = json_field(t->db, list, path);
                    /* json_extract gives text bare; ask for it quoted instead. */
                    {
                        sqlite3_stmt *stmt = NULL;
                        free(element);
                        element = NULL;
                        if (sqlite3_prepare_v2(t->db, "SELECT json_quote(json_extract(?1, ?2))", -1, &stmt, NULL) == SQLITE_OK) {
                            sqlite3_bind_text(stmt, 1, list, -1, SQLITE_STATIC);
                            sqlite3_bind_text(stmt, 2, path, -1, SQLITE_STATIC);
                            if (sqlite3_step(stmt) == SQLITE_ROW && sqlite3_column_text(stmt, 0))
                                element = strdup((const char *)sqlite3_column_text(stmt, 0));
                            sqlite3_finalize(stmt);
                        }
                    }
                    scoped = tpl_env_set(t->db, saved, variable, element);
                    free(element);
                    if (!scoped) { free(list); tpl_fail(t, "jinja:%s", "loop_variable"); return -1; }
                    *env = scoped;
                }
                stopped = tpl_block(t, at, env, live && items > 0, "endfor");
                if (*env != saved) { free(*env); *env = saved; }
                if (stopped != 1) { free(list); tpl_fail(t, "jinja:%s", "unclosed_for"); return -1; }
            }
            free(list);
        } else if (!strcmp(word, "set")) {
            const char *name = token->text + 3, *equals;
            char variable[64], *value = NULL, *next;
            size_t name_len;
            while (isspace((unsigned char)*name)) ++name;
            name_len = strcspn(name, " \t\n=");
            equals = name + name_len;
            while (isspace((unsigned char)*equals)) ++equals;
            if (!is_name(name, name_len) || name_len >= sizeof(variable) || *equals != '=' || equals[1] == '=') {
                tpl_fail(t, "jinja:%s", "set_form");
                return -1;
            }
            snprintf(variable, sizeof(variable), "%.*s", (int)name_len, name);
            if (tpl_expression(t, equals + 1, *env, live, &value, NULL, NULL) < 0) { free(value); return -1; }
            if (live && t->doc) {
                next = tpl_env_set(t->db, *env, variable, value);
                if (next) { free(*env); *env = next; }
            }
            free(value);
        } else {
            tpl_fail(t, "jinja:%s", word);
            return -1;
        }
    }
    return t->missing[0] ? -1 : 0;
}

static int template_render(sqlite3 *db, const char *tpl, const char *doc, FILE *out, char missing[96], char *undefined) {
    Tpl t;
    char *env = strdup("{}");
    size_t at = 0;
    int rc = -1;
    memset(&t, 0, sizeof(t));
    t.db = db; t.doc = doc; t.out = out; t.missing = missing; t.undefined = undefined;
    missing[0] = '\0';
    if (undefined) undefined[0] = '\0';
    if (env && tpl_tokenize(&t, tpl ? tpl : "") == 0) {
        int stopped = tpl_block(&t, &at, &env, doc != NULL, "");
        if (stopped == 1) tpl_fail(&t, "jinja:%s", "unopened_block");
        rc = missing[0] ? -1 : 0;
    } else if (!missing[0]) snprintf(missing, 96, "out_of_memory");
    for (size_t i = 0; i < t.count; ++i) free(t.tokens[i].text);
    free(t.tokens);
    free(env);
    return rc;
}

/* A standard notification's body is the file beside its definition, chosen by
 * message_type; only a non-standard one carries it in the definition. */
static char *notification_template(sqlite3 *db, const char *json, const char *definition_path,
                                   const char **format, char missing[96]) {
    char *standard = json_field(db, json, "$.is_standard");
    char *type = json_field(db, json, "$.message_type");
    char path[PATH_MAX], *text = NULL;
    const char *extension = ".md";
    size_t len = strlen(definition_path);
    missing[0] = '\0';
    *format = "markdown";
    if (type && !strcmp(type, "HTML")) { extension = ".html"; *format = "html"; }
    else if (type && !strcmp(type, "Plain Text")) { extension = ".txt"; *format = "text"; }
    if (standard && atoi(standard)) {
        if (len > 5 && len + 8 < sizeof(path)) {
            snprintf(path, sizeof(path), "%.*s%s", (int)(len - 5), definition_path, extension);
            text = read_small_file(path);
        }
        if (!text || !text[0]) { free(text); text = NULL; snprintf(missing, 96, "template_file_absent"); }
    } else text = json_field(db, json, "$.message");
    free(standard); free(type);
    return text;
}

/* record: New/Save/Submit/Cancel happen to a record. change: one field took a
 * new value. date: a date on the record is N days away. Anything else is a
 * controller hook this estate does not run. */
static const char *trigger_kind(const char *event) {
    if (!event) return "";
    if (!strcmp(event, "New") || !strcmp(event, "Save") || !strcmp(event, "Submit") || !strcmp(event, "Cancel"))
        return "record";
    if (!strcmp(event, "Value Change")) return "change";
    if (!strcmp(event, "Days Before") || !strcmp(event, "Days After")) return "date";
    return "";
}

static void emit_condition_object(FILE *file, const char *source, BfCondPlan *plan) {
    fprintf(file, "{\"source\":"); json_string(file, source ? source : "");
    if (!source || !source[0]) { fprintf(file, ",\"status\":\"none\"}"); memset(plan, 0, sizeof(*plan)); return; }
    bf_cond_compile_python(source, plan);
    fprintf(file, ",\"status\":\"%s\",\"plan_id\":\"%s\",\"plan\":", plan->compiled ? "compiled" : "unsupported", plan->plan_id);
    json_string(file, plan->plan ? plan->plan : "");
    fprintf(file, ",\"reads\":"); json_string_array(file, plan->reads, plan->read_count);
    fprintf(file, ",\"missing_operation\":"); json_string(file, plan->missing_operation);
    fputc('}', file);
}

static void count_missing(CondMissing **list, size_t *count, const char *operation) {
    size_t m = 0;
    CondMissing *next;
    if (!operation || !operation[0]) return;
    while (m < *count && strcmp((*list)[m].operation, operation)) ++m;
    if (m == *count) {
        next = realloc(*list, sizeof(**list) * (*count + 1));
        if (!next) return;
        *list = next;
        memset(&next[*count], 0, sizeof(*next));
        snprintf(next[*count].operation, sizeof(next->operation), "%s", operation);
        (*count)++;
    }
    (*list)[m].declarations++;
}

static int emit_notification_plans(const char *path, const char *app_path) {
    NotificationFiles files = {0};
    CondMissing *missing = NULL;
    size_t missing_count = 0, bound = 0, decides = 0;
    sqlite3 *db = NULL;
    FILE *file = NULL;
    int rc = -1;
    if (sqlite3_open(":memory:", &db) != SQLITE_OK) goto done;
    notification_walk(db, app_path, &files, 0);
    if (files.count) qsort(files.paths, files.count, sizeof(char *), path_order);
    file = !strcmp(path, "-") ? stdout : fopen(path, "wb");
    if (!file) { fprintf(stderr, "Error: cannot emit notification plans to %s\n", path); goto done; }
    fprintf(file, "{\"schema\":\"bonfyre.frappe-notification-plans.v1\",\"notifications\":[");
    for (size_t i = 0; i < files.count; ++i) {
        char *json = read_small_file(files.paths[i]);
        char *name, *doctype, *event, *channel, *condition, *field, *days, *subject, *message, *enabled, *assignees, *method;
        char subject_missing[96], message_missing[96], template_missing[96], digest[65] = "";
        const char *kind, *format = "";
        BfCondPlan plan;
        sqlite3_stmt *each = NULL;
        int decidable, first = 1;
        if (!json) continue;
        name = json_field(db, json, "$.name"); doctype = json_field(db, json, "$.document_type");
        event = json_field(db, json, "$.event"); channel = json_field(db, json, "$.channel");
        condition = json_field(db, json, "$.condition"); days = json_field(db, json, "$.days_in_advance");
        subject = json_field(db, json, "$.subject"); message = notification_template(db, json, files.paths[i], &format, template_missing);
        enabled = json_field(db, json, "$.enabled"); assignees = json_field(db, json, "$.send_to_all_assignees");
        method = json_field(db, json, "$.method");
        kind = trigger_kind(event);
        field = json_field(db, json, !strcmp(kind, "change") ? "$.value_changed" : "$.date_changed");
        if (i) fputc(',', file);
        fprintf(file, "{\"notification\":"); json_string(file, name ? name : "");
        fprintf(file, ",\"document_type\":"); json_string(file, doctype ? doctype : "");
        fprintf(file, ",\"channel\":"); json_string(file, channel ? channel : "");
        fprintf(file, ",\"enabled\":%s", enabled && atoi(enabled) ? "true" : "false");
        fprintf(file, ",\"trigger\":{\"event\":"); json_string(file, event ? event : "");
        fprintf(file, ",\"kind\":\"%s\",\"field\":", kind); json_string(file, field ? field : "");
        fprintf(file, ",\"days\":%d,\"missing_operation\":", days ? atoi(days) : 0);
        if (!kind[0]) {
            char op[96];
            snprintf(op, sizeof(op), "trigger:%.60s%s%.20s", event ? event : "", method ? ":" : "", method ? method : "");
            json_string(file, op);
            count_missing(&missing, &missing_count, op);
        } else json_string(file, "");
        fprintf(file, "},\"condition\":");
        emit_condition_object(file, condition, &plan);
        if (condition && condition[0] && !plan.compiled) count_missing(&missing, &missing_count, plan.missing_operation);
        decidable = kind[0] && (!condition || !condition[0] || plan.compiled);
        bf_cond_plan_free(&plan);

        fprintf(file, ",\"recipients\":[");
        if (sqlite3_prepare_v2(db,
              "SELECT COALESCE(json_extract(value,'$.receiver_by_document_field'), json_extract(value,'$.email_by_document_field')),"
              " COALESCE(json_extract(value,'$.receiver_by_role'), json_extract(value,'$.email_by_role')),"
              " json_extract(value,'$.cc'), json_extract(value,'$.bcc'), json_extract(value,'$.condition')"
              " FROM json_each(?1,'$.recipients')", -1, &each, NULL) == SQLITE_OK) {
            sqlite3_bind_text(each, 1, json, -1, SQLITE_STATIC);
            while (sqlite3_step(each) == SQLITE_ROW) {
                static const char *const selectors[] = {"field", "role", "address", "address"};
                const char *row_condition = (const char *)sqlite3_column_text(each, 4);
                for (int c = 0; c < 4; ++c) {
                    const char *value = (const char *)sqlite3_column_text(each, c);
                    BfCondPlan row_plan;
                    if (!value || !value[0]) continue;
                    fprintf(file, "%s{\"selector\":\"%s\",\"value\":", first ? "" : ",", selectors[c]);
                    json_string(file, value);
                    fprintf(file, ",\"condition\":");
                    emit_condition_object(file, row_condition, &row_plan);
                    if (row_condition && row_condition[0] && !row_plan.compiled) {
                        count_missing(&missing, &missing_count, row_plan.missing_operation);
                        decidable = 0;
                    }
                    bf_cond_plan_free(&row_plan);
                    fputc('}', file);
                    first = 0;
                }
            }
            sqlite3_finalize(each);
        }
        if (assignees && atoi(assignees))
            fprintf(file, "%s{\"selector\":\"assignees\",\"value\":\"\",\"condition\":{\"source\":\"\",\"status\":\"none\"}}", first ? "" : ",");
        fprintf(file, "]");

        template_render(db, subject, NULL, NULL, subject_missing, NULL);
        template_render(db, message, NULL, NULL, message_missing, NULL);
        if (template_missing[0]) snprintf(message_missing, sizeof(message_missing), "%s", template_missing);
        count_missing(&missing, &missing_count, subject_missing);
        count_missing(&missing, &missing_count, message_missing);
        if (message) bf_sha256_hex((const uint8_t *)message, strlen(message), digest);
        fprintf(file, ",\"subject\":{\"template\":"); json_string(file, subject ? subject : "");
        fprintf(file, ",\"missing_operation\":"); json_string(file, subject_missing);
        fprintf(file, "},\"message\":{\"format\":\"%s\",\"sha256\":\"%s\",\"bytes\":%zu,\"missing_operation\":", format, digest, message ? strlen(message) : 0);
        json_string(file, message_missing);
        /* decides: this estate can say whether it fires and for whom.
         * composes: it can also write the whole message. */
        fprintf(file, "},\"decides\":%s,\"composes\":%s}", decidable ? "true" : "false",
                decidable && !subject_missing[0] && !message_missing[0] ? "true" : "false");
        if (decidable) decides++;
        if (decidable && !subject_missing[0] && !message_missing[0]) bound++;
        free(json); free(name); free(doctype); free(event); free(channel); free(condition); free(field);
        free(days); free(subject); free(message); free(enabled); free(assignees); free(method);
    }
    fprintf(file, "],\"counts\":{\"notifications\":%zu,\"decides\":%zu,\"composes\":%zu},\"missing_operations\":[",
            files.count, decides, bound);
    for (size_t m = 0; m < missing_count; ++m) {
        fprintf(file, "%s{\"operation\":", m ? "," : ""); json_string(file, missing[m].operation);
        fprintf(file, ",\"definitions\":%zu}", missing[m].declarations);
    }
    fprintf(file, "],\"cannot_infer\":[");
    json_string(file, "a notification that decides and composes -> a message sent, delivered or read; a held message is transmitted by nobody");
    fputc(',', file);
    json_string(file, "a recipient selected by role -> a person; this estate holds no holder of a donor role");
    fputc(',', file);
    json_string(file, "a compiled condition -> the donor's server evaluating it; names outside the record refuse here");
    fprintf(file, "]}\n");
    if (file != stdout)
        printf("Notification plans: %zu definitions, %zu decided here, %zu composed in full\n", files.count, decides, bound);
    rc = ferror(file) ? -1 : 0;
done:
    if (file && file != stdout) fclose(file);
    for (size_t i = 0; i < files.count; ++i) free(files.paths[i]);
    free(files.paths); free(missing);
    if (db) sqlite3_close(db);
    return rc;
}

static int sql_true(sqlite3 *db, const char *sql, const char *a, const char *b, const char *c, int n) {
    sqlite3_stmt *stmt = NULL;
    int out = -1;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) return -1;
    if (a) sqlite3_bind_text(stmt, 1, a, -1, SQLITE_STATIC);
    if (b) sqlite3_bind_text(stmt, 2, b, -1, SQLITE_STATIC);
    if (c) sqlite3_bind_text(stmt, 3, c, -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 4, n);
    if (sqlite3_step(stmt) == SQLITE_ROW && sqlite3_column_type(stmt, 0) != SQLITE_NULL) out = sqlite3_column_int(stmt, 0) != 0;
    sqlite3_finalize(stmt);
    return out;
}

/* One definition, one event, one record: does it fire, for whom, saying what.
 * The answer is a held message. Nothing here can send one. */
/* not_carried names fields the caller's store keeps for its own use; a
 * condition reading one cannot be answered from that store's row. composed is
 * set when the message fires and its whole text was written. */
static int prepare_notification(FILE *out, const char *definition_path, const char *event, const char *doc,
                                const char *previous, const char *today, const char *not_carried,
                                char key_out[65], int *composed) {
    sqlite3 *db = NULL;
    char *json = read_small_file(definition_path);
    char *name, *doctype, *defined_event, *condition, *field, *days, *subject, *message, *enabled, *assignees, *docname;
    const char *kind, *reason = "", *format = "";
    char template_missing[96] = "", undefined[256] = "";
    char why[240] = "", subject_missing[96] = "", message_missing[96] = "", digest[65] = "", key[65], keytext[900];
    char *subject_text = NULL, *message_text = NULL;
    size_t subject_len = 0, message_len = 0;
    int fires = 1, n, first = 1, addressed = 0;
    char seen[2048] = "\n";   /* an address is named once however many rows select it */
    sqlite3_stmt *each = NULL;
    if (!json || !doc || !event || sqlite3_open(":memory:", &db) != SQLITE_OK || !json_text_is_notification(db, json)) {
        fprintf(stderr, "Error: --prepare-notification needs a Notification definition, --event and --doc\n");
        free(json);
        if (db) sqlite3_close(db);
        return 1;
    }
    name = json_field(db, json, "$.name"); doctype = json_field(db, json, "$.document_type");
    defined_event = json_field(db, json, "$.event"); condition = json_field(db, json, "$.condition");
    days = json_field(db, json, "$.days_in_advance"); subject = json_field(db, json, "$.subject");
    message = notification_template(db, json, definition_path, &format, template_missing); enabled = json_field(db, json, "$.enabled");
    assignees = json_field(db, json, "$.send_to_all_assignees");
    docname = doc_field(db, doc, "name");
    kind = trigger_kind(defined_event);
    field = json_field(db, json, !strcmp(kind, "change") ? "$.value_changed" : "$.date_changed");
    n = days ? atoi(days) : 0;

    if (!enabled || !atoi(enabled)) { fires = 0; reason = "the definition is switched off"; }
    else if (!kind[0]) { fires = -1; snprintf(why, sizeof(why), "trigger:%.80s is a controller hook this estate does not run", defined_event ? defined_event : ""); }
    else if (!strcmp(kind, "record")) {
        if (strcmp(event, defined_event)) { fires = 0; reason = "a different event happened to the record"; }
    } else if (!strcmp(kind, "change")) {
        char sql[400];
        if (strcmp(event, "Value Change") && strcmp(event, "Save")) { fires = 0; reason = "a different event happened to the record"; }
        else if (!previous) { fires = -1; snprintf(why, sizeof(why), "a change cannot be seen without the record as it was before"); }
        else if (!field || strchr(field, '"') || strchr(field, '\\') || strlen(field) > 120) { fires = -1; snprintf(why, sizeof(why), "the definition names no field to watch"); }
        else {
            snprintf(sql, sizeof(sql), "SELECT json_extract(?1,'$.\"%s\"') IS NOT json_extract(?2,'$.\"%s\"')", field, field);
            fires = sql_true(db, sql, doc, previous, NULL, 0);
            if (fires == 0) reason = "the watched field did not change";
            else if (fires < 0) snprintf(why, sizeof(why), "the record is not valid JSON");
        }
    } else {
        char sql[400], *value = doc_field(db, doc, field);
        if (strcmp(event, "Daily") && strcmp(event, defined_event)) { fires = 0; reason = "a different event happened to the record"; }
        else if (!value || !value[0]) { fires = 0; reason = "the record carries no date to count from"; }
        else {
            snprintf(sql, sizeof(sql), "SELECT date(?1) = date(COALESCE(?2,'now'), (?4 || ' days'))");
            fires = sql_true(db, sql, value, today, NULL, !strcmp(defined_event, "Days Before") ? n : -n);
            if (fires == 0) reason = "the date is not the stated number of days away";
            else if (fires < 0) snprintf(why, sizeof(why), "the record's date could not be read as a date");
        }
        free(value);
    }
    if (fires == 1 && condition && condition[0]) {
        BfCondPlan plan;
        char error[200];
        const char *kept = NULL;
        int compiled = bf_cond_compile_python(condition, &plan) == 0;
        for (size_t r = 0; compiled && not_carried && r < plan.read_count; ++r) {
            const char *hit = strstr(not_carried, plan.reads[r]);
            size_t len = strlen(plan.reads[r]);
            if (hit && (hit == not_carried || hit[-1] == ',') && (hit[len] == ',' || !hit[len])) kept = plan.reads[r];
        }
        if (!compiled) {
            fires = -1;
            snprintf(why, sizeof(why), "the condition uses %s, which this estate does not evaluate", plan.missing_operation);
        } else if (kept) {
            fires = -1;
            snprintf(why, sizeof(why), "the condition reads %s, which the store this record came from keeps for its own use", kept);
        } else {
            fires = bf_cond_evaluate(&plan, db, doc, NULL, NULL, error, sizeof(error));
            if (fires == 0) reason = "the condition does not hold for this record";
            else if (fires < 0) snprintf(why, sizeof(why), "%s", error);
        }
        bf_cond_plan_free(&plan);
    }

    snprintf(keytext, sizeof(keytext), "%.200s|%.60s|%.200s|%.200s|%.40s", name ? name : "", event,
             doctype ? doctype : "", docname ? docname : "", today ? today : "");
    bf_sha256_hex((const uint8_t *)keytext, strlen(keytext), key);
    if (key_out) memcpy(key_out, key, 65);
    if (composed) *composed = 0;
    fprintf(out, "{\"schema\":\"bonfyre.held-message.notification.v1\",\"notification\":"); json_string(out, name ? name : "");
    fprintf(out, ",\"document_type\":"); json_string(out, doctype ? doctype : "");
    fprintf(out, ",\"document\":"); json_string(out, docname ? docname : "");
    fprintf(out, ",\"event\":"); json_string(out, event);
    fprintf(out, ",\"fires\":%s,\"reason\":", fires < 0 ? "null" : fires ? "true" : "false");
    json_string(out, fires < 0 ? why : reason);
    fprintf(out, ",\"key\":\"%s\"", key);
    if (fires == 1) {
        fprintf(out, ",\"recipients\":[");
        if (sqlite3_prepare_v2(db,
              "SELECT COALESCE(json_extract(value,'$.receiver_by_document_field'), json_extract(value,'$.email_by_document_field')),"
              " COALESCE(json_extract(value,'$.receiver_by_role'), json_extract(value,'$.email_by_role')),"
              " json_extract(value,'$.cc'), json_extract(value,'$.bcc'), json_extract(value,'$.condition')"
              " FROM json_each(?1,'$.recipients')", -1, &each, NULL) == SQLITE_OK) {
            sqlite3_bind_text(each, 1, json, -1, SQLITE_STATIC);
            while (sqlite3_step(each) == SQLITE_ROW) {
                const char *row_condition = (const char *)sqlite3_column_text(each, 4);
                const char *withheld = "";
                if (row_condition && row_condition[0]) {
                    BfCondPlan plan;
                    char error[200];
                    int holds = bf_cond_compile_python(row_condition, &plan) == 0
                              ? bf_cond_evaluate(&plan, db, doc, NULL, NULL, error, sizeof(error)) : -1;
                    bf_cond_plan_free(&plan);
                    if (holds == 0) continue;
                    if (holds < 0) withheld = "this recipient's own condition could not be evaluated";
                }
                for (int c = 0; c < 4; ++c) {
                    const char *value = (const char *)sqlite3_column_text(each, c);
                    char *held = NULL, *text, *token, *save = NULL;
                    if (!value || !value[0]) continue;
                    if (c == 1) {
                        fprintf(out, "%s{\"selector\":\"role\",\"value\":", first ? "" : ","); json_string(out, value);
                        fprintf(out, ",\"address\":null,\"unresolved\":\"this estate holds no holder of a donor role\"}");
                        first = 0;
                        continue;
                    }
                    /* A field may name several fields, and each may hold several addresses. */
                    text = strdup(value);
                    for (token = text ? strtok_r(text, ",\n", &save) : NULL; token; token = strtok_r(NULL, ",\n", &save)) {
                        char *list, *address, *inner = NULL;
                        while (isspace((unsigned char)*token)) ++token;
                        if (!*token) continue;
                        held = c == 0 ? doc_field(db, doc, token) : strdup(token);
                        list = held ? held : NULL;
                        if (!list || !list[0]) {
                            fprintf(out, "%s{\"selector\":\"%s\",\"value\":", first ? "" : ",", c == 0 ? "field" : "address");
                            json_string(out, token);
                            fprintf(out, ",\"address\":null,\"unresolved\":\"the record carries nothing in that field\"}");
                            first = 0;
                            free(held);
                            continue;
                        }
                        for (address = strtok_r(list, ",\n", &inner); address; address = strtok_r(NULL, ",\n", &inner)) {
                            const char *at;
                            while (isspace((unsigned char)*address)) ++address;
                            if (!*address) continue;
                            at = strchr(address, '@');
                            if (at && !*withheld) {
                                char probe[300];
                                snprintf(probe, sizeof(probe), "\n%.290s\n", address);
                                if (strstr(seen, probe)) continue;
                                if (strlen(seen) + strlen(probe) < sizeof(seen)) strcat(seen, probe + 1);
                            }
                            fprintf(out, "%s{\"selector\":\"%s\",\"value\":", first ? "" : ",", c == 0 ? "field" : "address");
                            json_string(out, token);
                            if (*withheld) { fprintf(out, ",\"address\":null,\"unresolved\":"); json_string(out, withheld); }
                            else if (at && at != address && strchr(at, '.')) { fprintf(out, ",\"address\":"); json_string(out, address); fprintf(out, ",\"unresolved\":\"\""); addressed++; }
                            else fprintf(out, ",\"address\":null,\"unresolved\":\"the field names somebody and holds no address\"");
                            fprintf(out, "}");
                            first = 0;
                        }
                        free(held);
                    }
                    free(text);
                }
            }
            sqlite3_finalize(each);
        }
        if (assignees && atoi(assignees)) {
            fprintf(out, "%s{\"selector\":\"assignees\",\"value\":\"\",\"address\":null,\"unresolved\":\"this estate holds no assignment of that record\"}", first ? "" : ",");
        }
        fprintf(out, "],\"addressed\":%d", addressed);
        {
            FILE *mem = open_memstream(&subject_text, &subject_len);
            if (mem) { if (template_render(db, subject, doc, mem, subject_missing, NULL) != 0) subject_len = 0; fclose(mem); }
            mem = open_memstream(&message_text, &message_len);
            if (mem) { template_render(db, message, doc, mem, message_missing, undefined); fclose(mem); }
            if (template_missing[0]) snprintf(message_missing, sizeof(message_missing), "%s", template_missing);
        }
        if (message) bf_sha256_hex((const uint8_t *)message, strlen(message), digest);
        fprintf(out, ",\"subject\":");
        if (subject_missing[0]) fprintf(out, "null"); else json_string(out, subject_text ? subject_text : "");
        fprintf(out, ",\"subject_missing_operation\":"); json_string(out, subject_missing);
        fprintf(out, ",\"body\":");
        if (message_missing[0] || !message) fprintf(out, "null"); else json_string(out, message_text ? message_text : "");
        fprintf(out, ",\"body_format\":\"%s\",\"body_template_sha256\":\"%s\",\"body_missing_operation\":", format, digest);
        json_string(out, message_missing);
        /* Names the donor's own template leaves undefined are printed as written, as the donor prints them. */
        fprintf(out, ",\"left_as_written\":"); json_string(out, undefined);
        /* Where the donor links a record into its own desk, the name is written and the link is not. */
        fprintf(out, ",\"links_withheld\":%s", message && strstr(message, "get_link_to_form") ? "true" : "false");
        fprintf(out, ",\"dates_written_as\":\"2026-10-02 Fri 14:30 -- this estate's format, where the donor would consult a site or user setting\"");
        /* The states are the held-message states; none of them transmits. A
         * message whose text could not be written is not composed. */
        if (subject_missing[0] || message_missing[0]) fprintf(out, ",\"state\":null,\"transmitted\":false");
        else {
            fprintf(out, ",\"state\":\"composed\",\"transmitted\":false");
            if (composed) *composed = 1;
        }
    }
    fprintf(out, ",\"cannot_infer\":\"a held message -> a sent, delivered or read one; nothing in this estate transmits it\"}\n");
    free(subject_text); free(message_text);
    free(json); free(name); free(doctype); free(defined_event); free(condition); free(field); free(days);
    free(subject); free(message); free(enabled); free(assignees); free(docname);
    sqlite3_close(db);
    return fires < 0 ? 3 : 0;
}

/* ---- Metrics --------------------------------------------------------------
 * A donor Number Card or Dashboard Chart is a definition of one read: a
 * function of a field over the records of one kind, narrowed by filters, and
 * for a chart grouped by a field. It is compiled to a plan and answered from a
 * store this estate already has -- one table, read only, every value bound --
 * or refused by the name of what it needs. A card backed by a report or a
 * method is the donor's own code and is named, not approximated. */

typedef struct { int y, m, d; } CivilDate;

static long days_from_civil(CivilDate c) {
    int y = c.y - (c.m <= 2);
    long era = (y >= 0 ? y : y - 399) / 400;
    unsigned yoe = (unsigned)(y - era * 400);
    unsigned doy = (unsigned)((153 * (c.m + (c.m > 2 ? -3 : 9)) + 2) / 5 + c.d - 1);
    unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + (long)doe - 719468;
}

static CivilDate civil_from_days(long z) {
    CivilDate c;
    long era;
    unsigned doe, yoe, doy, mp;
    z += 719468;
    era = (z >= 0 ? z : z - 146096) / 146097;
    doe = (unsigned)(z - era * 146097);
    yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    mp = (5 * doy + 2) / 153;
    c.d = (int)(doy - (153 * mp + 2) / 5 + 1);
    c.m = (int)(mp < 10 ? mp + 3 : mp - 9);
    c.y = (int)(yoe + era * 400) + (c.m <= 2);
    return c;
}

static int days_in_month(int y, int m) {
    static const int days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    return m == 2 && ((y % 4 == 0 && y % 100 != 0) || y % 400 == 0) ? 29 : days[m - 1];
}

static CivilDate add_days(CivilDate c, long n) { return civil_from_days(days_from_civil(c) + n); }

static CivilDate add_months(CivilDate c, int n) {
    int months = c.y * 12 + (c.m - 1) + n, limit;
    c.y = months >= 0 ? months / 12 : -((11 - months) / 12);
    c.m = months - c.y * 12 + 1;
    limit = days_in_month(c.y, c.m);
    if (c.d > limit) c.d = limit;
    return c;
}

static CivilDate month_start(CivilDate c) { c.d = 1; return c; }
static CivilDate month_end(CivilDate c) { c.d = days_in_month(c.y, c.m); return c; }
static CivilDate quarter_start(CivilDate c) { c.m = (c.m - 1) / 3 * 3 + 1; c.d = 1; return c; }
static CivilDate quarter_end(CivilDate c) { return add_days(add_months(quarter_start(c), 3), -1); }
/* A week starts on Monday here, as ISO 8601 counts it. The donor reads the
 * first day of the week from a site setting; this is the estate's own. */
static CivilDate week_start(CivilDate c) { long z = days_from_civil(c); return civil_from_days(z - (z + 3) % 7); }

/* The donor's named spans (utils/data.py get_timespan_date_range). */
static int timespan_range(const char *name, CivilDate today, CivilDate *from, CivilDate *to) {
    char span[40];
    size_t i = 0;
    int n;
    for (; name[i] && i < sizeof(span) - 1; ++i) span[i] = (char)tolower((unsigned char)name[i]);
    span[i] = '\0';
    *from = *to = today;
    if (sscanf(span, "last %d days", &n) == 1 && (n == 7 || n == 14 || n == 30 || n == 90)) { *from = add_days(today, -n); return 0; }
    if (sscanf(span, "next %d days", &n) == 1 && (n == 7 || n == 14 || n == 30)) { *to = add_days(today, n); return 0; }
    if (!strcmp(span, "today")) return 0;
    if (!strcmp(span, "yesterday")) { *from = *to = add_days(today, -1); return 0; }
    if (!strcmp(span, "tomorrow")) { *from = *to = add_days(today, 1); return 0; }
    if (!strcmp(span, "last week") || !strcmp(span, "this week") || !strcmp(span, "next week")) {
        *from = week_start(add_days(today, span[0] == 'l' ? -7 : span[0] == 'n' ? 7 : 0));
        *to = add_days(*from, 6);
        return 0;
    }
    if (!strcmp(span, "last month") || !strcmp(span, "this month") || !strcmp(span, "next month")) {
        CivilDate in = add_months(today, span[0] == 'l' ? -1 : span[0] == 'n' ? 1 : 0);
        *from = month_start(in); *to = month_end(in);
        return 0;
    }
    if (!strcmp(span, "last quarter") || !strcmp(span, "this quarter") || !strcmp(span, "next quarter")) {
        CivilDate in = add_months(today, span[0] == 'l' ? -3 : span[0] == 'n' ? 3 : 0);
        *from = quarter_start(in); *to = quarter_end(in);
        return 0;
    }
    if (!strcmp(span, "last 6 months")) { *from = quarter_start(add_months(today, -6)); *to = quarter_end(add_months(today, -3)); return 0; }
    if (!strcmp(span, "next 6 months")) { *from = quarter_start(add_months(today, 3)); *to = quarter_end(add_months(today, 6)); return 0; }
    if (!strcmp(span, "last year") || !strcmp(span, "this year") || !strcmp(span, "next year")) {
        from->y = to->y = today.y + (span[0] == 'l' ? -1 : span[0] == 'n' ? 1 : 0);
        from->m = 1; from->d = 1; to->m = 12; to->d = 31;
        return 0;
    }
    return -1;
}

#define METRIC_MAX_BINDS 64

typedef struct {
    char where[4096];
    size_t where_len;
    char *binds[METRIC_MAX_BINDS];
    int numeric[METRIC_MAX_BINDS];
    int bind_count;
    int filters;
    char missing[96];        /* an operation this estate does not perform */
    char needs[256];         /* what the reader has to say first, comma separated */
    char not_carried[96];    /* a field the store's table does not hold */
    sqlite3 *meta;
    sqlite3 *store;          /* NULL while only compiling */
    const char *table;
    CivilDate today;
    char *const *params;     /* "Name=value" */
    const char *kept;        /* columns the store keeps for its own use, comma separated */
    size_t param_count;
} Metric;

static int plain_identifier(const char *text) {
    if (!text || !text[0] || strlen(text) > 64) return 0;
    for (const char *c = text; *c; ++c)
        if (!isalnum((unsigned char)*c) && *c != '_') return 0;
    return 1;
}

static void metric_miss(Metric *m, const char *fmt, const char *detail) {
    if (!m->missing[0]) snprintf(m->missing, sizeof(m->missing), fmt, detail);
}

static int metric_bind(Metric *m, const char *value, int numeric) {
    if (m->bind_count >= METRIC_MAX_BINDS) { metric_miss(m, "%s", "too_many_values"); return -1; }
    if (!(m->binds[m->bind_count] = strdup(value ? value : ""))) return -1;
    m->numeric[m->bind_count++] = numeric;
    return 0;
}

static int metric_column(Metric *m, const char *field) {
    sqlite3_stmt *stmt = NULL;
    int found = 0;
    if (!plain_identifier(field)) { metric_miss(m, "%s", "field_name"); return -1; }
    if (!m->store) return 0;
    /* A column the store uses for itself is not the donor's field of that name. */
    if (m->kept) {
        const char *hit = strstr(m->kept, field);
        size_t len = strlen(field);
        if (hit && (hit == m->kept || hit[-1] == ',') && (hit[len] == ',' || !hit[len])) {
            if (!m->not_carried[0]) snprintf(m->not_carried, sizeof(m->not_carried), "%s", field);
            return -1;
        }
    }
    if (sqlite3_prepare_v2(m->store, "SELECT 1 FROM pragma_table_info(?1) WHERE name=?2", -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, m->table, -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 2, field, -1, SQLITE_STATIC);
        found = sqlite3_step(stmt) == SQLITE_ROW;
        sqlite3_finalize(stmt);
    }
    if (!found && !m->not_carried[0]) snprintf(m->not_carried, sizeof(m->not_carried), "%s", field);
    return found ? 0 : -1;
}

static void metric_where(Metric *m, const char *fmt, const char *field) {
    m->where_len += (size_t)snprintf(m->where + m->where_len, sizeof(m->where) - m->where_len, "%s", m->filters ? " AND " : " WHERE ");
    if (m->where_len < sizeof(m->where))
        m->where_len += (size_t)snprintf(m->where + m->where_len, sizeof(m->where) - m->where_len, fmt, field, field);
    if (m->where_len >= sizeof(m->where)) { m->where_len = sizeof(m->where) - 1; metric_miss(m, "%s", "too_many_filters"); }
    m->filters++;
}

/* value_json is the filter's value as JSON text; value_type is its JSON type. */
static void metric_filter(Metric *m, const char *field, const char *op, const char *value_type, const char *value_json) {
    char lowered[24];
    size_t i = 0;
    if (!op) op = "=";
    for (; op[i] && i < sizeof(lowered) - 1; ++i) lowered[i] = (char)tolower((unsigned char)op[i]);
    lowered[i] = '\0';
    if (metric_column(m, field) != 0 && m->missing[0]) return;
    if (!value_type) value_type = "null";

    if (!strcmp(lowered, "timespan")) {
        CivilDate from, to;
        char text[16];
        if (strcmp(value_type, "text") || timespan_range(value_json, m->today, &from, &to) != 0) {
            metric_miss(m, "timespan:%.70s", value_json ? value_json : "");
            return;
        }
        metric_where(m, "date(\"%s\") BETWEEN ? AND ?", field);
        snprintf(text, sizeof(text), "%04d-%02d-%02d", from.y, from.m, from.d); metric_bind(m, text, 0);
        snprintf(text, sizeof(text), "%04d-%02d-%02d", to.y, to.m, to.d); metric_bind(m, text, 0);
        return;
    }
    if (!strcmp(lowered, "is")) {
        if (value_json && !strcmp(value_json, "set")) metric_where(m, "(\"%s\" IS NOT NULL AND \"%s\" != '')", field);
        else if (value_json && !strcmp(value_json, "not set")) metric_where(m, "(\"%s\" IS NULL OR \"%s\" = '')", field);
        else metric_miss(m, "filter:%s", "is");
        return;
    }
    if (!strcmp(lowered, "in") || !strcmp(lowered, "not in") || !strcmp(lowered, "between")) {
        sqlite3_stmt *each = NULL;
        char clause[1024];
        size_t len;
        int count = 0;
        if (strcmp(value_type, "array")) { metric_miss(m, "filter:%s_without_a_list", lowered[0] == 'b' ? "between" : "in"); return; }
        len = (size_t)snprintf(clause, sizeof(clause), "\"%%s\" %s", lowered[0] == 'b' ? "BETWEEN " : lowered[0] == 'n' ? "NOT IN (" : "IN (");
        if (sqlite3_prepare_v2(m->meta, "SELECT type, atom FROM json_each(?1)", -1, &each, NULL) != SQLITE_OK) return;
        sqlite3_bind_text(each, 1, value_json, -1, SQLITE_STATIC);
        while (sqlite3_step(each) == SQLITE_ROW && len < sizeof(clause) - 16) {
            const char *type = (const char *)sqlite3_column_text(each, 0);
            const char *atom = (const char *)sqlite3_column_text(each, 1);
            len += (size_t)snprintf(clause + len, sizeof(clause) - len, "%s?", !count ? "" : lowered[0] == 'b' ? " AND " : ",");
            metric_bind(m, atom ? atom : "", type && (!strcmp(type, "integer") || !strcmp(type, "real")));
            count++;
        }
        sqlite3_finalize(each);
        if (lowered[0] == 'b' ? count != 2 : count < 1) { metric_miss(m, "filter:%s_arity", lowered[0] == 'b' ? "between" : "in"); return; }
        if (lowered[0] != 'b') snprintf(clause + len, sizeof(clause) - len, ")");
        metric_where(m, clause, field);
        return;
    }
    {
        static const char *const simple[][2] = {
            {"=", "\"%s\" = ?"}, {"!=", "\"%s\" != ?"}, {">", "\"%s\" > ?"}, {"<", "\"%s\" < ?"},
            {">=", "\"%s\" >= ?"}, {"<=", "\"%s\" <= ?"}, {"like", "\"%s\" LIKE ?"}, {"not like", "\"%s\" NOT LIKE ?"}
        };
        for (size_t k = 0; k < sizeof(simple) / sizeof(simple[0]); ++k) {
            if (strcmp(lowered, simple[k][0])) continue;
            if (!strcmp(value_type, "array") || !strcmp(value_type, "object")) { metric_miss(m, "filter:%s_with_a_list", lowered); return; }
            if (!strcmp(value_type, "null")) {
                /* Equal to nothing is how the donor asks for unset. */
                metric_where(m, lowered[0] == '!' ? "(\"%s\" IS NOT NULL AND \"%s\" != '')" : "(\"%s\" IS NULL OR \"%s\" = '')", field);
                return;
            }
            metric_where(m, simple[k][1], field);
            metric_bind(m, !strcmp(value_type, "true") ? "1" : !strcmp(value_type, "false") ? "0" : value_json,
                        strcmp(value_type, "text") != 0);
            return;
        }
    }
    metric_miss(m, "filter:%.80s", lowered);
}

/* A dynamic filter's value is an expression the donor's client evaluates. The
 * one that matters asks for the reader's default of something; here that is a
 * thing the reader has to say, and without it the metric is not answered. */
static void metric_dynamic_value(Metric *m, const char *field, const char *op, const char *source) {
    const char *call = strstr(source, "frappe.defaults.get_user_default(");
    if (call) {
        char name[64];
        const char *open = call + strlen("frappe.defaults.get_user_default(");
        size_t len;
        while (isspace((unsigned char)*open)) ++open;
        if (*open != '"' && *open != '\'') { metric_miss(m, "%s", "dynamic_filter:user_default_form"); return; }
        len = strcspn(open + 1, "\"'");
        if (!len || len >= sizeof(name)) { metric_miss(m, "%s", "dynamic_filter:user_default_form"); return; }
        snprintf(name, sizeof(name), "%.*s", (int)len, open + 1);
        for (size_t p = 0; p < m->param_count; ++p) {
            size_t key = strcspn(m->params[p], "=");
            if (key == len && !strncmp(m->params[p], name, len) && m->params[p][key] == '=') {
                metric_filter(m, field, op, "text", m->params[p] + key + 1);
                return;
            }
        }
        if (metric_column(m, field) != 0 && m->missing[0]) return;
        if (!strstr(m->needs, name) && strlen(m->needs) + len + 3 < sizeof(m->needs)) {
            if (m->needs[0]) strcat(m->needs, ",");
            strcat(m->needs, name);
        }
        return;
    }
    {
        BfCondPlan plan;
        char *prefixed = malloc(strlen(source) + 6), *json = NULL, *text = NULL, error[160];
        if (!prefixed) return;
        sprintf(prefixed, "eval:%s", source);
        char env[96];
        /* Today is the day being asked about; the donor's client state is bound
         * empty, so an expression that falls back when it is absent falls back. */
        snprintf(env, sizeof(env), "{\"today\":\"%04d-%02d-%02d\",\"frappe\":{\"boot\":{}}}", m->today.y, m->today.m, m->today.d);
        if (bf_cond_compile(prefixed, &plan) != 0) metric_miss(m, "dynamic_filter:%.70s", plan.missing_operation);
        else if (plan.read_count || plan.parent_read_count) metric_miss(m, "%s", "dynamic_filter:reads_a_document");
        else if (bf_cond_evaluate_value(&plan, m->meta, "{}", NULL, env, &json, &text, error, sizeof(error)) < 0)
            metric_miss(m, "%s", "dynamic_filter:evaluation");
        else metric_filter(m, field, op, json && json[0] == '"' ? "text" : json && json[0] == '[' ? "array" : "real",
                           json && json[0] == '"' ? text : json);
        free(json); free(text); free(prefixed);
        bf_cond_plan_free(&plan);
    }
}

/* Both forms the donor writes: a list of [doctype, field, operator, value], or
 * an object of field: value / field: [operator, value]. */
static void metric_filters(Metric *m, const char *filters, const char *document_type, int dynamic) {
    sqlite3_stmt *each = NULL;
    if (!filters || !filters[0]) return;
    if (sqlite3_prepare_v2(m->meta,
          "SELECT json_type(?1), key, type, value,"
          " json_extract(value,'$[0]'), json_extract(value,'$[1]'), json_extract(value,'$[2]'),"
          " json_type(value,'$[3]'), json_extract(value,'$[3]'), json_type(value,'$[1]')"
          " FROM json_each(?1) WHERE json_valid(?1)", -1, &each, NULL) != SQLITE_OK) return;
    sqlite3_bind_text(each, 1, filters, -1, SQLITE_STATIC);
    while (sqlite3_step(each) == SQLITE_ROW && !m->missing[0]) {
        const char *container = (const char *)sqlite3_column_text(each, 0);
        const char *field, *op, *type, *value;
        if (container && !strcmp(container, "object")) {
            const char *entry_type = (const char *)sqlite3_column_text(each, 2);
            field = (const char *)sqlite3_column_text(each, 1);
            if (entry_type && !strcmp(entry_type, "array")) {
                op = (const char *)sqlite3_column_text(each, 4);
                type = (const char *)sqlite3_column_text(each, 9);
                value = (const char *)sqlite3_column_text(each, 5);
            } else { op = "="; type = entry_type; value = (const char *)sqlite3_column_text(each, 3); }
        } else {
            const char *of = (const char *)sqlite3_column_text(each, 4);
            if (of && document_type && strcmp(of, document_type)) { metric_miss(m, "%s", "filter_on_another_record_kind"); break; }
            field = (const char *)sqlite3_column_text(each, 5);
            op = (const char *)sqlite3_column_text(each, 6);
            type = (const char *)sqlite3_column_text(each, 7);
            value = (const char *)sqlite3_column_text(each, 8);
        }
        if (!field) { metric_miss(m, "%s", "filter_form"); break; }
        if (dynamic) {
            if (!type || strcmp(type, "text")) { metric_miss(m, "%s", "dynamic_filter:not_an_expression"); break; }
            metric_dynamic_value(m, field, op, value);
        } else metric_filter(m, field, op, type, value);
    }
    sqlite3_finalize(each);
}

typedef struct {
    char *name, *kind_field, *document_type, *function, *of, *group_by, *filters, *dynamic, *parent, *report, *method, *source, *timeseries;
    char *based_on, *timespan, *interval, *shape, *from_date, *to_date;
    int series;              /* a chart over time: one value per period */
    const char *kind;        /* "card" or "chart" */
    const char *sql_function;
} MetricDefinition;

static void metric_definition_free(MetricDefinition *d) {
    free(d->name); free(d->kind_field); free(d->document_type); free(d->function); free(d->of); free(d->group_by);
    free(d->filters); free(d->dynamic); free(d->parent); free(d->report); free(d->method); free(d->source); free(d->timeseries);
    free(d->based_on); free(d->timespan); free(d->interval); free(d->shape); free(d->from_date); free(d->to_date);
}

/* Reads the definition and names anything about its shape that is not a plain
 * aggregate over one kind of record. */
static void metric_definition(Metric *m, const char *json, MetricDefinition *d) {
    char *doctype = json_field(m->meta, json, "$.doctype");
    const char *function;
    memset(d, 0, sizeof(*d));
    d->kind = doctype && !strcmp(doctype, "Number Card") ? "card" : "chart";
    free(doctype);
    d->name = json_field(m->meta, json, "$.name");
    d->document_type = json_field(m->meta, json, "$.document_type");
    d->filters = json_field(m->meta, json, "$.filters_json");
    d->dynamic = json_field(m->meta, json, "$.dynamic_filters_json");
    d->parent = json_field(m->meta, json, "$.parent_document_type");
    d->report = json_field(m->meta, json, "$.report_name");
    d->of = json_field(m->meta, json, "$.aggregate_function_based_on");
    if (d->kind[1] == 'a') {
        d->kind_field = json_field(m->meta, json, "$.type");
        d->function = json_field(m->meta, json, "$.function");
        d->method = json_field(m->meta, json, "$.method");
        if (d->kind_field && !strcmp(d->kind_field, "Report")) metric_miss(m, "report:%.80s", d->report ? d->report : "");
        else if (d->kind_field && !strcmp(d->kind_field, "Custom")) metric_miss(m, "method:%.80s", d->method ? d->method : "");
    } else {
        d->kind_field = json_field(m->meta, json, "$.chart_type");
        d->function = json_field(m->meta, json, "$.group_by_type");
        d->group_by = json_field(m->meta, json, "$.group_by_based_on");
        d->source = json_field(m->meta, json, "$.source");
        d->timeseries = json_field(m->meta, json, "$.timeseries");
        if (!d->kind_field) metric_miss(m, "%s", "chart_type");
        else if (!strcmp(d->kind_field, "Report")) metric_miss(m, "report:%.80s", d->report ? d->report : "");
        else if (!strcmp(d->kind_field, "Custom")) metric_miss(m, "source:%.80s", d->source ? d->source : "");
        else if (strcmp(d->kind_field, "Group By")) {
            /* Count, Sum or Average over time: the chart type is the function. */
            static const char *const grains[] = {"Daily", "Weekly", "Monthly", "Quarterly", "Yearly"};
            int known = 0;
            d->series = 1;
            d->based_on = json_field(m->meta, json, "$.based_on");
            d->timespan = json_field(m->meta, json, "$.timespan");
            d->interval = json_field(m->meta, json, "$.time_interval");
            d->shape = json_field(m->meta, json, "$.type");
            d->from_date = json_field(m->meta, json, "$.from_date");
            d->to_date = json_field(m->meta, json, "$.to_date");
            free(d->function); d->function = strdup(d->kind_field);
            free(d->of); d->of = json_field(m->meta, json, "$.value_based_on");
            for (size_t g = 0; g < 5; ++g) known |= d->interval && !strcmp(d->interval, grains[g]);
            if (d->shape && !strcmp(d->shape, "Heatmap")) metric_miss(m, "%s", "heatmap");
            else if (!d->based_on || !d->based_on[0]) metric_miss(m, "%s", "date_field");
            else if (!known) metric_miss(m, "interval:%.70s", d->interval ? d->interval : "");
            else if (!d->timespan || (strcmp(d->timespan, "Last Week") && strcmp(d->timespan, "Last Month") &&
                     strcmp(d->timespan, "Last Quarter") && strcmp(d->timespan, "Last Year") &&
                     !(!strcmp(d->timespan, "Select Date Range") && d->from_date && d->to_date)))
                metric_miss(m, "timespan:%.70s", d->timespan ? d->timespan : "");
            else if (metric_column(m, d->based_on) != 0 && m->missing[0]) return;
        }
        else if (!d->group_by || !d->group_by[0]) metric_miss(m, "%s", "group_by_field");
        else if (strchr(d->group_by, '.')) metric_miss(m, "%s", "group_by_child_table");
    }
    if (m->missing[0]) return;
    if (d->parent && d->parent[0]) { metric_miss(m, "%s", "child_table_metric"); return; }
    if (!d->document_type || !d->document_type[0]) { metric_miss(m, "%s", "document_type"); return; }
    function = d->function && d->function[0] ? d->function : "Count";
    d->sql_function = !strcmp(function, "Count") ? "COUNT" : !strcmp(function, "Sum") ? "SUM" :
                      !strcmp(function, "Average") ? "AVG" : !strcmp(function, "Minimum") ? "MIN" :
                      !strcmp(function, "Maximum") ? "MAX" : NULL;
    if (!d->sql_function) { metric_miss(m, "function:%.80s", function); return; }
    if (strcmp(d->sql_function, "COUNT")) {
        if (!d->of || !d->of[0]) { metric_miss(m, "%s", "aggregate_field"); return; }
        if (metric_column(m, d->of) != 0 && m->missing[0]) return;
    }
    if (d->group_by && metric_column(m, d->group_by) != 0 && m->missing[0]) return;
    metric_filters(m, d->filters, d->document_type, 0);
    if (!m->missing[0]) metric_filters(m, d->dynamic, d->document_type, 1);
}

static void metric_free(Metric *m) {
    for (int i = 0; i < m->bind_count; ++i) free(m->binds[i]);
}

static void definition_walk(sqlite3 *db, const char *directory, NotificationFiles *out, int depth) {
    DIR *dir = depth <= 24 ? opendir(directory) : NULL;
    struct dirent *entry;
    if (!dir) return;
    while ((entry = readdir(dir)) != NULL) {
        char path[PATH_MAX];
        struct stat st;
        size_t len = strlen(entry->d_name);
        if (entry->d_name[0] == '.' || !strcmp(entry->d_name, "node_modules") || !strcmp(entry->d_name, "doctype")) continue;
        if (snprintf(path, sizeof(path), "%s/%s", directory, entry->d_name) >= (int)sizeof(path)) continue;
        if (lstat(path, &st) != 0 || S_ISLNK(st.st_mode)) continue;
        if (S_ISDIR(st.st_mode)) { definition_walk(db, path, out, depth + 1); continue; }
        if (!S_ISREG(st.st_mode) || len < 6 || strcmp(entry->d_name + len - 5, ".json")) continue;
        {
            char *json = read_small_file(path), *doctype = json ? json_field(db, json, "$.doctype") : NULL;
            if (doctype && (!strcmp(doctype, "Number Card") || !strcmp(doctype, "Dashboard Chart"))) {
                char **next = realloc(out->paths, sizeof(char *) * (out->count + 1));
                if (next) {
                    out->paths = next;
                    if ((out->paths[out->count] = strdup(path))) out->count++;
                }
            }
            free(doctype); free(json);
        }
    }
    closedir(dir);
}

static CivilDate metric_today(sqlite3 *db, const char *today) {
    CivilDate c = {1970, 1, 1};
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(db, "SELECT date(COALESCE(?1,'now'))", -1, &stmt, NULL) == SQLITE_OK) {
        if (today) sqlite3_bind_text(stmt, 1, today, -1, SQLITE_STATIC);
        if (sqlite3_step(stmt) == SQLITE_ROW && sqlite3_column_text(stmt, 0))
            sscanf((const char *)sqlite3_column_text(stmt, 0), "%d-%d-%d", &c.y, &c.m, &c.d);
        sqlite3_finalize(stmt);
    }
    return c;
}

static int emit_metric_plans(const char *path, const char *app_path) {
    NotificationFiles files = {0};
    CondMissing *missing = NULL, *needs = NULL;
    size_t missing_count = 0, needs_count = 0, compiled = 0, cards = 0, charts = 0, waiting = 0;
    sqlite3 *db = NULL;
    FILE *file = NULL;
    int rc = -1;
    if (sqlite3_open(":memory:", &db) != SQLITE_OK) goto done;
    definition_walk(db, app_path, &files, 0);
    if (files.count) qsort(files.paths, files.count, sizeof(char *), path_order);
    file = !strcmp(path, "-") ? stdout : fopen(path, "wb");
    if (!file) { fprintf(stderr, "Error: cannot emit metric plans to %s\n", path); goto done; }
    fprintf(file, "{\"schema\":\"bonfyre.frappe-metric-plans.v1\",\"metrics\":[");
    for (size_t i = 0; i < files.count; ++i) {
        char *json = read_small_file(files.paths[i]);
        Metric m;
        MetricDefinition d;
        if (!json) continue;
        memset(&m, 0, sizeof(m));
        m.meta = db;
        m.today = metric_today(db, "2026-01-01");
        metric_definition(&m, json, &d);
        if (d.kind[1] == 'a') cards++; else charts++;
        if (i) fputc(',', file);
        fprintf(file, "{\"kind\":\"%s\",\"metric\":", d.kind); json_string(file, d.name ? d.name : "");
        fprintf(file, ",\"document_type\":"); json_string(file, d.document_type ? d.document_type : "");
        fprintf(file, ",\"function\":"); json_string(file, d.sql_function ? d.sql_function : "");
        fprintf(file, ",\"of\":"); json_string(file, d.sql_function && strcmp(d.sql_function, "COUNT") && d.of ? d.of : "");
        fprintf(file, ",\"group_by\":"); json_string(file, d.group_by ? d.group_by : "");
        fprintf(file, ",\"over\":"); json_string(file, d.series && d.based_on ? d.based_on : "");
        fprintf(file, ",\"every\":"); json_string(file, d.series && d.interval ? d.interval : "");
        fprintf(file, ",\"filters\":%d,\"needs\":", m.filters); json_string(file, m.needs);
        fprintf(file, ",\"status\":\"%s\",\"missing_operation\":", m.missing[0] ? "unsupported" : "compiled");
        json_string(file, m.missing);
        fputc('}', file);
        if (m.missing[0]) count_missing(&missing, &missing_count, m.missing);
        else {
            compiled++;
            if (m.needs[0]) { waiting++; count_missing(&needs, &needs_count, m.needs); }
        }
        metric_free(&m);
        metric_definition_free(&d);
        free(json);
    }
    fprintf(file, "],\"counts\":{\"metrics\":%zu,\"cards\":%zu,\"charts\":%zu,\"compiled\":%zu,\"unsupported\":%zu,\"need_the_reader_to_say_something\":%zu},\"missing_operations\":[",
            files.count, cards, charts, compiled, files.count - compiled, waiting);
    for (size_t k = 0; k < missing_count; ++k) {
        fprintf(file, "%s{\"operation\":", k ? "," : ""); json_string(file, missing[k].operation);
        fprintf(file, ",\"definitions\":%zu}", missing[k].declarations);
    }
    fprintf(file, "],\"needs\":[");
    for (size_t k = 0; k < needs_count; ++k) {
        fprintf(file, "%s{\"reader_says\":", k ? "," : ""); json_string(file, needs[k].operation);
        fprintf(file, ",\"definitions\":%zu}", needs[k].declarations);
    }
    fprintf(file, "],\"week_starts\":\"Monday\",\"cannot_infer\":[");
    json_string(file, "a compiled metric -> a number; it is answered only against a store that carries the fields it reads");
    fputc(',', file);
    json_string(file, "a number read here -> the donor's number; the donor applies its own permissions and its own week");
    fprintf(file, "]}\n");
    if (file != stdout)
        printf("Metric plans: %zu definitions (%zu cards, %zu charts), %zu compiled, %zu unsupported\n",
               files.count, cards, charts, compiled, files.count - compiled);
    rc = ferror(file) ? -1 : 0;
done:
    if (file && file != stdout) fclose(file);
    for (size_t i = 0; i < files.count; ++i) free(files.paths[i]);
    free(files.paths); free(missing); free(needs);
    if (db) sqlite3_close(db);
    return rc;
}

/* A field name as a person would say it. */
static void metric_words(const char *field, char *out, size_t size) {
    size_t w = 0;
    for (const char *c = field ? field : ""; *c && w + 1 < size; ++c) out[w++] = *c == '_' ? ' ' : *c;
    out[w] = '\0';
}

/* What one compiled definition measures, in a sentence fragment. */
static void metric_said(const MetricDefinition *d, char *out, size_t size) {
    char of[160], by[160], over[160];
    const char *fn = d->sql_function ? d->sql_function : "";
    size_t w;
    metric_words(d->of, of, sizeof(of));
    metric_words(d->group_by, by, sizeof(by));
    metric_words(d->based_on, over, sizeof(over));
    if (!strcmp(fn, "COUNT") || !of[0]) w = (size_t)snprintf(out, size, "how many there are");
    else w = (size_t)snprintf(out, size, "the %s %s",
                              !strcmp(fn, "SUM") ? "total of" : !strcmp(fn, "AVG") ? "average" :
                              !strcmp(fn, "MIN") ? "smallest" : !strcmp(fn, "MAX") ? "largest" : "value of", of);
    if (w < size && by[0]) w += (size_t)snprintf(out + w, size - w, ", for each %s", by);
    if (w < size && d->series && over[0]) {
        const char *every = d->interval ? d->interval : "";
        snprintf(out + w, size - w, ", %s by %s",
                 every[0] == 'D' ? "each day" : every[0] == 'W' ? "each week" : every[0] == 'M' ? "each month" :
                 every[0] == 'Q' ? "each quarter" : every[0] == 'Y' ? "each year" : "over time", over);
    }
}

/* The same plans, one row per definition, beside the programs and conditions
 * the composer reads: what each record kind is counted by, and what is not
 * counted here and why. */
static int emit_metric_db(const char *path, const BfAppPack *pack, const char *app_path) {
    const char *app = pack && pack->schema && pack->schema->app_name ? pack->schema->app_name : "app";
    NotificationFiles files = {0};
    sqlite3 *db = NULL, *meta = NULL;
    sqlite3_stmt *ins = NULL, *del = NULL;
    size_t rows = 0;
    int rc = -1;
    if (sqlite3_open(path, &db) != SQLITE_OK || sqlite3_open(":memory:", &meta) != SQLITE_OK) goto done;
    if (surface_db_exec(db,
          "CREATE TABLE IF NOT EXISTS frappe_metric("
          " app TEXT NOT NULL, metric TEXT NOT NULL, kind TEXT NOT NULL, doctype TEXT NOT NULL,"
          " status TEXT NOT NULL, said TEXT, needs TEXT, missing_operation TEXT, definition TEXT,"
          " PRIMARY KEY(app, kind, metric));"
          "CREATE INDEX IF NOT EXISTS frappe_metric_doctype ON frappe_metric(doctype);") != 0) goto done;
    if (sqlite3_prepare_v2(db, "DELETE FROM frappe_metric WHERE app=?1", -1, &del, NULL) != SQLITE_OK) goto done;
    sqlite3_bind_text(del, 1, app, -1, SQLITE_STATIC);
    sqlite3_step(del);
    if (sqlite3_prepare_v2(db,
          "INSERT OR REPLACE INTO frappe_metric(app,metric,kind,doctype,status,said,needs,missing_operation,definition)"
          " VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)", -1, &ins, NULL) != SQLITE_OK) goto done;
    definition_walk(meta, app_path, &files, 0);
    if (files.count) qsort(files.paths, files.count, sizeof(char *), path_order);
    if (surface_db_exec(db, "BEGIN") != 0) goto done;
    for (size_t i = 0; i < files.count; ++i) {
        char *json = read_small_file(files.paths[i]);
        Metric m;
        MetricDefinition d;
        char said[600] = "";
        if (!json) continue;
        memset(&m, 0, sizeof(m));
        m.meta = meta;
        m.today = metric_today(meta, "2026-01-01");
        metric_definition(&m, json, &d);
        if (!m.missing[0]) metric_said(&d, said, sizeof(said));
        sqlite3_reset(ins);
        sqlite3_bind_text(ins, 1, app, -1, SQLITE_STATIC);
        sqlite3_bind_text(ins, 2, d.name ? d.name : "", -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(ins, 3, d.kind ? d.kind : "", -1, SQLITE_STATIC);
        sqlite3_bind_text(ins, 4, d.document_type ? d.document_type : "", -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(ins, 5, m.missing[0] ? "unsupported" : "compiled", -1, SQLITE_STATIC);
        sqlite3_bind_text(ins, 6, said, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(ins, 7, m.needs, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(ins, 8, m.missing, -1, SQLITE_TRANSIENT);
        /* The definition travels with its plan, so a reader needs the store
         * and not the application tree it was compiled from. */
        sqlite3_bind_text(ins, 9, m.missing[0] ? "" : json, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(ins) != SQLITE_DONE) {
            metric_free(&m); metric_definition_free(&d); free(json);
            surface_db_exec(db, "ROLLBACK");
            goto done;
        }
        rows++;
        metric_free(&m);
        metric_definition_free(&d);
        free(json);
    }
    if (surface_db_exec(db, "COMMIT") != 0) goto done;
    printf("  %s: %zu metric rows\n", app, rows);
    rc = 0;
done:
    if (rc != 0) fprintf(stderr, "Error: cannot write metrics into %s\n", path);
    for (size_t i = 0; i < files.count; ++i) free(files.paths[i]);
    free(files.paths);
    if (ins) sqlite3_finalize(ins);
    if (del) sqlite3_finalize(del);
    if (db) sqlite3_close(db);
    if (meta) sqlite3_close(meta);
    return rc;
}

/* One definition answered from one table of one store, opened read only. */
static int read_metric(const char *definition_path, const char *store_path, const char *table,
                       const char *today, char *const *params, size_t param_count, const char *kept) {
    char *json = strcmp(definition_path, "-") ? read_small_file(definition_path) : read_small_stream(stdin);
    sqlite3 *meta = NULL, *store = NULL;
    sqlite3_stmt *stmt = NULL;
    Metric m;
    MetricDefinition d;
    char sql[5000];
    int rc = 3;
    memset(&m, 0, sizeof(m));
    memset(&d, 0, sizeof(d));
    if (!json || !store_path || !plain_identifier(table) || sqlite3_open(":memory:", &meta) != SQLITE_OK ||
        sqlite3_open_v2(store_path, &store, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        fprintf(stderr, "Error: --read-metric needs a definition, --store and --table\n");
        rc = 1;
        goto done;
    }
    m.meta = meta; m.store = store; m.table = table; m.params = params; m.param_count = param_count; m.kept = kept;
    m.today = metric_today(meta, today);
    metric_definition(&m, json, &d);
    printf("{\"schema\":\"bonfyre.metric-reading.v1\",\"metric\":"); json_string(stdout, d.name ? d.name : "");
    printf(",\"kind\":\"%s\",\"table\":", d.kind ? d.kind : ""); json_string(stdout, table);
    if (m.missing[0] || m.not_carried[0] || m.needs[0]) {
        printf(",\"status\":\"refused\",\"missing_operation\":"); json_string(stdout, m.missing);
        printf(",\"not_carried\":"); json_string(stdout, m.not_carried);
        printf(",\"reader_must_say\":"); json_string(stdout, m.needs);
        printf("}\n");
        goto done;
    }
    if (d.series) {
        /* The donor's range: back from today by the span, to the start of that
         * period; then one value per period up to today. */
        CivilDate to = m.today, from, end;
        char text[16];
        double sum = 0, count = 0;
        int have = 0, first = 1, periods = 0;
        char row_date[16] = "";
        double row_sum = 0, row_count = 0;
        char grain = d.interval[0];
        if (!strcmp(d.timespan, "Select Date Range")) {
            if (sscanf(d.from_date, "%d-%d-%d", &from.y, &from.m, &from.d) != 3 || sscanf(d.to_date, "%d-%d-%d", &to.y, &to.m, &to.d) != 3) {
                printf(",\"status\":\"refused\",\"missing_operation\":\"timespan:Select Date Range\",\"not_carried\":\"\",\"reader_must_say\":\"\"}\n");
                goto done;
            }
        } else {
            from = !strcmp(d.timespan, "Last Week") ? add_days(to, -7) : !strcmp(d.timespan, "Last Month") ? add_months(to, -1)
                 : !strcmp(d.timespan, "Last Quarter") ? add_months(to, -3) : add_months(to, -12);
            from = grain == 'W' ? week_start(from) : grain == 'M' ? month_start(from) : grain == 'Q' ? quarter_start(from)
                 : grain == 'Y' ? (from.m = 1, from.d = 1, from) : from;
        }
        metric_where(&m, "date(\"%s\") BETWEEN ? AND ?", d.based_on);
        snprintf(text, sizeof(text), "%04d-%02d-%02d", from.y, from.m, from.d); metric_bind(&m, text, 0);
        snprintf(text, sizeof(text), "%04d-%02d-%02d", to.y, to.m, to.d); metric_bind(&m, text, 0);
        snprintf(sql, sizeof(sql), "SELECT date(\"%s\"), SUM(%s%s%s), COUNT(*) FROM \"%s\"%s GROUP BY 1 ORDER BY 1", d.based_on,
                 strcmp(d.sql_function, "COUNT") ? "\"" : "", strcmp(d.sql_function, "COUNT") ? d.of : "1",
                 strcmp(d.sql_function, "COUNT") ? "\"" : "", table, m.where);
        if (sqlite3_prepare_v2(store, sql, -1, &stmt, NULL) != SQLITE_OK) {
            printf(",\"status\":\"refused\",\"missing_operation\":\"store_read\",\"not_carried\":\"\",\"reader_must_say\":\"\"}\n");
            goto done;
        }
        for (int i = 0; i < m.bind_count; ++i) {
            if (m.numeric[i]) sqlite3_bind_double(stmt, i + 1, strtod(m.binds[i], NULL));
            else sqlite3_bind_text(stmt, i + 1, m.binds[i], -1, SQLITE_STATIC);
        }
        printf(",\"status\":\"read\",\"filters\":%d,\"every\":\"%s\",\"periods\":[", m.filters - 1, d.interval);
        have = sqlite3_step(stmt) == SQLITE_ROW;
        end = from;
        for (;;) {
            CivilDate start = periods ? add_days(end, 1) : from;
            char until[16];
            end = grain == 'D' ? start : grain == 'W' ? add_days(week_start(start), 6) : grain == 'M' ? month_end(start)
                : grain == 'Q' ? quarter_end(start) : (start.m = 12, start.d = 31, start);
            snprintf(until, sizeof(until), "%04d-%02d-%02d", end.y, end.m, end.d);
            sum = count = 0;
            while (have) {
                if (!row_date[0]) {
                    snprintf(row_date, sizeof(row_date), "%s", sqlite3_column_text(stmt, 0) ? (const char *)sqlite3_column_text(stmt, 0) : "");
                    row_sum = sqlite3_column_double(stmt, 1); row_count = sqlite3_column_double(stmt, 2);
                }
                if (strcmp(row_date, until) > 0) break;
                sum += row_sum; count += row_count;
                row_date[0] = '\0';
                have = sqlite3_step(stmt) == SQLITE_ROW;
            }
            printf("%s{\"until\":\"%s\",\"value\":%.15g}", first ? "" : ",", until,
                   !strcmp(d.sql_function, "COUNT") ? count : !strcmp(d.sql_function, "AVG") && count ? sum / count : sum);
            first = 0;
            if (++periods > 2000 || days_from_civil(end) >= days_from_civil(to)) break;
        }
        printf("],\"week_starts\":\"Monday\",\"cannot_infer\":\"a number read from this store -> the donor's number; the donor applies its own permissions, and this store holds only what was written to it\"}\n");
        rc = 0;
        goto done;
    }
    if (d.group_by)
        snprintf(sql, sizeof(sql), "SELECT \"%s\", %s(%s%s%s) AS n FROM \"%s\"%s GROUP BY \"%s\" ORDER BY n DESC",
                 d.group_by, d.sql_function, strcmp(d.sql_function, "COUNT") ? "\"" : "",
                 strcmp(d.sql_function, "COUNT") ? d.of : "1", strcmp(d.sql_function, "COUNT") ? "\"" : "",
                 table, m.where, d.group_by);
    else
        snprintf(sql, sizeof(sql), "SELECT %s(%s%s%s) FROM \"%s\"%s", d.sql_function,
                 strcmp(d.sql_function, "COUNT") ? "\"" : "", strcmp(d.sql_function, "COUNT") ? d.of : "*",
                 strcmp(d.sql_function, "COUNT") ? "\"" : "", table, m.where);
    if (sqlite3_prepare_v2(store, sql, -1, &stmt, NULL) != SQLITE_OK) {
        printf(",\"status\":\"refused\",\"missing_operation\":\"store_read\",\"not_carried\":\"\",\"reader_must_say\":\"\"}\n");
        goto done;
    }
    for (int i = 0; i < m.bind_count; ++i) {
        if (m.numeric[i]) sqlite3_bind_double(stmt, i + 1, strtod(m.binds[i], NULL));
        else sqlite3_bind_text(stmt, i + 1, m.binds[i], -1, SQLITE_STATIC);
    }
    printf(",\"status\":\"read\",\"filters\":%d,", m.filters);
    if (d.group_by) {
        int first = 1;
        printf("\"rows\":[");
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            printf("%s{\"label\":", first ? "" : ",");
            if (sqlite3_column_type(stmt, 0) == SQLITE_NULL) printf("null"); else json_string(stdout, (const char *)sqlite3_column_text(stmt, 0));
            printf(",\"value\":%.15g}", sqlite3_column_double(stmt, 1));
            first = 0;
        }
        printf("]");
    } else {
        /* A sum over no rows is nothing; the donor reports it as zero. */
        printf("\"value\":%.15g", sqlite3_step(stmt) == SQLITE_ROW ? sqlite3_column_double(stmt, 0) : 0.0);
    }
    printf(",\"week_starts\":\"Monday\",\"cannot_infer\":\"a number read from this store -> the donor's number; the donor applies its own permissions, and this store holds only what was written to it\"}\n");
    rc = 0;
done:
    if (stmt) sqlite3_finalize(stmt);
    metric_free(&m);
    metric_definition_free(&d);
    free(json);
    if (meta) sqlite3_close(meta);
    if (store) sqlite3_close(store);
    return rc;
}

static int notify_record(const char *directory, const char *document_type, const char *event, const char *doc,
                         const char *previous, const char *today, const char *not_carried,
                         const char *record_name, int queue_lines) {
    NotificationFiles files = {0};
    sqlite3 *db = NULL;
    char *named = NULL;
    if (!document_type || !event || !doc || sqlite3_open(":memory:", &db) != SQLITE_OK) {
        fprintf(stderr, "Error: --notify needs --document-type, --event and --doc\n");
        if (db) sqlite3_close(db);
        return 1;
    }
    /* The donor's templates call the record by its name. */
    if (record_name) {
        sqlite3_stmt *stmt = NULL;
        if (sqlite3_prepare_v2(db, "SELECT CASE WHEN json_type(?1,'$.name') IS NULL THEN json_set(?1,'$.name',?2) ELSE ?1 END",
                               -1, &stmt, NULL) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, doc, -1, SQLITE_STATIC);
            sqlite3_bind_text(stmt, 2, record_name, -1, SQLITE_STATIC);
            if (sqlite3_step(stmt) == SQLITE_ROW && sqlite3_column_text(stmt, 0))
                named = strdup((const char *)sqlite3_column_text(stmt, 0));
            sqlite3_finalize(stmt);
        }
        if (named) doc = named;
    }
    notification_walk(db, directory, &files, 0);
    if (files.count) qsort(files.paths, files.count, sizeof(char *), path_order);
    for (size_t i = 0; i < files.count; ++i) {
        char *json = read_small_file(files.paths[i]);
        char *kind = json ? json_field(db, json, "$.document_type") : NULL;
        if (kind && !strcmp(kind, document_type)) {
            if (queue_lines) {
                char *held = NULL, key[65];
                size_t held_len = 0;
                int composed = 0;
                FILE *mem = open_memstream(&held, &held_len);
                if (mem) {
                    prepare_notification(mem, files.paths[i], event, doc, previous, today, not_carried, key, &composed);
                    fclose(mem);
                    /* One line each: the message ends in a newline of its own. */
                    if (composed && held) printf("%s\t%s", key, held);
                }
                free(held);
            } else {
                prepare_notification(stdout, files.paths[i], event, doc, previous, today, not_carried, NULL, NULL);
            }
        }
        free(kind); free(json);
        free(files.paths[i]);
    }
    free(files.paths);
    free(named);
    sqlite3_close(db);
    return 0;
}

static void json_pack_revision(FILE *file, const BfAppPack *pack) {
    static const char digits[] = "0123456789abcdef";
    char revision[65];
    for (size_t index = 0; index < 32; ++index) {
        revision[index * 2] = digits[pack->pack_hash[index] >> 4];
        revision[index * 2 + 1] = digits[pack->pack_hash[index] & 15];
    }
    revision[64] = '\0';
    json_string(file, revision);
}

static void emit_schema_graph(FILE *file, const BfAppPack *pack) {
    fprintf(file, "\"schema\":{\"app\":"); json_string(file, pack->schema ? pack->schema->app_name : pack->app_name);
    fprintf(file, ",\"doctypes\":[");
    for (size_t i = 0; pack->schema && i < pack->schema->doctype_count; i++) {
        BfDocType *doctype = pack->schema->doctypes[i];
        if (i) fputc(',', file);
        fprintf(file, "{\"name\":"); json_string(file, doctype->name);
        fprintf(file, ",\"module\":"); json_string(file, doctype->module);
        fprintf(file, ",\"controller\":"); json_string(file, doctype->controller_path);
        fprintf(file, ",\"fields\":[");
        for (size_t j = 0; j < doctype->field_count; j++) {
            BfField *field = &doctype->fields[j];
            if (j) fputc(',', file);
            fprintf(file, "{\"name\":"); json_string(file, field->fieldname);
            fprintf(file, ",\"label\":"); json_string(file, field->label);
            fprintf(file, ",\"type\":%d,\"required\":%s,\"options\":", (int)field->fieldtype, field->reqd ? "true" : "false");
            json_string(file, field->options); fputc('}', file);
        }
        fprintf(file, "],\"relations\":[");
        for (size_t j = 0; j < doctype->relation_count; j++) {
            BfRelation *relation = &doctype->relations[j];
            if (j) fputc(',', file);
            fprintf(file, "{\"type\":%d,\"source\":", (int)relation->type); json_string(file, relation->source_doctype);
            fprintf(file, ",\"field\":"); json_string(file, relation->source_field);
            fprintf(file, ",\"target\":"); json_string(file, relation->target_doctype); fputc('}', file);
        }
        fprintf(file, "]}");
    }
    fprintf(file, "]}");
}

static void emit_bindings(FILE *file, const BfAppPack *pack) {
    fprintf(file, "\"bindings\":[");
    for (size_t i = 0; i < pack->binding_count; i++) {
        BfBinding *binding = pack->bindings[i];
        if (i) fputc(',', file);
        fprintf(file, "{\"doctype\":"); json_string(file, binding->doctype);
        fprintf(file, ",\"capabilities\":[");
        for (size_t j = 0; j < binding->capability_count; j++) { if (j) fputc(',', file); json_string(file, binding->capability_ids[j]); }
        fprintf(file, "]}");
    }
    fprintf(file, "]");
}

static void emit_rule_universes(FILE *file, const BfAppPack *pack) {
    fprintf(file, "\"rule_universes\":[");
    for (size_t i = 0; i < pack->rule_universe_count; i++) {
        BfRuleUniverse *rules = pack->rule_universes[i];
        if (i) fputc(',', file);
        fprintf(file, "{\"doctype\":"); json_string(file, rules->doctype);
        fprintf(file, ",\"proof_policy\":"); json_string(file, rules->proof_policy);
        fprintf(file, ",\"activation_policy\":"); json_string(file, rules->activation_policy);
        fprintf(file, ",\"operators\":[");
        for (size_t j = 0; j < rules->operator_count; j++) { if (j) fputc(',', file); json_string(file, rules->operators[j]); }
        fprintf(file, "]}");
    }
    fprintf(file, "]");
}

static int emit_projection(const char *path, const char *kind, const BfAppPack *pack) {
    FILE *file = fopen(path, "wb");
    if (!file) {
        fprintf(stderr, "Error: cannot emit %s to %s\n", kind, path);
        return -1;
    }
    fprintf(file, "{\"kind\":"); json_string(file, kind);
    fprintf(file, ",\"app\":"); json_string(file, pack->app_name);
    fprintf(file, ",\"version\":"); json_string(file, pack->app_version);
    fprintf(file, ",\"source_revision\":"); json_pack_revision(file, pack);
    fputc(',', file);
    if (!strcmp(kind, "BfSchemaGraph")) emit_schema_graph(file, pack);
    else if (!strcmp(kind, "CapabilityBindings")) emit_bindings(file, pack);
    else if (!strcmp(kind, "RuleUniverses")) emit_rule_universes(file, pack);
    else { emit_schema_graph(file, pack); fputc(',', file); emit_bindings(file, pack); fputc(',', file); emit_rule_universes(file, pack); }
    fprintf(file, ",\"routes\":[");
    for (size_t i = 0; i < pack->route_count; i++) { if (i) fputc(',', file); json_string(file, pack->api_routes[i]); }
    fprintf(file, "],\"surfaces\":[");
    for (size_t i = 0; i < pack->surface_count; i++) { if (i) fputc(',', file); json_string(file, pack->surfaces[i]); }
    fprintf(file, "],\"migrations\":[");
    for (size_t i = 0; i < pack->migration_count; i++) { if (i) fputc(',', file); json_string(file, pack->migrations[i]); }
    fprintf(file, "],\"coverage_score\":%.6f}\n", pack->coverage_score);
    int ok = ferror(file) ? -1 : 0;
    if (fclose(file) != 0) ok = -1;
    if (ok != 0) fprintf(stderr, "Error: failed writing %s to %s\n", kind, path);
    return ok;
}

/*
 * CMS is a declarative record surface, not a Frappe runtime.  This projection
 * deliberately moves only fields that can be represented without executing a
 * controller, importing a site, copying a credential, or pretending that a
 * Link/Table has become a resolved Bonfyre relationship.  The remaining
 * grammar is retained in the AppPack and can be lifted through an explicit
 * relation/authority bridge later.
 */
static int cms_field_is_layout(BfFieldType type) {
    return type == BF_TYPE_UNIT || type == BF_TYPE_BUTTON ||
           type == BF_TYPE_COLUMN_BREAK || type == BF_TYPE_HEADING ||
           type == BF_TYPE_SECTION_BREAK || type == BF_TYPE_TAB_BREAK ||
           type == BF_TYPE_ICON || type == BF_TYPE_IMAGE ||
           type == BF_TYPE_READ_ONLY;
}

static int cms_field_is_secret(const BfField *field) {
    const char *name = field && field->fieldname ? field->fieldname : "";
    char lowered[256];
    size_t length = strlen(name);
    if (length >= sizeof(lowered)) length = sizeof(lowered) - 1;
    for (size_t i = 0; i < length; ++i) lowered[i] = (char)tolower((unsigned char)name[i]);
    lowered[length] = '\0';
    return (field && (field->hidden || field->fieldtype == BF_TYPE_PASSWORD)) ||
           strstr(lowered, "password") || strstr(lowered, "token") ||
           strstr(lowered, "secret") || strstr(lowered, "api_key") ||
           strstr(lowered, "access_key") || strstr(lowered, "private_key");
}

static int cms_field_is_structural(BfFieldType type) {
    return type == BF_TYPE_TABLE || type == BF_TYPE_TABLE_MULTISELECT ||
           type == BF_TYPE_DYNAMIC_LINK;
}

static const char *cms_field_type(const BfField *field) {
    switch (field->fieldtype) {
        case BF_TYPE_INT:
        case BF_TYPE_DURATION:
        case BF_TYPE_RATING:
            return "integer";
        case BF_TYPE_FLOAT:
        case BF_TYPE_CURRENCY:
        case BF_TYPE_CURRENCY_DEPENDENT:
            return "decimal";
        case BF_TYPE_CHECK:
            return "boolean";
        case BF_TYPE_DATE:
        case BF_TYPE_DATETIME:
        case BF_TYPE_TIME:
            return "datetime";
        case BF_TYPE_TEXT:
        case BF_TYPE_LONG_TEXT:
        case BF_TYPE_SMALL_TEXT:
            return "text";
        case BF_TYPE_MARKDOWN:
        case BF_TYPE_HTML:
        case BF_TYPE_TEXT_EDITOR:
        case BF_TYPE_CODE:
            return "richtext";
        case BF_TYPE_JSON:
        case BF_TYPE_GEOLOCATION:
            return "json";
        case BF_TYPE_SELECT:
            return (field->options && field->options[0]) ? "enumeration" : "string";
        /* A Frappe Link remains an opaque reference value at this boundary.
         * Claiming it is a resolved relation would counterfeit referential
         * integrity before the Authority/Relation bridge has run. */
        case BF_TYPE_LINK:
        case BF_TYPE_ATTACH:
        case BF_TYPE_ATTACH_IMAGE:
        case BF_TYPE_SIGNATURE:
        case BF_TYPE_BARCODE:
        case BF_TYPE_COLOR:
        case BF_TYPE_DATA:
        case BF_TYPE_STRING:
        default:
            return "string";
    }
}

static void cms_schema_name(char out[128], const char *app, const char *doctype) {
    size_t pos = 0;
    const char *parts[] = { "frappe_", app ? app : "app", "_", doctype ? doctype : "doctype", NULL };
    for (size_t part = 0; parts[part] && pos + 1 < 128; ++part) {
        for (const unsigned char *p = (const unsigned char *)parts[part]; *p && pos + 1 < 128; ++p) {
            out[pos++] = isalnum(*p) ? (char)tolower(*p) : '_';
        }
    }
    while (pos > 1 && out[pos - 1] == '_') --pos;
    out[pos] = '\0';
}

static void emit_cms_enum(bf_jsonw *w, const char *options) {
    bf_jsonw_key(w, "enum"); bf_jsonw_arr(w);
    if (options) {
        const char *cursor = options;
        while (*cursor) {
            const char *end = cursor;
            while (*end && *end != '\n' && *end != '\r') ++end;
            const char *next = *end ? end + 1 : end;
            while (cursor < end && isspace((unsigned char)*cursor)) ++cursor;
            while (end > cursor && isspace((unsigned char)end[-1])) --end;
            if (end > cursor) bf_jsonw_strn(w, cursor, (size_t)(end - cursor));
            cursor = next;
        }
    }
    bf_jsonw_arr_end(w);
}

static BfDocType *find_doctype(const BfAppPack *pack, const char *name) {
    if (!pack || !pack->schema || !name) return NULL;
    for (size_t i = 0; i < pack->schema->doctype_count; ++i) {
        BfDocType *doctype = pack->schema->doctypes[i];
        if (doctype && doctype->name && strcmp(doctype->name, name) == 0) return doctype;
    }
    return NULL;
}

static int cms_field_emitted(const BfField *field) {
    if (!field->fieldname || !field->fieldname[0] || cms_field_is_layout(field->fieldtype) ||
        cms_field_is_secret(field) || cms_field_is_structural(field->fieldtype)) return 0;
    return strcmp(field->fieldname, "id") && strcmp(field->fieldname, "namespace") &&
           strcmp(field->fieldname, "created_at") && strcmp(field->fieldname, "updated_at") &&
           strcmp(field->fieldname, "published_at");
}

static const char *cms_field_name(const BfField *field) {
    return !strcmp(field->fieldname, "status") ? "frappe_status" : field->fieldname;
}

/* A conditional requirement travels into a CMS type only when the store can
 * evaluate it from the entry alone: compiled, not constant, and reading only
 * attributes this schema carries. Otherwise the reason it stayed behind. */
static const char *cms_required_when_withheld(const BfDocType *doctype, const char *source) {
    BfCondPlan plan;
    const char *why = NULL;
    bf_cond_compile(source, &plan);
    if (!plan.compiled) why = "the rule uses an operation this estate does not evaluate";
    else if (plan.constant[0]) why = "the rule has one answer for every record";
    else if (plan.parent_read_count || plan.env_read_count) why = "the rule reads beyond the entry itself";
    for (size_t r = 0; !why && r < plan.read_count; ++r) {
        if (!strcmp(plan.reads[r], "status")) {
            why = "the rule reads donor status, which is carried under a different name";
            break;
        }
        int carried = 0;
        for (size_t i = 0; i < doctype->field_count && !carried; ++i)
            carried = doctype->fields[i].fieldname && !strcmp(doctype->fields[i].fieldname, plan.reads[r]) &&
                      cms_field_emitted(&doctype->fields[i]);
        if (!carried) why = "the rule reads a field this content type does not carry";
    }
    bf_cond_plan_free(&plan);
    return why;
}

static int emit_cms_schema(const char *path, const BfAppPack *pack, const char *doctype_name) {
    BfDocType *doctype = find_doctype(pack, doctype_name);
    if (!doctype) {
        fprintf(stderr, "Error: DocType not found in compiled app: %s\n", doctype_name ? doctype_name : "");
        return -1;
    }
    if (doctype->is_child || doctype->istable) {
        fprintf(stderr, "Error: child DocType %s cannot be emitted as a top-level CMS type\n", doctype->name);
        return -1;
    }
    int has_status = 0, has_alias = 0;
    for (size_t i = 0; i < doctype->field_count; ++i) {
        const BfField *field = &doctype->fields[i];
        if (!cms_field_emitted(field)) continue;
        if (!strcmp(field->fieldname, "status")) has_status = 1;
        if (!strcmp(field->fieldname, "frappe_status")) has_alias = 1;
    }
    if (has_status && has_alias) {
        fprintf(stderr, "Error: DocType %s has both status and frappe_status; the CMS projection would collide\n", doctype->name);
        return -1;
    }
    char name[128];
    cms_schema_name(name, pack->schema ? pack->schema->app_name : pack->app_name, doctype->name);
    bf_jsonw w; bf_jsonw_init(&w, 0); bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "name"); bf_jsonw_str(&w, name);
    bf_jsonw_key(&w, "displayName"); bf_jsonw_str(&w, doctype->name);
    bf_jsonw_key(&w, "description"); bf_jsonw_str(&w, "Safe declarative projection of compiled Frappe metadata. It does not execute controllers, write to a Frappe site, resolve references, expose credentials, or grant authority.");
    bf_jsonw_key(&w, "bonfyre"); bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "projection"); bf_jsonw_str(&w, "frappe-cms-schema.v1");
    bf_jsonw_key(&w, "source_app"); bf_jsonw_str(&w, pack->schema ? pack->schema->app_name : pack->app_name);
    bf_jsonw_key(&w, "source_doctype"); bf_jsonw_str(&w, doctype->name);
    bf_jsonw_key(&w, "forbidden_inference"); bf_jsonw_str(&w, "A CMS record is not a Frappe document, controller execution, resolved relation, external write right, or authority grant.");
    if (has_status) {
        bf_jsonw_key(&w, "field_aliases"); bf_jsonw_obj(&w);
        bf_jsonw_key(&w, "status"); bf_jsonw_str(&w, "frappe_status");
        bf_jsonw_obj_end(&w);
    }
    bf_jsonw_key(&w, "conditions_withheld"); bf_jsonw_arr(&w);
    for (size_t i = 0; i < doctype->field_count; ++i) {
        const BfField *field = &doctype->fields[i];
        if (!cms_field_emitted(field) || !field->mandatory_depends_on_expr || !field->mandatory_depends_on_expr[0]) continue;
        const char *why = cms_required_when_withheld(doctype, field->mandatory_depends_on_expr);
        if (!why) continue;
        bf_jsonw_obj(&w); bf_jsonw_key(&w, "field"); bf_jsonw_str(&w, field->fieldname);
        bf_jsonw_key(&w, "reason"); bf_jsonw_str(&w, why); bf_jsonw_obj_end(&w);
    }
    bf_jsonw_arr_end(&w); bf_jsonw_obj_end(&w);
    bf_jsonw_key(&w, "attributes"); bf_jsonw_obj(&w);
    for (size_t i = 0; i < doctype->field_count; ++i) {
        const BfField *field = &doctype->fields[i];
        if (!cms_field_emitted(field)) continue;
        bf_jsonw_key(&w, cms_field_name(field)); bf_jsonw_obj(&w);
        bf_jsonw_key(&w, "type"); bf_jsonw_str(&w, cms_field_type(field));
        bf_jsonw_key(&w, "required"); bf_jsonw_bool(&w, field->reqd);
        if (field->mandatory_depends_on_expr && field->mandatory_depends_on_expr[0] &&
            !cms_required_when_withheld(doctype, field->mandatory_depends_on_expr)) {
            bf_jsonw_key(&w, "requiredWhen"); bf_jsonw_str(&w, field->mandatory_depends_on_expr);
        }
        if (field->fieldtype == BF_TYPE_SELECT && field->options && field->options[0]) emit_cms_enum(&w, field->options);
        if (field->fieldtype == BF_TYPE_LINK && field->options && field->options[0]) {
            bf_jsonw_key(&w, "frappe_reference_doctype"); bf_jsonw_str(&w, field->options);
        }
        if (field->label && field->label[0]) { bf_jsonw_key(&w, "label"); bf_jsonw_str(&w, field->label); }
        bf_jsonw_obj_end(&w);
    }
    bf_jsonw_obj_end(&w); bf_jsonw_obj_end(&w);
    if (w.oom) { bf_jsonw_free(&w); fprintf(stderr, "Error: CMS schema allocation failed\n"); return -1; }
    FILE *file = fopen(path, "wb");
    if (!file) { bf_jsonw_free(&w); fprintf(stderr, "Error: cannot emit CMS schema to %s\n", path); return -1; }
    int ok = fwrite(w.buf, 1, w.len, file) == w.len && fputc('\n', file) != EOF ? 0 : -1;
    bf_jsonw_free(&w);
    if (fclose(file) != 0) ok = -1;
    if (ok != 0) fprintf(stderr, "Error: failed writing CMS schema to %s\n", path);
    return ok;
}

int main(int argc, char **argv) {
    char *app_path = NULL;
    char *output_dir = "generated/projections/frappe";
    char *bonfyre_root = NULL;
    char *emit_schema_ir = NULL;
    char *emit_bindings = NULL;
    char *emit_rule_universes = NULL;
    char *emit_pack = NULL;
    char *emit_cms_schema_path = NULL;
    char *cms_doctype = NULL;
    char *emit_lineage_catalog_path = NULL;
    char *emit_interaction_fingerprints_path = NULL;
    char *emit_surface_programs_path = NULL;
    char *emit_surface_db_path = NULL;
    char *emit_condition_plans_path = NULL;
    char *eval_condition_source = NULL;
    char *eval_doc = NULL;
    char *eval_parent_doc = NULL;
    char *eval_env = NULL;
    char *emit_notification_plans_path = NULL;
    char *prepare_notification_path = NULL;
    char *notification_event = NULL;
    char *previous_doc = NULL;
    char *today = NULL;
    char *notify_path = NULL;
    char *document_type = NULL;
    char *not_carried = NULL;
    char *record_name = NULL;
    int queue_lines = 0;
    char *emit_metric_plans_path = NULL;
    char *read_metric_path = NULL;
    char *metric_store = NULL;
    char *metric_table = NULL;
    char *metric_params[16];
    size_t metric_param_count = 0;
    int max_phase = 12;
    bool dry_run = false;
    bool verbose __attribute__((unused)) = false;

    /* Parse options */
    static struct option long_options[] = {
        {"output", required_argument, 0, 'o'},
        {"root", required_argument, 0, 'r'},
        {"dry-run", no_argument, 0, 1},
        {"emit-schema-ir", required_argument, 0, 2},
        {"emit-bindings", required_argument, 0, 3},
        {"emit-rule-universes", required_argument, 0, 4},
        {"emit-pack", required_argument, 0, 5},
        {"phase", required_argument, 0, 6},
        {"emit-lineage-catalog", required_argument, 0, 7},
        {"emit-cms-schema", required_argument, 0, 8},
        {"cms-doctype", required_argument, 0, 9},
        {"emit-interaction-fingerprints", required_argument, 0, 10},
        {"emit-surface-programs", required_argument, 0, 11},
        {"emit-surface-db", required_argument, 0, 12},
        {"emit-condition-plans", required_argument, 0, 13},
        {"eval-condition", required_argument, 0, 14},
        {"doc", required_argument, 0, 15},
        {"parent-doc", required_argument, 0, 16},
        {"env", required_argument, 0, 17},
        {"emit-notification-plans", required_argument, 0, 18},
        {"prepare-notification", required_argument, 0, 19},
        {"event", required_argument, 0, 20},
        {"previous-doc", required_argument, 0, 21},
        {"today", required_argument, 0, 22},
        {"notify", required_argument, 0, 23},
        {"document-type", required_argument, 0, 24},
        {"not-carried", required_argument, 0, 25},
        {"name", required_argument, 0, 26},
        {"queue-lines", no_argument, 0, 27},
        {"emit-metric-plans", required_argument, 0, 28},
        {"read-metric", required_argument, 0, 29},
        {"store", required_argument, 0, 30},
        {"table", required_argument, 0, 31},
        {"param", required_argument, 0, 32},
        {"verbose", no_argument, 0, 'v'},
        {"help", no_argument, 0, 'h'},
        {0, 0, 0, 0}
    };

    int opt;
    int option_index = 0;

    while ((opt = getopt_long(argc, argv, "o:r:vh", long_options, &option_index)) != -1) {
        switch (opt) {
            case 'o':
                output_dir = optarg;
                break;
            case 'r':
                bonfyre_root = optarg;
                break;
            case 'v':
                verbose = true;
                break;
            case 'h':
                print_usage(argv[0]);
                return 0;
            case 1:  /* --dry-run */
                dry_run = true;
                break;
            case 2:  /* --emit-schema-ir */
                emit_schema_ir = optarg;
                break;
            case 3:  /* --emit-bindings */
                emit_bindings = optarg;
                break;
            case 4:  /* --emit-rule-universes */
                emit_rule_universes = optarg;
                break;
            case 5:  /* --emit-pack */
                emit_pack = optarg;
                break;
            case 6:  /* --phase */
                max_phase = atoi(optarg);
                if (max_phase < 0 || max_phase > 12) {
                    fprintf(stderr, "Error: Phase must be 0-12\n");
                    return 1;
                }
                break;
            case 7:  /* --emit-lineage-catalog */
                emit_lineage_catalog_path = optarg;
                break;
            case 8:  /* --emit-cms-schema */
                emit_cms_schema_path = optarg;
                break;
            case 9:  /* --cms-doctype */
                cms_doctype = optarg;
                break;
            case 10: /* --emit-interaction-fingerprints */
                emit_interaction_fingerprints_path = optarg;
                break;
            case 11: /* --emit-surface-programs */
                emit_surface_programs_path = optarg;
                break;
            case 12: /* --emit-surface-db */
                emit_surface_db_path = optarg;
                break;
            case 13: /* --emit-condition-plans */
                emit_condition_plans_path = optarg;
                break;
            case 14: /* --eval-condition */
                eval_condition_source = optarg;
                break;
            case 15: /* --doc */
                eval_doc = optarg;
                break;
            case 16: /* --parent-doc */
                eval_parent_doc = optarg;
                break;
            case 17: /* --env */
                eval_env = optarg;
                break;
            case 18: emit_notification_plans_path = optarg; break;
            case 19: prepare_notification_path = optarg; break;
            case 20: notification_event = optarg; break;
            case 21: previous_doc = optarg; break;
            case 22: today = optarg; break;
            case 23: notify_path = optarg; break;
            case 24: document_type = optarg; break;
            case 25: not_carried = optarg; break;
            case 26: record_name = optarg; break;
            case 27: queue_lines = 1; break;
            case 28: emit_metric_plans_path = optarg; break;
            case 29: read_metric_path = optarg; break;
            case 30: metric_store = optarg; break;
            case 31: metric_table = optarg; break;
            case 32: if (metric_param_count < 16) metric_params[metric_param_count++] = optarg; break;
            default:
                print_usage(argv[0]);
                return 1;
        }
    }

    if (emit_lineage_catalog_path && emit_lineage_catalog(emit_lineage_catalog_path) != 0) {
        return 1;
    }

    /* Preparing one held message reads one definition and one record. */
    if (prepare_notification_path)
        return prepare_notification(stdout, prepare_notification_path, notification_event, eval_doc, previous_doc, today, not_carried, NULL, NULL);

    /* One metric answered from one table of one store. */
    if (read_metric_path)
        return read_metric(read_metric_path, metric_store, metric_table, today, metric_params, metric_param_count, not_carried);

    /* Every definition for one kind of record, against one event. */
    if (notify_path)
        return notify_record(notify_path, document_type, notification_event, eval_doc, previous_doc, today,
                             not_carried, record_name, queue_lines);

    /* Evaluating one condition reads no application either. */
    if (eval_condition_source) return eval_condition(eval_condition_source, eval_doc, eval_parent_doc, eval_env);

    /* Catalog inspection is a complete read-only operation and needs no app. */
    if (optind >= argc && emit_lineage_catalog_path) return 0;

    /* Get app path */
    if (optind >= argc) {
        fprintf(stderr, "Error: Missing app_path argument\n\n");
        print_usage(argv[0]);
        return 1;
    }

    app_path = argv[optind];

    if (dry_run && (emit_schema_ir || emit_bindings || emit_rule_universes || emit_pack || emit_cms_schema_path || emit_interaction_fingerprints_path ||
                     emit_surface_programs_path || emit_surface_db_path || emit_condition_plans_path || emit_notification_plans_path || emit_metric_plans_path)) {
        fprintf(stderr, "Error: --dry-run cannot be combined with --emit-* because it must not write artifacts\n");
        return 1;
    }

    /* Determine bonfyre_root */
    if (!bonfyre_root) {
        bonfyre_root = getenv("BONFYRE_ROOT");
        if (!bonfyre_root) {
            bonfyre_root = "..";
        }
    }

    /* Print configuration */
    printf("═══════════════════════════════════════════════════════════════\n");
    printf("  BonfyreFrappeCompiler — Universal Uplift Runtime\n");
    printf("═══════════════════════════════════════════════════════════════\n");
    printf("\n");
    printf("Configuration:\n");
    printf("  App path:      %s\n", app_path);
    printf("  Bonfyre root:  %s\n", bonfyre_root);
    printf("  Output dir:    %s\n", output_dir);
    printf("  Max phase:     %d\n", max_phase);
    printf("  Dry run:       %s\n", dry_run ? "yes" : "no");
    printf("\n");

    /* Check app path exists */
    if (access(app_path, R_OK) != 0) {
        fprintf(stderr, "Error: Cannot access app path: %s\n", app_path);
        return 1;
    }

    /* Run compilation */
    setenv("BONFYRE_FRAPPE_OUTPUT_DIR", output_dir, 1);
    int error_code = 0;
    BfAppPack *pack = bf_compile_app_to_phase(app_path, bonfyre_root, max_phase, &error_code);

    if (error_code != 0) {
        fprintf(stderr, "\n✗ Compilation failed with error code: %d\n", error_code);
        return 1;
    }

    if (!pack) {
        fprintf(stderr, "\n✗ Compilation failed: pack is NULL\n");
        return 1;
    }

    /* Emit outputs if requested */
    if (emit_schema_ir) {
        printf("\nEmitting schema IR to: %s\n", emit_schema_ir);
        if (emit_projection(emit_schema_ir, "BfSchemaGraph", pack) != 0) { bf_app_pack_free(pack); return 1; }
    }

    if (emit_bindings) {
        printf("\nEmitting bindings to: %s\n", emit_bindings);
        if (emit_projection(emit_bindings, "CapabilityBindings", pack) != 0) { bf_app_pack_free(pack); return 1; }
    }

    if (emit_rule_universes) {
        printf("\nEmitting rule universes to: %s\n", emit_rule_universes);
        if (emit_projection(emit_rule_universes, "RuleUniverses", pack) != 0) { bf_app_pack_free(pack); return 1; }
    }

    if (emit_pack) {
        printf("\nEmitting app pack to: %s\n", emit_pack);
        if (emit_projection(emit_pack, "AppPack", pack) != 0) { bf_app_pack_free(pack); return 1; }
    }

    if (emit_cms_schema_path) {
        if (!cms_doctype || !cms_doctype[0]) {
            fprintf(stderr, "Error: --emit-cms-schema requires --cms-doctype NAME\n");
            bf_app_pack_free(pack);
            return 1;
        }
        printf("\nEmitting native CMS schema for %s to: %s\n", cms_doctype, emit_cms_schema_path);
        if (emit_cms_schema(emit_cms_schema_path, pack, cms_doctype) != 0) { bf_app_pack_free(pack); return 1; }
    }

    if (emit_interaction_fingerprints_path) {
        printf("\nEmitting interaction fingerprints to: %s\n", emit_interaction_fingerprints_path);
        if (emit_interaction_fingerprints(emit_interaction_fingerprints_path, pack) != 0) {
            bf_app_pack_free(pack);
            return 1;
        }
    }

    if (emit_metric_plans_path && emit_metric_plans(emit_metric_plans_path, app_path) != 0) {
        bf_app_pack_free(pack);
        return 1;
    }

    if (emit_notification_plans_path && emit_notification_plans(emit_notification_plans_path, app_path) != 0) {
        bf_app_pack_free(pack);
        return 1;
    }

    if (emit_condition_plans_path) {
        if (strcmp(emit_condition_plans_path, "-"))
            printf("\nEmitting condition plans to: %s\n", emit_condition_plans_path);
        if (emit_condition_plans(emit_condition_plans_path, pack) != 0) {
            bf_app_pack_free(pack);
            return 1;
        }
    }

    if (emit_surface_db_path) {
        printf("\nWriting surface programs into: %s\n", emit_surface_db_path);
        if (emit_surface_db(emit_surface_db_path, pack) != 0 ||
            emit_condition_db(emit_surface_db_path, pack) != 0 ||
            emit_metric_db(emit_surface_db_path, pack, app_path) != 0) {
            bf_app_pack_free(pack);
            return 1;
        }
    }

    if (emit_surface_programs_path) {
        if (strcmp(emit_surface_programs_path, "-"))
            printf("\nEmitting surface programs to: %s\n", emit_surface_programs_path);
        if (emit_surface_programs(emit_surface_programs_path, pack) != 0) {
            bf_app_pack_free(pack);
            return 1;
        }
    }

    /* Success */
    printf("\n═══════════════════════════════════════════════════════════════\n");
    printf("  ✓ Compilation Complete\n");
    printf("═══════════════════════════════════════════════════════════════\n");
    printf("\n");
    printf("Summary:\n");
    printf("  App:           %s\n", pack->schema ? pack->schema->app_name : "unknown");
    printf("  DocTypes:      %zu\n", pack->schema ? pack->schema->doctype_count : 0);
    printf("  Bindings:      %zu\n", pack->binding_count);
    printf("  Families:      %zu\n", pack->family_count);
    printf("  Relations:     %zu\n", pack->relation_count);
    printf("  Rule universes: %zu\n", pack->rule_universe_count);
    printf("  Routes:        %zu\n", pack->route_count);
    printf("  Surfaces:      %zu\n", pack->surface_count);
    printf("  Coverage:      %.1f%%\n", pack->coverage_score * 100.0);
    printf("\n");

    /* Cleanup */
    bf_app_pack_free(pack);

    return 0;
}
