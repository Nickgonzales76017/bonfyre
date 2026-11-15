#ifndef BONFYRE_UI_TREE_H
#define BONFYRE_UI_TREE_H
/* The shared markup builders, next to form_tree.h (controls) and the
 * SurfaceScene component vocabulary (authoring_native_components.c). A page
 * hands data to one of these and never prints the tag itself: each builder
 * escapes every value it writes and emits the class vocabulary that
 * deploy/tailwind/styles/aurekai-ui.pcss already styles (.ak-pattern, ak-*,
 * dui-*). Layout comes only from the numeric presentation vocabulary
 * declared in schemas/control/numeric-presentation.cue (UiLayout).
 *
 * Builders write the bytes the hand-printed markup they replace wrote, so
 * byte-checked tests keep passing. They are presentation only: none of them
 * reads a store, decides authority or picks a route. */
#include <stdio.h>
#include <string.h>
#include "../../BonfyreLex/src/bf_lex.h"
#include "../../BonfyreSurfaceScene/generated/native_numeric_presentation.h"

#define UI_UNUSED __attribute__((unused))

/* <tag> or <tag class="cls">; tag is a literal, cls is escaped. */
static UI_UNUSED void ui_open(FILE *o, const char *tag, const char *cls) {
    fputc('<', o); fputs(tag, o);
    if (cls) { fputs(" class=\"", o); bf_lex_html_emit(o, cls); fputc('"', o); }
    fputc('>', o);
}
static UI_UNUSED void ui_close(FILE *o, const char *tag) { fputs("</", o); fputs(tag, o); fputc('>', o); }

/* One escaped text element: <tag class="cls">text</tag>. */
static UI_UNUSED void ui_text(FILE *o, const char *tag, const char *cls, const char *text) {
    ui_open(o, tag, cls); bf_lex_html_emit(o, text); ui_close(o, tag);
}
/* The same, only when there is text to show. */
static UI_UNUSED void ui_text_if(FILE *o, const char *tag, const char *cls, const char *text) {
    if (text && *text) ui_text(o, tag, cls, text);
}

/* <a href="href" class="cls">label</a>; href and label escaped. */
static UI_UNUSED void ui_link(FILE *o, const char *href, const char *cls, const char *label) {
    fputs("<a href=\"", o); bf_lex_html_emit(o, href);
    if (cls) { fputs("\" class=\"", o); bf_lex_html_emit(o, cls); }
    fputs("\">", o); bf_lex_html_emit(o, label); fputs("</a>", o);
}
/* <a href="prefix<escaped id>suffix">label</a> -- the record link every list
 * row carries (/worlds/<space>/release/<id> and the like). */
static UI_UNUSED void ui_link_path(FILE *o, const char *const *parts, unsigned nparts, const char *label) {
    fputs("<a href=\"", o);
    for (unsigned i = 0; i < nparts; i++) { if (i & 1) bf_lex_html_emit(o, parts[i]); else fputs(parts[i], o); }
    fputs("\">", o); bf_lex_html_emit(o, label); fputs("</a>", o);
}

/* A group of pressed/unpressed toggle buttons declared as data: the filter
 * and view switches catalogue and feed pages carry. */
typedef struct { const char *value, *label; } UiToggle;
typedef struct {
    const char *cls;          /* group class, e.g. "media-modes" */
    const char *aria_label;   /* what the group chooses */
    const char *attr;         /* data attribute the page script reads */
    const UiToggle *options; unsigned count;
} UiToggleGroup;
static UI_UNUSED void ui_toggle_group(FILE *o, const UiToggleGroup *g) {
    fputs("<div class=\"", o); bf_lex_html_emit(o, g->cls); fputs("\" role=\"group\" aria-label=\"", o);
    bf_lex_html_emit(o, g->aria_label); fputs("\">", o);
    for (unsigned i = 0; i < g->count; i++) {
        fputs("<button type=\"button\" ", o); fputs(g->attr, o); fputs("=\"", o);
        bf_lex_html_emit(o, g->options[i].value);
        fputs(i ? "\" aria-pressed=\"false\">" : "\" aria-pressed=\"true\">", o);
        fputs(g->options[i].label, o); /* labels are page copy literals, already entity-encoded */
        fputs("</button>", o);
    }
    fputs("</div>", o);
}
/* A labelled search box a page script filters with. */
typedef struct { const char *label, *attr, *placeholder; int maxlength; } UiSearch;
static UI_UNUSED void ui_search(FILE *o, const UiSearch *s) {
    fprintf(o, "<label>%s<input type=\"search\" %s maxlength=\"%d\" placeholder=\"", s->label, s->attr, s->maxlength);
    bf_lex_html_emit(o, s->placeholder); fputs("\"></label>", o);
}

/* Rows of record links: <li><a href>title</a> <small>meta</small></li>. */
typedef struct { const char *href, *title, *meta; } UiRow;
static UI_UNUSED void ui_rows(FILE *o, const char *list_tag, const char *cls, const UiRow *rows, unsigned n) {
    ui_open(o, list_tag, cls);
    for (unsigned i = 0; i < n; i++) {
        fputs("<li>", o); ui_link(o, rows[i].href, NULL, rows[i].title);
        if (rows[i].meta && *rows[i].meta) { fputc(' ', o); ui_text(o, "small", NULL, rows[i].meta); }
        fputs("</li>", o);
    }
    ui_close(o, list_tag);
}

/* A table from a column spec and a row callback that writes the cells of row
 * i (with ui_cell). */
typedef struct { const char *label; } UiColumn;
typedef void (*UiRowFn)(FILE *o, void *ctx, unsigned row);
static UI_UNUSED void ui_cell(FILE *o, const char *text) { ui_text(o, "td", NULL, text); }
static UI_UNUSED void ui_table(FILE *o, const char *cls, const UiColumn *cols, unsigned ncols,
                               UiRowFn row, void *ctx, unsigned nrows) {
    fputs("<table class=\"ak-pattern ak-table", o);
    if (cls) { fputc(' ', o); bf_lex_html_emit(o, cls); }
    fputs("\"><thead><tr>", o);
    for (unsigned c = 0; c < ncols; c++) ui_text(o, "th", NULL, cols[c].label);
    fputs("</tr></thead><tbody>", o);
    for (unsigned r = 0; r < nrows; r++) { fputs("<tr>", o); row(o, ctx, r); fputs("</tr>", o); }
    fputs("</tbody></table>", o);
}

/* The empty state: one sentence and, when there is one, the one next step. */
static UI_UNUSED void ui_empty(FILE *o, const char *text, const char *href, const char *label) {
    fputs("<div class=\"ak-pattern ak-empty\" role=\"status\"><p>", o); bf_lex_html_emit(o, text); fputs("</p>", o);
    if (href && label) { fputs("<p>", o); ui_link(o, href, "ak-button is-primary", label); fputs("</p>", o); }
    fputs("</div>", o);
}

/* A figure and its label. */
static UI_UNUSED void ui_stat(FILE *o, const char *label, const char *value) {
    fputs("<div class=\"ak-stat\"><span class=\"ak-stat-value\">", o); bf_lex_html_emit(o, value);
    fputs("</span><span class=\"ak-stat-label\">", o); bf_lex_html_emit(o, label); fputs("</span></div>", o);
}

/* A small tag / badge. */
static UI_UNUSED void ui_tag(FILE *o, const char *text) { ui_text(o, "span", "ak-badge", text); }

/* A section with its heading: <section class="ak-pattern ak-section cls"><h2>title</h2>. */
static UI_UNUSED void ui_section_open(FILE *o, const char *cls, const char *title) {
    fputs("<section class=\"ak-pattern ak-section", o);
    if (cls) { fputc(' ', o); bf_lex_html_emit(o, cls); }
    fputs("\">", o);
    ui_text_if(o, "h2", NULL, title);
}
static UI_UNUSED void ui_section_close(FILE *o) { fputs("</section>", o); }

/* Layout from the numeric presentation vocabulary. A page declares, per
 * viewport, the properties it sets; the builder validates them against the
 * generated vocabulary and writes the data-<viewport>-<property> attributes
 * plus the --ak-<property>-<viewport> custom properties .ak-layout reads.
 * An out-of-vocabulary value is dropped, never printed. */
typedef struct { int prop; int value; } UiLayoutValue;        /* value: enum index or integer */
typedef struct { const UiLayoutValue *values; unsigned count; } UiLayoutView;
typedef struct { UiLayoutView view[BF_NP_VIEWPORTS]; } UiLayout;
static UI_UNUSED int ui_layout_valid(const UiLayoutValue *v) {
    if (v->prop < 0 || v->prop >= BF_NP_PROPS) return 0;
    const BfNpProp *p = &bf_np_props[v->prop];
    return p->range ? (v->value >= p->min && v->value <= p->max) : (v->value >= 0 && v->value < p->nvalues);
}
static UI_UNUSED void ui_layout_attrs(FILE *o, const UiLayout *l) {
    int vars = 0;
    for (int vp = 0; vp < BF_NP_VIEWPORTS; vp++)
        for (unsigned i = 0; i < l->view[vp].count; i++) {
            const UiLayoutValue *v = &l->view[vp].values[i];
            if (!ui_layout_valid(v)) continue;
            const BfNpProp *p = &bf_np_props[v->prop];
            if (p->range) { fprintf(o, " data-%s-%s=\"%d\"", bf_np_viewport[vp], p->name, v->value); vars = 1; }
            else fprintf(o, " data-%s-%s=\"%s\"", bf_np_viewport[vp], p->name, p->values[v->value]);
        }
    if (!vars) return;
    fputs(" style=\"", o);
    for (int vp = 0; vp < BF_NP_VIEWPORTS; vp++)
        for (unsigned i = 0; i < l->view[vp].count; i++) {
            const UiLayoutValue *v = &l->view[vp].values[i];
            if (!ui_layout_valid(v) || !bf_np_props[v->prop].range) continue;
            const char *unit = v->prop == BF_NP_COLUMNS || v->prop == BF_NP_SPAN ? "" : "px";
            fprintf(o, "--ak-%s-%s:%d%s;", bf_np_props[v->prop].name, bf_np_viewport[vp], v->value, unit);
        }
    fputc('"', o);
}
/* <tag class="ak-layout cls" data-...>: open a laid-out container. */
static UI_UNUSED void ui_layout_open(FILE *o, const char *tag, const char *cls, const UiLayout *l) {
    fputc('<', o); fputs(tag, o); fputs(" class=\"ak-layout", o);
    if (cls) { fputc(' ', o); bf_lex_html_emit(o, cls); }
    fputc('"', o);
    if (l) ui_layout_attrs(o, l);
    fputc('>', o);
}
#endif
