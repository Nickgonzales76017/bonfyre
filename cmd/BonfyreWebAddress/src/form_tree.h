#ifndef BONFYRE_FORM_TREE_H
#define BONFYRE_FORM_TREE_H
#include <stdio.h>
#include <string.h>
#include "bf_jsonw.h"
#include "../../BonfyreSurfaceScene/src/authoring_native_components.h"
/* Shared builders for request-time form trees: surfaces compose ui:card,
 * ui:action, ui:choice and ui:field data and render through the one native
 * renderer. No surface hand-prints control markup. */
typedef struct { const char *name, *value; } FtField;
typedef struct { const char *value, *label; int checked; } FtOption;
typedef struct { const char *label, *href; } FtLink;
static int __attribute__((unused)) ft_put(BfAuthoringTree *t, const char *id, const char *kind,
                  const char *parent, int pos, bf_jsonw *w) {
    int ok = t && id && kind && w && !w->oom && w->buf &&
             bf_authoring_tree_add(t, id, kind, parent, pos) &&
             bf_authoring_tree_data(t, id, "payload", w->buf);
    bf_jsonw_free(w);
    return ok;
}
static int __attribute__((unused)) ft_card(BfAuthoringTree *t, const char *id, const char *parent,
                   int pos, const char *title, const char *body,
                   const FtLink *links, unsigned nlinks) {
    bf_jsonw w;
    unsigned i;
    bf_jsonw_init(&w, 0);
    bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "title");
    bf_jsonw_str(&w, title ? title : "");
    bf_jsonw_key(&w, "body");
    bf_jsonw_str(&w, body ? body : "");
    bf_jsonw_key(&w, "actions");
    bf_jsonw_arr(&w);
    for (i = 0; i < nlinks; i++) {
        bf_jsonw_obj(&w);
        bf_jsonw_key(&w, "label");
        bf_jsonw_str(&w, links[i].label ? links[i].label : "");
        bf_jsonw_key(&w, "href");
        bf_jsonw_str(&w, links[i].href ? links[i].href : "");
        bf_jsonw_obj_end(&w);
    }
    bf_jsonw_arr_end(&w);
    bf_jsonw_obj_end(&w);
    return ft_put(t, id, "ui:card", parent, pos, &w);
}
static int __attribute__((unused)) ft_action(BfAuthoringTree *t, const char *id, const char *parent,
                     int pos, const char *label, const char *method,
                     const char *action, const FtField *fields, unsigned nfields) {
    bf_jsonw w;
    unsigned i;
    bf_jsonw_init(&w, 0);
    bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "label");
    bf_jsonw_str(&w, label ? label : "");
    bf_jsonw_key(&w, "method");
    bf_jsonw_str(&w, method ? method : "post");
    bf_jsonw_key(&w, "action");
    bf_jsonw_str(&w, action ? action : "");
    bf_jsonw_key(&w, "fields");
    bf_jsonw_arr(&w);
    for (i = 0; i < nfields; i++) {
        bf_jsonw_obj(&w);
        bf_jsonw_key(&w, "name");
        bf_jsonw_str(&w, fields[i].name ? fields[i].name : "");
        bf_jsonw_key(&w, "value");
        bf_jsonw_str(&w, fields[i].value ? fields[i].value : "");
        bf_jsonw_obj_end(&w);
    }
    bf_jsonw_arr_end(&w);
    bf_jsonw_obj_end(&w);
    return ft_put(t, id, "ui:action", parent, pos, &w);
}
static int __attribute__((unused)) ft_choice(BfAuthoringTree *t, const char *id, const char *parent,
                     int pos, const char *label, const char *name,
                     const FtOption *options, unsigned n) {
    bf_jsonw w;
    unsigned i;
    bf_jsonw_init(&w, 0);
    bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "label");
    bf_jsonw_str(&w, label ? label : "");
    bf_jsonw_key(&w, "name");
    bf_jsonw_str(&w, name ? name : "");
    bf_jsonw_key(&w, "options");
    bf_jsonw_arr(&w);
    for (i = 0; i < n; i++) {
        /* The group name repeats on every option: dotted section paths do
         * not push intermediate objects, so items carry all they render. */
        bf_jsonw_obj(&w);
        bf_jsonw_key(&w, "name");
        bf_jsonw_str(&w, name ? name : "");
        bf_jsonw_key(&w, "value");
        bf_jsonw_str(&w, options[i].value ? options[i].value : "");
        bf_jsonw_key(&w, "label");
        bf_jsonw_str(&w, options[i].label ? options[i].label : "");
        bf_jsonw_key(&w, "checked");
        bf_jsonw_bool(&w, options[i].checked);
        bf_jsonw_obj_end(&w);
    }
    bf_jsonw_arr_end(&w);
    bf_jsonw_obj_end(&w);
    return ft_put(t, id, "ui:choice", parent, pos, &w);
}
static int __attribute__((unused)) ft_field(BfAuthoringTree *t, const char *id, const char *parent,
                    int pos, const char *label, const char *type, const char *name,
                    const char *value, const char *placeholder, int multiline,
                    const char *rows, const char *maxlength, int required,
                    const char *inputmode) {
    bf_jsonw w;
    bf_jsonw_init(&w, 0);
    bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "label");
    bf_jsonw_str(&w, label ? label : "");
    bf_jsonw_key(&w, "type");
    bf_jsonw_str(&w, type ? type : "text");
    bf_jsonw_key(&w, "name");
    bf_jsonw_str(&w, name ? name : "");
    bf_jsonw_key(&w, "value");
    bf_jsonw_str(&w, value ? value : "");
    bf_jsonw_key(&w, "placeholder");
    bf_jsonw_str(&w, placeholder ? placeholder : "");
    bf_jsonw_key(&w, "multiline");
    bf_jsonw_bool(&w, multiline);
    bf_jsonw_key(&w, "rows");
    bf_jsonw_str(&w, rows ? rows : "3");
    bf_jsonw_key(&w, "maxlength");
    bf_jsonw_str(&w, maxlength ? maxlength : "");
    bf_jsonw_key(&w, "required");
    bf_jsonw_bool(&w, required);
    bf_jsonw_key(&w, "inputmode");
    bf_jsonw_str(&w, inputmode ? inputmode : "");
    bf_jsonw_obj_end(&w);
    return ft_put(t, id, "ui:field", parent, pos, &w);
}
static int __attribute__((unused)) ft_badge(BfAuthoringTree *t, const char *id, const char *parent,
                    int pos, const char *label) {
    bf_jsonw w;
    bf_jsonw_init(&w, 0);
    bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "label");
    bf_jsonw_str(&w, label ? label : "");
    bf_jsonw_obj_end(&w);
    return ft_put(t, id, "ui:badge", parent, pos, &w);
}
/* Render the tree to out, then clear and free it. Returns success. */
static int __attribute__((unused)) ft_render(BfAuthoringTree *t, FILE *out) {
    char *rendered = NULL;
    size_t rlen = 0;
    FILE *memory;
    int ok;
    if (!t || !out) {
        if (t) {
            bf_authoring_tree_clear(t);
            free(t);
        }
        return 0;
    }
    memory = open_memstream(&rendered, &rlen);
    ok = memory && bf_authoring_native_render(memory, t, 1048576);
    if (memory) fclose(memory);
    bf_authoring_tree_clear(t);
    free(t);
    if (!ok || !rendered) {
        free(rendered);
        return 0;
    }
    fputs(rendered, out);
    free(rendered);
    return 1;
}
#endif
