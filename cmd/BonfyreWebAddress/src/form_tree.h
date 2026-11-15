#ifndef BONFYRE_FORM_TREE_H
#define BONFYRE_FORM_TREE_H
#include <stdio.h>
#include <stdlib.h>
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
/* One input as data. rows > 0 makes a textarea; type "select" takes options
 * (checked marks the selected one); type "checkbox" honours checked. Every
 * attribute a real page needs is a field here, so no page prints a control. */
typedef struct {
    const char *label, *type, *name, *value, *placeholder, *maxlength, *inputmode;
    const char *pattern, *min, *max, *step, *autocomplete;
    int required, checked, rows;
    const FtOption *options;
    unsigned noptions;
} FtInput;
/* One submit button. name/value post which button was pressed; novalidate
 * skips client checks (a decline needs no age); tone is "quiet" or "danger". */
typedef struct { const char *label, *name, *value, *tone; int novalidate; const char *hook, *hook_value; } FtSubmit;
/* A whole form: hidden fields, inputs and buttons, rendered as one ui:action.
 * hook names one data-* attribute (lowercase, '-') that a page script or a
 * test finds the form by; hook_value is its value. Buttons carry their own. */
typedef struct {
    const char *id, *method, *action, *label, *hook, *hook_value;
    const FtField *hidden;
    unsigned nhidden;
    const FtInput *inputs;
    unsigned ninputs;
    const FtSubmit *submits;
    unsigned nsubmits;
} FtForm;
static void __attribute__((unused)) ft_kv(bf_jsonw *w, const char *key, const char *value) {
    bf_jsonw_key(w, key);
    bf_jsonw_str(w, value ? value : "");
}
/* A visitor's search or pasted link echoed back into its box must not make the
 * renderer refuse the whole form: the renderer rejects any string that looks
 * like script even though it would escape it, so such an echo is shown empty.
 * Only search boxes soften this way: an edit field that held stored text
 * would lose it, so edit forms with arbitrary stored text stay out of here. */
static const char *__attribute__((unused)) ft_echo(const char *v) {
    if (!v) return "";
    if (strstr(v, "javascript:") || strstr(v, "<script") || strstr(v, "onerror=") ||
        strstr(v, "onclick=") || strlen(v) > 32000) return "";
    return v;
}
static int __attribute__((unused)) ft_input(BfAuthoringTree *t, const char *id, const char *parent,
                    int pos, const FtInput *in) {
    bf_jsonw w;
    char rows[16];
    unsigned i;
    if (!in) return 0;
    snprintf(rows, sizeof(rows), "%d", in->rows > 0 ? in->rows : 3);
    bf_jsonw_init(&w, 0);
    bf_jsonw_obj(&w);
    ft_kv(&w, "label", in->label);
    ft_kv(&w, "type", in->type && *in->type ? in->type : "text");
    ft_kv(&w, "name", in->name);
    ft_kv(&w, "value", in->type && (!strcmp(in->type, "search") || !strcmp(in->type, "url")) ? ft_echo(in->value) : in->value);
    ft_kv(&w, "placeholder", in->placeholder);
    bf_jsonw_key(&w, "multiline");
    bf_jsonw_bool(&w, in->rows > 0);
    ft_kv(&w, "rows", rows);
    ft_kv(&w, "maxlength", in->maxlength);
    bf_jsonw_key(&w, "required");
    bf_jsonw_bool(&w, in->required);
    ft_kv(&w, "inputmode", in->inputmode);
    ft_kv(&w, "pattern", in->pattern);
    ft_kv(&w, "min", in->min);
    ft_kv(&w, "max", in->max);
    ft_kv(&w, "step", in->step);
    ft_kv(&w, "autocomplete", in->autocomplete);
    bf_jsonw_key(&w, "checked");
    bf_jsonw_bool(&w, in->checked);
    bf_jsonw_key(&w, "options");
    bf_jsonw_arr(&w);
    for (i = 0; i < in->noptions; i++) {
        bf_jsonw_obj(&w);
        ft_kv(&w, "value", in->options[i].value);
        ft_kv(&w, "label", in->options[i].label);
        bf_jsonw_key(&w, "checked");
        bf_jsonw_bool(&w, in->options[i].checked);
        bf_jsonw_obj_end(&w);
    }
    bf_jsonw_arr_end(&w);
    bf_jsonw_obj_end(&w);
    return ft_put(t, id, "ui:field", parent, pos, &w);
}
static int __attribute__((unused)) ft_field(BfAuthoringTree *t, const char *id, const char *parent,
                    int pos, const char *label, const char *type, const char *name,
                    const char *value, const char *placeholder, int multiline,
                    const char *rows, const char *maxlength, int required,
                    const char *inputmode) {
    FtInput in = {label, type, name, value, placeholder, maxlength, inputmode,
                  NULL, NULL, NULL, NULL, NULL, required, 0, 0, NULL, 0};
    in.rows = multiline ? (rows && atoi(rows) > 0 ? atoi(rows) : 3) : 0;
    return ft_input(t, id, parent, pos, &in);
}
/* Add a whole form under parent (NULL for the root) at pos. */
static int __attribute__((unused)) ft_form_add(BfAuthoringTree *t, const char *parent, int pos,
                                               const FtForm *f) {
    bf_jsonw w;
    char child[128];
    unsigned i;
    int ok;
    if (!t || !f || !f->id) return 0;
    bf_jsonw_init(&w, 0);
    bf_jsonw_obj(&w);
    ft_kv(&w, "label", f->label);
    ft_kv(&w, "method", f->method ? f->method : "post");
    ft_kv(&w, "action", f->action);
    ft_kv(&w, "hook", f->hook);
    ft_kv(&w, "hook_value", f->hook_value);
    bf_jsonw_key(&w, "fields");
    bf_jsonw_arr(&w);
    for (i = 0; i < f->nhidden; i++) {
        if (!f->hidden[i].name) continue;
        bf_jsonw_obj(&w);
        ft_kv(&w, "name", f->hidden[i].name);
        ft_kv(&w, "value", f->hidden[i].value);
        bf_jsonw_obj_end(&w);
    }
    bf_jsonw_arr_end(&w);
    bf_jsonw_key(&w, "buttons");
    bf_jsonw_arr(&w);
    for (i = 0; i < f->nsubmits; i++) {
        bf_jsonw_obj(&w);
        ft_kv(&w, "label", f->submits[i].label);
        ft_kv(&w, "name", f->submits[i].name);
        ft_kv(&w, "value", f->submits[i].value);
        ft_kv(&w, "tone", f->submits[i].tone);
        bf_jsonw_key(&w, "novalidate");
        bf_jsonw_bool(&w, f->submits[i].novalidate);
        ft_kv(&w, "hook", f->submits[i].hook);
        ft_kv(&w, "hook_value", f->submits[i].hook_value);
        bf_jsonw_obj_end(&w);
    }
    bf_jsonw_arr_end(&w);
    bf_jsonw_obj_end(&w);
    ok = ft_put(t, f->id, "ui:action", parent, pos, &w);
    for (i = 0; ok && i < f->ninputs; i++) {
        snprintf(child, sizeof(child), "%s-%u", f->id, i);
        ok = ft_input(t, child, f->id, (int)i, &f->inputs[i]);
    }
    return ok;
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
/* Render one form to out on its own. Returns success; on failure prints
 * nothing, as every other ft_render caller does. */
static int __attribute__((unused)) ft_form(FILE *out, const FtForm *f) {
    BfAuthoringTree *t = calloc(1, sizeof(*t));
    if (!t) return 0;
    if (!ft_form_add(t, NULL, 0, f)) {
        bf_authoring_tree_clear(t);
        free(t);
        return 0;
    }
    return ft_render(t, out);
}
/* Render one form onto the end of a page fragment being assembled in buf
 * (used bytes so far in *used, NUL-terminated). Nothing is appended when the
 * form does not fit or does not render. Returns success. */
static int __attribute__((unused)) ft_form_append(char *buf, size_t cap, size_t *used, const FtForm *f) {
    char *rendered = NULL;
    size_t len = 0;
    FILE *mem;
    int ok;
    if (!buf || !used || *used >= cap) return 0;
    mem = open_memstream(&rendered, &len);
    if (!mem) return 0;
    ok = ft_form(mem, f);
    fclose(mem);
    if (ok && rendered && len < cap - *used) {
        memcpy(buf + *used, rendered, len + 1);
        *used += len;
    } else ok = 0;
    free(rendered);
    return ok;
}
#endif
