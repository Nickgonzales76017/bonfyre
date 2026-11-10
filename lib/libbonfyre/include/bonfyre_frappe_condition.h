/**
 * bonfyre_frappe_condition.h
 *
 * Frappe's conditional field properties (depends_on, mandatory_depends_on,
 * read_only_depends_on, collapsible_depends_on) compiled into one
 * source-preserving plan per distinct expression, with the fields each plan
 * reads. A plan is the donor's client-side form rule and nothing more: it is
 * not a validation this estate performs and not a permission.
 */

#ifndef BONFYRE_FRAPPE_CONDITION_H
#define BONFYRE_FRAPPE_CONDITION_H

#include <stddef.h>
#include <sqlite3.h>

typedef struct BfCondNode BfCondNode;

typedef struct {
    int compiled;                 /* 1 = plan produced, 0 = missing_operation names why not */
    char form[8];                 /* "bare", "eval" or "fn" */
    char *plan;                   /* canonical s-expression; NULL when not compiled */
    char *shape;                  /* the same tree with names and literals erased */
    char plan_id[40];             /* "cp-" + the first 32 hex of the SHA-256 of the plan text */
    char shape_id[40];            /* "cs-" + the same of the shape text */
    char missing_operation[96];
    char constant[16];            /* "", "always_true" or "always_false" */
    char diagnostic[200];
    char **reads;                 /* top-level document fields the plan reads */
    size_t read_count;
    char **parent_reads;          /* fields read from the parent document */
    size_t parent_read_count;
    char **env_reads;             /* anything that is neither doc nor parent */
    size_t env_read_count;
    BfCondNode *root;
} BfCondPlan;

/* Always fills *out; returns 0 when a plan was compiled, -1 otherwise. */
int bf_cond_compile(const char *source, BfCondPlan *out);
/* The same, for the Python expression a donor Notification states (no prefix). */
int bf_cond_compile_python(const char *expression, BfCondPlan *out);
void bf_cond_plan_free(BfCondPlan *plan);

/* Evaluates a compiled plan against one document held as JSON text. Returns 1
 * or 0 for the boolean Frappe's form would compute, and -1 with a reason when
 * the donor's expression would have thrown or reads something not bound.
 * parent_json and env_json may be NULL; env_json holds whatever the estate
 * binds for names that are neither doc nor parent, as nested objects. */
int bf_cond_evaluate(const BfCondPlan *plan, sqlite3 *db, const char *doc_json,
                     const char *parent_json, const char *env_json,
                     char *error, size_t error_cap);

/* The value itself: as JSON text, and as the donor's template would print it.
 * Either pointer may be NULL; what is returned is the caller's to free.
 * Returns the value's truth, or -1 with a reason. */
int bf_cond_evaluate_value(const BfCondPlan *plan, sqlite3 *db, const char *doc_json,
                           const char *parent_json, const char *env_json,
                           char **json, char **text, char *error, size_t error_cap);

/* Says a plan in plain words. label returns the label a field carries and
 * whether it is a tick box, or NULL when it has none. Returns -1 and an empty
 * string when the plan has no natural phrasing. */
typedef const char *(*BfCondLabelFn)(void *ctx, const char *fieldname, int *is_check);
int bf_cond_words(const BfCondPlan *plan, BfCondLabelFn label, void *ctx, char *out, size_t cap);

#endif
