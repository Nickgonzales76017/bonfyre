/*
 * BonfyreGraph — Upgrade V: Typed Generators + Static Verification
 *
 * Small type/contract system for generator parameters:
 *   string, number, boolean, null
 *   enum(val1|val2|...)
 *   range(lo..hi)
 *   pattern(regex)
 *   int(lo..hi), float(lo..hi)
 *   optional_<type>
 *
 * Type-safe symbolic rewriting: verify values satisfy constraints
 * before applying REDUCE_SYMBOLIC updates.
 */
#ifndef TYPE_CHECK_H
#define TYPE_CHECK_H

#include <sqlite3.h>
#include <stddef.h>

/* Type constraint kinds */
#define TC_STRING    0
#define TC_NUMBER    1
#define TC_BOOLEAN   2
#define TC_NULL      3
#define TC_ENUM      4
#define TC_INT_RANGE 5
#define TC_FLT_RANGE 6
#define TC_PATTERN   7
#define TC_ANY       8
#define TC_OPTIONAL  9

/* Parsed type constraint */
typedef struct {
    int    kind;
    char   field_name[256];
    int    position;
    double range_lo;
    double range_hi;
    char   enum_vals[4096];   /* pipe-separated: "val1|val2|val3" */
    char   pattern[512];      /* regex pattern for TC_PATTERN */
    int    is_optional;
} TypeConstraint;

/* Bootstrap _type_constraints table. */
int tc_bootstrap(sqlite3 *db);

/* Infer type constraints for all positions in a family.
   Analyzes binding values and writes constraints to _type_constraints.
   Returns number of constraints inferred. */
int tc_infer(sqlite3 *db, int family_id);

/* Infer constraints for all families in a content type. */
int tc_infer_all(sqlite3 *db, const char *content_type);

/* Validate a value against a position's constraint.
   Returns 1 if valid, 0 if violation.
   If violation_msg is non-NULL, writes human-readable error. */
int tc_validate(sqlite3 *db, int family_id, int position,
                const char *value, char *violation_msg, size_t msg_sz);

/* Validate a full JSON object against its family's constraints.
   Returns 1 if all valid, 0 if any violation.
   out_errors (if non-NULL): JSON array of violation messages. Caller frees. */
int tc_validate_json(sqlite3 *db, int family_id,
                     const char *json, char **out_errors);

/* Get constraints for a family as JSON.
   Caller frees out_json. Returns constraint count. */
int tc_constraints_json(sqlite3 *db, int family_id, char **out_json);

/* Verify generator-binding compatibility:
   check that stored bindings actually match the generator's type signature.
   Returns count of violations found. */
int tc_verify_family(sqlite3 *db, int family_id);

/* Safe symbolic update: validates value before applying.
   Returns 0 on success, -1 on type violation. */
int tc_safe_update(sqlite3 *db, const char *content_type,
                   int target_id, const char *field, const char *value);

#endif /* TYPE_CHECK_H */
