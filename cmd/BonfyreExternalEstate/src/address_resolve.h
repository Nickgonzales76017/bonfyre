#ifndef BF_ADDRESS_RESOLVE_H
#define BF_ADDRESS_RESOLVE_H
#include <sqlite3.h>
#include <stdio.h>
#define BF_ADDRESS_TERMS 32
/* A read-only projection over the existing AddressPlane. No copied registry,
 * inferred owner truth, network access, or requirement-state mutation. */
typedef struct {
    const char *kind;
    const char *need[BF_ADDRESS_TERMS], *forbid[BF_ADDRESS_TERMS];
    int need_count, forbid_count, limit;
} BfAddressQuery;
typedef struct {
    const char *identity, *owner, *verifier, *lowerer, *frontier;
    const char *name, *url;
    const char *const *dependencies;
    size_t dependency_count;
} BfAddressCandidate;
typedef struct {
    sqlite3_int64 considered, eligible;
    int returned, mask_words, has_more, unknown_coordinate;
} BfAddressResolution;
typedef int (*BfAddressVisitor)(void *, const BfAddressCandidate *);
/* Results are buffered until all rows validate. Callback pointers are borrowed
 * only during the call. Missing schema is an error, never an empty universe. */
int bf_address_resolve(sqlite3 *db, const BfAddressQuery *query,
                       BfAddressVisitor visit, void *context,
                       BfAddressResolution *result, FILE *err);
/* Storage location is resolved by the owner, not its native/UI consumers. */
int bf_address_open_index(sqlite3 **db, FILE *err);
int bf_address_resolve_path(const char *path, const BfAddressQuery *query,
                            BfAddressVisitor visit, void *context,
                            BfAddressResolution *result, FILE *err);
#endif
