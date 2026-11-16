#ifndef BF_WEB_STORE_H
#define BF_WEB_STORE_H
/* The stores a serve worker opens, declared in schemas/storage/stores, and
 * the one way to open them (bf_store_open: declared busy timeout, a lease on
 * the current request, the writer policy where the store declares one). The
 * durability header comes first so the generated header defines each store's
 * bf_store_policy_<name>. */
#include "bf_store_durability.h"
#include "../generated/store_schema.h"
#endif
