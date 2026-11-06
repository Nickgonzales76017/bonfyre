#ifndef BF_COMMERCE_H
#define BF_COMMERCE_H

/* The commerce chain preserves fifteen non-interchangeable established facts.
 * Recording payment, delivery, or satisfaction must never infer a later fact. */

#include <stddef.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

#define BF_COMMERCE_TEXT_MAX 256
#define BF_COMMERCE_STAGE_COUNT 15

typedef struct BfCommerce BfCommerce;
typedef enum BfCommerceStatus { BF_COMMERCE_OK = 0, BF_COMMERCE_INVALID, BF_COMMERCE_NOT_FOUND, BF_COMMERCE_STORAGE, BF_COMMERCE_CAPACITY } BfCommerceStatus;
typedef enum BfCommerceStage {
    BF_COMMERCE_PRODUCT, BF_COMMERCE_LISTING, BF_COMMERCE_INVENTORY, BF_COMMERCE_AVAILABILITY,
    BF_COMMERCE_OFFER, BF_COMMERCE_QUOTE, BF_COMMERCE_CART, BF_COMMERCE_CHECKOUT,
    BF_COMMERCE_COMMITMENT, BF_COMMERCE_PAYMENT, BF_COMMERCE_SETTLEMENT, BF_COMMERCE_FULFILMENT,
    BF_COMMERCE_DELIVERY, BF_COMMERCE_SATISFACTION, BF_COMMERCE_OWNERSHIP
} BfCommerceStage;

typedef struct BfCommerceProgress { size_t count; BfCommerceStage stages[BF_COMMERCE_STAGE_COUNT]; } BfCommerceProgress;

int bf_commerce_open(const char *database_path, BfCommerce **out, FILE *err);
int bf_commerce_open_database(void *sqlite_database, BfCommerce **out, FILE *err);
void bf_commerce_close(BfCommerce *commerce);
/* Requires exactly the immediately prior stage. The owner is evidence of the
 * responsible subsystem and must be named, but a caller-selected string does
 * not impersonate a verification of that external owner. */
BfCommerceStatus bf_commerce_record(BfCommerce *commerce, const char *exchange,
                                    BfCommerceStage stage, const char *owner,
                                    const char *detail, const char *at);
BfCommerceStatus bf_commerce_reached(BfCommerce *commerce, const char *exchange,
                                     BfCommerceStage stage, int *out);
BfCommerceStatus bf_commerce_furthest(BfCommerce *commerce, const char *exchange,
                                      BfCommerceStage *out, int *out_found);
BfCommerceStatus bf_commerce_outstanding(BfCommerce *commerce, const char *exchange,
                                         BfCommerceProgress *out);
/* A stage establishes only itself. This is deliberately callable so downstream
 * code cannot invent a transitive completion shortcut. */
int bf_commerce_implies(BfCommerceStage earlier, BfCommerceStage later);
const char *bf_commerce_stage_name(BfCommerceStage stage);
const char *bf_commerce_stage_owner(BfCommerceStage stage);
const char *bf_commerce_stage_does_not_establish(BfCommerceStage stage);
const char *bf_commerce_status_name(BfCommerceStatus status);

#ifdef __cplusplus
}
#endif

#endif
