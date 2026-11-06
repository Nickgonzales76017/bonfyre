#ifndef BONFYRE_PROCUREMENT_H
#define BONFYRE_PROCUREMENT_H

#include <stddef.h>

int bf_procurement_rfq(const char *json, size_t len,
                       char **out, char *error, size_t error_size);
int bf_procurement_quote(const char *json, size_t len,
                         char **out, char *error, size_t error_size);
int bf_procurement_compare(const char *json, size_t len, const char *as_of,
                           char **out, char *error, size_t error_size);
int bf_procurement_allocate(const char *json, size_t len,
                            char **out, char *error, size_t error_size);
int bf_procurement_approval_request(const char *json, size_t len,
                                    char **out, char *error, size_t error_size);
int bf_procurement_order_intent(const char *json, size_t len,
                                char **out, char *error, size_t error_size);

int bf_procurement_contract_preflight(const char *json, size_t len,
                                      char **out, char *error, size_t error_size);

#define BF_PROCUREMENT_PURCHASE_PREFIX "procurement-draft:"
#define BF_PROCUREMENT_RFQ_PREFIX "procurement-rfq:"
#define BF_PROCUREMENT_QUOTE_PREFIX "procurement-quote:"
#define BF_PROCUREMENT_COMPARISON_PREFIX "procurement-comparison:"
#define BF_PROCUREMENT_ALLOCATION_PREFIX "procurement-allocation:"
#define BF_PROCUREMENT_APPROVAL_PREFIX "procurement-approval:"
#define BF_PROCUREMENT_ORDER_INTENT_PREFIX "procurement-order-intent:"

#endif
