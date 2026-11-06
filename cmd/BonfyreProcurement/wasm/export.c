#include "../src/procurement.h"

#include <stddef.h>
#include <stdlib.h>
#include <string.h>

enum {
    BF_PROC_WASM_RFQ = 1,
    BF_PROC_WASM_QUOTE = 2,
    BF_PROC_WASM_COMPARE = 3,
    BF_PROC_WASM_ALLOCATE = 4,
    BF_PROC_WASM_APPROVAL_REQUEST = 5,
    BF_PROC_WASM_ORDER_INTENT = 6,
    BF_PROC_WASM_CONTRACT_PREFLIGHT = 7
};

static char *result_body;
static size_t result_len;
static char result_error[256];
static size_t result_error_len;

void bf_procurement_wasm_reset(void) {
    free(result_body);
    result_body = NULL;
    result_len = 0;
    result_error[0] = 0;
    result_error_len = 0;
}

int bf_procurement_wasm_run(int operation,
                            const char *input,
                            size_t input_len,
                            const char *as_of,
                            size_t as_of_len) {
    bf_procurement_wasm_reset();
    if (!input || !input_len) {
        memcpy(result_error, "missing input", 14);
        result_error_len = 13;
        return 0;
    }

    char *stamp = NULL;
    if (as_of_len) {
        if (!as_of || as_of_len > 64) {
            memcpy(result_error, "invalid as-of", 14);
            result_error_len = 13;
            return 0;
        }
        stamp = malloc(as_of_len + 1);
        if (!stamp) {
            memcpy(result_error, "out of memory", 14);
            result_error_len = 13;
            return 0;
        }
        memcpy(stamp, as_of, as_of_len);
        stamp[as_of_len] = 0;
    }

    int ok = operation == BF_PROC_WASM_RFQ
        ? bf_procurement_rfq(input, input_len, &result_body, result_error, sizeof result_error)
        : operation == BF_PROC_WASM_QUOTE
        ? bf_procurement_quote(input, input_len, &result_body, result_error, sizeof result_error)
        : operation == BF_PROC_WASM_COMPARE
        ? bf_procurement_compare(input, input_len, stamp, &result_body, result_error, sizeof result_error)
        : operation == BF_PROC_WASM_ALLOCATE
        ? bf_procurement_allocate(input, input_len, &result_body, result_error, sizeof result_error)
        : operation == BF_PROC_WASM_APPROVAL_REQUEST
        ? bf_procurement_approval_request(input, input_len, &result_body, result_error, sizeof result_error)
        : operation == BF_PROC_WASM_ORDER_INTENT
        ? bf_procurement_order_intent(input, input_len, &result_body, result_error, sizeof result_error)
        : operation == BF_PROC_WASM_CONTRACT_PREFLIGHT
        ? bf_procurement_contract_preflight(input, input_len, &result_body, result_error, sizeof result_error)
        : 0;
    free(stamp);

    if (!ok) {
        if (!result_error[0]) memcpy(result_error, "unknown operation", 18);
        result_error_len = strlen(result_error);
        free(result_body);
        result_body = NULL;
        return 0;
    }
    result_len = strlen(result_body);
    return 1;
}

const char *bf_procurement_wasm_output(void) { return result_body; }
size_t bf_procurement_wasm_output_len(void) { return result_len; }
const char *bf_procurement_wasm_error(void) { return result_error; }
size_t bf_procurement_wasm_error_len(void) { return result_error_len; }
