/* test_lambda_tensors.c — Roundtrip + benchmark tests for liblambda-tensors
 *
 * Tests:
 *   1. V1 encode/decode roundtrip
 *   2. V2 encode/decode roundtrip
 *   3. V1 delta encode/decode roundtrip
 *   4. V2 delta encode/decode roundtrip
 *   5. Interned encode/decode roundtrip
 *   6. Huffman measurement consistency
 *   7. Arithmetic measurement consistency
 *   8. Family compression benchmark (N=100, N=1000)
 */

#include "lambda_tensors.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static int tests_run = 0;
static int tests_passed = 0;

#define ASSERT(cond, msg) do { \
    tests_run++; \
    if (!(cond)) { \
        printf("  FAIL: %s (line %d)\n", msg, __LINE__); \
    } else { \
        tests_passed++; \
    } \
} while(0)

/* Generate a binding that shares structure+strings with others in the family */
static char *make_family_binding(int member_id, int num_fields) {
    char *buf = malloc(8192);
    int off = 0;
    off += sprintf(buf + off, "[");
    for (int i = 0; i < num_fields; i++) {
        if (i > 0) off += sprintf(buf + off, ",");
        switch (i % 7) {
        case 0: /* shared string (same across family) */
            off += sprintf(buf + off, "\"service_offer\"");
            break;
        case 1: /* member-specific int */
            off += sprintf(buf + off, "%d", member_id * 10 + i);
            break;
        case 2: /* shared string with slight variation */
            off += sprintf(buf + off, "\"building_%d\"", member_id % 5);
            break;
        case 3: /* price (varies per member) */
            off += sprintf(buf + off, "%.2f", 10.0 + (double)(member_id % 20));
            break;
        case 4: /* shared enum string */
            off += sprintf(buf + off, "\"%s\"",
                           member_id % 3 == 0 ? "draft" :
                           member_id % 3 == 1 ? "published" : "archived");
            break;
        case 5: /* threshold int (few distinct values) */
            off += sprintf(buf + off, "%d", (member_id % 4) * 10);
            break;
        case 6: /* null or bool */
            off += sprintf(buf + off, "%s",
                           member_id % 2 ? "true" : "null");
            break;
        }
    }
    off += sprintf(buf + off, "]");
    buf[off] = '\0';
    return buf;
}

static void test_v1_roundtrip(void) {
    printf("Test: V1 encode/decode roundtrip\n");

    const char *input = "[42,null,\"hello world\",3.14,true,false]";
    unsigned char *packed = NULL;
    size_t packed_len = 0;
    char *decoded = NULL;

    int n = lt_encode_v1(input, &packed, &packed_len);
    ASSERT(n == 6, "V1 encode returns 6 fields");
    ASSERT(packed != NULL, "V1 encode produces output");
    ASSERT(packed_len > 0, "V1 packed length > 0");
    ASSERT(packed_len < strlen(input), "V1 packed smaller than JSON");

    int n2 = lt_decode_v1(packed, packed_len, &decoded);
    ASSERT(n2 == 6, "V1 decode returns 6 fields");
    ASSERT(decoded != NULL, "V1 decode produces output");

    /* Verify the decoded JSON parses back to the same values */
    ASSERT(strstr(decoded, "42") != NULL, "V1 decoded contains 42");
    ASSERT(strstr(decoded, "null") != NULL, "V1 decoded contains null");
    ASSERT(strstr(decoded, "hello world") != NULL, "V1 decoded contains string");
    ASSERT(strstr(decoded, "true") != NULL, "V1 decoded contains true");
    ASSERT(strstr(decoded, "false") != NULL, "V1 decoded contains false");

    free(packed);
    free(decoded);
    printf("  V1 roundtrip: %zu bytes JSON → %zu bytes packed (%.1f%%)\n",
           strlen(input), packed_len, 100.0 * (double)packed_len / (double)strlen(input));
}

static void test_v2_roundtrip(void) {
    printf("Test: V2 encode/decode roundtrip\n");

    const char *input = "[42,null,\"hello\",3.14,true,\"\",17,-5,\"hello\"]";
    unsigned char *packed = NULL;
    size_t packed_len = 0;
    char *decoded = NULL;

    int n = lt_encode_v2(input, &packed, &packed_len);
    ASSERT(n > 0, "V2 encode succeeds");
    ASSERT(packed != NULL, "V2 encode produces output");

    int n2 = lt_decode_v2(packed, packed_len, &decoded);
    ASSERT(n2 > 0, "V2 decode succeeds");
    ASSERT(decoded != NULL, "V2 decode produces output");

    ASSERT(strstr(decoded, "42") != NULL, "V2 decoded contains 42");
    ASSERT(strstr(decoded, "null") != NULL, "V2 decoded contains null");
    ASSERT(strstr(decoded, "hello") != NULL, "V2 decoded contains string");

    /* V2 should pack tighter than V1 (empty string + small int + strref) */
    unsigned char *v1_packed = NULL;
    size_t v1_len = 0;
    lt_encode_v1(input, &v1_packed, &v1_len);
    ASSERT(packed_len <= v1_len, "V2 packs at least as tight as V1");

    printf("  V2 roundtrip: V1=%zu V2=%zu bytes\n", v1_len, packed_len);

    free(packed);
    free(v1_packed);
    free(decoded);
}

static void test_v1_delta_roundtrip(void) {
    printf("Test: V1 delta encode/decode roundtrip\n");

    const char *ref_json = "[42,null,\"hello\",3.14,true]";
    const char *tgt_json = "[42,null,\"world\",3.14,false]";

    unsigned char *ref_packed = NULL, *tgt_packed = NULL;
    size_t ref_len = 0, tgt_len = 0;
    lt_encode_v1(ref_json, &ref_packed, &ref_len);
    lt_encode_v1(tgt_json, &tgt_packed, &tgt_len);

    unsigned char *delta = NULL;
    size_t delta_len = 0;
    int rc = lt_delta_encode_v1(ref_packed, ref_len, tgt_packed, tgt_len,
                                &delta, &delta_len);
    ASSERT(rc >= 0, "V1 delta encode succeeds");
    ASSERT(delta_len < tgt_len, "V1 delta smaller than full packed");

    unsigned char *reconstructed = NULL;
    size_t recon_len = 0;
    rc = lt_delta_decode_v1(ref_packed, ref_len, delta, delta_len,
                            &reconstructed, &recon_len);
    ASSERT(rc >= 0, "V1 delta decode succeeds");
    ASSERT(recon_len == tgt_len, "V1 delta reconstructed matches target length");
    ASSERT(memcmp(reconstructed, tgt_packed, tgt_len) == 0,
           "V1 delta reconstructed is byte-exact");

    printf("  V1 delta: packed=%zu delta=%zu (%.1f%% of packed)\n",
           tgt_len, delta_len, 100.0 * (double)delta_len / (double)tgt_len);

    free(ref_packed); free(tgt_packed);
    free(delta); free(reconstructed);
}

static void test_v2_delta_roundtrip(void) {
    printf("Test: V2 delta encode/decode roundtrip\n");

    const char *ref_json = "[42,null,\"hello\",3.14,true,\"\",100]";
    const char *tgt_json = "[42,null,\"world\",3.14,false,\"\",105]";

    unsigned char *ref_packed = NULL, *tgt_packed = NULL;
    size_t ref_len = 0, tgt_len = 0;
    lt_encode_v2(ref_json, &ref_packed, &ref_len);
    lt_encode_v2(tgt_json, &tgt_packed, &tgt_len);

    unsigned char *delta = NULL;
    size_t delta_len = 0;
    int rc = lt_delta_encode_v2(ref_packed, ref_len, tgt_packed, tgt_len,
                                &delta, &delta_len);
    ASSERT(rc >= 0, "V2 delta encode succeeds");

    unsigned char *reconstructed = NULL;
    size_t recon_len = 0;
    rc = lt_delta_decode_v2(ref_packed, ref_len, delta, delta_len,
                            &reconstructed, &recon_len);
    ASSERT(rc >= 0, "V2 delta decode succeeds");
    ASSERT(recon_len == tgt_len, "V2 delta reconstructed matches target length");
    ASSERT(memcmp(reconstructed, tgt_packed, tgt_len) == 0,
           "V2 delta reconstructed is byte-exact");

    printf("  V2 delta: packed=%zu delta=%zu\n", tgt_len, delta_len);

    free(ref_packed); free(tgt_packed);
    free(delta); free(reconstructed);
}

static void test_interned_roundtrip(void) {
    printf("Test: Interned encode/decode roundtrip\n");

    /* Build a family with shared strings */
    LtStringTable strtab;
    lt_strtab_init(&strtab);
    LtStrtabCtx *ctx = lt_strtab_ctx_new();

    const char *m1 = "[\"dog_walking\",42,\"building_a\",true]";
    const char *m2 = "[\"dog_walking\",99,\"building_b\",false]";
    const char *m3 = "[\"dog_walking\",17,\"building_a\",true]";

    lt_strtab_ingest(ctx, m1);
    lt_strtab_ingest(ctx, m2);
    lt_strtab_ingest(ctx, m3);
    lt_strtab_finalize(ctx, &strtab);
    lt_strtab_ctx_free(ctx);

    ASSERT(strtab.count > 0, "String table has entries");

    /* Encode and decode with interning */
    unsigned char *packed = NULL;
    size_t packed_len = 0;
    int n = lt_encode_v2_interned(m1, &strtab, &packed, &packed_len);
    ASSERT(n > 0, "Interned encode succeeds");

    char *decoded = NULL;
    int n2 = lt_decode_v2_interned(packed, packed_len, &strtab, &decoded);
    ASSERT(n2 > 0, "Interned decode succeeds");
    ASSERT(strstr(decoded, "dog_walking") != NULL, "Interned decoded has shared string");
    ASSERT(strstr(decoded, "42") != NULL, "Interned decoded has int");

    /* Measure savings */
    int raw_v2 = lt_measure_v2(m1);
    int interned = lt_measure_v2_interned(m1, &strtab);
    ASSERT(interned <= raw_v2, "Interned <= V2 raw");
    printf("  Interned: V2=%d interned=%d bytes, strtab=%d strings\n",
           raw_v2, interned, strtab.count);

    free(packed);
    free(decoded);
    lt_strtab_free(&strtab);
}

static void test_huffman_measurement(void) {
    printf("Test: Huffman measurement consistency\n");

    LtHuffTable htab;
    lt_huff_init(&htab);

    /* Build PMF from a family of 50 members */
    char **members = malloc(50 * sizeof(char *));
    for (int i = 0; i < 50; i++) {
        members[i] = make_family_binding(i, 14);
        lt_huff_ingest(&htab, members[i]);
    }
    lt_huff_finalize(&htab);

    ASSERT(htab.num_positions == 14, "Huffman has 14 positions");

    /* Huffman measure should be <= V2 measure for every member */
    int all_huffman_leq = 1;
    for (int i = 0; i < 50; i++) {
        int v2_size = lt_measure_v2(members[i]);
        int huff_size = lt_measure_v2_huffman(members[i], &htab);
        if (huff_size > v2_size) all_huffman_leq = 0;
    }
    ASSERT(all_huffman_leq, "Huffman <= V2 for all members");

    /* Huffman delta measure should be <= V2 delta measure */
    int all_delta_leq = 1;
    for (int i = 1; i < 50; i++) {
        int v2_delta = lt_delta_measure_v2(members[0], members[i]);
        int huff_delta = lt_delta_measure_v2_huffman(members[0], members[i], &htab);
        if (huff_delta > v2_delta) all_delta_leq = 0;
    }
    ASSERT(all_delta_leq, "Huffman delta <= V2 delta for all members");

    printf("  Huffman: header=%d bytes, %d positions\n",
           lt_huff_header_size(&htab), htab.num_positions);

    for (int i = 0; i < 50; i++) free(members[i]);
    free(members);
    lt_huff_free(&htab);
}

static void test_arithmetic_measurement(void) {
    printf("Test: Arithmetic measurement (Shannon limit)\n");

    LtHuffTable htab;
    lt_huff_init(&htab);

    char **members = malloc(50 * sizeof(char *));
    for (int i = 0; i < 50; i++) {
        members[i] = make_family_binding(i, 14);
        lt_huff_ingest(&htab, members[i]);
    }
    lt_huff_finalize(&htab);

    /* Arithmetic should be <= Huffman (no integer rounding) */
    int arith_leq_huff = 1;
    for (int i = 0; i < 50; i++) {
        int huff = lt_measure_v2_huffman(members[i], &htab);
        int arith = lt_measure_v2_arithmetic(members[i], &htab);
        if (arith > huff) arith_leq_huff = 0;
    }
    ASSERT(arith_leq_huff, "Arithmetic <= Huffman for all members");

    /* Log one sample */
    int huff_sample = lt_measure_v2_huffman(members[0], &htab);
    int arith_sample = lt_measure_v2_arithmetic(members[0], &htab);
    printf("  Sample: Huffman=%d arithmetic=%d (%.2f%% tighter)\n",
           huff_sample, arith_sample,
           huff_sample > 0 ? 100.0 * (1.0 - (double)arith_sample / (double)huff_sample) : 0.0);

    for (int i = 0; i < 50; i++) free(members[i]);
    free(members);
    lt_huff_free(&htab);
}

static void bench_family(int n, int num_fields) {
    printf("Benchmark: N=%d, %d fields per member\n", n, num_fields);

    char **members = malloc((size_t)n * sizeof(char *));
    size_t raw_total = 0;
    for (int i = 0; i < n; i++) {
        members[i] = make_family_binding(i, num_fields);
        raw_total += strlen(members[i]);
    }

    /* V1 packed */
    int v1_packed_total = 0;
    for (int i = 0; i < n; i++)
        v1_packed_total += lt_measure_v1(members[i]);

    /* V2 packed */
    int v2_packed_total = 0;
    for (int i = 0; i < n; i++)
        v2_packed_total += lt_measure_v2(members[i]);

    /* V1 delta */
    int v1_delta_total = lt_measure_v1(members[0]); /* ref */
    for (int i = 1; i < n; i++)
        v1_delta_total += lt_delta_measure_v1(members[0], members[i]);

    /* V2 delta */
    int v2_delta_total = lt_measure_v2(members[0]);
    for (int i = 1; i < n; i++)
        v2_delta_total += lt_delta_measure_v2(members[0], members[i]);

    /* Interned */
    LtStringTable strtab;
    lt_strtab_init(&strtab);
    LtStrtabCtx *ctx = lt_strtab_ctx_new();
    for (int i = 0; i < n; i++)
        lt_strtab_ingest(ctx, members[i]);
    lt_strtab_finalize(ctx, &strtab);
    lt_strtab_ctx_free(ctx);

    int strtab_hdr = lt_strtab_header_size(&strtab);
    int interned_packed_total = strtab_hdr;
    for (int i = 0; i < n; i++)
        interned_packed_total += lt_measure_v2_interned(members[i], &strtab);

    int interned_delta_total = strtab_hdr + lt_measure_v2_interned(members[0], &strtab);
    for (int i = 1; i < n; i++)
        interned_delta_total += lt_delta_measure_v2_interned(members[0], members[i], &strtab);

    /* Huffman */
    LtHuffTable htab;
    lt_huff_init(&htab);
    for (int i = 0; i < n; i++)
        lt_huff_ingest(&htab, members[i]);
    lt_huff_finalize(&htab);

    int huff_hdr = lt_huff_header_size(&htab);
    int huff_packed_total = huff_hdr;
    for (int i = 0; i < n; i++)
        huff_packed_total += lt_measure_v2_huffman(members[i], &htab);

    int huff_delta_total = huff_hdr + lt_measure_v2_huffman(members[0], &htab);
    for (int i = 1; i < n; i++)
        huff_delta_total += lt_delta_measure_v2_huffman(members[0], members[i], &htab);

    /* Arithmetic */
    int arith_packed_total = huff_hdr;
    for (int i = 0; i < n; i++)
        arith_packed_total += lt_measure_v2_arithmetic(members[i], &htab);

    int arith_delta_total = huff_hdr + lt_measure_v2_arithmetic(members[0], &htab);
    for (int i = 1; i < n; i++)
        arith_delta_total += lt_delta_measure_v2_arithmetic(members[0], members[i], &htab);

    printf("  Raw JSON:        %8zu bytes\n", raw_total);
    printf("  ── Packed ──\n");
    printf("  V1 packed:       %8d (%5.1f%%)\n", v1_packed_total, 100.0 * v1_packed_total / (double)raw_total);
    printf("  V2 packed:       %8d (%5.1f%%)\n", v2_packed_total, 100.0 * v2_packed_total / (double)raw_total);
    printf("  Interned packed: %8d (%5.1f%%)\n", interned_packed_total, 100.0 * interned_packed_total / (double)raw_total);
    printf("  Huffman packed:  %8d (%5.1f%%)\n", huff_packed_total, 100.0 * huff_packed_total / (double)raw_total);
    printf("  Arith packed:    %8d (%5.1f%%)\n", arith_packed_total, 100.0 * arith_packed_total / (double)raw_total);
    printf("  ── Gen+Delta ──\n");
    printf("  V1 delta:        %8d (%5.1f%%)\n", v1_delta_total, 100.0 * v1_delta_total / (double)raw_total);
    printf("  V2 delta:        %8d (%5.1f%%)\n", v2_delta_total, 100.0 * v2_delta_total / (double)raw_total);
    printf("  Interned delta:  %8d (%5.1f%%)\n", interned_delta_total, 100.0 * interned_delta_total / (double)raw_total);
    printf("  Huffman delta:   %8d (%5.1f%%)\n", huff_delta_total, 100.0 * huff_delta_total / (double)raw_total);
    printf("  Arith delta:     %8d (%5.1f%%)\n", arith_delta_total, 100.0 * arith_delta_total / (double)raw_total);
    printf("  ── Overhead ──\n");
    printf("  String table:    %8d bytes\n", strtab_hdr);
    printf("  Huffman header:  %8d bytes\n", huff_hdr);
    printf("\n");

    for (int i = 0; i < n; i++) free(members[i]);
    free(members);
    lt_strtab_free(&strtab);
    lt_huff_free(&htab);
}

int main(void) {
    printf("=== liblambda-tensors test suite ===\n\n");

    test_v1_roundtrip();
    test_v2_roundtrip();
    test_v1_delta_roundtrip();
    test_v2_delta_roundtrip();
    test_interned_roundtrip();
    test_huffman_measurement();
    test_arithmetic_measurement();

    printf("\n--- Benchmarks ---\n\n");
    bench_family(100, 14);
    bench_family(1000, 14);
    bench_family(10000, 14);

    printf("=== Results: %d/%d tests passed ===\n",
           tests_passed, tests_run);

    return (tests_passed == tests_run) ? 0 : 1;
}
