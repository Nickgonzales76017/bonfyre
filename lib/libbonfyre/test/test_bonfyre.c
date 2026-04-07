/*
 * test_bonfyre.c — tests for libbonfyre runtime library.
 */
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include "bonfyre.h"

static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) static void test_##name(void)
#define RUN(name) do { \
    printf("  %-50s", #name); \
    test_##name(); \
    printf("PASS\n"); \
    tests_passed++; \
} while(0)

/* ---------------------------------------------------------------- */

TEST(artifact_init) {
    BfArtifact a;
    bf_artifact_init(&a);
    assert(a.artifact_id[0] == '\0');
    assert(a.atoms_count == 0);
    assert(a.component_total == 0);
}

TEST(artifact_parse) {
    const char *json =
        "{"
        "  \"artifact_id\": \"test-123\","
        "  \"artifact_type\": \"transcript\","
        "  \"source_system\": \"BonfyreTranscribe\","
        "  \"created_at\": \"2026-04-06T00:00:00Z\","
        "  \"root_hash\": \"abc123\","
        "  \"atoms\": [{\"id\": \"a1\"}, {\"id\": \"a2\"}],"
        "  \"operators\": [{\"id\": \"op1\"}],"
        "  \"realizations\": [{\"id\": \"r1\"}, {\"id\": \"r2\"}, {\"id\": \"r3\"}]"
        "}";

    BfArtifact a;
    bf_artifact_parse(&a, json);

    assert(strcmp(a.artifact_id, "test-123") == 0);
    assert(strcmp(a.artifact_type, "transcript") == 0);
    assert(strcmp(a.source_system, "BonfyreTranscribe") == 0);
    assert(a.atoms_count == 2);
    assert(a.operators_count == 1);
    assert(a.realizations_count == 3);
    assert(a.component_total == 6);
    assert(a.family_key[0] != '\0');
    assert(a.canonical_key[0] != '\0');
}

TEST(artifact_keys_deterministic) {
    BfArtifact a, b;
    bf_artifact_init(&a);
    bf_artifact_init(&b);
    strcpy(a.artifact_type, "transcript");
    strcpy(a.source_system, "BonfyreTranscribe");
    a.atoms_count = 5; a.operators_count = 1; a.realizations_count = 3;
    strcpy(b.artifact_type, "transcript");
    strcpy(b.source_system, "BonfyreTranscribe");
    b.atoms_count = 5; b.operators_count = 1; b.realizations_count = 3;

    bf_artifact_compute_keys(&a);
    bf_artifact_compute_keys(&b);

    assert(strcmp(a.family_key, b.family_key) == 0);
    assert(strcmp(a.canonical_key, b.canonical_key) == 0);
}

TEST(artifact_family_key_groups) {
    BfArtifact a, b;
    bf_artifact_init(&a);
    bf_artifact_init(&b);
    /* Same type+system, different counts → same family_key, different canonical_key */
    strcpy(a.artifact_type, "brief");
    strcpy(a.source_system, "BonfyreBrief");
    a.atoms_count = 1;
    strcpy(b.artifact_type, "brief");
    strcpy(b.source_system, "BonfyreBrief");
    b.atoms_count = 10;

    bf_artifact_compute_keys(&a);
    bf_artifact_compute_keys(&b);

    assert(strcmp(a.family_key, b.family_key) == 0);
    assert(strcmp(a.canonical_key, b.canonical_key) != 0);
}

TEST(artifact_to_json) {
    BfArtifact a;
    bf_artifact_init(&a);
    strcpy(a.artifact_id, "test-1");
    strcpy(a.artifact_type, "proof");
    strcpy(a.source_system, "BonfyreProof");
    bf_artifact_compute_keys(&a);

    char buf[2048];
    int n = bf_artifact_to_json(&a, buf, sizeof(buf));
    assert(n > 0);
    assert(strstr(buf, "test-1") != NULL);
    assert(strstr(buf, "proof") != NULL);
    assert(strstr(buf, "family_key") != NULL);
}

TEST(sha256_known_vector) {
    /* SHA-256("abc") = ba7816bf... */
    char hex[65];
    bf_sha256_hex((const uint8_t *)"abc", 3, hex);
    assert(strcmp(hex, "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad") == 0);
}

TEST(sha256_empty) {
    char hex[65];
    bf_sha256_hex((const uint8_t *)"", 0, hex);
    assert(strcmp(hex, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855") == 0);
}

TEST(fnv1a_basic) {
    uint64_t h = BF_FNV1A_INIT;
    h = bf_fnv1a64(h, "test", 4);
    assert(h != BF_FNV1A_INIT);

    /* Same input → same hash */
    uint64_t h2 = BF_FNV1A_INIT;
    h2 = bf_fnv1a64(h2, "test", 4);
    assert(h == h2);
}

TEST(normalize_token) {
    char buf[64];
    bf_normalize_token(buf, sizeof(buf), "BonfyreTranscribe");
    assert(strcmp(buf, "bonfyretranscribe") == 0);

    bf_normalize_token(buf, sizeof(buf), "Hello---World");
    assert(strcmp(buf, "hello-world") == 0);

    bf_normalize_token(buf, sizeof(buf), "");
    assert(strcmp(buf, "unknown") == 0);

    bf_normalize_token(buf, sizeof(buf), NULL);
    assert(strcmp(buf, "unknown") == 0);
}

TEST(ensure_dir) {
    /* Just test it doesn't crash on an existing path */
    assert(bf_ensure_dir("/tmp") == 0);
}

TEST(iso_timestamp) {
    char buf[64];
    bf_iso_timestamp(buf, sizeof(buf));
    /* Should look like 2026-04-06T... */
    assert(buf[4] == '-');
    assert(buf[7] == '-');
    assert(buf[10] == 'T');
    assert(buf[strlen(buf) - 1] == 'Z');
}

TEST(arg_has) {
    char *argv[] = {"prog", "--verbose", "--out", "dir"};
    assert(bf_arg_has(4, argv, "--verbose") == 1);
    assert(bf_arg_has(4, argv, "--missing") == 0);
}

TEST(arg_value) {
    char *argv[] = {"prog", "--out", "mydir", "--verbose"};
    assert(strcmp(bf_arg_value(4, argv, "--out"), "mydir") == 0);
    assert(bf_arg_value(4, argv, "--missing") == NULL);
}

TEST(json_str) {
    const char *json = "{\"name\": \"Alice\", \"city\": \"Tokyo\"}";
    char buf[64];
    assert(bf_json_str(json, "name", buf, sizeof(buf)) == 1);
    assert(strcmp(buf, "Alice") == 0);
    assert(bf_json_str(json, "city", buf, sizeof(buf)) == 1);
    assert(strcmp(buf, "Tokyo") == 0);
    assert(bf_json_str(json, "missing", buf, sizeof(buf)) == 0);
}

TEST(json_int) {
    const char *json = "{\"count\": 42, \"name\": \"test\"}";
    int val;
    assert(bf_json_int(json, "count", &val) == 1);
    assert(val == 42);
}

TEST(operator_registry_count) {
    assert(BF_OPERATOR_COUNT == 46);
}

TEST(operator_find) {
    const BfOperator *op = bf_operator_find("bonfyre-transcribe");
    assert(op != NULL);
    assert(strcmp(op->name, "transcribe") == 0);
    assert(op->flags & BF_OP_PURE);
    assert(op->flags & BF_OP_CACHEABLE);
    assert(op->exactness == BF_EXACT_LOSSY);
    assert(strcmp(op->layer, "transform") == 0);
}

TEST(operator_find_by_name) {
    const BfOperator *op = bf_operator_find_by_name("cms");
    assert(op != NULL);
    assert(strcmp(op->binary, "bonfyre-cms") == 0);
    assert(op->flags & BF_OP_STATEFUL);
    assert(strcmp(op->layer, "surface") == 0);
}

TEST(operator_layers) {
    /* Verify substrate layer */
    const BfOperator *op = bf_operator_find_by_name("hash");
    assert(op != NULL);
    assert(strcmp(op->layer, "substrate") == 0);

    /* Verify value layer */
    op = bf_operator_find_by_name("meter");
    assert(op != NULL);
    assert(strcmp(op->layer, "value") == 0);
}

TEST(operator_pure_vs_stateful) {
    /* No operator should be both PURE and STATEFUL */
    for (int i = 0; i < BF_OPERATOR_COUNT; i++) {
        uint32_t f = BF_OPERATORS[i].flags;
        assert(!((f & BF_OP_PURE) && (f & BF_OP_STATEFUL)));
    }
}

/* ---------------------------------------------------------------- */

int main(void) {
    printf("libbonfyre test suite\n");
    printf("=====================\n\n");

    RUN(artifact_init);
    RUN(artifact_parse);
    RUN(artifact_keys_deterministic);
    RUN(artifact_family_key_groups);
    RUN(artifact_to_json);
    RUN(sha256_known_vector);
    RUN(sha256_empty);
    RUN(fnv1a_basic);
    RUN(normalize_token);
    RUN(ensure_dir);
    RUN(iso_timestamp);
    RUN(arg_has);
    RUN(arg_value);
    RUN(json_str);
    RUN(json_int);
    RUN(operator_registry_count);
    RUN(operator_find);
    RUN(operator_find_by_name);
    RUN(operator_layers);
    RUN(operator_pure_vs_stateful);

    printf("\n%d passed, %d failed\n", tests_passed, tests_failed);
    return tests_failed > 0 ? 1 : 0;
}
