/*
 * Focused dispatch test for probe_contract_output().
 * Compiled by including fabric_exec.c directly so the static
 * probe_contract_output symbol is reachable from this translation unit.
 */
#include "../src/fabric_exec.c"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

static int g_failures = 0;

static const char *write_fixture(const char *name, const char *content) {
    static char path[512];
    snprintf(path, sizeof(path), "/tmp/bonfyre-probe-test-%s.txt", name);
    FILE *f = fopen(path, "w");
    if (!f) {
        fprintf(stderr, "FAIL: could not open fixture %s\n", path);
        exit(1);
    }
    fputs(content, f);
    fclose(f);
    return path;
}

static void check(const char *probe, const char *fixture_name, const char *content,
                   int expected, const char *label) {
    const char *path = write_fixture(fixture_name, content);
    int got = probe_contract_output(probe, path, 0, NULL);
    if (got != expected) {
        fprintf(stderr, "FAIL: probe=%s case=%s expected=%d got=%d\n", probe, label, expected, got);
        g_failures++;
    } else {
        printf("PASS: probe=%s case=%s\n", probe, label);
    }
    remove(path);
}

static void check_with_input(const char *probe, const char *fixture_name, const char *content,
                             const char *input_path, int expected, const char *label) {
    const char *path = write_fixture(fixture_name, content);
    int got = probe_contract_output(probe, path, 0, input_path);
    if (got != expected) {
        fprintf(stderr, "FAIL: probe=%s case=%s expected=%d got=%d\n", probe, label, expected, got);
        g_failures++;
    } else {
        printf("PASS: probe=%s case=%s\n", probe, label);
    }
    remove(path);
}

int main(void) {
    /* runtime-semantic now requires the declared --out directory to really
     * exist, proving the child process was genuinely invoked with that
     * dynamically generated path rather than matching a fixed string. */
    mkdir("/tmp/bonfyre-probe-runtime-realdir", 0755);
    check("runtime-semantic", "runtime-pass",
          "runtime-executed --out /tmp/bonfyre-probe-runtime-realdir\n",
          1, "runtime marker present and directory exists");
    check("runtime-semantic", "runtime-fail-no-dir",
          "runtime-executed --out /tmp/bonfyre-probe-runtime-does-not-exist\n",
          0, "runtime marker present but directory missing");
    check("runtime-semantic", "runtime-fail-no-marker",
          "some other output\n",
          0, "runtime marker missing");
    rmdir("/tmp/bonfyre-probe-runtime-realdir");

    /* leapfrog-semantic now parses the real drift/reversibility numbers and
     * requires them within tolerance, not just the presence of headers. */
    check("leapfrog-semantic", "leapfrog-pass",
          "  \xe2\x94\x80\xe2\x94\x80 Hamiltonian drift \xe2\x94\x80\xe2\x94\x80\n"
          "  H_final    : +0.12345678\n"
          "  max |\xce\x94H|   : 0.00010000\n"
          "  mean |\xce\x94H|  : 0.00005000\n"
          "  \xe2\x94\x80\xe2\x94\x80 Reversibility \xe2\x94\x80\xe2\x94\x80\n"
          "  \xe2\x80\x96q_rev - q_0\xe2\x80\x96  : 1.00e-05\n"
          "  \xe2\x80\x96p_rev - p_0\xe2\x80\x96  : 1.00e-05\n"
          "  \xe2\x80\x96p_rev\xe2\x80\x96        : 0.500000  (should match \xe2\x80\x96p_0\xe2\x80\x96 = 0.500000)\n",
          1, "leapfrog drift and reversibility within tolerance");
    check("leapfrog-semantic", "leapfrog-fail-drift",
          "  \xe2\x94\x80\xe2\x94\x80 Hamiltonian drift \xe2\x94\x80\xe2\x94\x80\n"
          "  H_final    : +0.12345678\n"
          "  max |\xce\x94H|   : 5.00000000\n"
          "  mean |\xce\x94H|  : 0.00005000\n"
          "  \xe2\x94\x80\xe2\x94\x80 Reversibility \xe2\x94\x80\xe2\x94\x80\n"
          "  \xe2\x80\x96q_rev - q_0\xe2\x80\x96  : 1.00e-05\n"
          "  \xe2\x80\x96p_rev - p_0\xe2\x80\x96  : 1.00e-05\n"
          "  \xe2\x80\x96p_rev\xe2\x80\x96        : 0.500000  (should match \xe2\x80\x96p_0\xe2\x80\x96 = 0.500000)\n",
          0, "leapfrog drift exceeds tolerance");
    check("leapfrog-semantic", "leapfrog-fail-nan",
          "Hamiltonian drift: nan\nReversibility should match input\n",
          0, "leapfrog contains nan");
    check("leapfrog-semantic", "leapfrog-fail-missing",
          "Hamiltonian drift: 0.0001\n",
          0, "leapfrog missing reversibility marker");

    check("violence-semantic", "violence-pass",
          "FINAL REPORT\nphysics.step  : 32\nnearest score : 1.000000\n",
          1, "violence markers present, no nan");
    check("violence-semantic", "violence-fail",
          "FINAL REPORT\nphysics.step  : 16\nnearest score : 1.000000\n",
          0, "violence wrong step count");

    check("index-semantic", "index-pass",
          "Indexed 12 artifact families into index.sqlite\n",
          1, "index markers present");
    check("index-semantic", "index-fail",
          "Indexed 12 artifact families\n",
          0, "index missing db marker");

    /* wire-ingest-semantic recounts the capture it was handed. What this
     * does not prove: the report strings below are written here, so this
     * shows the probe's dispatch and arithmetic and not the wire organ.
     * BONFYRE_TEST_PCAP names a capture somebody recorded; without one the
     * case is reported as not run. */
    {
        const char *capture = getenv("BONFYRE_TEST_PCAP");
        unsigned long long packets = 0, bytes = 0;
        if (!capture || !*capture) {
            printf("NOT RUN: probe=wire-ingest-semantic (set BONFYRE_TEST_PCAP to a real capture)\n");
        } else if (pcap_record_totals(capture, &packets, &bytes) != 0 || packets == 0) {
            fprintf(stderr, "FAIL: BONFYRE_TEST_PCAP is not a readable capture: %s\n", capture);
            g_failures++;
        } else {
            char report[256];
            snprintf(report, sizeof(report),
                     "{\"packet_count\":%llu,\"byte_count\":%llu,\"mode\":\"metadata-only\"}", packets, bytes);
            check_with_input("wire-ingest-semantic", "wire-pass", report, capture,
                             1, "wire report matches the capture's own records");
            snprintf(report, sizeof(report),
                     "{\"packet_count\":%llu,\"byte_count\":%llu,\"mode\":\"metadata-only\"}", packets + 1, bytes);
            check_with_input("wire-ingest-semantic", "wire-fail", report, capture,
                             0, "wire report with one packet too many");
            check_with_input("wire-ingest-semantic", "wire-not-a-capture", report, __FILE__,
                             0, "input that is not a capture");
        }
    }

    check("detect-objects-semantic", "detect-pass",
          "{\"model\": \"apple-vision/people-r2\", \"frame_count\": 1, \"frames_not_detected\": 0, "
          "\"frames\": [{\"file\": \"a.jpg\", \"detections\": [{\"label\": \"face\", \"confidence\": 0.8}]}]}",
          1, "a detection was reported for the frame");
    check("detect-objects-semantic", "detect-empty",
          "{\"model\": \"apple-vision/people-r2\", \"frame_count\": 1, \"frames_not_detected\": 0, "
          "\"frames\": [{\"file\": \"a.jpg\", \"detections\": []}]}",
          0, "an empty detection list does not certify");

    /* ledger-semantic now cross-checks the reported "Raw bytes" figure
     * against the real input artifact's actual size on disk. */
    {
        char input_path[512];
        struct stat info;
        char report[512];

        snprintf(input_path, sizeof(input_path), "%s", write_fixture("ledger-input", "a real ledger input fixture\n"));
        stat(input_path, &info);
        /* Real bonfyre-ledger reports the size of the artifact's whole
         * containing directory, which is always >= the single input file. */
        snprintf(report, sizeof(report),
                 "Replacement cost:  $        1.23\nPortfolio value:   $        4.56\n"
                 "Raw bytes:     %12lld              \n", (long long)info.st_size + 5000);
        check_with_input("ledger-semantic", "ledger-pass", report, input_path,
                         1, "ledger raw bytes at least cover real input size");
        snprintf(report, sizeof(report),
                 "Replacement cost:  $        1.23\nPortfolio value:   $        4.56\n"
                 "Raw bytes:     %12lld              \n", (long long)info.st_size - 1);
        check_with_input("ledger-semantic", "ledger-fail", report, input_path,
                         0, "ledger raw bytes smaller than real input size is impossible");
        remove(input_path);
    }

    /* compress-roundtrip-semantic performs a real zstd decode and compares
     * digests against the original input -- not just presence of output. */
    {
        const char *input_path = write_fixture("compress-input",
            "Bonfyre completion validates durable evidence via a real compression roundtrip.\n");
        char compressed_path[256];
        char command[1024];

        snprintf(compressed_path, sizeof(compressed_path), "/tmp/bonfyre-probe-test-compress-output.zst");
        remove(compressed_path);
        snprintf(command, sizeof(command), "zstd -q -f '%s' -o '%s'", input_path, compressed_path);
        if (system(command) == 0) {
            int got = probe_contract_output("compress-roundtrip-semantic", compressed_path, 0, input_path);
            if (got != 1) {
                fprintf(stderr, "FAIL: probe=compress-roundtrip-semantic case=real zstd roundtrip decodes to matching digest expected=1 got=%d\n", got);
                g_failures++;
            } else {
                printf("PASS: probe=compress-roundtrip-semantic case=real zstd roundtrip decodes to matching digest\n");
            }

            /* Corrupt the compressed artifact and prove the probe now rejects it. */
            {
                FILE *f = fopen(compressed_path, "r+b");
                if (f) { fseek(f, 8, SEEK_SET); fputc('\xff', f); fputc('\xff', f); fclose(f); }
            }
            got = probe_contract_output("compress-roundtrip-semantic", compressed_path, 0, input_path);
            if (got != 0) {
                fprintf(stderr, "FAIL: probe=compress-roundtrip-semantic case=corrupted archive rejected expected=0 got=%d\n", got);
                g_failures++;
            } else {
                printf("PASS: probe=compress-roundtrip-semantic case=corrupted archive rejected\n");
            }
        } else {
            fprintf(stderr, "SKIP: zstd unavailable, compress-roundtrip-semantic case not exercised\n");
        }
        remove(compressed_path);
        remove(input_path);
    }

    if (g_failures) {
        fprintf(stderr, "\n%d probe dispatch check(s) failed\n", g_failures);
        return 1;
    }
    printf("\nall probe dispatch checks passed\n");
    return 0;
}
