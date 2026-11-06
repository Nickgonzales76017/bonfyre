#define _GNU_SOURCE
#define _DARWIN_C_SOURCE
#include "bonfyre.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* Only fopen/fclose in the hash translation unit are redirected. Reads and
 * stream error flags are the platform's stdio, including partial-read errors. */
typedef struct { int prefix; int delivered; } Fault;
static FILE *close_failure;
static int closed;
#ifndef BF_SHA256_FAULT_IO_ONLY
static int failures, checks;
#endif
#ifdef __APPLE__
static int fault_read(void *cookie, char *buf, int size) {
#else
static ssize_t fault_read(void *cookie, char *buf, size_t size) {
#endif
    Fault *fault = cookie;
    if (fault->prefix && !fault->delivered && size >= 3) {
        memcpy(buf, "abc", 3); fault->delivered = 1; return 3;
    }
    errno = EIO; return -1;
}
static int fault_close(void *cookie) { free(cookie); closed++; return 0; }

FILE *bf_test_fopen(const char *path, const char *mode) {
    const char *failed_path = getenv("BF_TEST_SHA256_FAIL");
    int injected = failed_path && !strcmp(path, failed_path);
    if (injected || !strcmp(path, ":read-error") || !strcmp(path, ":partial-error")) {
        Fault *fault = calloc(1, sizeof(*fault));
        if (!fault) return NULL;
        fault->prefix = injected || !strcmp(path, ":partial-error");
#ifdef __APPLE__
        FILE *stream = funopen(fault, fault_read, NULL, NULL, fault_close);
#else
        cookie_io_functions_t io = {.read = fault_read, .close = fault_close};
        FILE *stream = fopencookie(fault, "r", io);
#endif
        if (!stream) free(fault);
        return stream;
    }
    if (!strcmp(path, ":close-error")) {
        close_failure = tmpfile();
        if (close_failure) { fputs("abc", close_failure); rewind(close_failure); }
        return close_failure;
    }
    return fopen(path, mode);
}
int bf_test_fclose(FILE *stream) {
    int fail = stream == close_failure;
    if (fail) close_failure = NULL;
    int result = fclose(stream);
    if (fail) { closed++; errno = EIO; return EOF; }
    return result;
}
#ifndef BF_SHA256_FAULT_IO_ONLY
static void check(int ok, const char *name) {
    checks++;
    if (!ok) { fprintf(stderr, "FAIL: %s\n", name); failures++; }
}
static void refused(const char *path, int expected_errno) {
    char hex[65]; memset(hex, 'x', sizeof(hex)); errno = 0;
    int result = bf_sha256_file(path, hex), error = errno;
    check(result == -1, path);
    check(hex[0] == '\0', "failure leaves no digest");
    if (expected_errno) check(error == expected_errno, "failure preserves errno");
}
int main(void) {
    char dir[] = "/tmp/bonfyre-sha256-XXXXXX";
    if (!mkdtemp(dir)) return 2;
    char path[256], absent[256], hex[65];
    snprintf(path, sizeof(path), "%s/input", dir);
    snprintf(absent, sizeof(absent), "%s/missing", dir);
    FILE *file = fopen(path, "wb"); if (!file) return 2; fclose(file);
    check(bf_sha256_file(path, hex) == 0 && !strcmp(hex, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"), "empty file");
    file = fopen(path, "wb"); if (!file) return 2; fputs("abc", file); fclose(file);
    check(bf_sha256_file(path, hex) == 0 && !strcmp(hex, "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"), "abc vector");
    file = fopen(path, "wb"); if (!file) return 2;
    for (int i = 0; i < 8193; i++) fputc(i % 251, file);
    fclose(file);
    check(bf_sha256_file(path, hex) == 0 && !strcmp(hex, "7e3691790cd64b19d4edb1a80e988214515abeb53aa0f34ffbfe4b4bf405d120"), "buffer boundary and partial tail");
    refused(absent, ENOENT);
    refused(dir, 0);
    refused(":read-error", EIO);
    refused(":partial-error", EIO);
    refused(":close-error", EIO);
    check(closed == 3, "all fault streams closed");
    unlink(path); rmdir(dir);
    printf("sha256 file: %d checks, %d failures\n", checks, failures);
    return failures ? 1 : 0;
}
#endif
