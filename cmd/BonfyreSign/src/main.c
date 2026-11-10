/* The institution's signature.
 *
 * The estate holds 31,000 evidence rows and every one carries digests: the
 * digest of what an owner read and of what it produced. A digest says the bytes
 * did not change. It does not say who produced them, so every claim in the
 * estate is checkable by the estate and by nobody else. That is the difference
 * between "we recorded this" and "here is a receipt anyone can verify".
 *
 * bf_crypto could not close that gap. Its header advertised Ed25519 and it
 * implemented a symmetric tag: `pk` was H(seed) and verified nothing, and
 * bf_crypto_sign_verify was a stub returning -1 for every input. Nothing called
 * it, so nothing was broken, but the next caller to need a signature would have
 * published a key that cannot check anything.
 *
 * This is the real thing, via libsodium, kept in its own binary so libbonfyre --
 * which every other organ links -- does not acquire a dependency it does not
 * need.
 *
 * What this key is, and is not. It is an identity and attestation key: it signs
 * statements this estate makes about work it did. It is not a payment key, it
 * holds no funds, and it is deliberately not the key an x402 payment would be
 * authorized with. Those are different powers and giving one key both is how an
 * attestation becomes a withdrawal.
 *
 * Cannot infer: a valid signature -> a true statement. It says this estate
 * made the statement and that the bytes have not moved since. Whether the
 * statement is correct is the evidence's problem, not the signature's.
 */
#define _POSIX_C_SOURCE 200809L
#include <sodium.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

#define SK_FILE "institution.ed25519.secret"
#define PK_FILE "institution.ed25519.public"

static const char *arg(int argc, char **argv, const char *flag) {
    for (int i = 2; i + 1 < argc; i++)
        if (!strcmp(argv[i], flag)) return argv[i + 1];
    return NULL;
}

static void hex_of(const unsigned char *raw, size_t n, char *out) {
    static const char *digits = "0123456789abcdef";
    for (size_t i = 0; i < n; i++) {
        out[i * 2] = digits[raw[i] >> 4];
        out[i * 2 + 1] = digits[raw[i] & 0x0f];
    }
    out[n * 2] = '\0';
}

static int unhex(const char *hex, unsigned char *out, size_t want) {
    if (!hex || strlen(hex) != want * 2) return 0;
    for (size_t i = 0; i < want; i++) {
        unsigned v;
        if (sscanf(hex + i * 2, "%2x", &v) != 1) return 0;
        out[i] = (unsigned char)v;
    }
    return 1;
}

static int read_whole(const char *path, unsigned char **out, size_t *len) {
    FILE *fh = fopen(path, "rb");
    long size;
    if (!fh) return 0;
    if (fseek(fh, 0, SEEK_END) || (size = ftell(fh)) < 0) { fclose(fh); return 0; }
    rewind(fh);
    *out = malloc((size_t)size + 1);
    if (!*out) { fclose(fh); return 0; }
    *len = fread(*out, 1, (size_t)size, fh);
    (*out)[*len] = '\0';
    fclose(fh);
    return 1;
}

static int read_key_hex(const char *dir, const char *name, unsigned char *out, size_t want) {
    char path[1024], hex[256];
    FILE *fh;
    snprintf(path, sizeof(path), "%s/%s", dir, name);
    if (!(fh = fopen(path, "r"))) return 0;
    if (!fgets(hex, sizeof(hex), fh)) { fclose(fh); return 0; }
    fclose(fh);
    hex[strcspn(hex, "\r\n")] = '\0';
    return unhex(hex, out, want);
}

/* The secret is written before anything else can see it. A signing key that is
 * world-readable for even one moment has been published. */
static int write_secret(const char *path, const char *hex) {
    int fd;
    FILE *fh;
    if ((fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0600)) < 0) return 0;
    if (!(fh = fdopen(fd, "w"))) { close(fd); return 0; }
    fprintf(fh, "%s\n", hex);
    fclose(fh);
    return 1;
}

static int cmd_keygen(int argc, char **argv) {
    const char *dir = arg(argc, argv, "--key-dir");
    unsigned char pk[crypto_sign_PUBLICKEYBYTES], sk[crypto_sign_SECRETKEYBYTES];
    char pk_hex[crypto_sign_PUBLICKEYBYTES * 2 + 1], sk_hex[crypto_sign_SECRETKEYBYTES * 2 + 1];
    char path[1024];
    FILE *fh;
    if (!dir) { fprintf(stderr, "keygen requires --key-dir\n"); return 2; }
    snprintf(path, sizeof(path), "%s/%s", dir, SK_FILE);
    /* Refusing rather than overwriting: a regenerated identity silently
     * invalidates every signature the previous one made. */
    if (!access(path, F_OK)) {
        fprintf(stderr, "%s already exists; refusing to replace an identity that "
                        "has already signed things\n", path);
        return 3;
    }
    crypto_sign_keypair(pk, sk);
    hex_of(pk, sizeof(pk), pk_hex);
    hex_of(sk, sizeof(sk), sk_hex);
    if (!write_secret(path, sk_hex)) {
        fprintf(stderr, "cannot write %s: %s\n", path, strerror(errno));
        return 1;
    }
    snprintf(path, sizeof(path), "%s/%s", dir, PK_FILE);
    if (!(fh = fopen(path, "w"))) return 1;
    fprintf(fh, "%s\n", pk_hex);
    fclose(fh);
    printf("{\"schema\":\"bonfyre-institution-identity.v1\",\"algorithm\":\"ed25519\","
           "\"public_key\":\"%s\",\"secret_mode\":\"0600\","
           "\"purpose\":\"identity and attestation\","
           "\"cannot_infer\":[\"this key -> a payment key; it holds no funds and authorizes no transfer\","
           "\"a valid signature -> a true statement\"]}\n", pk_hex);
    return 0;
}

static int cmd_sign(int argc, char **argv) {
    const char *dir = arg(argc, argv, "--key-dir");
    const char *file = arg(argc, argv, "--file");
    unsigned char sk[crypto_sign_SECRETKEYBYTES], pk[crypto_sign_PUBLICKEYBYTES];
    unsigned char sig[crypto_sign_BYTES], *body = NULL, digest[crypto_hash_sha256_BYTES];
    char sig_hex[crypto_sign_BYTES * 2 + 1], pk_hex[crypto_sign_PUBLICKEYBYTES * 2 + 1];
    char digest_hex[crypto_hash_sha256_BYTES * 2 + 1], stamp[32];
    size_t len = 0;
    time_t now;
    struct tm tm;
    if (!dir || !file) { fprintf(stderr, "sign requires --key-dir and --file\n"); return 2; }
    if (!read_key_hex(dir, SK_FILE, sk, sizeof(sk))) { fprintf(stderr, "no usable secret in %s\n", dir); return 1; }
    if (!read_key_hex(dir, PK_FILE, pk, sizeof(pk))) { fprintf(stderr, "no usable public key in %s\n", dir); return 1; }
    if (!read_whole(file, &body, &len)) { fprintf(stderr, "cannot read %s\n", file); return 1; }
    /* Sign the digest rather than the body: the evidence file is tens of
     * megabytes and a detached signature over its hash is the same assertion at
     * a size anyone can carry. */
    crypto_hash_sha256(digest, body, len);
    crypto_sign_detached(sig, NULL, digest, sizeof(digest), sk);
    free(body);
    hex_of(sig, sizeof(sig), sig_hex);
    hex_of(pk, sizeof(pk), pk_hex);
    hex_of(digest, sizeof(digest), digest_hex);
    now = time(NULL);
    gmtime_r(&now, &tm);
    strftime(stamp, sizeof(stamp), "%Y-%m-%dT%H:%M:%SZ", &tm);
    printf("{\"schema\":\"bonfyre-signed-attestation.v1\",\"file\":\"%s\",\"bytes\":%zu,"
           "\"content_sha256\":\"%s\",\"algorithm\":\"ed25519\",\"public_key\":\"%s\","
           "\"signature\":\"%s\",\"signed_at\":\"%s\","
           "\"cannot_infer\":[\"a valid signature -> a true statement; it says this estate made it and the bytes have not moved\","
           "\"an attestation -> a payment authorization\"]}\n",
           file, len, digest_hex, pk_hex, sig_hex, stamp);
    return 0;
}

static int cmd_verify(int argc, char **argv) {
    const char *file = arg(argc, argv, "--file");
    const char *pk_hex = arg(argc, argv, "--public");
    const char *sig_hex = arg(argc, argv, "--signature");
    unsigned char pk[crypto_sign_PUBLICKEYBYTES], sig[crypto_sign_BYTES];
    unsigned char *body = NULL, digest[crypto_hash_sha256_BYTES];
    size_t len = 0;
    int ok;
    if (!file || !pk_hex || !sig_hex) {
        fprintf(stderr, "verify requires --file --public --signature\n");
        return 2;
    }
    if (!unhex(pk_hex, pk, sizeof(pk)) || !unhex(sig_hex, sig, sizeof(sig))) {
        fprintf(stderr, "public key or signature is not the right length\n");
        return 2;
    }
    if (!read_whole(file, &body, &len)) { fprintf(stderr, "cannot read %s\n", file); return 1; }
    crypto_hash_sha256(digest, body, len);
    free(body);
    ok = crypto_sign_verify_detached(sig, digest, sizeof(digest), pk) == 0;
    printf("{\"schema\":\"bonfyre-signature-verification.v1\",\"file\":\"%s\","
           "\"verified\":%s,\"algorithm\":\"ed25519\","
           "\"cannot_infer\":[\"verified -> the statement being true\"]}\n",
           file, ok ? "true" : "false");
    return ok ? 0 : 4;
}

static void usage(void) {
    fputs("bonfyre-sign — the institution's identity and attestation key (Ed25519)\n\n"
          "  keygen --key-dir DIR            create the identity; refuses to replace one\n"
          "  sign   --key-dir DIR --file F   detached signature over the file's digest\n"
          "  verify --file F --public HEX --signature HEX\n\n"
          "This key signs statements about work. It is not a payment key, holds no\n"
          "funds, and authorizes no transfer.\n", stderr);
}

int main(int argc, char **argv) {
    if (sodium_init() < 0) { fprintf(stderr, "libsodium failed to initialise\n"); return 1; }
    if (argc < 2) { usage(); return 2; }
    enum { SG_KEYGEN, SG_SIGN, SG_VERIFY };
    static const BfCommandEntry sign_commands[] = {
        {"keygen", SG_KEYGEN, 0, NULL},
        {"sign", SG_SIGN, 0, NULL},
        {"verify", SG_VERIFY, 0, NULL},
    };
    const BfCommandEntry *command = bf_command_lookup(sign_commands,
        sizeof(sign_commands) / sizeof(sign_commands[0]), argv[1]);
    if (command && command->id == SG_KEYGEN) return cmd_keygen(argc, argv);
    if (command && command->id == SG_SIGN)   return cmd_sign(argc, argv);
    if (command && command->id == SG_VERIFY) return cmd_verify(argc, argv);
    usage();
    return 2;
}
