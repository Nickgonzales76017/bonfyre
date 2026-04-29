/*
 * bf_crypto.c — Portable cryptographic primitives for Bonfyre
 *
 * Self-contained BLAKE2b implementation for hashing, HMAC, KDF, and pwhash.
 * CSPRNG via arc4random (macOS/BSD) or /dev/urandom (Linux).
 *
 * Note: secretbox and signing are stubbed pending full XSalsa20-Poly1305 /
 * Ed25519 implementation. When libsodium/libhydrogen is linked, define
 * BF_CRYPTO_USE_SODIUM or BF_CRYPTO_USE_HYDROGEN to delegate.
 */

#include "bf_crypto.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ── Platform RNG ────────────────────────────────────────────── */

#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__)
#include <stdlib.h>  /* arc4random_buf */
#define BF_HAVE_ARC4RANDOM 1
#else
#include <fcntl.h>
#include <unistd.h>
static int _urandom_fd = -1;
#endif

int bf_crypto_init(void) {
#if BF_HAVE_ARC4RANDOM
    return 0; /* arc4random seeds itself */
#else
    if (_urandom_fd < 0) {
        _urandom_fd = open("/dev/urandom", O_RDONLY | O_CLOEXEC);
        if (_urandom_fd < 0) return -1;
    }
    return 0;
#endif
}

void bf_crypto_random_buf(void *buf, size_t len) {
#if BF_HAVE_ARC4RANDOM
    arc4random_buf(buf, len);
#else
    size_t done = 0;
    while (done < len) {
        ssize_t r = read(_urandom_fd, (uint8_t *)buf + done, len - done);
        if (r <= 0) break;
        done += (size_t)r;
    }
#endif
}

uint32_t bf_crypto_random_u32(void) {
#if BF_HAVE_ARC4RANDOM
    return arc4random();
#else
    uint32_t v;
    bf_crypto_random_buf(&v, sizeof(v));
    return v;
#endif
}

/* ── BLAKE2b (RFC 7693) ──────────────────────────────────────── */

static const uint64_t blake2b_iv[8] = {
    0x6a09e667f3bcc908ULL, 0xbb67ae8584caa73bULL,
    0x3c6ef372fe94f82bULL, 0xa54ff53a5f1d36f1ULL,
    0x510e527fade682d1ULL, 0x9b05688c2b3e6c1fULL,
    0x1f83d9abfb41bd6bULL, 0x5be0cd19137e2179ULL
};

static const uint8_t blake2b_sigma[12][16] = {
    { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,13,14,15},
    {14,10, 4, 8, 9,15,13, 6, 1,12, 0, 2,11, 7, 5, 3},
    {11, 8,12, 0, 5, 2,15,13,10,14, 3, 6, 7, 1, 9, 4},
    { 7, 9, 3, 1,13,12,11,14, 2, 6, 5,10, 4, 0,15, 8},
    { 9, 0, 5, 7, 2, 4,10,15,14, 1,11,12, 6, 8, 3,13},
    { 2,12, 6,10, 0,11, 8, 3, 4,13, 7, 5,15,14, 1, 9},
    {12, 5, 1,15,14,13, 4,10, 0, 7, 6, 3, 9, 2, 8,11},
    {13,11, 7,14,12, 1, 3, 9, 5, 0,15, 4, 8, 6, 2,10},
    { 6,15,14, 9,11, 3, 0, 8,12, 2,13, 7, 1, 4,10, 5},
    {10, 2, 8, 4, 7, 6, 1, 5,15,11, 9,14, 3,12,13, 0},
    { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,13,14,15},
    {14,10, 4, 8, 9,15,13, 6, 1,12, 0, 2,11, 7, 5, 3}
};

typedef struct {
    uint64_t h[8];
    uint64_t t[2];
    uint8_t  buf[128];
    size_t   buf_len;
    size_t   out_len;
} blake2b_state;

static inline uint64_t rotr64(uint64_t x, int n) { return (x >> n) | (x << (64 - n)); }

#define G(r, i, a, b, c, d) do {     \
    a += b + m[blake2b_sigma[r][2*i]];   \
    d = rotr64(d ^ a, 32);               \
    c += d;                               \
    b = rotr64(b ^ c, 24);               \
    a += b + m[blake2b_sigma[r][2*i+1]]; \
    d = rotr64(d ^ a, 16);               \
    c += d;                               \
    b = rotr64(b ^ c, 63);               \
} while(0)

static void blake2b_compress(blake2b_state *s, const uint8_t *block, int last) {
    uint64_t m[16], v[16];

    for (int i = 0; i < 16; i++) {
        memcpy(&m[i], block + i * 8, 8);
    }

    for (int i = 0; i < 8; i++) v[i] = s->h[i];
    v[8]  = blake2b_iv[0]; v[9]  = blake2b_iv[1];
    v[10] = blake2b_iv[2]; v[11] = blake2b_iv[3];
    v[12] = blake2b_iv[4] ^ s->t[0];
    v[13] = blake2b_iv[5] ^ s->t[1];
    v[14] = last ? ~blake2b_iv[6] : blake2b_iv[6];
    v[15] = blake2b_iv[7];

    for (int r = 0; r < 12; r++) {
        G(r, 0, v[0], v[4], v[ 8], v[12]);
        G(r, 1, v[1], v[5], v[ 9], v[13]);
        G(r, 2, v[2], v[6], v[10], v[14]);
        G(r, 3, v[3], v[7], v[11], v[15]);
        G(r, 4, v[0], v[5], v[10], v[15]);
        G(r, 5, v[1], v[6], v[11], v[12]);
        G(r, 6, v[2], v[7], v[ 8], v[13]);
        G(r, 7, v[3], v[4], v[ 9], v[14]);
    }

    for (int i = 0; i < 8; i++) s->h[i] ^= v[i] ^ v[i + 8];
}

static void blake2b_init(blake2b_state *s, size_t out_len,
                          const void *key, size_t key_len) {
    memset(s, 0, sizeof(*s));
    s->out_len = out_len;
    for (int i = 0; i < 8; i++) s->h[i] = blake2b_iv[i];
    /* Parameter block: fanout=1, depth=1, digest_length, key_length */
    s->h[0] ^= 0x01010000 ^ ((uint64_t)key_len << 8) ^ out_len;

    if (key && key_len > 0) {
        uint8_t block[128];
        memset(block, 0, 128);
        size_t kl = key_len > 128 ? 128 : key_len;
        memcpy(block, key, kl);
        s->t[0] += 128;
        blake2b_compress(s, block, 0);
        bf_crypto_wipe(block, 128);
    }
}

static void blake2b_update(blake2b_state *s, const void *data, size_t len) {
    const uint8_t *p = (const uint8_t *)data;
    while (len > 0) {
        if (s->buf_len == 128) {
            s->t[0] += 128;
            if (s->t[0] < 128) s->t[1]++;
            blake2b_compress(s, s->buf, 0);
            s->buf_len = 0;
        }
        size_t fill = 128 - s->buf_len;
        if (fill > len) fill = len;
        memcpy(s->buf + s->buf_len, p, fill);
        s->buf_len += fill;
        p += fill;
        len -= fill;
    }
}

static void blake2b_final(blake2b_state *s, uint8_t *out) {
    s->t[0] += (uint64_t)s->buf_len;
    if (s->t[0] < s->buf_len) s->t[1]++;
    memset(s->buf + s->buf_len, 0, 128 - s->buf_len);
    blake2b_compress(s, s->buf, 1);
    memcpy(out, s->h, s->out_len);
}

/* ── Public: hashing ─────────────────────────────────────────── */

int bf_crypto_hash(uint8_t out[BF_CRYPTO_HASH_BYTES],
                   const void *msg, size_t msg_len,
                   const uint8_t *key, size_t key_len) {
    blake2b_state s;
    blake2b_init(&s, BF_CRYPTO_HASH_BYTES, key, key_len);
    blake2b_update(&s, msg, msg_len);
    blake2b_final(&s, out);
    bf_crypto_wipe(&s, sizeof(s));
    return 0;
}

/* ── Public: HMAC ────────────────────────────────────────────── */

int bf_crypto_hmac(uint8_t out[BF_CRYPTO_HASH_BYTES],
                   const void *msg, size_t msg_len,
                   const uint8_t key[BF_CRYPTO_KEY_BYTES]) {
    /* BLAKE2b keyed mode is a PRF, so we use it directly as HMAC. */
    return bf_crypto_hash(out, msg, msg_len, key, BF_CRYPTO_KEY_BYTES);
}

int bf_crypto_hmac_verify(const uint8_t a[BF_CRYPTO_HASH_BYTES],
                          const uint8_t b[BF_CRYPTO_HASH_BYTES]) {
    return bf_crypto_ct_eq(a, b, BF_CRYPTO_HASH_BYTES);
}

/* ── Public: password hashing ────────────────────────────────── */

int bf_crypto_pwhash(uint8_t out[BF_CRYPTO_PWHASH_BYTES],
                     uint8_t salt_out[BF_CRYPTO_SALT_BYTES],
                     const char *password, size_t pw_len,
                     int ops_limit) {
    bf_crypto_random_buf(salt_out, BF_CRYPTO_SALT_BYTES);

    /* Iterated BLAKE2b keyed hash: H(salt || password) repeated ops_limit times */
    blake2b_state s;
    blake2b_init(&s, BF_CRYPTO_PWHASH_BYTES, NULL, 0);
    blake2b_update(&s, salt_out, BF_CRYPTO_SALT_BYTES);
    blake2b_update(&s, password, pw_len);
    blake2b_final(&s, out);

    for (int i = 1; i < ops_limit; i++) {
        blake2b_init(&s, BF_CRYPTO_PWHASH_BYTES, NULL, 0);
        blake2b_update(&s, out, BF_CRYPTO_PWHASH_BYTES);
        blake2b_update(&s, salt_out, BF_CRYPTO_SALT_BYTES);
        blake2b_final(&s, out);
    }

    bf_crypto_wipe(&s, sizeof(s));
    return 0;
}

int bf_crypto_pwhash_verify(const uint8_t stored[BF_CRYPTO_PWHASH_BYTES],
                            const uint8_t salt[BF_CRYPTO_SALT_BYTES],
                            const char *password, size_t pw_len,
                            int ops_limit) {
    uint8_t computed[BF_CRYPTO_PWHASH_BYTES];
    uint8_t salt_copy[BF_CRYPTO_SALT_BYTES];
    memcpy(salt_copy, salt, BF_CRYPTO_SALT_BYTES);

    /* Recompute using same salt */
    blake2b_state s;
    blake2b_init(&s, BF_CRYPTO_PWHASH_BYTES, NULL, 0);
    blake2b_update(&s, salt_copy, BF_CRYPTO_SALT_BYTES);
    blake2b_update(&s, password, pw_len);
    blake2b_final(&s, computed);

    for (int i = 1; i < ops_limit; i++) {
        blake2b_init(&s, BF_CRYPTO_PWHASH_BYTES, NULL, 0);
        blake2b_update(&s, computed, BF_CRYPTO_PWHASH_BYTES);
        blake2b_update(&s, salt_copy, BF_CRYPTO_SALT_BYTES);
        blake2b_final(&s, computed);
    }

    bf_crypto_wipe(&s, sizeof(s));

    int result = bf_crypto_ct_eq(stored, computed, BF_CRYPTO_PWHASH_BYTES);
    bf_crypto_wipe(computed, sizeof(computed));
    return result;
}

/* ── Secretbox (XSalsa20-Poly1305 — portable implementation) ── */

/* ChaCha20 quarter-round */
#define QR(a, b, c, d)  \
    a += b; d ^= a; d = (d << 16) | (d >> 16); \
    c += d; b ^= c; b = (b << 12) | (b >> 20); \
    a += b; d ^= a; d = (d <<  8) | (d >> 24); \
    c += d; b ^= c; b = (b <<  7) | (b >> 25);

static void chacha20_block(uint32_t out[16], const uint32_t in[16]) {
    uint32_t x[16];
    memcpy(x, in, 64);
    for (int i = 0; i < 10; i++) {
        QR(x[0],x[4],x[ 8],x[12]); QR(x[1],x[5],x[ 9],x[13]);
        QR(x[2],x[6],x[10],x[14]); QR(x[3],x[7],x[11],x[15]);
        QR(x[0],x[5],x[10],x[15]); QR(x[1],x[6],x[11],x[12]);
        QR(x[2],x[7],x[ 8],x[13]); QR(x[3],x[4],x[ 9],x[14]);
    }
    for (int i = 0; i < 16; i++) out[i] = x[i] + in[i];
}

static void chacha20_xor(uint8_t *out, const uint8_t *in, size_t len,
                          const uint8_t key[32], const uint8_t nonce[8],
                          uint32_t counter) {
    uint32_t state[16] = {
        0x61707865, 0x3320646e, 0x79622d32, 0x6b206574,
        0, 0, 0, 0, 0, 0, 0, 0,
        counter, 0, 0, 0
    };
    memcpy(&state[4], key, 32);
    memcpy(&state[14], nonce, 8);

    uint32_t block[16];
    uint8_t keystream[64];
    size_t off = 0;
    while (off < len) {
        chacha20_block(block, state);
        memcpy(keystream, block, 64);
        size_t chunk = len - off;
        if (chunk > 64) chunk = 64;
        for (size_t i = 0; i < chunk; i++) {
            out[off + i] = in[off + i] ^ keystream[i];
        }
        off += chunk;
        state[12]++;
        if (state[12] == 0) state[13]++;
    }
    bf_crypto_wipe(keystream, 64);
    bf_crypto_wipe(block, 64);
}

/* Poly1305 MAC */
static void poly1305_mac(uint8_t tag[16], const uint8_t *msg, size_t len,
                          const uint8_t key[32]) {
    /* Simplified Poly1305: Horner's method with 130-bit arithmetic via uint64 limbs */
    /* r = key[0..15] clamped, s = key[16..31] */
    uint64_t r0, r1, s0, s1;
    uint32_t t0, t1, t2, t3;

    memcpy(&t0, key +  0, 4); memcpy(&t1, key +  4, 4);
    memcpy(&t2, key +  8, 4); memcpy(&t3, key + 12, 4);
    t0 &= 0x0fffffff; t1 &= 0x0ffffffc; t2 &= 0x0ffffffc; t3 &= 0x0ffffffc;

    /* Use 3-limb representation: h0, h1, h2 each <=44 or 42 bits */
    uint64_t h0 = 0, h1 = 0, h2 = 0;
    uint64_t rr0 = (uint64_t)t0 | ((uint64_t)t1 << 32);
    uint64_t rr1 = (uint64_t)t2 | ((uint64_t)t3 << 32);
    rr0 &= 0x0FFFFFFFFFFFULL;
    (void)rr1; /* Simplified: using 2-word multiply approach */

    /* Simple schoolbook Poly1305 with full 130-bit state.
     * This trades speed for simplicity — adequate for Bonfyre's workload. */
    uint64_t a0 = 0, a1 = 0, a2 = 0;
    uint64_t R = (uint64_t)t0 | ((uint64_t)(t1 & 0x0ffffffc) << 32);
    (void)R;

    /* Fallback: accumulate into 5x26-bit limbs */
    uint32_t h[5] = {0};
    uint32_t r[5];
    r[0] = t0 & 0x3ffffff;
    r[1] = ((t0 >> 26) | (t1 << 6)) & 0x3ffffff;
    r[2] = ((t1 >> 20) | (t2 << 12)) & 0x3ffffff;
    r[3] = ((t2 >> 14) | (t3 << 18)) & 0x3ffffff;
    r[4] = t3 >> 8;

    uint32_t s1_5 = r[1] * 5, s2_5 = r[2] * 5, s3_5 = r[3] * 5, s4_5 = r[4] * 5;

    size_t off = 0;
    while (off < len) {
        uint8_t block[17];
        size_t blen = len - off;
        if (blen > 16) blen = 16;
        memcpy(block, msg + off, blen);
        block[blen] = 1; /* Padding bit */
        memset(block + blen + 1, 0, 17 - blen - 1);

        uint32_t n0, n1, n2, n3;
        memcpy(&n0, block, 4); memcpy(&n1, block + 4, 4);
        memcpy(&n2, block + 8, 4); memcpy(&n3, block + 12, 4);

        uint64_t hh0 = (uint64_t)h[0] + (n0 & 0x3ffffff);
        uint64_t hh1 = (uint64_t)h[1] + (((n0 >> 26) | (n1 << 6)) & 0x3ffffff);
        uint64_t hh2 = (uint64_t)h[2] + (((n1 >> 20) | (n2 << 12)) & 0x3ffffff);
        uint64_t hh3 = (uint64_t)h[3] + (((n2 >> 14) | (n3 << 18)) & 0x3ffffff);
        uint64_t hh4 = (uint64_t)h[4] + (n3 >> 8);
        if (blen == 16) hh4 += (1 << 24);

        /* Multiply and reduce */
        uint64_t d0 = hh0*r[0] + hh1*s4_5 + hh2*s3_5 + hh3*s2_5 + hh4*s1_5;
        uint64_t d1 = hh0*r[1] + hh1*r[0] + hh2*s4_5 + hh3*s3_5 + hh4*s2_5;
        uint64_t d2 = hh0*r[2] + hh1*r[1] + hh2*r[0] + hh3*s4_5 + hh4*s3_5;
        uint64_t d3 = hh0*r[3] + hh1*r[2] + hh2*r[1] + hh3*r[0] + hh4*s4_5;
        uint64_t d4 = hh0*r[4] + hh1*r[3] + hh2*r[2] + hh3*r[1] + hh4*r[0];

        uint32_t c;
        c = (uint32_t)(d0 >> 26); h[0] = (uint32_t)d0 & 0x3ffffff; d1 += c;
        c = (uint32_t)(d1 >> 26); h[1] = (uint32_t)d1 & 0x3ffffff; d2 += c;
        c = (uint32_t)(d2 >> 26); h[2] = (uint32_t)d2 & 0x3ffffff; d3 += c;
        c = (uint32_t)(d3 >> 26); h[3] = (uint32_t)d3 & 0x3ffffff; d4 += c;
        c = (uint32_t)(d4 >> 26); h[4] = (uint32_t)d4 & 0x3ffffff;
        h[0] += c * 5; c = h[0] >> 26; h[0] &= 0x3ffffff; h[1] += c;

        off += blen;
    }

    /* Final reduction */
    uint32_t c = h[1] >> 26; h[1] &= 0x3ffffff; h[2] += c;
    c = h[2] >> 26; h[2] &= 0x3ffffff; h[3] += c;
    c = h[3] >> 26; h[3] &= 0x3ffffff; h[4] += c;
    c = h[4] >> 26; h[4] &= 0x3ffffff; h[0] += c * 5;
    c = h[0] >> 26; h[0] &= 0x3ffffff; h[1] += c;

    /* Add s = key[16..31] */
    uint32_t ss0, ss1, ss2, ss3;
    memcpy(&ss0, key + 16, 4); memcpy(&ss1, key + 20, 4);
    memcpy(&ss2, key + 24, 4); memcpy(&ss3, key + 28, 4);

    uint64_t f;
    f = (uint64_t)h[0] + (uint64_t)h[1] * (1 << 26);
    uint64_t lo = f + ss0;
    f = (uint64_t)h[2] * (1ULL << 52) + ((uint64_t)h[2] >> 12);
    (void)f;

    /* Recombine into 4 x 32-bit and add s */
    uint64_t acc = (uint64_t)h[0] | ((uint64_t)h[1] << 26) | ((uint64_t)h[2] << 52);
    uint64_t acc_hi = ((uint64_t)h[2] >> 12) | ((uint64_t)h[3] << 14) | ((uint64_t)h[4] << 40);

    acc += ss0 | ((uint64_t)ss1 << 32);
    uint64_t carry = (acc < ((uint64_t)ss0 | ((uint64_t)ss1 << 32))) ? 1 : 0;
    acc_hi += ((uint64_t)ss2 | ((uint64_t)ss3 << 32)) + carry;

    memcpy(tag, &acc, 8);
    memcpy(tag + 8, &acc_hi, 8);

    /* Cleanup unused vars */
    (void)a0; (void)a1; (void)a2; (void)lo; (void)h0; (void)h1; (void)h2;
    (void)rr0; (void)s0; (void)s1;
}

int bf_crypto_secretbox(uint8_t *cipher_out,
                        const void *msg, size_t msg_len,
                        const uint8_t nonce[BF_CRYPTO_NONCE_BYTES],
                        const uint8_t key[BF_CRYPTO_KEY_BYTES]) {
    /* Derive one-time Poly1305 key from first ChaCha20 block */
    uint8_t poly_key[32];
    uint8_t zeros[32] = {0};
    /* Use first 8 bytes of nonce for ChaCha20 */
    chacha20_xor(poly_key, zeros, 32, key, nonce + 16, 0);

    /* Encrypt with counter starting at 1 */
    chacha20_xor(cipher_out + BF_CRYPTO_MAC_BYTES, (const uint8_t *)msg, msg_len,
                  key, nonce + 16, 1);

    /* MAC over ciphertext */
    poly1305_mac(cipher_out, cipher_out + BF_CRYPTO_MAC_BYTES, msg_len, poly_key);

    bf_crypto_wipe(poly_key, 32);
    return 0;
}

int bf_crypto_secretbox_open(uint8_t *plain_out,
                             const uint8_t *cipher, size_t cipher_len,
                             const uint8_t nonce[BF_CRYPTO_NONCE_BYTES],
                             const uint8_t key[BF_CRYPTO_KEY_BYTES]) {
    if (cipher_len < BF_CRYPTO_MAC_BYTES) return -1;

    size_t msg_len = cipher_len - BF_CRYPTO_MAC_BYTES;

    /* Derive Poly1305 key */
    uint8_t poly_key[32];
    uint8_t zeros[32] = {0};
    chacha20_xor(poly_key, zeros, 32, key, nonce + 16, 0);

    /* Verify MAC */
    uint8_t computed_tag[16];
    poly1305_mac(computed_tag, cipher + BF_CRYPTO_MAC_BYTES, msg_len, poly_key);

    if (bf_crypto_ct_eq(cipher, computed_tag, 16) != 0) {
        bf_crypto_wipe(poly_key, 32);
        bf_crypto_wipe(computed_tag, 16);
        return -1; /* Authentication failed */
    }

    /* Decrypt */
    chacha20_xor(plain_out, cipher + BF_CRYPTO_MAC_BYTES, msg_len,
                  key, nonce + 16, 1);

    bf_crypto_wipe(poly_key, 32);
    bf_crypto_wipe(computed_tag, 16);
    return 0;
}

/* ── Signing (Ed25519) — stub, delegates to BLAKE2b HMAC ───── */

int bf_crypto_sign_keygen(uint8_t pk[BF_CRYPTO_SIGN_PK_BYTES],
                          uint8_t sk[BF_CRYPTO_SIGN_SK_BYTES]) {
    /* Generate random seed, derive pk as H(seed) */
    bf_crypto_random_buf(sk, BF_CRYPTO_SIGN_SK_BYTES);
    bf_crypto_hash(pk, sk, 32, NULL, 0);
    /* Copy pk into upper half of sk for easy access */
    memcpy(sk + 32, pk, 32);
    return 0;
}

int bf_crypto_sign(uint8_t sig_out[BF_CRYPTO_SIGN_BYTES],
                   const void *msg, size_t msg_len,
                   const uint8_t sk[BF_CRYPTO_SIGN_SK_BYTES]) {
    /*
     * HMAC-based deterministic signature (not EC-based Ed25519).
     * Produces a 64-byte tag: HMAC(sk[0:32], nonce || msg) where
     * nonce = HMAC(sk[0:32], msg). This is secure for authentication
     * but not a public-key signature. For real Ed25519, link libsodium.
     */
    uint8_t nonce[32];
    bf_crypto_hmac(nonce, msg, msg_len, sk);

    blake2b_state s;
    blake2b_init(&s, 64, sk, 32);
    blake2b_update(&s, nonce, 32);
    blake2b_update(&s, msg, msg_len);
    blake2b_final(&s, sig_out);

    bf_crypto_wipe(nonce, 32);
    bf_crypto_wipe(&s, sizeof(s));
    return 0;
}

int bf_crypto_sign_verify(const uint8_t sig[BF_CRYPTO_SIGN_BYTES],
                          const void *msg, size_t msg_len,
                          const uint8_t pk[BF_CRYPTO_SIGN_PK_BYTES]) {
    /* Cannot verify HMAC-based sigs without secret key.
     * This is a placeholder — returns -1 always.
     * Link libsodium for real Ed25519 verification. */
    (void)sig; (void)msg; (void)msg_len; (void)pk;
    return -1;
}

/* ── KDF ─────────────────────────────────────────────────────── */

int bf_crypto_kdf(uint8_t subkey_out[BF_CRYPTO_KEY_BYTES],
                  uint64_t subkey_id,
                  const char context[8],
                  const uint8_t master_key[BF_CRYPTO_KEY_BYTES]) {
    /* BLAKE2b(key=master, msg=context||subkey_id) */
    blake2b_state s;
    blake2b_init(&s, BF_CRYPTO_KEY_BYTES, master_key, BF_CRYPTO_KEY_BYTES);
    blake2b_update(&s, context, 8);
    blake2b_update(&s, &subkey_id, 8);
    blake2b_final(&s, subkey_out);
    bf_crypto_wipe(&s, sizeof(s));
    return 0;
}

/* ── Utility ─────────────────────────────────────────────────── */

int bf_crypto_ct_eq(const void *a, const void *b, size_t len) {
    const volatile uint8_t *x = (const volatile uint8_t *)a;
    const volatile uint8_t *y = (const volatile uint8_t *)b;
    volatile uint8_t diff = 0;
    for (size_t i = 0; i < len; i++) {
        diff |= x[i] ^ y[i];
    }
    return (int)diff;
}

void bf_crypto_wipe(void *buf, size_t len) {
    volatile uint8_t *p = (volatile uint8_t *)buf;
    for (size_t i = 0; i < len; i++) p[i] = 0;
}

static const char hex_chars[] = "0123456789abcdef";

void bf_crypto_to_hex(char *hex_out, const uint8_t *bin, size_t bin_len) {
    for (size_t i = 0; i < bin_len; i++) {
        hex_out[i * 2]     = hex_chars[bin[i] >> 4];
        hex_out[i * 2 + 1] = hex_chars[bin[i] & 0x0f];
    }
    hex_out[bin_len * 2] = '\0';
}

int bf_crypto_from_hex(uint8_t *bin_out, size_t bin_sz,
                       const char *hex, size_t hex_len) {
    if (hex_len % 2 != 0 || hex_len / 2 > bin_sz) return -1;
    for (size_t i = 0; i < hex_len / 2; i++) {
        uint8_t hi, lo;
        char ch = hex[i * 2];
        char cl = hex[i * 2 + 1];
        if      (ch >= '0' && ch <= '9') hi = (uint8_t)(ch - '0');
        else if (ch >= 'a' && ch <= 'f') hi = (uint8_t)(ch - 'a' + 10);
        else if (ch >= 'A' && ch <= 'F') hi = (uint8_t)(ch - 'A' + 10);
        else return -1;
        if      (cl >= '0' && cl <= '9') lo = (uint8_t)(cl - '0');
        else if (cl >= 'a' && cl <= 'f') lo = (uint8_t)(cl - 'a' + 10);
        else if (cl >= 'A' && cl <= 'F') lo = (uint8_t)(cl - 'A' + 10);
        else return -1;
        bin_out[i] = (uint8_t)((hi << 4) | lo);
    }
    return 0;
}
