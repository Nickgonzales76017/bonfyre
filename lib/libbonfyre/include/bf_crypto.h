/*
 * bf_crypto.h — Cryptographic primitives for Bonfyre
 *
 * Lightweight wrapper inspired by libhydrogen (ISC license, ~3 KLOC).
 * Provides: password hashing, HMAC, AEAD secretbox, Ed25519 signing, KDF.
 *
 * Implementation strategy:
 *   - When <sodium.h> or <hydrogen.h> is available, delegate to it.
 *   - Otherwise, self-contained portable implementations:
 *       BLAKE2b for hashing/HMAC/KDF
 *       XSalsa20-Poly1305 for AEAD (secretbox)
 *       Ed25519 for signing
 *
 * All keys are fixed-size byte arrays. No heap allocation in hot paths.
 */

#ifndef BF_CRYPTO_H
#define BF_CRYPTO_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Constants ───────────────────────────────────────────────── */

#define BF_CRYPTO_HASH_BYTES    32
#define BF_CRYPTO_KEY_BYTES     32
#define BF_CRYPTO_NONCE_BYTES   24
#define BF_CRYPTO_MAC_BYTES     16
#define BF_CRYPTO_SIGN_BYTES    64
#define BF_CRYPTO_SIGN_PK_BYTES 32
#define BF_CRYPTO_SIGN_SK_BYTES 64
#define BF_CRYPTO_PWHASH_BYTES  32
#define BF_CRYPTO_SALT_BYTES    16

/* ── Initialization ──────────────────────────────────────────── */

/* Must be called once before any other bf_crypto* function.
 * Seeds the internal CSPRNG. Returns 0 on success. */
int bf_crypto_init(void);

/* ── Random ──────────────────────────────────────────────────── */

void bf_crypto_random_buf(void *buf, size_t len);
uint32_t bf_crypto_random_u32(void);

/* ── Hashing (BLAKE2b) ───────────────────────────────────────── */

/* Generic hash: out must be BF_CRYPTO_HASH_BYTES.
 * key may be NULL for unkeyed hash. */
int bf_crypto_hash(uint8_t out[BF_CRYPTO_HASH_BYTES],
                   const void *msg, size_t msg_len,
                   const uint8_t *key, size_t key_len);

/* ── HMAC ────────────────────────────────────────────────────── */

/* HMAC-BLAKE2b: out is BF_CRYPTO_HASH_BYTES. */
int bf_crypto_hmac(uint8_t out[BF_CRYPTO_HASH_BYTES],
                   const void *msg, size_t msg_len,
                   const uint8_t key[BF_CRYPTO_KEY_BYTES]);

/* Constant-time comparison of two MACs. Returns 0 on match. */
int bf_crypto_hmac_verify(const uint8_t a[BF_CRYPTO_HASH_BYTES],
                          const uint8_t b[BF_CRYPTO_HASH_BYTES]);

/* ── Password hashing ────────────────────────────────────────── */

/* Hash a password with a random salt.
 * out: BF_CRYPTO_PWHASH_BYTES
 * salt_out: BF_CRYPTO_SALT_BYTES (filled with random salt)
 * ops_limit: iteration count (recommend >= 3 for interactive, >= 6 for sensitive) */
int bf_crypto_pwhash(uint8_t out[BF_CRYPTO_PWHASH_BYTES],
                     uint8_t salt_out[BF_CRYPTO_SALT_BYTES],
                     const char *password, size_t pw_len,
                     int ops_limit);

/* Verify a password against a stored hash + salt. Returns 0 on match. */
int bf_crypto_pwhash_verify(const uint8_t stored[BF_CRYPTO_PWHASH_BYTES],
                            const uint8_t salt[BF_CRYPTO_SALT_BYTES],
                            const char *password, size_t pw_len,
                            int ops_limit);

/* ── Secretbox (AEAD) ────────────────────────────────────────── */

/* Encrypt + authenticate in-place.
 * cipher_out must have room for msg_len + BF_CRYPTO_MAC_BYTES.
 * nonce: BF_CRYPTO_NONCE_BYTES (caller generates, must be unique). */
int bf_crypto_secretbox(uint8_t *cipher_out,
                        const void *msg, size_t msg_len,
                        const uint8_t nonce[BF_CRYPTO_NONCE_BYTES],
                        const uint8_t key[BF_CRYPTO_KEY_BYTES]);

/* Decrypt + verify. plain_out must have room for cipher_len - BF_CRYPTO_MAC_BYTES.
 * Returns 0 on success, -1 on authentication failure. */
int bf_crypto_secretbox_open(uint8_t *plain_out,
                             const uint8_t *cipher, size_t cipher_len,
                             const uint8_t nonce[BF_CRYPTO_NONCE_BYTES],
                             const uint8_t key[BF_CRYPTO_KEY_BYTES]);

/* ── Signing (Ed25519) ───────────────────────────────────────── */

/* Generate a keypair. */
int bf_crypto_sign_keygen(uint8_t pk[BF_CRYPTO_SIGN_PK_BYTES],
                          uint8_t sk[BF_CRYPTO_SIGN_SK_BYTES]);

/* Sign a message. sig_out must be BF_CRYPTO_SIGN_BYTES. */
int bf_crypto_sign(uint8_t sig_out[BF_CRYPTO_SIGN_BYTES],
                   const void *msg, size_t msg_len,
                   const uint8_t sk[BF_CRYPTO_SIGN_SK_BYTES]);

/* Verify a signature. Returns 0 on valid signature. */
int bf_crypto_sign_verify(const uint8_t sig[BF_CRYPTO_SIGN_BYTES],
                          const void *msg, size_t msg_len,
                          const uint8_t pk[BF_CRYPTO_SIGN_PK_BYTES]);

/* ── KDF (Key Derivation) ───────────────────────────────────── */

/* Derive a subkey from a master key + context string.
 * subkey_out must be BF_CRYPTO_KEY_BYTES.
 * context: 8-byte string describing purpose (e.g. "bfApiKey"). */
int bf_crypto_kdf(uint8_t subkey_out[BF_CRYPTO_KEY_BYTES],
                  uint64_t subkey_id,
                  const char context[8],
                  const uint8_t master_key[BF_CRYPTO_KEY_BYTES]);

/* ── Utility ─────────────────────────────────────────────────── */

/* Constant-time memory comparison. Returns 0 if equal. */
int bf_crypto_ct_eq(const void *a, const void *b, size_t len);

/* Secure memory wipe. */
void bf_crypto_wipe(void *buf, size_t len);

/* Hex encode/decode for storing hashes as text. */
void bf_crypto_to_hex(char *hex_out, const uint8_t *bin, size_t bin_len);
int bf_crypto_from_hex(uint8_t *bin_out, size_t bin_sz,
                       const char *hex, size_t hex_len);

#ifdef __cplusplus
}
#endif

#endif /* BF_CRYPTO_H */
