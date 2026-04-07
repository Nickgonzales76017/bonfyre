/*
 * bf_sha256.c — FIPS 180-4 SHA-256, no external dependencies.
 *
 * Previously duplicated in BonfyreHash, BonfyreIngest, BonfyrePipeline.
 * Now shared via libbonfyre.
 */
#include "bonfyre.h"
#include <stdio.h>
#include <string.h>

static const uint32_t K256[64] = {
    0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
    0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
    0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
    0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
    0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
    0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
    0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
    0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2
};

#define RR(x,n) (((x)>>(n))|((x)<<(32-(n))))
#define SHA_S0(x) (RR(x,2)^RR(x,13)^RR(x,22))
#define SHA_S1(x) (RR(x,6)^RR(x,11)^RR(x,25))
#define SHA_s0(x) (RR(x,7)^RR(x,18)^((x)>>3))
#define SHA_s1(x) (RR(x,17)^RR(x,19)^((x)>>10))
#define CH(x,y,z) (((x)&(y))^((~(x))&(z)))
#define MAJ(x,y,z) (((x)&(y))^((x)&(z))^((y)&(z)))

void bf_sha256_init(BfSha256 *c) {
    c->h[0]=0x6a09e667; c->h[1]=0xbb67ae85; c->h[2]=0x3c6ef372; c->h[3]=0xa54ff53a;
    c->h[4]=0x510e527f; c->h[5]=0x9b05688c; c->h[6]=0x1f83d9ab; c->h[7]=0x5be0cd19;
    c->total = 0;
}

static void sha256_block(BfSha256 *c, const uint8_t *d) {
    uint32_t W[64], a, b, cc2, dd, e, f, g, h;
    for (int i = 0; i < 16; i++)
        W[i] = (uint32_t)d[i*4]<<24 | (uint32_t)d[i*4+1]<<16 |
               (uint32_t)d[i*4+2]<<8 | d[i*4+3];
    for (int i = 16; i < 64; i++)
        W[i] = SHA_s1(W[i-2]) + W[i-7] + SHA_s0(W[i-15]) + W[i-16];

    a=c->h[0]; b=c->h[1]; cc2=c->h[2]; dd=c->h[3];
    e=c->h[4]; f=c->h[5]; g=c->h[6]; h=c->h[7];

    for (int i = 0; i < 64; i++) {
        uint32_t t1 = h + SHA_S1(e) + CH(e,f,g) + K256[i] + W[i];
        uint32_t t2 = SHA_S0(a) + MAJ(a,b,cc2);
        h=g; g=f; f=e; e=dd+t1; dd=cc2; cc2=b; b=a; a=t1+t2;
    }
    c->h[0]+=a; c->h[1]+=b; c->h[2]+=cc2; c->h[3]+=dd;
    c->h[4]+=e; c->h[5]+=f; c->h[6]+=g; c->h[7]+=h;
}

void bf_sha256_update(BfSha256 *c, const uint8_t *data, size_t len) {
    size_t off = c->total % 64;
    c->total += len;
    if (off > 0) {
        size_t fill = 64 - off;
        if (len < fill) { memcpy(c->buf + off, data, len); return; }
        memcpy(c->buf + off, data, fill);
        sha256_block(c, c->buf);
        data += fill; len -= fill;
    }
    while (len >= 64) { sha256_block(c, data); data += 64; len -= 64; }
    if (len > 0) memcpy(c->buf, data, len);
}

void bf_sha256_final(BfSha256 *c, uint8_t hash[32]) {
    size_t off = c->total % 64;
    c->buf[off++] = 0x80;
    if (off > 56) {
        memset(c->buf + off, 0, 64 - off);
        sha256_block(c, c->buf);
        off = 0;
    }
    memset(c->buf + off, 0, 56 - off);
    uint64_t bits = c->total * 8;
    for (int i = 0; i < 8; i++)
        c->buf[56 + i] = (uint8_t)(bits >> (56 - 8 * i));
    sha256_block(c, c->buf);
    for (int i = 0; i < 8; i++) {
        hash[i*4]   = (uint8_t)(c->h[i] >> 24);
        hash[i*4+1] = (uint8_t)(c->h[i] >> 16);
        hash[i*4+2] = (uint8_t)(c->h[i] >> 8);
        hash[i*4+3] = (uint8_t)(c->h[i]);
    }
}

static const char hex_lut[16] = "0123456789abcdef";

static void hash_to_hex(const uint8_t hash[32], char hex[65]) {
    for (int i = 0; i < 32; i++) {
        hex[i*2]   = hex_lut[hash[i] >> 4];
        hex[i*2+1] = hex_lut[hash[i] & 0x0f];
    }
    hex[64] = '\0';
}

void bf_sha256_hex(const uint8_t *data, size_t len, char hex[65]) {
    BfSha256 ctx;
    uint8_t hash[32];
    bf_sha256_init(&ctx);
    bf_sha256_update(&ctx, data, len);
    bf_sha256_final(&ctx, hash);
    hash_to_hex(hash, hex);
}

int bf_sha256_file(const char *path, char hex[65]) {
    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    BfSha256 ctx;
    bf_sha256_init(&ctx);

    uint8_t buf[8192];
    size_t n;
    while ((n = fread(buf, 1, sizeof(buf), f)) > 0)
        bf_sha256_update(&ctx, buf, n);
    fclose(f);

    uint8_t hash[32];
    bf_sha256_final(&ctx, hash);
    hash_to_hex(hash, hex);
    return 0;
}
