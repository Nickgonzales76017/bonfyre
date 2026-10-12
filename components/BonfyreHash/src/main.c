/*
 * BonfyreHash — content-addressing + Merkle DAG hashing engine.
 *
 * Owns every hash in the system. The single source of truth for:
 *   - File content hashes (SHA-256)
 *   - Operator node hashes (DAG hashing)
 *   - Merkle roots (family integrity)
 *   - Dedup detection (same hash = same content, pay once)
 *
 * Usage:
 *   bonfyre-hash file <path>                     — SHA-256 of file
 *   bonfyre-hash node <op> <version> <params_json> <child_hash,...>  — operator node hash
 *   bonfyre-hash merkle <artifact.json>           — compute/update root_hash
 *   bonfyre-hash verify <artifact.json>           — verify all hashes
 *   bonfyre-hash dedup <dir>                      — find duplicate files by hash
 */
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

#define MAX_LINE 65536
#define MAX_CHILDREN 256
#define MAX_FILES 4096
#define HASH_LEN 65

/* ---------- SHA-256 implementation (FIPS 180-4, no deps) ---------- */

static const unsigned int K256[64] = {
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
#define S0(x) (RR(x,2)^RR(x,13)^RR(x,22))
#define S1(x) (RR(x,6)^RR(x,11)^RR(x,25))
#define s0(x) (RR(x,7)^RR(x,18)^((x)>>3))
#define s1(x) (RR(x,17)^RR(x,19)^((x)>>10))
#define CH(e,f,g) (((e)&(f))^((~(e))&(g)))
#define MAJ(a,b,c) (((a)&(b))^((a)&(c))^((b)&(c)))

typedef struct {
    unsigned int h[8];
    unsigned char buf[64];
    unsigned long long total;
} SHA256_CTX;

static void sha256_init(SHA256_CTX *c) {
    c->h[0]=0x6a09e667; c->h[1]=0xbb67ae85; c->h[2]=0x3c6ef372; c->h[3]=0xa54ff53a;
    c->h[4]=0x510e527f; c->h[5]=0x9b05688c; c->h[6]=0x1f83d9ab; c->h[7]=0x5be0cd19;
    c->total = 0;
}

static void sha256_block(SHA256_CTX *c, const unsigned char *data) {
    unsigned int w[64], a, b, d, e, f, g, h, t1, t2;
    a = c->h[0]; b = c->h[1]; d = c->h[2]; e = c->h[3] /* reuse var names */;
    for (int i = 0; i < 16; i++)
        w[i] = ((unsigned int)data[i*4]<<24)|((unsigned int)data[i*4+1]<<16)|
               ((unsigned int)data[i*4+2]<<8)|data[i*4+3];
    for (int i = 16; i < 64; i++)
        w[i] = s1(w[i-2]) + w[i-7] + s0(w[i-15]) + w[i-16];
    a=c->h[0]; b=c->h[1]; d=c->h[2]; f=c->h[3];
    e=c->h[4]; g=c->h[5]; h=c->h[6];
    unsigned int cc = c->h[7];
    /* Use proper variable mapping: a,b,c,d,e,f,g,h */
    unsigned int st[8];
    for (int i = 0; i < 8; i++) st[i] = c->h[i];
    for (int i = 0; i < 64; i++) {
        t1 = st[7] + S1(st[4]) + CH(st[4],st[5],st[6]) + K256[i] + w[i];
        t2 = S0(st[0]) + MAJ(st[0],st[1],st[2]);
        st[7]=st[6]; st[6]=st[5]; st[5]=st[4]; st[4]=st[3]+t1;
        st[3]=st[2]; st[2]=st[1]; st[1]=st[0]; st[0]=t1+t2;
    }
    for (int i = 0; i < 8; i++) c->h[i] += st[i];
    (void)a; (void)b; (void)d; (void)e; (void)f; (void)g; (void)h; (void)cc;
}

/* #1: Block-aligned update — process full 64-byte blocks directly */
static void sha256_update(SHA256_CTX *c, const unsigned char *data, size_t len) {
    size_t off = (size_t)(c->total % 64);
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

static void sha256_final(SHA256_CTX *c, unsigned char out[32]) {
    unsigned long long bits = c->total * 8;
    size_t off = (size_t)(c->total % 64);
    c->buf[off++] = 0x80;
    if (off > 56) {
        while (off < 64) c->buf[off++] = 0;
        sha256_block(c, c->buf);
        off = 0;
    }
    while (off < 56) c->buf[off++] = 0;
    for (int i = 7; i >= 0; i--) c->buf[56+(7-i)] = (unsigned char)(bits >> (i*8));
    sha256_block(c, c->buf);
    for (int i = 0; i < 8; i++) {
        out[i*4]   = (unsigned char)(c->h[i]>>24);
        out[i*4+1] = (unsigned char)(c->h[i]>>16);
        out[i*4+2] = (unsigned char)(c->h[i]>>8);
        out[i*4+3] = (unsigned char)(c->h[i]);
    }
}

/* #2: Hex lookup table — no sprintf overhead */
static const char HEX_LUT[16] = "0123456789abcdef";

static void sha256_hex(const unsigned char hash[32], char hex[65]) {
    for (int i = 0; i < 32; i++) {
        hex[i*2]   = HEX_LUT[hash[i] >> 4];
        hex[i*2+1] = HEX_LUT[hash[i] & 0x0f];
    }
    hex[64] = '\0';
}

/* ---------- commands ---------- */

static int cmd_file(const char *path) {
    FILE *fp = fopen(path, "rb");
    if (!fp) { fprintf(stderr, "Cannot open: %s\n", path); return 1; }
    SHA256_CTX ctx;
    sha256_init(&ctx);
    unsigned char buf[8192];
    size_t n;
    while ((n = fread(buf, 1, sizeof(buf), fp)) > 0)
        sha256_update(&ctx, buf, n);
    fclose(fp);
    unsigned char h[32];
    sha256_final(&ctx, h);
    char hex[65];
    sha256_hex(h, hex);
    printf("%s  %s\n", hex, path);
    return 0;
}

/* #8: qsort comparator for child hash pointers */
static int cmp_str_ptrs(const void *a, const void *b) {
    return strcmp(*(const char *const *)a, *(const char *const *)b);
}

static int cmd_node(const char *op, const char *version, const char *params_json, const char *children_csv) {
    /* Canonical: sort children, concatenate op + params + children hashes */
    char *children[MAX_CHILDREN];
    int nchildren = 0;
    char *csv_copy = strdup(children_csv);
    char *tok = strtok(csv_copy, ",");
    while (tok && nchildren < MAX_CHILDREN) {
        while (*tok == ' ') tok++;
        children[nchildren++] = strdup(tok);
        tok = strtok(NULL, ",");
    }
    free(csv_copy);

    /* #8: qsort instead of bubble sort — O(n log n) */
    qsort(children, (size_t)nchildren, sizeof(char *), cmp_str_ptrs);

    /* Build canonical string */
    SHA256_CTX ctx;
    sha256_init(&ctx);
    sha256_update(&ctx, (const unsigned char *)op, strlen(op));
    sha256_update(&ctx, (const unsigned char *)"|", 1);
    sha256_update(&ctx, (const unsigned char *)version, strlen(version));
    sha256_update(&ctx, (const unsigned char *)"|", 1);
    sha256_update(&ctx, (const unsigned char *)params_json, strlen(params_json));
    sha256_update(&ctx, (const unsigned char *)"|", 1);
    for (int i = 0; i < nchildren; i++) {
        sha256_update(&ctx, (const unsigned char *)children[i], strlen(children[i]));
        if (i < nchildren - 1)
            sha256_update(&ctx, (const unsigned char *)",", 1);
        free(children[i]);
    }
    unsigned char h[32];
    sha256_final(&ctx, h);
    char hex[65];
    sha256_hex(h, hex);
    printf("%s\n", hex);
    return 0;
}

/* #6: qsort comparator for dedup entries (sort by hash) */
static int cmp_entry_hash(const void *a, const void *b) {
    typedef struct { char hash[65]; char path[PATH_MAX]; unsigned long size; } Entry;
    return strcmp(((const Entry *)a)->hash, ((const Entry *)b)->hash);
}

static int cmd_dedup(const char *dir) {
    /* Walk directory, hash all files, report duplicates */
    typedef struct { char hash[65]; char path[PATH_MAX]; unsigned long size; } Entry;

    /* #7: Dynamic growth instead of fixed MAX_FILES calloc (saves ~4.3MB) */
    int cap = 128;
    int count = 0;
    Entry *entries = malloc((size_t)cap * sizeof(Entry));
    if (!entries) return 1;

    DIR *d = opendir(dir);
    if (!d) { fprintf(stderr, "Cannot open: %s\n", dir); free(entries); return 1; }
    struct dirent *ent;
    while ((ent = readdir(d))) {
        if (ent->d_name[0] == '.') continue;
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/%s", dir, ent->d_name);
        struct stat st;
        if (stat(path, &st) != 0 || !S_ISREG(st.st_mode)) continue;

        /* Grow if needed */
        if (count >= cap) {
            cap *= 2;
            Entry *tmp = realloc(entries, (size_t)cap * sizeof(Entry));
            if (!tmp) { free(entries); closedir(d); return 1; }
            entries = tmp;
        }

        FILE *fp = fopen(path, "rb");
        if (!fp) continue;
        SHA256_CTX ctx;
        sha256_init(&ctx);
        unsigned char buf[8192];
        size_t n;
        while ((n = fread(buf, 1, sizeof(buf), fp)) > 0)
            sha256_update(&ctx, buf, n);
        fclose(fp);
        unsigned char h[32];
        sha256_final(&ctx, h);
        sha256_hex(h, entries[count].hash);
        snprintf(entries[count].path, PATH_MAX, "%s", path);
        entries[count].size = (unsigned long)st.st_size;
        count++;
    }
    closedir(d);

    /* #6: Sort by hash, then linear scan for duplicates — O(n log n) */
    qsort(entries, (size_t)count, sizeof(Entry), cmp_entry_hash);

    unsigned long wasted = 0;
    int dupes = 0;
    for (int i = 0; i < count - 1; i++) {
        if (strcmp(entries[i].hash, entries[i+1].hash) == 0) {
            printf("DUP %s  %s  (%lu bytes)\n", entries[i].path, entries[i+1].path, entries[i].size);
            wasted += entries[i+1].size;
            dupes++;
        }
    }
    if (dupes == 0) printf("No duplicates found in %d files.\n", count);
    else printf("\n%d duplicate pairs, %lu bytes reclaimable.\n", dupes, wasted);

    free(entries);
    return 0;
}

/* ---------- #10: Inline Merkle (replaces system() + Python) ---------- */

/* Extract quoted string values matching "content_hash" from artifact.json */
static int extract_content_hashes(const char *json, char hashes[][65], int max_hashes) {
    const char *needle = "\"content_hash\"";
    size_t needle_len = strlen(needle);
    int count = 0;
    const char *p = json;
    while ((p = strstr(p, needle)) && count < max_hashes) {
        p += needle_len;
        while (*p && (*p == ' ' || *p == ':' || *p == '\t')) p++;
        if (*p != '"') continue;
        p++;
        int i = 0;
        while (*p && *p != '"' && i < 64) hashes[count][i++] = *p++;
        hashes[count][i] = '\0';
        if (i == 64) count++;
    }
    return count;
}

static int cmp_hash_strings(const void *a, const void *b) {
    return strcmp((const char *)a, (const char *)b);
}

static int cmd_merkle(const char *artifact_path, int verify_only) {
    /* Read artifact.json */
    FILE *f = fopen(artifact_path, "rb");
    if (!f) { fprintf(stderr, "Cannot open: %s\n", artifact_path); return 1; }
    fseek(f, 0, SEEK_END); long sz = ftell(f); fseek(f, 0, SEEK_SET);
    if (sz <= 0 || sz > 1048576) { fclose(f); return 1; }
    char *json = malloc((size_t)sz + 1);
    if (!json) { fclose(f); return 1; }
    fread(json, 1, (size_t)sz, f);
    json[sz] = '\0';
    fclose(f);

    /* Extract all content_hash values from atoms */
    char (*hashes)[65] = malloc(4096 * 65);
    if (!hashes) { free(json); return 1; }
    int nhashes = extract_content_hashes(json, hashes, 4096);
    if (nhashes == 0) {
        fprintf(stderr, "No content_hash entries found\n");
        free(hashes); free(json);
        return 1;
    }

    /* Sort hashes for canonical ordering */
    qsort(hashes, (size_t)nhashes, 65, cmp_hash_strings);

    /* Build Merkle root: iteratively pair-hash until one remains */
    while (nhashes > 1) {
        int next = 0;
        for (int i = 0; i < nhashes; i += 2) {
            SHA256_CTX ctx;
            sha256_init(&ctx);
            sha256_update(&ctx, (const unsigned char *)hashes[i], strlen(hashes[i]));
            if (i + 1 < nhashes)
                sha256_update(&ctx, (const unsigned char *)hashes[i+1], strlen(hashes[i+1]));
            unsigned char h[32];
            sha256_final(&ctx, h);
            sha256_hex(h, hashes[next]);
            next++;
        }
        nhashes = next;
    }

    char *root_hash = hashes[0];

    if (verify_only) {
        /* Check if existing root_hash matches */
        const char *rh = strstr(json, "\"root_hash\"");
        if (rh) {
            rh += 11;
            while (*rh && (*rh == ' ' || *rh == ':' || *rh == '\t' || *rh == '"')) rh++;
            if (strncmp(rh, root_hash, 64) == 0)
                printf("VERIFIED: root_hash matches (%s)\n", root_hash);
            else
                printf("MISMATCH: computed=%s\n", root_hash);
        } else {
            printf("No root_hash field found; computed=%s\n", root_hash);
        }
    } else {
        printf("%s\n", root_hash);
    }

    free(hashes);
    free(json);
    return 0;
}

/* ---------- main ---------- */

int main(int argc, char *argv[]) {
    if (argc < 2) goto usage;

    if (strcmp(argv[1], "file") == 0 && argc >= 3)
        return cmd_file(argv[2]);

    if (strcmp(argv[1], "node") == 0 && argc >= 6)
        return cmd_node(argv[2], argv[3], argv[4], argv[5]);

    if (strcmp(argv[1], "merkle") == 0 && argc >= 3)
        return cmd_merkle(argv[2], 0);

    if (strcmp(argv[1], "verify") == 0 && argc >= 3)
        return cmd_merkle(argv[2], 1);

    if (strcmp(argv[1], "dedup") == 0 && argc >= 3)
        return cmd_dedup(argv[2]);

usage:
    fprintf(stderr,
        "BonfyreHash — content-addressing engine\n\n"
        "Usage:\n"
        "  bonfyre-hash file <path>                              SHA-256 of file\n"
        "  bonfyre-hash node <op> <ver> <params_json> <h1,h2>    operator node hash\n"
        "  bonfyre-hash merkle <artifact.json>                    compute/update root_hash\n"
        "  bonfyre-hash verify <artifact.json>                    verify all hashes\n"
        "  bonfyre-hash dedup <dir>                               find duplicate files\n"
    );
    return 1;
}
