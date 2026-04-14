/*
 * tokenizer.c — BPE tokenizer for FPQ Ember runtime
 *
 * Supports HuggingFace tokenizer.json with:
 *   - BPE merges (LLaMA/Mistral/TinyLlama/Qwen style)
 *   - byte_fallback=true (<0xXX> tokens for raw bytes)
 *   - Normalizer: prepend '▁', replace spaces with '▁'
 *   - Special tokens: <unk>=0, <s>=1, </s>=2
 *
 * The JSON parser is minimal: no library dependency.
 * It handles the subset of JSON actually used in tokenizer.json.
 */
#include "tokenizer.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <ctype.h>

/* ═══════════════════════════════════════════════════════
 * Internal structures
 * ═══════════════════════════════════════════════════════ */

#define TOK_HASH_SIZE  65537  /* prime, covers 32K vocab well */
#define TOK_MAX_MERGE_LEN 256

typedef struct tok_hash_entry {
    const char *str;          /* points into str_pool */
    int32_t     id;
    struct tok_hash_entry *next;
} tok_hash_entry_t;

/* merge_rank: maps merged token id → rank (position in merges list)
 * We encode the pair key as: (id1 * VOCAB_MAX + id2).
 * For vocab ≤ 65536: key fits in uint64_t. */
typedef struct merge_entry {
    uint64_t key;             /* id1 << 20 | id2  (each ≤ 1M) */
    int32_t  rank;
    int32_t  merged_id;
    struct merge_entry *next;
} merge_entry_t;

#define MERGE_HASH_SIZE  131101  /* prime > 2×n_merges */

struct tokenizer {
    /* vocab */
    int        vocab_size;
    char     **id_to_str;       /* id → string (points into str_pool) */
    uint8_t   *id_is_byte;      /* id → 1 if byte-fallback token <0xXX> */
    uint8_t   *id_byte_val;     /* id → raw byte value (if is_byte) */

    /* string → id hash map */
    tok_hash_entry_t **str_map;
    int str_map_size;

    /* merge priority table */
    merge_entry_t **merge_map;
    int merge_map_size;

    /* string pool for all vocab strings */
    char *str_pool;
    size_t str_pool_size;

    /* special token IDs */
    int bos_id;
    int eos_id;
    int unk_id;

    /* added/special tokens for pre-scan during encode */
    char **special_str;   /* special token strings (owned) */
    int   *special_id;    /* corresponding token IDs */
    int    n_special;
};

/* ═══════════════════════════════════════════════════════
 * Minimal JSON tokenizer (slurps the whole file)
 * ═══════════════════════════════════════════════════════ */

static char *read_file(const char *path, size_t *len_out) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    fseek(f, 0, SEEK_END);
    size_t len = (size_t)ftell(f);
    fseek(f, 0, SEEK_SET);
    char *buf = (char *)malloc(len + 1);
    if (!buf) { fclose(f); return NULL; }
    size_t rd = fread(buf, 1, len, f);
    fclose(f);
    buf[rd] = '\0';
    if (len_out) *len_out = rd;
    return buf;
}

/* Skip whitespace */
static const char *skip_ws(const char *p) {
    while (*p && (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')) p++;
    return p;
}

/* Parse a JSON string (starting after the opening '"').
 * Writes decoded bytes into dst (up to dstlen-1), null-terminates.
 * Returns pointer after closing '"'. */
static const char *parse_json_string(const char *p, char *dst, int dstlen) {
    int i = 0;
    while (*p && *p != '"') {
        if (*p == '\\' && p[1]) {
            p++;
            switch (*p) {
                case '"':  if (i < dstlen-1) dst[i++] = '"';  break;
                case '\\': if (i < dstlen-1) dst[i++] = '\\'; break;
                case '/':  if (i < dstlen-1) dst[i++] = '/';  break;
                case 'n':  if (i < dstlen-1) dst[i++] = '\n'; break;
                case 'r':  if (i < dstlen-1) dst[i++] = '\r'; break;
                case 't':  if (i < dstlen-1) dst[i++] = '\t'; break;
                case 'b':  if (i < dstlen-1) dst[i++] = '\b'; break;
                case 'f':  if (i < dstlen-1) dst[i++] = '\f'; break;
                case 'u': {
                    /* \uXXXX — decode to UTF-8 */
                    if (p[1] && p[2] && p[3] && p[4]) {
                        unsigned int cp = 0;
                        for (int j = 1; j <= 4; j++) {
                            char c = p[j];
                            cp <<= 4;
                            if (c >= '0' && c <= '9') cp |= (unsigned)(c - '0');
                            else if (c >= 'a' && c <= 'f') cp |= (unsigned)(c - 'a' + 10);
                            else if (c >= 'A' && c <= 'F') cp |= (unsigned)(c - 'A' + 10);
                        }
                        p += 4;
                        /* Encode codepoint to UTF-8 */
                        if (cp < 0x80) {
                            if (i < dstlen-1) dst[i++] = (char)cp;
                        } else if (cp < 0x800) {
                            if (i+1 < dstlen-1) {
                                dst[i++] = (char)(0xC0 | (cp >> 6));
                                dst[i++] = (char)(0x80 | (cp & 0x3F));
                            }
                        } else {
                            if (i+2 < dstlen-1) {
                                dst[i++] = (char)(0xE0 | (cp >> 12));
                                dst[i++] = (char)(0x80 | ((cp >> 6) & 0x3F));
                                dst[i++] = (char)(0x80 | (cp & 0x3F));
                            }
                        }
                    }
                    break;
                }
                default: if (i < dstlen-1) dst[i++] = *p; break;
            }
        } else {
            if (i < dstlen-1) dst[i++] = *p;
        }
        p++;
    }
    dst[i] = '\0';
    if (*p == '"') p++;
    return p;
}

/* Find a JSON key in a JSON object blob. Returns pointer to the value,
 * or NULL if not found. Simple scan — not fully general. */
static const char *json_find_key(const char *json, const char *key) {
    char needle[256];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char *p = strstr(json, needle);
    if (!p) return NULL;
    p += strlen(needle);
    p = skip_ws(p);
    if (*p != ':') return NULL;
    return skip_ws(p + 1);
}

/* ═══════════════════════════════════════════════════════
 * Hash helpers
 * ═══════════════════════════════════════════════════════ */

/* FNV-1a 32-bit */
static uint32_t fnv32(const char *s) {
    uint32_t h = 2166136261u;
    while (*s) {
        h ^= (uint8_t)*s++;
        h *= 16777619u;
    }
    return h;
}

static void str_map_insert(tok_hash_entry_t **map, int map_size,
                           const char *str, int id,
                           tok_hash_entry_t *pool, int pool_idx) {
    uint32_t slot = fnv32(str) % (uint32_t)map_size;
    tok_hash_entry_t *e = &pool[pool_idx];
    e->str = str;
    e->id  = id;
    e->next = map[slot];
    map[slot] = e;
}

static int str_map_get(tok_hash_entry_t **map, int map_size, const char *str) {
    uint32_t slot = fnv32(str) % (uint32_t)map_size;
    for (tok_hash_entry_t *e = map[slot]; e; e = e->next)
        if (strcmp(e->str, str) == 0) return e->id;
    return -1;
}

static void merge_map_insert(merge_entry_t **map, int map_size,
                             int id1, int id2, int rank, int merged_id,
                             merge_entry_t *pool, int pool_idx) {
    uint64_t key = ((uint64_t)(uint32_t)id1 << 20) | (uint32_t)id2;
    uint32_t slot = (uint32_t)(key ^ (key >> 17)) % (uint32_t)map_size;
    merge_entry_t *e = &pool[pool_idx];
    e->key = key;
    e->rank = rank;
    e->merged_id = merged_id;
    e->next = map[slot];
    map[slot] = e;
}

static merge_entry_t *merge_map_get(merge_entry_t **map, int map_size,
                                    int id1, int id2) {
    uint64_t key = ((uint64_t)(uint32_t)id1 << 20) | (uint32_t)id2;
    uint32_t slot = (uint32_t)(key ^ (key >> 17)) % (uint32_t)map_size;
    for (merge_entry_t *e = map[slot]; e; e = e->next)
        if (e->key == key) return e;
    return NULL;
}

/* ═══════════════════════════════════════════════════════
 * tok_load — parse tokenizer.json
 * ═══════════════════════════════════════════════════════ */

tokenizer_t *tok_load(const char *json_path) {
    size_t json_len = 0;
    char *json = read_file(json_path, &json_len);
    if (!json) {
        fprintf(stderr, "tok_load: cannot open %s\n", json_path);
        return NULL;
    }

    tokenizer_t *tok = (tokenizer_t *)calloc(1, sizeof(tokenizer_t));
    tok->bos_id = 1;  /* <s> */
    tok->eos_id = 2;  /* </s> */
    tok->unk_id = 0;  /* <unk> */

    /* ── Locate "model" section ── */
    const char *model_start = json_find_key(json, "model");
    if (!model_start) {
        fprintf(stderr, "tok_load: no 'model' key in %s\n", json_path);
        free(json); free(tok); return NULL;
    }

    /* ── Count vocab items (scan for "vocab": { ... }) ── */
    const char *vocab_start = json_find_key(model_start, "vocab");
    if (!vocab_start || *vocab_start != '{') {
        fprintf(stderr, "tok_load: no vocab object\n");
        free(json); free(tok); return NULL;
    }

    /* First pass: count vocab entries */
    int vocab_size = 0;
    {
        const char *p = vocab_start + 1;
        int depth = 1;
        while (*p && depth > 0) {
            p = skip_ws(p);
            if (*p == '{') { depth++; p++; continue; }
            if (*p == '}') { depth--; p++; continue; }
            if (*p == '"') {
                /* key */
                char tmp[512];
                p++; p = parse_json_string(p, tmp, sizeof(tmp));
                p = skip_ws(p);
                if (*p == ':') { p++; p = skip_ws(p); }
                /* skip value (integer) */
                while (*p && *p != ',' && *p != '}') p++;
                if (*p == ',') p++;
                if (depth == 1) vocab_size++;
            } else {
                p++;
            }
        }
    }

    if (vocab_size <= 0 || vocab_size > 200000) {
        fprintf(stderr, "tok_load: unexpected vocab_size=%d\n", vocab_size);
        free(json); free(tok); return NULL;
    }

    tok->vocab_size = vocab_size;
    tok->id_to_str  = (char **)calloc((size_t)vocab_size, sizeof(char *));
    tok->id_is_byte = (uint8_t *)calloc((size_t)vocab_size, 1);
    tok->id_byte_val= (uint8_t *)calloc((size_t)vocab_size, 1);

    /* Allocate string pool: rough estimate 32 bytes avg */
    tok->str_pool_size = (size_t)vocab_size * 32 + 65536;
    tok->str_pool = (char *)malloc(tok->str_pool_size);
    size_t pool_offset = 0;

    /* Hash map for string → id */
    tok->str_map_size = TOK_HASH_SIZE;
    tok->str_map = (tok_hash_entry_t **)calloc((size_t)tok->str_map_size,
                                                sizeof(tok_hash_entry_t *));
    tok_hash_entry_t *entry_pool = (tok_hash_entry_t *)malloc(
            (size_t)vocab_size * sizeof(tok_hash_entry_t));

    /* Second pass: parse vocab */
    {
        const char *p = vocab_start + 1;
        int depth = 1;
        while (*p && depth > 0) {
            p = skip_ws(p);
            if (!*p) break;
            if (*p == '{') { depth++; p++; continue; }
            if (*p == '}') { depth--; p++; continue; }
            if (*p != '"') { p++; continue; }

            /* key: token string */
            char key_buf[512];
            p++; p = parse_json_string(p, key_buf, sizeof(key_buf));
            p = skip_ws(p);
            if (*p == ':') p++;
            p = skip_ws(p);

            /* value: integer token id */
            int id = (int)strtol(p, (char **)&p, 10);
            while (*p && *p != ',' && *p != '}') p++;
            if (*p == ',') p++;

            if (id < 0 || id >= vocab_size) continue;

            /* copy string into pool */
            size_t klen = strlen(key_buf) + 1;
            if (pool_offset + klen > tok->str_pool_size) {
                tok->str_pool_size *= 2;
                tok->str_pool = (char *)realloc(tok->str_pool, tok->str_pool_size);
            }
            memcpy(tok->str_pool + pool_offset, key_buf, klen);
            tok->id_to_str[id] = tok->str_pool + pool_offset;

            /* Check if byte-fallback token: <0xXX> */
            if (key_buf[0] == '<' && key_buf[1] == '0' && key_buf[2] == 'x'
                    && key_buf[5] == '>') {
                char hex[3] = { key_buf[3], key_buf[4], '\0' };
                tok->id_is_byte[id] = 1;
                tok->id_byte_val[id] = (uint8_t)strtol(hex, NULL, 16);
            }

            /* Insert into string → id map */
            str_map_insert(tok->str_map, tok->str_map_size,
                           tok->str_pool + pool_offset,
                           id, entry_pool, id);
            pool_offset += klen;
        }
    }

    /* ── Parse merges ── */
    const char *merges_start = json_find_key(model_start, "merges");
    int n_merges = 0;

    if (merges_start && *merges_start == '[') {
        /* Count merges */
        const char *p = merges_start + 1;
        while (*p && *p != ']') {
            p = skip_ws(p);
            if (*p == '"') {
                n_merges++;
                p++;
                while (*p && *p != '"') { if (*p == '\\') p++; p++; }
                if (*p == '"') p++;
            } else if (*p) p++;
        }
    }

    /* Build merge hash table */
    tok->merge_map_size = MERGE_HASH_SIZE;
    tok->merge_map = (merge_entry_t **)calloc((size_t)tok->merge_map_size,
                                               sizeof(merge_entry_t *));
    merge_entry_t *merge_pool = (merge_entry_t *)malloc(
            (size_t)n_merges * sizeof(merge_entry_t));

    if (merges_start && *merges_start == '[' && n_merges > 0) {
        const char *p = merges_start + 1;
        int rank = 0;
        while (*p && *p != ']' && rank < n_merges) {
            p = skip_ws(p);
            if (*p != '"') { p++; continue; }

            char merge_str[512];
            p++; p = parse_json_string(p, merge_str, sizeof(merge_str));
            while (*p && *p != ',' && *p != ']') p++;
            if (*p == ',') p++;

            /* Split on space: "tok1 tok2" */
            char *space = strchr(merge_str, ' ');
            if (!space) continue;
            *space = '\0';
            const char *s1 = merge_str;
            const char *s2 = space + 1;

            int id1 = str_map_get(tok->str_map, tok->str_map_size, s1);
            int id2 = str_map_get(tok->str_map, tok->str_map_size, s2);

            if (id1 < 0 || id2 < 0) { rank++; continue; }

            /* Find the merged token (s1 + s2 concatenated) */
            char merged_str[512];
            snprintf(merged_str, sizeof(merged_str), "%s%s", s1, s2);
            int merged_id = str_map_get(tok->str_map, tok->str_map_size, merged_str);
            if (merged_id < 0) { rank++; continue; }

            merge_map_insert(tok->merge_map, tok->merge_map_size,
                             id1, id2, rank, merged_id,
                             merge_pool, rank);
            rank++;
        }
    }

    /* ── Parse added_tokens: collect special tokens for pre-scan ── */
    {
        const char *at_start = json_find_key(json, "added_tokens");
        int n_at = 0;
        if (at_start && *at_start == '[') {
            const char *p = at_start + 1;
            while (*p && *p != ']') {
                while (*p && *p != '{' && *p != ']') p++;
                if (*p == '{') { n_at++; p++; }
            }
        }
        if (n_at > 0) {
            tok->special_str = (char **)calloc((size_t)n_at, sizeof(char *));
            tok->special_id  = (int *)calloc((size_t)n_at, sizeof(int));
            tok->n_special   = 0;

            const char *p = at_start + 1;
            char buf[256];
            while (*p && *p != ']' && tok->n_special < n_at) {
                while (*p && *p != '{' && *p != ']') p++;
                if (*p != '{') break;
                p++;
                /* Parse {"id":N,"content":"...","special":true/false,...} */
                int at_id = -1;
                char at_content[256] = "";
                int at_special = 0;
                /* Read keys until closing } */
                while (*p && *p != '}') {
                    while (*p && *p != '"' && *p != '}') p++;
                    if (*p != '"') break;
                    p++; p = parse_json_string(p, buf, sizeof(buf));
                    while (*p && *p != ':' && *p != '}') p++;
                    if (*p != ':') break;
                    p++;
                    while (*p == ' ' || *p == '\t') p++;
                    if (strcmp(buf, "id") == 0) {
                        at_id = (int)strtol(p, NULL, 10);
                        while (*p && *p != ',' && *p != '}') p++;
                    } else if (strcmp(buf, "content") == 0) {
                        if (*p == '"') { p++; p = parse_json_string(p, at_content, sizeof(at_content)); }
                        while (*p && *p != ',' && *p != '}') p++;
                    } else if (strcmp(buf, "special") == 0) {
                        at_special = (strncmp(p, "true", 4) == 0) ? 1 : 0;
                        while (*p && *p != ',' && *p != '}') p++;
                    } else {
                        while (*p && *p != ',' && *p != '}') p++;
                    }
                    if (*p == ',') p++;
                }
                if (*p == '}') p++;
                if (at_special && at_id >= 0 && at_content[0]) {
                    tok->special_str[tok->n_special] = strdup(at_content);
                    tok->special_id[tok->n_special]  = at_id;
                    tok->n_special++;
                }
            }
        }
    }

    free(json);

    fprintf(stderr, "tok_load: vocab=%d  merges=%d  bos=%d eos=%d  special=%d\n",
            vocab_size, n_merges, tok->bos_id, tok->eos_id, tok->n_special);
    return tok;
}

/* ═══════════════════════════════════════════════════════
 * BPE encode
 * ═══════════════════════════════════════════════════════ */

/* UTF-8 byte sequence character length */
static int utf8_char_len(unsigned char c) {
    if (c < 0x80) return 1;
    if ((c & 0xE0) == 0xC0) return 2;
    if ((c & 0xF0) == 0xE0) return 3;
    if ((c & 0xF8) == 0xF0) return 4;
    return 1;
}

/* ▁ in UTF-8 is: E2 96 81 (3 bytes) */
static const char SPACE_SYMBOL[] = "\xE2\x96\x81";

int *tok_encode(tokenizer_t *tok, const char *text, int add_bos, int *n_out) {
    if (!tok || !text || !n_out) return NULL;

    size_t text_len = strlen(text);
    /* After normalization, text could be ~2x longer due to ▁ (3 bytes vs 1) */
    size_t norm_cap = text_len * 4 + 8;
    char *norm = (char *)malloc(norm_cap);

    /* Normalize: prepend ▁, replace spaces with ▁ */
    size_t ni = 0;
    /* Prepend ▁ */
    memcpy(norm + ni, SPACE_SYMBOL, 3); ni += 3;
    for (size_t i = 0; i < text_len; i++) {
        if ((unsigned char)text[i] == ' ') {
            memcpy(norm + ni, SPACE_SYMBOL, 3); ni += 3;
        } else {
            norm[ni++] = text[i];
        }
    }
    norm[ni] = '\0';

    /* Initial token sequence: one token per UTF-8 character */
    /* Worst case: each byte becomes a token */
    int *tokens = (int *)malloc((ni + 8) * sizeof(int));
    int ntok = 0;

    /* Look up ▁ (word-start sentinel) token ID once */
    int space_token_id = str_map_get(tok->str_map, tok->str_map_size, SPACE_SYMBOL);

    size_t i = 0;
    int after_special = 0; /* 1 = just emitted a special token; next word needs ▁ */
    while (i < ni) {
        /* ── Special token pre-scan (e.g. </s> → 2) ── */
        int found_special = 0;
        for (int si = 0; si < tok->n_special; si++) {
            const char *sp = tok->special_str[si];
            size_t splen = strlen(sp);
            if (splen > 0 && i + splen <= ni &&
                memcmp(norm + i, sp, splen) == 0) {
                tokens[ntok++] = tok->special_id[si];
                i += splen;
                found_special = 1;
                after_special = 1; /* next piece needs ▁ re-insertion */
                break;
            }
        }
        if (found_special) continue;

        /* After a special token, SentencePiece re-inserts ▁ before the next
         * character (treating it as start-of-word). Emit ▁ standalone if:
         *  - we just saw a special token
         *  - the current character is NOT itself ▁
         *  - the ▁ token exists in the vocabulary */
        if (after_special) {
            after_special = 0;
            int is_space_sym = (i + 3 <= ni &&
                                (unsigned char)norm[i]   == 0xE2 &&
                                (unsigned char)norm[i+1] == 0x96 &&
                                (unsigned char)norm[i+2] == 0x81);
            if (!is_space_sym && space_token_id >= 0) {
                tokens[ntok++] = space_token_id;
            }
        }

        unsigned char c = (unsigned char)norm[i];
        int clen = utf8_char_len(c);
        if (i + (size_t)clen > ni) clen = 1; /* Safety: clamp at end */

        /* Extract character */
        char ch[8]; int chlen = clen;
        memcpy(ch, norm + i, (size_t)clen);
        ch[clen] = '\0';

        /* Look up in vocab */
        int id = str_map_get(tok->str_map, tok->str_map_size, ch);
        if (id >= 0) {
            tokens[ntok++] = id;
        } else {
            /* Byte fallback: encode each byte as <0xXX> */
            for (int b = 0; b < chlen; b++) {
                uint8_t bval = (uint8_t)norm[i + (size_t)b];
                /* Byte fallback tokens are at IDs 3..258 (0x00=3, ..., 0xFF=258) */
                int bid = (int)bval + 3;
                if (bid < tok->vocab_size) {
                    tokens[ntok++] = bid;
                } else {
                    tokens[ntok++] = tok->unk_id;
                }
            }
        }
        i += (size_t)clen;
    }
    free(norm);

    /* BPE merge passes: repeatedly find best (lowest-rank) pair, merge all */
    int changed = 1;
    while (changed && ntok > 1) {
        changed = 0;
        /* Find the best (lowest-rank) adjacent pair */
        int best_rank = INT32_MAX;
        int best_id1  = -1;
        int best_id2  = -1;
        int best_merged = -1;

        for (int j = 0; j < ntok - 1; j++) {
            merge_entry_t *me = merge_map_get(tok->merge_map, tok->merge_map_size,
                                              tokens[j], tokens[j+1]);
            if (me && me->rank < best_rank) {
                best_rank   = me->rank;
                best_id1    = tokens[j];
                best_id2    = tokens[j+1];
                best_merged = me->merged_id;
            }
        }

        if (best_id1 < 0) break; /* No more merges available */

        /* Apply the winning merge everywhere in the sequence */
        for (int j = 0; j < ntok - 1; j++) {
            if (tokens[j] == best_id1 && tokens[j+1] == best_id2) {
                tokens[j] = best_merged;
                memmove(tokens + j + 1, tokens + j + 2,
                        (size_t)(ntok - j - 2) * sizeof(int));
                ntok--;
                /* Do NOT decrement j: after merge, tokens[j] is the new
                 * merged token. Check pair (j, j+1) in the next iteration,
                 * which is now (merged, old_tokens[j+2]). */
                changed = 1;
            }
        }
    }

    /* Prepend BOS if requested */
    if (add_bos) {
        int *result = (int *)malloc((size_t)(ntok + 1) * sizeof(int));
        result[0] = tok->bos_id;
        memcpy(result + 1, tokens, (size_t)ntok * sizeof(int));
        free(tokens);
        *n_out = ntok + 1;
        return result;
    }

    tokens = (int *)realloc(tokens, (size_t)ntok * sizeof(int));
    *n_out = ntok;
    return tokens;
}

/* ═══════════════════════════════════════════════════════
 * tok_id_to_str — single token to string
 * ═══════════════════════════════════════════════════════ */

const char *tok_id_to_str(tokenizer_t *tok, int id) {
    if (!tok || id < 0 || id >= tok->vocab_size) return "";
    const char *s = tok->id_to_str[id];
    return s ? s : "";
}

/* ═══════════════════════════════════════════════════════
 * tok_decode — sequence to UTF-8 string
 * ═══════════════════════════════════════════════════════ */

char *tok_decode(tokenizer_t *tok, const int *ids, int n) {
    if (!tok || !ids || n <= 0) {
        char *s = (char *)malloc(1);
        s[0] = '\0';
        return s;
    }

    /* Build output buffer */
    size_t cap = (size_t)n * 8 + 4;
    char *out  = (char *)malloc(cap);
    size_t pos = 0;

    for (int i = 0; i < n; i++) {
        if (ids[i] == tok->bos_id || ids[i] == tok->eos_id) continue;

        if (ids[i] >= 0 && ids[i] < tok->vocab_size && tok->id_is_byte[ids[i]]) {
            /* Byte fallback: emit raw byte */
            if (pos >= cap - 4) { cap *= 2; out = (char *)realloc(out, cap); }
            out[pos++] = (char)tok->id_byte_val[ids[i]];
            continue;
        }

        const char *s = tok_id_to_str(tok, ids[i]);
        if (!s || !*s) continue;

        /* Replace ▁ (E2 96 81) with space */
        size_t slen = strlen(s);
        if (pos + slen + 4 > cap) { cap = cap * 2 + slen; out = (char *)realloc(out, cap); }

        /* Copy, replacing ▁ with ' ' */
        size_t j = 0;
        while (j < slen) {
            if (j + 2 < slen &&
                (unsigned char)s[j]   == 0xE2 &&
                (unsigned char)s[j+1] == 0x96 &&
                (unsigned char)s[j+2] == 0x81) {
                out[pos++] = ' ';
                j += 3;
            } else {
                out[pos++] = s[j++];
            }
        }
    }
    out[pos] = '\0';
    /* Strip leading space (artifact of ▁-prepend normalization) */
    if (pos > 0 && out[0] == ' ')
        memmove(out, out + 1, pos);   /* pos includes NUL so we get it */
    return out;
}

/* ═══════════════════════════════════════════════════════
 * Accessors
 * ═══════════════════════════════════════════════════════ */

int tok_vocab_size(tokenizer_t *tok) { return tok ? tok->vocab_size : 0; }
int tok_bos_id(tokenizer_t *tok)     { return tok ? tok->bos_id : 1; }
int tok_eos_id(tokenizer_t *tok)     { return tok ? tok->eos_id : 2; }

void tok_free(tokenizer_t *tok) {
    if (!tok) return;
    free(tok->id_to_str);
    free(tok->id_is_byte);
    free(tok->id_byte_val);
    free(tok->str_map);
    free(tok->merge_map);
    free(tok->str_pool);
    for (int i = 0; i < tok->n_special; i++) free(tok->special_str[i]);
    free(tok->special_str);
    free(tok->special_id);
    free(tok);
}
