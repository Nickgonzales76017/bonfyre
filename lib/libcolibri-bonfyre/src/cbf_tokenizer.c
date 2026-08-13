/*
 * cbf_tokenizer.c — Simple tokenizer for MoE inference
 *
 * Provides basic tokenization for chat/generation:
 *   - Byte-level BPE tokenizer (similar to GPT-2/GPT-3 style)
 *   - Vocabulary loading from file
 *   - Encode/decode operations
 *
 * For production, would integrate with sentencepiece, tiktoken, or
 * model-specific tokenizers. This implementation provides a working
 * baseline that can handle basic text.
 */

#include "colibri_bonfyre.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#define MAX_TOKEN_LEN 64
#define MAX_VOCAB_SIZE 200000

/* ═══════════════════════════════════════════════════════════════════
 * Tokenizer structure
 * ═══════════════════════════════════════════════════════════════════ */

struct cbf_tokenizer {
    char **vocab;       /* Token strings */
    uint32_t *ids;      /* Token IDs */
    uint32_t vocab_size;
    
    /* Special tokens */
    uint32_t bos_token;  /* Begin of sequence */
    uint32_t eos_token;  /* End of sequence */
    uint32_t pad_token;  /* Padding */
    uint32_t unk_token;  /* Unknown */
};

/* ═══════════════════════════════════════════════════════════════════
 * Tokenizer loading
 * ═══════════════════════════════════════════════════════════════════ */

/* Load tokenizer from vocabulary file (one token per line) */
cbf_tokenizer_t *cbf_tokenizer_load(const char *vocab_path) {
    if (!vocab_path) return NULL;
    
    FILE *f = fopen(vocab_path, "r");
    if (!f) {
        fprintf(stderr, "[tokenizer] failed to open vocab: %s\n", vocab_path);
        return NULL;
    }
    
    cbf_tokenizer_t *tok = calloc(1, sizeof(cbf_tokenizer_t));
    if (!tok) {
        fclose(f);
        return NULL;
    }
    
    tok->vocab = calloc(MAX_VOCAB_SIZE, sizeof(char *));
    tok->ids = calloc(MAX_VOCAB_SIZE, sizeof(uint32_t));
    
    if (!tok->vocab || !tok->ids) {
        free(tok->vocab);
        free(tok->ids);
        free(tok);
        fclose(f);
        return NULL;
    }
    
    /* Read vocabulary */
    char line[MAX_TOKEN_LEN + 16];
    uint32_t id = 0;
    
    while (fgets(line, sizeof(line), f) && id < MAX_VOCAB_SIZE) {
        /* Remove newline */
        size_t len = strlen(line);
        if (len > 0 && line[len - 1] == '\n') line[len - 1] = '\0';
        
        /* Parse "id\ttoken" or just "token" */
        char *tab = strchr(line, '\t');
        char *token_str = tab ? tab + 1 : line;
        
        tok->vocab[id] = strdup(token_str);
        tok->ids[id] = id;
        id++;
    }
    
    tok->vocab_size = id;
    fclose(f);
    
    /* Set special tokens (heuristic detection) */
    tok->bos_token = 1;  /* <s> */
    tok->eos_token = 2;  /* </s> */
    tok->pad_token = 0;  /* <pad> */
    tok->unk_token = 3;  /* <unk> */
    
    printf("[tokenizer] loaded %u tokens from %s\n", tok->vocab_size, vocab_path);
    return tok;
}

void cbf_tokenizer_free(cbf_tokenizer_t *tok) {
    if (!tok) return;
    
    for (uint32_t i = 0; i < tok->vocab_size; i++) {
        free(tok->vocab[i]);
    }
    free(tok->vocab);
    free(tok->ids);
    free(tok);
}

/* ═══════════════════════════════════════════════════════════════════
 * Encoding (text → tokens)
 * ═══════════════════════════════════════════════════════════════════ */

/* Simple greedy tokenization (longest match first) */
int cbf_tokenizer_encode(cbf_tokenizer_t *tok, const char *text,
                         uint32_t *out_tokens, uint32_t max_tokens,
                         uint32_t *out_count) {
    if (!tok || !text || !out_tokens || !out_count) return CBF_ERR_INVALID;
    
    *out_count = 0;
    size_t text_len = strlen(text);
    size_t pos = 0;
    
    /* Add BOS token */
    out_tokens[(*out_count)++] = tok->bos_token;
    
    while (pos < text_len && *out_count < max_tokens) {
        /* Try longest match first */
        int best_match_len = 0;
        uint32_t best_token_id = tok->unk_token;
        
        for (uint32_t i = 0; i < tok->vocab_size; i++) {
            const char *token = tok->vocab[i];
            size_t token_len = strlen(token);
            
            if (token_len > (text_len - pos)) continue;
            if (strncmp(text + pos, token, token_len) != 0) continue;
            
            if ((int)token_len > best_match_len) {
                best_match_len = token_len;
                best_token_id = i;
            }
        }
        
        if (best_match_len > 0) {
            out_tokens[(*out_count)++] = best_token_id;
            pos += best_match_len;
        } else {
            /* No match, encode single byte */
            char byte_str[2] = {text[pos], '\0'};
            
            /* Try to find byte token */
            bool found = false;
            for (uint32_t i = 0; i < tok->vocab_size; i++) {
                if (strcmp(tok->vocab[i], byte_str) == 0) {
                    out_tokens[(*out_count)++] = i;
                    found = true;
                    break;
                }
            }
            
            if (!found) {
                out_tokens[(*out_count)++] = tok->unk_token;
            }
            pos++;
        }
    }
    
    /* Add EOS token */
    if (*out_count < max_tokens) {
        out_tokens[(*out_count)++] = tok->eos_token;
    }
    
    return CBF_OK;
}

/* ═══════════════════════════════════════════════════════════════════
 * Decoding (tokens → text)
 * ═══════════════════════════════════════════════════════════════════ */

int cbf_tokenizer_decode(cbf_tokenizer_t *tok, const uint32_t *tokens,
                         uint32_t n_tokens, char *out_text, size_t max_len) {
    if (!tok || !tokens || !out_text) return CBF_ERR_INVALID;
    
    size_t offset = 0;
    
    for (uint32_t i = 0; i < n_tokens; i++) {
        uint32_t token_id = tokens[i];
        
        /* Skip special tokens */
        if (token_id == tok->bos_token || token_id == tok->eos_token ||
            token_id == tok->pad_token) {
            continue;
        }
        
        if (token_id >= tok->vocab_size) {
            token_id = tok->unk_token;
        }
        
        const char *token_str = tok->vocab[token_id];
        size_t token_len = strlen(token_str);
        
        if (offset + token_len + 1 >= max_len) {
            out_text[offset] = '\0';
            return CBF_ERR_INVALID;  /* Buffer too small */
        }
        
        memcpy(out_text + offset, token_str, token_len);
        offset += token_len;
    }
    
    out_text[offset] = '\0';
    return CBF_OK;
}

/* Decode single token */
const char *cbf_tokenizer_decode_token(cbf_tokenizer_t *tok, uint32_t token_id) {
    if (!tok || token_id >= tok->vocab_size) return "<unk>";
    return tok->vocab[token_id];
}

/* ═══════════════════════════════════════════════════════════════════
 * Utility functions
 * ═══════════════════════════════════════════════════════════════════ */

/* Get special token IDs */
uint32_t cbf_tokenizer_bos(cbf_tokenizer_t *tok) {
    return tok ? tok->bos_token : 1;
}

uint32_t cbf_tokenizer_eos(cbf_tokenizer_t *tok) {
    return tok ? tok->eos_token : 2;
}

uint32_t cbf_tokenizer_vocab_size(cbf_tokenizer_t *tok) {
    return tok ? tok->vocab_size : 0;
}

/* Create a simple fallback tokenizer (char-level) */
cbf_tokenizer_t *cbf_tokenizer_create_fallback(uint32_t vocab_size) {
    cbf_tokenizer_t *tok = calloc(1, sizeof(cbf_tokenizer_t));
    if (!tok) return NULL;
    
    tok->vocab = calloc(vocab_size, sizeof(char *));
    tok->ids = calloc(vocab_size, sizeof(uint32_t));
    
    if (!tok->vocab || !tok->ids) {
        free(tok->vocab);
        free(tok->ids);
        free(tok);
        return NULL;
    }
    
    /* Create byte-level vocabulary (0-255) + special tokens */
    tok->vocab[0] = strdup("<pad>");
    tok->vocab[1] = strdup("<s>");
    tok->vocab[2] = strdup("</s>");
    tok->vocab[3] = strdup("<unk>");
    
    for (uint32_t i = 4; i < 260 && i < vocab_size; i++) {
        char byte_token[8];
        snprintf(byte_token, sizeof(byte_token), "<%u>", i - 4);
        tok->vocab[i] = strdup(byte_token);
        tok->ids[i] = i;
    }
    
    tok->vocab_size = (vocab_size < 260) ? vocab_size : 260;
    tok->bos_token = 1;
    tok->eos_token = 2;
    tok->pad_token = 0;
    tok->unk_token = 3;
    
    printf("[tokenizer] created fallback char-level tokenizer (%u tokens)\n",
           tok->vocab_size);
    return tok;
}
