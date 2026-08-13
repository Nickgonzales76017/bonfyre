/*
 * tokenizer.h — BPE tokenizer for FPQ Ember runtime
 *
 * Reads HuggingFace tokenizer.json (BPE with byte_fallback=true).
 * Compatible with LLaMA / TinyLlama / Mistral / Qwen vocab format.
 *
 * Quick start:
 *   tokenizer_t *tok = tok_load("path/to/tokenizer.json");
 *   int n; int *ids = tok_encode(tok, "Hello, world!", 1, &n);
 *   for (int i = 0; i < n; i++) printf("%s", tok_id_to_str(tok, ids[i]));
 *   free(ids);
 *   tok_free(tok);
 */
#ifndef BONFYRE_TOKENIZER_H
#define BONFYRE_TOKENIZER_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque tokenizer handle */
typedef struct tokenizer tokenizer_t;

/*
 * Load a tokenizer from a tokenizer.json file.
 * Returns NULL on failure.
 */
tokenizer_t *tok_load(const char *json_path);

/*
 * Encode a UTF-8 string to token IDs.
 * add_bos: prepend BOS token (<s> = 1)
 * n_out:   filled with token count
 * Returns malloc'd array; caller must free().
 */
int *tok_encode(tokenizer_t *tok, const char *text, int add_bos, int *n_out);

/*
 * Decode a single token ID to its string representation.
 * Returns pointer into internal storage (do not free).
 * Handles byte-fallback tokens: <0x0A> → '\n', etc.
 */
const char *tok_id_to_str(tokenizer_t *tok, int id);

/*
 * Decode a sequence of token IDs to a UTF-8 string.
 * Returns malloc'd string; caller must free().
 * Note: byte-fallback tokens are decoded to raw bytes.
 */
char *tok_decode(tokenizer_t *tok, const int *ids, int n);

/* Vocab size */
int tok_vocab_size(tokenizer_t *tok);

/* Special token IDs */
int tok_bos_id(tokenizer_t *tok);
int tok_eos_id(tokenizer_t *tok);

void tok_free(tokenizer_t *tok);

#ifdef __cplusplus
}
#endif
#endif /* BONFYRE_TOKENIZER_H */
