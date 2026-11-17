/* bf_static_embed — text to vector with a distilled static embedding.
 *
 * A .bfse file (bonfyre-embed distill) holds a teacher model's vocabulary,
 * the Unicode fold table its tokenizer uses, and one int8 row per token. A
 * text is embedded by tokenizing it exactly as the teacher's uncased BERT
 * WordPiece tokenizer would, averaging the rows of its tokens and normalising.
 * No model runtime is involved, so CMS publishing, the web address server and
 * a browser (components/BonfyreServiceSite/static-embed.mjs) all compute the
 * same vector.
 *
 * A .bfsi search index is a set of unit vectors stored as int8 rows with one
 * scale each, bound to the SHA-256 of the .bfse that produced them:
 *   "BFSI" u32 version=1 u32 dims u32 count u8 model_sha256[32]
 *   float32 scale[count]; int8 row[count][dims]
 * The passages the rows stand for travel beside it, in the same order.
 *
 * cannot infer: a static vector -> the teacher's sentence vector (order and
 * context are gone); a high score -> the same meaning; scores compare only
 * within one .bfse. */
#ifndef BF_STATIC_EMBED_H
#define BF_STATIC_EMBED_H

#include <stddef.h>
#include <stdint.h>

typedef struct BfStaticModel BfStaticModel;

/* NULL with a reason in err when the file is not a readable version 1 .bfse. */
BfStaticModel *bf_static_load(const char *path, char *err, size_t err_size);
void bf_static_free(BfStaticModel *model);
int bf_static_dims(const BfStaticModel *model);
/* SHA-256 of the .bfse file the model was loaded from. */
const uint8_t *bf_static_model_sha256(const BfStaticModel *model);

/* Writes a unit vector of bf_static_dims floats; returns the number of tokens
 * that carried a row (0 means the text says nothing the model knows). */
int bf_static_embed(const BfStaticModel *model, const char *text, float *out);

/* Writes count unit vectors (count x dims floats) as a .bfsi index. */
int bf_static_index_write(const char *path, const BfStaticModel *model, int count, const float *vectors);

/* A .bfsi index mapped read-only, for scoring a query against every row
 * without copying it. NULL with a reason in err when the file is not a
 * version 1 index, its width differs from the model's, or its rows were made
 * by another .bfse (vectors from two models are not comparable). */
typedef struct BfStaticIndex BfStaticIndex;
BfStaticIndex *bf_static_index_map(const char *path, const BfStaticModel *model, char *err, size_t err_size);
void bf_static_index_unmap(BfStaticIndex *index);
int bf_static_index_count(const BfStaticIndex *index);

/* The best k rows for a unit query vector, best first, written as row
 * numbers and cosine scores. accept (may be NULL) is asked about each row
 * before it is ranked, so a caller can leave rows out (one source, newer
 * than a date) without a second pass. Returns how many were written. */
int bf_static_index_top(const BfStaticIndex *index, const float *query, int k,
                        int (*accept)(int row, void *ctx), void *ctx, int *rows, float *scores);

#endif
