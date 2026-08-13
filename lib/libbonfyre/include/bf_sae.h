/*
 * bf_sae.h — Bonfyre Sparse Autoencoder (.bfsae) format and runtime API.
 *
 * .bfsae is a mmap-friendly binary format for SAE feature dictionaries.
 * It stores the encoder/decoder matrices, per-feature labels, thresholds,
 * and calibration metadata for a single (model_family, layer, site) tuple.
 *
 * Wire layout (little-endian, 8-byte aligned):
 *
 *   [BfsaeHeader]            — fixed 256 bytes
 *   [BfsaeFeatureMeta × N]  — N = header.feature_count
 *   [float encoder[N][D]]   — encoder matrix, fp16 packed
 *   [float decoder[D][N]]   — decoder matrix, fp16 packed
 *   [BfsaeLabelEntry × N]   — label strings (variable, offset-indexed)
 *   [uint8_t label_heap[]]  — NUL-terminated label strings
 *
 * The encoder/decoder sections are addressed via header.encoder_offset and
 * header.decoder_offset. Both are byte offsets from the file start.
 *
 * Usage (mmap path):
 *   BfsaeDict *dict = bfsae_open(path);          // mmap + validate
 *   BfsaeActivation *act = bfsae_activate(dict, residual_fp16, dim, topk);
 *   bfsae_activation_free(act);
 *   bfsae_close(dict);
 *
 * Feature manifest output (JSON):
 *   bfsae_manifest_json(act, out, out_sz)         // fills features.json
 */

#pragma once
#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── magic / version ──────────────────────────────────────────────────────── */

#define BFSAE_MAGIC      "BFSAE1\0\0"   /* 8 bytes, null-padded          */
#define BFSAE_MAGIC_LEN  8
#define BFSAE_VERSION    1

/* ── activation site ──────────────────────────────────────────────────────── */

typedef enum {
    BFSAE_SITE_RESIDUAL   = 0,   /* post-layer residual stream (recommended) */
    BFSAE_SITE_MLP        = 1,   /* MLP output pre-residual                  */
    BFSAE_SITE_ATTENTION  = 2,   /* attention output pre-residual            */
    BFSAE_SITE_EMBED      = 3,   /* embedding layer                          */
} BfsaeSite;

/* ── quantisation dtype ───────────────────────────────────────────────────── */

typedef enum {
    BFSAE_DTYPE_FP32  = 0,
    BFSAE_DTYPE_FP16  = 1,
    BFSAE_DTYPE_BF16  = 2,
    BFSAE_DTYPE_INT8  = 3,
    BFSAE_DTYPE_FPQ   = 4,   /* Bonfyre FPQ compressed                      */
} BfsaeDtype;

/* ── feature class tags ───────────────────────────────────────────────────── */
#define BFSAE_TAG_NONE       0x00
#define BFSAE_TAG_DANGER     0x01  /* activates on harmful content              */
#define BFSAE_TAG_STYLE      0x02  /* register / tone / formality               */
#define BFSAE_TAG_TASK       0x04  /* task-type marker (summarise, classify…)   */
#define BFSAE_TAG_LANGUAGE   0x08  /* language / script detection               */
#define BFSAE_TAG_QUALITY    0x10  /* quality/confidence signal                 */
#define BFSAE_TAG_SYNTAX     0x20  /* syntactic / structural feature            */
#define BFSAE_TAG_SEMANTIC   0x40  /* high-level semantic concept               */
#define BFSAE_TAG_NOISE      0x80  /* uninformative / polysemantic              */

/* ── header (256 bytes fixed) ────────────────────────────────────────────── */

typedef struct {
    char     magic[8];           /* "BFSAE1\0\0"                              */
    uint8_t  version;            /* BFSAE_VERSION = 1                         */
    uint8_t  site;               /* BfsaeSite                                 */
    uint8_t  dtype;              /* BfsaeDtype (encoder + decoder matrices)   */
    uint8_t  _pad0[1];
    uint32_t layer;              /* transformer layer index (0-based)          */
    uint32_t hidden_dim;         /* model residual/hidden dimension D          */
    uint32_t feature_count;      /* number of SAE features N                  */
    uint64_t encoder_offset;     /* byte offset to float16[N][D] encoder       */
    uint64_t decoder_offset;     /* byte offset to float16[D][N] decoder       */
    uint64_t meta_offset;        /* byte offset to BfsaeFeatureMeta[N]         */
    uint64_t label_offset;       /* byte offset to BfsaeLabelEntry[N]          */
    uint64_t label_heap_offset;  /* byte offset to label string heap           */
    uint64_t file_size;          /* total file size in bytes                   */
    char     model_family[64];   /* e.g. "qwen3-8b", "llama3-8b"              */
    char     model_revision[32]; /* e.g. "v1.0", git sha, or ""               */
    char     training_notes[32]; /* e.g. "pile-10bt-l1=1e-3" or ""            */
    uint8_t  checksum[32];       /* SHA-256 of file body (after header.chksum) */
    uint8_t  _pad1[24];          /* pad to 256 bytes total                    */
} BfsaeHeader;  /* static_assert(sizeof(BfsaeHeader) == 256) */

/* ── per-feature metadata (32 bytes) ─────────────────────────────────────── */

typedef struct {
    uint32_t feature_id;         /* stable feature index [0, N)               */
    float    bias;               /* per-feature decoder bias                  */
    float    mean_activation;    /* calibration mean activation                */
    float    std_activation;     /* calibration std activation                 */
    float    danger_threshold;   /* threshold for BFSAE_TAG_DANGER            */
    float    default_threshold;  /* default top-k gate threshold              */
    uint8_t  tags;               /* BFSAE_TAG_* bitmask                        */
    uint8_t  _pad[7];
} BfsaeFeatureMeta;  /* 32 bytes */

/* ── label table entry (8 bytes, label string in heap) ───────────────────── */

typedef struct {
    uint32_t feature_id;
    uint32_t label_offset;       /* byte offset into label_heap               */
} BfsaeLabelEntry;

/* ── runtime activation result ───────────────────────────────────────────── */

typedef struct {
    uint32_t feature_id;
    float    activation;
    float    normalised;         /* activation / std_activation                */
    uint8_t  tags;
    const char *label;           /* pointer into mmap'd label heap, or ""     */
} BfsaeTopFeature;

typedef struct {
    uint32_t            count;       /* actual top-k returned (≤ requested)  */
    uint32_t            top_k;       /* top-k requested                       */
    uint32_t            layer;
    const BfsaeHeader  *header;      /* back-pointer to owning dict header    */
    BfsaeTopFeature     features[];  /* flexible array, count entries         */
} BfsaeActivation;

/* ── dict handle (opaque) ─────────────────────────────────────────────────── */

typedef struct {
    const BfsaeHeader      *header;
    const BfsaeFeatureMeta *meta;     /* mmap pointer to meta section         */
    const BfsaeLabelEntry  *labels;   /* mmap pointer to label table          */
    const char             *label_heap; /* mmap pointer to label strings      */
    const void             *encoder;  /* mmap pointer to encoder matrix       */
    const void             *decoder;  /* mmap pointer to decoder matrix       */
    size_t                  file_size;
    void                   *_mmap_base;
    int                     _fd;
} BfsaeDict;

/* ── API ──────────────────────────────────────────────────────────────────── */

/*
 * bfsae_open — mmap a .bfsae file, validate magic/checksum, return dict.
 * Returns NULL on error (prints reason to stderr).
 */
BfsaeDict *bfsae_open(const char *path);

/*
 * bfsae_close — unmap and free a dict.
 */
void bfsae_close(BfsaeDict *dict);

/*
 * bfsae_activate — run encoder: residual_fp16[hidden_dim] → top-k features.
 * residual: pointer to float16 array of length hidden_dim.
 * top_k:    number of features to return (sorted descending by activation).
 * Returns heap-allocated BfsaeActivation; caller frees with bfsae_activation_free().
 */
BfsaeActivation *bfsae_activate(const BfsaeDict *dict,
                                 const void      *residual_fp16,
                                 uint32_t         hidden_dim,
                                 uint32_t         top_k);

/*
 * bfsae_activate_f32 — same as bfsae_activate but input is float32.
 */
BfsaeActivation *bfsae_activate_f32(const BfsaeDict *dict,
                                     const float     *residual_f32,
                                     uint32_t         hidden_dim,
                                     uint32_t         top_k);

/*
 * bfsae_activation_free — free a result returned by bfsae_activate*.
 */
void bfsae_activation_free(BfsaeActivation *act);

/*
 * bfsae_manifest_json — write features.json payload to out[0..out_sz).
 * artifact_id: parent artifact this activation belongs to (may be NULL).
 * Returns bytes written (excl. NUL), or -1 on truncation.
 */
int bfsae_manifest_json(const BfsaeActivation *act,
                         const char            *artifact_id,
                         char                  *out,
                         size_t                 out_sz);

/*
 * bfsae_danger_check — return 1 if any DANGER-tagged feature in act
 * exceeds threshold alpha.  0 = clear, 1 = danger, -1 = no danger features.
 */
int bfsae_danger_check(const BfsaeActivation *act, float alpha);

/*
 * bfsae_feature_hash — stable FNV-1a semantic hash of top-k feature signature.
 * out_hex: buffer of at least 17 bytes, receives 16-char hex + NUL.
 */
void bfsae_feature_hash(const BfsaeActivation *act, char *out_hex);

/*
 * bfsae_inspect — print human-readable dict summary to stdout.
 */
void bfsae_inspect(const BfsaeDict *dict);

/*
 * bfsae_write_synthetic — write a minimal synthetic .bfsae for testing.
 * feature_count random decoder directions, unit-norm, no labels.
 */
int bfsae_write_synthetic(const char *path,
                           const char *model_family,
                           uint32_t    layer,
                           uint32_t    hidden_dim,
                           uint32_t    feature_count);

#ifdef __cplusplus
}
#endif
