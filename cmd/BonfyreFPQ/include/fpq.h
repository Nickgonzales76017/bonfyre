/*
 * fpq.h — Functional Polar Quantization v2
 *
 * Dense weights are source/lift material, not the only useful runtime form.
 * FPQ factors them into shared tensor structure, executable seed programs and
 * explicit correction state, while model identity remains outside the codec.
 *
 * v2 — Lambda-Polar Morphism (Φ: L → G):
 *
 *   ENCODE: weights → Haar rotation (FWHT) → recursive S^N polar decomposition
 *           → TYPE INFERENCE (angles ~ Beta → subtract E[θ] for free)
 *           → seed discovery on DEVIATIONS (logarithmic depth convergence)
 *           → QJL 1-bit residual → emit {seed, qjl_bits}
 *           (NO per-block radius — radius is TYPE-INFERRED from √N * σ̂)
 *
 *   DECODE: seed → recursive combinator expansion → deviations
 *           → add type-inferred E[θ] → inverse polar (with inferred radius)
 *           → inverse FWHT → weights
 *
 * KEY THEORETICAL ADVANCES (v2):
 *   1. Zero-Metadata: radius derived from tensor type (System F inference),
 *      codebook constants via De Bruijn indices (never stored)
 *   2. Logarithmic Depth: REFINE chains → each β-reduction halves error
 *      (vs TurboQuant's linear bit-rate scaling)
 *   3. Morphism Preservation: Φ preserves inner products as Category
 *      isomorphism, not stochastic JL approximation
 *
 * The seed IS the model. Everything else is math.
 */
#ifndef BONFYRE_FPQ_H
#define BONFYRE_FPQ_H

#include <stdint.h>
#include <stddef.h>

/* ── Configuration ── */
#define FPQ_VERSION          "3.0.0"
#define FPQ_MAGIC            0x46505121  /* "FPQ!" */
#define FPQ_BLOCK_DIM        256         /* vectors processed in blocks of 256 */
#define FPQ_SEED_MAX_DEPTH   24          /* max combinator recursion depth */
#define FPQ_QJL_PROJECTIONS  64          /* number of random projections for QJL */
#define FPQ_E8_DIM           8           /* E8 lattice dimension for angle groups */
#define FPQ_REFINE_MAX       6           /* max additive refinement layers */
#define FPQ_CODEBOOK_SIZE    32          /* De Bruijn constant codebook entries */

/* Forward declarations for v4 types used in fpq_tensor_t */
typedef struct fpq_ghost fpq_ghost_t;

/* v3 — Lambda Calculus Optimizations */
#define FPQ_MODE_FWHT        0           /* default: FWHT + polar + seed */
#define FPQ_MODE_DIRECT      1           /* structured: skip FWHT, raw angles */
#define FPQ_MODE_COORD       2           /* supercombinator: FWHT + direct 2-bit quantization */
#define FPQ_MEMO_SLOTS       128         /* memoization cache slots */

/* ── Haar Rotation (Fast Walsh-Hadamard Transform) ── */

/*
 * In-place FWHT on n floats (n must be power of 2).
 * After this, coordinates follow a concentrated Beta distribution
 * regardless of the input. Data-oblivious.
 */
void fpq_fwht(float *x, size_t n);

/*
 * Inverse FWHT — same operation with 1/n scaling.
 */
void fpq_fwht_inverse(float *x, size_t n);

/*
 * Apply random sign flips before FWHT for full Haar randomization.
 * seed determines the random signs (reproducible).
 */
void fpq_random_signs(float *x, size_t n, uint64_t seed);
void fpq_random_signs_inverse(float *x, size_t n, uint64_t seed);


/* ── Recursive Polar Decomposition (S^N shelling) ── */

/*
 * Convert N-dimensional vector to polar form:
 *   - angles[0..n-2]: N-1 phase angles on S^{N-1}
 *   - returns: the global radius (vector norm / energy)
 *
 * This is the recursive S^N decomposition. Pairs are converted
 * to (r, θ), then radii are paired again, recursively, until
 * one final radius remains.
 */
float fpq_polar_encode(const float *x, size_t n, float *angles);

/*
 * Inverse: angles + radius → Cartesian coordinates.
 */
void fpq_polar_decode(float radius, const float *angles, size_t n, float *x);

/*
 * PAIRWISE recursive polar decomposition (for direct mode):
 *
 * Instead of the N-sphere chain where θ_0 affects ALL coordinates,
 * pairwise decomposition works hierarchically:
 *   Level 0: N/2 pair angles (each affects 2 adjacent coordinates)
 *   Level 1: N/4 pair angles (each affects 4 coordinates)
 *   ...
 *   Level log2(N)-1: 1 angle (global phase)
 *
 * Error amplification: O(log N) vs O(N) for N-sphere.
 * Structure preservation: adjacent-pair angles capture local patterns.
 *
 * Angle layout: [level0: N/2] [level1: N/4] [level2: N/8] ... [last: 1]
 */
float fpq_polar_encode_pairwise(const float *x, size_t n, float *angles);
void  fpq_polar_decode_pairwise(float radius, const float *angles, size_t n, float *x);


/* ── Sphere Hardening ── */

/*
 * In high dimensions, Gaussian-distributed data concentrates on a thin
 * shell at radius ≈ sqrt(N). After FWHT, the radius becomes predictable.
 * We store it once per block and spend all bits on angles.
 */
typedef struct {
    float    radius;          /* single scalar: the block norm */
    size_t   n_angles;        /* = block_dim - 1 */
    float   *angles;          /* the N-1 phase angles */
} fpq_polar_t;


/* ── Seed Combinator Engine (The Lambda Core) ── */

/*
 * v2 Combinator vocabulary — the λ-terms that compose into seed programs:
 *
 * ORIGINAL (v1):
 *   ROT(θ)      — base rotation by fixed angle θ
 *   PAIR(a, b)  — apply a to first half, b to second half (recursive split)
 *   SCALE(s,a)  — multiply angles from a by scalar s
 *   SHIFT(d,a)  — shift angles from a by offset d
 *   REP(k, a)   — repeat combinator a, k times (Church numeral encoding)
 *   FOLD(a)     — Y-combinator: fixed-point recursive application
 *   LERP(t,a,b) — interpolate between a and b outputs
 *   FREQ(k,p)   — sin(k*i/n + p): frequency-domain primitive
 *
 * NEW (v2 — Lambda-Polar):
 *   RAMP(a,b)   — linear ramp from a to b: a + (b-a)*i/n
 *   REFINE(a,b) — additive refinement: a + b (chained β-reduction)
 *   DBREF(idx)  — De Bruijn reference to universal constant codebook
 *   CHURCH(n,a) — Church numeral: n-fold composition of child
 *   DCT(k,amp)  — DCT-II basis: amp * cos(π*(2i+1)*k/(2n))
 */

typedef enum {
    FPQ_OP_ROT    = 0,   /* base rotation: 1 float param */
    FPQ_OP_PAIR   = 1,   /* binary split: 2 child combinators */
    FPQ_OP_SCALE  = 2,   /* scale: 1 float + 1 child */
    FPQ_OP_SHIFT  = 3,   /* shift: 1 float + 1 child */
    FPQ_OP_REP    = 4,   /* repeat: 1 int + 1 child (Church numeral) */
    FPQ_OP_FOLD   = 5,   /* Y-combinator: 1 child (fixed-point) */
    FPQ_OP_LERP   = 6,   /* interpolate between 2 children: 1 float param */
    FPQ_OP_FREQ   = 7,   /* frequency: sin(k*i/n + phase), 2 float params */
    /* v2 ops */
    FPQ_OP_RAMP   = 8,   /* linear ramp: param[0] to param[1] */
    FPQ_OP_REFINE = 9,   /* additive refinement: left + right */
    FPQ_OP_DBREF  = 10,  /* De Bruijn codebook reference: iparam = index */
    FPQ_OP_CHURCH = 11,  /* Church numeral: iparam-fold compose of child */
    FPQ_OP_DCT    = 12,  /* DCT-II basis: iparam=k, param[0]=amplitude */
} fpq_op_t;

typedef struct fpq_node {
    fpq_op_t        op;
    float           param[2];     /* up to 2 float parameters */
    int             iparam;       /* integer parameter (for REP) */
    struct fpq_node *left;        /* first child (or NULL) */
    struct fpq_node *right;       /* second child (or NULL) */
} fpq_node_t;

/*
 * A seed program: the root combinator + metadata
 */
typedef struct {
    fpq_node_t *root;            /* combinator tree root */
    size_t      target_dim;      /* how many angles this generates */
    size_t      tree_size;       /* number of nodes (= program size) */
    float       distortion;      /* MSE vs original angles */
} fpq_seed_t;

/*
 * Expand a seed program into an angle sequence.
 * output must have space for target_dim floats.
 */
void fpq_seed_expand(const fpq_seed_t *seed, float *output);

/*
 * Discover the minimal seed that approximates the target angles.
 * This is the Kolmogorov compression step — find shortest λ-term
 * that generates angles within tolerance.
 *
 * max_nodes: budget for combinator tree size
 * tolerance: max MSE allowed
 * returns: allocated seed (caller must free with fpq_seed_free)
 */
fpq_seed_t *fpq_seed_discover(const float *target_angles, size_t n,
                               size_t max_nodes, float tolerance);

fpq_node_t *fpq_node_alloc(fpq_op_t op);
void         fpq_seed_free(fpq_seed_t *seed);
fpq_seed_t  *fpq_seed_clone(const fpq_seed_t *src);
int          fpq_seed_equal(const fpq_seed_t *a, const fpq_seed_t *b);
uint64_t     fpq_seed_hash(const fpq_seed_t *seed);
size_t       fpq_seed_program_bytes(const fpq_seed_t *seed);
void         fpq_node_free(fpq_node_t *node);
size_t       fpq_node_count(const fpq_node_t *node);


/* ── QJL Bias Correction ── */

/*
 * After polar quantization + seed compression, there's a residual error.
 * QJL projects this error into a 1-bit random subspace to preserve
 * inner product relationships (critical for attention scores).
 *
 * For each block:
 *   residual = original_angles - seed_expanded_angles
 *   qjl_bits = sign(random_projection(residual))
 *
 * At decode time, the QJL bits correct the inner product estimate.
 */

typedef struct {
    uint64_t *bits;              /* packed 1-bit projections */
    size_t    n_projections;     /* = FPQ_QJL_PROJECTIONS */
    size_t    n_elements;        /* = block dimension */
    uint64_t  proj_seed;         /* seed for random projection matrix */
} fpq_qjl_t;

/*
 * Compute QJL correction bits from the residual.
 */
fpq_qjl_t *fpq_qjl_encode(const float *residual, size_t n, uint64_t proj_seed);

/*
 * Correct an inner product estimate using QJL bits.
 * Returns the bias-corrected <a, b> estimate.
 */
float fpq_qjl_correct_dot(const fpq_qjl_t *qjl_a, const fpq_qjl_t *qjl_b,
                           float raw_dot);

/*
 * Reconstruct an approximation of the residual from QJL 1-bit signs.
 * Uses 1-bit compressed sensing: x̂ = (||x||/m) Σ y_i φ_i.
 * residual_norm: the L2 norm of the original residual (stored per block).
 */
void fpq_qjl_reconstruct(const fpq_qjl_t *qjl, float residual_norm,
                          float *output);

void fpq_qjl_free(fpq_qjl_t *qjl);


/* ── Full FPQ Codec ── */

/*
 * A factored tensor representation: shared/base structure, block programs,
 * quantized coordinates and explicit correction state. It is not model identity.
 */
typedef struct {
    char       name[128];         /* tensor name (e.g., "model.layers.0.self_attn.q_proj.weight") */
    size_t     original_rows;     /* original shape */
    size_t     original_cols;
    size_t     n_blocks;          /* number of FPQ_BLOCK_DIM blocks */

    /* Per-block data */
    fpq_seed_t **seeds;           /* one seed program per block */
    fpq_qjl_t  **qjl;            /* one QJL correction per block */
    float       *radii;           /* one radius per block (sphere hardening) */
    uint64_t     haar_seed;       /* random sign seed for FWHT */

    /* v3 — Lambda calculus optimizations */
    fpq_seed_t  *base_seed;       /* graph reduction: tensor-level shared DAG node */
    uint8_t      mode;            /* FPQ_MODE_FWHT=0, DIRECT=1, COORD=2 */

    /* COORD mode: direct Lloyd-Max quantization (supercombinator) */
    float       *coord_scales;    /* per-block RMS scale factor */
    uint8_t    **coord_quants;    /* per-block quantization indices [block][padded_dim] */
    uint8_t      coord_bits;      /* bits per coordinate (2 or 3) */
    float       *coord_residual_norms;  /* per-block L2 norm of quantization residual */

    /* v4 — Novel optimization vectors */
    uint8_t    *chaos_r_idx;       /* per-block chaotic codebook parameter index */
    fpq_ghost_t *ghost;            /* rank-1 error correction (NULL if disabled) */
    int          sbb_group_id;     /* SBB group index (-1 = not grouped) */
    float       *sbb_scale_delta;  /* legacy auxiliary payload; v6+ layouts repurpose it */

    /* v5 — Probabilistic Inference Decompression */
    float        pid_alpha;        /* causal prediction coefficient [0..1] */

    /* Compression stats */
    size_t     total_seed_nodes;  /* sum of all seed tree sizes */
    size_t     total_bits;        /* total encoded size in bits */
    float      avg_distortion;    /* average MSE across blocks */
} fpq_tensor_t;

/*
 * Compress a weight tensor using FPQ.
 *   weights: row-major float array [rows × cols]
 *   max_seed_nodes: budget per block for seed complexity
 *   tolerance: max distortion per block
 */
fpq_tensor_t *fpq_encode_tensor(const float *weights, size_t rows, size_t cols,
                                 const char *name, size_t max_seed_nodes,
                                 float tolerance);

/*
 * Decompress a tensor back to float weights.
 *   output: pre-allocated [rows × cols] array
 */
void fpq_decode_tensor(const fpq_tensor_t *tensor, float *output);

/*
 * Compute a dot product between two FPQ-compressed vectors
 * WITHOUT fully decompressing. Uses seed expansion + QJL correction.
 */
float fpq_dot_product(const fpq_tensor_t *a, size_t row_a,
                      const fpq_tensor_t *b, size_t row_b);

void fpq_tensor_free(fpq_tensor_t *tensor);


/* ── Serialization (the "model file" is just seeds) ── */

/*
 * FPQ! v4 flag 1 stores exact seed dictionaries, block distortion and QJL.
 * Read compatibility: v1-v3 and interim v4 flag 0 (per-block seed bodies).
 * Coordinate/auxiliary representations use FPQ0 and are refused here.
 * Save publishes atomically after checking writes; failure preserves the
 * previous destination. Load refuses partial/extra data and clears n_tensors
 * on failure. Trees are limited to depth 128 / 1,048,576 nodes; cumulative
 * allocations during one load are limited to 1 GiB, including block clones.
 */

int fpq_save(const char *path, fpq_tensor_t **tensors, size_t n_tensors);
fpq_tensor_t **fpq_load(const char *path, size_t *n_tensors);


/* ── GGML Model Reader ── */

/*
 * Read a GGML-format model file (whisper, gemma, etc.) and return
 * the weight tensors as float arrays.
 */
typedef struct {
    char    name[128];
    uint32_t n_dims;
    size_t  rows;
    size_t  cols;
    uint32_t ggml_type;
    size_t  n_elements;
    float  *data;     /* row-major float32 */
} fpq_raw_tensor_t;

fpq_raw_tensor_t *fpq_ggml_read(const char *path, size_t *n_tensors);
fpq_raw_tensor_t *fpq_safetensors_read(const char *path, size_t *n_tensors);
void fpq_raw_tensor_free(fpq_raw_tensor_t *tensors, size_t n);

/* Write tensors as BF16 safetensors file (fp32 data converted to bf16) */
int fpq_safetensors_write(const char *path, const fpq_raw_tensor_t *tensors,
                          size_t n_tensors);

/* Write GGUF F16 file with FPQ v9 compression (llama.cpp compatible) */
int fpq_gguf_write_v9(const char *input_path, const char *output_path,
                       int coord_bits);


/* ═══════════════════════════════════════════════════════════════════
 * Native .fpq format — compact storage (~1.6 bpw = 20× from fp32)
 *
 * Stores v9 multiscale data in compact binary format:
 *   - LR factors: INT8/INT6/INT4 quantized U*S and Vt
 *   - Residual: E8 lattice indices + RVQ tile indices
 *   - Ghost vectors: INT8 compressed u, v, sigma
 *   - Metadata: fp16 scales + INT8 norms
 *
 * Reader reconstructs to FP32 on-the-fly for inference.
 * ═══════════════════════════════════════════════════════════════════ */
#define FPQ_NATIVE_MAGIC    0x46505130  /* "FPQ0" */
#define FPQ_NATIVE_VERSION  10

/* Header-only inspection is deliberately distinct from reconstruction or
 * generation: it verifies that an FPQ0 artifact is structurally readable
 * without claiming execution parity or allocating its full tensor estate. */
typedef struct {
    uint32_t version;
    uint32_t tensor_count;
    uint64_t weight_count;
    uint64_t stored_bytes;
} fpq_native_inspection_t;

/*
 * Write native .fpq format — compact binary storage per tensor.
 * Input: already-processed fp32 data (post algebra-compress).
 * Each tensor is stored as compact v9 multiscale encoding.
 */
int fpq_native_write(const char *path, const fpq_raw_tensor_t *tensors,
                     size_t n_tensors);

/*
 * Read native .fpq format back into fp32 tensors.
 * Returns allocated tensor array (caller frees with fpq_raw_tensor_free).
 */
fpq_raw_tensor_t *fpq_native_read(const char *path, size_t *n_tensors);

/* Read and validate the FPQ0 header/table only. Returns 0 on success. */
int fpq_native_inspect(const char *path, fpq_native_inspection_t *out);

/*
 * Write native .fpq format from v9-encoded tensors (pre-compressed).
 * Stores the compact representation directly without re-encoding.
 */
int fpq_native_write_v9(const char *path, fpq_tensor_t **tensors,
                        size_t n_tensors, int coord_bits);


/* ── Utility ── */

float fpq_mse(const float *a, const float *b, size_t n);
float fpq_cosine_sim(const float *a, const float *b, size_t n);
float fpq_bits_per_weight(const fpq_tensor_t *tensor);

/* Print compression report */
void fpq_report(const fpq_tensor_t *tensor);


/* ── De Bruijn Universal Constant Codebook ── */

/*
 * Hard-coded constants derived from π, e, and De Bruijn indices.
 * NEVER stored in the file — the codebook IS the Lambda term.
 * DBREF(i) → FPQ_CODEBOOK[i]. Zero metadata.
 *
 * This satisfies Proof Requirement #1 (Zero-Metadata Overhead):
 * The "codebook" has zero bits — it's a fixed-point combinator
 * that generates the quantization grid from universal constants.
 */
extern const float FPQ_CODEBOOK[FPQ_CODEBOOK_SIZE];

/*
 * Get De Bruijn indexed constant. The index IS the encoding.
 */
float fpq_dbref(int index);


/* ── Type-Inferred Radius (System F Type Inference) ── */

/*
 * After FWHT, block coordinates ~ N(0, σ²/N).
 * The radius (L2 norm) concentrates: r ≈ σ * √(1 - 1/(2N)).
 * σ is a property of the TENSOR TYPE (layer position), not per-block data.
 *
 * This eliminates per-block radius storage entirely:
 *   TurboQuant: 0.1–0.2 bits/weight overhead for scale/zero
 *   FPQ v2:     0 bits/weight — radius derived from type
 *
 * Proof Requirement #4: bits saved = n_blocks * 32 ≈ 0.125 bpw
 */
typedef struct {
    float sigma;              /* estimated standard deviation of tensor weights */
    float expected_radius;    /* σ * √(block_dim) — the "type" of this tensor */
    float radius_variance;    /* measured variance of block radii around expected */
} fpq_type_info_t;

/*
 * Infer the type (expected radius) from a weight tensor's statistics.
 * This is the System F type inference — the "type" determines the radius.
 */
fpq_type_info_t fpq_infer_type(const float *weights, size_t n_weights, size_t block_dim);

/*
 * Compute expected polar angles for a block of given dimension.
 * After FWHT, angles[0..n-3] concentrate near π/2 (Beta distribution mode).
 * angles[n-2] has mean π (uniform on [0, 2π]).
 *
 * Returning these for FREE (no storage) is the core of the zero-metadata proof.
 * We only need to compress DEVIATIONS from these expected values.
 */
void fpq_expected_angles(size_t block_dim, float *expected);

/* Reset the memoization cache (call before each tensor encode) */
void fpq_memo_reset(void);


/* ═══════════════════════════════════════════════════════════════════
 * v4 OPTIMIZATIONS: Three novel vectors beyond the 0.996 cosine wall
 * ═══════════════════════════════════════════════════════════════════ */

/* ── Optimization A: Shared Base-Basis (SBB) ──
 *
 * Cross-tensor shared basis for Q/K/V/O weights in same transformer layer.
 * Compute a shared RMS scale profile across the attention group.
 * v4 reuses that basis while encoding compatible tensors. Its persisted tensor
 * body still carries the reconstructed full per-block scale, so v4 does not
 * claim stored-byte savings from SBB.
 *
 * Insight: Q/K/V weights in the same layer operate on the same subspace.
 * Their FWHT-domain RMS profiles are highly correlated (>0.95 typically).
 */

/* Layer group for SBB: up to 6 tensors (Q, K, V, O, ffn_gate, ffn_up) */
#define FPQ_SBB_MAX_GROUP  6

typedef struct {
    size_t   n_tensors;                       /* how many tensors in this group */
    size_t   n_blocks;                        /* block count (shared across group) */
    float   *shared_scales;                   /* [n_blocks] shared RMS profile */
    float  **scale_deltas;                    /* [n_tensors][n_blocks] per-tensor delta */
} fpq_sbb_t;

/*
 * Compute SBB shared profile from a group of tensors.
 * All tensors must have the same total element count (or be padded).
 * Returns allocated SBB structure.
 */
fpq_sbb_t *fpq_sbb_compute(const float **weights_group, size_t n_tensors,
                            size_t n_elements, uint64_t haar_seed);
void fpq_sbb_free(fpq_sbb_t *sbb);


/* ── Optimization B: Chaotic Fractal Codebook ──
 *
 * Replace static Lloyd-Max centroids with dynamically generated centroids
 * from a logistic map: x_{n+1} = r * x_n * (1 - x_n).
 *
 * For each block: a 1-byte parameter r_idx selects the chaotic attractor
 * that best fits the block's actual FWHT-domain coordinate distribution.
 * The centroids are generated at encode AND decode from the r parameter alone.
 *
 * "16-bit Lloyd-Max for the price of 1-bit flag."
 * Cost: 1 byte per block (0.03 bpw overhead).
 * Benefit: adaptive centroids match heavy-tailed / skewed distributions.
 */
#define FPQ_CHAOS_R_STEPS  64   /* 64 candidate r values to search */

/*
 * Generate n centroids from a logistic map with parameter r.
 * r should be in [3.57, 4.0] (chaotic regime).
 * Centroids are sorted and symmetrized for quantization.
 */
void fpq_chaos_generate_centroids(float r, int n_levels, float *centroids,
                                   float *boundaries);

/*
 * Find the optimal r parameter for a block of normalized FWHT coordinates.
 * Returns r_idx (0..FPQ_CHAOS_R_STEPS-1) and fills centroids/boundaries.
 */
uint8_t fpq_chaos_find_best_r(const float *normalized_coords, size_t n,
                               int n_levels, float *best_centroids,
                               float *best_boundaries);


/* ── Optimization C: Error-Correcting Ghost Head ──
 *
 * Per-tensor rank-1 correction for structured quantization error. This is
 * exception state attached to one tensor, not a cross-tensor shared basis.
 *
 * After COORD quantization, the residual error has systematic structure
 * (not i.i.d. noise). A rank-1 SVD approximation u*v^T captures
 * the dominant error mode. This correction is applied at decode time.
 *
 * Cost depends on this tensor's rows + cols.
 * Benefit: corrects the dominant error mode that QJL misses.
 */

typedef struct fpq_ghost {
    float *u;           /* left singular vector [rows] */
    float *v;           /* right singular vector [cols] */
    float  sigma;       /* singular value (scale) */
    size_t rows;
    size_t cols;
} fpq_ghost_t;

/*
 * Compute rank-1 ghost correction from quantization error matrix.
 * error_matrix = original_weights - decoded_weights (row-major [rows×cols])
 */
fpq_ghost_t *fpq_ghost_compute(const float *error_matrix, size_t rows, size_t cols);

/*
 * Apply ghost correction: output += sigma * u * v^T
 */
void fpq_ghost_apply(const fpq_ghost_t *ghost, float *output);
void fpq_ghost_free(fpq_ghost_t *ghost);


/* Extended tensor struct fields for v4 (stored in fpq_tensor_t) */
/* ghost correction vectors */
/* sbb shared scale index (-1 = not using SBB) */
/* chaos r_idx per block */

/*
 * v4 encode: COORD + Chaotic Codebook + QJL + Ghost Head
 * Optionally uses SBB shared scales from a pre-computed group.
 * sbb_tensor_idx: index of this tensor within the SBB group (-1 to disable).
 */
fpq_tensor_t *fpq_encode_tensor_v4(const float *weights, size_t rows, size_t cols,
                                    const char *name, int coord_bits,
                                    const fpq_sbb_t *sbb, int sbb_tensor_idx);

/*
 * v4 decode: Chaotic dequantize + QJL + Ghost correction
 */
void fpq_decode_tensor_v4(const fpq_tensor_t *tensor, float *output);


/* ═══════════════════════════════════════════════════════════════════
 * v5 — PROBABILISTIC INFERENCE DECOMPRESSION (PID)
 *
 * The decoder doesn't just reconstruct — it PREDICTS.
 *
 * Key insight: adjacent blocks in the same tensor share FWHT-domain
 * coordinate correlations. Block[b]'s coordinate[i] is correlated
 * with Block[b-1]'s coordinate[i] because nearby weight vectors
 * encode related features.
 *
 * PID exploits this via causal DPCM:
 *   ENCODE: predict[b] = α * decoded[b-1], residual = fwht[b] - predict[b]
 *           → quantize the RESIDUAL (lower variance → same bits, less error)
 *   DECODE: predict[b] = α * decoded[b-1], output = predict + dequant(residual)
 *
 * The prediction coefficient α is estimated per-tensor from the lag-1
 * FWHT-domain correlation. When ρ > 0, Var(residual) = Var(orig)·(1-ρ²).
 * For ρ=0.3, that's a 9% variance reduction — free cosine gain.
 *
 * Combined with v4 (CHAOS + GHOST): v5 = v4 + causal prediction.
 * ═══════════════════════════════════════════════════════════════════ */

/*
 * v5 encode: v4 + Causal Block Prediction (DPCM)
 * Returns tensor with pid_alpha stored for decode.
 */
fpq_tensor_t *fpq_encode_tensor_v5(const float *weights, size_t rows, size_t cols,
                                    const char *name, int coord_bits,
                                    const fpq_sbb_t *sbb, int sbb_tensor_idx);

/*
 * v5 decode: v4 decode + causal prediction reconstruction
 */
void fpq_decode_tensor_v5(const fpq_tensor_t *tensor, float *output);


/* ═══════════════════════════════════════════════════════════════════
 * v5+ — LIE ALGEBRA CORRELATION PROBE
 *
 * Measures inter-block correlation in multiple domains:
 *   raw weights, Lie algebra (logm), eigenvalue spectrum.
 * Returns Lie algebra Pearson r.
 * ═══════════════════════════════════════════════════════════════════ */
float fpq_lie_probe(const float *weights, size_t total, const char *name);


/* ═══════════════════════════════════════════════════════════════════
 * v6 — MANIFOLD-AGNOSTIC SPECTRAL QUANTIZATION (MASQ)
 *
 * Geodesic quantization on the Stiefel manifold:
 *   - Spectral DPCM: predict eigenvalues (r=0.97 correlation)
 *   - Lie-Delta Transport: skew-symmetric Ω (120 upper-tri values)
 *   - Spectral Governor: Ledoit-Wolf shrinkage toward global mean
 *   - QJL residual + Ghost Head
 *
 * Treats blocks as elements of GL(n,R), decomposes into
 * Sym + Skew, then encodes the manifold residual instead of
 * raw Euclidean weights.
 * ═══════════════════════════════════════════════════════════════════ */

/*
 * v6 encode: MASQ (Lie-Delta + Spectral Governor + QJL + Ghost)
 */
fpq_tensor_t *fpq_encode_tensor_v6(const float *weights, size_t rows, size_t cols,
                                     const char *name, int coord_bits);

/*
 * v6 decode: Spectral DPCM + Lie-Delta reconstruct + Governor + Ghost
 */
void fpq_decode_tensor_v6(const fpq_tensor_t *tensor, float *output);


/* ═══════════════════════════════════════════════════════════════════
 * v7 — HOLOGRAPHIC LATTICE QUANTIZATION
 *
 * Uses computational geometry to make lattice quantization fast:
 *   - E8 Lattice Snapping: O(1) fast quantizer (Conway-Sloane)
 *   - Log-Polar Warp: μ-law companding aligns lattice to Fisher info
 *   - RVQ Correction Tiles: shared 8D correction patterns (4-bit index)
 *   - Chaos-Seeded Trellis: deterministic 16-state refinement
 *   - QJL + Ghost Head: residual correction
 *
 * Target: cos ≥ 0.999 at bpw ≈ 3.0–3.5
 * ═══════════════════════════════════════════════════════════════════ */

/*
 * v7 encode: E8 Lattice + Log-Polar Warp + RVQ Tiles + Trellis + QJL + Ghost
 */
fpq_tensor_t *fpq_encode_tensor_v7(const float *weights, size_t rows, size_t cols,
                                     const char *name, int coord_bits);

/*
 * v7 decode: Reverse lattice snap + inverse warp + tile correction + Ghost
 */
void fpq_decode_tensor_v7(const fpq_tensor_t *tensor, float *output);


/* ═══════════════════════════════════════════════════════════════════
 * v8 — RECURSIVE LATTICE-FLOW (RLF) QUANTIZATION
 *
 * The mathematical terminus: E8 Geometry × Trellis Dynamics × RVQ.
 *   - Trellis-Coded Lattice Quantization (TCLQ): 8-state Viterbi
 *     selects between E8 cosets (D₈ vs D₈+½), 0 bpw overhead
 *   - 256-Tile 16D RVQ: paired E8 groups capture inter-group
 *     spectral correlation (0.5 bpw, same as v7)
 *   - Spectral Smoothness Regularizer in Viterbi cost
 *   - QJL + Ghost Head
 *
 * Target: cos ≥ 0.999 at bpw ≈ 3.0–3.5
 * ═══════════════════════════════════════════════════════════════════ */

/*
 * v8 encode: Viterbi TCLQ + 16D RVQ + QJL + Ghost
 */
fpq_tensor_t *fpq_encode_tensor_v8(const float *weights, size_t rows, size_t cols,
                                     const char *name, int coord_bits);

/*
 * v8 decode: E8 points + 16D tile correction + inverse warp + Ghost
 */
void fpq_decode_tensor_v8(const fpq_tensor_t *tensor, float *output);

/*
 * v9 encode: Unified Multiscale — Low-rank SVD + v8 RLF on residual + QJL + Ghost
 */
fpq_tensor_t *fpq_encode_tensor_v9(const float *weights, size_t rows, size_t cols,
                                     const char *name, int coord_bits);

/*
 * v9 decode: Low-rank reconstruction + v8 residual decode + Ghost
 */
void fpq_decode_tensor_v9(const fpq_tensor_t *tensor, float *output);

/*
 * v9 deep analysis: rank sweep, factor quant sensitivity, entropy measurement
 */
void fpq_v9_analyze(const float *weights, size_t rows, size_t cols,
                    const char *name, int coord_bits);

#endif /* BONFYRE_FPQ_H */
