/*
 * libfpq.h — BonfyreFPQ Public C Library API
 *
 * Five functions. That's the entire interface.
 *
 *   fpq_open()          — Load .fpq model, build SLI contexts
 *   fpq_matmul()        — y = W[name] @ x (no decode, 8× bandwidth reduction)
 *   fpq_close()         — Free everything
 *   fpq_decode_tensor() — Decode one tensor to FP32 (escape hatch)
 *   fpq_decode_all()    — Decode entire model to safetensors
 *
 * Usage:
 *   fpq_model_t *m = fpq_open("model.fpq");
 *   fpq_matmul(m, "model.layers.0.mlp.gate_proj.weight", x, y);
 *   fpq_close(m);
 */

#ifndef BONFYRE_LIBFPQ_H
#define BONFYRE_LIBFPQ_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque model handle */
typedef struct fpq_model fpq_model_t;

/* Model metadata */
typedef struct {
    size_t   n_tensors;          /* total tensor count */
    size_t   n_sli_tensors;      /* tensors with SLI contexts (weight matrices) */
    size_t   n_passthrough;      /* small/1D tensors stored as raw fp32 */
    size_t   total_params;       /* sum of all tensor element counts */
    uint32_t format_version;     /* .fpq file version (e.g. 12) */
} fpq_info_t;

/* Per-tensor metadata */
typedef struct {
    const char *name;
    size_t      rows;
    size_t      cols;
    int         has_sli;         /* 1 = SLI-ready weight matrix, 0 = passthrough */
    float       bpw;             /* effective bits per weight in .fpq */
} fpq_tensor_info_t;

/*
 * Load a .fpq model file.
 *
 * Reads the file, builds SLI contexts for all weight matrices.
 * After this call, fpq_matmul() is ready — no further setup needed.
 *
 * Returns NULL on error (prints to stderr).
 */
fpq_model_t *fpq_open(const char *path);

/*
 * Compute y = W @ x via Spectral Lattice Inference.
 *
 * tensor_name: exact name as stored in the .fpq file
 *              (e.g. "model.layers.0.self_attn.q_proj.weight")
 * x:      input activation vector [cols]  (caller-owned)
 * y:      output vector [rows]  (caller-allocated)
 *
 * Returns 0 on success, -1 if tensor not found or not SLI-capable.
 *
 * Thread safety: safe to call from multiple threads with different
 * tensor names. NOT safe to call with the same tensor from multiple
 * threads (SLI contexts have per-tensor scratch buffers).
 */
int fpq_matmul(fpq_model_t *m, const char *tensor_name,
               const float *x, float *y);

/*
 * Close model and free all resources.
 */
void fpq_close(fpq_model_t *m);

/*
 * Decode a single tensor to FP32.
 *
 * out: pre-allocated [rows × cols] float array
 * Returns 0 on success, -1 if tensor not found.
 *
 * For passthrough tensors, copies the stored fp32 data directly.
 * For SLI tensors, runs the full v9 decode path.
 */
int fpq_decode_one(fpq_model_t *m, const char *tensor_name, float *out);

/*
 * Decode entire model to safetensors file.
 *
 * out_path: path used for safetensors output
 * Returns 0 on success.
 */
int fpq_decode_all(fpq_model_t *m, const char *out_path);

/*
 * Query model info.
 */
fpq_info_t fpq_info(fpq_model_t *m);

/*
 * Get per-tensor info.
 * Returns NULL if index out of range.
 */
const fpq_tensor_info_t *fpq_tensor_at(fpq_model_t *m, size_t index);

/*
 * Look up tensor by name.
 * Returns NULL if not found.
 */
const fpq_tensor_info_t *fpq_tensor_find(fpq_model_t *m, const char *name);

/*
 * Get raw float pointer for passthrough (small/1D) tensors.
 * Returns NULL if tensor is SLI-capable (use fpq_matmul instead)
 * or not found.
 *
 * The returned pointer is owned by the model — do not free.
 * Valid until fpq_close().
 */
const float *fpq_get_passthrough(fpq_model_t *m, const char *tensor_name);

#ifdef __cplusplus
}
#endif

#endif /* BONFYRE_LIBFPQ_H */
