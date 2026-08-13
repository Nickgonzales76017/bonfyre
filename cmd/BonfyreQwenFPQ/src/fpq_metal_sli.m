#import <Foundation/Foundation.h>
#import <Metal/Metal.h>

#include "fpqx.h"
#include <pthread.h>
#include <string.h>

/* This is deliberately a small FPQ-native kernel, not a GGUF bridge.
 * One 256-lane threadgroup owns one output row.  Each lane applies the same
 * XorShift sign as the CPU SLI path to its input coordinate and reduces the
 * dot products over the row's 256-wide transformed residual blocks. */
static NSString *const kFPQSLIMetalSource = @
"#include <metal_stdlib>\n"
"using namespace metal;\n"
"struct Params { uint rows; uint cols; uint bpr; ulong seed; };\n"
"inline ulong xs(ulong s) { s ^= s << 13; s ^= s >> 7; s ^= s << 17; return s; }\n"
"kernel void fpq_sli_rows(device const float *z [[buffer(0)]],\n"
"                         device const float *x [[buffer(1)]],\n"
"                         device float *out [[buffer(2)]],\n"
"                         constant Params &p [[buffer(3)]],\n"
"                         uint tid [[thread_index_in_threadgroup]],\n"
"                         uint row [[threadgroup_position_in_grid]]) {\n"
"  if (row >= p.rows) return;\n"
"  float sum = 0.0f;\n"
"  for (uint bj = 0; bj < p.bpr; ++bj) {\n"
"    uint col = bj * 256u + tid;\n"
"    if (col < p.cols) {\n"
"      ulong s = p.seed ^ (ulong)(row * p.bpr + bj);\n"
"      uint steps = tid / 64u + 1u;\n"
"      for (uint step = 0; step < steps; ++step) s = xs(s);\n"
"      float xv = ((s >> (tid & 63u)) & 1ul) ? -x[col] : x[col];\n"
"      sum += z[((row * p.bpr + bj) * 256u) + tid] * xv;\n"
"    }\n"
"  }\n"
"  threadgroup float partial[256];\n"
"  partial[tid] = sum;\n"
"  threadgroup_barrier(mem_flags::mem_threadgroup);\n"
"  for (uint stride = 128u; stride > 0u; stride >>= 1u) {\n"
"    if (tid < stride) partial[tid] += partial[tid + stride];\n"
"    threadgroup_barrier(mem_flags::mem_threadgroup);\n"
"  }\n"
"  if (tid == 0u) out[row] += partial[0];\n"
"}\n";

typedef struct {
    id<MTLBuffer> z;
    id<MTLBuffer> x;
    id<MTLBuffer> output;
} fpq_metal_sli_state_t;

typedef struct {
    uint32_t rows;
    uint32_t cols;
    uint32_t bpr;
    uint64_t seed;
} fpq_metal_sli_params_t;

static id<MTLDevice> g_device;
static id<MTLCommandQueue> g_queue;
static id<MTLComputePipelineState> g_pipeline;
static pthread_once_t g_once = PTHREAD_ONCE_INIT;

static int fpq_metal_enabled(void) {
    const char *value = getenv("BONFYRE_QWEN_METAL_SLI");
    /* The first FPQ-native Metal kernel is exact but intentionally remains an
     * explicit lane until its tiled vocabulary projection supersedes the CPU
     * BLAS path.  Never let an experimental backend silently regress a live
     * generation service. */
    return value && *value && strcmp(value, "0") != 0;
}

static void fpq_metal_init(void) {
    @autoreleasepool {
        g_device = MTLCreateSystemDefaultDevice();
        if (!g_device) return;
        NSError *error = nil;
        id<MTLLibrary> library = [g_device newLibraryWithSource:kFPQSLIMetalSource
                                                         options:nil
                                                           error:&error];
        if (!library) return;
        id<MTLFunction> function = [library newFunctionWithName:@"fpq_sli_rows"];
        if (!function) return;
        g_pipeline = [g_device newComputePipelineStateWithFunction:function error:&error];
        if (!g_pipeline) return;
        g_queue = [g_device newCommandQueue];
    }
}

static fpq_metal_sli_state_t *fpq_metal_state_create(const fpqx_sli_ctx_t *ctx) {
    if (!ctx || !ctx->z_data || !ctx->n_total_blocks ||
        ctx->n_total_blocks > SIZE_MAX / (256u * sizeof(float))) return NULL;
    pthread_once(&g_once, fpq_metal_init);
    if (!g_device || !g_pipeline || !g_queue) return NULL;

    @autoreleasepool {
        fpq_metal_sli_state_t *state = calloc(1, sizeof(*state));
        if (!state) return NULL;
        NSUInteger z_bytes = (NSUInteger)(ctx->n_total_blocks * 256u * sizeof(float));
        state->z = [g_device newBufferWithBytes:ctx->z_data
                                          length:z_bytes
                                         options:MTLResourceStorageModeShared];
        state->x = [g_device newBufferWithLength:(NSUInteger)(ctx->cols * sizeof(float))
                                         options:MTLResourceStorageModeShared];
        state->output = [g_device newBufferWithLength:(NSUInteger)(ctx->rows * sizeof(float))
                                              options:MTLResourceStorageModeShared];
        if (!state->z || !state->x || !state->output) {
            state->z = nil; state->x = nil; state->output = nil;
            free(state);
            return NULL;
        }
        return state;
    }
}

int fpqx_metal_sli_try_matvec(fpqx_sli_ctx_t *ctx,
                              const float *x,
                              float *output,
                              uint64_t haar_seed) {
    if (!fpq_metal_enabled() || !ctx || !x || !output || !ctx->z_data ||
        !ctx->z_precomputed || ctx->z_data_f16 || ctx->cols == 0 || ctx->rows == 0 ||
        ctx->blocks_per_row == 0 || ctx->rows > UINT32_MAX || ctx->cols > UINT32_MAX ||
        ctx->blocks_per_row > UINT32_MAX) return -1;

    fpq_metal_sli_state_t *state = (fpq_metal_sli_state_t *)ctx->metal_sli;
    if (!state) {
        state = fpq_metal_state_create(ctx);
        if (!state) return -1;
        ctx->metal_sli = state;
    }
    @autoreleasepool {
        memcpy([state->x contents], x, ctx->cols * sizeof(float));
        memcpy([state->output contents], output, ctx->rows * sizeof(float));
        fpq_metal_sli_params_t params = {
            .rows = (uint32_t)ctx->rows,
            .cols = (uint32_t)ctx->cols,
            .bpr = (uint32_t)ctx->blocks_per_row,
            .seed = haar_seed,
        };
        id<MTLCommandBuffer> command = [g_queue commandBuffer];
        id<MTLComputeCommandEncoder> encoder = [command computeCommandEncoder];
        [encoder setComputePipelineState:g_pipeline];
        [encoder setBuffer:state->z offset:0 atIndex:0];
        [encoder setBuffer:state->x offset:0 atIndex:1];
        [encoder setBuffer:state->output offset:0 atIndex:2];
        [encoder setBytes:&params length:sizeof(params) atIndex:3];
        MTLSize threads = MTLSizeMake(256, 1, 1);
        MTLSize groups = MTLSizeMake(ctx->rows, 1, 1);
        [encoder dispatchThreadgroups:groups threadsPerThreadgroup:threads];
        [encoder endEncoding];
        [command commit];
        [command waitUntilCompleted];
        if (command.status != MTLCommandBufferStatusCompleted) return -1;
        memcpy(output, [state->output contents], ctx->rows * sizeof(float));
    }
    return 0;
}

void fpqx_metal_sli_free(fpqx_sli_ctx_t *ctx) {
    if (!ctx || !ctx->metal_sli) return;
    fpq_metal_sli_state_t *state = (fpq_metal_sli_state_t *)ctx->metal_sli;
    state->z = nil; state->x = nil; state->output = nil;
    free(state);
    ctx->metal_sli = NULL;
}
