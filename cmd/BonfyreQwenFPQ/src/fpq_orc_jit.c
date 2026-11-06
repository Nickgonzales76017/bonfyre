/* In-process specialisation of the SLI kernel through LLVM ORC.
 *
 * What is cached is keyed by what makes the machine code different: the
 * shape. The host's CPU and features are fixed for the life of the process
 * and the kernel source for the life of the build, so within a process the
 * shape is the whole key. */
#include "fpq_orc_jit.h"
#include "sli_jit_kernel_ir.h"

#include <llvm-c/Core.h>
#include <llvm-c/Error.h>
#include <llvm-c/IRReader.h>
#include <llvm-c/LLJIT.h>
#include <llvm-c/Orc.h>
#include <llvm-c/Target.h>
#include <llvm-c/TargetMachine.h>
#include <llvm-c/Transforms/PassBuilder.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct { size_t rows, bpr; fpq_sli_kernel_fn fn; } Compiled;

static pthread_mutex_t g_lock = PTHREAD_MUTEX_INITIALIZER;
static LLVMOrcLLJITRef g_jit;
static LLVMTargetMachineRef g_machine;
static Compiled *g_compiled;
static size_t g_count, g_cap, g_reused;
static int g_failed;

static double now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1000.0 + (double)ts.tv_nsec / 1e6;
}

static int quiet(LLVMErrorRef error) {
    if (!error) return 0;
    char *message = LLVMGetErrorMessage(error);
    if (getenv("BONFYRE_QWEN_DEBUG")) fprintf(stderr, "fpq_orc: %s\n", message);
    LLVMDisposeErrorMessage(message);
    return 1;
}

static int bring_up(void) {
    char *triple, *cpu, *features, *message = NULL;
    LLVMTargetRef target;
    if (g_jit) return 0;
    if (g_failed) return 1;
    LLVMInitializeNativeTarget();
    LLVMInitializeNativeAsmPrinter();
    if (quiet(LLVMOrcCreateLLJIT(&g_jit, NULL))) { g_jit = NULL; g_failed = 1; return 1; }
    /* The machine the kernel is optimised for is this one, with what it has. */
    triple = LLVMGetDefaultTargetTriple();
    cpu = LLVMGetHostCPUName();
    features = LLVMGetHostCPUFeatures();
    if (LLVMGetTargetFromTriple(triple, &target, &message)) { LLVMDisposeMessage(message); g_failed = 1; }
    else g_machine = LLVMCreateTargetMachine(target, triple, cpu, features, LLVMCodeGenLevelAggressive, LLVMRelocPIC, LLVMCodeModelJITDefault);
    LLVMDisposeMessage(triple); LLVMDisposeMessage(cpu); LLVMDisposeMessage(features);
    if (!g_machine) g_failed = 1;
    return g_failed;
}

/* The template with both sentinels replaced and the function renamed, so
 * several shapes live in one JIT without colliding. */
static char *specialise(size_t rows, size_t bpr, const char *symbol) {
    static const char *const from[] = { "1000000007", "1000000009", "sli_jit_kernel" };
    char to[3][64];
    size_t cap = sizeof(FPQ_SLI_KERNEL_IR) + 4096, used = 0;
    char *out = malloc(cap);
    const char *at = FPQ_SLI_KERNEL_IR;
    if (!out) return NULL;
    snprintf(to[0], sizeof(to[0]), "%zu", rows);
    snprintf(to[1], sizeof(to[1]), "%zu", bpr);
    snprintf(to[2], sizeof(to[2]), "%s", symbol);
    while (*at) {
        int hit = -1;
        for (int k = 0; k < 3; k++) if (!strncmp(at, from[k], strlen(from[k]))) { hit = k; break; }
        if (used + 80 >= cap) { char *bigger = realloc(out, cap *= 2); if (!bigger) { free(out); return NULL; } out = bigger; }
        if (hit >= 0) { used += (size_t)sprintf(out + used, "%s", to[hit]); at += strlen(from[hit]); }
        else out[used++] = *at++;
    }
    out[used] = '\0';
    return out;
}

fpq_sli_kernel_fn fpq_orc_sli_kernel(size_t rows, size_t bpr, double *compile_ms) {
    fpq_sli_kernel_fn fn = NULL;
    double started = now_ms();
    if (compile_ms) *compile_ms = 0.0;
    if (!rows || !bpr) return NULL;
    pthread_mutex_lock(&g_lock);
    for (size_t i = 0; i < g_count; i++)
        if (g_compiled[i].rows == rows && g_compiled[i].bpr == bpr) { g_reused++; fn = g_compiled[i].fn; goto done; }
    if (bring_up()) goto done;
    {
        char symbol[64], *ir, *message = NULL;
        /* The context is made here and handed to the JIT, which then owns it. */
        LLVMContextRef ctx = LLVMContextCreate();
        LLVMOrcThreadSafeContextRef tsctx = LLVMOrcCreateNewThreadSafeContextFromLLVMContext(ctx);
        LLVMMemoryBufferRef buffer;
        LLVMModuleRef module = NULL;
        LLVMPassBuilderOptionsRef options;
        LLVMOrcThreadSafeModuleRef tsm;
        LLVMOrcExecutorAddress address = 0;
        snprintf(symbol, sizeof(symbol), "sli_kernel_r%zu_b%zu", rows, bpr);
        if (!(ir = specialise(rows, bpr, symbol))) { LLVMOrcDisposeThreadSafeContext(tsctx); goto done; }
        buffer = LLVMCreateMemoryBufferWithMemoryRangeCopy(ir, strlen(ir), "sli_kernel");
        free(ir);
        if (LLVMParseIRInContext(ctx, buffer, &module, &message)) {
            if (getenv("BONFYRE_QWEN_DEBUG")) fprintf(stderr, "fpq_orc: %s\n", message ? message : "IR did not parse");
            LLVMDisposeMessage(message); LLVMOrcDisposeThreadSafeContext(tsctx); goto done;
        }
        {
            char *layout = LLVMCopyStringRepOfTargetData(LLVMCreateTargetDataLayout(g_machine));
            char *triple = LLVMGetTargetMachineTriple(g_machine);
            LLVMSetDataLayout(module, layout); LLVMSetTarget(module, triple);
            LLVMDisposeMessage(layout); LLVMDisposeMessage(triple);
        }
        /* The shape is now a pair of constants; the ordinary O3 pipeline does the rest. */
        options = LLVMCreatePassBuilderOptions();
        if (quiet(LLVMRunPasses(module, "default<O3>", g_machine, options))) {
            LLVMDisposePassBuilderOptions(options); LLVMDisposeModule(module); LLVMOrcDisposeThreadSafeContext(tsctx); goto done;
        }
        LLVMDisposePassBuilderOptions(options);
        tsm = LLVMOrcCreateNewThreadSafeModule(module, tsctx);
        LLVMOrcDisposeThreadSafeContext(tsctx);
        if (quiet(LLVMOrcLLJITAddLLVMIRModule(g_jit, LLVMOrcLLJITGetMainJITDylib(g_jit), tsm))) { LLVMOrcDisposeThreadSafeModule(tsm); goto done; }
        if (quiet(LLVMOrcLLJITLookup(g_jit, &address, symbol)) || !address) goto done;
        fn = (fpq_sli_kernel_fn)(uintptr_t)address;
        if (g_count == g_cap) {
            size_t grow = g_cap ? g_cap * 2 : 16;
            Compiled *bigger = realloc(g_compiled, grow * sizeof(*bigger));
            if (!bigger) goto done;       /* still usable, just not remembered */
            g_compiled = bigger; g_cap = grow;
        }
        g_compiled[g_count++] = (Compiled){ rows, bpr, fn };
        if (compile_ms) *compile_ms = now_ms() - started;
    }
done:
    pthread_mutex_unlock(&g_lock);
    return fn;
}

void fpq_orc_stats(size_t *compiled, size_t *reused) {
    pthread_mutex_lock(&g_lock);
    if (compiled) *compiled = g_count;
    if (reused) *reused = g_reused;
    pthread_mutex_unlock(&g_lock);
}
