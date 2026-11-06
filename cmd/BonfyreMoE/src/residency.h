#ifndef BONFYRE_MOE_RESIDENCY_H
#define BONFYRE_MOE_RESIDENCY_H

#include <stdint.h>

/* On-demand residency for a GGUF served by llama-server.
 *
 * The listening port is owned by this process, not by the model. The model is
 * loaded on the first request, only after a memory grant is admitted, and is
 * unloaded (grant released) after idle_seconds with no open connection. A
 * deferred load answers 503 with the reason instead of pushing the machine into
 * swap. GET /health and GET /bonfyre/residency answer without loading; the
 * relay is per connection, so those two are read on a connection's first
 * request only.
 *
 * Which model, and how much context, is decided per load from the GGUF's own
 * shape and the memory free at that moment: the strongest model that fits
 * loads with the most context that fits. A fallback that is resident is
 * replaced by a stronger one once there is room for it, between requests. */
typedef struct MoeResidency {
    const char *server_program;   /* llama-server */
    const char *models[32];       /* in preference order; the first one admitted loads */
    int model_count;
    int discovered;               /* models came from a scan: rank strongest (largest) first */
    const char *host;             /* listen host, default 127.0.0.1 */
    uint16_t port;                /* public port */
    uint16_t backend_port;        /* llama-server's private port */
    uint32_t gpu_layers;
    uint32_t context_size;        /* least context a load may have */
    uint32_t max_context;         /* most context a load is given when memory allows */
    uint32_t idle_seconds;        /* 0 = stay resident once loaded */
    uint32_t load_timeout_seconds;
    const char *admission_db;
    const char *plane;            /* grant owner, e.g. moe-serve:7000 */
    int64_t estimate_bytes;       /* 0 = derive from the checkpoint */
    int64_t memory_available;     /* 0 = measure; set only by tests */
    const char *memory_available_file; /* tests: re-read this file's number at every decision */
} MoeResidency;

int moe_residency_serve(const MoeResidency *config);

#endif
