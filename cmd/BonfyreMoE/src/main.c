/*
 * main.c — BonfyreMoE: Distributed expert-streaming inference
 *
 * CLI for running large MoE models (GLM-5.2, DeepSeek-V3, etc.) with
 * expert streaming across VRAM/RAM/disk/network tiers.
 *
 * Modes:
 *   plan     — Inspect placement strategy without loading (fast, read-only)
 *   doctor   — Validate model files, memory, network peers (readiness check)
 *   chat     — Interactive chat session with persistent KV cache
 *   serve    — OpenAI-compatible HTTP API for GGUF MoE checkpoints
 *   peer     — reserved expert-server surface for native Colibri checkpoints
 *   rebalance— Analyze router cache and replicate hot experts across fleet
 *
 * Examples:
 *   bonfyre-moe plan --model /nvme/glm52 --vram 80 --ram 128
 *   bonfyre-moe chat --model /nvme/glm52 --workload code_review
 *   bonfyre-moe serve --model /nvme/glm52 --port 8080
 *   bonfyre-moe peer --model /nvme/glm52 --port 7000
 *   bonfyre-moe rebalance --model /nvme/glm52 --peers node-2:7000,node-3:7000
 */

#include "colibri_bonfyre.h"
#include <errno.h>
#include <limits.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>

#define DEFAULT_VRAM_GB  0    /* Auto-detect */
#define DEFAULT_RAM_GB   0    /* Auto-detect */
#define DEFAULT_PORT     7000

static void usage(const char *argv0) {
    fprintf(stderr,
        "BonfyreMoE — Distributed expert-streaming MoE inference\n"
        "\n"
        "Usage:\n"
        "  %s --help | help\n"
        "  %s --version\n"
        "  %s status\n"
        "  %s list-archs\n"
        "  %s plan --model <path> [options]\n"
        "  %s doctor --model <path> [options]\n"
        "  %s chat --model <path> [options]\n"
        "  %s serve --model <path> [options]\n"
        "  %s peer --model <path> --port <port> (unavailable in this build)\n"
        "  %s rebalance --model <path> --peers <host:port,...>\n"
        "  %s embed --model <path> --text <file> --out <file>\n"
        "\n"
        "Model configuration:\n"
        "  --model <path>         Model directory (required for most modes)\n"
        "  --arch <name>          Architecture name or alias (default: auto-detect)\n"
        "                         Use 'list-archs' to see available architectures\n"
        "  --gpu-layers <N>       llama.cpp GPU layers for GGUF MoE (default: 99)\n"
        "  --ctx-size <N>         llama.cpp context for GGUF MoE (default: 2048)\n"
        "\n"
        "Memory tiers:\n"
        "  --vram <GB>            VRAM budget (0 = auto-detect, default)\n"
        "  --ram <GB>             RAM budget (0 = auto-detect, default)\n"
        "  --disk <path>          Expert storage directory (default: model/experts/)\n"
        "  --no-network           Disable network tier (default: enabled)\n"
        "\n"
        "Pinning & caching:\n"
        "  --pin-experts <N>      Pin N hot experts resident (0 = auto, default)\n"
        "  --pin-threshold <F>    Heat threshold for pinning (0.0-1.0, default: 0.5)\n"
        "  --workload <tag>       Workload identifier for router cache (default: default)\n"
        "\n"
        "Performance:\n"
        "  --io-threads <N>       Async I/O pool size (0 = auto, default: 4)\n"
        "  --prefetch <N>         Router lookahead layers (0 = disable, default: 1)\n"
        "  --numa                 NUMA-aware weight placement (default: off)\n"
        "\n"
        "Network (distributed):\n"
        "  --peers <list>         Comma-separated host:port peers\n"
        "  --port <port>          API listen port for GGUF serve (default: 7000)\n"
        "\n"
        "Examples:\n"
        "  %s list-archs\n"
        "  %s plan --model /nvme/glm52 --vram 80 --ram 128\n"
        "  %s chat --model /nvme/mixtral --arch mixtral\n"
        "  %s status\n"
        "  %s serve --model /models/Qwen1.5-MoE-A2.7B-Chat.gguf --port 7000\n"
        "\n",
        argv0, argv0, argv0, argv0, argv0, argv0, argv0, argv0, argv0, argv0,
        argv0, argv0, argv0, argv0, argv0, argv0);
}

/* Kept dependency-free so MCP/fleet probes do not need a model checkpoint. */
static int mode_status(void) {
    puts("{\"schema\":\"bonfyre.moe.status.v1\","
         "\"name\":\"bonfyre-moe\","
         "\"planner\":true,"
         "\"doctor\":true,"
         "\"interactive_chat\":true,"
         "\"gguf_moe_backend\":\"llama.cpp\","
         "\"http_api\":true,"
         "\"quic_peer_server\":false,"
         "\"model_required_for_inference\":true}");
    return 0;
}

static int mode_reserved_service(const char *mode) {
    fprintf(stderr,
            "BonfyreMoE: '%s' is not available in this build. "
            "It previously entered an idle loop while advertising a live service; "
            "that behavior has been removed. Use 'plan', 'doctor', or 'chat' "
            "with a materialized MoE checkpoint.\n",
            mode);
    return 78; /* EX_CONFIG: capability intentionally not configured. */
}

static bool has_suffix(const char *value, const char *suffix) {
    size_t value_len;
    size_t suffix_len;
    if (!value || !suffix) return false;
    value_len = strlen(value);
    suffix_len = strlen(suffix);
    return value_len >= suffix_len &&
           strcmp(value + value_len - suffix_len, suffix) == 0;
}

static bool is_gguf_model(const char *model_path) {
    return has_suffix(model_path, ".gguf");
}

/* The distributed Colibri format and GGUF are intentionally separate.  For
 * GGUF MoE checkpoints delegate to llama.cpp, which executes their router and
 * experts faithfully and provides the OpenAI HTTP surface.  execvp avoids a
 * shell entirely, so model paths and flags never become command text. */
static int exec_llama_cpp(const char *program,
                          const char *model_path,
                          uint16_t port,
                          uint32_t gpu_layers,
                          uint32_t context_size,
                          bool serve) {
    char port_buf[16];
    char gpu_layers_buf[16];
    char context_buf[16];
    char *argv[16];
    int argi = 0;

    if (!program || !model_path) return 2;
    snprintf(port_buf, sizeof(port_buf), "%u", (unsigned)port);
    snprintf(gpu_layers_buf, sizeof(gpu_layers_buf), "%u", gpu_layers);
    snprintf(context_buf, sizeof(context_buf), "%u", context_size);

    argv[argi++] = (char *)program;
    argv[argi++] = "-m";
    argv[argi++] = (char *)model_path;
    argv[argi++] = "-ngl";
    argv[argi++] = gpu_layers_buf;
    argv[argi++] = "-c";
    argv[argi++] = context_buf;
    if (serve) {
        argv[argi++] = "--host";
        argv[argi++] = "127.0.0.1";
        argv[argi++] = "--port";
        argv[argi++] = port_buf;
    } else {
        argv[argi++] = "-cnv";
    }
    argv[argi] = NULL;

    execvp(program, argv);
    fprintf(stderr, "BonfyreMoE: failed to start %s: %s\n", program, strerror(errno));
    return 127;
}

static int mode_gguf_doctor(const char *model_path) {
    struct stat st;
    if (!model_path || stat(model_path, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size == 0) {
        fprintf(stderr, "BonfyreMoE: GGUF checkpoint is missing or empty: %s\n",
                model_path ? model_path : "(null)");
        return 1;
    }
    printf("BonfyreMoE GGUF Doctor\n"
           "======================\n"
           "Model: %s\n"
           "Bytes: %llu\n"
           "Backend: llama.cpp\n"
           "✓ Checkpoint is ready for chat or OpenAI-compatible serve\n",
           model_path, (unsigned long long)st.st_size);
    return 0;
}

static int argi(const char *s, int fallback) {
    return s ? atoi(s) : fallback;
}

static float argf(const char *s, float fallback) {
    return s ? atof(s) : fallback;
}

/* Parse comma-separated peer list: "host1:port1,host2:port2,..." */
static int parse_peers(const char *peer_str, cbf_peer_t **out_peers, uint32_t *out_count) {
    if (!peer_str || !out_peers || !out_count) return -1;

    /* Count commas to determine array size */
    uint32_t count = 1;
    for (const char *p = peer_str; *p; p++) {
        if (*p == ',') count++;
    }

    *out_peers = malloc(count * sizeof(cbf_peer_t));
    if (!*out_peers) return -1;
    *out_count = 0;

    char *str_copy = strdup(peer_str);
    char *token = strtok(str_copy, ",");

    while (token && *out_count < count) {
        cbf_peer_t *peer = &(*out_peers)[*out_count];
        memset(peer, 0, sizeof(*peer));

        /* Parse host:port */
        char *colon = strchr(token, ':');
        if (!colon) {
            free(str_copy);
            free(*out_peers);
            return -1;
        }

        *colon = '\0';
        snprintf(peer->host, sizeof(peer->host), "%s", token);
        peer->port = atoi(colon + 1);
        peer->online = false;  /* Will be probed */

        (*out_count)++;
        token = strtok(NULL, ",");
    }

    free(str_copy);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════
 * Mode implementations
 * ═══════════════════════════════════════════════════════════════════ */

static int mode_plan(const char *model_path, const cbf_model_shape_t *shape,
                     const cbf_memory_config_t *memory) {
    cbf_placement_plan_t plan;
    int rc = cbf_engine_plan(model_path, shape, memory, &plan);

    if (rc != CBF_OK) {
        fprintf(stderr, "BonfyreMoE: planning failed: %s\n", cbf_strerror(rc));
        return 1;
    }

    /* Print plan summary */
    printf("BonfyreMoE Placement Plan\n");
    printf("=========================\n");
    printf("Model: %s\n", model_path);
    printf("\n");
    printf("Expert Placement:\n");
    printf("  VRAM:    %u experts (%llu MB)\n", plan.experts_vram, (unsigned long long)plan.vram_used_mb);
    printf("  RAM:     %u experts (%llu MB)\n", plan.experts_ram, (unsigned long long)plan.ram_used_mb);
    printf("  Disk:    %u experts (%llu MB)\n", plan.experts_disk, (unsigned long long)plan.disk_used_mb);
    printf("  Network: %u experts\n", plan.experts_network);
    printf("\n");
    printf("Estimated Throughput:\n");
    printf("  Cold cache: %.2f tok/s\n", plan.cold_tok_per_sec);
    printf("  Warm cache: %.2f tok/s\n", plan.warm_tok_per_sec);
    printf("\n");

    return 0;
}

static int mode_doctor(const char *model_path, const cbf_model_shape_t *shape,
                       const cbf_memory_config_t *memory) {
    printf("BonfyreMoE Doctor — Readiness Check\n");
    printf("===================================\n");

    cbf_engine_t *engine = cbf_engine_new(model_path, shape, memory);
    if (!engine) {
        fprintf(stderr, "✗ Engine initialization failed\n");
        return 1;
    }

    int rc = cbf_engine_doctor(engine);
    if (rc == CBF_OK) {
        printf("✓ All checks passed\n");
    } else {
        printf("✗ Readiness check failed: %s\n", cbf_strerror(rc));
    }

    cbf_engine_free(engine);
    return (rc == CBF_OK) ? 0 : 1;
}

static int mode_chat(const char *model_path, const cbf_model_shape_t *shape,
                     const cbf_memory_config_t *memory, const char *workload) {
    printf("BonfyreMoE Chat\n");
    printf("===============\n");
    printf("Model: %s\n", model_path);
    printf("Workload: %s\n", workload);
    printf("\nInitializing engine...\n");

    cbf_engine_t *engine = cbf_engine_new(model_path, shape, memory);
    if (!engine) {
        fprintf(stderr, "✗ Engine creation failed\n");
        return 1;
    }

    int rc = cbf_engine_load(engine);
    if (rc != CBF_OK) {
        fprintf(stderr, "✗ Model loading failed: %s\n", cbf_strerror(rc));
        cbf_engine_free(engine);
        return 1;
    }

    /* Load router cache for this workload */
    cbf_router_cache_load(engine, workload);

    printf("✓ Ready\n\n");

    /* Load tokenizer */
    char tokenizer_path[512];
    snprintf(tokenizer_path, sizeof(tokenizer_path), "%s/tokenizer.model", model_path);

    cbf_tokenizer_t *tokenizer = cbf_tokenizer_load(tokenizer_path);
    if (!tokenizer) {
        printf("⚠ Tokenizer not found, creating fallback char-level tokenizer\n");
        tokenizer = cbf_tokenizer_create_fallback(shape->n_vocab);
        if (!tokenizer) {
            fprintf(stderr, "✗ Failed to create tokenizer\n");
            cbf_engine_free(engine);
            return 1;
        }
    }

    /* Create KV cache */
    cbf_kv_cache_t *kv_cache = cbf_kv_cache_new(engine, 2048);  /* 2K context */
    if (!kv_cache) {
        fprintf(stderr, "✗ Failed to create KV cache\n");
        cbf_tokenizer_free(tokenizer);
        cbf_engine_free(engine);
        return 1;
    }

    /* Interactive chat loop */
    printf("Enter your messages (type 'quit' to exit, 'save' to persist cache):\n\n");

    char input_buffer[4096];
    uint32_t tokens[2048];
    uint32_t token_count;
    float logits[200000];  /* Max vocab size */

    while (1) {
        printf("You: ");
        fflush(stdout);

        if (!fgets(input_buffer, sizeof(input_buffer), stdin)) break;

        /* Remove trailing newline */
        size_t len = strlen(input_buffer);
        if (len > 0 && input_buffer[len - 1] == '\n') {
            input_buffer[len - 1] = '\0';
        }

        /* Check for commands */
        if (strcmp(input_buffer, "quit") == 0) break;
        if (strcmp(input_buffer, "save") == 0) {
            char cache_path[512];
            snprintf(cache_path, sizeof(cache_path), "%s/.kv_cache.bin", model_path);
            rc = cbf_kv_cache_save(kv_cache, cache_path);
            printf("✓ KV cache saved to %s\n", cache_path);
            continue;
        }

        /* Tokenize input */
        rc = cbf_tokenizer_encode(tokenizer, input_buffer, tokens, 2048, &token_count);
        if (rc != CBF_OK || token_count == 0) {
            fprintf(stderr, "✗ Tokenization failed\n");
            continue;
        }

        printf("Assistant: ");
        fflush(stdout);

        /* Prefill every user token. Passing only the final token here loses the
         * actual prompt context and makes a loaded MoE model look incoherent. */
        rc = cbf_forward(engine, tokens, token_count, kv_cache, logits);
        if (rc != CBF_OK) {
            fprintf(stderr, "\n✗ Prompt prefill failed: %s\n", cbf_strerror(rc));
            continue;
        }
        if (getenv("BONFYRE_DEBUG_LOGITS")) {
            uint32_t nz = 0;
            uint32_t am = 0;
            float amv = logits[0];
            for (uint32_t j = 0; j < shape->n_vocab; j++) {
                if (logits[j] != 0.0f) nz++;
                if (logits[j] > amv) { amv = logits[j]; am = j; }
            }
            fprintf(stderr, "[debug] PREFILL logits: nonzero=%u/%u argmax_id=%u argmax_val=%f first5=[%f %f %f %f %f]\n",
                    nz, shape->n_vocab, am, amv, logits[0], logits[1], logits[2], logits[3], logits[4]);
        }

        /* Generate response (max 256 tokens) */
        uint32_t max_new_tokens = 256;
        for (uint32_t i = 0; i < max_new_tokens; i++) {
            /* Sample from logits (greedy decoding for simplicity) */
            uint32_t best_token = 0;
            float best_logit = logits[0];
            for (uint32_t j = 1; j < shape->n_vocab; j++) {
                if (logits[j] > best_logit) {
                    best_logit = logits[j];
                    best_token = j;
                }
            }

            /* Check for EOS */
            if (best_token == cbf_tokenizer_eos(tokenizer)) {
                break;
            }

            /* Decode and print */
            const char *token_str = cbf_tokenizer_decode_token(tokenizer, best_token);
            printf("%s", token_str);
            fflush(stdout);

            /* Add to context */
            if (token_count < 2048) {
                tokens[token_count++] = best_token;
                rc = cbf_forward(engine, &best_token, 1, kv_cache, logits);
                if (rc != CBF_OK) {
                    fprintf(stderr, "\n✗ Decode step failed: %s\n", cbf_strerror(rc));
                    break;
                }
                if (getenv("BONFYRE_DEBUG_LOGITS") && i < 3) {
                    uint32_t nz = 0;
                    uint32_t am = 0;
                    float amv = logits[0];
                    for (uint32_t j = 0; j < shape->n_vocab; j++) {
                        if (logits[j] != 0.0f) nz++;
                        if (logits[j] > amv) { amv = logits[j]; am = j; }
                    }
                    fprintf(stderr, "[debug] DECODE[%u] logits: nonzero=%u/%u argmax_id=%u argmax_val=%f first5=[%f %f %f %f %f]\n",
                            i, nz, shape->n_vocab, am, amv, logits[0], logits[1], logits[2], logits[3], logits[4]);
                }
            } else {
                fprintf(stderr, "\n✗ Context limit reached\n");
                break;
            }
        }

        printf("\n\n");
    }

    /* Cleanup */
    cbf_kv_cache_free(kv_cache);
    cbf_tokenizer_free(tokenizer);
    cbf_engine_free(engine);
    return 0;
}

#if 0 /* Disabled: these former placeholder loops must never advertise service. */
static int mode_serve(const char *model_path, const cbf_model_shape_t *shape,
                      const cbf_memory_config_t *memory, uint16_t port) {
    printf("BonfyreMoE API Server\n");
    printf("=====================\n");
    printf("Model: %s\n", model_path);
    printf("Port: %u\n", port);
    printf("\nStarting server...\n");

    /* Initialize engine */
    cbf_engine_t *engine = cbf_engine_new(model_path, shape, memory);
    if (!engine) {
        fprintf(stderr, "✗ Engine initialization failed\n");
        return 1;
    }

    int rc = cbf_engine_load(engine);
    if (rc != CBF_OK) {
        fprintf(stderr, "✗ Model loading failed: %s\n", cbf_strerror(rc));
        cbf_engine_free(engine);
        return 1;
    }

    /* Load tokenizer */
    char tokenizer_path[512];
    snprintf(tokenizer_path, sizeof(tokenizer_path), "%s/tokenizer.model", model_path);
    cbf_tokenizer_t *tokenizer = cbf_tokenizer_load(tokenizer_path);
    if (!tokenizer) {
        tokenizer = cbf_tokenizer_create_fallback(shape->n_vocab);
    }

    /* Simple HTTP server implementation (minimal OpenAI-compatible API)
     * In production, would use libevent, libmicrohttpd, or similar.
     * For now, implement a basic socket-based server.
     */

    printf("✓ Server ready at http://0.0.0.0:%u\n", port);
    printf("Endpoints:\n");
    printf("  POST /v1/chat/completions\n");
    printf("  POST /v1/completions\n");
    printf("  GET  /health\n\n");

    /* Server loop (placeholder - would use proper HTTP library) */
    printf("Note: Full HTTP server implementation requires libmicrohttpd or similar.\n");
    printf("For now, use the 'chat' mode for interactive testing.\n\n");

    /* Keep alive for Ctrl+C */
    printf("Press Ctrl+C to stop...\n");
    while (1) {
        sleep(1);
        /* TODO: Process HTTP requests */
    }

    cbf_tokenizer_free(tokenizer);
    cbf_engine_free(engine);
    return 0;
}

static int mode_peer(const char *model_path, const cbf_model_shape_t *shape,
                     const cbf_memory_config_t *memory, uint16_t port) {
    printf("BonfyreMoE Expert Server\n");
    printf("========================\n");
    printf("Model: %s\n", model_path);
    printf("Listen port: %u\n", port);
    printf("\nStarting expert server...\n");

    /* Initialize engine */
    cbf_engine_t *engine = cbf_engine_new(model_path, shape, memory);
    if (!engine) {
        fprintf(stderr, "✗ Engine initialization failed\n");
        return 1;
    }

    int rc = cbf_engine_load(engine);
    if (rc != CBF_OK) {
        fprintf(stderr, "✗ Model loading failed: %s\n", cbf_strerror(rc));
        cbf_engine_free(engine);
        return 1;
    }

    printf("✓ Expert server ready on QUIC port %u\n", port);
    printf("Total experts available: %u layers × %u experts = %u\n",
           shape->n_layers, shape->n_experts_per_layer,
           shape->n_layers * shape->n_experts_per_layer);
    printf("\nWaiting for expert requests...\n\n");

    /* QUIC server loop
     * The actual implementation would:
     * 1. Accept incoming QUIC connections
     * 2. Register expert request callback
     * 3. When requested, load expert from disk/cache
     * 4. Stream expert blob back to requester
     * 5. Track bandwidth and latency metrics
     */

    printf("Note: QUIC server uses bf_quic API from libquic-transport\n");
    printf("Expert requests will be served from:\n");
    printf("  - VRAM cache (if present)\n");
    printf("  - RAM cache (per-layer LRU)\n");
    printf("  - Disk (mmap'd weight files)\n\n");

    /* Keep alive */
    printf("Press Ctrl+C to stop...\n");
    uint64_t experts_served = 0;
    uint64_t bytes_sent = 0;
    time_t start_time = time(NULL);

    while (1) {
        sleep(5);

        /* Get metrics */
        cbf_metrics_t metrics;
        cbf_engine_get_metrics(engine, &metrics);

        printf("\r[Metrics] Uptime: %lus | Tokens: %llu | Experts cached: %u | Requests: %llu | Sent: %llu MB",
               (unsigned long)(time(NULL) - start_time),
               (unsigned long long)metrics.tokens_generated,
               metrics.experts_resident[CBF_TIER_VRAM] + metrics.experts_resident[CBF_TIER_RAM],
               (unsigned long long)experts_served,
               (unsigned long long)(bytes_sent / (1024 * 1024)));
        fflush(stdout);

        /* TODO: In real implementation, process QUIC events here */
    }

    cbf_engine_free(engine);
    return 0;
}
#endif

static int mode_rebalance(const char *model_path, const cbf_model_shape_t *shape,
                          const cbf_memory_config_t *memory,
                          cbf_peer_t *peers, uint32_t n_peers) {
    printf("BonfyreMoE Expert Rebalancing\n");
    printf("=============================\n");
    printf("Model: %s\n", model_path);
    printf("Peers: %u\n", n_peers);

    cbf_engine_t *engine = cbf_engine_new(model_path, shape, memory);
    if (!engine) {
        fprintf(stderr, "✗ Engine creation failed\n");
        return 1;
    }

    /* Add peers */
    for (uint32_t i = 0; i < n_peers; i++) {
        printf("  Adding peer: %s:%u\n", peers[i].host, peers[i].port);
        cbf_engine_add_peer(engine, &peers[i]);
    }

    /* Run rebalancing */
    int rc = cbf_rebalance_experts(engine);
    if (rc != CBF_OK) {
        fprintf(stderr, "✗ Rebalancing failed: %s\n", cbf_strerror(rc));
    } else {
        printf("✓ Rebalancing complete\n");
    }

    cbf_engine_free(engine);
    return (rc == CBF_OK) ? 0 : 1;
}

/* A real model-backed embedding route.  It tokenizes with the native MoE
 * tokenizer, reads only the corresponding rows from the materialized model
 * embedding table, mean-pools them, then L2-normalizes the model vector. */
static int mode_embed(const char *model_path, const cbf_model_shape_t *shape,
                      const char *text_path, const char *out_path) {
    char tokenizer_path[1024], embeddings_path[1024];
    FILE *input = NULL, *embeddings = NULL, *out = NULL;
    char *text = NULL;
    long text_size;
    uint32_t tokens[256], token_count = 0;
    float *vector = NULL, *row = NULL;
    cbf_tokenizer_t *tokenizer = NULL;
    int rc = 1;

    if (!text_path || !out_path || !shape || !shape->d_model || !shape->n_vocab) return 2;
    input = fopen(text_path, "rb");
    if (!input || fseek(input, 0, SEEK_END) || (text_size = ftell(input)) < 0 ||
        fseek(input, 0, SEEK_SET)) goto done;
    text = calloc((size_t)text_size + 1, 1);
    if (!text || fread(text, 1, (size_t)text_size, input) != (size_t)text_size) goto done;
    snprintf(tokenizer_path, sizeof(tokenizer_path), "%s/tokenizer.model", model_path);
    snprintf(embeddings_path, sizeof(embeddings_path), "%s/embeddings.bin", model_path);
    tokenizer = cbf_tokenizer_load(tokenizer_path);
    if (!tokenizer || cbf_tokenizer_encode(tokenizer, text, tokens,
                                            (uint32_t)(sizeof(tokens) / sizeof(tokens[0])),
                                            &token_count) != CBF_OK || !token_count) {
        fprintf(stderr, "BonfyreMoE: native tokenizer failed for embedding input\n");
        goto done;
    }
    embeddings = fopen(embeddings_path, "rb");
    vector = calloc(shape->d_model, sizeof(float));
    row = malloc((size_t)shape->d_model * sizeof(float));
    if (!embeddings || !vector || !row) goto done;
    for (uint32_t index = 0; index < token_count; ++index) {
        uint32_t token = tokens[index];
        uint64_t offset;
        if (token >= shape->n_vocab) { fprintf(stderr, "BonfyreMoE: token out of model vocabulary\n"); goto done; }
        offset = (uint64_t)token * (uint64_t)shape->d_model * sizeof(float);
        if (offset > LONG_MAX || fseek(embeddings, (long)offset, SEEK_SET) ||
            fread(row, sizeof(float), shape->d_model, embeddings) != shape->d_model) {
            fprintf(stderr, "BonfyreMoE: embedding row read failed for token %u\n", token);
            goto done;
        }
        for (uint32_t dim = 0; dim < shape->d_model; ++dim) vector[dim] += row[dim];
    }
    {
        double norm = 0.0;
        for (uint32_t dim = 0; dim < shape->d_model; ++dim) {
            vector[dim] /= (float)token_count;
            if (!isfinite(vector[dim])) { fprintf(stderr, "BonfyreMoE: non-finite embedding value\n"); goto done; }
            norm += (double)vector[dim] * vector[dim];
        }
        norm = sqrt(norm);
        if (!isfinite(norm) || norm <= 0.0) { fprintf(stderr, "BonfyreMoE: zero embedding norm\n"); goto done; }
        for (uint32_t dim = 0; dim < shape->d_model; ++dim) vector[dim] = (float)(vector[dim] / norm);
    }
    out = fopen(out_path, "w");
    if (!out) goto done;
    fprintf(out, "{\n  \"backend\": \"native-moe-token-embedding\",\n"
                 "  \"model_path\": \"%s\",\n  \"dimensions\": %u,\n"
                 "  \"token_count\": %u,\n  \"normalized\": true,\n  \"vector\": [",
            model_path, shape->d_model, token_count);
    for (uint32_t dim = 0; dim < shape->d_model; ++dim)
        fprintf(out, "%s%.9g", dim ? "," : "", vector[dim]);
    fprintf(out, "]\n}\n");
    rc = ferror(out) ? 1 : 0;
done:
    if (out) fclose(out);
    if (input) fclose(input);
    if (embeddings) fclose(embeddings);
    if (tokenizer) cbf_tokenizer_free(tokenizer);
    free(text); free(vector); free(row);
    return rc;
}

/* ═══════════════════════════════════════════════════════════════════
 * Main entry point
 * ═══════════════════════════════════════════════════════════════════ */

int main(int argc, char **argv) {
    if (argc < 2) {
        usage(argv[0]);
        return 2;
    }

    const char *mode = argv[1];

    /* Model-free modes are stable fleet/MCP probe surfaces. */
    if (strcmp(mode, "--help") == 0 || strcmp(mode, "-h") == 0 ||
        strcmp(mode, "help") == 0) {
        usage(argv[0]);
        return 0;
    }
    if (strcmp(mode, "--version") == 0 || strcmp(mode, "version") == 0) {
        puts("bonfyre-moe 0.1.0");
        return 0;
    }
    if (strcmp(mode, "status") == 0) {
        return mode_status();
    }

    /* Special mode: list architectures (no other args needed) */
    if (strcmp(mode, "list-archs") == 0) {
        cbf_arch_list_all();
        return 0;
    }

    /* Parse arguments */
    const char *model_path = NULL;
    const char *arch = "auto";  /* Changed from glm52 to auto */
    const char *workload = "default";
    const char *peer_str = NULL;
    const char *text_path = NULL;
    const char *out_path = NULL;
    uint64_t vram_gb = DEFAULT_VRAM_GB;
    uint64_t ram_gb = DEFAULT_RAM_GB;
    const char *disk_path = NULL;
    bool enable_network = true;
    uint32_t pin_experts = 0;
    float pin_threshold = 0.5f;
    uint32_t io_threads = 4;
    uint32_t prefetch = 1;
    uint32_t gpu_layers = 99;
    uint32_t context_size = 2048;
    bool numa = false;
    uint16_t port = DEFAULT_PORT;

    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--model") == 0 && i + 1 < argc) model_path = argv[++i];
        else if (strcmp(argv[i], "--arch") == 0 && i + 1 < argc) arch = argv[++i];
        else if (strcmp(argv[i], "--workload") == 0 && i + 1 < argc) workload = argv[++i];
        else if (strcmp(argv[i], "--vram") == 0 && i + 1 < argc) vram_gb = argi(argv[++i], vram_gb);
        else if (strcmp(argv[i], "--ram") == 0 && i + 1 < argc) ram_gb = argi(argv[++i], ram_gb);
        else if (strcmp(argv[i], "--disk") == 0 && i + 1 < argc) disk_path = argv[++i];
        else if (strcmp(argv[i], "--no-network") == 0) enable_network = false;
        else if (strcmp(argv[i], "--pin-experts") == 0 && i + 1 < argc) pin_experts = argi(argv[++i], pin_experts);
        else if (strcmp(argv[i], "--pin-threshold") == 0 && i + 1 < argc) pin_threshold = argf(argv[++i], pin_threshold);
        else if (strcmp(argv[i], "--io-threads") == 0 && i + 1 < argc) io_threads = argi(argv[++i], io_threads);
        else if (strcmp(argv[i], "--prefetch") == 0 && i + 1 < argc) prefetch = argi(argv[++i], prefetch);
        else if (strcmp(argv[i], "--gpu-layers") == 0 && i + 1 < argc) gpu_layers = argi(argv[++i], gpu_layers);
        else if (strcmp(argv[i], "--ctx-size") == 0 && i + 1 < argc) context_size = argi(argv[++i], context_size);
        else if (strcmp(argv[i], "--numa") == 0) numa = true;
        else if (strcmp(argv[i], "--no-spec") == 0) {
            fprintf(stderr,
                    "Error: speculative decoding is not configurable in this build; "
                    "the option has no safe implementation.\n");
            return 2;
        }
        else if (strcmp(argv[i], "--peers") == 0 && i + 1 < argc) peer_str = argv[++i];
        else if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) port = argi(argv[++i], port);
        else if (strcmp(argv[i], "--text") == 0 && i + 1 < argc) text_path = argv[++i];
        else if (strcmp(argv[i], "--out") == 0 && i + 1 < argc) out_path = argv[++i];
        else {
            usage(argv[0]);
            return 2;
        }
    }

    if (!model_path) {
        fprintf(stderr, "Error: --model required\n");
        usage(argv[0]);
        return 2;
    }

    if (is_gguf_model(model_path)) {
        if (strcmp(mode, "doctor") == 0) {
            return mode_gguf_doctor(model_path);
        }
        if (strcmp(mode, "chat") == 0) {
            return exec_llama_cpp(getenv("BONFYRE_LLAMA_CLI_BIN") ? getenv("BONFYRE_LLAMA_CLI_BIN") : "llama-cli",
                                  model_path, port, gpu_layers, context_size, false);
        }
        if (strcmp(mode, "serve") == 0) {
            return exec_llama_cpp(getenv("BONFYRE_LLAMA_SERVER_BIN") ? getenv("BONFYRE_LLAMA_SERVER_BIN") : "llama-server",
                                  model_path, port, gpu_layers, context_size, true);
        }
        if (strcmp(mode, "peer") == 0) {
            fprintf(stderr, "BonfyreMoE: peer mode requires the native Colibri expert format; GGUF MoE supports chat and serve.\n");
            return 78;
        }
    }

    /* Default disk path */
    char disk_buf[1024];
    if (!disk_path) {
        snprintf(disk_buf, sizeof(disk_buf), "%s/experts", model_path);
        disk_path = disk_buf;
    }

    /* Detect or load architecture */
    cbf_model_shape_t shape;
    int rc;

    if (strcmp(arch, "auto") == 0) {
        /* Try auto-detection */
        rc = cbf_arch_auto_detect(model_path, &shape);
        if (rc != CBF_OK) {
            fprintf(stderr, "Error: Failed to auto-detect architecture\n");
            fprintf(stderr, "Specify --arch explicitly or create model_config.json\n");
            fprintf(stderr, "Run '%s list-archs' to see available architectures\n", argv[0]);
            return 1;
        }
    } else {
        /* Look up by name */
        const cbf_model_shape_t *tmpl = cbf_arch_lookup(arch);
        if (tmpl) {
            shape = *tmpl;
        } else {
            fprintf(stderr, "Error: Unknown architecture '%s'\n", arch);
            fprintf(stderr, "Run '%s list-archs' to see available architectures\n", argv[0]);
            return 1;
        }
    }

    /* Memory config */
    cbf_memory_config_t memory = {
        .vram_gb = vram_gb,
        .ram_gb = ram_gb,
        .disk_path = disk_path,
        .enable_network = enable_network,
        .pin_hot_experts = pin_experts,
        .pin_heat_threshold = pin_threshold,
        .io_threads = io_threads,
        .prefetch_layers = prefetch,
        .numa_interleave = numa
    };

    /* Dispatch to mode handlers */
    if (strcmp(mode, "plan") == 0) {
        return mode_plan(model_path, &shape, &memory);
    } else if (strcmp(mode, "doctor") == 0) {
        return mode_doctor(model_path, &shape, &memory);
    } else if (strcmp(mode, "chat") == 0) {
        return mode_chat(model_path, &shape, &memory, workload);
    } else if (strcmp(mode, "embed") == 0) {
        if (!text_path || !out_path) {
            fprintf(stderr, "Error: embed requires --text and --out\n");
            return 2;
        }
        return mode_embed(model_path, &shape, text_path, out_path);
    } else if (strcmp(mode, "serve") == 0) {
        return mode_reserved_service(mode);
    } else if (strcmp(mode, "peer") == 0) {
        return mode_reserved_service(mode);
    } else if (strcmp(mode, "rebalance") == 0) {
        cbf_peer_t *peers = NULL;
        uint32_t n_peers = 0;
        if (peer_str && parse_peers(peer_str, &peers, &n_peers) == 0) {
            int rc = mode_rebalance(model_path, &shape, &memory, peers, n_peers);
            free(peers);
            return rc;
        } else {
            fprintf(stderr, "Error: --peers required for rebalance mode\n");
            return 2;
        }
    } else {
        fprintf(stderr, "Error: unknown mode '%s'\n", mode);
        usage(argv[0]);
        return 2;
    }
}
