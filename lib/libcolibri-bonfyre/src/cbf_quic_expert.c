/*
 * cbf_quic_expert.c — QUIC-based expert push/pull/rebalance across fleet
 *
 * Implements network tier (4th memory tier) for expert streaming:
 *   - Expert pull: fetch expert from remote peer via QUIC stream
 *   - Expert replicate: push expert to remote peer
 *   - Fleet rebalancing: analyze router cache, redistribute hot experts
 *
 * Uses libquic-transport with family-key-based stream multiplexing.
 * Expert family key: FNV-1a-64(layer_idx || expert_idx)
 */

#include "colibri_bonfyre.h"
#include "cbf_internal.h"
#include "bonfyre.h"
#include "bf_quic.h"
#include "fragment.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdatomic.h>

/* Expert transfer chunk size (16KB optimal for QUIC) */
#define EXPERT_CHUNK_SIZE (16 * 1024)

/* ═══════════════════════════════════════════════════════════════════
 * Expert family key (for QUIC stream multiplexing)
 * ═══════════════════════════════════════════════════════════════════ */

static uint64_t expert_family_key(uint32_t layer_idx, uint32_t expert_idx) {
    /* FNV-1a-64 hash */
    uint64_t hash = 0xcbf29ce484222325ULL;
    
    /* Hash layer index */
    for (int i = 0; i < 4; i++) {
        hash ^= (uint8_t)(layer_idx >> (i * 8));
        hash *= 0x100000001b3ULL;
    }
    
    /* Hash expert index */
    for (int i = 0; i < 4; i++) {
        hash ^= (uint8_t)(expert_idx >> (i * 8));
        hash *= 0x100000001b3ULL;
    }
    
    return hash;
}

static void format_family_key(uint64_t hash, char out[17]) {
    snprintf(out, 17, "%016llx", (unsigned long long)hash);
}

/* ═══════════════════════════════════════════════════════════════════
 * Expert pull (fetch expert from remote peer via QUIC)
 * ═══════════════════════════════════════════════════════════════════ */

typedef struct {
    uint8_t *data;
    size_t size;
    size_t received;
    atomic_bool complete;
} expert_recv_ctx_t;

static void expert_recv_cb(const char *family_key,
                           const uint8_t *data, size_t len,
                           int fin, void *user) {
    expert_recv_ctx_t *ctx = (expert_recv_ctx_t *)user;
    
    /* Allocate buffer on first chunk */
    if (ctx->data == NULL) {
        ctx->size = len * 2;  /* Initial estimate */
        ctx->data = malloc(ctx->size);
        if (!ctx->data) return;
    }
    
    /* Grow buffer if needed */
    if (ctx->received + len > ctx->size) {
        ctx->size = ctx->received + len + EXPERT_CHUNK_SIZE;
        ctx->data = realloc(ctx->data, ctx->size);
        if (!ctx->data) return;
    }
    
    /* Copy chunk */
    memcpy(ctx->data + ctx->received, data, len);
    ctx->received += len;
    
    if (fin) {
        atomic_store(&ctx->complete, true);
    }
}

int cbf_expert_pull(cbf_engine_t *engine,
                    uint32_t layer_idx,
                    uint32_t expert_idx,
                    const char *source_host,
                    cbf_tier_t target_tier) {
    if (!engine || !source_host) return CBF_ERR_INVALID;
    
    if (!engine->quic_ctx) {
        fprintf(stderr, "[cbf_quic] QUIC context not initialized\n");
        return CBF_ERR_INVALID;
    }
    
    /* Find peer by host */
    bf_quic_conn_t *conn = NULL;
    
    for (int i = 0; i < MAX_PEERS; i++) {
        if (engine->peers[i].info.online &&
            strcmp(engine->peers[i].info.host, source_host) == 0) {
            conn = engine->peers[i].conn;
            break;
        }
    }
    
    /* Connect if not already connected */
    if (!conn) {
        /* Parse host:port from source_host */
        char host[64];
        uint16_t port = 7000;
        
        const char *colon = strchr(source_host, ':');
        if (colon) {
            size_t host_len = colon - source_host;
            if (host_len >= sizeof(host)) host_len = sizeof(host) - 1;
            memcpy(host, source_host, host_len);
            host[host_len] = '\0';
            port = (uint16_t)atoi(colon + 1);
        } else {
            snprintf(host, sizeof(host), "%s", source_host);
        }
        
        conn = bf_quic_connect(engine->quic_ctx, host, port);
        
        if (!conn) {
            fprintf(stderr, "[cbf_quic] failed to connect to %s:%u\n", host, port);
            return CBF_ERR_INVALID;
        }
        
        /* Store connection in peer table */
        for (int i = 0; i < MAX_PEERS; i++) {
            if (!engine->peers[i].info.online) {
                snprintf(engine->peers[i].info.host, sizeof(engine->peers[i].info.host), "%s", host);
                engine->peers[i].info.port = port;
                engine->peers[i].info.online = true;
                engine->peers[i].conn = conn;
                atomic_store(&engine->peers[i].connected, true);
                engine->n_peers++;
                break;
            }
        }
    }
    
    /* Prepare stream metadata */
    uint64_t fam_hash = expert_family_key(layer_idx, expert_idx);
    char fam_key[17];
    format_family_key(fam_hash, fam_key);
    
    bf_quic_stream_meta_t meta = {
        .layer = BF_LAYER_SUBSTRATE,  /* Experts are substrate tier */
        .total_bytes = 0,  /* Size unknown until transfer */
    };
    snprintf(meta.family_key, sizeof(meta.family_key), "%s", fam_key);
    
    /* Open stream */
    bf_quic_stream_t *stream = bf_quic_stream_open(conn, &meta);
    if (!stream) {
        fprintf(stderr, "[cbf_quic] failed to open expert stream L%u E%u\n",
                layer_idx, expert_idx);
        return CBF_ERR_INVALID;
    }
    
    /* Set up receive context */
    expert_recv_ctx_t recv_ctx = {
        .data = NULL,
        .size = 0,
        .received = 0,
    };
    atomic_store(&recv_ctx.complete, false);
    
    /* Start receive (polling until complete) */
    int rc = bf_quic_recv_start(conn, expert_recv_cb, &recv_ctx);
    if (rc != 0) {
        bf_quic_stream_close(stream);
        return CBF_ERR_INVALID;
    }
    
    /* Poll until complete (with timeout) */
    int timeout_count = 0;
    while (!atomic_load(&recv_ctx.complete) && timeout_count < 100) {
        bf_quic_recv_poll(conn, 100);  /* 100ms timeout */
        timeout_count++;
    }
    
    bf_quic_stream_close(stream);
    
    if (!atomic_load(&recv_ctx.complete)) {
        fprintf(stderr, "[cbf_quic] expert transfer timeout L%u E%u\n",
                layer_idx, expert_idx);
        free(recv_ctx.data);
        return CBF_ERR_INVALID;
    }
    
    /* Store expert in target tier */
    printf("[cbf_quic] received expert L%u E%u: %zu bytes → tier %d\n",
           layer_idx, expert_idx, recv_ctx.received, target_tier);
    
    /* Next step is storing the received expert in the engine cache via
     * store_expert() and updating the expert_cache_t residency metadata. */
    
    free(recv_ctx.data);
    return CBF_OK;
}

/* ═══════════════════════════════════════════════════════════════════
 * Expert replicate (push expert to remote peer via QUIC)
 * ═══════════════════════════════════════════════════════════════════ */

int cbf_expert_replicate(cbf_engine_t *engine, uint32_t layer_idx, uint32_t expert_idx,
                         const char *target_peer) {
    if (!engine || !target_peer) return CBF_ERR_INVALID;
    
    if (!engine->quic_ctx) {
        fprintf(stderr, "[cbf_quic] QUIC context not initialized\n");
        return CBF_ERR_INVALID;
    }
    
    /* Find expert in cache */
    expert_blob_t *expert = NULL;
    
    /* Next step is loading the expert from the engine cache through
     * load_expert() in cbf_forward.c before replication. */
    
    if (!expert) {
        fprintf(stderr, "[cbf_quic] expert L%u E%u not in cache\n",
                layer_idx, expert_idx);
        return CBF_ERR_INVALID;
    }
    
    /* Connect to target peer */
    char host[64];
    uint16_t port = 7000;
    
    const char *colon = strchr(target_peer, ':');
    if (colon) {
        size_t host_len = colon - target_peer;
        if (host_len >= sizeof(host)) host_len = sizeof(host) - 1;
        memcpy(host, target_peer, host_len);
        host[host_len] = '\0';
        port = (uint16_t)atoi(colon + 1);
    } else {
        snprintf(host, sizeof(host), "%s", target_peer);
    }
    
    bf_quic_conn_t *conn = bf_quic_connect(engine->quic_ctx, host, port);
    if (!conn) {
        fprintf(stderr, "[cbf_quic] failed to connect to %s:%u\n", host, port);
        return CBF_ERR_INVALID;
    }
    
    /* Prepare stream metadata */
    uint64_t fam_hash = expert_family_key(layer_idx, expert_idx);
    char fam_key[17];
    format_family_key(fam_hash, fam_key);
    
    bf_quic_stream_meta_t meta = {
        .layer = BF_LAYER_SUBSTRATE,
        .total_bytes = expert->size,
    };
    snprintf(meta.family_key, sizeof(meta.family_key), "%s", fam_key);
    
    /* Open stream */
    bf_quic_stream_t *stream = bf_quic_stream_open(conn, &meta);
    if (!stream) {
        fprintf(stderr, "[cbf_quic] failed to open expert stream L%u E%u\n",
                layer_idx, expert_idx);
        bf_quic_conn_close(conn);
        return CBF_ERR_INVALID;
    }
    
    /* Send expert data in chunks */
    size_t sent = 0;
    
    while (sent < expert->size) {
        size_t chunk_size = expert->size - sent;
        if (chunk_size > EXPERT_CHUNK_SIZE) chunk_size = EXPERT_CHUNK_SIZE;
        
        int fin = (sent + chunk_size >= expert->size);
        
        int rc = bf_quic_stream_write(stream, expert->data + sent, chunk_size, fin);
        if (rc != 0) {
            fprintf(stderr, "[cbf_quic] failed to send expert chunk L%u E%u\n",
                    layer_idx, expert_idx);
            bf_quic_stream_close(stream);
            bf_quic_conn_close(conn);
            return CBF_ERR_INVALID;
        }
        
        sent += chunk_size;
    }
    
    bf_quic_stream_close(stream);
    bf_quic_conn_close(conn);
    
    printf("[cbf_quic] replicated expert L%u E%u: %zu bytes → %s\n",
           layer_idx, expert_idx, expert->size, target_peer);
    
    return CBF_OK;
}

/* ═══════════════════════════════════════════════════════════════════
 * Fleet rebalancing (redistribute experts across peers)
 * ═══════════════════════════════════════════════════════════════════ */

int cbf_rebalance_experts(cbf_engine_t *engine) {
    if (!engine) return CBF_ERR_INVALID;
    
    if (engine->n_peers == 0) {
        fprintf(stderr, "[cbf_rebalance] no peers configured\n");
        return CBF_ERR_INVALID;
    }
    
    /* Query hot experts from fragment store */
    if (!engine->frag_store) {
        fprintf(stderr, "[cbf_rebalance] no fragment store attached\n");
        return CBF_ERR_INVALID;
    }
    
    bf_fragment_query_t query = {
        .kind = "expert_routing_heat",
        .perspective = NULL,  /* All workloads */
        .min_confidence = 0.5f,  /* Hot experts only */
        .start_after_ms = -1,
        .end_before_ms = -1,
        .limit = 100,  /* Top 100 hottest */
        .offset = 0
    };
    
    int result_count = 0;
    bf_fragment_t **results = bf_fragment_query(engine->frag_store, &query, &result_count);
    
    if (!results || result_count < 0) {
        fprintf(stderr, "[cbf_rebalance] failed to query hot experts\n");
        return CBF_ERR_IO;
    }
    
    printf("[cbf_rebalance] rebalancing %d hot experts across %u peers\n",
           result_count, engine->n_peers);
    
    /* Distribute experts round-robin across peers */
    int peer_idx = 0;
    int replicated = 0;
    
    for (int i = 0; i < result_count; i++) {
        bf_fragment_t *frag = results[i];
        
        /* Parse expert coordinates from payload */
        uint32_t layer_idx = 0, expert_idx = 0;
        
        if (frag->payload_json) {
            const char *layer_str = strstr(frag->payload_json, "\"layer_idx\":");
            const char *expert_str = strstr(frag->payload_json, "\"expert_idx\":");
            
            if (layer_str) sscanf(layer_str + 12, "%u", &layer_idx);
            if (expert_str) sscanf(expert_str + 13, "%u", &expert_idx);
        }
        
        /* Find next online peer */
        while (peer_idx < MAX_PEERS && !engine->peers[peer_idx].info.online) {
            peer_idx++;
        }
        
        if (peer_idx >= MAX_PEERS) {
            peer_idx = 0;
            while (peer_idx < MAX_PEERS && !engine->peers[peer_idx].info.online) {
                peer_idx++;
            }
        }
        
        if (peer_idx < MAX_PEERS) {
            char peer_addr[80];
            snprintf(peer_addr, sizeof(peer_addr), "%s:%u",
                     engine->peers[peer_idx].info.host,
                     engine->peers[peer_idx].info.port);
            
            printf("[cbf_rebalance] L%u E%u (heat=%.3f) → %s\n",
                   layer_idx, expert_idx, frag->confidence, peer_addr);
            
            /* Replicate expert (non-blocking in real implementation) */
            int rc = cbf_expert_replicate(engine, layer_idx, expert_idx, peer_addr);
            if (rc == 0) replicated++;
            
            peer_idx++;
        }
        
        bf_fragment_free(frag);
    }
    
    free(results);
    
    printf("[cbf_rebalance] successfully replicated %d/%d experts\n",
           replicated, result_count);
    
    return CBF_OK;
}

/* ═══════════════════════════════════════════════════════════════════
 * Peer management
 * ═══════════════════════════════════════════════════════════════════ */

int cbf_engine_add_peer(cbf_engine_t *engine, const cbf_peer_t *peer) {
    if (!engine || !peer) return CBF_ERR_INVALID;
    
    printf("[cbf_peer] adding peer: %s:%u\n", peer->host, peer->port);
    
    /* Find empty slot */
    for (int i = 0; i < MAX_PEERS; i++) {
        if (!engine->peers[i].info.online) {
            engine->peers[i].info = *peer;
            engine->peers[i].info.online = true;
            engine->peers[i].conn = NULL;
            atomic_store(&engine->peers[i].connected, false);
            engine->n_peers++;
            printf("[cbf_peer] peer %d: %s:%u (total=%u)\n",
                   i, peer->host, peer->port, engine->n_peers);
            return CBF_OK;
        }
    }
    
    fprintf(stderr, "[cbf_peer] peer table full (%d max)\n", MAX_PEERS);
    return CBF_ERR_INVALID;
}

int cbf_engine_remove_peer(cbf_engine_t *engine, const char *host) {
    if (!engine || !host) return CBF_ERR_INVALID;
    
    for (int i = 0; i < MAX_PEERS; i++) {
        if (engine->peers[i].info.online && strcmp(engine->peers[i].info.host, host) == 0) {
            /* Close connection if active */
            if (engine->peers[i].conn) {
                bf_quic_conn_close(engine->peers[i].conn);
                engine->peers[i].conn = NULL;
            }
            
            engine->peers[i].info.online = false;
            atomic_store(&engine->peers[i].connected, false);
            engine->n_peers--;
            
            printf("[cbf_peer] removed peer %d: %s (remaining=%u)\n",
                   i, host, engine->n_peers);
            return CBF_OK;
        }
    }
    
    fprintf(stderr, "[cbf_peer] peer not found: %s\n", host);
    return CBF_ERR_INVALID;
}

int cbf_engine_list_peers(const cbf_engine_t *engine,
                          cbf_peer_t **out_peers,
                          uint32_t *out_count) {
    if (!engine || !out_peers || !out_count) return CBF_ERR_INVALID;
    
    *out_count = 0;
    cbf_peer_t *peers = calloc(MAX_PEERS, sizeof(cbf_peer_t));
    if (!peers) return CBF_ERR_INVALID;
    
    for (int i = 0; i < MAX_PEERS && *out_count < engine->n_peers; i++) {
        if (engine->peers[i].info.online) {
            peers[*out_count] = engine->peers[i].info;
            (*out_count)++;
        }
    }
    
    *out_peers = peers;
    return CBF_OK;
}
