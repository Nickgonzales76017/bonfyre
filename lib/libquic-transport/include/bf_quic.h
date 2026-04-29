/*
 * bf_quic.h — QUIC stream multiplexer for Bonfyre artifact distribution
 *
 * Maps artifact families 1:1 to QUIC streams with layer-based priority:
 *   surface  → priority 0 (highest)
 *   value    → priority 1
 *   transform→ priority 2
 *   substrate→ priority 3 (lowest)
 *
 * Zero head-of-line blocking between families. 0-RTT reconnect for
 * persistent sync sessions (BonfyreSync). Bulk transfer mode for
 * large corpus pushes (BonfyreDistribute).
 *
 * Wraps ngtcp2 + ngtcp2_crypto_ossl underneath.
 *
 * Link: -lbf_quic -lngtcp2 -lngtcp2_crypto_ossl -lssl -lcrypto
 */
#ifndef BF_QUIC_H
#define BF_QUIC_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Layer priorities (maps to QUIC stream urgency) ──────────── */

#define BF_LAYER_SURFACE    0   /* deliverables, offers, proofs  */
#define BF_LAYER_VALUE      1   /* ledger, meter, repurpose      */
#define BF_LAYER_TRANSFORM  2   /* briefs, cleans, tags          */
#define BF_LAYER_SUBSTRATE  3   /* raw transcripts, media files  */
#define BF_LAYER_COUNT      4

/* ── Error codes ─────────────────────────────────────────────── */

#define BF_QUIC_OK              0
#define BF_QUIC_ERR_CONNECT    -1
#define BF_QUIC_ERR_TLS        -2
#define BF_QUIC_ERR_STREAM     -3
#define BF_QUIC_ERR_TIMEOUT    -4
#define BF_QUIC_ERR_CLOSED     -5
#define BF_QUIC_ERR_MEMORY     -6
#define BF_QUIC_ERR_INVALID    -7

/* ── Opaque handles ──────────────────────────────────────────── */

typedef struct bf_quic_ctx        bf_quic_ctx_t;
typedef struct bf_quic_conn       bf_quic_conn_t;
typedef struct bf_quic_stream     bf_quic_stream_t;

/* ── Stream metadata (per artifact family) ───────────────────── */

typedef struct {
    char     family_key[17];    /* FNV-1a-64 hex from BfArtifact */
    uint8_t  layer;             /* BF_LAYER_* constant            */
    uint64_t total_bytes;       /* expected payload size (0=unknown) */
    char     content_hash[65];  /* SHA-256 hex for dedup (optional) */
} bf_quic_stream_meta_t;

/* ── Transfer result (per stream) ────────────────────────────── */

typedef struct {
    char     family_key[17];
    uint64_t bytes_sent;
    uint64_t bytes_acked;
    double   elapsed_ms;
    int      status;            /* BF_QUIC_OK or error code */
    uint8_t  zero_rtt;          /* 1 if 0-RTT was used */
} bf_quic_stream_result_t;

/* ── Batch transfer result ───────────────────────────────────── */

typedef struct {
    int                      count;
    bf_quic_stream_result_t *streams;     /* caller-owned after return */
    double                   total_ms;
    uint64_t                 total_bytes;
    int                      succeeded;
    int                      failed;
} bf_quic_batch_result_t;

/* ── Callbacks ───────────────────────────────────────────────── */

/* Called when data arrives for a family stream (receiver side) */
typedef void (*bf_quic_recv_cb)(const char *family_key,
                                 const uint8_t *data, size_t len,
                                 int fin, void *user);

/* Called when a stream completes (sender side) */
typedef void (*bf_quic_done_cb)(const bf_quic_stream_result_t *result,
                                 void *user);

/* ── Context lifecycle ───────────────────────────────────────── */

/* Create a QUIC transport context.
 * cert_path/key_path: TLS certificate for server mode (NULL for client-only).
 * session_file: path to 0-RTT ticket storage (NULL to disable). */
bf_quic_ctx_t *bf_quic_ctx_new(const char *cert_path,
                                const char *key_path,
                                const char *session_file);

void bf_quic_ctx_free(bf_quic_ctx_t *ctx);

/* Set 0-RTT early data token from prior connection (enables 0-RTT) */
int bf_quic_ctx_set_token(bf_quic_ctx_t *ctx,
                           const uint8_t *token, size_t len);

/* ── Client connections ──────────────────────────────────────── */

/* Connect to a remote Bonfyre node.
 * Returns a connection handle or NULL on failure.
 * On success, the QUIC handshake is complete (or 0-RTT in-flight). */
bf_quic_conn_t *bf_quic_connect(bf_quic_ctx_t *ctx,
                                 const char *host, uint16_t port);

/* Reconnect using stored 0-RTT token (for BonfyreSync persistent links) */
bf_quic_conn_t *bf_quic_reconnect_0rtt(bf_quic_ctx_t *ctx,
                                         const char *host, uint16_t port);

void bf_quic_conn_close(bf_quic_conn_t *conn);

/* ── Stream creation (family-mapped) ─────────────────────────── */

/* Open a new unidirectional stream for an artifact family.
 * The stream is created with priority derived from meta->layer.
 * Multiple streams can be open simultaneously (one per family). */
bf_quic_stream_t *bf_quic_stream_open(bf_quic_conn_t *conn,
                                       const bf_quic_stream_meta_t *meta);

/* Write artifact data to a family stream.
 * Data is framed internally with content-hash for integrity.
 * fin=1 signals end of stream. */
int bf_quic_stream_write(bf_quic_stream_t *stream,
                          const uint8_t *data, size_t len, int fin);

/* Close a stream (sends FIN if not already sent) */
void bf_quic_stream_close(bf_quic_stream_t *stream);

/* ── Bulk transfer (BonfyreDistribute replacement) ───────────── */

/* Artifact descriptor for batch transfer */
typedef struct {
    const char  *path;           /* local file path to send         */
    const char  *family_key;     /* FNV-1a-64 hex                   */
    const char  *content_hash;   /* SHA-256 hex (for dedup)         */
    uint8_t      layer;          /* BF_LAYER_* constant             */
} bf_quic_artifact_t;

/* Send multiple artifacts over family-multiplexed streams.
 * Each artifact gets its own stream, prioritized by layer.
 * Surface-layer artifacts are delivered first.
 * Returns aggregate result (caller frees result.streams). */
bf_quic_batch_result_t bf_quic_send_batch(bf_quic_conn_t *conn,
                                           const bf_quic_artifact_t *artifacts,
                                           int count,
                                           bf_quic_done_cb on_done,
                                           void *user);

/* ── Receive side (for BonfyreSync inbound) ──────────────────── */

/* Start listening for incoming artifact streams.
 * recv_cb fires per-chunk as data arrives. */
int bf_quic_recv_start(bf_quic_conn_t *conn,
                        bf_quic_recv_cb recv_cb,
                        void *user);

/* Drive the event loop for recv (call in a loop or from kqueue/epoll) */
int bf_quic_recv_poll(bf_quic_conn_t *conn, int timeout_ms);

/* ── Server mode (for BonfyreApi QUIC endpoint) ──────────────── */

typedef struct bf_quic_server bf_quic_server_t;

/* Accept callback: fires for each new incoming connection */
typedef void (*bf_quic_accept_cb)(bf_quic_conn_t *conn, void *user);

/* Start a QUIC server on bind_addr:port */
bf_quic_server_t *bf_quic_server_start(bf_quic_ctx_t *ctx,
                                         const char *bind_addr,
                                         uint16_t port,
                                         bf_quic_accept_cb on_accept,
                                         void *user);

void bf_quic_server_stop(bf_quic_server_t *srv);

/* Drive server event loop (returns number of events processed, -1 on error) */
int bf_quic_server_poll(bf_quic_server_t *srv, int timeout_ms);

/* ── Utility ─────────────────────────────────────────────────── */

/* Map BfArtifact type string to layer constant */
int bf_quic_layer_from_type(const char *artifact_type);

/* Human-readable error string */
const char *bf_quic_strerror(int err);

#ifdef __cplusplus
}
#endif

#endif /* BF_QUIC_H */
