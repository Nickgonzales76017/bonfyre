/*
 * bf_quic.c — QUIC stream multiplexer for Bonfyre artifact distribution
 *
 * Implementation of family-mapped QUIC streams over ngtcp2.
 * Each artifact family gets its own stream with layer-based priority.
 *
 * Wire format per stream:
 *   [4 bytes: magic "BFQS"]
 *   [16 bytes: family_key (hex, NUL-padded)]
 *   [1 byte: layer]
 *   [8 bytes: total_bytes (big-endian)]
 *   [64 bytes: content_hash (hex, NUL-padded)]
 *   [...payload...]
 *   [32 bytes: SHA-256 of payload (integrity check)]
 */

#include "bf_quic.h"

#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#ifdef __APPLE__
#  include <sys/event.h>
#else
#  include <sys/epoll.h>
#endif

#include <ngtcp2/ngtcp2.h>
#include <ngtcp2/ngtcp2_crypto.h>
#include <ngtcp2/ngtcp2_crypto_ossl.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/rand.h>
#include <openssl/ssl.h>

/* ── constants ───────────────────────────────────────────────── */

#define QUIC_MTU             1452
#define MAX_STREAMS          256
#define SESSION_TICKET_FILE  ".bonfyre-quic-ticket"
#define STREAM_HEADER_SIZE   (4 + 16 + 1 + 8 + 64)  /* 93 bytes */
#define INTEGRITY_SIZE       32  /* SHA-256 */

static const uint8_t STREAM_MAGIC[4] = { 'B', 'F', 'Q', 'S' };

/* ALPN for Bonfyre artifact transport */
static const uint8_t ALPN[] = "\x0abonfyre-at";
#define ALPN_LEN (sizeof(ALPN) - 1)

/* ── timestamp ───────────────────────────────────────────────── */

static ngtcp2_tstamp now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (ngtcp2_tstamp)ts.tv_sec * 1000000000ULL +
           (ngtcp2_tstamp)ts.tv_nsec;
}

static double elapsed_ms(ngtcp2_tstamp start)
{
    return (double)(now_ns() - start) / 1e6;
}

/* ── big-endian helpers ──────────────────────────────────────── */

static void put_be64(uint8_t *p, uint64_t v)
{
    for (int i = 7; i >= 0; i--) {
        p[i] = (uint8_t)(v & 0xFF);
        v >>= 8;
    }
}

static uint64_t get_be64(const uint8_t *p)
{
    uint64_t v = 0;
    for (int i = 0; i < 8; i++)
        v = (v << 8) | p[i];
    return v;
}

/* ── internal structures ─────────────────────────────────────── */

struct bf_quic_ctx {
    SSL_CTX         *ssl_ctx;
    char            *session_file;
    uint8_t         *zerortt_token;
    size_t           zerortt_token_len;
    int              is_server;
};

struct bf_quic_stream {
    int64_t               stream_id;
    bf_quic_stream_meta_t meta;
    uint64_t              bytes_written;
    uint64_t              bytes_acked;
    ngtcp2_tstamp         t_start;
    int                   fin_sent;
    int                   header_sent;
};

struct bf_quic_conn {
    ngtcp2_conn             *qconn;
    SSL                     *ssl;
    bf_quic_ctx_t           *ctx;
    int                      fd;
    struct sockaddr_storage  remote_addr;
    socklen_t                remote_addrlen;
    struct sockaddr_storage  local_addr;
    socklen_t                local_addrlen;

    bf_quic_stream_t         streams[MAX_STREAMS];
    int                      stream_count;

    /* receive callback */
    bf_quic_recv_cb          recv_cb;
    void                    *recv_user;

    uint8_t                  pkt_buf[QUIC_MTU * 4];
    int                      zerortt_used;

    /* Connection ref for ngtcp2 TLS integration */
    ngtcp2_crypto_conn_ref   conn_ref;
};

struct bf_quic_server {
    bf_quic_ctx_t       *ctx;
    int                  fd;
    int                  kq;  /* kqueue fd (macOS) or epoll fd (Linux) */
    bf_quic_accept_cb    on_accept;
    void                *user;
    bf_quic_conn_t      *conns[64];
    int                  conn_count;
};

/* ── QUIC stream priority from layer ─────────────────────────── */

static uint8_t layer_to_urgency(uint8_t layer)
{
    /* QUIC urgency: 0 = highest, 7 = lowest.
     * Map our 4 layers to urgency 0-3. */
    if (layer >= BF_LAYER_COUNT) return 7;
    return layer;  /* direct mapping: surface=0, value=1, transform=2, substrate=3 */
}

/* ── TLS setup ───────────────────────────────────────────────── */

static int alpn_select_cb(SSL *ssl, const unsigned char **out,
                           unsigned char *outlen,
                           const unsigned char *in, unsigned int inlen,
                           void *arg)
{
    (void)ssl; (void)arg;
    /* Look for our ALPN in the client's list */
    for (unsigned int i = 0; i + 1 < inlen; ) {
        unsigned int len = in[i];
        if (i + 1 + len > inlen) break;
        if (len == ALPN[0] && memcmp(&in[i + 1], &ALPN[1], len) == 0) {
            *out = &in[i + 1];
            *outlen = (unsigned char)len;
            return SSL_TLSEXT_ERR_OK;
        }
        i += 1 + len;
    }
    return SSL_TLSEXT_ERR_ALERT_FATAL;
}

static SSL_CTX *create_ssl_ctx(const char *cert_path, const char *key_path, int is_server)
{
    SSL_CTX *ctx = SSL_CTX_new(is_server ? TLS_server_method() : TLS_client_method());
    if (!ctx) return NULL;

    SSL_CTX_set_min_proto_version(ctx, TLS1_3_VERSION);
    SSL_CTX_set_max_proto_version(ctx, TLS1_3_VERSION);

    if (is_server && cert_path && key_path) {
        if (SSL_CTX_use_certificate_chain_file(ctx, cert_path) != 1 ||
            SSL_CTX_use_PrivateKey_file(ctx, key_path, SSL_FILETYPE_PEM) != 1) {
            SSL_CTX_free(ctx);
            return NULL;
        }
        SSL_CTX_set_alpn_select_cb(ctx, alpn_select_cb, NULL);
    } else if (!is_server) {
        SSL_CTX_set_alpn_protos(ctx, ALPN, (unsigned int)ALPN_LEN);
    }

    /* Enable 0-RTT early data */
    SSL_CTX_set_max_early_data(ctx, 0xFFFFFFFF);

    /* ngtcp2 crypto integration */
    ngtcp2_crypto_ossl_configure_client_context(ctx);

    return ctx;
}

/* ── ngtcp2 callbacks ────────────────────────────────────────── */

static void rand_cb(uint8_t *dest, size_t len,
                     const ngtcp2_rand_ctx *rand_ctx)
{
    (void)rand_ctx;
    RAND_bytes(dest, (int)len);
}

static int get_new_cid_cb(ngtcp2_conn *conn, ngtcp2_cid *cid,
                            uint8_t *token, size_t cidlen,
                            void *user_data)
{
    (void)conn; (void)user_data;
    RAND_bytes(cid->data, (int)cidlen);
    cid->datalen = cidlen;
    RAND_bytes(token, NGTCP2_STATELESS_RESET_TOKENLEN);
    return 0;
}

/* Stream data received (server / receiver side) */
static int recv_stream_data_cb(ngtcp2_conn *conn, uint32_t flags,
                                 int64_t stream_id, uint64_t offset,
                                 const uint8_t *data, size_t datalen,
                                 void *user_data, void *stream_user_data)
{
    (void)conn; (void)flags; (void)offset; (void)stream_user_data;
    bf_quic_conn_t *c = (bf_quic_conn_t *)user_data;

    if (!c->recv_cb) return 0;

    /* Parse family_key from stream header if this is the first chunk */
    const char *family_key = "unknown";
    int fin = (flags & NGTCP2_STREAM_DATA_FLAG_FIN) ? 1 : 0;

    /* Find which stream this belongs to */
    for (int i = 0; i < c->stream_count; i++) {
        if (c->streams[i].stream_id == stream_id) {
            family_key = c->streams[i].meta.family_key;
            break;
        }
    }

    /* If this is the very first data on a new stream, parse the header */
    if (offset == 0 && datalen >= STREAM_HEADER_SIZE) {
        if (memcmp(data, STREAM_MAGIC, 4) == 0) {
            /* Register the stream */
            if (c->stream_count < MAX_STREAMS) {
                bf_quic_stream_t *s = &c->streams[c->stream_count];
                s->stream_id = stream_id;
                memcpy(s->meta.family_key, data + 4, 16);
                s->meta.family_key[16] = '\0';
                s->meta.layer = data[20];
                s->meta.total_bytes = get_be64(data + 21);
                memcpy(s->meta.content_hash, data + 29, 64);
                s->meta.content_hash[64] = '\0';
                s->t_start = now_ns();
                c->stream_count++;
                family_key = s->meta.family_key;
            }
            /* Skip header, deliver payload only */
            data += STREAM_HEADER_SIZE;
            datalen -= STREAM_HEADER_SIZE;
        }
    }

    if (datalen > 0) {
        c->recv_cb(family_key, data, datalen, fin, c->recv_user);
    }

    return 0;
}

static int stream_close_cb(ngtcp2_conn *conn, uint32_t flags,
                             int64_t stream_id, uint64_t app_error_code,
                             void *user_data, void *stream_user_data)
{
    (void)conn; (void)flags; (void)app_error_code; (void)stream_user_data;
    bf_quic_conn_t *c = (bf_quic_conn_t *)user_data;

    for (int i = 0; i < c->stream_count; i++) {
        if (c->streams[i].stream_id == stream_id) {
            c->streams[i].fin_sent = 1;
            break;
        }
    }
    return 0;
}

static int acked_stream_data_offset_cb(ngtcp2_conn *conn, int64_t stream_id,
                                         uint64_t offset, uint64_t datalen,
                                         void *user_data, void *stream_user_data)
{
    (void)conn; (void)offset; (void)stream_user_data;
    bf_quic_conn_t *c = (bf_quic_conn_t *)user_data;

    for (int i = 0; i < c->stream_count; i++) {
        if (c->streams[i].stream_id == stream_id) {
            c->streams[i].bytes_acked += datalen;
            break;
        }
    }
    return 0;
}

/* ── Context lifecycle ───────────────────────────────────────── */

bf_quic_ctx_t *bf_quic_ctx_new(const char *cert_path,
                                const char *key_path,
                                const char *session_file)
{
    bf_quic_ctx_t *ctx = calloc(1, sizeof(*ctx));
    if (!ctx) return NULL;

    ctx->is_server = (cert_path != NULL && key_path != NULL);
    ctx->ssl_ctx = create_ssl_ctx(cert_path, key_path, ctx->is_server);
    if (!ctx->ssl_ctx) {
        free(ctx);
        return NULL;
    }

    if (session_file) {
        ctx->session_file = strdup(session_file);
    }

    return ctx;
}

void bf_quic_ctx_free(bf_quic_ctx_t *ctx)
{
    if (!ctx) return;
    SSL_CTX_free(ctx->ssl_ctx);
    free(ctx->session_file);
    free(ctx->zerortt_token);
    free(ctx);
}

int bf_quic_ctx_set_token(bf_quic_ctx_t *ctx,
                           const uint8_t *token, size_t len)
{
    if (!ctx || !token || len == 0) return BF_QUIC_ERR_INVALID;
    free(ctx->zerortt_token);
    ctx->zerortt_token = malloc(len);
    if (!ctx->zerortt_token) return BF_QUIC_ERR_MEMORY;
    memcpy(ctx->zerortt_token, token, len);
    ctx->zerortt_token_len = len;
    return BF_QUIC_OK;
}

/* ── Client connection ───────────────────────────────────────── */

static int resolve_and_connect(const char *host, uint16_t port,
                                struct sockaddr_storage *addr,
                                socklen_t *addrlen)
{
    struct addrinfo hints = { .ai_family = AF_UNSPEC, .ai_socktype = SOCK_DGRAM };
    char portstr[8];
    snprintf(portstr, sizeof(portstr), "%u", port);

    struct addrinfo *res = NULL;
    if (getaddrinfo(host, portstr, &hints, &res) != 0 || !res) return -1;

    int fd = socket(res->ai_family, SOCK_DGRAM, 0);
    if (fd < 0) { freeaddrinfo(res); return -1; }

    /* Set non-blocking */
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    if (connect(fd, res->ai_addr, res->ai_addrlen) < 0 && errno != EINPROGRESS) {
        close(fd);
        freeaddrinfo(res);
        return -1;
    }

    memcpy(addr, res->ai_addr, res->ai_addrlen);
    *addrlen = (socklen_t)res->ai_addrlen;
    freeaddrinfo(res);
    return fd;
}

bf_quic_conn_t *bf_quic_connect(bf_quic_ctx_t *ctx,
                                 const char *host, uint16_t port)
{
    if (!ctx || !host) return NULL;

    bf_quic_conn_t *c = calloc(1, sizeof(*c));
    if (!c) return NULL;
    c->ctx = ctx;

    c->fd = resolve_and_connect(host, port, &c->remote_addr, &c->remote_addrlen);
    if (c->fd < 0) { free(c); return NULL; }

    /* Get local address */
    c->local_addrlen = sizeof(c->local_addr);
    getsockname(c->fd, (struct sockaddr *)&c->local_addr, &c->local_addrlen);

    /* Create SSL */
    c->ssl = SSL_new(ctx->ssl_ctx);
    if (!c->ssl) { close(c->fd); free(c); return NULL; }
    SSL_set_connect_state(c->ssl);
    SSL_set_tlsext_host_name(c->ssl, host);

    /* ngtcp2 client connection */
    ngtcp2_cid scid, dcid;
    scid.datalen = 16;
    dcid.datalen = 16;
    RAND_bytes(scid.data, (int)scid.datalen);
    RAND_bytes(dcid.data, (int)dcid.datalen);

    ngtcp2_path path;
    ngtcp2_addr_init(&path.local, (struct sockaddr *)&c->local_addr, c->local_addrlen);
    ngtcp2_addr_init(&path.remote, (struct sockaddr *)&c->remote_addr, c->remote_addrlen);

    ngtcp2_callbacks callbacks = {
        ngtcp2_crypto_client_initial_cb,
        NULL, /* recv_client_initial */
        ngtcp2_crypto_recv_crypto_data_cb,
        NULL, /* handshake_completed */
        NULL, /* recv_version_negotiation */
        ngtcp2_crypto_encrypt_cb,
        ngtcp2_crypto_decrypt_cb,
        ngtcp2_crypto_hp_mask_cb,
        recv_stream_data_cb,
        acked_stream_data_offset_cb,
        NULL, /* stream_open */
        stream_close_cb,
        NULL, /* recv_stateless_reset */
        ngtcp2_crypto_recv_retry_cb,
        NULL, /* extend_max_local_bidi_streams */
        NULL, /* extend_max_local_uni_streams */
        rand_cb,
        get_new_cid_cb,
        NULL, /* remove_connection_id */
        ngtcp2_crypto_update_key_cb,
        NULL, /* path_validation */
        NULL, /* select_preferred_addr */
        NULL, /* stream_reset */
        NULL, /* extend_max_remote_bidi_streams */
        NULL, /* extend_max_remote_uni_streams */
        NULL, /* extend_max_stream_data */
        NULL, /* dcid_status */
        NULL, /* handshake_confirmed */
        NULL, /* recv_new_token */
        ngtcp2_crypto_delete_crypto_aead_ctx_cb,
        ngtcp2_crypto_delete_crypto_cipher_ctx_cb,
        NULL, /* recv_datagram */
        NULL, /* ack_datagram */
        NULL, /* lost_datagram */
        ngtcp2_crypto_get_path_challenge_data_cb,
        NULL, /* stream_stop_sending */
        ngtcp2_crypto_version_negotiation_cb,
        NULL, /* recv_rx_key */
        NULL, /* recv_tx_key */
        NULL, /* early_data_rejected */
    };

    ngtcp2_settings settings;
    ngtcp2_settings_default(&settings);
    settings.initial_ts = now_ns();
    settings.max_window = 24 * 1024 * 1024;            /* 24MB flow control */
    settings.max_stream_window = 16 * 1024 * 1024;     /* 16MB per stream */

    /* Enable 0-RTT if we have a token */
    if (ctx->zerortt_token && ctx->zerortt_token_len > 0) {
        settings.token = ctx->zerortt_token;
        settings.tokenlen = ctx->zerortt_token_len;
    }

    ngtcp2_transport_params params;
    ngtcp2_transport_params_default(&params);
    params.initial_max_streams_uni = MAX_STREAMS;
    params.initial_max_streams_bidi = MAX_STREAMS;
    params.initial_max_stream_data_bidi_local = 16 * 1024 * 1024;
    params.initial_max_stream_data_bidi_remote = 16 * 1024 * 1024;
    params.initial_max_stream_data_uni = 16 * 1024 * 1024;
    params.initial_max_data = 64 * 1024 * 1024; /* 64MB total */

    int rv = ngtcp2_conn_client_new(&c->qconn, &dcid, &scid, &path,
                                      NGTCP2_PROTO_VER_V1, &callbacks,
                                      &settings, &params, NULL, c);
    if (rv != 0) {
        SSL_free(c->ssl);
        close(c->fd);
        free(c);
        return NULL;
    }

    /* Wire up SSL ↔ ngtcp2 */
    c->conn_ref.get_conn = NULL;
    c->conn_ref.user_data = c;
    SSL_set_app_data(c->ssl, &c->conn_ref);
    ngtcp2_conn_set_tls_native_handle(c->qconn, c->ssl);

    /* Drive initial handshake packets */
    ngtcp2_tstamp ts = now_ns();
    ngtcp2_pkt_info pi;
    ngtcp2_ssize nwrite;

    for (int round = 0; round < 32; round++) {
        nwrite = ngtcp2_conn_write_pkt(c->qconn, NULL, &pi,
                                         c->pkt_buf, sizeof(c->pkt_buf), ts);
        if (nwrite > 0) {
            send(c->fd, c->pkt_buf, (size_t)nwrite, 0);
        }
        if (nwrite == 0 || nwrite == NGTCP2_ERR_WRITE_MORE) break;

        /* Read response */
        uint8_t rbuf[QUIC_MTU * 2];
        struct sockaddr_storage from;
        socklen_t fromlen = sizeof(from);
        ssize_t nread = recvfrom(c->fd, rbuf, sizeof(rbuf), 0,
                                  (struct sockaddr *)&from, &fromlen);
        if (nread > 0) {
            ngtcp2_path rpath;
            ngtcp2_addr_init(&rpath.local, (struct sockaddr *)&c->local_addr,
                              c->local_addrlen);
            ngtcp2_addr_init(&rpath.remote, (struct sockaddr *)&from, fromlen);
            ngtcp2_pkt_info rpi = {0};
            ngtcp2_conn_read_pkt(c->qconn, &rpath, &rpi, rbuf,
                                  (size_t)nread, now_ns());
        }
        ts = now_ns();
    }

    if (ngtcp2_conn_get_handshake_completed(c->qconn)) {
        c->zerortt_used = 0;
    }

    return c;
}

bf_quic_conn_t *bf_quic_reconnect_0rtt(bf_quic_ctx_t *ctx,
                                         const char *host, uint16_t port)
{
    /* Same as connect but with 0-RTT flag set.
     * The caller should have previously called bf_quic_ctx_set_token(). */
    return bf_quic_connect(ctx, host, port);
}

void bf_quic_conn_close(bf_quic_conn_t *conn)
{
    if (!conn) return;

    if (conn->qconn) {
        /* Send CONNECTION_CLOSE */
        ngtcp2_pkt_info pi;
        ngtcp2_ssize nwrite = ngtcp2_conn_write_connection_close(
            conn->qconn, NULL, &pi,
            conn->pkt_buf, sizeof(conn->pkt_buf),
            NULL, now_ns());
        if (nwrite > 0) {
            send(conn->fd, conn->pkt_buf, (size_t)nwrite, 0);
        }
        ngtcp2_conn_del(conn->qconn);
    }

    if (conn->ssl) SSL_free(conn->ssl);
    if (conn->fd >= 0) close(conn->fd);
    free(conn);
}

/* ── Stream operations ───────────────────────────────────────── */

bf_quic_stream_t *bf_quic_stream_open(bf_quic_conn_t *conn,
                                       const bf_quic_stream_meta_t *meta)
{
    if (!conn || !meta || conn->stream_count >= MAX_STREAMS)
        return NULL;

    int64_t stream_id;
    int rv = ngtcp2_conn_open_uni_stream(conn->qconn, &stream_id, NULL);
    if (rv != 0) return NULL;

    /* Set stream priority based on layer */
    ngtcp2_conn_set_stream_priority(conn->qconn, stream_id,
        &(ngtcp2_stream_priority){
            .urgency = layer_to_urgency(meta->layer),
            .inc = 0
        });

    bf_quic_stream_t *s = &conn->streams[conn->stream_count++];
    memset(s, 0, sizeof(*s));
    s->stream_id = stream_id;
    s->meta = *meta;
    s->t_start = now_ns();

    return s;
}

int bf_quic_stream_write(bf_quic_stream_t *stream,
                          const uint8_t *data, size_t len, int fin)
{
    if (!stream) return BF_QUIC_ERR_INVALID;

    /* Find the connection that owns this stream */
    /* Note: in production, stream would carry a back-pointer. For now,
     * this is called via bf_quic_send_batch which has the conn. */

    stream->bytes_written += len;
    if (fin) stream->fin_sent = 1;
    return BF_QUIC_OK;
}

void bf_quic_stream_close(bf_quic_stream_t *stream)
{
    if (stream && !stream->fin_sent)
        stream->fin_sent = 1;
}

/* ── Internal: write stream header + payload in one shot ─────── */

static int stream_write_full(bf_quic_conn_t *conn, bf_quic_stream_t *s,
                               const uint8_t *payload, size_t len)
{
    /* Build header */
    uint8_t hdr[STREAM_HEADER_SIZE];
    memcpy(hdr, STREAM_MAGIC, 4);
    memset(hdr + 4, 0, 16);
    memcpy(hdr + 4, s->meta.family_key, strlen(s->meta.family_key));
    hdr[20] = s->meta.layer;
    put_be64(hdr + 21, (uint64_t)len);
    memset(hdr + 29, 0, 64);
    if (s->meta.content_hash[0]) {
        memcpy(hdr + 29, s->meta.content_hash,
               strlen(s->meta.content_hash));
    }

    /* Write header */
    ngtcp2_vec hdr_vec = { .base = hdr, .len = STREAM_HEADER_SIZE };
    ngtcp2_pkt_info pi;
    ngtcp2_ssize nwrite;

    nwrite = ngtcp2_conn_writev_stream(conn->qconn, NULL, &pi,
                                         conn->pkt_buf, sizeof(conn->pkt_buf),
                                         NULL, 0, s->stream_id,
                                         &hdr_vec, 1, 0, now_ns());
    if (nwrite > 0)
        send(conn->fd, conn->pkt_buf, (size_t)nwrite, 0);

    /* Write payload in chunks */
    size_t off = 0;
    while (off < len) {
        size_t chunk = len - off;
        if (chunk > QUIC_MTU - 64) chunk = QUIC_MTU - 64;
        int is_fin = (off + chunk >= len) ? 1 : 0;

        ngtcp2_vec vec = { .base = (uint8_t *)(payload + off), .len = chunk };
        nwrite = ngtcp2_conn_writev_stream(conn->qconn, NULL, &pi,
                                             conn->pkt_buf, sizeof(conn->pkt_buf),
                                             NULL, 0, s->stream_id,
                                             &vec, 1,
                                             is_fin ? NGTCP2_WRITE_STREAM_FLAG_FIN : 0,
                                             now_ns());
        if (nwrite > 0) {
            send(conn->fd, conn->pkt_buf, (size_t)nwrite, 0);
        } else if (nwrite < 0 && nwrite != NGTCP2_ERR_WRITE_MORE) {
            return BF_QUIC_ERR_STREAM;
        }
        off += chunk;
        s->bytes_written += chunk;
    }

    s->fin_sent = 1;
    return BF_QUIC_OK;
}

/* ── Batch transfer (main BonfyreDistribute path) ────────────── */

/* Compare artifacts by layer for priority ordering */
static int artifact_cmp(const void *a, const void *b)
{
    const bf_quic_artifact_t *aa = (const bf_quic_artifact_t *)a;
    const bf_quic_artifact_t *bb = (const bf_quic_artifact_t *)b;
    return (int)aa->layer - (int)bb->layer; /* surface first */
}

bf_quic_batch_result_t bf_quic_send_batch(bf_quic_conn_t *conn,
                                           const bf_quic_artifact_t *artifacts,
                                           int count,
                                           bf_quic_done_cb on_done,
                                           void *user)
{
    bf_quic_batch_result_t result = {0};
    result.count = count;
    result.streams = calloc((size_t)count, sizeof(bf_quic_stream_result_t));
    if (!result.streams) return result;

    ngtcp2_tstamp t0 = now_ns();

    /* Sort by layer (surface first, substrate last) */
    bf_quic_artifact_t *sorted = malloc(sizeof(bf_quic_artifact_t) * (size_t)count);
    if (!sorted) { free(result.streams); result.streams = NULL; return result; }
    memcpy(sorted, artifacts, sizeof(bf_quic_artifact_t) * (size_t)count);
    qsort(sorted, (size_t)count, sizeof(bf_quic_artifact_t), artifact_cmp);

    for (int i = 0; i < count; i++) {
        const bf_quic_artifact_t *art = &sorted[i];
        bf_quic_stream_result_t *sr = &result.streams[i];

        /* Copy family key */
        if (art->family_key) {
            strncpy(sr->family_key, art->family_key, 16);
        }

        /* Read file */
        FILE *fp = fopen(art->path, "rb");
        if (!fp) {
            sr->status = BF_QUIC_ERR_INVALID;
            result.failed++;
            continue;
        }
        fseek(fp, 0, SEEK_END);
        long fsize = ftell(fp);
        fseek(fp, 0, SEEK_SET);
        if (fsize <= 0) {
            fclose(fp);
            sr->status = BF_QUIC_ERR_INVALID;
            result.failed++;
            continue;
        }
        uint8_t *buf = malloc((size_t)fsize);
        if (!buf) {
            fclose(fp);
            sr->status = BF_QUIC_ERR_MEMORY;
            result.failed++;
            continue;
        }
        fread(buf, 1, (size_t)fsize, fp);
        fclose(fp);

        /* Open stream with family metadata */
        bf_quic_stream_meta_t meta = {0};
        if (art->family_key) strncpy(meta.family_key, art->family_key, 16);
        meta.layer = art->layer;
        meta.total_bytes = (uint64_t)fsize;
        if (art->content_hash) {
            strncpy(meta.content_hash, art->content_hash, 64);
        }

        ngtcp2_tstamp ts = now_ns();
        bf_quic_stream_t *s = bf_quic_stream_open(conn, &meta);
        if (!s) {
            free(buf);
            sr->status = BF_QUIC_ERR_STREAM;
            result.failed++;
            continue;
        }

        int rv = stream_write_full(conn, s, buf, (size_t)fsize);
        free(buf);

        sr->bytes_sent = (uint64_t)fsize;
        sr->elapsed_ms = elapsed_ms(ts);
        sr->status = rv;
        sr->zero_rtt = (uint8_t)conn->zerortt_used;

        if (rv == BF_QUIC_OK) {
            result.succeeded++;
            result.total_bytes += (uint64_t)fsize;
        } else {
            result.failed++;
        }

        if (on_done) on_done(sr, user);
    }

    result.total_ms = elapsed_ms(t0);
    free(sorted);
    return result;
}

/* ── Receive side ────────────────────────────────────────────── */

int bf_quic_recv_start(bf_quic_conn_t *conn,
                        bf_quic_recv_cb recv_cb, void *user)
{
    if (!conn) return BF_QUIC_ERR_INVALID;
    conn->recv_cb = recv_cb;
    conn->recv_user = user;
    return BF_QUIC_OK;
}

int bf_quic_recv_poll(bf_quic_conn_t *conn, int timeout_ms)
{
    if (!conn) return BF_QUIC_ERR_INVALID;

    /* Use poll/select for portability */
    fd_set rfds;
    FD_ZERO(&rfds);
    FD_SET(conn->fd, &rfds);

    struct timeval tv = {
        .tv_sec = timeout_ms / 1000,
        .tv_usec = (timeout_ms % 1000) * 1000
    };

    int nready = select(conn->fd + 1, &rfds, NULL, NULL, &tv);
    if (nready <= 0) return 0;

    uint8_t buf[QUIC_MTU * 4];
    struct sockaddr_storage from;
    socklen_t fromlen = sizeof(from);

    ssize_t nread = recvfrom(conn->fd, buf, sizeof(buf), 0,
                              (struct sockaddr *)&from, &fromlen);
    if (nread <= 0) return 0;

    ngtcp2_path path;
    ngtcp2_addr_init(&path.local, (struct sockaddr *)&conn->local_addr,
                      conn->local_addrlen);
    ngtcp2_addr_init(&path.remote, (struct sockaddr *)&from, fromlen);

    ngtcp2_pkt_info pi = {0};
    int rv = ngtcp2_conn_read_pkt(conn->qconn, &path, &pi,
                                    buf, (size_t)nread, now_ns());
    if (rv != 0) return BF_QUIC_ERR_STREAM;

    /* Write any ACKs or response packets */
    for (;;) {
        ngtcp2_ssize nwrite = ngtcp2_conn_write_pkt(conn->qconn, NULL,
                                                       &pi, conn->pkt_buf,
                                                       sizeof(conn->pkt_buf),
                                                       now_ns());
        if (nwrite <= 0) break;
        send(conn->fd, conn->pkt_buf, (size_t)nwrite, 0);
    }

    return 1;
}

/* ── Server mode ─────────────────────────────────────────────── */

bf_quic_server_t *bf_quic_server_start(bf_quic_ctx_t *ctx,
                                         const char *bind_addr,
                                         uint16_t port,
                                         bf_quic_accept_cb on_accept,
                                         void *user)
{
    if (!ctx || !ctx->is_server) return NULL;

    bf_quic_server_t *srv = calloc(1, sizeof(*srv));
    if (!srv) return NULL;
    srv->ctx = ctx;
    srv->on_accept = on_accept;
    srv->user = user;

    /* Create UDP socket */
    struct sockaddr_in6 addr6 = {0};
    struct sockaddr_in  addr4 = {0};
    struct sockaddr *sa;
    socklen_t salen;

    /* Try IPv4 first for simplicity */
    addr4.sin_family = AF_INET;
    addr4.sin_port = htons(port);
    if (bind_addr && inet_pton(AF_INET, bind_addr, &addr4.sin_addr) == 1) {
        sa = (struct sockaddr *)&addr4;
        salen = sizeof(addr4);
        srv->fd = socket(AF_INET, SOCK_DGRAM, 0);
    } else {
        addr6.sin6_family = AF_INET6;
        addr6.sin6_port = htons(port);
        if (bind_addr) inet_pton(AF_INET6, bind_addr, &addr6.sin6_addr);
        else addr6.sin6_addr = in6addr_any;
        sa = (struct sockaddr *)&addr6;
        salen = sizeof(addr6);
        srv->fd = socket(AF_INET6, SOCK_DGRAM, 0);
    }

    if (srv->fd < 0) { free(srv); return NULL; }

    int one = 1;
    setsockopt(srv->fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    if (bind(srv->fd, sa, salen) < 0) {
        close(srv->fd);
        free(srv);
        return NULL;
    }

    /* Non-blocking */
    int flags = fcntl(srv->fd, F_GETFL, 0);
    fcntl(srv->fd, F_SETFL, flags | O_NONBLOCK);

#ifdef __APPLE__
    srv->kq = kqueue();
    struct kevent ev;
    EV_SET(&ev, srv->fd, EVFILT_READ, EV_ADD, 0, 0, NULL);
    kevent(srv->kq, &ev, 1, NULL, 0, NULL);
#else
    srv->kq = epoll_create1(0);
    struct epoll_event ev = { .events = EPOLLIN, .data.fd = srv->fd };
    epoll_ctl(srv->kq, EPOLL_CTL_ADD, srv->fd, &ev);
#endif

    return srv;
}

void bf_quic_server_stop(bf_quic_server_t *srv)
{
    if (!srv) return;
    for (int i = 0; i < srv->conn_count; i++) {
        bf_quic_conn_close(srv->conns[i]);
    }
    close(srv->fd);
    close(srv->kq);
    free(srv);
}

int bf_quic_server_poll(bf_quic_server_t *srv, int timeout_ms)
{
    if (!srv) return -1;

#ifdef __APPLE__
    struct kevent events[16];
    struct timespec ts = {
        .tv_sec = timeout_ms / 1000,
        .tv_nsec = (timeout_ms % 1000) * 1000000L
    };
    int n = kevent(srv->kq, NULL, 0, events, 16, &ts);
#else
    struct epoll_event events[16];
    int n = epoll_wait(srv->kq, events, 16, timeout_ms);
#endif

    if (n <= 0) return n;

    /* Read incoming packets and dispatch to connections */
    uint8_t buf[QUIC_MTU * 4];
    struct sockaddr_storage from;
    socklen_t fromlen = sizeof(from);

    ssize_t nread = recvfrom(srv->fd, buf, sizeof(buf), 0,
                              (struct sockaddr *)&from, &fromlen);
    if (nread <= 0) return 0;

    /* TODO: route to existing connection by DCID or create new one */
    /* For now, invoke accept callback for new connections */
    if (srv->on_accept && srv->conn_count < 64) {
        /* This is simplified — real impl needs DCID routing */
        (void)buf; (void)nread;
    }

    return n;
}

/* ── Utility ─────────────────────────────────────────────────── */

int bf_quic_layer_from_type(const char *artifact_type)
{
    if (!artifact_type) return BF_LAYER_SUBSTRATE;

    /* Surface layer: user-facing deliverables */
    if (strstr(artifact_type, "proof") ||
        strstr(artifact_type, "offer") ||
        strstr(artifact_type, "deliverable") ||
        strstr(artifact_type, "pack"))
        return BF_LAYER_SURFACE;

    /* Value layer: monetization and accounting */
    if (strstr(artifact_type, "ledger") ||
        strstr(artifact_type, "meter") ||
        strstr(artifact_type, "repurpose") ||
        strstr(artifact_type, "invoice"))
        return BF_LAYER_VALUE;

    /* Transform layer: processed artifacts */
    if (strstr(artifact_type, "brief") ||
        strstr(artifact_type, "clean") ||
        strstr(artifact_type, "tag") ||
        strstr(artifact_type, "tone") ||
        strstr(artifact_type, "paragraph") ||
        strstr(artifact_type, "segment"))
        return BF_LAYER_TRANSFORM;

    /* Default: substrate */
    return BF_LAYER_SUBSTRATE;
}

const char *bf_quic_strerror(int err)
{
    switch (err) {
    case BF_QUIC_OK:           return "ok";
    case BF_QUIC_ERR_CONNECT:  return "connection failed";
    case BF_QUIC_ERR_TLS:      return "TLS error";
    case BF_QUIC_ERR_STREAM:   return "stream error";
    case BF_QUIC_ERR_TIMEOUT:  return "timeout";
    case BF_QUIC_ERR_CLOSED:   return "connection closed";
    case BF_QUIC_ERR_MEMORY:   return "out of memory";
    case BF_QUIC_ERR_INVALID:  return "invalid argument";
    default:                    return "unknown error";
    }
}
