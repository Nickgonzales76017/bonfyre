/*
 * qwen_serve.c — Resident service for Qwen inference
 */
#include "qwen_serve.h"
#include <bonfyre.h>
#include <bf_json.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdint.h>

#ifdef BF_HAS_QUIC
#include <bf_quic.h>
static bf_quic_conn_t *g_qwen_quic_current_conn = NULL;
#endif

static char *qwen_json_dup_string(const bf_json_doc_t *doc,
                                  const bf_json_node_t *obj,
                                  const char *key) {
    const bf_json_node_t *n;
    int len = 0;
    const char *s;
    char *out;

    if (!doc || !obj || !key) return NULL;
    n = bf_json_obj_get(doc, obj, key);
    if (!n) return NULL;
    s = bf_json_get_str(n, &len);
    if (!s || len < 0) return NULL;
    out = (char *)malloc((size_t)len + 1);
    if (!out) return NULL;
    memcpy(out, s, (size_t)len);
    out[len] = '\0';
    return out;
}

qwen_request_t *qwen_parse_request(const char *line) {
    bf_json_doc_t *doc = NULL;
    const bf_json_node_t *root = NULL;
    const bf_json_node_t *type_node = NULL;
    char err[256];
    int int_value = 0;
    double double_value = 0.0;
    char type_buf[64];
    if (!line || !*line) return NULL;

    doc = bf_json_parse_str(line, err, sizeof(err));
    if (!doc) return NULL;
    root = bf_json_root(doc);
    if (!root || root->type != BF_JSON_OBJECT) {
        bf_json_free(doc);
        return NULL;
    }

    qwen_request_t *req = (qwen_request_t *)calloc(1, sizeof(*req));
    if (!req) {
        bf_json_free(doc);
        return NULL;
    }

    type_node = bf_json_obj_get(doc, root, "type");
    if (!type_node || bf_json_get_str_copy(type_node, type_buf, sizeof(type_buf)) <= 0) {
        bf_json_free(doc);
        free(req);
        return NULL;
    }

    if (strcmp(type_buf, "warm") == 0) {
        req->type = QWEN_REQ_WARM;
    } else if (strcmp(type_buf, "health") == 0) {
        req->type = QWEN_REQ_HEALTH;
    } else if (strcmp(type_buf, "status") == 0) {
        req->type = QWEN_REQ_STATUS;
    } else if (strcmp(type_buf, "generate") == 0) {
        req->type = QWEN_REQ_GENERATE;
        req->prompt = qwen_json_dup_string(doc, root, "prompt");
        req->mode = qwen_json_dup_string(doc, root, "mode");
        req->max_new_tokens = bf_json_int(line, "max_new_tokens", &int_value) ? int_value : 512;
        req->temperature = bf_json_double(line, "temperature", &double_value) ? (float)double_value : 0.2f;
        if (!req->mode) req->mode = strdup("code");
    } else if (strcmp(type_buf, "generate_file") == 0) {
        req->type = QWEN_REQ_GENERATE_FILE;
        req->prompt_path = qwen_json_dup_string(doc, root, "prompt_path");
        req->out_path = qwen_json_dup_string(doc, root, "out");
        req->mode = qwen_json_dup_string(doc, root, "mode");
        req->max_new_tokens = bf_json_int(line, "max_new_tokens", &int_value) ? int_value : 512;
        req->temperature = bf_json_double(line, "temperature", &double_value) ? (float)double_value : 0.2f;
        if (!req->mode) req->mode = strdup("code");
    } else if (strcmp(type_buf, "shutdown") == 0) {
        req->type = QWEN_REQ_SHUTDOWN;
    } else {
        req->type = QWEN_REQ_UNKNOWN;
    }

    req->request_id = qwen_json_dup_string(doc, root, "request_id");
    bf_json_free(doc);
    return req;
}

void qwen_request_free(qwen_request_t *req) {
    if (!req) return;
    free(req->prompt);
    free(req->prompt_path);
    free(req->out_path);
    free(req->request_id);
    free(req->mode);
    free(req);
}

void qwen_emit_response(const qwen_response_t *resp) {
    if (!resp) return;

    printf("{\"success\":%s", resp->success ? "true" : "false");

    if (resp->text) {
        /* Escape quotes in text */
        printf(",\"text\":\"");
        for (const char *p = resp->text; *p; p++) {
            if (*p == '"') printf("\\\"");
            else if (*p == '\n') printf("\\n");
            else if (*p == '\r') printf("\\r");
            else if (*p == '\t') printf("\\t");
            else if (*p == '\\') printf("\\\\");
            else putchar(*p);
        }
        printf("\"");
    }

    if (resp->meta_json) {
        printf(",\"meta\":%s", resp->meta_json);
    }

    if (resp->error) {
        printf(",\"error\":\"");
        for (const char *p = resp->error; *p; p++) {
            if (*p == '"') printf("\\\"");
            else if (*p == '\n') printf("\\n");
            else putchar(*p);
        }
        printf("\"");
    }

    printf(",\"tokens_generated\":%d", resp->tokens_generated);
    printf(",\"token_streams_opened\":%d", resp->token_streams_opened);
    printf(",\"time_seconds\":%.3f", resp->time_seconds);
    printf("}\n");
    fflush(stdout);
}

void qwen_response_free(qwen_response_t *resp) {
    if (!resp) return;
    free(resp->text);
    free(resp->meta_json);
    free(resp->error);
}

static char *qwen_response_to_json(const qwen_response_t *resp) {
    size_t cap = 4096 + (resp && resp->text ? strlen(resp->text) * 2 : 0) +
                 (resp && resp->meta_json ? strlen(resp->meta_json) : 0) +
                 (resp && resp->error ? strlen(resp->error) * 2 : 0);
    char *buf;
    size_t off = 0;
    const char *p;

    if (!resp) return NULL;
    buf = (char *)calloc(cap, 1);
    if (!buf) return NULL;
    off += (size_t)snprintf(buf + off, cap - off, "{\"success\":%s",
                            resp->success ? "true" : "false");
    if (resp->text) {
        off += (size_t)snprintf(buf + off, cap - off, ",\"text\":\"");
        for (p = resp->text; *p; p++) {
            if (off + 3 >= cap) break;
            if (*p == '"') { buf[off++] = '\\'; buf[off++] = '"'; }
            else if (*p == '\n') { buf[off++] = '\\'; buf[off++] = 'n'; }
            else if (*p == '\r') { buf[off++] = '\\'; buf[off++] = 'r'; }
            else if (*p == '\t') { buf[off++] = '\\'; buf[off++] = 't'; }
            else if (*p == '\\') { buf[off++] = '\\'; buf[off++] = '\\'; }
            else buf[off++] = *p;
        }
        buf[off++] = '"';
        buf[off] = '\0';
    }
    if (resp->meta_json) {
        off += (size_t)snprintf(buf + off, cap - off, ",\"meta\":%s", resp->meta_json);
    }
    if (resp->error) {
        off += (size_t)snprintf(buf + off, cap - off, ",\"error\":\"");
        for (p = resp->error; *p; p++) {
            if (off + 3 >= cap) break;
            if (*p == '"') { buf[off++] = '\\'; buf[off++] = '"'; }
            else if (*p == '\n') { buf[off++] = '\\'; buf[off++] = 'n'; }
            else if (*p == '\r') { buf[off++] = '\\'; buf[off++] = 'r'; }
            else if (*p == '\t') { buf[off++] = '\\'; buf[off++] = 't'; }
            else if (*p == '\\') { buf[off++] = '\\'; buf[off++] = '\\'; }
            else buf[off++] = *p;
        }
        buf[off++] = '"';
        buf[off] = '\0';
    }
    off += (size_t)snprintf(buf + off, cap - off, ",\"tokens_generated\":%d", resp->tokens_generated);
    off += (size_t)snprintf(buf + off, cap - off, ",\"token_streams_opened\":%d", resp->token_streams_opened);
    off += (size_t)snprintf(buf + off, cap - off, ",\"time_seconds\":%.3f}\n", resp->time_seconds);
    return buf;
}

static void qwen_emit_quic_meter_json(const qwen_request_t *req,
                                      const qwen_response_t *resp) {
    const char *path = getenv("BONFYRE_QWEN_QUIC_METER_JSON");
    FILE *fp;
    if (!path || !path[0] || !resp) return;
    if (bf_ensure_parent_dir(path) != 0) return;
    fp = fopen(path, "w");
    if (!fp) return;
    fprintf(fp,
            "{\n"
            "  \"schema_version\": \"bonfyre.qwen_fpq_quic_server_meter.v1\",\n"
            "  \"request_id\": \"%s\",\n"
            "  \"success\": %s,\n"
            "  \"tokens_generated\": %d,\n"
            "  \"token_streams_opened\": %d,\n"
            "  \"time_seconds\": %.6f,\n"
            "  \"request_type\": \"%s\"\n"
            "}\n",
            (req && req->request_id) ? req->request_id : "",
            resp->success ? "true" : "false",
            resp->tokens_generated,
            resp->token_streams_opened,
            resp->time_seconds,
            (req && req->type == QWEN_REQ_GENERATE) ? "generate" :
            (req && req->type == QWEN_REQ_GENERATE_FILE) ? "generate_file" :
            (req && req->type == QWEN_REQ_STATUS) ? "status" :
            (req && req->type == QWEN_REQ_WARM) ? "warm" : "other");
    fclose(fp);
}

#ifdef BF_HAS_QUIC
static void qwen_quic_send_json(bf_quic_conn_t *conn,
                                const char *family,
                                uint8_t layer,
                                const char *json) {
    bf_quic_stream_meta_t meta = {0};
    bf_quic_stream_t *stream;
    if (!conn || !json) return;
    snprintf(meta.family_key, sizeof(meta.family_key), "%s", family ? family : "qwen-response");
    meta.layer = layer;
    meta.total_bytes = (uint64_t)strlen(json);
    stream = bf_quic_stream_open(conn, &meta);
    if (!stream) return;
    (void)bf_quic_stream_write(stream, (const uint8_t *)json, strlen(json), 1);
    bf_quic_stream_close(stream);
}
#endif

typedef struct {
    char *text;
    size_t text_len;
    size_t text_cap;
    int token_count;
    int quic_stream_count;
#ifdef BF_HAS_QUIC
    bf_quic_conn_t *quic_conn;
    const char *request_id;
    bf_quic_stream_t *token_stream;
    int token_seq;
#endif
} generate_ctx_t;

static void generate_token_cb(int token_id, const char *text, void *user_data) {
    generate_ctx_t *ctx = (generate_ctx_t *)user_data;
    (void)token_id;
    if (!text) return;

    size_t len = strlen(text);
    if (ctx->text_len + len + 1 > ctx->text_cap) {
        ctx->text_cap = ctx->text_cap * 2 + len + 1024;
        ctx->text = (char *)realloc(ctx->text, ctx->text_cap);
    }

    memcpy(ctx->text + ctx->text_len, text, len);
    ctx->text_len += len;
    ctx->text[ctx->text_len] = '\0';
    ctx->token_count++;
#ifdef BF_HAS_QUIC
    if (ctx->quic_conn) {
        char line[1024];
        int n = snprintf(line, sizeof(line),
                         "{\"type\":\"token\",\"request_id\":\"%s\",\"sequence\":%d,\"token_id\":%d,\"text\":\"%s\"}\n",
                         ctx->request_id ? ctx->request_id : "", ctx->token_seq++, token_id, text);
        if (!ctx->token_stream) {
            bf_quic_stream_meta_t meta = {0};
            snprintf(meta.family_key, sizeof(meta.family_key), "%s", "qwen-token");
            meta.layer = BF_LAYER_VALUE;
            meta.total_bytes = 0;
            ctx->token_stream = bf_quic_stream_open(ctx->quic_conn, &meta);
            if (ctx->token_stream) ctx->quic_stream_count++;
        }
        if (ctx->token_stream && n > 0) {
            (void)bf_quic_stream_write(ctx->token_stream, (const uint8_t *)line, (size_t)n, 0);
        }
    }
#endif
}

static int qwen_handle_request(qwen_runtime_t *rt,
                               const qwen_config_t *base_config,
                               const qwen_request_t *req,
                               qwen_response_t *resp) {
    if (!rt || !base_config || !req || !resp) return -1;
    switch (req->type) {
        case QWEN_REQ_WARM: {
            int r = qwen_runtime_warm(rt);
            resp->success = (r == 0);
            if (r != 0) resp->error = strdup("Warm failed");
            return r;
        }
        case QWEN_REQ_HEALTH:
            resp->success = 1;
            resp->meta_json = strdup("{\"status\":\"ok\",\"service\":\"bonfyre-qwen-fpq\"}");
            return 0;
        case QWEN_REQ_STATUS:
            resp->success = 1;
            resp->meta_json = qwen_runtime_status_json(rt);
            return 0;
        case QWEN_REQ_GENERATE: {
            generate_ctx_t ctx = {0};
            int r;
            ctx.text_cap = 4096;
            ctx.text = (char *)malloc(ctx.text_cap);
            if (!ctx.text) {
                resp->success = 0;
                resp->error = strdup("OOM");
                return -1;
            }
            ctx.text[0] = '\0';
#ifdef BF_HAS_QUIC
            ctx.request_id = req->request_id;
            ctx.quic_conn = g_qwen_quic_current_conn;
#endif
            rt->config = *base_config;
            rt->config.max_new_tokens = req->max_new_tokens;
            rt->config.temperature = req->temperature;
            rt->config.mode = req->mode ? req->mode : base_config->mode;
            r = qwen_runtime_generate(rt, req->prompt, generate_token_cb, NULL, &ctx);
#ifdef BF_HAS_QUIC
            if (ctx.token_stream) {
                bf_quic_stream_close(ctx.token_stream);
                ctx.token_stream = NULL;
            }
#endif
            resp->success = (r == 0);
            resp->text = ctx.text;
            resp->tokens_generated = ctx.token_count;
            resp->token_streams_opened = ctx.quic_stream_count;
            if (r != 0) resp->error = strdup("Generation failed");
            return r;
        }
        case QWEN_REQ_GENERATE_FILE: {
            generate_ctx_t ctx = {0};
            int r;
            ctx.text_cap = 4096;
            ctx.text = (char *)malloc(ctx.text_cap);
            if (!ctx.text) {
                resp->success = 0;
                resp->error = strdup("OOM");
                return -1;
            }
            ctx.text[0] = '\0';
#ifdef BF_HAS_QUIC
            ctx.request_id = req->request_id;
            ctx.quic_conn = g_qwen_quic_current_conn;
#endif
            rt->config = *base_config;
            rt->config.max_new_tokens = req->max_new_tokens;
            rt->config.temperature = req->temperature;
            rt->config.mode = req->mode ? req->mode : base_config->mode;
            r = qwen_runtime_generate_file(rt, req->prompt_path, req->out_path,
                                           generate_token_cb, &ctx);
#ifdef BF_HAS_QUIC
            if (ctx.token_stream) {
                bf_quic_stream_close(ctx.token_stream);
                ctx.token_stream = NULL;
            }
#endif
            resp->success = (r == 0);
            resp->text = ctx.text;
            resp->tokens_generated = ctx.token_count;
            resp->token_streams_opened = ctx.quic_stream_count;
            if (r != 0) resp->error = strdup("File generation failed");
            return r;
        }
        case QWEN_REQ_SHUTDOWN:
            resp->success = 1;
            return 1;
        default:
            resp->success = 0;
            resp->error = strdup("Unknown request type");
            return -1;
    }
}

int qwen_serve(qwen_runtime_t *rt) {
    qwen_config_t base_config;
    if (!rt) return -1;

    base_config = rt->config;
    fprintf(stderr, "qwen_serve: starting service (JSONL over stdin/stdout)\n");
    fprintf(stderr, "qwen_serve: model loaded, ready for requests\n");

    char line[65536];

    while (fgets(line, sizeof(line), stdin)) {
        /* Strip newline */
        size_t len = strlen(line);
        while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r')) {
            line[--len] = '\0';
        }

        if (len == 0) continue;

        qwen_request_t *req = qwen_parse_request(line);
        if (!req) {
            qwen_response_t resp = {0};
            resp.success = 0;
            resp.error = strdup("Invalid request JSON");
            qwen_emit_response(&resp);
            qwen_response_free(&resp);
            continue;
        }

        qwen_response_t resp = {0};
        struct timespec t0, t1;
        clock_gettime(CLOCK_MONOTONIC, &t0);

        {
            int rc = qwen_handle_request(rt, &base_config, req, &resp);
            if (req->type == QWEN_REQ_SHUTDOWN) {
                resp.success = 1;
                qwen_emit_response(&resp);
                qwen_request_free(req);
                fprintf(stderr, "qwen_serve: shutdown requested\n");
                return 0;
            }
            (void)rc;
        }

        clock_gettime(CLOCK_MONOTONIC, &t1);
        resp.time_seconds = (t1.tv_sec - t0.tv_sec) +
                           (t1.tv_nsec - t0.tv_nsec) / 1e9;
        qwen_emit_quic_meter_json(req, &resp);

        qwen_emit_response(&resp);
        qwen_response_free(&resp);
        qwen_request_free(req);
    }

    fprintf(stderr, "qwen_serve: stdin closed, exiting\n");
    return 0;
}

#ifdef BF_HAS_QUIC
typedef struct {
    qwen_runtime_t *rt;
    qwen_config_t base_config;
    bf_quic_conn_t *conn;
    char *req_buf;
    size_t req_len;
    size_t req_cap;
} qwen_quic_session_t;

static void qwen_quic_recv_cb(const char *family_key,
                              const uint8_t *data, size_t len,
                              int fin, void *user) {
    qwen_quic_session_t *sess = (qwen_quic_session_t *)user;
    qwen_request_t *req;
    qwen_response_t resp = {0};
    char *json;
    struct timespec t0, t1;
    (void)family_key;

    if (!sess || !data || len == 0) return;
    if (sess->req_len + len + 1 > sess->req_cap) {
        size_t next_cap = sess->req_cap ? sess->req_cap * 2 : 8192;
        while (next_cap < sess->req_len + len + 1) next_cap *= 2;
        sess->req_buf = (char *)realloc(sess->req_buf, next_cap);
        sess->req_cap = next_cap;
    }
    if (!sess->req_buf) return;
    memcpy(sess->req_buf + sess->req_len, data, len);
    sess->req_len += len;
    sess->req_buf[sess->req_len] = '\0';
    if (!fin) return;

    req = qwen_parse_request(sess->req_buf);
    if (!req) {
        resp.success = 0;
        resp.error = strdup("Invalid request JSON");
    } else {
        clock_gettime(CLOCK_MONOTONIC, &t0);
        g_qwen_quic_current_conn = sess->conn;
        (void)qwen_handle_request(sess->rt, &sess->base_config, req, &resp);
        g_qwen_quic_current_conn = NULL;
        clock_gettime(CLOCK_MONOTONIC, &t1);
        resp.time_seconds = (t1.tv_sec - t0.tv_sec) +
                           (t1.tv_nsec - t0.tv_nsec) / 1e9;
    }

    json = qwen_response_to_json(&resp);
    if (json) {
        qwen_quic_send_json(sess->conn, "qwen-response", BF_LAYER_VALUE, json);
        free(json);
    }
    qwen_emit_quic_meter_json(req, &resp);
    qwen_response_free(&resp);
    qwen_request_free(req);
    sess->req_len = 0;
}

static void qwen_quic_accept_cb(bf_quic_conn_t *conn, void *user) {
    qwen_quic_session_t *sess = (qwen_quic_session_t *)user;
    if (!sess) return;
    sess->conn = conn;
    sess->req_len = 0;
    (void)bf_quic_recv_start(conn, qwen_quic_recv_cb, sess);
}

int qwen_serve_quic(qwen_runtime_t *rt,
                    const char *bind_addr,
                    uint16_t port,
                    const char *cert_path,
                    const char *key_path) {
    bf_quic_ctx_t *ctx;
    bf_quic_server_t *srv;
    qwen_quic_session_t sess;

    if (!rt) return -1;
    memset(&sess, 0, sizeof(sess));
    sess.rt = rt;
    sess.base_config = rt->config;

    ctx = bf_quic_ctx_new(cert_path, key_path, ".bonfyre-qwen-quic-ticket");
    if (!ctx) {
        fprintf(stderr, "qwen_serve_quic: failed to create QUIC context\n");
        return -1;
    }
    srv = bf_quic_server_start(ctx, bind_addr ? bind_addr : "0.0.0.0", port,
                               qwen_quic_accept_cb, &sess);
    if (!srv) {
        fprintf(stderr, "qwen_serve_quic: failed to start QUIC server on %s:%u\n",
                bind_addr ? bind_addr : "0.0.0.0", (unsigned)port);
        bf_quic_ctx_free(ctx);
        return -1;
    }
    fprintf(stderr, "qwen_serve_quic: listening on %s:%u\n",
            bind_addr ? bind_addr : "0.0.0.0", (unsigned)port);
    for (;;) {
        int rv = bf_quic_server_poll(srv, 100);
        if (rv < 0) break;
    }
    bf_quic_server_stop(srv);
    bf_quic_ctx_free(ctx);
    free(sess.req_buf);
    return 0;
}
#else
int qwen_serve_quic(qwen_runtime_t *rt,
                    const char *bind_addr,
                    uint16_t port,
                    const char *cert_path,
                    const char *key_path) {
    (void)rt;
    (void)bind_addr;
    (void)port;
    (void)cert_path;
    (void)key_path;
    fprintf(stderr, "qwen_serve_quic: QUIC support not built\n");
    return -1;
}
#endif
