/*
 * qwen_serve.h — Resident service for Qwen inference
 *
 * JSONL protocol over stdin/stdout for persistent model service.
 */
#pragma once

#include "qwen_runtime.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Request types */
typedef enum {
    QWEN_REQ_WARM,
    QWEN_REQ_HEALTH,
    QWEN_REQ_STATUS,
    QWEN_REQ_GENERATE,
    QWEN_REQ_GENERATE_FILE,
    QWEN_REQ_SHUTDOWN,
    QWEN_REQ_UNKNOWN
} qwen_request_type_t;

/* Request structure */
typedef struct {
    qwen_request_type_t type;
    char *prompt;
    char *prompt_path;
    char *out_path;
    char *request_id;
    int max_new_tokens;
    float temperature;
    char *mode;
} qwen_request_t;

/* Response structure */
typedef struct {
    int success;
    char *text;
    char *meta_json;
    char *error;
    int tokens_generated;
    int token_streams_opened;
    double time_seconds;
} qwen_response_t;

/* Run resident service */
int qwen_serve(qwen_runtime_t *rt);
int qwen_serve_quic(qwen_runtime_t *rt,
                    const char *bind_addr,
                    uint16_t port,
                    const char *cert_path,
                    const char *key_path);

/* Parse JSONL request */
qwen_request_t *qwen_parse_request(const char *line);

/* Free request */
void qwen_request_free(qwen_request_t *req);

/* Emit JSONL response */
void qwen_emit_response(const qwen_response_t *resp);

/* Free response */
void qwen_response_free(qwen_response_t *resp);

#ifdef __cplusplus
}
#endif
