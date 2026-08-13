/*
 * bf_log.h — Structured ring-buffer logger
 *
 * Features:
 *   - Lock-free per-thread ring buffers (no contention)
 *   - 6 severity levels: TRACE, DEBUG, INFO, WARN, ERROR, FATAL
 *   - ISO 8601 timestamps with microsecond precision
 *   - Pluggable sinks: stderr, file (with rotation), custom callback
 *   - Compile-time level filtering via BF_LOG_MIN_LEVEL
 *   - Structured key=value fields
 *
 * Usage:
 *   bf_log_init(BF_LOG_INFO, NULL);  // stderr at INFO+
 *   BF_LOG_INFO("msg=%s count=%d", "hello", 42);
 *   bf_log_shutdown();
 */

#ifndef BF_LOG_H
#define BF_LOG_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Severity levels ─────────────────────────────────────────── */

typedef enum {
    BF_LOG_LVL_TRACE = 0,
    BF_LOG_LVL_DEBUG = 1,
    BF_LOG_LVL_INFO  = 2,
    BF_LOG_LVL_WARN  = 3,
    BF_LOG_LVL_ERROR = 4,
    BF_LOG_LVL_FATAL = 5,
    BF_LOG_LVL_OFF   = 6
} bf_log_level_t;

/* ── Sink configuration ──────────────────────────────────────── */

/* Custom sink callback. level, timestamp, file, line, message. */
typedef void (*bf_log_sink_fn)(bf_log_level_t level,
                                const char *timestamp,
                                const char *file,
                                int line,
                                const char *message,
                                void *user_data);

typedef struct {
    const char    *file_path;      /* NULL = stderr only */
    size_t         max_file_bytes; /* Max bytes before rotation (0 = no limit) */
    int            max_files;      /* Number of rotated files to keep (default 3) */
    bf_log_sink_fn custom_sink;    /* Optional custom sink */
    void          *sink_user_data;
} bf_log_config_t;

/* ── Lifecycle ───────────────────────────────────────────────── */

/* Initialize the logger. level = minimum severity to emit.
 * config may be NULL for defaults (stderr, INFO). */
int bf_log_init(bf_log_level_t level, const bf_log_config_t *config);

/* Flush all ring buffers and close files. */
void bf_log_shutdown(void);

/* Change the runtime log level. */
void bf_log_set_level(bf_log_level_t level);

/* Get current log level. */
bf_log_level_t bf_log_get_level(void);

/* ── Core logging function ───────────────────────────────────── */

/* Not typically called directly — use macros below. */
void bf_log_write(bf_log_level_t level, const char *file, int line,
                  const char *fmt, ...)
#ifdef __GNUC__
    __attribute__((format(printf, 4, 5)))
#endif
;

/* ── Convenience macros ──────────────────────────────────────── */

#ifndef BF_LOG_MIN_LEVEL
#define BF_LOG_MIN_LEVEL BF_LOG_LVL_TRACE
#endif

#define BF_LOG_TRACE(...) do { \
    if (BF_LOG_LVL_TRACE >= BF_LOG_MIN_LEVEL) \
        bf_log_write(BF_LOG_LVL_TRACE, __FILE__, __LINE__, __VA_ARGS__); \
} while(0)

#define BF_LOG_DEBUG(...) do { \
    if (BF_LOG_LVL_DEBUG >= BF_LOG_MIN_LEVEL) \
        bf_log_write(BF_LOG_LVL_DEBUG, __FILE__, __LINE__, __VA_ARGS__); \
} while(0)

#define BF_LOG_INFO(...) do { \
    if (BF_LOG_LVL_INFO >= BF_LOG_MIN_LEVEL) \
        bf_log_write(BF_LOG_LVL_INFO, __FILE__, __LINE__, __VA_ARGS__); \
} while(0)

#define BF_LOG_WARN(...) do { \
    if (BF_LOG_LVL_WARN >= BF_LOG_MIN_LEVEL) \
        bf_log_write(BF_LOG_LVL_WARN, __FILE__, __LINE__, __VA_ARGS__); \
} while(0)

#define BF_LOG_ERROR(...) do { \
    if (BF_LOG_LVL_ERROR >= BF_LOG_MIN_LEVEL) \
        bf_log_write(BF_LOG_LVL_ERROR, __FILE__, __LINE__, __VA_ARGS__); \
} while(0)

#define BF_LOG_FATAL(...) do { \
    if (BF_LOG_LVL_FATAL >= BF_LOG_MIN_LEVEL) \
        bf_log_write(BF_LOG_LVL_FATAL, __FILE__, __LINE__, __VA_ARGS__); \
} while(0)

/* ── Flush ───────────────────────────────────────────────────── */

/* Force flush all buffered log entries to sinks. */
void bf_log_flush(void);

#ifdef __cplusplus
}
#endif

#endif /* BF_LOG_H */
