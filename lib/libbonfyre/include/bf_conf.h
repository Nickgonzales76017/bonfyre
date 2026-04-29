/*
 * bf_conf.h — INI + TOML configuration file parser
 *
 * Supports:
 *   - INI-style:    [section] key = value
 *   - TOML basics:  [section] key = "string" / 42 / 3.14 / true
 *   - Inline comments (#, ;)
 *   - Environment variable fallback: $ENV_VAR or ${ENV_VAR}
 *   - Layered loading: system → user → local → env override
 *
 * Standard search order:
 *   1. /etc/bonfyre/<name>.conf
 *   2. ~/.config/bonfyre/<name>.conf
 *   3. ./<name>.conf  (or explicit path)
 *   4. Environment variables: BONFYRE_<SECTION>_<KEY> (uppercased)
 *
 * Usage:
 *   bf_conf_t *cfg = bf_conf_load("pipeline");  // searches standard paths
 *   const char *out = bf_conf_str(cfg, "output", "dir", "/tmp");
 *   int workers    = bf_conf_int(cfg, "pool", "workers", 4);
 *   bf_conf_free(cfg);
 */

#ifndef BF_CONF_H
#define BF_CONF_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Types ───────────────────────────────── */

typedef struct bf_conf bf_conf_t;

/* ── Lifecycle ───────────────────────────── */

/*
 * Load configuration by name, searching standard paths.
 * Returns NULL on allocation failure (missing files are not errors).
 */
bf_conf_t *bf_conf_load(const char *name);

/*
 * Load configuration from a specific file path.
 * Returns NULL on failure.
 */
bf_conf_t *bf_conf_load_file(const char *path);

/*
 * Create an empty configuration (for programmatic use).
 */
bf_conf_t *bf_conf_new(void);

/*
 * Free all resources.
 */
void bf_conf_free(bf_conf_t *cfg);

/* ── Typed getters (with defaults) ───────── */

const char *bf_conf_str(const bf_conf_t *cfg,
                        const char *section, const char *key,
                        const char *def);

int bf_conf_int(const bf_conf_t *cfg,
                const char *section, const char *key,
                int def);

double bf_conf_double(const bf_conf_t *cfg,
                      const char *section, const char *key,
                      double def);

int bf_conf_bool(const bf_conf_t *cfg,
                 const char *section, const char *key,
                 int def);

/* ── Setters ─────────────────────────────── */

void bf_conf_set(bf_conf_t *cfg,
                 const char *section, const char *key,
                 const char *value);

/* ── Enumeration ─────────────────────────── */

/*
 * Callback for bf_conf_each(). Return 0 to continue, non-zero to stop.
 */
typedef int (*bf_conf_visitor_fn)(const char *section, const char *key,
                                  const char *value, void *userdata);

void bf_conf_each(const bf_conf_t *cfg, bf_conf_visitor_fn fn, void *userdata);

/* ── Save ────────────────────────────────── */

/*
 * Write configuration to a file in INI format.
 * Returns 0 on success, -1 on error.
 */
int bf_conf_save(const bf_conf_t *cfg, const char *path);

#ifdef __cplusplus
}
#endif

#endif /* BF_CONF_H */
