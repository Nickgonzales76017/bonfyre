/*
 * bf_hotload.c — Hot-reload pipeline stages via dlopen
 *
 * Thread-safe plugin loader with file-watcher for automatic hot-swap.
 * macOS: kqueue on plugin_dir
 * Linux: inotify on plugin_dir
 */

#include "bf_hotload.h"

#include <dlfcn.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#ifdef __APPLE__
#include <sys/event.h>
#define HOTLOAD_KQUEUE 1
#define PLUGIN_EXT ".dylib"
#elif defined(__linux__)
#include <sys/inotify.h>
#define HOTLOAD_INOTIFY 1
#define PLUGIN_EXT ".so"
#endif

/* ── Internal stage record ───────────────────────────────────── */

struct bf_loaded_stage {
    char                path[4096];
    char                stage_name[64];
    void               *handle;         /* dlopen handle              */
    bf_stage_entry_fn   entry;
    bf_stage_cleanup_fn cleanup;        /* may be NULL                */
    bf_stage_meta_t     meta;           /* copy of exported metadata  */
    time_t              load_time;
    uint64_t            exec_count;
    double              total_ms;
};

/* ── A/B test pair ───────────────────────────────────────────── */

typedef struct {
    char    stage_name[64];
    char    a_name[64];   /* stage_name        */
    char    b_name[80];   /* stage_name__b     */
} bf_ab_pair_t;

/* ── Registry ────────────────────────────────────────────────── */

struct bf_hotload_registry {
    bf_loaded_stage_t   stages[BF_HOTLOAD_MAX_STAGES];
    int                 n_stages;

    bf_ab_pair_t        ab_pairs[16];
    int                 n_ab;

    char                plugin_dir[4096];
    void               *user_data;

    /* File-watcher */
    int                 watch_fd;
    volatile int        watcher_running;
    pthread_t           watcher_thread;
    pthread_mutex_t     lock;
};

/* ── FNV-1a hash (for A/B routing) ───────────────────────────── */

static uint32_t fnv1a(const char *s) {
    uint32_t h = 2166136261u;
    if (!s) return h;
    for (; *s; s++) {
        h ^= (uint8_t)*s;
        h *= 16777619u;
    }
    return h;
}

/* ── Find stage by name ──────────────────────────────────────── */

static bf_loaded_stage_t *find_stage(bf_hotload_registry_t *reg,
                                      const char *name) {
    for (int i = 0; i < reg->n_stages; i++) {
        if (strcmp(reg->stages[i].stage_name, name) == 0)
            return &reg->stages[i];
    }
    return NULL;
}

/* ── Unload a single stage ───────────────────────────────────── */

static void unload_stage(bf_loaded_stage_t *s) {
    if (s->cleanup) s->cleanup();
    if (s->handle) dlclose(s->handle);
    s->handle = NULL;
    s->entry = NULL;
    s->cleanup = NULL;
}

/* ── Load a single plugin ────────────────────────────────────── */

static int load_plugin(bf_hotload_registry_t *reg, const char *path,
                        bf_loaded_stage_t *slot) {
    void *handle = dlopen(path, RTLD_NOW | RTLD_LOCAL);
    if (!handle) {
        fprintf(stderr, "[hotload] dlopen(%s): %s\n", path, dlerror());
        return -1;
    }

    /* Validate metadata */
    const bf_stage_meta_t *meta = dlsym(handle, "bf_stage_meta");
    if (!meta) {
        fprintf(stderr, "[hotload] %s: missing bf_stage_meta symbol\n", path);
        dlclose(handle);
        return -1;
    }

    if (meta->abi_version != BF_STAGE_ABI_VERSION) {
        fprintf(stderr, "[hotload] %s: ABI version %d != expected %d\n",
                path, meta->abi_version, BF_STAGE_ABI_VERSION);
        dlclose(handle);
        return -1;
    }

    if (!meta->stage_name || strlen(meta->stage_name) == 0) {
        fprintf(stderr, "[hotload] %s: empty stage_name in metadata\n", path);
        dlclose(handle);
        return -1;
    }

    /* Find entry point */
    bf_stage_entry_fn entry = (bf_stage_entry_fn)dlsym(handle, "bf_stage_entry");
    if (!entry) {
        fprintf(stderr, "[hotload] %s: missing bf_stage_entry symbol\n", path);
        dlclose(handle);
        return -1;
    }

    /* Optional cleanup */
    bf_stage_cleanup_fn cleanup =
        (bf_stage_cleanup_fn)dlsym(handle, "bf_stage_cleanup");

    /* Fill slot */
    snprintf(slot->path, sizeof(slot->path), "%s", path);
    snprintf(slot->stage_name, sizeof(slot->stage_name), "%s", meta->stage_name);
    slot->handle = handle;
    slot->entry = entry;
    slot->cleanup = cleanup;
    slot->meta = *meta;
    slot->load_time = time(NULL);
    slot->exec_count = 0;
    slot->total_ms = 0;

    fprintf(stderr, "[hotload] loaded '%s' v%s from %s\n",
            meta->stage_name, meta->version ? meta->version : "?", path);

    return 0;
}

/* ── File-watcher thread ─────────────────────────────────────── */

static void try_reload_file(bf_hotload_registry_t *reg, const char *filename) {
    /* Only process plugin files */
    const char *ext = strrchr(filename, '.');
    if (!ext) return;
    if (strcmp(ext, PLUGIN_EXT) != 0) return;

    char full_path[4096];
    snprintf(full_path, sizeof(full_path), "%s/%s",
             reg->plugin_dir, filename);

    /* Small delay for file to finish writing */
    usleep(100000);

    pthread_mutex_lock(&reg->lock);

    /* Check if already loaded with same name — reload it */
    /* Extract expected stage name from filename: stage_<name>.dylib */
    for (int i = 0; i < reg->n_stages; i++) {
        if (strstr(reg->stages[i].path, filename)) {
            fprintf(stderr, "[hotload] reloading '%s'\n",
                    reg->stages[i].stage_name);
            unload_stage(&reg->stages[i]);
            load_plugin(reg, full_path, &reg->stages[i]);
            pthread_mutex_unlock(&reg->lock);
            return;
        }
    }

    /* New plugin — add it */
    if (reg->n_stages < BF_HOTLOAD_MAX_STAGES) {
        bf_loaded_stage_t *slot = &reg->stages[reg->n_stages];
        if (load_plugin(reg, full_path, slot) == 0) {
            reg->n_stages++;
        }
    }

    pthread_mutex_unlock(&reg->lock);
}

#ifdef HOTLOAD_KQUEUE
static void *watcher_thread_fn(void *arg) {
    bf_hotload_registry_t *reg = (bf_hotload_registry_t *)arg;

    int dirfd = open(reg->plugin_dir, O_RDONLY);
    if (dirfd < 0) return NULL;

    struct kevent ev;
    EV_SET(&ev, dirfd, EVFILT_VNODE,
           EV_ADD | EV_ENABLE | EV_CLEAR,
           NOTE_WRITE | NOTE_RENAME, 0, NULL);
    kevent(reg->watch_fd, &ev, 1, NULL, 0, NULL);

    while (reg->watcher_running) {
        struct kevent out;
        struct timespec ts = { .tv_sec = 1, .tv_nsec = 0 };
        int n = kevent(reg->watch_fd, NULL, 0, &out, 1, &ts);
        if (n > 0) {
            /* Directory changed — scan for new/modified plugins */
            DIR *d = opendir(reg->plugin_dir);
            if (d) {
                struct dirent *ent;
                while ((ent = readdir(d)) != NULL) {
                    try_reload_file(reg, ent->d_name);
                }
                closedir(d);
            }
        }
    }

    close(dirfd);
    return NULL;
}
#elif defined(HOTLOAD_INOTIFY)
static void *watcher_thread_fn(void *arg) {
    bf_hotload_registry_t *reg = (bf_hotload_registry_t *)arg;

    int wd = inotify_add_watch(reg->watch_fd, reg->plugin_dir,
                                IN_CLOSE_WRITE | IN_MOVED_TO);
    if (wd < 0) return NULL;

    char buf[4096]
        __attribute__((aligned(__alignof__(struct inotify_event))));

    while (reg->watcher_running) {
        ssize_t len = read(reg->watch_fd, buf, sizeof(buf));
        if (len <= 0) {
            usleep(100000);
            continue;
        }

        const struct inotify_event *event;
        for (char *ptr = buf; ptr < buf + len;
             ptr += sizeof(*event) + event->len) {
            event = (const struct inotify_event *)ptr;
            if (event->len > 0 && event->name[0] != '.') {
                try_reload_file(reg, event->name);
            }
        }
    }

    inotify_rm_watch(reg->watch_fd, wd);
    return NULL;
}
#endif

/* ── Public API ──────────────────────────────────────────────── */

bf_hotload_registry_t *bf_hotload_create(const char *plugin_dir, int watch) {
    bf_hotload_registry_t *reg = calloc(1, sizeof(*reg));
    if (!reg) return NULL;

    snprintf(reg->plugin_dir, sizeof(reg->plugin_dir), "%s", plugin_dir);
    pthread_mutex_init(&reg->lock, NULL);
    reg->watch_fd = -1;

    /* Scan directory for existing plugins */
    DIR *d = opendir(plugin_dir);
    if (d) {
        struct dirent *ent;
        while ((ent = readdir(d)) != NULL) {
            const char *ext = strrchr(ent->d_name, '.');
            if (!ext || strcmp(ext, PLUGIN_EXT) != 0) continue;

            char path[4096];
            snprintf(path, sizeof(path), "%s/%s", plugin_dir, ent->d_name);

            if (reg->n_stages < BF_HOTLOAD_MAX_STAGES) {
                bf_loaded_stage_t *slot = &reg->stages[reg->n_stages];
                if (load_plugin(reg, path, slot) == 0) {
                    reg->n_stages++;
                }
            }
        }
        closedir(d);
    }

    /* Start file watcher if requested */
    if (watch) {
#ifdef HOTLOAD_KQUEUE
        reg->watch_fd = kqueue();
#elif defined(HOTLOAD_INOTIFY)
        reg->watch_fd = inotify_init1(IN_NONBLOCK);
#endif
        if (reg->watch_fd >= 0) {
            reg->watcher_running = 1;
            pthread_create(&reg->watcher_thread, NULL,
                            watcher_thread_fn, reg);
        }
    }

    return reg;
}

void bf_hotload_destroy(bf_hotload_registry_t *reg) {
    if (!reg) return;

    /* Stop watcher */
    if (reg->watcher_running) {
        reg->watcher_running = 0;
        pthread_join(reg->watcher_thread, NULL);
    }
    if (reg->watch_fd >= 0) close(reg->watch_fd);

    /* Unload all plugins */
    for (int i = 0; i < reg->n_stages; i++) {
        unload_stage(&reg->stages[i]);
    }

    pthread_mutex_destroy(&reg->lock);
    free(reg);
}

int bf_hotload_load(bf_hotload_registry_t *reg, const char *path) {
    pthread_mutex_lock(&reg->lock);

    if (reg->n_stages >= BF_HOTLOAD_MAX_STAGES) {
        pthread_mutex_unlock(&reg->lock);
        return -1;
    }

    bf_loaded_stage_t *slot = &reg->stages[reg->n_stages];
    int rc = load_plugin(reg, path, slot);
    if (rc == 0) reg->n_stages++;

    pthread_mutex_unlock(&reg->lock);
    return rc;
}

int bf_hotload_reload(bf_hotload_registry_t *reg, const char *stage_name,
                       const char *new_path) {
    pthread_mutex_lock(&reg->lock);

    bf_loaded_stage_t *s = find_stage(reg, stage_name);
    if (!s) {
        pthread_mutex_unlock(&reg->lock);
        return -1;
    }

    const char *path = new_path ? new_path : s->path;

    /* Save stats */
    uint64_t prev_exec = s->exec_count;
    double prev_ms = s->total_ms;

    unload_stage(s);
    int rc = load_plugin(reg, path, s);

    if (rc == 0) {
        /* Carry forward cumulative stats */
        s->exec_count = prev_exec;
        s->total_ms = prev_ms;
    }

    pthread_mutex_unlock(&reg->lock);
    return rc;
}

const bf_loaded_stage_t *bf_hotload_get(const bf_hotload_registry_t *reg,
                                         const char *stage_name) {
    for (int i = 0; i < reg->n_stages; i++) {
        if (strcmp(reg->stages[i].stage_name, stage_name) == 0)
            return &reg->stages[i];
    }
    return NULL;
}

int bf_hotload_exec(bf_hotload_registry_t *reg, const char *stage_name,
                     bf_stage_ctx_t *ctx) {
    pthread_mutex_lock(&reg->lock);

    bf_loaded_stage_t *s = find_stage(reg, stage_name);
    if (!s || !s->entry) {
        pthread_mutex_unlock(&reg->lock);
        return -1;  /* No plugin; caller falls back to built-in */
    }

    bf_stage_entry_fn entry = s->entry;
    pthread_mutex_unlock(&reg->lock);

    /* Inject user data */
    ctx->user_data = reg->user_data;

    /* Execute with timing */
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    int rc = entry(ctx);

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double ms = ((double)(t1.tv_sec - t0.tv_sec) * 1000.0) +
                ((double)(t1.tv_nsec - t0.tv_nsec) / 1e6);
    ctx->elapsed_ms = ms;

    /* Update stats (lockless for counters — acceptable race) */
    s->exec_count++;
    s->total_ms += ms;

    return rc;
}

void bf_hotload_set_user_data(bf_hotload_registry_t *reg, void *data) {
    reg->user_data = data;
}

const bf_stage_meta_t *bf_hotload_meta(const bf_hotload_registry_t *reg,
                                        const char *stage_name) {
    const bf_loaded_stage_t *s = bf_hotload_get(reg, stage_name);
    return s ? &s->meta : NULL;
}

int bf_hotload_list(const bf_hotload_registry_t *reg,
                     const char **names, int max_names) {
    int count = 0;
    for (int i = 0; i < reg->n_stages && count < max_names; i++) {
        names[count++] = reg->stages[i].stage_name;
    }
    return count;
}

/* ── A/B testing ─────────────────────────────────────────────── */

int bf_hotload_ab_register(bf_hotload_registry_t *reg,
                            const char *stage_name,
                            const char *a_path,
                            const char *b_path) {
    if (reg->n_ab >= 16) return -1;

    /* Load A under its normal name */
    int rc = bf_hotload_load(reg, a_path);
    if (rc != 0) return rc;

    /* Load B — temporarily override stage_name in metadata check */
    if (reg->n_stages >= BF_HOTLOAD_MAX_STAGES) return -1;

    bf_loaded_stage_t *slot = &reg->stages[reg->n_stages];
    rc = load_plugin(reg, b_path, slot);
    if (rc != 0) return rc;

    /* Override the slot's stage_name to the __b variant */
    char b_name[80];
    snprintf(b_name, sizeof(b_name), "%s__b", stage_name);
    snprintf(slot->stage_name, sizeof(slot->stage_name), "%s", b_name);
    reg->n_stages++;

    /* Register A/B pair */
    bf_ab_pair_t *ab = &reg->ab_pairs[reg->n_ab++];
    snprintf(ab->stage_name, sizeof(ab->stage_name), "%s", stage_name);
    snprintf(ab->a_name, sizeof(ab->a_name), "%s", stage_name);
    snprintf(ab->b_name, sizeof(ab->b_name), "%s", b_name);

    fprintf(stderr, "[hotload] A/B registered for '%s': A=%s B=%s\n",
            stage_name, a_path, b_path);

    return 0;
}

int bf_hotload_ab_exec(bf_hotload_registry_t *reg, const char *stage_name,
                        bf_stage_ctx_t *ctx) {
    /* Find A/B pair */
    for (int i = 0; i < reg->n_ab; i++) {
        if (strcmp(reg->ab_pairs[i].stage_name, stage_name) == 0) {
            /* Route based on pipeline_id hash */
            uint32_t h = fnv1a(ctx->pipeline_id);
            const char *target = (h % 2 == 0)
                ? reg->ab_pairs[i].a_name
                : reg->ab_pairs[i].b_name;
            return bf_hotload_exec(reg, target, ctx);
        }
    }

    /* No A/B pair — fall through to normal exec */
    return bf_hotload_exec(reg, stage_name, ctx);
}
