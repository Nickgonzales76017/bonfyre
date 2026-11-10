#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif
#ifdef __APPLE__
#ifndef _DARWIN_C_SOURCE
#define _DARWIN_C_SOURCE
#endif
#endif

/*
 * residency.c — a model earns its memory per request.
 *
 * `bonfyre-moe serve` used to exec llama-server and hold the whole checkpoint
 * wired on the GPU for as long as launchd kept it alive: 5.5 GiB for three and
 * a half days with no request at all, on a 16 GiB machine already deep in swap.
 * Now this process owns the port and the model is a tenant behind it:
 *
 *   first request   ask ResourceAdmission for a memory grant, then load
 *   idle            unload after idle_seconds with no open connection, release
 *   no room         503 with the admission reason; nothing is loaded
 *
 * The relay is byte-for-byte TCP, so the OpenAI-compatible API, streaming and
 * every llama-server route keep working unchanged.
 */

#include "residency.h"

#include <bf_admission.h>
#include <bf_jsonw.h>

#include <arpa/inet.h>
#include <errno.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <spawn.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

extern char **environ;

static const MoeResidency *cfg;
static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static pid_t child;
static const char *current;   /* the checkpoint resident now, or the first choice */
static int current_choice = -1;
static int64_t current_bytes, current_context;
static BfModelShape shapes[32];
static int usable[32];        /* readable chat GGUF that has not failed to load */
static int order[32], candidates;
static time_t last_promotion_check;
static unsigned promotions;
static int64_t grant_id;
static int active;
static time_t last_activity;
static unsigned loads, unloads, refusals;
static char last_refusal[256];
static volatile sig_atomic_t stopping;

/* A caller needs to know whether waiting for memory can help.  A checkpoint
 * that llama-server cannot start is a different repair from an admission
 * deferral, and must not create an automatic retry loop. */
typedef struct MoeLoadFailure {
    const char *type;
    int retryable;
} MoeLoadFailure;

static const MoeLoadFailure failure_none = {"none", 0};
static const MoeLoadFailure failure_resource_retry = {"resource_unavailable", 1};
static const MoeLoadFailure failure_resource_final = {"resource_unavailable", 0};
static const MoeLoadFailure failure_model_unsupported = {"model_unsupported", 0};
static const MoeLoadFailure failure_backend_unavailable = {"local_backend_unavailable", 0};

/* A lease is a caller's batch holding the resident model: while any lease is
 * live, the model is neither promoted nor unloaded for idleness. A lease ends
 * when released or when its ttl passes without renewal, so a caller that
 * crashes cannot hold the memory for longer than one ttl. */
#define MAX_LEASES 16
#define MAX_LEASE_SECONDS 3600
static struct { int64_t id; time_t expires; } leases[MAX_LEASES];
static int64_t next_lease = 1;
static unsigned leases_granted;

static int leases_live_locked(void) {
    int live = 0;
    time_t now = time(NULL);
    for (int i = 0; i < MAX_LEASES; i++) {
        if (leases[i].id && leases[i].expires <= now) leases[i].id = 0;
        if (leases[i].id) live++;
    }
    return live;
}

static void say(const char *format, ...) {
    char stamp[32];
    time_t now = time(NULL);
    struct tm tm;
    va_list args;
    gmtime_r(&now, &tm);
    strftime(stamp, sizeof stamp, "%Y-%m-%dT%H:%M:%SZ", &tm);
    fprintf(stderr, "%s residency: ", stamp);
    va_start(args, format);
    vfprintf(stderr, format, args);
    va_end(args);
    fputc('\n', stderr);
}

static void on_signal(int signal_number) {
    (void)signal_number;
    stopping = 1;
}

/* The KV cache is q8_0: half the bytes of f16 for the same context. */
#define KV_ELEMENT_BYTES 1.0625

static int64_t measured_available(void) {
    if (cfg->memory_available_file) {
        FILE *file = fopen(cfg->memory_available_file, "r");
        long long value = -1;
        if (file) { if (fscanf(file, "%lld", &value) != 1) value = -1; fclose(file); }
        return value;
    }
    return cfg->memory_available > 0 ? cfg->memory_available : bf_admission_memory_available();
}

static int64_t estimate_bytes(int choice, int64_t context) {
    if (cfg->estimate_bytes > 0) return cfg->estimate_bytes;
    return bf_admission_model_bytes(&shapes[choice], context);
}

static int64_t context_for(int choice, int64_t spendable) {
    int64_t low = cfg->context_size ? cfg->context_size : 4096, high = cfg->max_context ? cfg->max_context : low;
    if (high < low) high = low;
    if (cfg->estimate_bytes > 0) return cfg->estimate_bytes <= spendable ? low : 0;
    return bf_admission_model_context(&shapes[choice], spendable, low, high);
}

static const char *base_name(const char *path) {
    const char *slash = strrchr(path, '/');
    return slash ? slash + 1 : path;
}

/* Memory a new load may spend: available now, less the floor, less what other
 * owners hold open on the ledger. reclaim adds what unloading ours would free. */
static int64_t spendable_now(int64_t reclaim, BfAdmissionPolicy *policy) {
    BfAdmissionGrants *grants = NULL;
    int64_t available = measured_available();
    int64_t committed = 0;
    if (available < 0) return -1;
    if (bf_admission_open(cfg->admission_db, &grants, stderr) == 0) {
        committed = bf_admission_committed_bytes(grants, BF_ADMISSION_MEMORY_VOLUME, NULL);
        bf_admission_close(grants);
    }
    bf_admission_policy_memory(policy);
    return available + reclaim - policy->protected_floor_bytes - (committed > 0 ? committed : 0);
}

static int connect_backend(void) {
    struct sockaddr_in address = {0};
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    address.sin_family = AF_INET;
    address.sin_port = htons(cfg->backend_port);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(fd, (struct sockaddr *)&address, sizeof address) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static int backend_ready(void) {
    char reply[64] = {0};
    const char *probe = "GET /health HTTP/1.0\r\nHost: 127.0.0.1\r\n\r\n";
    struct timeval timeout = {2, 0};
    int fd = connect_backend();
    ssize_t got;
    if (fd < 0) return 0;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof timeout);
    if (write(fd, probe, strlen(probe)) < 0) { close(fd); return 0; }
    got = read(fd, reply, sizeof reply - 1);
    close(fd);
    return got > 12 && strncmp(reply + 9, "200", 3) == 0;
}

static void release_grant_locked(void) {
    BfAdmissionGrants *grants = NULL;
    if (grant_id <= 0) return;
    if (bf_admission_open(cfg->admission_db, &grants, stderr) == 0) {
        bf_admission_release_grant(grants, grant_id, NULL);
        bf_admission_close(grants);
    }
    grant_id = 0;
}

static void reap_child_locked(void) {
    int status;
    if (child > 0 && waitpid(child, &status, WNOHANG) == child) {
        say("model process %d exited (%s %d); grant released", (int)child,
            WIFSIGNALED(status) ? "signal" : "status",
            WIFSIGNALED(status) ? WTERMSIG(status) : WEXITSTATUS(status));
        child = 0;
        release_grant_locked();
    }
}

static void stop_child_locked(const char *why) {
    int status;
    if (child <= 0) return;
    kill(child, SIGTERM);
    for (int waited = 0; waited < 100; waited++) {
        if (waitpid(child, &status, WNOHANG) == child) { child = 0; break; }
        usleep(100000);
    }
    if (child > 0) {
        kill(child, SIGKILL);
        waitpid(child, &status, 0);
        child = 0;
    }
    release_grant_locked();
    unloads++;
    say("unloaded %s (%s)", current, why);
}

/* Returns 0 when a model is resident; otherwise fills reason. With a
 * preference (a choice index, or -1), only that model is tried. */
static int ensure_loaded_locked(char *reason, size_t reason_size, int only, MoeLoadFailure *failure) {
    BfAdmissionGrants *grants = NULL;
    BfAdmissionPolicy policy;
    BfAdmissionDecision decision;
    BfResourceRequest request;
    BfAdmissionVerdict verdict;
    char port[16], layers[16], context[16];
    char *argv[24];
    char first_refusal[256] = "", first_unsupported[256] = "";
    int64_t need = -1, spendable, available, ctx = 0;
    int argc, rank, choice = -1, spawn_error;
    int resource_retryable = 0, saw_unsupported = 0;
    time_t deadline, began;

    *failure = failure_none;
    reap_child_locked();
    if (child > 0) return 0;
retry:
    available = measured_available();
    spendable = spendable_now(0, &policy);
    if (available < 0 || spendable < -((int64_t)1 << 40)) {
        *failure = failure_resource_retry;
        snprintf(reason, reason_size, "available memory could not be measured");
        goto refused;
    }
    if (bf_admission_open(cfg->admission_db, &grants, stderr) != 0) {
        *failure = failure_resource_retry;
        snprintf(reason, reason_size, "admission ledger could not be opened: %s", cfg->admission_db);
        goto refused;
    }
    /* Strongest first: each candidate gets the most context the memory allows,
     * and the first the ledger admits earns the load. */
    for (rank = 0; rank < candidates; rank++) {
        choice = order[rank];
        if (!usable[choice] || (only >= 0 && choice != only)) continue;
        ctx = context_for(choice, spendable);
        need = estimate_bytes(choice, ctx ? ctx : (cfg->context_size ? cfg->context_size : 4096));
        if (!ctx) {
            if (first_refusal[0]) continue;
            if (need > policy.max_grant_bytes)
                snprintf(first_refusal, sizeof first_refusal, "reject: %s needs %" PRId64 " MiB, more than this machine can give one model",
                         base_name(cfg->models[choice]), need >> 20);
            else {
                snprintf(first_refusal, sizeof first_refusal, "defer: %s needs %" PRId64 " MiB, %" PRId64 " MiB spendable beyond the floor; "
                         "other programs are using the memory now; retry when it frees",
                         base_name(cfg->models[choice]), need >> 20, spendable > 0 ? spendable >> 20 : 0);
                resource_retryable = 1;
            }
            continue;
        }
        request = (BfResourceRequest){.plane = cfg->plane, .kind = "model-resident",
                                      .estimated_bytes = need, .volume = BF_ADMISSION_MEMORY_VOLUME,
                                      .detail = cfg->models[choice]};
        verdict = bf_admission_request_grant(grants, &request, &policy, available, NULL, &decision, &grant_id);
        if (verdict == BF_ADMISSION_ADMIT) break;
        grant_id = 0;
        if (!first_refusal[0])
            snprintf(first_refusal, sizeof first_refusal, "%s: %s needs %" PRId64 " MiB, %" PRId64 " MiB available beyond the floor; %s",
                     bf_admission_verdict_name(verdict), base_name(cfg->models[choice]), need >> 20,
                     decision.spendable_bytes >> 20, decision.reason);
        if (verdict == BF_ADMISSION_DEFER) resource_retryable = 1;
    }
    bf_admission_close(grants);
    if (rank == candidates) {
        if (saw_unsupported) {
            *failure = failure_model_unsupported;
            snprintf(reason, reason_size, "%s", first_unsupported);
        } else {
            *failure = resource_retryable ? failure_resource_retry : failure_resource_final;
            if (!first_refusal[0]) snprintf(first_refusal, sizeof first_refusal, "no loadable chat model");
            snprintf(reason, reason_size, "%s", first_refusal);
        }
        goto refused;
    }
    current = cfg->models[choice];
    current_choice = choice;
    current_bytes = need;
    current_context = ctx;

    snprintf(port, sizeof port, "%u", (unsigned)cfg->backend_port);
    snprintf(layers, sizeof layers, "%u", cfg->gpu_layers);
    snprintf(context, sizeof context, "%" PRId64, ctx);
    argc = 0;
    argv[argc++] = (char *)cfg->server_program;
    argv[argc++] = "-m"; argv[argc++] = (char *)current;
    argv[argc++] = "-ngl"; argv[argc++] = layers;
    argv[argc++] = "-c"; argv[argc++] = context;
    argv[argc++] = "-fa"; argv[argc++] = "on";
    argv[argc++] = "-ctk"; argv[argc++] = "q8_0";
    argv[argc++] = "-ctv"; argv[argc++] = "q8_0";
    argv[argc++] = "--host"; argv[argc++] = "127.0.0.1";
    argv[argc++] = "--port"; argv[argc++] = port;
    argv[argc] = NULL;
    spawn_error = posix_spawnp(&child, cfg->server_program, NULL, NULL, argv, environ);
    if (spawn_error != 0) {
        child = 0;
        release_grant_locked();
        *failure = failure_backend_unavailable;
        snprintf(reason, reason_size, "could not start %s: %s", cfg->server_program, strerror(spawn_error));
        goto refused;
    }
    say("loading %s with %" PRId64 " context (pid %d, grant %" PRId64 ", %" PRId64 " MiB)",
        base_name(current), ctx, (int)child, grant_id, need >> 20);
    began = time(NULL);
    deadline = began + cfg->load_timeout_seconds;
    while (time(NULL) < deadline && !stopping) {
        reap_child_locked();
        if (child <= 0) {
            /* A checkpoint this llama-server cannot run is set aside for the
             * life of the process, and the next candidate is tried. */
            usable[choice] = 0;
            saw_unsupported = 1;
            if (!first_unsupported[0])
                snprintf(first_unsupported, sizeof first_unsupported, "%s is not supported by %s",
                         base_name(current), cfg->server_program);
            say("%s did not load; trying the next model", base_name(current));
            goto retry;
        }
        if (backend_ready()) {
            BfAdmissionGrants *ledger = NULL;
            /* Resident now: its bytes have left available memory, so the
             * claim is settled rather than counted a second time. */
            if (bf_admission_open(cfg->admission_db, &ledger, stderr) == 0) {
                bf_admission_settle_grant(ledger, grant_id);
                bf_admission_close(ledger);
            }
            loads++;
            last_activity = time(NULL);
            say("resident after %lds", (long)(time(NULL) - began));
            return 0;
        }
        usleep(250000);
    }
    stop_child_locked("load timed out");
    usable[choice] = 0;
    saw_unsupported = 1;
    if (!first_unsupported[0])
        snprintf(first_unsupported, sizeof first_unsupported, "%s did not become ready within %us",
                 base_name(current), cfg->load_timeout_seconds);
    say("%s timed out; trying the next model", base_name(current));
    goto retry;

refused:
    snprintf(last_refusal, sizeof last_refusal, "%s", reason);
    refusals++;
    say("load refused (%s): %s", failure->type, reason);
    return -1;
}

/* A fallback is resident and no request is open: if a stronger model now fits
 * in what unloading the fallback would free, make room for it. */
static void promote_locked(void) {
    BfAdmissionPolicy policy;
    int64_t spendable;
    int rank;
    if (child <= 0 || active > 0 || current_choice < 0 || order[0] == current_choice || leases_live_locked() > 0) return;
    /* A leased batch is never swapped (above). A caller without a lease gets
     * a quiet spell as a courtesy: back-to-back requests are left alone. */
    if (time(NULL) - last_activity < 30 || time(NULL) - last_promotion_check < 30) return;
    last_promotion_check = time(NULL);
    spendable = spendable_now(current_bytes, &policy) - ((int64_t)256 << 20);
    for (rank = 0; rank < candidates && order[rank] != current_choice; rank++) {
        int choice = order[rank];
        if (usable[choice] && context_for(choice, spendable) > 0) {
            say("%s now fits; replacing %s", base_name(cfg->models[choice]), base_name(current));
            stop_child_locked("a stronger model fits");
            promotions++;
            return;
        }
    }
}

static void json_text(char *out, size_t size, const char *text) {
    size_t at = 0;
    for (; text && *text && at + 2 < size; text++)
        out[at++] = (*text == '"' || *text == '\\' || (unsigned char)*text < 0x20) ? '\'' : *text;
    out[at] = '\0';
}

static void reply(int fd, const char *status, const char *extra_headers, const char *body) {
    char head[512];
    int length = snprintf(head, sizeof head,
                          "HTTP/1.1 %s\r\nContent-Type: application/json\r\nContent-Length: %zu\r\n%sConnection: close\r\n\r\n",
                          status, strlen(body), extra_headers ? extra_headers : "");
    char drain[4096];
    struct timeval timeout = {1, 0};
    if (write(fd, head, (size_t)length) < 0 || write(fd, body, strlen(body)) < 0) return;
    /* Closing with the request still unread makes the kernel send a reset, and
     * the client loses the answer it was just given. Finish, then drain. */
    shutdown(fd, SHUT_WR);
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof timeout);
    while (read(fd, drain, sizeof drain) > 0) {}
}

static void reply_load_failure(int fd, const MoeLoadFailure *failure, const char *reason) {
    bf_jsonw body;
    bf_jsonw_init(&body, 0);
    bf_jsonw_obj(&body);
    bf_jsonw_key(&body, "error");
    bf_jsonw_obj(&body);
    bf_jsonw_key(&body, "type"); bf_jsonw_str(&body, failure->type);
    bf_jsonw_key(&body, "message"); bf_jsonw_str(&body, reason);
    bf_jsonw_key(&body, "retryable"); bf_jsonw_bool(&body, failure->retryable);
    bf_jsonw_obj_end(&body);
    bf_jsonw_obj_end(&body);
    reply(fd, "503 Service Unavailable", failure->retryable ? "Retry-After: 30\r\n" : NULL, body.buf);
    bf_jsonw_free(&body);
}

static void reply_residency(int fd) {
    char body[4096], model[256], refusal[256], list[2048];
    size_t at = 0;
    pthread_mutex_lock(&lock);
    reap_child_locked();
    json_text(model, sizeof model, current);
    json_text(refusal, sizeof refusal, last_refusal);
    list[0] = '\0';
    for (int rank = 0; rank < candidates && at + 300 < sizeof list; rank++) {
        char name[256];
        int choice = order[rank];
        json_text(name, sizeof name, base_name(cfg->models[choice]));
        at += (size_t)snprintf(list + at, sizeof list - at, "%s{\"model\":\"%s\",\"usable\":%s,\"min_bytes\":%" PRId64 "}",
                               rank ? "," : "", name, usable[choice] ? "true" : "false",
                               estimate_bytes(choice, cfg->context_size ? cfg->context_size : 4096));
    }
    snprintf(body, sizeof body,
             "{\"schema\":\"bonfyre.moe.residency.v2\",\"model\":\"%s\",\"context\":%" PRId64 ",\"choices\":%d,\"resident\":%s,\"pid\":%d,"
             "\"grant_id\":%" PRId64 ",\"open_connections\":%d,\"idle_unload_seconds\":%u,"
             "\"seconds_since_activity\":%ld,\"loads\":%u,\"unloads\":%u,\"promotions\":%u,\"refusals\":%u,\"last_refusal\":\"%s\","
             "\"leases\":%d,\"leases_granted\":%u,"
             "\"candidates\":[%s]}\n",
             model, child > 0 ? current_context : (int64_t)0, candidates, child > 0 ? "true" : "false", (int)child, grant_id, active, cfg->idle_seconds,
             last_activity ? (long)(time(NULL) - last_activity) : -1L, loads, unloads, promotions, refusals, refusal,
             leases_live_locked(), leases_granted, list);
    pthread_mutex_unlock(&lock);
    reply(fd, "200 OK", NULL, body);
}

static void relay(int client, int backend) {
    struct pollfd fds[2] = {{client, POLLIN, 0}, {backend, POLLIN, 0}};
    char buffer[65536];
    int open_in[2] = {1, 1};
    time_t quiet_since = time(NULL);
    /* An idle keep-alive connection would otherwise pin the model forever. */
    time_t quiet_limit = cfg->idle_seconds > 60 ? (time_t)cfg->idle_seconds : 60;
    while (open_in[0] || open_in[1]) {
        int polled = poll(fds, 2, 1000);
        if (polled < 0) { if (errno == EINTR) continue; break; }
        if (polled == 0) { if (time(NULL) - quiet_since >= quiet_limit) break; continue; }
        quiet_since = time(NULL);
        for (int side = 0; side < 2; side++) {
            int from = side ? backend : client, to = side ? client : backend;
            ssize_t got;
            if (!open_in[side] || !(fds[side].revents & (POLLIN | POLLHUP | POLLERR))) continue;
            got = read(from, buffer, sizeof buffer);
            if (got <= 0) {
                open_in[side] = 0;
                fds[side].events = 0;
                shutdown(to, SHUT_WR);
                if (side == 1) open_in[0] = 0; /* the model finished answering */
                continue;
            }
            for (ssize_t sent = 0; sent < got;) {
                ssize_t wrote = write(to, buffer + sent, (size_t)(got - sent));
                if (wrote <= 0) { open_in[0] = open_in[1] = 0; break; }
                sent += wrote;
            }
            pthread_mutex_lock(&lock);
            last_activity = time(NULL);
            pthread_mutex_unlock(&lock);
        }
    }
}

/* name=value from a request line's query string. */
static int query_value(const char *line, const char *name, char *out, size_t size) {
    const char *q = strchr(line, '?');
    size_t n = strlen(name);
    if (!q) return 0;
    for (const char *at = q + 1; at && *at && *at != ' '; at = strchr(at, '&') ? strchr(at, '&') + 1 : NULL) {
        if (strncmp(at, name, n) == 0 && at[n] == '=') {
            size_t len = strcspn(at + n + 1, "& \r\n");
            if (len >= size) len = size - 1;
            memcpy(out, at + n + 1, len);
            out[len] = '\0';
            return 1;
        }
    }
    return 0;
}

/* The candidate a caller asked for: a basename, or a part of one, ignoring
 * case ("qwen2.5-3b-instruct"). The first listed name that matches wins. */
static int preferred_choice(const char *list) {
    char names[512], *save = NULL;
    snprintf(names, sizeof names, "%s", list);
    for (char *name = strtok_r(names, ",", &save); name; name = strtok_r(NULL, ",", &save))
        for (int rank = 0; rank < candidates; rank++) {
            int choice = order[rank];
            const char *base = base_name(cfg->models[choice]);
            size_t n = strlen(name);
            if (!usable[choice] || !n) continue;
            for (const char *at = base; *at; at++)
                if (strncasecmp(at, name, n) == 0) return choice;
        }
    return -1;
}

/* POST /bonfyre/lease?ttl=S[&prefer=a,b]   load if needed, pin, answer the file
 * POST /bonfyre/lease?renew=ID&ttl=S       extend
 * POST /bonfyre/lease?release=ID           end
 * A preference swaps the resident model only when no other lease holds it;
 * otherwise the lease is on the model that is resident and says so. */
static void serve_lease(int client, const char *line) {
    char value[512], body[1024], safe[512], reason[256] = {0};
    MoeLoadFailure failure = failure_none;
    long ttl = 600;
    int64_t id;
    if (query_value(line, "ttl", value, sizeof value)) ttl = strtol(value, NULL, 10);
    if (ttl < 1) ttl = 1;
    if (ttl > MAX_LEASE_SECONDS) ttl = MAX_LEASE_SECONDS;
    pthread_mutex_lock(&lock);
    reap_child_locked();
    int release = query_value(line, "release", value, sizeof value);
    int renew = !release && query_value(line, "renew", value, sizeof value);
    if (release || renew) {
        id = strtoll(value, NULL, 10);
        leases_live_locked();
        for (int i = 0; i < MAX_LEASES; i++)
            if (id && leases[i].id == id) {
                if (renew) leases[i].expires = time(NULL) + ttl; else leases[i].id = 0;
                if (!renew) last_activity = time(NULL);
                pthread_mutex_unlock(&lock);
                snprintf(body, sizeof body, "{\"lease\":%" PRId64 ",\"%s\":true}\n", id, renew ? "renewed" : "released");
                reply(client, "200 OK", NULL, body);
                return;
            }
        pthread_mutex_unlock(&lock);
        reply(client, "404 Not Found", NULL, "{\"error\":{\"type\":\"no_such_lease\",\"message\":\"the lease ended or was never granted\"}}\n");
        return;
    }
    int want = query_value(line, "prefer", value, sizeof value) ? preferred_choice(value) : -1;
    int slot = -1;
    leases_live_locked();
    for (int i = 0; i < MAX_LEASES && slot < 0; i++) if (!leases[i].id) slot = i;
    if (slot < 0) {
        pthread_mutex_unlock(&lock);
        reply(client, "503 Service Unavailable", "Retry-After: 30\r\n", "{\"error\":{\"type\":\"leases_full\",\"message\":\"every lease slot is held\"}}\n");
        return;
    }
    if (want >= 0 && child > 0 && current_choice != want && active == 0 && leases_live_locked() == 0)
        stop_child_locked("a lease asked for another model");
    /* The preferred model first; failing that, whatever fits. The refusal a
     * caller sees is the one for the model it asked for. */
    char fallback[256] = {0};
    if (child <= 0 && ensure_loaded_locked(reason, sizeof reason, want, &failure) != 0 &&
        (want < 0 || ensure_loaded_locked(fallback, sizeof fallback, -1, &failure) != 0)) {
        pthread_mutex_unlock(&lock);
        reply_load_failure(client, &failure, reason);
        return;
    }
    id = next_lease++;
    leases[slot].id = id;
    leases[slot].expires = time(NULL) + ttl;
    leases_granted++;
    json_text(safe, sizeof safe, current);
    snprintf(body, sizeof body, "{\"lease\":%" PRId64 ",\"model_path\":\"%s\",\"context\":%" PRId64 ",\"ttl\":%ld,\"preferred\":%s}\n",
             id, safe, current_context, ttl, want < 0 ? "null" : current_choice == want ? "true" : "false");
    pthread_mutex_unlock(&lock);
    reply(client, "200 OK", NULL, body);
}

static void *serve_client(void *argument) {
    int client = (int)(intptr_t)argument, backend = -1;
    char peek[1024] = {0}, reason[256] = {0};
    MoeLoadFailure failure = failure_none;
    struct timeval timeout = {15, 0};
    ssize_t got;
    int loaded;

    setsockopt(client, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof timeout);
    got = recv(client, peek, sizeof peek - 1, MSG_PEEK);
    if (got > 0 && strncmp(peek, "GET /bonfyre/residency", 22) == 0) { reply_residency(client); goto done; }
    if (got > 0 && strncmp(peek, "POST /bonfyre/lease", 19) == 0) {
        char *eol = strstr(peek, "\r\n");
        if (eol) *eol = '\0';
        serve_lease(client, peek);
        goto done;
    }
    pthread_mutex_lock(&lock);
    reap_child_locked();
    if (got > 0 && strncmp(peek, "GET /health", 11) != 0) promote_locked();
    loaded = child > 0;
    if (!loaded && got > 0 && strncmp(peek, "GET /health", 11) == 0) {
        pthread_mutex_unlock(&lock);
        reply(client, "200 OK", NULL, "{\"status\":\"ok\",\"resident\":false,\"loads_on_demand\":true}\n");
        goto done;
    }
    if (!loaded && ensure_loaded_locked(reason, sizeof reason, -1, &failure) != 0) {
        pthread_mutex_unlock(&lock);
        reply_load_failure(client, &failure, reason);
        goto done;
    }
    active++;
    pthread_mutex_unlock(&lock);
    timeout.tv_sec = 0;
    setsockopt(client, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof timeout);
    backend = connect_backend();
    if (backend >= 0) relay(client, backend);
    else reply(client, "502 Bad Gateway", NULL, "{\"error\":{\"type\":\"model_unreachable\"}}\n");
    pthread_mutex_lock(&lock);
    active--;
    last_activity = time(NULL);
    pthread_mutex_unlock(&lock);
done:
    if (backend >= 0) close(backend);
    close(client);
    return NULL;
}

int moe_residency_serve(const MoeResidency *config) {
    struct sockaddr_in address = {0};
    struct sigaction action = {0};
    BfAdmissionGrants *grants = NULL;
    int listener, yes = 1;

    cfg = config;
    if (cfg->model_count < 1) return 2;
    for (int m = 0; m < cfg->model_count; m++) {
        usable[m] = cfg->estimate_bytes > 0 ||
                    (bf_admission_model_shape(cfg->models[m], KV_ELEMENT_BYTES, &shapes[m]) == 0 && shapes[m].chat);
        if (!usable[m]) say("skipping %s: not a readable chat GGUF", cfg->models[m]);
        if (usable[m]) order[candidates++] = m;
    }
    if (!candidates) { fprintf(stderr, "BonfyreMoE: none of the models is a readable chat GGUF\n"); return 2; }
    /* A scan has no stated preference: the larger checkpoint is the stronger
     * one, so it is tried first and the smaller ones are its fallbacks. */
    if (cfg->discovered)
        for (int a = 1; a < candidates; a++)
            for (int b = a; b > 0 && shapes[order[b]].file_bytes > shapes[order[b - 1]].file_bytes; b--) {
                int t = order[b]; order[b] = order[b - 1]; order[b - 1] = t;
            }
    current = cfg->models[order[0]];
    action.sa_handler = on_signal;
    sigaction(SIGTERM, &action, NULL);
    sigaction(SIGINT, &action, NULL);
    signal(SIGPIPE, SIG_IGN);

    /* A predecessor that crashed holding a grant must not keep its memory. */
    if (bf_admission_open(cfg->admission_db, &grants, stderr) != 0) return 1;
    if (bf_admission_release_plane(grants, cfg->plane, NULL) > 0)
        say("released grants left open by an earlier %s", cfg->plane);
    bf_admission_close(grants);

    listener = socket(AF_INET, SOCK_STREAM, 0);
    if (listener < 0) { perror("socket"); return 1; }
    setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes);
    address.sin_family = AF_INET;
    address.sin_port = htons(cfg->port);
    if (inet_pton(AF_INET, cfg->host, &address.sin_addr) != 1) {
        fprintf(stderr, "BonfyreMoE: --host must be an IPv4 address: %s\n", cfg->host);
        return 2;
    }
    if (bind(listener, (struct sockaddr *)&address, sizeof address) != 0 || listen(listener, 64) != 0) {
        fprintf(stderr, "BonfyreMoE: cannot listen on %s:%u: %s\n", cfg->host, (unsigned)cfg->port, strerror(errno));
        close(listener);
        return 1;
    }
    say("listening on %s:%u; the strongest of %d models that fits loads on first request%s", cfg->host, (unsigned)cfg->port,
        candidates,
        cfg->idle_seconds ? "" : " and then stays resident");

    while (!stopping) {
        struct pollfd ready = {listener, POLLIN, 0};
        int polled = poll(&ready, 1, 1000);
        if (polled > 0 && (ready.revents & POLLIN)) {
            int client = accept(listener, NULL, NULL);
            pthread_t thread;
            if (client >= 0) {
                if (pthread_create(&thread, NULL, serve_client, (void *)(intptr_t)client) == 0) pthread_detach(thread);
                else close(client);
            }
        }
        pthread_mutex_lock(&lock);
        reap_child_locked();
        if (child > 0 && cfg->idle_seconds && active == 0 && leases_live_locked() == 0 &&
            time(NULL) - last_activity >= (time_t)cfg->idle_seconds)
            stop_child_locked("idle");
        pthread_mutex_unlock(&lock);
    }
    close(listener);
    pthread_mutex_lock(&lock);
    stop_child_locked("server stopping");
    pthread_mutex_unlock(&lock);
    return 0;
}
