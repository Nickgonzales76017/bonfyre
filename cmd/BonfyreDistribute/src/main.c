/* POSIX/GNU interfaces used below are hidden by glibc under -std=c11 without
   a feature macro. macOS exposes them regardless, so this class of break is
   invisible on the dev machine and fatal on the Linux deploy host. */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

#include <ctype.h>
#include "../../../lib/libbonfyre/include/bf_help.h"
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"
#include "bf_lex.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <spawn.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <sqlite3.h>

#ifdef BF_HAS_QUIC
#include <bf_quic.h>
#include <bonfyre.h>
#endif

extern char **environ;

#define MAX_TEXT 65536
#define MAX_TARGETS 16
#define MAX_URL    2048

static char *read_file(const char *path, long *size_out) {
    FILE *fp = fopen(path, "rb");
    if (!fp) return NULL;
    fseek(fp, 0, SEEK_END);
    long size = ftell(fp);
    fseek(fp, 0, SEEK_SET);
    if (size < 0) {
        fclose(fp);
        return NULL;
    }
    char *buffer = malloc((size_t)size + 1);
    if (!buffer) {
        fclose(fp);
        return NULL;
    }
    fread(buffer, 1, (size_t)size, fp);
    fclose(fp);
    buffer[size] = '\0';
    if (size_out) *size_out = size;
    return buffer;
}

static int extract_string_value(const char *json, const char *key, char *buffer, size_t size) {
    /* Delegates to the one implementation. This copy stopped at the first
     * quote, and truncated silently on overflow rather than reporting that the
     * value did not fit -- a prefix returned as if it were whole. */
    return bf_lex_json_string(json, key, buffer, size);
}

static int extract_int_value(const char *json, const char *key, int *value) {
    /* Delegates to the one implementation, which refuses a string where a
     * number was asked for instead of running atoi over a quote. */
    char scalar[64];
    if (!bf_lex_json_scalar(json, key, scalar, sizeof(scalar))) return 0;
    *value = atoi(scalar);
    return 1;
}

static int command_offers(const char *offers_path) {
    long size = 0;
    char *json = read_file(offers_path, &size);
    if (!json) {
        fprintf(stderr, "Failed to read offers file.\n");
        return 1;
    }

    /* BonfyreOffer writes offerName; older generated lists wrote offer_name.
     * Counting only the old key reported every current offer list as empty. */
    int count = 0;
    static const char *keys[] = { "\"offer_name\"", "\"offerName\"" };
    for (size_t k = 0; k < sizeof keys / sizeof *keys; k++)
        for (const char *cursor = json; (cursor = strstr(cursor, keys[k])) != NULL; cursor += strlen(keys[k]))
            count++;
    printf("{\"kind\":\"offers\",\"path\":\"%s\",\"offerCount\":%d}\n", offers_path, count);
    free(json);
    return 0;
}

static int command_snapshot(const char *snapshot_path) {
    long size = 0;
    char *json = read_file(snapshot_path, &size);
    if (!json) {
        fprintf(stderr, "Failed to read snapshot file.\n");
        return 1;
    }
    int total_sends = 0;
    int pending_count = 0;
    int live_offer_count = 0;
    char best_channel[128] = "";
    extract_int_value(json, "total_sends", &total_sends);
    extract_int_value(json, "pending_count", &pending_count);
    extract_int_value(json, "live_offer_count", &live_offer_count);
    extract_string_value(json, "best_channel", best_channel, sizeof(best_channel));
    printf("{\"kind\":\"distribution-snapshot\",\"path\":\"%s\",\"totalSends\":%d,\"pending\":%d,\"liveOffers\":%d,\"bestChannel\":\"%s\"}\n",
           snapshot_path, total_sends, pending_count, live_offer_count, best_channel);
    free(json);
    return 0;
}

/* The messages this estate has composed are records in its content store,
 * each with the state it is in: withheld, handed to a person, abandoned.
 * This reads them and counts them by state. It sends nothing and changes
 * nothing; a message handed to a person is that person's to send. */
static int command_messages(const char *store_path) {
    sqlite3 *store = NULL;
    sqlite3_stmt *rows = NULL;
    long total = 0;
    int first = 1;
    if (sqlite3_open_v2(store_path, &store, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(store, "SELECT COALESCE(state,''), COUNT(*) FROM outbound_message "
                                  "GROUP BY 1 ORDER BY 1", -1, &rows, NULL) != SQLITE_OK) {
        fprintf(stderr, "No readable outbound_message records in %s\n", store_path);
        if (rows) sqlite3_finalize(rows);
        if (store) sqlite3_close(store);
        return 1;
    }
    printf("{\"kind\":\"outbound-messages\",\"by_state\":{");
    while (sqlite3_step(rows) == SQLITE_ROW) {
        const char *state = (const char *)sqlite3_column_text(rows, 0);
        long count = (long)sqlite3_column_int64(rows, 1);
        printf("%s\"", first ? "" : ",");
        for (const char *c = state; *c; c++) if (*c != '"' && *c != '\\' && (unsigned char)*c >= 0x20) putchar(*c);
        printf("\":%ld", count);
        total += count;
        first = 0;
    }
    printf("},\"total\":%ld,\"dispatched_by_this_command\":0}\n", total);
    sqlite3_finalize(rows);
    sqlite3_close(store);
    return total > 0 ? 0 : 2;
}

static int command_message(const char *offers_path, const char *offer_name, const char *channel) {
    long size = 0;
    char *json = read_file(offers_path, &size);
    if (!json) {
        fprintf(stderr, "Failed to read offers file.\n");
        return 1;
    }

    const char *match = strstr(json, offer_name);
    if (!match) {
        fprintf(stderr, "Offer not found: %s\n", offer_name);
        free(json);
        return 1;
    }

    /* A message says what the offer says. An offer that has not been quoted
     * has no promise, price or turnaround to state, and its own record marks
     * it not sendable; composing a pitch for it would put words in it. */
    char sendable[16] = "", state[64] = "";
    char promise[2048] = "", price[128] = "", turnaround[256] = "";
    /* Read from the start of the object that holds the name, so a list of
     * offers is answered from the one that was asked for. */
    const char *scope = match;
    while (scope > json && *scope != '{') scope--;
    bf_lex_json_scalar(scope, "sendable", sendable, sizeof(sendable));
    extract_string_value(scope, "commercialState", state, sizeof(state));
    if (strcmp(sendable, "true") != 0) {
        fprintf(stderr, "Offer is not sendable (%s): %s\n",
                state[0] ? state : "no commercial state recorded", offer_name);
        free(json);
        return 2;
    }
    if (!extract_string_value(scope, "promise", promise, sizeof(promise)) || !promise[0]) {
        fprintf(stderr, "Offer states no promise, so there is nothing to say: %s\n", offer_name);
        free(json);
        return 2;
    }
    extract_string_value(scope, "price", price, sizeof(price));
    extract_string_value(scope, "turnaround", turnaround, sizeof(turnaround));

    printf("Channel: %s\n", channel);
    printf("Offer: %s\n", offer_name);
    printf("Message: %s", promise);
    if (price[0]) printf(" Price: %s.", price);
    if (turnaround[0]) printf(" Turnaround: %s.", turnaround);
    printf("\n");
    free(json);
    return 0;
}

/* ── send command: POST JSON payload to webhook URLs via curl ── */

static int curl_post(const char *url, const char *json_path) {
    pid_t pid;
    char content_type[] = "Content-Type: application/json";
    char *argv[] = {
        "curl", "-s", "-S", "-f",
        "-X", "POST",
        "-H", content_type,
        "-d", NULL,   /* will be @path */
        (char *)url,
        NULL
    };
    char at_path[MAX_URL];
    snprintf(at_path, sizeof(at_path), "@%s", json_path);
    argv[9] = at_path;

    posix_spawnattr_t attr;
    posix_spawnattr_init(&attr);
    int rc = posix_spawnp(&pid, "curl", NULL, &attr, argv, environ);
    posix_spawnattr_destroy(&attr);
    if (rc != 0) {
        fprintf(stderr, "  ✗ Failed to spawn curl: %s\n", strerror(rc));
        return 1;
    }
    int status = 0;
    waitpid(pid, &status, 0);
    return WIFEXITED(status) ? WEXITSTATUS(status) : 1;
}

static int command_send(const char *payload_path, int target_count, char **targets) {
    if (target_count == 0) {
        fprintf(stderr, "No targets specified. Use --webhook <URL> one or more times.\n");
        return 1;
    }

    long size = 0;
    char *json = read_file(payload_path, &size);
    if (!json) {
        fprintf(stderr, "Failed to read payload: %s\n", payload_path);
        return 1;
    }
    free(json); /* just validate it's readable */

    struct timespec t0;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    int ok = 0, fail = 0;
    for (int i = 0; i < target_count; i++) {
        fprintf(stderr, "  → %s ... ", targets[i]);
        int rc = curl_post(targets[i], payload_path);
        if (rc == 0) {
            fprintf(stderr, "✓\n");
            ok++;
        } else {
            fprintf(stderr, "✗ (exit %d)\n", rc);
            fail++;
        }
    }

    struct timespec t1;
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double elapsed = (double)(t1.tv_sec - t0.tv_sec) +
                     (double)(t1.tv_nsec - t0.tv_nsec) / 1e9;

    printf("{\"kind\":\"distribution-result\",\"payload\":\"%s\","
           "\"targets\":%d,\"delivered\":%d,\"failed\":%d,"
           "\"elapsed_ms\":%.1f}\n",
           payload_path, target_count, ok, fail, elapsed * 1000.0);
    return fail > 0 ? 1 : 0;
}

static void usage(void) {
    fprintf(stderr,
            "Usage:\n"
            "  bonfyre-distribute offers <_generated-offers.json>\n"
            "  bonfyre-distribute snapshot <_distribution-pipeline-snapshot.json>\n"
            "  bonfyre-distribute message <_generated-offers.json> <offer-name> <channel>\n" "  bonfyre-distribute messages <content-store.db>   count composed messages by state; sends nothing\n"
            "  bonfyre-distribute send <payload.json> --webhook <URL> [--webhook <URL> ...]\n"
#ifdef BF_HAS_QUIC
            "  bonfyre-distribute quic-send <dir> --host <HOST> --port <PORT>\n"
            "      Send all artifacts in <dir> over QUIC with family-multiplexed streams.\n"
            "      Surface-layer artifacts are prioritized. 0-RTT on reconnect.\n"
#endif
            );
}

int main(int argc, char **argv) {
    if (bf_help_requested(argc, argv)) { usage(); return 0; }
    enum {
        DS_MESSAGE, DS_MESSAGES, DS_OFFERS, DS_QUIC_SEND, DS_SEND, DS_SNAPSHOT,
    };
    static const BfCommandEntry distribute_commands[] = {
        {"message", DS_MESSAGE, 0, NULL},
        {"messages", DS_MESSAGES, 0, NULL},
        {"offers", DS_OFFERS, 0, NULL},
        {"quic-send", DS_QUIC_SEND, 0, NULL},
        {"send", DS_SEND, 0, NULL},
        {"snapshot", DS_SNAPSHOT, 0, NULL},
    };
    const BfCommandEntry *command = argc < 2 ? NULL : bf_command_lookup(distribute_commands,
        sizeof(distribute_commands) / sizeof(distribute_commands[0]), argv[1]);
    if (argc >= 3 && command && command->id == DS_OFFERS) {
        return command_offers(argv[2]);
    }
    if (argc >= 3 && command && command->id == DS_SNAPSHOT) {
        return command_snapshot(argv[2]);
    }
    if (argc >= 3 && command && command->id == DS_MESSAGES) { return command_messages(argv[2]); } if (argc >= 5 && command && command->id == DS_MESSAGE) {
        return command_message(argv[2], argv[3], argv[4]);
    }
    if (argc >= 3 && command && command->id == DS_SEND) {
        const char *payload = argv[2];
        char *webhooks[MAX_TARGETS];
        int wh_count = 0;
        for (int i = 3; i < argc - 1; i++) {
            if (strcmp(argv[i], "--webhook") == 0 && wh_count < MAX_TARGETS) {
                webhooks[wh_count++] = argv[++i];
            }
        }
        return command_send(payload, wh_count, webhooks);
    }
#ifdef BF_HAS_QUIC
    if (argc >= 3 && command && command->id == DS_QUIC_SEND) {
        const char *dir = argv[2];
        const char *host = "127.0.0.1";
        uint16_t port = 4443;
        for (int i = 3; i < argc - 1; i++) {
            if (strcmp(argv[i], "--host") == 0) host = argv[++i];
            else if (strcmp(argv[i], "--port") == 0) port = (uint16_t)atoi(argv[++i]);
        }
        return command_quic_send(dir, host, port);
    }
#endif
    usage();
    return 1;
}

#ifdef BF_HAS_QUIC
/* ── QUIC distribution: family-multiplexed artifact streams ── */

static void quic_done_cb(const bf_quic_stream_result_t *r, void *user) {
    (void)user;
    fprintf(stderr, "  %s %s (%.1f ms, %llu bytes%s)\n",
            r->status == BF_QUIC_OK ? "✓" : "✗",
            r->family_key,
            r->elapsed_ms,
            (unsigned long long)r->bytes_sent,
            r->zero_rtt ? ", 0-RTT" : "");
}

static int command_quic_send(const char *dir, const char *host, uint16_t port) {
    /* Discover artifacts in directory */
    bf_quic_artifact_t artifacts[256];
    int count = 0;

    DIR *dp = opendir(dir);
    if (!dp) {
        fprintf(stderr, "Cannot open directory: %s\n", dir);
        return 1;
    }
    struct dirent *ent;
    while ((ent = readdir(dp)) != NULL && count < 256) {
        if (ent->d_name[0] == '.') continue;

        /* Look for artifact.json in each subdirectory */
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/%s/artifact.json", dir, ent->d_name);
        struct stat st;
        if (stat(path, &st) != 0) {
            /* Try direct files */
            snprintf(path, sizeof(path), "%s/%s", dir, ent->d_name);
            if (stat(path, &st) != 0 || !S_ISREG(st.st_mode)) continue;
        }

        /* Parse artifact to get family_key and type for layer mapping */
        FILE *fp = fopen(path, "rb");
        if (!fp) continue;
        char json[8192];
        size_t n = fread(json, 1, sizeof(json) - 1, fp);
        fclose(fp);
        json[n] = '\0';

        BfArtifact art;
        bf_artifact_init(&art);
        bf_artifact_parse(&art, json);

        artifacts[count].path = strdup(path);
        artifacts[count].family_key = strdup(art.family_key);
        artifacts[count].content_hash = strdup(art.root_hash);
        artifacts[count].layer = (uint8_t)bf_quic_layer_from_type(art.artifact_type);
        count++;
    }
    closedir(dp);

    if (count == 0) {
        fprintf(stderr, "No artifacts found in %s\n", dir);
        return 1;
    }

    fprintf(stderr, "Distributing %d artifacts over QUIC to %s:%u\n", count, host, port);
    fprintf(stderr, "  Priority: surface → value → transform → substrate\n");

    /* Connect */
    bf_quic_ctx_t *ctx = bf_quic_ctx_new(NULL, NULL, ".bonfyre-quic-ticket");
    if (!ctx) {
        fprintf(stderr, "Failed to create QUIC context\n");
        return 1;
    }

    bf_quic_conn_t *conn = bf_quic_connect(ctx, host, port);
    if (!conn) {
        fprintf(stderr, "Failed to connect to %s:%u\n", host, port);
        bf_quic_ctx_free(ctx);
        return 1;
    }

    /* Send batch */
    bf_quic_batch_result_t result = bf_quic_send_batch(conn, artifacts, count,
                                                        quic_done_cb, NULL);
    printf("{\"kind\":\"quic-distribution-result\","
           "\"host\":\"%s\",\"port\":%u,"
           "\"artifacts\":%d,\"delivered\":%d,\"failed\":%d,"
           "\"total_bytes\":%llu,\"elapsed_ms\":%.1f}\n",
           host, port,
           result.count, result.succeeded, result.failed,
           (unsigned long long)result.total_bytes, result.total_ms);

    /* Cleanup */
    free(result.streams);
    for (int i = 0; i < count; i++) {
        free((void *)artifacts[i].path);
        free((void *)artifacts[i].family_key);
        free((void *)artifacts[i].content_hash);
    }
    bf_quic_conn_close(conn);
    bf_quic_ctx_free(ctx);
    return result.failed > 0 ? 1 : 0;
}
#endif /* BF_HAS_QUIC */
