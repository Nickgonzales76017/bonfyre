#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <spawn.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

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
    char needle[256];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char *pos = strstr(json, needle);
    if (!pos) return 0;
    pos = strchr(pos + strlen(needle), ':');
    if (!pos) return 0;
    pos++;
    while (*pos && isspace((unsigned char)*pos)) pos++;
    if (*pos != '"') return 0;
    pos++;
    const char *end = strchr(pos, '"');
    if (!end) return 0;
    size_t len = (size_t)(end - pos);
    if (len >= size) len = size - 1;
    memcpy(buffer, pos, len);
    buffer[len] = '\0';
    return 1;
}

static int extract_int_value(const char *json, const char *key, int *value) {
    char needle[256];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char *pos = strstr(json, needle);
    if (!pos) return 0;
    pos = strchr(pos + strlen(needle), ':');
    if (!pos) return 0;
    pos++;
    while (*pos && isspace((unsigned char)*pos)) pos++;
    *value = atoi(pos);
    return 1;
}

static int command_offers(const char *offers_path) {
    long size = 0;
    char *json = read_file(offers_path, &size);
    if (!json) {
        fprintf(stderr, "Failed to read offers file.\n");
        return 1;
    }

    int count = 0;
    const char *cursor = json;
    while ((cursor = strstr(cursor, "\"offer_name\"")) != NULL) {
        count++;
        cursor += 12;
    }
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

    char buyer_segment[256] = "";
    const char *segment_pos = strstr(match, "\"buyer_segment\"");
    if (segment_pos) {
        extract_string_value(segment_pos, "buyer_segment", buyer_segment, sizeof(buyer_segment));
    }

    printf("Channel: %s\n", channel);
    printf("Offer: %s\n", offer_name);
    printf("Message: I have a proof-backed local-first offer for %s. If you have one messy recording, I can turn it into a transcript, summary, and next steps quickly.\n",
           buyer_segment[0] ? buyer_segment : "operators");
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

int main(int argc, char **argv) {
    if (argc >= 3 && strcmp(argv[1], "offers") == 0) {
        return command_offers(argv[2]);
    }
    if (argc >= 3 && strcmp(argv[1], "snapshot") == 0) {
        return command_snapshot(argv[2]);
    }
    if (argc >= 5 && strcmp(argv[1], "message") == 0) {
        return command_message(argv[2], argv[3], argv[4]);
    }
    if (argc >= 3 && strcmp(argv[1], "send") == 0) {
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
    fprintf(stderr,
            "Usage:\n"
            "  bonfyre-distribute offers <_generated-offers.json>\n"
            "  bonfyre-distribute snapshot <_distribution-pipeline-snapshot.json>\n"
            "  bonfyre-distribute message <_generated-offers.json> <offer-name> <channel>\n"
            "  bonfyre-distribute send <payload.json> --webhook <URL> [--webhook <URL> ...]\n");
    return 1;
}
