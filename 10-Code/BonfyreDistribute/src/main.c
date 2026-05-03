#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_TEXT 65536

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
    fprintf(stderr,
            "Usage:\n"
            "  bonfyre-distribute offers <_generated-offers.json>\n"
            "  bonfyre-distribute snapshot <_distribution-pipeline-snapshot.json>\n"
            "  bonfyre-distribute message <_generated-offers.json> <offer-name> <channel>\n");
    return 1;
}
