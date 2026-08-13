#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static char *read_file(const char *path, long *size_out) {
    FILE *fp = fopen(path, "rb");
    if (!fp) {
        perror("fopen");
        return NULL;
    }
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

static int has_key(const char *json, const char *key) {
    char needle[256];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    return strstr(json, needle) != NULL;
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

static int command_inspect_intake(const char *path) {
    long size = 0;
    char *json = read_file(path, &size);
    if (!json) return 1;

    const char *required[] = {
        "schemaVersion", "manifest", "sourceFile", "jobId", "jobSlug",
        "jobTitle", "fileName", "dataBase64", NULL
    };
    int valid = 1;
    for (int i = 0; required[i]; i++) {
        if (!has_key(json, required[i])) {
            valid = 0;
        }
    }

    char job_slug[256] = "";
    char job_title[256] = "";
    char file_name[256] = "";
    extract_string_value(json, "jobSlug", job_slug, sizeof(job_slug));
    extract_string_value(json, "jobTitle", job_title, sizeof(job_title));
    extract_string_value(json, "fileName", file_name, sizeof(file_name));

    printf("{\n");
    printf("  \"kind\": \"intake-package\",\n");
    printf("  \"path\": \"%s\",\n", path);
    printf("  \"valid\": %s,\n", valid ? "true" : "false");
    printf("  \"jobSlug\": \"%s\",\n", job_slug);
    printf("  \"jobTitle\": \"%s\",\n", job_title);
    printf("  \"fileName\": \"%s\",\n", file_name);
    printf("  \"sizeBytes\": %ld\n", size);
    printf("}\n");

    free(json);
    return valid ? 0 : 1;
}

static int command_inspect_status(const char *path) {
    long size = 0;
    char *json = read_file(path, &size);
    if (!json) return 1;

    const char *required[] = {
        "sourceSystem", "jobSlug", "status", "deliverableMarkdown", "quality", NULL
    };
    int valid = 1;
    for (int i = 0; required[i]; i++) {
        if (!has_key(json, required[i])) {
            valid = 0;
        }
    }

    char source_system[256] = "";
    char job_slug[256] = "";
    char status[256] = "";
    extract_string_value(json, "sourceSystem", source_system, sizeof(source_system));
    extract_string_value(json, "jobSlug", job_slug, sizeof(job_slug));
    extract_string_value(json, "status", status, sizeof(status));

    printf("{\n");
    printf("  \"kind\": \"browser-status\",\n");
    printf("  \"path\": \"%s\",\n", path);
    printf("  \"valid\": %s,\n", valid ? "true" : "false");
    printf("  \"sourceSystem\": \"%s\",\n", source_system);
    printf("  \"jobSlug\": \"%s\",\n", job_slug);
    printf("  \"status\": \"%s\",\n", status);
    printf("  \"sizeBytes\": %ld\n", size);
    printf("}\n");

    free(json);
    return valid ? 0 : 1;
}

int main(int argc, char **argv) {
    if (argc != 3) {
        fprintf(stderr,
                "Usage:\n"
                "  bonfyre-sync inspect-intake <path>\n"
                "  bonfyre-sync inspect-status <path>\n");
        return 1;
    }

    if (strcmp(argv[1], "inspect-intake") == 0) {
        return command_inspect_intake(argv[2]);
    }
    if (strcmp(argv[1], "inspect-status") == 0) {
        return command_inspect_status(argv[2]);
    }

    fprintf(stderr, "Unknown command: %s\n", argv[1]);
    return 1;
}
