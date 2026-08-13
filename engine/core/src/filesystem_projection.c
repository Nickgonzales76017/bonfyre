#define _POSIX_C_SOURCE 200809L
#include "bf_filesystem.h"

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

static int make_directory(const char *path) {
    if (mkdir(path, 0755) == 0) {
        return 0;
    }
    return errno == EEXIST ? 0 : -1;
}

static void safe_component(const char *value, char *output, size_t output_size) {
    size_t index = 0;

    while (value != NULL && *value != '\0' && index + 1 < output_size) {
        unsigned char character = (unsigned char)*value++;
        output[index++] = isalnum(character) || character == '.' ||
                          character == '_' || character == '-'
            ? (char)character
            : '_';
    }
    output[index] = '\0';
}

static void json_string(FILE *file, const char *value) {
    const unsigned char *cursor = (const unsigned char *)(value == NULL ? "" : value);

    fputc('"', file);
    while (*cursor != '\0') {
        unsigned char character = *cursor++;
        if (character == '"' || character == '\\') {
            fputc('\\', file);
            fputc(character, file);
        } else if (character == '\n') {
            fputs("\\n", file);
        } else if (character < 0x20) {
            fprintf(file, "\\u%04x", character);
        } else {
            fputc(character, file);
        }
    }
    fputc('"', file);
}

static int write_artifacts(sqlite3 *database, const char *directory, int *count) {
    sqlite3_stmt *statement = NULL;
    int step_result;

    if (sqlite3_prepare_v2(database,
            "SELECT digest,uri,media_type,locator,bytes FROM artifacts ORDER BY digest",
            -1, &statement, NULL) != SQLITE_OK) {
        return -1;
    }
    *count = 0;
    while ((step_result = sqlite3_step(statement)) == SQLITE_ROW) {
        const char *digest = (const char *)sqlite3_column_text(statement, 0);
        const char *uri = (const char *)sqlite3_column_text(statement, 1);
        const char *media_type = (const char *)sqlite3_column_text(statement, 2);
        const char *locator = (const char *)sqlite3_column_text(statement, 3);
        sqlite3_int64 bytes = sqlite3_column_int64(statement, 4);
        char link_path[PATH_MAX];
        char metadata_path[PATH_MAX];
        FILE *metadata;

        if (snprintf(link_path, sizeof(link_path), "%s/%s.artifact", directory, digest) >=
                (int)sizeof(link_path) ||
            snprintf(metadata_path, sizeof(metadata_path), "%s/%s.json", directory, digest) >=
                (int)sizeof(metadata_path) ||
            symlink(locator, link_path) != 0) {
            sqlite3_finalize(statement);
            return -1;
        }
        metadata = fopen(metadata_path, "wx");
        if (metadata == NULL) {
            sqlite3_finalize(statement);
            return -1;
        }
        fputs("{\"uri\":", metadata);
        json_string(metadata, uri);
        fputs(",\"digest\":", metadata);
        json_string(metadata, digest);
        fputs(",\"media_type\":", metadata);
        json_string(metadata, media_type);
        fprintf(metadata, ",\"bytes\":%lld}\n", (long long)bytes);
        if (fclose(metadata) != 0) {
            sqlite3_finalize(statement);
            return -1;
        }
        ++*count;
    }
    sqlite3_finalize(statement);
    return step_result == SQLITE_DONE ? 0 : -1;
}

static int write_missions(sqlite3 *database, const char *directory, int *count) {
    sqlite3_stmt *statement = NULL;
    int step_result;

    if (sqlite3_prepare_v2(database,
            "SELECT id,status,workgraph_cursor FROM missions ORDER BY id",
            -1, &statement, NULL) != SQLITE_OK) {
        return -1;
    }
    *count = 0;
    while ((step_result = sqlite3_step(statement)) == SQLITE_ROW) {
        const char *identifier = (const char *)sqlite3_column_text(statement, 0);
        const char *status = (const char *)sqlite3_column_text(statement, 1);
        const char *cursor = (const char *)sqlite3_column_text(statement, 2);
        char component[256];
        char path[PATH_MAX];
        FILE *file;

        safe_component(identifier, component, sizeof(component));
        if (component[0] == '\0' ||
            snprintf(path, sizeof(path), "%s/%s.json", directory, component) >=
                (int)sizeof(path)) {
            sqlite3_finalize(statement);
            return -1;
        }
        file = fopen(path, "wx");
        if (file == NULL) {
            sqlite3_finalize(statement);
            return -1;
        }
        fputs("{\"mission_id\":", file);
        json_string(file, identifier);
        fputs(",\"status\":", file);
        json_string(file, status);
        fputs(",\"workgraph_cursor\":", file);
        json_string(file, cursor);
        fputs("}\n", file);
        if (fclose(file) != 0) {
            sqlite3_finalize(statement);
            return -1;
        }
        ++*count;
    }
    sqlite3_finalize(statement);
    return step_result == SQLITE_DONE ? 0 : -1;
}

static int write_catalog(sqlite3 *database, const char *path) {
    sqlite3_stmt *statement = NULL;
    char generation[65] = "uncompiled";
    int public_commands;
    int typed_contracts;
    FILE *file;

    if (sqlite3_prepare_v2(database,
            "SELECT value FROM fabric_meta WHERE key='catalog_generation'",
            -1, &statement, NULL) == SQLITE_OK &&
        sqlite3_step(statement) == SQLITE_ROW) {
        snprintf(generation, sizeof(generation), "%s",
                 sqlite3_column_text(statement, 0));
    }
    sqlite3_finalize(statement);
    statement = NULL;
    if (sqlite3_prepare_v2(database,
            "SELECT count(*),sum(EXISTS(SELECT 1 FROM operator_contract_bindings c "
            "WHERE c.operator_id=b.operator_id)) FROM catalog_bindings b "
            "WHERE b.operator_id LIKE 'command.%' AND b.binding_state='bound'",
            -1, &statement, NULL) != SQLITE_OK ||
        sqlite3_step(statement) != SQLITE_ROW) {
        sqlite3_finalize(statement);
        return -1;
    }
    public_commands = sqlite3_column_int(statement, 0);
    typed_contracts = sqlite3_column_int(statement, 1);
    sqlite3_finalize(statement);
    file = fopen(path, "wx");
    if (file == NULL) {
        return -1;
    }
    fprintf(file,
            "{\"catalog_generation\":\"%s\",\"public_commands\":%d,"
            "\"typed_command_contracts\":%d}\n",
            generation, public_commands, typed_contracts);
    return fclose(file);
}

int bf_filesystem_project(sqlite3 *database, const char *target, FILE *output, FILE *error) {
    char absolute_target[PATH_MAX];
    char temporary[PATH_MAX];
    char artifacts[PATH_MAX];
    char missions[PATH_MAX];
    char catalog[PATH_MAX];
    char current[PATH_MAX];
    int artifact_count = 0;
    int mission_count = 0;

    if (database == NULL || target == NULL || target[0] == '\0') {
        fprintf(error, "fabric: filesystem projection requires a target\n");
        return 2;
    }
    if (target[0] == '/') {
        if (snprintf(absolute_target, sizeof(absolute_target), "%s", target) >=
            (int)sizeof(absolute_target)) {
            return 1;
        }
    } else if (getcwd(current, sizeof(current)) == NULL ||
               snprintf(absolute_target, sizeof(absolute_target), "%s/%s", current, target) >=
                   (int)sizeof(absolute_target)) {
        fprintf(error, "fabric: invalid filesystem projection target\n");
        return 1;
    }
    if (access(absolute_target, F_OK) == 0 ||
        snprintf(temporary, sizeof(temporary), "%s.tmp.%ld", absolute_target,
                 (long)getpid()) >= (int)sizeof(temporary) ||
        snprintf(artifacts, sizeof(artifacts), "%s/artifacts", temporary) >=
            (int)sizeof(artifacts) ||
        snprintf(missions, sizeof(missions), "%s/missions", temporary) >=
            (int)sizeof(missions) ||
        snprintf(catalog, sizeof(catalog), "%s/catalog.json", temporary) >=
            (int)sizeof(catalog)) {
        fprintf(error, "fabric: filesystem projection target already exists or is too long\n");
        return 1;
    }
    if (make_directory(temporary) != 0 || make_directory(artifacts) != 0 ||
        make_directory(missions) != 0 ||
        write_artifacts(database, artifacts, &artifact_count) != 0 ||
        write_missions(database, missions, &mission_count) != 0 ||
        write_catalog(database, catalog) != 0 || rename(temporary, absolute_target) != 0) {
        fprintf(error, "fabric: filesystem projection failed: %s\n", strerror(errno));
        return 1;
    }
    fprintf(output, "projection=%s\nartifacts=%d\nmissions=%d\nstate=complete\n",
            absolute_target, artifact_count, mission_count);
    return 0;
}
