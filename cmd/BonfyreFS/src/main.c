/*
 * BonfyreFS — real macFUSE mount over the fabric's live state.
 *
 * Minimal, honest scope: read-only, two top-level directories backed by
 * live queries against BONFYRE_STATE_DIR/fabric.db on every call (not a
 * snapshot taken at mount time):
 *
 *   /Missions/<mission_id>.json   -- id, status, workgraph_cursor
 *   /Artifacts/<digest>.json      -- uri, digest, media_type, bytes
 *
 * This proves the actual, previously-missing capability -- a real macFUSE
 * mount that Finder and any POSIX process can see and read -- rather than
 * a directory generated once under /tmp. It intentionally does not yet
 * cover every namespace kind or writable files (see
 * scripts/bonfyre-fs-effects-watch for the writable-effect-file half,
 * which works today without this mount and can be wired to it later).
 *
 * Optional `--mission <id>` scopes the whole mount to one mission: an
 * agent/tool given a scoped mount sees only its own mission file and only
 * the artifacts that mission's real events actually reference as
 * input/output -- nothing else in the fabric is visible through that
 * mount point. Without the flag, behavior is unchanged (every mission and
 * artifact is visible), so this is additive, not a breaking change.
 */
#define FUSE_USE_VERSION 26
#define _POSIX_C_SOURCE 200809L

#include <fuse.h>
#include <sqlite3.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static sqlite3 *g_db = NULL;
static time_t g_mount_time;
static char g_mission_scope[160] = "";

/* When scoped, an artifact is visible only if some event in the scoped
 * mission actually references it as input or output -- real provenance,
 * not a guess. */
static int artifact_in_scope(const char *digest) {
    if (!g_mission_scope[0]) {
        return 1;
    }
    sqlite3_stmt *statement = NULL;
    int in_scope = 0;
    if (sqlite3_prepare_v2(g_db,
            "SELECT 1 FROM artifacts a WHERE a.digest=? AND a.uri IN ("
            "SELECT input_uri FROM events WHERE mission_id=? AND input_uri IS NOT NULL "
            "UNION SELECT output_uri FROM events WHERE mission_id=? AND output_uri IS NOT NULL)",
            -1, &statement, NULL) == SQLITE_OK) {
        sqlite3_bind_text(statement, 1, digest, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 2, g_mission_scope, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 3, g_mission_scope, -1, SQLITE_TRANSIENT);
        in_scope = sqlite3_step(statement) == SQLITE_ROW;
    }
    sqlite3_finalize(statement);
    return in_scope;
}

static int open_database(void) {
    const char *state_dir = getenv("BONFYRE_STATE_DIR");
    char path[4096];

    if (!state_dir || !state_dir[0]) {
        fprintf(stderr, "bonfyrefs: BONFYRE_STATE_DIR is required\n");
        return -1;
    }
    snprintf(path, sizeof(path), "%s/fabric.db", state_dir);
    if (sqlite3_open_v2(path, &g_db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        fprintf(stderr, "bonfyrefs: cannot open %s: %s\n", path, sqlite3_errmsg(g_db));
        return -1;
    }
    return 0;
}

/* Parses "/Missions/<id>.json" or "/Artifacts/<digest>.json" into (kind,
 * identity). Returns 0 and fills kind/identity on match, -1 otherwise. */
static int parse_path(const char *path, char kind[16], char identity[256]) {
    const char *rest;
    size_t length;

    if (strncmp(path, "/Missions/", 10) == 0) {
        snprintf(kind, 16, "mission");
        rest = path + 10;
    } else if (strncmp(path, "/Artifacts/", 11) == 0) {
        snprintf(kind, 16, "artifact");
        rest = path + 11;
    } else {
        return -1;
    }
    length = strlen(rest);
    if (length < 6 || strcmp(rest + length - 5, ".json") != 0) {
        return -1;
    }
    if (length - 5 >= 256) {
        return -1;
    }
    memcpy(identity, rest, length - 5);
    identity[length - 5] = '\0';
    return identity[0] ? 0 : -1;
}

/* Builds the live JSON body for a given kind+identity. Returns malloc'd
 * string (caller frees) or NULL if not found. */
static char *build_content(const char *kind, const char *identity) {
    sqlite3_stmt *statement = NULL;
    char *out = NULL;

    if (strcmp(kind, "mission") == 0) {
        if (g_mission_scope[0] && strcmp(identity, g_mission_scope) != 0) {
            return NULL;
        }
        if (sqlite3_prepare_v2(g_db,
                "SELECT id,status,workgraph_cursor FROM missions WHERE id=?",
                -1, &statement, NULL) != SQLITE_OK) {
            return NULL;
        }
        sqlite3_bind_text(statement, 1, identity, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(statement) == SQLITE_ROW) {
            const char *id = (const char *)sqlite3_column_text(statement, 0);
            const char *status = (const char *)sqlite3_column_text(statement, 1);
            const char *cursor = (const char *)sqlite3_column_text(statement, 2);
            out = malloc(2048);
            if (out) {
                snprintf(out, 2048,
                         "{\"mission_id\":\"%s\",\"status\":\"%s\",\"workgraph_cursor\":\"%s\"}\n",
                         id ? id : "", status ? status : "", cursor ? cursor : "");
            }
        }
        sqlite3_finalize(statement);
    } else if (strcmp(kind, "artifact") == 0) {
        if (!artifact_in_scope(identity)) {
            return NULL;
        }
        if (sqlite3_prepare_v2(g_db,
                "SELECT digest,uri,media_type,bytes FROM artifacts WHERE digest=?",
                -1, &statement, NULL) != SQLITE_OK) {
            return NULL;
        }
        sqlite3_bind_text(statement, 1, identity, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(statement) == SQLITE_ROW) {
            const char *digest = (const char *)sqlite3_column_text(statement, 0);
            const char *uri = (const char *)sqlite3_column_text(statement, 1);
            const char *media_type = (const char *)sqlite3_column_text(statement, 2);
            sqlite3_int64 bytes = sqlite3_column_int64(statement, 3);
            out = malloc(2048);
            if (out) {
                snprintf(out, 2048,
                         "{\"digest\":\"%s\",\"uri\":\"%s\",\"media_type\":\"%s\",\"bytes\":%lld}\n",
                         digest ? digest : "", uri ? uri : "", media_type ? media_type : "",
                         (long long)bytes);
            }
        }
        sqlite3_finalize(statement);
    }
    return out;
}

static int bfs_getattr(const char *path, struct stat *stbuf) {
    memset(stbuf, 0, sizeof(struct stat));
    stbuf->st_mtime = stbuf->st_ctime = stbuf->st_atime = g_mount_time;

    if (strcmp(path, "/") == 0 || strcmp(path, "/Missions") == 0 ||
        strcmp(path, "/Artifacts") == 0) {
        stbuf->st_mode = S_IFDIR | 0555;
        stbuf->st_nlink = 2;
        return 0;
    }

    char kind[16], identity[256];
    if (parse_path(path, kind, identity) != 0) {
        return -ENOENT;
    }
    char *content = build_content(kind, identity);
    if (!content) {
        return -ENOENT;
    }
    stbuf->st_mode = S_IFREG | 0444;
    stbuf->st_nlink = 1;
    stbuf->st_size = (off_t)strlen(content);
    free(content);
    return 0;
}

static int bfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                       off_t offset, struct fuse_file_info *fi) {
    (void)offset;
    (void)fi;
    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);

    if (strcmp(path, "/") == 0) {
        filler(buf, "Missions", NULL, 0);
        filler(buf, "Artifacts", NULL, 0);
        return 0;
    }
    if (strcmp(path, "/Missions") == 0) {
        sqlite3_stmt *statement = NULL;
        const char *sql = g_mission_scope[0]
            ? "SELECT id FROM missions WHERE id=? ORDER BY id"
            : "SELECT id FROM missions ORDER BY id";
        if (sqlite3_prepare_v2(g_db, sql, -1, &statement, NULL) == SQLITE_OK) {
            if (g_mission_scope[0]) {
                sqlite3_bind_text(statement, 1, g_mission_scope, -1, SQLITE_TRANSIENT);
            }
            while (sqlite3_step(statement) == SQLITE_ROW) {
                const char *id = (const char *)sqlite3_column_text(statement, 0);
                char name[300];
                if (id) {
                    snprintf(name, sizeof(name), "%s.json", id);
                    filler(buf, name, NULL, 0);
                }
            }
        }
        sqlite3_finalize(statement);
        return 0;
    }
    if (strcmp(path, "/Artifacts") == 0) {
        sqlite3_stmt *statement = NULL;
        const char *sql = g_mission_scope[0]
            ? "SELECT DISTINCT a.digest FROM artifacts a WHERE a.uri IN ("
              "SELECT input_uri FROM events WHERE mission_id=? AND input_uri IS NOT NULL "
              "UNION SELECT output_uri FROM events WHERE mission_id=? AND output_uri IS NOT NULL) "
              "ORDER BY a.digest"
            : "SELECT digest FROM artifacts ORDER BY digest";
        if (sqlite3_prepare_v2(g_db, sql, -1, &statement, NULL) == SQLITE_OK) {
            if (g_mission_scope[0]) {
                sqlite3_bind_text(statement, 1, g_mission_scope, -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(statement, 2, g_mission_scope, -1, SQLITE_TRANSIENT);
            }
            while (sqlite3_step(statement) == SQLITE_ROW) {
                const char *digest = (const char *)sqlite3_column_text(statement, 0);
                char name[300];
                if (digest) {
                    snprintf(name, sizeof(name), "%s.json", digest);
                    filler(buf, name, NULL, 0);
                }
            }
        }
        sqlite3_finalize(statement);
        return 0;
    }
    return -ENOENT;
}

static int bfs_open(const char *path, struct fuse_file_info *fi) {
    char kind[16], identity[256];

    if (parse_path(path, kind, identity) != 0) {
        return -ENOENT;
    }
    if ((fi->flags & O_ACCMODE) != O_RDONLY) {
        return -EACCES;
    }
    char *content = build_content(kind, identity);
    if (!content) {
        return -ENOENT;
    }
    free(content);
    return 0;
}

static int bfs_read(const char *path, char *buf, size_t size, off_t offset,
                    struct fuse_file_info *fi) {
    (void)fi;
    char kind[16], identity[256];

    if (parse_path(path, kind, identity) != 0) {
        return -ENOENT;
    }
    char *content = build_content(kind, identity);
    if (!content) {
        return -ENOENT;
    }
    size_t length = strlen(content);
    int result;
    if ((size_t)offset >= length) {
        result = 0;
    } else {
        if (offset + size > length) {
            size = length - offset;
        }
        memcpy(buf, content + offset, size);
        result = (int)size;
    }
    free(content);
    return result;
}

static struct fuse_operations bfs_ops = {
    .getattr = bfs_getattr,
    .readdir = bfs_readdir,
    .open = bfs_open,
    .read = bfs_read,
};

int main(int argc, char *argv[]) {
    char *filtered[64];
    int filtered_argc = 0;

    if (argc > (int)(sizeof(filtered) / sizeof(filtered[0]))) {
        fprintf(stderr, "bonfyrefs: too many arguments\n");
        return 1;
    }
    for (int index = 0; index < argc; index++) {
        if (strcmp(argv[index], "--mission") == 0 && index + 1 < argc) {
            snprintf(g_mission_scope, sizeof(g_mission_scope), "%s", argv[index + 1]);
            index++;
            continue;
        }
        if (strncmp(argv[index], "--mission=", 10) == 0) {
            snprintf(g_mission_scope, sizeof(g_mission_scope), "%s", argv[index] + 10);
            continue;
        }
        filtered[filtered_argc++] = argv[index];
    }

    g_mount_time = time(NULL);
    if (open_database() != 0) {
        return 1;
    }
    return fuse_main(filtered_argc, filtered, &bfs_ops, NULL);
}
