/* bonfyre-release-approval: the same Approve the work board offers, for an
 * MCP caller acting for the owner (native_tool_invoke runs it).
 *
 *   bonfyre-release-approval list    [--operator-db F]           JSON
 *   bonfyre-release-approval approve --release R --actor A [--operator-db F] [--approvals DIR]
 *
 * approve acts only on an open release-approval goal that approved_activation.sh
 * offered (so acceptance already passed), writes <release>.approved once with
 * O_EXCL|O_NOFOLLOW where the release watcher reads it, and notes the approval
 * on the goal. The watcher, not this program, verifies again and activates,
 * and restores the previous release on failure. */
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include "bf_jsonw.h"

static int word(const char *s, size_t max) { size_t n = strlen(s); return n && n <= max && strspn(s, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-:.@_") == n; }

int main(int argc, char **argv) {
    const char *cmd = argc > 1 ? argv[1] : "", *home = getenv("HOME") ? getenv("HOME") : "", *release = NULL, *actor = NULL;
    char opdb[1024], approvals[1024];
    snprintf(opdb, sizeof(opdb), "%s/.bonfyre/operator/operator.sqlite3", home);
    snprintf(approvals, sizeof(approvals), "%s/.bonfyre/state/approvals", home);
    for (int i = 2; i < argc; i++) {
        if (!strcmp(argv[i], "--operator-db") && i + 1 < argc) snprintf(opdb, sizeof(opdb), "%s", argv[++i]);
        else if (!strcmp(argv[i], "--approvals") && i + 1 < argc) snprintf(approvals, sizeof(approvals), "%s", argv[++i]);
        else if (!strcmp(argv[i], "--release") && i + 1 < argc) release = argv[++i];
        else if (!strcmp(argv[i], "--actor") && i + 1 < argc) actor = argv[++i];
        else { fprintf(stderr, "unknown argument %s\n", argv[i]); return 2; }
    }
    if (strcmp(cmd, "list") && strcmp(cmd, "approve")) { fputs("usage: bonfyre-release-approval list | approve --release R --actor A\n", stderr); return 2; }
    sqlite3 *db = NULL; sqlite3_stmt *q = NULL;
    if (sqlite3_open_v2(opdb, &db, SQLITE_OPEN_READWRITE, NULL) != SQLITE_OK) { fprintf(stderr, "cannot open %s\n", opdb); return 1; }
    sqlite3_busy_timeout(db, 3000);
    if (!strcmp(cmd, "list")) {
        bf_jsonw w; bf_jsonw_init(&w, 0); bf_jsonw_obj(&w); bf_jsonw_key(&w, "open"); bf_jsonw_arr(&w);
        if (sqlite3_prepare_v2(db, "SELECT substr(dedupe_key,18),id,details,last_error FROM goals WHERE source='release-approval' AND dedupe_key LIKE 'release-approval:%' AND state NOT IN('resolved','superseded') ORDER BY updated_at DESC LIMIT 20", -1, &q, NULL) == SQLITE_OK)
            while (sqlite3_step(q) == SQLITE_ROW) {
                bf_jsonw_obj(&w);
                bf_jsonw_key(&w, "release"); bf_jsonw_str(&w, (const char *)sqlite3_column_text(q, 0));
                bf_jsonw_key(&w, "goal"); bf_jsonw_str(&w, (const char *)sqlite3_column_text(q, 1));
                bf_jsonw_key(&w, "details"); bf_jsonw_str(&w, (const char *)sqlite3_column_text(q, 2));
                bf_jsonw_key(&w, "status"); bf_jsonw_str(&w, (const char *)sqlite3_column_text(q, 3));
                bf_jsonw_obj_end(&w);
            }
        sqlite3_finalize(q); sqlite3_close(db);
        bf_jsonw_arr_end(&w); bf_jsonw_obj_end(&w); fwrite(w.buf, 1, w.len, stdout); fputc('\n', stdout); bf_jsonw_free(&w); return 0;
    }
    if (!release || !actor || !word(release, 90) || strchr(release, ':') || strchr(release, '.') || strchr(release, '@') || strchr(release, '_') || !word(actor, 120)) { fputs("approve needs --release (letters, digits, hyphens) and --actor\n", stderr); sqlite3_close(db); return 2; }
    char key[128], goal[64] = "", path[1200], line[512]; snprintf(key, sizeof(key), "release-approval:%s", release);
    if (sqlite3_prepare_v2(db, "SELECT id FROM goals WHERE dedupe_key=?1 AND source='release-approval' AND state NOT IN('resolved','superseded')", -1, &q, NULL) == SQLITE_OK) {
        sqlite3_bind_text(q, 1, key, -1, SQLITE_STATIC); if (sqlite3_step(q) == SQLITE_ROW) snprintf(goal, sizeof(goal), "%s", (const char *)sqlite3_column_text(q, 0));
    }
    sqlite3_finalize(q);
    if (!*goal) { fprintf(stderr, "%s is not an open release approval\n", release); sqlite3_close(db); return 1; }
    mkdir(approvals, 0700);
    snprintf(path, sizeof(path), "%s/%s.approved", approvals, release);
    int fd = open(path, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW, 0600);
    if (fd < 0) { fprintf(stderr, errno == EEXIST ? "%s is already approved\n" : "cannot write the approval for %s\n", release); sqlite3_close(db); return 1; }
    int len = snprintf(line, sizeof(line), "release\t%s\ngoal\t%s\nactor\t%s\napproved_at\t%lld\n", release, goal, actor, (long long)time(NULL));
    int wrote = len > 0 && write(fd, line, (size_t)len) == len; close(fd);
    if (!wrote) { unlink(path); fputs("the approval could not be saved\n", stderr); sqlite3_close(db); return 1; }
    char note[160]; time_t now = time(NULL); struct tm tm; char day[32] = ""; if (gmtime_r(&now, &tm)) strftime(day, sizeof(day), "%a %d %b %Y", &tm);
    snprintf(note, sizeof(note), "Approved by %s through MCP %s; the release is being put live", actor, day);
    if (sqlite3_prepare_v2(db, "UPDATE goals SET last_error=?2,updated_at=?3 WHERE id=?1", -1, &q, NULL) == SQLITE_OK) {
        sqlite3_bind_text(q, 1, goal, -1, SQLITE_STATIC); sqlite3_bind_text(q, 2, note, -1, SQLITE_STATIC); sqlite3_bind_double(q, 3, (double)now); sqlite3_step(q);
    }
    sqlite3_finalize(q); sqlite3_close(db);
    bf_jsonw w; bf_jsonw_init(&w, 0); bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "approved"); bf_jsonw_str(&w, release); bf_jsonw_key(&w, "goal"); bf_jsonw_str(&w, goal);
    bf_jsonw_key(&w, "next"); bf_jsonw_str(&w, "the release watcher verifies again and activates; the goal reports the outcome");
    bf_jsonw_obj_end(&w); fwrite(w.buf, 1, w.len, stdout); fputc('\n', stdout); bf_jsonw_free(&w); return 0;
}
