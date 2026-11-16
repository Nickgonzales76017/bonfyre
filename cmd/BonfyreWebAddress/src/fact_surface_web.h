#ifndef BONFYRE_FACT_SURFACE_WEB_H
#define BONFYRE_FACT_SURFACE_WEB_H
#include "account_continuity_web.h"
#include "ui_tree.h"
#include "../generated/native_fact_surfaces.h"
/* Fact surfaces: one renderer for every row of schemas/control/fact-surfaces.cue.
 *
 *   /x            every row this viewer may see, with where it shows
 *   /x/<slug>     one page row: its count and its newest records
 *
 * A page row reads one declared store read-only through that store's lease,
 * with the row's one SELECT bound to its row limit, a progress deadline and a
 * per-cell byte cap. Audience is checked on every request: public rows for
 * anyone, member rows for a signed-in session, admin rows for the bonfyre
 * admin grant /studio/admin checks. Nothing here writes. A record shown is
 * what the producer wrote, not a claim that it is current or verified. */

#define FS_CELL_MAX 240
#define FS_DEADLINE_MS 1500
#define FS_MAX_COLS 12

typedef enum { FS_ANON = 0, FS_MEMBER = 1, FS_ADMIN = 2 } FsReach;

static FsReach fs_reach(const BfRequest *r) {
    AcnViewer v;
    if (!acn_viewer(r->bearer, r->auth_db, &v)) return FS_ANON;
    return session_institution_authorized(r->bearer, r->auth_db, "bonfyre", "admin") ? FS_ADMIN : FS_MEMBER;
}
static int fs_need(const BfFactSurface *f) { return !strcmp(f->audience, "admin") ? FS_ADMIN : !strcmp(f->audience, "member") ? FS_MEMBER : FS_ANON; }

typedef struct { struct timespec end; } FsDeadline;
static int fs_progress(void *ctx) {
    FsDeadline *d = ctx; struct timespec now; clock_gettime(CLOCK_MONOTONIC, &now);
    return now.tv_sec > d->end.tv_sec || (now.tv_sec == d->end.tv_sec && now.tv_nsec > d->end.tv_nsec);
}
static sqlite3 *fs_open(const BfFactSurface *f) {
    char path[PATH_MAX];
    const char *p = getenv(f->env);
    if (!p || !*p) {
        const char *home = getenv("HOME");
        if (!f->home_file || !home || snprintf(path, sizeof(path), "%s/%s", home, f->home_file + 0) >= (int)sizeof(path)) return NULL;
        p = path;
    }
    struct stat st;
    if (stat(p, &st) || !S_ISREG(st.st_mode)) return NULL;
    sqlite3 *db = bf_store_open_ro(p, 1000, &f->policy);
    if (!db) return NULL;
    return db;
}
/* Each statement gets its own deadline; past it SQLite interrupts the step. */
static FsDeadline fs_deadline;
static void fs_arm(sqlite3 *db) {
    clock_gettime(CLOCK_MONOTONIC, &fs_deadline.end);
    fs_deadline.end.tv_sec += FS_DEADLINE_MS / 1000; fs_deadline.end.tv_nsec += (long)(FS_DEADLINE_MS % 1000) * 1000000L;
    if (fs_deadline.end.tv_nsec >= 1000000000L) { fs_deadline.end.tv_sec++; fs_deadline.end.tv_nsec -= 1000000000L; }
    sqlite3_progress_handler(db, 1000, fs_progress, &fs_deadline);
}

/* The rows read, held so ui_table can ask for them by index. */
typedef struct { unsigned n, cols; char (*cell)[FS_CELL_MAX + 4]; } FsRows;
static void fs_copy(char *dst, const unsigned char *src) {
    size_t n = src ? strlen((const char *)src) : 0;
    if (n > FS_CELL_MAX) {
        n = FS_CELL_MAX;
        while (n && (src[n] & 0xC0) == 0x80) n--;   /* never cut inside a character */
        memcpy(dst, src, n); memcpy(dst + n, "\xe2\x80\xa6", 4);
        return;
    }
    if (n) memcpy(dst, src, n);
    dst[n] = '\0';
}
static int fs_read(sqlite3 *db, const BfFactSurface *f, FsRows *out, long long *count) {
    sqlite3_stmt *st = NULL;
    *count = -1; memset(out, 0, sizeof(*out));
    fs_arm(db);
    if (sqlite3_prepare_v2(db, f->count_sql, -1, &st, NULL) == SQLITE_OK && sqlite3_stmt_readonly(st) && sqlite3_step(st) == SQLITE_ROW)
        *count = sqlite3_column_int64(st, 0);
    sqlite3_finalize(st); st = NULL;
    fs_arm(db);
    if (sqlite3_prepare_v2(db, f->sql, -1, &st, NULL) != SQLITE_OK || !sqlite3_stmt_readonly(st) ||
        (unsigned)sqlite3_column_count(st) != f->ncolumns || f->ncolumns > FS_MAX_COLS) { sqlite3_finalize(st); return 0; }
    sqlite3_bind_int(st, 1, f->limit);
    out->cols = f->ncolumns;
    out->cell = calloc((size_t)f->limit * f->ncolumns, sizeof(*out->cell));
    if (!out->cell) { sqlite3_finalize(st); return 0; }
    while (out->n < (unsigned)f->limit && sqlite3_step(st) == SQLITE_ROW)
        { for (unsigned c = 0; c < f->ncolumns; c++) fs_copy(out->cell[out->n * f->ncolumns + c], sqlite3_column_text(st, (int)c)); out->n++; }
    sqlite3_finalize(st);
    return 1;
}
static void fs_table_row(FILE *o, void *ctx, unsigned row) {
    FsRows *r = ctx;
    for (unsigned c = 0; c < r->cols; c++) ui_cell(o, r->cell[row * r->cols + c]);
}

static void fs_head(const char *kicker, const char *title, const char *lede) {
    ui_open(stdout, "header", "ak-pattern ak-section");
    ui_text(stdout, "p", "ak-kicker", kicker);
    ui_text(stdout, "h1", NULL, title);
    ui_text_if(stdout, "p", NULL, lede);
    ui_close(stdout, "header");
}
static void fs_stats(const BfFactSurface *f, const char *const *labels, const char *const *values, unsigned n) {
    UiLayoutValue desk[] = {{BF_NP_COLUMNS, f->cols_desktop}}, phone[] = {{BF_NP_COLUMNS, f->cols_phone}};
    UiLayout l; memset(&l, 0, sizeof(l));
    l.view[BF_NP_VP_DESKTOP] = (UiLayoutView){desk, 1}; l.view[BF_NP_VP_PHONE] = (UiLayoutView){phone, 1};
    ui_layout_open(stdout, "div", "ak-stats", &l);
    for (unsigned i = 0; i < n; i++) ui_stat(stdout, labels[i], values[i]);
    ui_close(stdout,"div");
}

static void fs_page(const BfFactSurface *f, int personal) {
    sqlite3 *db = fs_open(f);
    FsRows rows; long long count = -1;
    int ok = db && fs_read(db, f, &rows, &count);
    if (db) sqlite3_close(db);
    ak_app_http(200, personal);
    ak_app_open(f->title, "workbench", "route-fact-surface");
    fs_head(!strcmp(f->audience, "public") ? "What the estate keeps" : !strcmp(f->audience, "member") ? "Your records" : "Estate records", f->title, f->lede);
    char shown[32], total[48];
    snprintf(shown, sizeof(shown), "%u", ok ? rows.n : 0);
    if (count >= 0) snprintf(total, sizeof(total), "%lld", count); else snprintf(total, sizeof(total), "Too many to count quickly");
    const char *labels[] = {"Records", "Shown here"}, *values[] = {total, shown};
    fs_stats(f, labels, values, 2);
    ui_section_open(stdout, NULL, NULL);
    if (!ok || !rows.n) ui_empty(stdout, ok ? f->empty : "These records cannot be read on this server right now.", "/x", "Everything kept");
    else {
        UiColumn cols[FS_MAX_COLS];
        for (unsigned c = 0; c < f->ncolumns; c++) cols[c].label = f->columns[c];
        ui_table(stdout, NULL, cols, f->ncolumns, fs_table_row, &rows, rows.n);
    }
    ui_text(stdout, "p", "ak-empty", "Shown as recorded. A record here is not a check that it is still true.");
    ui_section_close(stdout);
    ak_app_close();
    if (ok) free(rows.cell);
}

static void fs_index(FsReach reach, int personal) {
    ak_app_http(200, personal);
    ak_app_open("Everything kept", "workbench", "route-fact-surface");
    fs_head("What the estate keeps", "Everything kept", "Each kind of record the estate keeps that you may see, and where to find it.");
    UiRow list[BF_FACT_SURFACES]; unsigned n = 0;
    char href[BF_FACT_SURFACES][96];
    for (size_t i = 0; i < BF_FACT_SURFACES; i++) {
        const BfFactSurface *f = &bf_fact_surfaces[i];
        if (fs_need(f) > (int)reach) continue;
        if (f->route && f->on && !strcmp(f->on, "site")) continue;   /* a file every member site serves, not a page */
        if (f->slug) snprintf(href[n], sizeof(href[n]), "/x/%s", f->slug); else snprintf(href[n], sizeof(href[n]), "%s", f->route);
        int dup = 0;
        for (unsigned k = 0; k < n; k++) if (!strcmp(href[k], href[n])) dup = 1;
        if (dup) continue;
        list[n] = (UiRow){href[n], f->title, f->lede}; n++;
    }
    ui_section_open(stdout, NULL, NULL);
    ui_open(stdout,"div","ak-pattern ak-list-group");
    ui_rows(stdout, "ul", NULL, list, n);
    ui_close(stdout,"div");
    if (reach == FS_ANON) ui_empty(stdout, "Sign in to see your own records here too.", "/login?next=/x", "Sign in");
    ui_section_close(stdout);
    ak_app_close();
}

static int serve_fact_surface(const BfRequest *r) {
    if (strcmp(r->method, "GET") && strcmp(r->method, "HEAD")) return 0;
    char path[160]; size_t len = strcspn(r->target, "?#");
    if (len >= sizeof(path)) return 0;
    memcpy(path, r->target, len); path[len] = '\0';
    if (len > 1 && path[len - 1] == '/') path[--len] = '\0';
    if (!strcmp(path, "/x")) { FsReach reach = fs_reach(r); fs_index(reach, reach != FS_ANON); return 1; }
    if (strncmp(path, "/x/", 3)) return 0;
    for (size_t i = 0; i < BF_FACT_SURFACES; i++) {
        const BfFactSurface *f = &bf_fact_surfaces[i];
        if (!f->slug || strcmp(f->slug, path + 3)) continue;
        int need = fs_need(f);
        if (need == FS_ANON) { fs_page(f, 0); return 1; }
        FsReach reach = fs_reach(r);
        if (reach == FS_ANON) { acn_signin(path); return 1; }
        if ((int)reach < need) { acn_notice(403, "Not open to this account", "These records are kept for the estate's administrators.", "/x", "Everything kept"); return 1; }
        fs_page(f, 1);
        return 1;
    }
    return 0;
}
#endif
