/* bonfyre-provider-estate: the downstream accounts Aurekai is acquiring, and
 * what Aurekai may actually do in each.
 *
 * Two hand-kept ledgers, read together:
 *   providers.tsv    one row per provider target: what it manages, what tags
 *                    describe its sign-up (A O M P K D C S), the customer
 *                    container it offers where one is known, its partner route.
 *   acquisition.tsv  one row per provider: how far the account has got, the
 *                    identifiers worth keeping, and the six tenancy questions.
 *
 * Holding credentials is not owning an account, and an account is not a
 * partner relationship. The tracker therefore keeps identifiers and
 * references only. `check` refuses any cell that reads like a secret, so a
 * key pasted in a hurry is caught before it is committed.
 *
 *   bonfyre-provider-estate check   [--dir D]
 *   bonfyre-provider-estate summary [--dir D]          JSON
 *   bonfyre-provider-estate next    [--dir D] [--limit N] [--wave W]
 *   bonfyre-provider-estate matrix  [--dir D]          TSV
 *   bonfyre-provider-estate publish [--dir D] --operator-db F [--wave W]
 *   bonfyre-provider-estate customer open|show|close --provider P --customer C   (customer.h)
 *
 * publish keeps one bounded checklist goal so provider setup cannot bury the
 * agents' own work under one row per provider. A provider with a machine
 * identity that answered a remote check is shown as connected; the remaining
 * rows retain their exact setup route without claiming an account exists.
 */
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>
#include <time.h>
#include "bf_jsonw.h"

#define MAX_ROWS 256
#define MAX_COLS 24
#define LINE_MAX_BYTES 4096

typedef struct { char *cell[MAX_COLS]; int n; int line; } Row;
typedef struct { Row row[MAX_ROWS]; int n; char *text; } Table;

static const char *const provider_cols[] = {"rank","slug","provider","family","wave","tags","manages","tenancy_primitive","partner_route","evidence"};
static const char *const tracker_cols[] = {"slug","stage","owner_email","account_id","organization_id","billing","two_factor","recovery_location",
    "machine_identity","secret_reference","kyb","domain_verified","partner_status","partner_reference",
    "create_container","delegate","operate","meter","revoke","transfer","notes"};
#define NP (int)(sizeof(provider_cols) / sizeof(provider_cols[0]))
#define NT (int)(sizeof(tracker_cols) / sizeof(tracker_cols[0]))

/* Stages are ordered: each one is what the chat called the finish line for
 * that step, and reaching one does not imply any later one. */
static const char *const route_cols[] = {"slug","step","url","check","checked"};
#define NR (int)(sizeof(route_cols) / sizeof(route_cols[0]))
/* The pages a person moves through, in order. Each was opened on the checked
 * date: signup-side pages load signed out, the rest land on the provider's
 * sign-in first, which is the point at which the person is ready for them. */
static const char *const steps[] = {"signup","security","organization","domain","token","app","connect","startup","partner",NULL};
static Table routes, prefill;
static const char *const prefill_cols[] = {"slug","param","checked"};

static const char *const stages[] = {"none","account","organization","secured","machine","recorded","partner_submitted",NULL};
static const char *const families[] = {"cloud","communications","email","payments","commerce","identity","crm-support","collaboration",
    "data","observability-security","source-ci","deployment","domains",NULL};
static const char *const tri[] = {"unknown","yes","no",NULL};
static const char *const billing_v[] = {"unknown","none","card","invoice","trial",NULL};
static const char *const kyb_v[] = {"unknown","not_required","pending","verified","refused",NULL};
static const char *const partner_v[] = {"none","bookmarked","submitted","contacted","approved","refused",NULL};
static const char *const wave_words[] = {"A","self-service accounts to establish tonight","B","cloud hierarchy",
    "C","communications","D","after the identity pack: business, partner and regulated accounts",NULL};

static int errors;
static void fail(const char *file, int line, const char *what, const char *value) {
    fprintf(stderr, "%s:%d: %s%s%s\n", file, line, what, value ? ": " : "", value ? value : "");
    errors++;
}
static int in(const char *const *set, const char *v) { for (; *set; set++) if (!strcmp(*set, v)) return 1; return 0; }
static int stage_index(const char *v) { for (int i = 0; stages[i]; i++) if (!strcmp(stages[i], v)) return i; return -1; }

static int load(const char *path, Table *t) {
    FILE *f = fopen(path, "rb"); long size; char *p;
    memset(t, 0, sizeof(*t));
    if (!f) { fail(path, 0, "cannot read", NULL); return 0; }
    fseek(f, 0, SEEK_END); size = ftell(f); fseek(f, 0, SEEK_SET);
    if (size < 0 || size > 4 * 1024 * 1024 || !(t->text = malloc((size_t)size + 1)) || fread(t->text, 1, (size_t)size, f) != (size_t)size) {
        fclose(f); fail(path, 0, "cannot read", NULL); return 0;
    }
    fclose(f); t->text[size] = 0;
    p = t->text;
    for (int line = 1; *p; line++) {
        char *end = strchr(p, '\n'), *c = p; Row *r;
        if (end) *end = 0;
        if (strlen(p) > LINE_MAX_BYTES) { fail(path, line, "line too long", NULL); return 0; }
        if (*p) {
            if (t->n >= MAX_ROWS) { fail(path, line, "too many rows", NULL); return 0; }
            r = &t->row[t->n++]; r->line = line;
            for (;;) {
                char *tab = strchr(c, '\t');
                if (r->n >= MAX_COLS) { fail(path, line, "too many columns", NULL); return 0; }
                r->cell[r->n++] = c;
                if (!tab) break;
                *tab = 0; c = tab + 1;
            }
        }
        if (!end) break;
        p = end + 1;
    }
    return 1;
}

static int header_ok(const char *path, const Table *t, const char *const *cols, int n) {
    if (!t->n || t->row[0].n != n) { fail(path, 1, "header does not match the declared columns", NULL); return 0; }
    for (int i = 0; i < n; i++) if (strcmp(t->row[0].cell[i], cols[i])) { fail(path, 1, "unexpected column", t->row[0].cell[i]); return 0; }
    return 1;
}

/* A cell that reads like a credential. Known key prefixes, private key blocks,
 * and long unbroken runs of token characters mixing letters and digits. A
 * reference names where a secret lives (a vault path, a keychain item); it is
 * allowed only in secret_reference and recovery_location and must say so. */
static int looks_secret(const char *v) {
    static const char *const prefixes[] = {"sk_live_","sk_test_","rk_live_","pk_live_","sk-","ghp_","gho_","ghs_","github_pat_","glpat-","xoxb-","xoxp-",
        "AKIA","ASIA","AIza","ya29.","SG.","shpat_","shpss_","whsec_","-----BEGIN","eyJ",NULL};
    size_t run = 0; int letters = 0, digits = 0;
    for (int i = 0; prefixes[i]; i++) if (strstr(v, prefixes[i])) return 1;
    for (const char *p = v;; p++) {
        if (*p && (isalnum((unsigned char)*p) || strchr("_-+/=", *p))) { run++; if (isalpha((unsigned char)*p)) letters = 1; if (isdigit((unsigned char)*p)) digits = 1; continue; }
        if (run >= 32 && letters && digits) return 1;
        if (!*p) break;
        run = 0; letters = digits = 0;
    }
    return 0;
}
static int is_reference(const char *v) {
    static const char *const schemes[] = {"op://","keychain:","bonfyre-credentials:","vault:","1password:","paper:","file:",NULL};
    if (!*v) return 1;
    for (int i = 0; schemes[i]; i++) if (!strncmp(v, schemes[i], strlen(schemes[i]))) return 1;
    return 0;
}

static int check(const char *dir, Table *p, Table *t) {
    char pp[1024], tp[1024];
    snprintf(pp, sizeof(pp), "%s/providers.tsv", dir); snprintf(tp, sizeof(tp), "%s/acquisition.tsv", dir);
    if (!load(pp, p) || !load(tp, t) || !header_ok(pp, p, provider_cols, NP) || !header_ok(tp, t, tracker_cols, NT)) return 0;
    for (int i = 1; i < p->n; i++) {
        Row *r = &p->row[i];
        if (r->n != NP) { fail(pp, r->line, "wrong number of columns", NULL); continue; }
        if (atoi(r->cell[0]) != i) fail(pp, r->line, "rank out of order", r->cell[0]);
        if (!*r->cell[1] || strspn(r->cell[1], "abcdefghijklmnopqrstuvwxyz0123456789-") != strlen(r->cell[1])) fail(pp, r->line, "slug must be lowercase words joined by hyphens", r->cell[1]);
        for (int j = 1; j < i; j++) if (!strcmp(p->row[j].cell[1], r->cell[1])) fail(pp, r->line, "duplicate provider", r->cell[1]);
        if (!*r->cell[2]) fail(pp, r->line, "provider name missing", NULL);
        if (!in(families, r->cell[3])) fail(pp, r->line, "unknown family", r->cell[3]);
        if (strlen(r->cell[4]) != 1 || !strchr("ABCD", r->cell[4][0])) fail(pp, r->line, "wave must be A, B, C or D", r->cell[4]);
        if (!*r->cell[5] || strspn(r->cell[5], "AOMPKDCS") != strlen(r->cell[5])) fail(pp, r->line, "tags may only use A O M P K D C S", r->cell[5]);
        /* A named customer container is a research claim, so it carries the
         * page it came from; unverified carries nothing. */
        if (strcmp(r->cell[7], "unverified") && strncmp(r->cell[9], "https://", 8) && strcmp(r->cell[9], "source thread 2026-10-26"))
            fail(pp, r->line, "a tenancy primitive needs evidence (an https source)", r->cell[1]);
        for (int c = 0; c < NP; c++) if (c != 9 && looks_secret(r->cell[c])) fail(pp, r->line, "a credential-like value does not belong in the registry", provider_cols[c]);
    }
    {
        char rp[1024]; snprintf(rp, sizeof(rp), "%s/routes.tsv", dir);
        if (!load(rp, &routes) || !header_ok(rp, &routes, route_cols, NR)) return 0;
        for (int i = 1; i < routes.n; i++) {
            Row *r = &routes.row[i]; int known = 0;
            if (r->n != NR) { fail(rp, r->line, "wrong number of columns", NULL); continue; }
            for (int j = 1; j < p->n && !known; j++) known = p->row[j].n == NP && !strcmp(p->row[j].cell[1], r->cell[0]);
            if (!known) fail(rp, r->line, "slug is not in providers.tsv", r->cell[0]);
            if (!in(steps, r->cell[1])) fail(rp, r->line, "unknown step", r->cell[1]);
            if (strncmp(r->cell[2], "https://", 8) || strchr(r->cell[2], ' ')) fail(rp, r->line, "a route must be one https address", r->cell[2]);
            if (strcmp(r->cell[3], "loads") && strcmp(r->cell[3], "after-sign-in")) fail(rp, r->line, "check must be loads or after-sign-in", r->cell[3]);
            if (strlen(r->cell[4]) != 10) fail(rp, r->line, "checked must be a date", r->cell[4]);
            if (strchr(r->cell[2], '?') && looks_secret(strchr(r->cell[2], '?'))) fail(rp, r->line, "a route query carries a credential", r->cell[0]);
            for (int j = 1; j < i; j++) if (!strcmp(routes.row[j].cell[0], r->cell[0]) && !strcmp(routes.row[j].cell[1], r->cell[1])) fail(rp, r->line, "duplicate step", r->cell[1]);
        }
    }
    {
        /* Signup pages that read the email from their own address, each tried
         * with a placeholder on the checked date and never submitted. */
        char fp[1024]; snprintf(fp, sizeof(fp), "%s/prefill.tsv", dir);
        if (!load(fp, &prefill) || !header_ok(fp, &prefill, prefill_cols, 3)) return 0;
        for (int i = 1; i < prefill.n; i++) {
            Row *r = &prefill.row[i]; int signup = 0;
            if (r->n != 3) { fail(fp, r->line, "wrong number of columns", NULL); continue; }
            for (int j = 1; j < routes.n && !signup; j++) signup = !strcmp(routes.row[j].cell[0], r->cell[0]) && !strcmp(routes.row[j].cell[1], "signup") && !strchr(routes.row[j].cell[2], '?');
            if (!signup) fail(fp, r->line, "prefill needs a signup route with no query of its own", r->cell[0]);
            if (!*r->cell[1] || strspn(r->cell[1], "abcdefghijklmnopqrstuvwxyz_") != strlen(r->cell[1])) fail(fp, r->line, "param must be a plain field name", r->cell[1]);
        }
    }
    if (t->n != p->n) fail(tp, 0, "the tracker must hold one row per provider", NULL);
    for (int i = 1; i < t->n; i++) {
        Row *r = &t->row[i]; int found = 0;
        if (r->n != NT) { fail(tp, r->line, "wrong number of columns", NULL); continue; }
        for (int j = 1; j < p->n && !found; j++) found = p->row[j].n == NP && !strcmp(p->row[j].cell[1], r->cell[0]);
        if (!found) fail(tp, r->line, "slug is not in providers.tsv", r->cell[0]);
        for (int j = 1; j < i; j++) if (!strcmp(t->row[j].cell[0], r->cell[0])) fail(tp, r->line, "duplicate tracker row", r->cell[0]);
        if (stage_index(r->cell[1]) < 0) fail(tp, r->line, "unknown stage", r->cell[1]);
        if (*r->cell[2] && (!strchr(r->cell[2], '@') || strchr(r->cell[2], ' '))) fail(tp, r->line, "owner_email is not an address", r->cell[2]);
        if (!in(billing_v, r->cell[5])) fail(tp, r->line, "billing must be unknown, none, card, invoice or trial", r->cell[5]);
        if (!in(tri, r->cell[6])) fail(tp, r->line, "two_factor must be unknown, yes or no", r->cell[6]);
        if (!in(kyb_v, r->cell[10])) fail(tp, r->line, "unknown kyb state", r->cell[10]);
        if (!in(tri, r->cell[11])) fail(tp, r->line, "domain_verified must be unknown, yes or no", r->cell[11]);
        if (!in(partner_v, r->cell[12])) fail(tp, r->line, "unknown partner_status", r->cell[12]);
        for (int c = 14; c <= 19; c++) if (!in(tri, r->cell[c])) fail(tp, r->line, "tenancy answers must be unknown, yes or no", tracker_cols[c]);
        if (!is_reference(r->cell[7])) fail(tp, r->line, "recovery_location must name where recovery material is kept (op://, keychain:, vault:, paper:)", NULL);
        if (!is_reference(r->cell[9])) fail(tp, r->line, "secret_reference must name where the secret is kept, never the secret", NULL);
        for (int c = 0; c < NT; c++) if (looks_secret(r->cell[c])) fail(tp, r->line, "this cell reads like a credential; keep only a reference to it", tracker_cols[c]);
        /* A stage claims what the earlier steps established. */
        {
            int s = stage_index(r->cell[1]);
            if (s >= 1 && !*r->cell[2]) fail(tp, r->line, "an account stage needs owner_email", r->cell[0]);
            if (s >= 3 && strcmp(r->cell[6], "yes")) fail(tp, r->line, "secured needs two_factor yes", r->cell[0]);
            if (s >= 3 && !*r->cell[7]) fail(tp, r->line, "secured needs recovery_location", r->cell[0]);
            if (s >= 5 && !*r->cell[3] && !*r->cell[4]) fail(tp, r->line, "recorded needs an account or organization id", r->cell[0]);
            if (s == 6 && strcmp(r->cell[12], "submitted") && strcmp(r->cell[12], "contacted") && strcmp(r->cell[12], "approved") && strcmp(r->cell[12], "refused"))
                fail(tp, r->line, "partner_submitted needs a partner_status past bookmarked", r->cell[0]);
        }
    }
    return errors == 0;
}

static const Row *tracker_for(const Table *t, const char *slug) {
    for (int i = 1; i < t->n; i++) if (t->row[i].n == NT && !strcmp(t->row[i].cell[0], slug)) return &t->row[i];
    return NULL;
}

static void summary(const Table *p, const Table *t) {
    bf_jsonw w; int by_stage[8] = {0}, by_wave[4] = {0}, needs[4] = {0}, partner = 0, known_tenancy = 0, answered = 0;
    for (int i = 1; i < p->n; i++) {
        const Row *r = &p->row[i], *a = tracker_for(t, r->cell[1]);
        by_wave[r->cell[4][0] - 'A']++;
        if (strchr(r->cell[5], 'D')) needs[0]++;
        if (strchr(r->cell[5], 'C')) needs[1]++;
        if (strchr(r->cell[5], 'K')) needs[2]++;
        if (strchr(r->cell[5], 'S')) needs[3]++;
        if (strcmp(r->cell[7], "unverified")) known_tenancy++;
        if (a) {
            int s = stage_index(a->cell[1]); if (s >= 0) by_stage[s]++;
            if (!strcmp(a->cell[12], "submitted") || !strcmp(a->cell[12], "contacted") || !strcmp(a->cell[12], "approved")) partner++;
            for (int c = 14; c <= 19; c++) if (strcmp(a->cell[c], "unknown")) { answered++; break; }
        }
    }
    bf_jsonw_init(&w, 0); bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "providers"); bf_jsonw_int(&w, p->n - 1);
    bf_jsonw_key(&w, "waves"); bf_jsonw_obj(&w);
    for (int i = 0; i < 4; i++) { char k[2] = {(char)('A' + i), 0}; bf_jsonw_key(&w, k); bf_jsonw_int(&w, by_wave[i]); }
    bf_jsonw_obj_end(&w);
    bf_jsonw_key(&w, "stages"); bf_jsonw_obj(&w);
    for (int i = 0; stages[i]; i++) { bf_jsonw_key(&w, stages[i]); bf_jsonw_int(&w, by_stage[i]); }
    bf_jsonw_obj_end(&w);
    bf_jsonw_key(&w, "needs"); bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "company_domain"); bf_jsonw_int(&w, needs[0]);
    bf_jsonw_key(&w, "card"); bf_jsonw_int(&w, needs[1]);
    bf_jsonw_key(&w, "business_verification"); bf_jsonw_int(&w, needs[2]);
    bf_jsonw_key(&w, "sales_approval"); bf_jsonw_int(&w, needs[3]);
    bf_jsonw_obj_end(&w);
    bf_jsonw_key(&w, "partner_applications_out"); bf_jsonw_int(&w, partner);
    bf_jsonw_key(&w, "tenancy_primitive_known"); bf_jsonw_int(&w, known_tenancy);
    bf_jsonw_key(&w, "tenancy_questions_started"); bf_jsonw_int(&w, answered);
    bf_jsonw_obj_end(&w);
    fwrite(w.buf, 1, w.len, stdout); fputc('\n', stdout); bf_jsonw_free(&w);
}

static const char *wave_title(char wave) { for (int i = 0; wave_words[i]; i += 2) if (wave_words[i][0] == wave) return wave_words[i + 1]; return ""; }

/* The next accounts to open, in the chat's order, and what to have in hand. */
static void next(const Table *p, const Table *t, int limit, char only) {
    int shown = 0; char current = 0;
    for (char wave = 'A'; wave <= 'D' && shown < limit; wave++) {
        if (only && wave != only) continue;
        for (int i = 1; i < p->n && shown < limit; i++) {
            const Row *r = &p->row[i], *a = tracker_for(t, r->cell[1]); int s = a ? stage_index(a->cell[1]) : 0;
            const char *tags = r->cell[5];
            if (r->cell[4][0] != wave || s >= 5) continue;
            if (current != wave) { printf("%sWave %c: %s\n", current ? "\n" : "", wave, wave_title(wave)); current = wave; }
            printf("  %s  (now: %s)\n    have ready:", r->cell[2], a ? a->cell[1] : "none");
            printf(" verified email%s%s%s%s\n", strchr(tags, 'D') ? ", company-domain mailbox" : "", strchr(tags, 'C') ? ", card" : "",
                   strchr(tags, 'K') ? ", legal entity and tax ID (business verification)" : "", strchr(tags, 'S') ? ", expect a sales conversation" : "");
            printf("    finish line: Aurekai organization, two-factor, recovery saved, test container%s%s\n",
                   strchr(tags, 'M') ? ", one machine identity" : "", strchr(tags, 'P') ? ", partner application" : "");
            if (strcmp(r->cell[7], "unverified")) printf("    customer container: %s\n      (%s)\n", r->cell[7], r->cell[9]);
            if (strchr(tags, 'P')) printf("    partner route: %s\n", r->cell[8]);
            for (int k = 0; steps[k]; k++) for (int j = 1; j < routes.n; j++)
                if (!strcmp(routes.row[j].cell[0], r->cell[1]) && !strcmp(routes.row[j].cell[1], steps[k]))
                    printf("    %-12s %s\n", steps[k], routes.row[j].cell[2]);
            shown++;
        }
    }
    if (!shown) puts("Every provider in this selection is recorded or further.");
}


/* One checklist goal in the operator store the account work index reads. One
 * row per provider buried the agents' own work under 102 sign-ups, so the
 * board carries a single item. It is ordered by the end goal, customers
 * managing their own accounts through Aurekai: what customers can use now,
 * which held accounts still need their customer side built, then the rest,
 * each opened at the page that makes a key (an existing GitHub or Google
 * sign-in answers there) rather than at a sign-up form. */
static const char *route_for(const char *slug, const char *step) {
    for (int j = 1; j < routes.n; j++) if (!strcmp(routes.row[j].cell[0], slug) && !strcmp(routes.row[j].cell[1], step)) return routes.row[j].cell[2];
    return NULL;
}
static int held(const Row *a) { return a && a->cell[8][0] && strstr(a->cell[20], "verified"); }
/* Customers can use it once a customer container or delegated access was made
 * and closed again against the real provider, which the notes record. */
static int customer_live(const Row *a) { return held(a) && (!strcmp(a->cell[15], "yes") || !strcmp(a->cell[14], "yes")) && strstr(a->cell[20], "customer"); }
static int publish(const Table *p, const Table *t, const char *db_path, char only) {
    sqlite3 *db = NULL; sqlite3_stmt *up = NULL; size_t cap = 65536, w = 0; int open = 0, total = 0, connected = 0, live = 0; double now = (double)time(NULL);
    char *details = malloc(cap);
    static const char *entry[] = {"connect", "token", "app", "organization", "signup", NULL};
    (void)only;
    if (!details) return 1;
    details[0] = 0; total = p->n - 1;
    for (int pass = 0; pass < 3; pass++) {
        int first = 1;
        for (int i = 1; i < p->n && w < cap - 1024; i++) {
            const Row *r = &p->row[i], *a = tracker_for(t, r->cell[1]);
            if (a && stage_index(a->cell[1]) >= 5) continue;
            int kind = customer_live(a) ? 0 : held(a) ? 1 : 2;
            if (kind != pass) continue;
            if (pass == 0) connected++, live++;
            else if (pass == 1) connected++;
            else open++;
            if (first) {
                static const char *heads[] = {
                    "Customers can use these now (connect, check access, revoke from Aurekai):",
                    "Account held and answering; the customer side is the next build:",
                    "Not connected yet. Each link opens where the key is made; sign in with GitHub or Google if asked:"};
                w += (size_t)snprintf(details + w, cap - w, "%s%s\n", w ? "\n" : "", heads[pass]); first = 0;
            }
            const char *what = strcmp(r->cell[7], "unverified") ? r->cell[7] : r->cell[6];
            if (pass == 0) {
                const char *c = route_for(r->cell[1], "connect"); if (c && strncmp(c, "https://aurekai.work/connect/", 29)) c = NULL;
                if (c) w += (size_t)snprintf(details + w, cap - w, "%s: customers get %s. %s\n", r->cell[2], what, c);
                else w += (size_t)snprintf(details + w, cap - w, "%s: customers get %s, opened and closed by Aurekai (bonfyre-provider-estate customer open|show|close --provider %s)\n", r->cell[2], what, r->cell[1]);
            } else if (pass == 1) {
                if (!strncmp(what, "none", 4)) w += (size_t)snprintf(details + w, cap - w, "%s: kept for Aurekai's own use; %s\n", r->cell[2], what);
                else w += (size_t)snprintf(details + w, cap - w, "%s: build customers' %s on the held key\n", r->cell[2], what);
            } else {
                const char *go = NULL;
                for (int k = 0; entry[k] && !go; k++) go = route_for(r->cell[1], entry[k]);
                w += (size_t)snprintf(details + w, cap - w, "%s (gives customers %s): %s\n", r->cell[2], what, go ? go : "no page mapped");
            }
        }
    }
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READWRITE, NULL) != SQLITE_OK) { fprintf(stderr, "cannot open %s\n", db_path); free(details); sqlite3_close(db); return 1; }
    sqlite3_busy_timeout(db, 5000);
    if (sqlite3_prepare_v2(db,
          "INSERT INTO goals(id,dedupe_key,title,details,priority,state,source,created_at,updated_at) VALUES('provider-estate-checklist','provider-estate:checklist',?1,?2,40,?3,'provider-estate-checklist',?4,?4) "
          "ON CONFLICT(dedupe_key) DO UPDATE SET title=excluded.title,details=excluded.details,state=excluded.state,"
          "updated_at=CASE WHEN goals.details=excluded.details THEN goals.updated_at ELSE excluded.updated_at END", -1, &up, NULL) != SQLITE_OK) {
        fprintf(stderr, "operator store refused: %s\n", sqlite3_errmsg(db)); free(details); sqlite3_close(db); return 1;
    }
    char title[160]; snprintf(title, sizeof(title), "Customer accounts: %d of %d providers usable by customers, %d more connected, %d to connect", live, total, connected - live, open);
    sqlite3_bind_text(up, 1, title, -1, SQLITE_STATIC); sqlite3_bind_text(up, 2, details, -1, SQLITE_STATIC);
    sqlite3_bind_text(up, 3, open ? "blocked" : "resolved", -1, SQLITE_STATIC); sqlite3_bind_double(up, 4, now);
    int rc = sqlite3_step(up) == SQLITE_DONE ? 0 : 1;
    if (rc) fprintf(stderr, "operator store refused the checklist: %s\n", sqlite3_errmsg(db));
    sqlite3_finalize(up); sqlite3_close(db); free(details);
    if (!rc) printf("provider estate: one checklist task, %d connected, %d of %d still open\n", connected, open, total);
    return rc;
}
#include "customer.h"

/* The provider accounts Aurekai holds, for the holding organization's page:
 * name, what customers get, and whether customers can use it yet. Identifiers
 * and references only, as the tracker keeps them. */
static void held_list(const Table *p, const Table *t) {
    bf_jsonw w; bf_jsonw_init(&w, 0); bf_jsonw_arr(&w);
    for (int i = 1; i < p->n; i++) {
        const Row *r = &p->row[i], *a = tracker_for(t, r->cell[1]);
        if (!held(a)) continue;
        bf_jsonw_obj(&w);
        bf_jsonw_key(&w, "slug"); bf_jsonw_str(&w, r->cell[1]);
        bf_jsonw_key(&w, "name"); bf_jsonw_str(&w, r->cell[2]);
        bf_jsonw_key(&w, "account"); bf_jsonw_str(&w, a->cell[3][0] ? a->cell[3] : a->cell[4]);
        bf_jsonw_key(&w, "customers"); bf_jsonw_str(&w, customer_live(a) ? "usable" : "to build");
        bf_jsonw_key(&w, "gives"); bf_jsonw_str(&w, strcmp(r->cell[7], "unverified") ? r->cell[7] : r->cell[6]);
        bf_jsonw_obj_end(&w);
    }
    bf_jsonw_arr_end(&w); fwrite(w.buf, 1, w.len, stdout); fputc('\n', stdout); bf_jsonw_free(&w);
}

static void matrix(const Table *p, const Table *t) {
    puts("provider\tcreate_container\tdelegate\toperate\tmeter\trevoke\ttransfer\ttenancy_primitive");
    for (int i = 1; i < p->n; i++) {
        const Row *r = &p->row[i], *a = tracker_for(t, r->cell[1]);
        printf("%s", r->cell[2]);
        for (int c = 14; c <= 19; c++) printf("\t%s", a ? a->cell[c] : "unknown");
        printf("\t%s\n", r->cell[7]);
    }
}

int main(int argc, char **argv) {
    const char *dir = ".", *cmd = argc > 1 ? argv[1] : "", *operator_db = NULL; int limit = 12; char wave = 0;
    static Table providers, tracker;
    if (!strcmp(cmd, "customer")) return customer_cmd(argc, argv);
    for (int i = 2; i < argc; i++) {
        if (!strcmp(argv[i], "--dir") && i + 1 < argc) dir = argv[++i];
        else if (!strcmp(argv[i], "--limit") && i + 1 < argc) limit = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--operator-db") && i + 1 < argc) operator_db = argv[++i];
        else if (!strcmp(argv[i], "--wave") && i + 1 < argc && strlen(argv[i + 1]) == 1 && strchr("ABCD", argv[i + 1][0])) wave = argv[++i][0];
        else { fprintf(stderr, "unknown argument %s\n", argv[i]); return 2; }
    }
    if (strcmp(cmd, "check") && strcmp(cmd, "summary") && strcmp(cmd, "next") && strcmp(cmd, "matrix") && strcmp(cmd, "publish") && strcmp(cmd, "held")) {
        fputs("usage: bonfyre-provider-estate check|summary|next|matrix|publish [--dir DIR] [--operator-db FILE] [--limit N] [--wave A|B|C|D]\n", stderr);
        return 2;
    }
    if (limit < 1) limit = 1;
    if (!check(dir, &providers, &tracker)) { fprintf(stderr, "provider estate: %d problem%s\n", errors, errors == 1 ? "" : "s"); return 1; }
    if (!strcmp(cmd, "check")) printf("provider estate: %d providers, tracker consistent, no credentials found\n", providers.n - 1);
    else if (!strcmp(cmd, "summary")) summary(&providers, &tracker);
    else if (!strcmp(cmd, "next")) next(&providers, &tracker, limit, wave);
    else if (!strcmp(cmd, "held")) held_list(&providers, &tracker);
    else if (!strcmp(cmd, "publish")) {
        if (!operator_db) { fputs("publish needs --operator-db\n", stderr); return 2; }
        int rc = publish(&providers, &tracker, operator_db, wave); free(providers.text); free(tracker.text); free(routes.text); free(prefill.text); return rc;
    }
    else matrix(&providers, &tracker);
    free(providers.text); free(tracker.text); free(routes.text); free(prefill.text);
    return 0;
}
