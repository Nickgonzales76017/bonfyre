#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sqlite3.h>
#include "../src/dateplan_projection.h"

static void check(int ok, const char *message) {
    if (!ok) { fprintf(stderr, "dateplan projection FAIL: %s\n", message); exit(1); }
}

static void execute(sqlite3 *db, const char *sql) {
    char *error = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &error) != SQLITE_OK) {
        fprintf(stderr, "SQL: %s\n", error ? error : "unknown");
        sqlite3_free(error);
        exit(1);
    }
}

static char *slurp(const char *path, size_t cap) {
    FILE *f = fopen(path, "rb");
    char *buf;
    size_t n;
    long size;
    if (!f) return NULL;
    if (fseek(f, 0, SEEK_END) != 0 || (size = ftell(f)) < 0 || (size_t)size > cap) {
        fclose(f);
        return NULL;
    }
    rewind(f);
    buf = malloc((size_t)size + 1);
    if (!buf) {
        fclose(f);
        return NULL;
    }
    n = fread(buf, 1, (size_t)size, f);
    fclose(f);
    if (n != (size_t)size) {
        free(buf);
        return NULL;
    }
    buf[n] = '\0';
    return buf;
}

int main(void) {
    char prompt[BF_DATEPLAN_PROMPT_CAP], sha[65];
    BfDatePlanResult result;
    char *golden;
    check(bf_dateplan_prompt("Dallas, TX", "first date", 0, 60, "hiking, coffee",
                             prompt, sizeof(prompt), sha) == 0, "prompt builds");
    check(strstr(prompt, "Dallas, TX") != NULL, "prompt carries location");
    check(strstr(prompt, "Budget: 0 to 60") != NULL, "prompt carries budget");
    check(strstr(prompt, "cost_min") != NULL, "prompt demands costs");
    check(strstr(prompt, "safety_note") != NULL, "prompt demands safety notes");
    check(strlen(sha) == 64, "prompt hashed");
    check(bf_dateplan_prompt("Dallas", "x", 60, 0, "", prompt, sizeof(prompt), sha) == -1,
          "inverted budget refused");
    check(bf_dateplan_prompt("Dallas", "x", 0, 100001, "", prompt, sizeof(prompt), sha) == -1,
          "huge budget refused");
    check(bf_dateplan_prompt("", "x", 0, 60, "", prompt, sizeof(prompt), sha) == -1,
          "empty location refused");
    golden = slurp("tests/fixtures/dateplan-golden-1.json", 32768);
    if (!golden) golden = slurp("cmd/BonfyreWebAddress/tests/fixtures/dateplan-golden-1.json", 32768);
    check(golden != NULL, "golden fixture loads");
    memset(&result, 0, sizeof(result));
    check(bf_dateplan_verify(golden, 60, "qwen-test", sha, &result) == 1, "golden verifies");
    check(result.plans_admitted == 2 && result.plans_dropped == 1, "over-budget dropped");
    check(strstr(result.admitted, "Lakeside coffee walk") != NULL, "admitted kept");
    check(strstr(result.admitted, "Steakhouse evening") == NULL, "over-budget cut");
    check(strstr(result.admitted, "backup_plan") != NULL, "backup kept");
    check(strlen(result.output_sha256) == 64, "output hashed");
    check(!strcmp(result.prompt_sha256, sha), "prompt receipt kept");
    check(!strcmp(result.model, "qwen-test"), "model kept");
    check(bf_dateplan_verify(golden, 0, "qwen-test", sha, &result) == 1 &&
          result.plans_admitted == 1, "zero budget admits free plan");
    check(bf_dateplan_verify("not json", 60, "qwen-test", sha, &result) == 0,
          "garbage admits nothing");
    check(bf_dateplan_verify("{\"plans\":[]}", 60, "qwen-test", sha, &result) == 0,
          "empty plans admit nothing");
    check(bf_dateplan_verify("{\"plans\":[{\"title\":\"t\"}],\"backup_plan\":{}}", 60,
                             "qwen-test", sha, &result) == 0, "shapeless admits nothing");
    {
        /* A plan without a safety note must not survive. */
        const char *nosafe = "{\"plans\":[{\"title\":\"t\",\"venue\":\"v\",\"time\":\"w\",\"reason\":\"r\","
                             "\"cost_min\":0,\"cost_max\":5}],\"backup_plan\":{\"title\":\"b\",\"venue\":\"v\","
                             "\"time\":\"w\",\"reason\":\"r\"}}";
        check(bf_dateplan_verify(nosafe, 60, "qwen-test", sha, &result) == 0,
              "missing safety note refused");
    }
    free(golden);
    {
        sqlite3 *db = NULL;
        char *html = NULL;
        size_t len = 0;
        FILE *mem;
        check(sqlite3_open(":memory:", &db) == SQLITE_OK, "history db open");
        mem = open_memstream(&html, &len);
        check(mem != NULL, "open history memory");
        check(bf_dateplan_history(mem, db, "person:a", 24) == 0, "missing table has no history");
        check(fclose(mem) == 0, "close history memory");
        check(html == NULL || html[0] == '\0', "missing table renders silent");
        free(html);
        sqlite3_close(db);
    }
    {
        sqlite3 *db = NULL;
        char *html = NULL;
        size_t len = 0;
        FILE *mem;
        check(sqlite3_open(":memory:", &db) == SQLITE_OK, "history rows db open");
        execute(db, "CREATE TABLE date_plan (id INTEGER PRIMARY KEY, plan TEXT, location TEXT, intent TEXT,"
                    "budget_min TEXT, budget_max TEXT, interests TEXT, plan_json TEXT,"
                    "costs_admitted TEXT, accepted TEXT, generation_model TEXT,"
                    "created_at TEXT, actor_subject TEXT, status TEXT);"
                    "INSERT INTO date_plan VALUES(NULL,'p1','Dallas','first date','0','60','hiking',"
                    "'{\"plans\":[{\"title\":\"Lakeside picnic\"}]}','12','true','stub','2026-11-11',"
                    "'person:a','published');");
        mem = open_memstream(&html, &len);
        check(mem != NULL, "open history rows memory");
        check(bf_dateplan_history(mem, db, "person:a", 24) == 1, "one plan renders");
        check(fclose(mem) == 0, "close history rows memory");
        check(strstr(html, "ak-pattern ak-card") != NULL, "history speaks components");
        check(strstr(html, "Lakeside picnic") != NULL, "first plan title shown");
        check(strstr(html, ">Accepted</span>") != NULL, "acceptance shown");
        check(strstr(html, "admitted 12") != NULL, "admitted costs shown");
        free(html);
        sqlite3_close(db);
    }
    {
        char catalog[] = "/tmp/bonfyre-dateplan-catalog-XXXXXX";
        char prices[] = "/tmp/bonfyre-dateplan-prices-XXXXXX";
        char *saved_model = getenv("BONFYRE_DATEPLAN_MODEL");
        char *saved_catalog = getenv("BONFYRE_MODEL_CATALOG");
        char *saved_prices = getenv("BONFYRE_PRICES_DB");
        sqlite3 *cdb = NULL, *pdb = NULL;
        double in = 0, out = 0;
        char at[16];
        int cfd = mkstemp(catalog), pfd = mkstemp(prices);
        check(cfd >= 0 && pfd >= 0, "temp stores");
        close(cfd);
        close(pfd);
        check(sqlite3_open(catalog, &cdb) == SQLITE_OK, "catalog open");
        execute(cdb, "CREATE TABLE catalog_edges(src_node_id TEXT, rel TEXT, dst_node_id TEXT, meta TEXT);"
                     "INSERT INTO catalog_edges VALUES"
                     "('capability:dateplan-generate','uses_model','model:local-test','');");
        sqlite3_close(cdb);
        check(sqlite3_open(prices, &pdb) == SQLITE_OK, "prices open");
        execute(pdb, "CREATE TABLE observed_price(item TEXT, amount REAL, currency TEXT, unit TEXT,"
                     "publisher TEXT, observed_at TEXT, kind TEXT, source TEXT, evidence_ref TEXT);"
                     "INSERT INTO observed_price VALUES"
                     "('tiny-test','0.000001','USD','token in','Router T','2026-11-01','inference','s','e'),"
                     "('tiny-test','0.000002','USD','token out','Router T','2026-11-01','inference','s','e');");
        sqlite3_close(pdb);
        setenv("BONFYRE_MODEL_CATALOG", catalog, 1);
        setenv("BONFYRE_PRICES_DB", prices, 1);
        unsetenv("BONFYRE_DATEPLAN_MODEL");
        check(bf_dateplan_price("tiny-test", &in, &out, at, sizeof(at)) == 1 &&
              in > 0 && out > 0 && !strcmp(at, "2026-11-01"), "observed rates read");
        check(bf_dateplan_price("nope", &in, &out, at, sizeof(at)) == 0, "missing rate absent");
        {
            BfDatePlanResult unavailable;
            memset(&unavailable, 0, sizeof(unavailable));
            setenv("BONFYRE_QWEN_BINARY", "/nonexistent-qwen-test-binary", 1);
            check(bf_dateplan_generate(NULL, "Dallas, TX", "first date", 0, 60, "hiking",
                                       &unavailable) == -2, "failed runner unavailable");
            check(unavailable.unavail_reason[0] != '\0', "unavailable reason kept");
            check(strstr(unavailable.unavail_reason, "no generation model selected") == NULL,
                  "catalogue edge resolved the model");
            unsetenv("BONFYRE_QWEN_BINARY");
        }
        if (saved_model) setenv("BONFYRE_DATEPLAN_MODEL", saved_model, 1);
        else unsetenv("BONFYRE_DATEPLAN_MODEL");
        if (saved_catalog) setenv("BONFYRE_MODEL_CATALOG", saved_catalog, 1);
        if (saved_prices) setenv("BONFYRE_PRICES_DB", saved_prices, 1);
        unlink(catalog);
        unlink(prices);
    }
    {
        BfDatePlanResult live;
        char *saved = getenv("BONFYRE_DATEPLAN_MODEL");
        if (!saved || !*saved) {
            puts("dateplan live generation skipped: BONFYRE_DATEPLAN_MODEL unset");
        } else {
            int rc = bf_dateplan_generate(NULL, "Dallas, TX", "first date", 0, 60,
                                          "hiking", &live);
            if (rc == -2) {
                puts("dateplan live generation skipped: no model available");
            } else {
                check(rc == 1, "live generation admits");
                check(live.plans_admitted >= 1, "live admits a plan");
                printf("dateplan live: model=%s admitted=%d dropped=%d ms=%ld\n",
                       live.model, live.plans_admitted, live.plans_dropped, live.duration_ms);
            }
        }
    }
    puts("dateplan projection: PASS");
    return 0;
}
