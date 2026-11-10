#define _POSIX_C_SOURCE 200809L

#include "bf_control.h"
#include "bf_command_lookup.h"

#include <ctype.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "bf_lex.h"
/* Delegates to the one implementation. Three byte-identical copies of this
 * existed; they agreed, which was luck rather than design. */
static const char *option_value(int argc, char **argv, const char *option)
{
    return bf_lex_argv_option(argc, argv, option);
}

static int64_t days_from_civil(int64_t year, int64_t month, int64_t day)
{
    int64_t era;
    int64_t year_of_era;
    int64_t day_of_year;
    int64_t day_of_era;
    year -= month <= 2;
    era = (year >= 0 ? year : year - 399) / 400;
    year_of_era = year - era * 400;
    day_of_year = (153 * (month + (month > 2 ? -3 : 9)) + 2) / 5 + day - 1;
    day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    return era * 146097 + day_of_era - 719468;
}

static int parse_iso_ms(const char *text, int64_t *out)
{
    int year, month, day, hour, minute, second;
    char separator;
    if (!text || sscanf(text, "%4d-%2d-%2d%c%2d:%2d:%2d", &year, &month, &day,
                        &separator, &hour, &minute, &second) != 7 ||
        (separator != 'T' && separator != ' ') || month < 1 || month > 12 ||
        day < 1 || day > 31 || hour < 0 || hour > 23 || minute < 0 || minute > 59 ||
        second < 0 || second > 59) return -1;
    *out = ((days_from_civil(year, month, day) * 24 + hour) * 60 + minute) * 60000LL +
           (int64_t)second * 1000LL;
    return 0;
}

static void now_iso(char out[40])
{
    time_t current = time(NULL);
    struct tm utc;
    gmtime_r(&current, &utc);
    strftime(out, 40, "%Y-%m-%dT%H:%M:%S+00:00", &utc);
}

static BfAttention attention_from_name(const char *name, int *ok)
{
    *ok = 1;
    if (strcmp(name, "hot") == 0) return BF_ATTENTION_HOT;
    if (strcmp(name, "warm") == 0) return BF_ATTENTION_WARM;
    if (strcmp(name, "cool") == 0) return BF_ATTENTION_COOL;
    if (strcmp(name, "cold") == 0) return BF_ATTENTION_COLD;
    *ok = 0;
    return BF_ATTENTION_WARM;
}

static int apply_schema(sqlite3 *db)
{
    return sqlite3_exec(db,
        "PRAGMA journal_mode=WAL;"
        "CREATE TABLE IF NOT EXISTS watches("
        "watch_id TEXT PRIMARY KEY,subject TEXT NOT NULL,"
        "temperature TEXT NOT NULL DEFAULT 'warm',reheat_at TEXT,reheat_on TEXT NOT NULL DEFAULT '',"
        "attempts INTEGER NOT NULL DEFAULT 0,last_checked_at TEXT,last_change_at TEXT,"
        "detail TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL);"
        "CREATE INDEX IF NOT EXISTS idx_watches_due ON watches(temperature,reheat_at);",
        NULL, NULL, NULL) == SQLITE_OK ? 0 : -1;
}

static int schedule_watch(sqlite3 *db, int argc, char **argv)
{
    const char *id = option_value(argc, argv, "--watch");
    const char *subject = option_value(argc, argv, "--subject");
    const char *temperature = option_value(argc, argv, "--temperature");
    const char *reheat_at = option_value(argc, argv, "--reheat-at");
    const char *reheat_on = option_value(argc, argv, "--reheat-on");
    const char *detail = option_value(argc, argv, "--detail");
    const char *at = option_value(argc, argv, "--at");
    sqlite3_stmt *statement = NULL;
    char generated[40];
    int valid;
    BfAttention parsed;
    int64_t ignored;
    if (!id || !subject) return -1;
    if (!temperature) temperature = "warm";
    parsed = attention_from_name(temperature, &valid);
    if (!valid || (reheat_at && parse_iso_ms(reheat_at, &ignored) != 0) ||
        (at && parse_iso_ms(at, &ignored) != 0) ||
        (parsed == BF_ATTENTION_COOL && (!reheat_at && (!reheat_on || !reheat_on[0])))) return -1;
    if (!at) {
        now_iso(generated);
        at = generated;
    }
    if (sqlite3_prepare_v2(db,
        "INSERT INTO watches(watch_id,subject,temperature,reheat_at,reheat_on,detail,created_at)"
        " VALUES(?,?,?,?,?,?,?) ON CONFLICT(watch_id) DO UPDATE SET"
        " subject=excluded.subject,temperature=excluded.temperature,reheat_at=excluded.reheat_at,"
        " reheat_on=excluded.reheat_on,detail=excluded.detail", -1, &statement, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(statement, 1, id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, subject, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 3, temperature, -1, SQLITE_STATIC);
    if (reheat_at) sqlite3_bind_text(statement, 4, reheat_at, -1, SQLITE_TRANSIENT); else sqlite3_bind_null(statement, 4);
    sqlite3_bind_text(statement, 5, reheat_on ? reheat_on : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 6, detail ? detail : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 7, at, -1, SQLITE_TRANSIENT);
    valid = sqlite3_step(statement) == SQLITE_DONE;
    sqlite3_finalize(statement);
    if (!valid) return -1;
    printf("{\"watch_id\":\"%s\",\"temperature\":\"%s\"}\n", id, temperature);
    return 0;
}

static int check_watch(sqlite3 *db, int argc, char **argv)
{
    const char *id = option_value(argc, argv, "--watch");
    const char *changed_text = option_value(argc, argv, "--changed");
    const char *at = option_value(argc, argv, "--at");
    sqlite3_stmt *statement = NULL;
    const char *temperature;
    const char *reheat_at;
    const char *reheat_on;
    BfAttentionState state;
    int valid;
    char generated[40];
    int64_t ignored;
    if (!id || !changed_text || (strcmp(changed_text, "true") != 0 && strcmp(changed_text, "false") != 0) ||
        (at && parse_iso_ms(at, &ignored) != 0)) return -1;
    if (!at) { now_iso(generated); at = generated; }
    if (sqlite3_prepare_v2(db, "SELECT temperature,attempts,reheat_at,reheat_on FROM watches WHERE watch_id=?", -1, &statement, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(statement, 1, id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_ROW) { sqlite3_finalize(statement); return -1; }
    temperature = (const char *)sqlite3_column_text(statement, 0);
    reheat_at = (const char *)sqlite3_column_text(statement, 2);
    reheat_on = (const char *)sqlite3_column_text(statement, 3);
    state.level = attention_from_name(temperature, &valid);
    state.unchanged_checks = sqlite3_column_int(statement, 1);
    state.has_reheat_condition = (reheat_at && reheat_at[0]) || (reheat_on && reheat_on[0]);
    sqlite3_finalize(statement);
    if (!valid) return -1;
    state = bf_attention_check(state, strcmp(changed_text, "true") == 0);
    if (sqlite3_prepare_v2(db,
        "UPDATE watches SET attempts=?,temperature=?,last_checked_at=?,"
        "last_change_at=CASE WHEN ? THEN ? ELSE last_change_at END WHERE watch_id=?", -1, &statement, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_int(statement, 1, state.unchanged_checks);
    sqlite3_bind_text(statement, 2, bf_attention_name(state.level), -1, SQLITE_STATIC);
    sqlite3_bind_text(statement, 3, at, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(statement, 4, strcmp(changed_text, "true") == 0);
    sqlite3_bind_text(statement, 5, at, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 6, id, -1, SQLITE_TRANSIENT);
    valid = sqlite3_step(statement) == SQLITE_DONE;
    sqlite3_finalize(statement);
    if (!valid) return -1;
    printf("{\"watch_id\":\"%s\",\"temperature\":\"%s\",\"unchanged_checks\":%d,\"in_context_cut\":%s}\n",
           id, bf_attention_name(state.level), state.unchanged_checks,
           bf_attention_in_context_cut(&state) ? "true" : "false");
    return 0;
}

static int reheat_watch(sqlite3 *db, int argc, char **argv)
{
    const char *id = option_value(argc, argv, "--watch");
    const char *signal = option_value(argc, argv, "--signal");
    const char *at = option_value(argc, argv, "--at");
    sqlite3_stmt *statement = NULL;
    const char *temperature;
    const char *reheat_on;
    char generated[40];
    int valid;
    int64_t ignored;
    if (!id || (at && parse_iso_ms(at, &ignored) != 0)) return -1;
    if (!at) { now_iso(generated); at = generated; }
    if (sqlite3_prepare_v2(db, "SELECT temperature,reheat_on FROM watches WHERE watch_id=?", -1, &statement, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(statement, 1, id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_ROW) { sqlite3_finalize(statement); return -1; }
    temperature = (const char *)sqlite3_column_text(statement, 0);
    reheat_on = (const char *)sqlite3_column_text(statement, 1);
    valid = strcmp(temperature, "cool") != 0 || (signal && reheat_on && strcmp(signal, reheat_on) == 0);
    sqlite3_finalize(statement);
    if (!valid) return -1;
    if (sqlite3_prepare_v2(db, "UPDATE watches SET temperature='hot',attempts=0,last_change_at=? WHERE watch_id=?", -1, &statement, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(statement, 1, at, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, id, -1, SQLITE_TRANSIENT);
    valid = sqlite3_step(statement) == SQLITE_DONE;
    sqlite3_finalize(statement);
    if (!valid) return -1;
    printf("{\"watch_id\":\"%s\",\"temperature\":\"hot\"}\n", id);
    return 0;
}

static int close_watch(sqlite3 *db, int argc, char **argv)
{
    const char *id = option_value(argc, argv, "--watch");
    sqlite3_stmt *statement = NULL;
    int valid;
    if (!id || sqlite3_prepare_v2(db, "UPDATE watches SET temperature='cold' WHERE watch_id=?", -1, &statement, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(statement, 1, id, -1, SQLITE_TRANSIENT);
    valid = sqlite3_step(statement) == SQLITE_DONE;
    sqlite3_finalize(statement);
    if (!valid) return -1;
    printf("{\"watch_id\":\"%s\",\"temperature\":\"cold\"}\n", id);
    return 0;
}

static int print_due(sqlite3 *db, int argc, char **argv)
{
    const char *at = option_value(argc, argv, "--at");
    sqlite3_stmt *statement = NULL;
    char generated[40];
    int64_t now;
    int first = 1;
    if (!at) { now_iso(generated); at = generated; }
    if (parse_iso_ms(at, &now) != 0 || sqlite3_prepare_v2(db,
        "SELECT watch_id,subject,temperature,reheat_at,attempts FROM watches WHERE temperature!='cold' ORDER BY watch_id", -1, &statement, NULL) != SQLITE_OK) return -1;
    putchar('[');
    while (sqlite3_step(statement) == SQLITE_ROW) {
        const char *id = (const char *)sqlite3_column_text(statement, 0);
        const char *subject = (const char *)sqlite3_column_text(statement, 1);
        const char *temperature = (const char *)sqlite3_column_text(statement, 2);
        const char *reheat_at = (const char *)sqlite3_column_text(statement, 3);
        int due = strcmp(temperature, "hot") == 0 || strcmp(temperature, "warm") == 0;
        int64_t deadline;
        if (!due && reheat_at && parse_iso_ms(reheat_at, &deadline) == 0 && now >= deadline) due = 1;
        if (!due) continue;
        if (!first) putchar(',');
        first = 0;
        printf("{\"watch_id\":\"%s\",\"subject\":\"%s\",\"temperature\":\"%s\",\"attempts\":%d}",
               id, subject, temperature, sqlite3_column_int(statement, 4));
    }
    sqlite3_finalize(statement);
    fputs("]\n", stdout);
    return 0;
}

static void usage(void)
{
    fputs("usage: bonfyre-attention schedule --db FILE --watch ID --subject TEXT [--temperature hot|warm|cool|cold] [--reheat-at ISO] [--reheat-on SIGNAL] [--at ISO]\n"
          "       bonfyre-attention check --db FILE --watch ID --changed true|false [--at ISO]\n"
          "       bonfyre-attention reheat --db FILE --watch ID [--signal SIGNAL] [--at ISO]\n"
          "       bonfyre-attention due --db FILE [--at ISO]\n"
          "       bonfyre-attention close --db FILE --watch ID\n", stderr);
}

#define ATTENTION_COMMANDS(X) \
    X("check", AC_CHECK) \
    X("close", AC_CLOSE) \
    X("due", AC_DUE) \
    X("reheat", AC_REHEAT) \
    X("schedule", AC_SCHEDULE)

enum AttentionCommand {
#define ATTENTION_ENUM(name, command_id) command_id,
    ATTENTION_COMMANDS(ATTENTION_ENUM)
#undef ATTENTION_ENUM
};

static const BfCommandEntry attention_commands[] = {
#define ATTENTION_ENTRY(name, command_id) {name, command_id, 0, NULL},
    ATTENTION_COMMANDS(ATTENTION_ENTRY)
#undef ATTENTION_ENTRY
};

int main(int argc, char **argv)
{
    const char *database;
    const BfCommandEntry *command;
    sqlite3 *db = NULL;
    int result;
    if (argc < 2) { usage(); return 2; }
    command = bf_command_lookup(attention_commands,
                                sizeof(attention_commands) / sizeof(attention_commands[0]), argv[1]);
    if (!command) {
        usage(); return 2;
    }
    database = option_value(argc, argv, "--db");
    if (!database || sqlite3_open(database, &db) != SQLITE_OK || apply_schema(db) != 0) {
        sqlite3_close(db); return 1;
    }
    switch (command->id) {
        case AC_SCHEDULE: result = schedule_watch(db, argc, argv); break;
        case AC_CHECK: result = check_watch(db, argc, argv); break;
        case AC_REHEAT: result = reheat_watch(db, argc, argv); break;
        case AC_DUE: result = print_due(db, argc, argv); break;
        case AC_CLOSE: result = close_watch(db, argc, argv); break;
        default: result = -1; break;
    }
    sqlite3_close(db);
    if (result != 0) { usage(); return 2; }
    return 0;
}
