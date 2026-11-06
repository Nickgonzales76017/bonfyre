#define _POSIX_C_SOURCE 200809L

#include "bf_occurrence.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include "../../../lib/libbonfyre/include/bf_help.h"
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

#define FIELD_COUNT 9
#define FIELD_MAX 2048

typedef struct Notification {
    char repo[FIELD_MAX];
    char subject_type[128];
    char title[FIELD_MAX];
    char reason[128];
    char updated_at[64];
    char api_url[FIELD_MAX];
    char last_actor[256];
    char actor_kind[32];
    char evidence_ref[FIELD_MAX];
} Notification;

static const char *BOT_LOGINS[] = {
    "vercel", "vercel[bot]", "github-actions", "github-actions[bot]",
    "codecov", "codecov[bot]", "dependabot", "dependabot[bot]", "netlify",
    "netlify[bot]", "chatgpt-codex-connector", "chatgpt-codex-connector[bot]", NULL,
};

static int same_folded(const char *left, const char *right)
{
    while (*left != '\0' && *right != '\0') {
        if (tolower((unsigned char)*left) != tolower((unsigned char)*right)) return 0;
        left++;
        right++;
    }
    return *left == '\0' && *right == '\0';
}

static int bot_login(const char *login)
{
    for (size_t index = 0; BOT_LOGINS[index] != NULL; index++) {
        if (same_folded(login, BOT_LOGINS[index])) return 1;
    }
    return 0;
}

static const char *event_kind_for(const Notification *notification, const char *self_login)
{
    if (strcmp(notification->actor_kind, "bot") == 0) return NULL;
    if (self_login != NULL && self_login[0] != '\0' &&
        same_folded(notification->last_actor, self_login)) return NULL;
    if (strcmp(notification->reason, "assign") == 0 ||
        strcmp(notification->reason, "author") == 0 ||
        strcmp(notification->reason, "state_change") == 0 ||
        strcmp(notification->reason, "subscribed") == 0) return "state_changed";
    if (strcmp(notification->reason, "mention") == 0 ||
        strcmp(notification->reason, "comment") == 0 ||
        strcmp(notification->reason, "review_requested") == 0) return "inbound_reply";
    return NULL;
}

static int copy_field(char *out, size_t out_size, const char *begin, size_t length)
{
    if (length >= out_size) return -1;
    memcpy(out, begin, length);
    out[length] = '\0';
    return 0;
}

static int parse_tsv(char *line, Notification *out)
{
    char *fields[FIELD_COUNT] = {0};
    size_t count = 0;
    char *cursor = line;
    memset(out, 0, sizeof(*out));
    while (count < FIELD_COUNT) {
        fields[count++] = cursor;
        cursor = strchr(cursor, '\t');
        if (cursor == NULL) break;
        *cursor++ = '\0';
    }
    if (count != FIELD_COUNT || strchr(cursor == NULL ? fields[count - 1] : cursor, '\t') != NULL) return -1;
    if (copy_field(out->repo, sizeof(out->repo), fields[0], strlen(fields[0])) != 0 ||
        copy_field(out->subject_type, sizeof(out->subject_type), fields[1], strlen(fields[1])) != 0 ||
        copy_field(out->title, sizeof(out->title), fields[2], strlen(fields[2])) != 0 ||
        copy_field(out->reason, sizeof(out->reason), fields[3], strlen(fields[3])) != 0 ||
        copy_field(out->updated_at, sizeof(out->updated_at), fields[4], strlen(fields[4])) != 0 ||
        copy_field(out->api_url, sizeof(out->api_url), fields[5], strlen(fields[5])) != 0 ||
        copy_field(out->last_actor, sizeof(out->last_actor), fields[6], strlen(fields[6])) != 0 ||
        copy_field(out->actor_kind, sizeof(out->actor_kind), fields[7], strlen(fields[7])) != 0 ||
        copy_field(out->evidence_ref, sizeof(out->evidence_ref), fields[8], strlen(fields[8])) != 0) return -1;
    return 0;
}

static int parse_notification_row(char *line, Notification *out)
{
    char *fields[6] = {0};
    size_t count = 0;
    char *cursor = line;
    memset(out, 0, sizeof(*out));
    while (count < 6) {
        fields[count++] = cursor;
        cursor = strchr(cursor, '\t');
        if (cursor == NULL) break;
        *cursor++ = '\0';
    }
    if (count != 6 || strchr(cursor == NULL ? fields[count - 1] : cursor, '\t') != NULL) return -1;
    if (copy_field(out->repo, sizeof(out->repo), fields[0], strlen(fields[0])) != 0 ||
        copy_field(out->subject_type, sizeof(out->subject_type), fields[1], strlen(fields[1])) != 0 ||
        copy_field(out->title, sizeof(out->title), fields[2], strlen(fields[2])) != 0 ||
        copy_field(out->reason, sizeof(out->reason), fields[3], strlen(fields[3])) != 0 ||
        copy_field(out->updated_at, sizeof(out->updated_at), fields[4], strlen(fields[4])) != 0 ||
        copy_field(out->api_url, sizeof(out->api_url), fields[5], strlen(fields[5])) != 0) return -1;
    return 0;
}

static void iso_utc(char output[64], const char *input)
{
    size_t length = strlen(input);
    if (length > 0 && input[length - 1] == 'Z' && length + 5 < 64) {
        memcpy(output, input, length - 1);
        memcpy(output + length - 1, "+00:00", 7);
    } else {
        snprintf(output, 64, "%s", input);
    }
}

static void json_escaped(FILE *out, const char *text)
{
    for (; *text != '\0'; text++) {
        switch (*text) {
        case '"': fputs("\\\"", out); break;
        case '\\': fputs("\\\\", out); break;
        case '\n': fputs("\\n", out); break;
        case '\r': fputs("\\r", out); break;
        case '\t': fputs("\\t", out); break;
        default:
            if ((unsigned char)*text >= 0x20) fputc(*text, out);
        }
    }
}

static int payload_json(const Notification *notification, char *out, size_t out_size)
{
    FILE *stream = tmpfile();
    long length;
    if (stream == NULL) return -1;
    fputs("{\"title\":\"", stream); json_escaped(stream, notification->title);
    fputs("\",\"reason\":\"", stream); json_escaped(stream, notification->reason);
    fputs("\",\"last_actor\":\"", stream); json_escaped(stream, notification->last_actor);
    fputs("\",\"subject_type\":\"", stream); json_escaped(stream, notification->subject_type);
    fputs("\"}", stream);
    if (fflush(stream) != 0 || fseek(stream, 0, SEEK_END) != 0 || (length = ftell(stream)) < 0 ||
        (size_t)length >= out_size || fseek(stream, 0, SEEK_SET) != 0 ||
        fread(out, 1, (size_t)length, stream) != (size_t)length) {
        fclose(stream);
        return -1;
    }
    out[length] = '\0';
    fclose(stream);
    return 0;
}

static void notification_ref(const Notification *notification, char out[FIELD_MAX])
{
    const char *number = strrchr(notification->api_url, '/');
    snprintf(out, FIELD_MAX, "%s#%s", notification->repo, number != NULL ? number + 1 : "");
}

static int consume(FILE *input, BfOccurrenceSpine *spine, const char *self_login, int dry_run)
{
    char line[FIELD_MAX * 6];
    int notifications = 0;
    int committed = 0;
    int suppressed = 0;
    int duplicate = 0;
    while (fgets(line, sizeof(line), input) != NULL) {
        Notification notification;
        const char *kind;
        char ref[FIELD_MAX];
        char observed_at[64];
        char payload[FIELD_MAX * 2];
        int64_t event_id = 0;
        line[strcspn(line, "\r\n")] = '\0';
        if (line[0] == '\0') continue;
        if (parse_tsv(line, &notification) != 0) {
            fprintf(stderr, "bonfyre-github-watch: malformed fixture/gh row\n");
            return 1;
        }
        notifications++;
        kind = event_kind_for(&notification, self_login);
        notification_ref(&notification, ref);
        if (kind == NULL) {
            printf("· %s  suppressed/%s  %s\n", ref,
                   strcmp(notification.actor_kind, "bot") == 0 ? "bot" :
                   (self_login != NULL && self_login[0] != '\0' &&
                    same_folded(notification.last_actor, self_login)) ? "self" : "no-op",
                   notification.last_actor[0] != '\0' ? notification.last_actor : notification.actor_kind);
            suppressed++;
            continue;
        }
        if (dry_run) {
            printf("  %s  %s  %s\n", ref, kind, notification.last_actor);
            committed++;
            continue;
        }
        iso_utc(observed_at, notification.updated_at);
        if (payload_json(&notification, payload, sizeof(payload)) != 0) return 1;
        BfOccurrenceSpec spec = {
            .source = "github", .actor = notification.repo, .event_kind = kind,
            .subject_ref = ref, .observed_at = observed_at, .payload_json = payload,
            .evidence_ref = notification.evidence_ref, .recorded_at = NULL,
        };
        BfOccurrenceStatus status = bf_occurrence_observe(spine, &spec, &event_id);
        if (status == BF_OCCURRENCE_DUPLICATE) {
            printf("  %s  already-recorded  %s\n", ref, notification.last_actor);
            duplicate++;
        } else if (status == BF_OCCURRENCE_OK) {
            printf("  %s  %s  %s\n", ref, kind, notification.last_actor);
            committed++;
        } else {
            fprintf(stderr, "bonfyre-github-watch: occurrence write failed: %s\n",
                    bf_occurrence_status_name(status));
            return 1;
        }
    }
    printf("\n%d notifications -> %d occurrences, %d suppressed as bot/no-op, %d already recorded\n",
           notifications, committed, suppressed, duplicate);
    return 0;
}

static int safe_api_url(const char *url)
{
    static const char prefix[] = "https://api.github.com/";
    if (strncmp(url, prefix, sizeof(prefix) - 1) != 0) return 0;
    for (const unsigned char *cursor = (const unsigned char *)url + sizeof(prefix) - 1;
         *cursor != '\0'; cursor++) {
        if (!(isalnum(*cursor) || *cursor == '/' || *cursor == '_' || *cursor == '-' ||
              *cursor == '?' || *cursor == '=' || *cursor == '&' || *cursor == '.')) return 0;
    }
    return 1;
}

static int gh_row(const char *command, char *out, size_t out_size)
{
    FILE *pipe = popen(command, "r");
    int status;
    if (pipe == NULL) return -1;
    if (fgets(out, (int)out_size, pipe) == NULL) {
        pclose(pipe);
        return -1;
    }
    out[strcspn(out, "\r\n")] = '\0';
    status = pclose(pipe);
    return status == 0 ? 0 : -1;
}

static int parse_actor_row(char *line, char *login, size_t login_size,
                           char *evidence, size_t evidence_size)
{
    char *tab = strchr(line, '\t');
    if (tab == NULL) return -1;
    *tab++ = '\0';
    return copy_field(login, login_size, line, strlen(line)) == 0 &&
           copy_field(evidence, evidence_size, tab, strlen(tab)) == 0 ? 0 : -1;
}

static void resolve_last_actor(Notification *notification)
{
    char command[FIELD_MAX + 512];
    char row[FIELD_MAX * 2];
    if (!safe_api_url(notification->api_url)) {
        snprintf(notification->actor_kind, sizeof(notification->actor_kind), "unknown");
        snprintf(notification->evidence_ref, sizeof(notification->evidence_ref), "%s", notification->api_url);
        return;
    }
    snprintf(command, sizeof(command),
             "gh api '%s/comments?per_page=20' --jq 'if length == 0 then empty else .[-1] | [.user.login,.html_url] | @tsv end'",
             notification->api_url);
    if (gh_row(command, row, sizeof(row)) != 0 ||
        parse_actor_row(row, notification->last_actor, sizeof(notification->last_actor),
                        notification->evidence_ref, sizeof(notification->evidence_ref)) != 0) {
        snprintf(command, sizeof(command), "gh api '%s' --jq '[.user.login,.html_url] | @tsv'",
                 notification->api_url);
        if (gh_row(command, row, sizeof(row)) != 0 ||
            parse_actor_row(row, notification->last_actor, sizeof(notification->last_actor),
                            notification->evidence_ref, sizeof(notification->evidence_ref)) != 0) {
            notification->last_actor[0] = '\0';
            snprintf(notification->evidence_ref, sizeof(notification->evidence_ref), "%s",
                     notification->api_url);
        }
    }
    snprintf(notification->actor_kind, sizeof(notification->actor_kind), "%s",
             bot_login(notification->last_actor) ? "bot" : "human");
}

static int consume_github(BfOccurrenceSpine *spine, const char *self_login, int dry_run,
                          int limit)
{
    char command[256];
    char row[FIELD_MAX * 4];
    FILE *feed;
    FILE *fixture;
    int status;
    if (limit < 1 || limit > 100) {
        fprintf(stderr, "bonfyre-github-watch: --limit must be 1..100\n");
        return 1;
    }
    snprintf(command, sizeof(command),
             "gh api 'notifications?all=true&per_page=%d' --jq '.[] | [.repository.full_name,.subject.type,.subject.title,.reason,.updated_at,(.subject.url // \"\")] | @tsv'",
             limit);
    feed = popen(command, "r");
    if (feed == NULL) return 1;
    fixture = tmpfile();
    if (fixture == NULL) {
        pclose(feed);
        return 1;
    }
    while (fgets(row, sizeof(row), feed) != NULL) {
        Notification notification;
        row[strcspn(row, "\r\n")] = '\0';
        if (parse_notification_row(row, &notification) != 0) {
            fclose(fixture);
            pclose(feed);
            fprintf(stderr, "bonfyre-github-watch: gh emitted an invalid notification row\n");
            return 1;
        }
        resolve_last_actor(&notification);
        fprintf(fixture, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
                notification.repo, notification.subject_type, notification.title, notification.reason,
                notification.updated_at, notification.api_url, notification.last_actor,
                notification.actor_kind, notification.evidence_ref);
    }
    status = pclose(feed);
    if (status != 0 || fseek(fixture, 0, SEEK_SET) != 0) {
        fclose(fixture);
        return 1;
    }
    status = consume(fixture, spine, self_login, dry_run);
    fclose(fixture);
    return status;
}

static void usage(void)
{
    fputs("usage: bonfyre-github-watch ingest --db FILE (--fixture TSV | --github) [--self LOGIN] [--limit N] [--dry-run]\n"
          "       fixture columns: repo, subject_type, title, reason, updated_at, api_url, last_actor, actor_kind, evidence_ref\n",
          stderr);
}

int main(int argc, char **argv)
{
    if (bf_help_requested(argc, argv)) { usage(); return 0; }
    const char *database_path = NULL;
    const char *fixture_path = NULL;
    const char *self_login = "";
    int github = 0;
    int limit = 30;
    int dry_run = 0;
    FILE *input;
    BfOccurrenceSpine *spine = NULL;
    int result;
    enum { GW_INGEST };
    static const BfCommandEntry watch_commands[] = {
        {"ingest", GW_INGEST, 0, NULL},
    };
    const BfCommandEntry *command = argc < 2 ? NULL : bf_command_lookup(watch_commands,
        sizeof(watch_commands) / sizeof(watch_commands[0]), argv[1]);
    if (!command || command->id != GW_INGEST) {
        usage();
        return 2;
    }
    for (int index = 2; index < argc; index++) {
        if (strcmp(argv[index], "--db") == 0 && index + 1 < argc) database_path = argv[++index];
        else if (strcmp(argv[index], "--fixture") == 0 && index + 1 < argc) fixture_path = argv[++index];
        else if (strcmp(argv[index], "--self") == 0 && index + 1 < argc) self_login = argv[++index];
        else if (strcmp(argv[index], "--github") == 0) github = 1;
        else if (strcmp(argv[index], "--limit") == 0 && index + 1 < argc) limit = atoi(argv[++index]);
        else if (strcmp(argv[index], "--dry-run") == 0) dry_run = 1;
        else {
            usage();
            return 2;
        }
    }
    if ((fixture_path == NULL && !github) || (fixture_path != NULL && github) ||
        (!dry_run && database_path == NULL)) {
        usage();
        return 2;
    }
    if (!dry_run && bf_occurrence_open(database_path, &spine, stderr) != 0) {
        return 1;
    }
    if (github) {
        char login[256];
        if (self_login[0] == '\0' &&
            gh_row("gh api user --jq '.login'", login, sizeof(login)) == 0) self_login = login;
        result = consume_github(spine, self_login, dry_run, limit);
    } else {
        input = fopen(fixture_path, "r");
        if (input == NULL) {
            perror(fixture_path);
            bf_occurrence_close(spine);
            return 1;
        }
        result = consume(input, spine, self_login, dry_run);
        fclose(input);
    }
    bf_occurrence_close(spine);
    return result;
}
