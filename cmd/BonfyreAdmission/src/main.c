#include "bf_admission.h"
#include "bf_command_lookup.h"
#include "bf_help.h"

#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf_lex.h"
/* Delegates to the one implementation. Three byte-identical copies of this
 * existed; they agreed, which was luck rather than design. */
static const char *option_value(int argc, char **argv, const char *option)
{
    return bf_lex_argv_option(argc, argv, option);
}

static int parse_i64(const char *text, int64_t *out)
{
    char *end = NULL;
    long long value;
    if (!text || !out) return -1;
    errno = 0;
    value = strtoll(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0') return -1;
    *out = (int64_t)value;
    return 0;
}

/* Delegates to the one implementation. Nineteen copies of this shape existed
 * with sixteen different opinions about escaping.
 */
static void json_string(const char *text)
{
    bf_lex_json_emit(stdout, text);
}

/* `none` removes a limit; a bare `0` is refused rather than quietly ignored.
 *
 * 0 was accepted and discarded: the library reads 0 as "unset" and substituted
 * the 10 GiB default, so `--protected-floor-bytes 0` did the opposite of what it
 * says and said nothing about it. Rejecting 0 with the word that works is the
 * whole fix -- the caller who meant "no floor" gets it, and the caller who meant
 * "the default" was already getting it by omitting the flag. */
static int policy_option(int argc, char **argv, const char *option, int64_t *field)
{
    const char *text = option_value(argc, argv, option);
    int64_t value;
    if (!text) return 0;
    if (strcmp(text, "none") == 0) {
        *field = BF_ADMISSION_LIMIT_NONE;
        return 0;
    }
    if (parse_i64(text, &value) != 0 || value < 0) return -1;
    if (value == 0) {
        fprintf(stderr,
                "%s 0 is ambiguous: 0 means \"unset, use the default\" here. "
                "Pass %s none to remove the limit, or omit the flag for the default.\n",
                option, option);
        return -1;
    }
    *field = value;
    return 0;
}

static int request_grant(BfAdmissionGrants *grants, int argc, char **argv)
{
    const char *plane = option_value(argc, argv, "--plane");
    const char *kind = option_value(argc, argv, "--kind");
    const char *bytes_text = option_value(argc, argv, "--bytes");
    const char *volume = option_value(argc, argv, "--volume");
    const char *detail = option_value(argc, argv, "--detail");
    const char *at = option_value(argc, argv, "--at");
    const char *free_text = option_value(argc, argv, "--free-bytes");
    BfAdmissionPolicy policy;
    BfResourceRequest request;
    BfAdmissionDecision decision;
    int64_t estimate;
    int64_t free_bytes;
    int64_t grant_id = 0;
    BfAdmissionVerdict verdict;

    if (!plane || !kind || !bytes_text || parse_i64(bytes_text, &estimate) != 0) return -1;
    if (!volume) volume = "/";
    if (free_text) {
        if (parse_i64(free_text, &free_bytes) != 0 || free_bytes < 0) return -1;
    } else {
        free_bytes = bf_admission_free(volume);
        if (free_bytes < 0) return -1;
    }
    if (bf_admission_volume_is_memory(volume)) bf_admission_policy_memory(&policy);
    else bf_admission_policy_defaults(&policy);
    if (policy_option(argc, argv, "--protected-floor-bytes", &policy.protected_floor_bytes) != 0 ||
        policy_option(argc, argv, "--per-plane-quota-bytes", &policy.per_plane_quota_bytes) != 0 ||
        policy_option(argc, argv, "--max-grant-bytes", &policy.max_grant_bytes) != 0) return -1;
    request = (BfResourceRequest){
        .plane = plane, .kind = kind, .estimated_bytes = estimate,
        .volume = volume, .detail = detail ? detail : ""};
    verdict = bf_admission_request_grant(grants, &request, &policy, free_bytes, at,
                                         &decision, &grant_id);
    fputs("{\"verdict\":", stdout);
    json_string(bf_admission_verdict_name(verdict));
    fputs(",\"plane\":", stdout);
    json_string(plane);
    fputs(",\"kind\":", stdout);
    json_string(kind);
    printf(",\"grant_id\":%" PRId64 ",\"free_bytes\":%" PRId64
           ",\"committed_bytes\":%" PRId64 ",\"spendable_bytes\":%" PRId64
           ",\"reason\":", grant_id, decision.free_bytes, decision.committed_bytes,
           decision.spendable_bytes);
    json_string(decision.reason);
    fputs("}\n", stdout);
    return 0;
}

static int release_grant(BfAdmissionGrants *grants, int argc, char **argv)
{
    const char *id_text = option_value(argc, argv, "--grant");
    const char *at = option_value(argc, argv, "--at");
    int64_t id;
    if (!id_text || parse_i64(id_text, &id) != 0 || id <= 0 ||
        bf_admission_release_grant(grants, id, at) != 0) return -1;
    printf("{\"released_grant_id\":%" PRId64 "}\n", id);
    return 0;
}

static int print_status(BfAdmissionGrants *grants, int argc, char **argv)
{
    const char *volume = option_value(argc, argv, "--volume");
    const char *plane = option_value(argc, argv, "--plane");
    int64_t committed;
    if (!volume) volume = "/";
    committed = bf_admission_committed_bytes(grants, volume, plane);
    if (committed < 0) return -1;
    fputs("{\"volume\":", stdout);
    json_string(volume);
    fputs(",\"plane\":", stdout);
    if (plane) json_string(plane); else fputs("null", stdout);
    printf(",\"committed_bytes\":%" PRId64 "}\n", committed);
    return 0;
}

static int reap_grants(BfAdmissionGrants *grants, int argc, char **argv)
{
    const char *older_text = option_value(argc, argv, "--older-than-seconds");
    const char *at = option_value(argc, argv, "--at");
    int64_t older_than;
    int64_t reaped;
    if (!older_text || parse_i64(older_text, &older_than) != 0 || older_than < 0) return -1;
    reaped = bf_admission_reap_expired(grants, older_than, at);
    if (reaped < 0) return -1;
    printf("{\"reaped\":%" PRId64 "}\n", reaped);
    return 0;
}

static int print_memory(void)
{
    BfAdmissionPolicy policy;
    int64_t available = bf_admission_memory_available();
    if (available < 0) return -1;
    bf_admission_policy_memory(&policy);
    printf("{\"volume\":\"" BF_ADMISSION_MEMORY_VOLUME "\",\"available_bytes\":%" PRId64
           ",\"protected_floor_bytes\":%" PRId64 "}\n", available, policy.protected_floor_bytes);
    return 0;
}

static void usage(void)
{
    fputs("usage: bonfyre-admission request --db FILE --plane ID --kind KIND --bytes N [--volume PATH] [--free-bytes N] [--at ISO]\n"
          "       bonfyre-admission release --db FILE --grant ID [--at ISO]\n"
          "       bonfyre-admission status --db FILE [--volume PATH] [--plane ID]\n"
          "       bonfyre-admission reap --db FILE --older-than-seconds N [--at ISO]\n"
          "       bonfyre-admission memory\n"
          "\n"
          "--volume memory makes the grant a claim on RAM instead of disk: a model\n"
          "asks before it loads and releases when it unloads.\n", stderr);
}

#define ADMISSION_COMMANDS(X) \
    X("memory", AD_MEMORY) \
    X("reap", AD_REAP) \
    X("release", AD_RELEASE) \
    X("request", AD_REQUEST) \
    X("status", AD_STATUS)

enum AdmissionCommand {
#define ADMISSION_ENUM(name, command_id) command_id,
    ADMISSION_COMMANDS(ADMISSION_ENUM)
#undef ADMISSION_ENUM
};

static const BfCommandEntry admission_commands[] = {
#define ADMISSION_ENTRY(name, command_id) {name, command_id, 0, NULL},
    ADMISSION_COMMANDS(ADMISSION_ENTRY)
#undef ADMISSION_ENTRY
};

int main(int argc, char **argv)
{
    const char *database;
    const BfCommandEntry *command;
    BfAdmissionGrants *grants = NULL;
    int result;
    if (bf_help_requested(argc, argv)) { usage(); return 0; }
    if (argc < 2) { usage(); return 2; }
    command = bf_command_lookup(admission_commands,
                                sizeof(admission_commands) / sizeof(admission_commands[0]), argv[1]);
    if (!command) {
        usage();
        return 2;
    }
    if (command->id == AD_MEMORY) return print_memory() == 0 ? 0 : 1;
    database = option_value(argc, argv, "--db");
    if (!database || bf_admission_open(database, &grants, stderr) != 0) return 1;
    switch (command->id) {
        case AD_REQUEST: result = request_grant(grants, argc, argv); break;
        case AD_RELEASE: result = release_grant(grants, argc, argv); break;
        case AD_STATUS: result = print_status(grants, argc, argv); break;
        case AD_REAP: result = reap_grants(grants, argc, argv); break;
        default: result = -1; break;
    }
    bf_admission_close(grants);
    if (result != 0) {
        usage();
        return 2;
    }
    return 0;
}
