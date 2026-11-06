#include "campaign.h"
#include <stdio.h>
#include <string.h>
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

int main(int argc, char **argv) {
    enum { CP_HELP, CP_REQUEST };
    static const BfCommandEntry campaign_commands[] = {
        {"--help", CP_HELP, 0, NULL},
        {"help", CP_HELP, 0, NULL},
        {"request", CP_REQUEST, 0, NULL},
    };
    const BfCommandEntry *command = argc < 2 ? NULL : bf_command_lookup(campaign_commands,
        sizeof(campaign_commands) / sizeof(campaign_commands[0]), argv[1]);
    if (argc == 2 && command && command->id == CP_HELP) {
        puts("bonfyre-campaign request\nRead one version:1 JSON build/parse/validate request on stdin.\nSee cmd/BonfyreCampaign/README.md for the bounded protocol.");
        return 0;
    }
    bf_jsonw out;
    bf_jsonw_init(&out, 0);
    int status;
    if (argc != 2 || !command || command->id != CP_REQUEST) status = bf_campaign_error(&out, "invalid_command");
    else {
        char input[BF_CAMPAIGN_REQUEST_MAX + 1];
        size_t n = fread(input, 1, sizeof(input), stdin);
        if (ferror(stdin)) status = bf_campaign_error(&out, "input_read_failed");
        else if (n > BF_CAMPAIGN_REQUEST_MAX) status = bf_campaign_error(&out, "request_too_large");
        else status = bf_campaign_request(input, n, &out);
    }
    if (out.oom || !out.buf || fwrite(out.buf, 1, out.len, stdout) != out.len || putchar('\n') == EOF) status = 3;
    bf_jsonw_free(&out);
    return status;
}
