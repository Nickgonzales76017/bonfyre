#include "recall.h"
#include "../../../lib/libbonfyre/include/bf_help.h"
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

#include <string.h>

static void usage(void) {
    fprintf(stderr,
        "BonfyreRecall %s — learned candidate recall + rebuildable candidate stores\n"
        "  bonfyre-recall query --store DB --text TEXT [--top N] [--passages] [--hybrid] [--model MODEL]\n"
        "      --hybrid fuses meaning with the passages' own words (local, reciprocal rank)\n"
        "  bonfyre-recall index-jsonl --store DB --input-jsonl FILE [--model MODEL] [--apply]\n"
        "  bonfyre-recall index-fragments --cms-db DB --store DB --work-dir DIR\n"
        "      [--namespace NS] [--status published|draft|archived|all] [--model MODEL] [--apply]\n"
        "  bonfyre-recall index-sources --store DB --work-dir DIR [--model MODEL] [--apply]\n"
        "      [--text FILE|INGEST_DIR[=LABEL]]... [--records CMS_DB:TYPE[:NAMESPACE]]... [--jsonl FILE]...\n"
        "      passages are cut by bonfyre-paragraph; any CMS record type is readable\n"
        "  bonfyre-recall index-words --store DB   add the word index to a store built before it existed\n"
        "Learned geometry proposes candidates only; exact owners must recheck them.\n",
        BF_RECALL_VERSION);
}

int main(int argc, char **argv) {
    if (bf_help_requested(argc, argv)) { usage(); return 0; }
    bf_recall_init(argv[0]);
    if (argc < 2) {
        usage();
        return 1;
    }
    enum {
        RC_INDEX_FRAGMENTS, RC_INDEX_JSONL, RC_INDEX_SOURCES, RC_INDEX_WORDS, RC_QUERY,
    };
    static const BfCommandEntry recall_commands[] = {
        {"index-fragments", RC_INDEX_FRAGMENTS, 0, NULL},
        {"index-jsonl", RC_INDEX_JSONL, 0, NULL},
        {"index-sources", RC_INDEX_SOURCES, 0, NULL},
        {"index-words", RC_INDEX_WORDS, 0, NULL},
        {"query", RC_QUERY, 0, NULL},
    };
    const BfCommandEntry *command = bf_command_lookup(recall_commands,
        sizeof(recall_commands) / sizeof(recall_commands[0]), argv[1]);
    if (command) {
        if (command->id == RC_QUERY) return bf_recall_query(argc, argv);
        if (command->id == RC_INDEX_JSONL) return bf_recall_index_jsonl(argc, argv);
        if (command->id == RC_INDEX_FRAGMENTS) return bf_recall_index_fragments(argc, argv);
        if (command->id == RC_INDEX_SOURCES) return bf_recall_index_sources(argc, argv);
        if (command->id == RC_INDEX_WORDS) {
            const char *store = bf_opt(argc, argv, "--store");
            if (!store || !*store) { fputs("index-words requires --store DB\n", stderr); return 2; }
            if (bf_recall_word_index(store, NULL)) return 8;
            puts("{\"schema\":\"bonfyre.semantic-index-result.v1\",\"word_index\":true}");
            return 0;
        }
    }
    usage();
    return 2;
}
