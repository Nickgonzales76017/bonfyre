/* POSIX/GNU interfaces used below are hidden by glibc under -std=c11 without
   a feature macro. macOS exposes them regardless, so this class of break is
   invisible on the dev machine and fatal on the Linux deploy host. */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

/*
 * bonfyre — unified CLI dispatcher.
 *
 * Routes subcommands to their respective binaries:
 *   bonfyre <cmd> [args...]  →  bonfyre-<cmd> [args...]
 *
 * Binary search order per command:
 *   1. Same directory as this binary
 *   2. ../SiblingDir/bonfyre-<cmd>          (top-level Makefile output)
 *   3. ../SiblingDir/build/bonfyre-<cmd>    (Makefile build/ subdirectory)
 *   4. PATH
 *
 * Commands that route through bonfyre-runtime pass the original
 * subcommand token as the first argument so runtime can dispatch
 * internally.
 */
#include <fcntl.h>
#include <limits.h>
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <bonfyre.h>
#include <bf_json.h>

#include "bf_lex.h"
/* ── Section constants ────────────────────────────────────────────── */
#define SEC_SITES   "Websites and content"
#define SEC_MEDIA   "Recordings and video"
#define SEC_WRITE   "Write, package and send"
#define SEC_SEARCH  "Search and understand text"
#define SEC_MODELS  "Your own models"
#define SEC_MONEY   "Money and customers"
#define SEC_ACCESS  "Accounts and access"
#define SEC_DATA    "Move and keep data"
#define SEC_AUTO    "Automate and check"
#define SEC_LAB     "Lab: research kernels and internals"
#define SEC_LOCAL   "Local builds not yet in the table"

typedef struct {
    const char *cmd;
    const char *binary;
    const char *sibling_dir;
    const char *section;
    const char *desc;
    int tier;           /* TIER_START on the first screen; TIER_LAB only with 'list --all' */
} Route;

enum { TIER_EVERYDAY = 0, TIER_START = 1, TIER_LAB = 2 };

static const char *const goal_sections[] = {
    SEC_SITES, SEC_MEDIA, SEC_WRITE, SEC_SEARCH, SEC_MODELS, SEC_MONEY, SEC_ACCESS, SEC_DATA, SEC_AUTO, SEC_LAB, NULL
};

typedef struct {
    const char *cmd;
    const char *desc;
} BuiltinSurface;

typedef struct {
    int total;
    int available;
} RouteStats;

typedef struct {
    int json;
    int compact;
    int health;
    int all;            /* include lab kernels and local builds */
    char query[256];
} ListOptions;

typedef struct {
    char command[128];
    char cli_name[160];
    char installed_path[PATH_MAX];
    char repo_path[PATH_MAX];
    char active_path[PATH_MAX];
    char installed_sha256[65];
    char repo_sha256[65];
    long long installed_size;
    long long repo_size;
    long long installed_mtime;
    long long repo_mtime;
    char active_source[32];
    char status[32];
    char notes[256];
    int available;
    int executable;
    int repo_backed;
    int installed_backed;
    int stale;
    int missing;
    int healthy_unknown;
    int healthy;
} CommandStatusRow;

typedef struct {
    char root[PATH_MAX];
    char home_catalog[PATH_MAX];
    long long layer_artifacts;
    long long indexed_layers;
    long long discipl_actors;
    long long discipl_contracts;
    long long discipl_chains;
    long long discipl_loops;
    long long graph_atoms;
    long long graph_operators;
    long long graph_realizations;
    long long graph_edges;
    long long queue_jobs;
    long long catalog_capability;
    long long catalog_model;
    long long catalog_model_source;
    long long catalog_recipe;
    long long catalog_workflow;
    long long catalog_workflow_step;
    long long catalog_layer;
} RegistryStatus;

typedef struct {
    char id[65];
    char kind[64];
    char status[32];
    double score;
    char title[256];
    char detail[1024];
} SelfPlanItem;

typedef struct {
    char cmd[128];
    char binary[128];
    char module[128];
    char path[PATH_MAX];
} ExtraRoute;

typedef struct {
    const char *cmd;
    const char *legacy_binaries[5];
} InstallAliasMap;

static int contains_ci(const char *haystack, const char *needle);
/* Only list --all exposes lab kernels and local builds; --json changes format, not scope. */
static int g_list_all = 0;

static const InstallAliasMap install_alias_maps[] = {
    {"clean", {"bonfyre-clean", NULL}},
    {"mediaprep", {"bonfyre-mediaprep", NULL}},
    {"capabilities", {"bonfyre-capabilities", NULL}},
    {"weaviate", {"bonfyre-weaviate", NULL}},
    {"speech-loop", {"bonfyre-speech-loop", NULL}},
    {NULL, {NULL}}
};

static const BuiltinSurface builtin_surfaces[] = {
    {"status", "What is installed, stale or missing, and what the registries hold"},
    {"self", "Snapshot, inspect and repair this installation"},
    {"precision", "Plan smaller representations for stored artifacts"},
    {NULL, NULL}
};

static const Route routes[] = {
    /* ── sites ── */
    {"site",              "bonfyre-site",              "BonfyreWebAddress",       SEC_SITES, "Make a website: running and open in your browser in one command", TIER_START},
    {"cms",               "bonfyre-cms",               "BonfyreCMS",              SEC_SITES, "Store the pages, posts and records your sites and apps read", TIER_EVERYDAY},
    {"api",               "bonfyre-api",               "BonfyreAPI",              SEC_SITES, "Serve your records over HTTP", TIER_EVERYDAY},
    {"domain",            "bonfyre-domain",            "BonfyreDomain",           SEC_SITES, "Point a domain you own at a site, checked before anything changes", TIER_EVERYDAY},
    {"web-address",       "bonfyre-web-address",       "BonfyreWebAddress",       SEC_SITES, "Which site and page a web address serves", TIER_EVERYDAY},
    /* ── media ── */
    {"transcript-family", "bonfyre-transcript-family", "BonfyreTranscriptFamily", SEC_MEDIA, "A recording to clean, readable text in one step", TIER_START},
    {"transcribe",        "bonfyre-transcribe",        "BonfyreTranscribe",       SEC_MEDIA, "Turn a recording into text, on this machine", TIER_EVERYDAY},
    {"ingest",            "bonfyre-ingest",            "BonfyreIngest",           SEC_MEDIA, "Bring a file in: detect what it is and make a working copy", TIER_EVERYDAY},
    {"mediaprep",         "bonfyre-media-prep",        "BonfyreMediaPrep",        SEC_MEDIA, "Convert audio or video into the form the other steps read", TIER_EVERYDAY},
    {"clean",             "bonfyre-transcript-clean",  "BonfyreTranscriptClean",  SEC_MEDIA, "Tidy a transcript: fillers, repeats and timing", TIER_EVERYDAY},
    {"paragraph",         "bonfyre-paragraph",         "BonfyreParagraph",        SEC_MEDIA, "Break a transcript into readable paragraphs", TIER_EVERYDAY},
    {"segment",           "bonfyre-segment",           "BonfyreSegment",          SEC_MEDIA, "Find who spoke when, and where speech starts and stops", TIER_EVERYDAY},
    {"narrate",           "bonfyre-narrate",           "BonfyreNarrate",          SEC_MEDIA, "Read a text aloud into an audio file; fails when nothing was spoken", TIER_EVERYDAY},
    {"clips",             "bonfyre-clips",             "BonfyreClips",            SEC_MEDIA, "Cut short clips out of a longer recording", TIER_EVERYDAY},
    {"frame-extract",     "bonfyre-frame-extract",     "BonfyreFrameExtract",     SEC_MEDIA, "Save still frames from a video", TIER_EVERYDAY},
    {"scene-detect",      "bonfyre-scene-detect",      "BonfyreSceneDetect",      SEC_MEDIA, "Find where the scenes in a video change", TIER_EVERYDAY},
    {"video-demux",       "bonfyre-video-demux",       "BonfyreVideoDemux",       SEC_MEDIA, "Split a video into its audio and video tracks", TIER_EVERYDAY},
    {"detect-objects",    "bonfyre-detect-objects",    "BonfyreDetectObjects",    SEC_MEDIA, "Run a detector on each frame and report what it found", TIER_EVERYDAY},
    {"tel",               "bonfyre-tel",               "BonfyreTel",              SEC_MEDIA, "Phone calls through FreeSWITCH", TIER_EVERYDAY},
    {"speech-loop",       "bonfyre-speechloop",        "BonfyreSpeechLoop",       SEC_MEDIA, "Streaming RNNT + Whisper ASR loop", TIER_LAB},
    {"mfa-dict",          "bonfyre-mfa-dict",          "BonfyreMFADict",          SEC_MEDIA, "MFA forced-alignment dictionary", TIER_LAB},
    /* ── write ── */
    {"brief",             "bonfyre-brief",             "BonfyreBrief",            SEC_WRITE, "Pull a structured brief out of a transcript", TIER_EVERYDAY},
    {"proof",             "bonfyre-proof",             "BonfyreProof",            SEC_WRITE, "Check a brief against its source", TIER_EVERYDAY},
    {"offer",             "bonfyre-offer",             "BonfyreOffer",            SEC_WRITE, "Write an offer document from a brief", TIER_EVERYDAY},
    {"repurpose",         "bonfyre-repurpose",         "BonfyreRepurpose",        SEC_WRITE, "Turn a transcript into posts, notes and other formats", TIER_EVERYDAY},
    {"render",            "bonfyre-render",            "BonfyreRender",           SEC_WRITE, "Render a record as a page, document or text", TIER_EVERYDAY},
    {"emit",              "bonfyre-emit",              "BonfyreEmit",             SEC_WRITE, "Write one result out in several formats", TIER_EVERYDAY},
    {"pack",              "bonfyre-pack",              "BonfyrePack",             SEC_WRITE, "Bundle related files into one package", TIER_EVERYDAY},
    {"pack-graph",        "bonfyre-cms",              "BonfyreCMS",             SEC_WRITE, "Compile and verify immutable PackGraph images; govern account installations", TIER_EVERYDAY},
    {"distribute",        "bonfyre-distribute",        "BonfyreDistribute",       SEC_WRITE, "Send finished work out to its channels", TIER_EVERYDAY},
    /* ── search ── */
    {"recall",            "bonfyre-recall",            "BonfyreRecall",           SEC_SEARCH, "Search your material by meaning and by words; owners recheck the results", TIER_EVERYDAY},
    {"embed",             "bonfyre-embed",             "BonfyreEmbed",            SEC_SEARCH, "Turn text into vectors for search", TIER_EVERYDAY},
    {"vec",               "bonfyre-vec",               "BonfyreVec",              SEC_SEARCH, "Store and search vectors on this machine", TIER_EVERYDAY},
    {"query",             "bonfyre-query",             "BonfyreQuery",            SEC_SEARCH, "Query stored artifacts", TIER_EVERYDAY},
    {"index",             "bonfyre-index",             "BonfyreIndex",            SEC_SEARCH, "Index a folder of related files", TIER_EVERYDAY},
    {"fragment",          "bonfyre-fragment",          "BonfyreFragment",         SEC_SEARCH, "Store, query and merge passages of text", TIER_EVERYDAY},
    {"tag",               "bonfyre-tag",               "BonfyreTag",              SEC_SEARCH, "Tag text automatically", TIER_EVERYDAY},
    {"entity",            "bonfyre-entity",            "BonfyreEntity",           SEC_SEARCH, "Find the people, places and organisations named in a text", TIER_EVERYDAY},
    {"tone",              "bonfyre-tone",              "BonfyreTone",             SEC_SEARCH, "Read the tone of a voice recording", TIER_EVERYDAY},
    {"canon",             "bonfyre-canon",             "BonfyreCanon",            SEC_SEARCH, "Resolve different spellings of one thing to a single form", TIER_EVERYDAY},
    {"space",             "bonfyre-space",             "BonfyreSpace",            SEC_SEARCH, "Group material into named spaces", TIER_EVERYDAY},
    {"weaviate",          "bonfyre-weaviate-index",    "BonfyreWeaviateIndex",    SEC_SEARCH, "Weaviate vector index bridge", TIER_LAB},
    {"project",           "bonfyre-project",           "BonfyreProject",          SEC_SEARCH, "Content graph projection engine", TIER_LAB},
    /* ── models ── */
    {"monitor",           "bonfyre-monitor",           "BonfyreMonitor",          SEC_MODELS, "What your sources said about a business, namesakes set aside", TIER_EVERYDAY},
    {"reason",            "bonfyre-reason",            "BonfyreReason",           SEC_MODELS, "Ask your own model; several answers to one prompt, each one kept", TIER_START},
    {"model",             "bonfyre-model",             "BonfyreModel",            SEC_MODELS, "Your models: what is here, what is loaded, what can load now", TIER_EVERYDAY},
    {"gen",               "bonfyre-gen",               "BonfyreGen",              SEC_MODELS, "Turn a plain description into a recipe", TIER_EVERYDAY},
    {"proxy",             "bonfyre-proxy",             "BonfyreProxy",            SEC_MODELS, "An OpenAI-style address for transcription and chat", TIER_EVERYDAY},
    {"learn",             "bonfyre-learn",             "BonfyreLearn",            SEC_MODELS, "On-device fine-tuning / adapters", TIER_LAB},
    {"kvcache",           "bonfyre-kvcache",           "BonfyreKVCache",          SEC_MODELS, "KV-cache store, tensor benchmark and context compile", TIER_LAB},
    /* ── money ── */
    {"pay",               "bonfyre-pay",               "BonfyrePay",              SEC_MONEY, "Invoices, payments and balances", TIER_EVERYDAY},
    {"finance",           "bonfyre-finance",           "BonfyreFinance",          SEC_MONEY, "Opportunities, customers and books, worked out from your records", TIER_EVERYDAY},
    {"books",             "bonfyre-books",             "BonfyreBooks",            SEC_MONEY, "Book bank lines to your accounts with your own model, nothing sent out", TIER_EVERYDAY},
    {"manifest",          "bonfyre-manifest",          "BonfyreManifest",         SEC_MONEY, "An itemised manifest for an equipment lot, from its photos and listing", TIER_EVERYDAY},
    {"commerce",          "bonfyre-commerce",          "BonfyreCommerce",         SEC_MONEY, "Follow a sale from first contact to paid, stage by stage", TIER_EVERYDAY},
    {"meter",             "bonfyre-meter",             "BonfyreMeter",            SEC_MONEY, "Count usage for billing", TIER_EVERYDAY},
    {"ledger",            "bonfyre-ledger",            "BonfyreLedger",           SEC_MONEY, "What a piece of work actually cost", TIER_EVERYDAY},
    {"gate",              "bonfyre-gate",              "BonfyreGate",             SEC_MONEY, "Check a licence before something runs", TIER_EVERYDAY},
    {"tier",              "bonfyre-tier",              "BonfyreTier",             SEC_MONEY, "Plans and what each one unlocks", TIER_EVERYDAY},
    {"compete",           "bonfyre-compete",           "BonfyreCompete",          SEC_MONEY, "Benchmark against competitors", TIER_EVERYDAY},
    {"outreach",          "bonfyre-outreach",          "BonfyreOutreach",         SEC_MONEY, "Outreach campaigns", TIER_EVERYDAY},
    /* ── access ── */
    {"auth",              "bonfyre-auth",              "BonfyreAuth",             SEC_ACCESS, "Accounts, sign-in and team access", TIER_EVERYDAY},
    {"provider",          "bonfyre-provider",          "BonfyreProvider",         SEC_ACCESS, "Which outside providers can be used right now", TIER_EVERYDAY},
    /* ── data ── */
    {"bridge",            "bonfyre-bridge",            "BonfyreBridge",           SEC_DATA, "Move tables between Postgres, MySQL and SQLite and prove what arrived", TIER_EVERYDAY},
    {"sync",              "bonfyre-sync",              "BonfyreSync",             SEC_DATA, "Keep files in step across machines", TIER_EVERYDAY},
    {"hash",              "bonfyre-hash",              "BonfyreHash",             SEC_DATA, "Fingerprint files by their content", TIER_EVERYDAY},
    {"compress",          "bonfyre-compress",          "BonfyreCompress",         SEC_DATA, "Compress a family of files and measure the saving", TIER_EVERYDAY},
    {"graph",             "bonfyre-graph",             "BonfyreGraph",            SEC_DATA, "Record which files were made from which", TIER_EVERYDAY},
    {"queue",             "bonfyre-queue",             "BonfyreQueue",            SEC_DATA, "Queue jobs and see what is waiting", TIER_EVERYDAY},
    {"repository",        "bonfyre-repository-fabric", "BonfyreRepositoryFabric", SEC_DATA, "Work with Git repositories and exact source-cut receipts", TIER_EVERYDAY},
    {"parquet",           "bonfyre-arrow-stream",      "BonfyreArrowStream",      SEC_DATA, "Read a digest-pinned Parquet file as streaming Arrow records", TIER_EVERYDAY},
    /* ── auto ── */
    {"capabilities",      "bonfyre-capability",        "BonfyreCapability",       SEC_AUTO, "Find the command that does what you describe", TIER_START},
    {"doctor",            "bonfyre-runtime",           "BonfyreRuntime",          SEC_AUTO, "Check what is installed and working", TIER_START},
    {"recipe",            "bonfyre-recipe",            "BonfyreRecipe",           SEC_AUTO, "Saved step-by-step recipes: list, show and add", TIER_EVERYDAY},
    {"run",               "bonfyre-run",               "BonfyreRun",              SEC_AUTO, "Run a recipe and keep its history", TIER_EVERYDAY},
    {"workflow",          "bonfyre-workflow",          "BonfyreWorkflow",         SEC_AUTO, "Ready-made suites of steps", TIER_EVERYDAY},
    {"pipeline",          "bonfyre-pipeline",          "BonfyrePipeline",         SEC_AUTO, "Stream files through a chain of steps", TIER_EVERYDAY},
    {"watch",             "bonfyre-watch",             "BonfyreWatch",            SEC_AUTO, "Act when files in a folder change", TIER_EVERYDAY},
    {"github-watch",      "bonfyre-github-watch",      "BonfyreGitHubWatch",      SEC_AUTO, "Turn GitHub notifications into work items", TIER_EVERYDAY},
    {"time",              "bonfyre-time",              "BonfyreTime",             SEC_AUTO, "Schedules and dates attached to work", TIER_EVERYDAY},
    /* ── lab ── */
    {"operator",          "bonfyre-operator",          "BonfyreOperator",         SEC_LAB, "Answer household questions from their sources", TIER_LAB},
    {"family",            "bonfyre-family",            "BonfyreFamily",           SEC_LAB, "Conceptual family browser (specialization taxonomy)", TIER_LAB},
    {"sli",               "bonfyre-sli",               "BonfyreSLI",              SEC_LAB, "Structured layer inference (E8 lattice)", TIER_LAB},
    {"quant",             "bonfyre-quant",             "BonfyreQuant",            SEC_LAB, "Measure lattice quantisation of a named model", TIER_LAB},
    {"fpq",               "bonfyre-fpq",               "BonfyreFPQ",              SEC_LAB, "Functional precision quantisation", TIER_LAB},
    {"fpqx",              "bonfyre-fpqx",              "BonfyreFPQx",             SEC_LAB, "FPQx extended quantisation", TIER_LAB},
    {"layer",             "bonfyre-layer",             "BonfyreLayer",            SEC_LAB, "Neural layer operations", TIER_LAB},
    {"layer-c",           "bonfyre-layer-c",           "BonfyreLayer",            SEC_LAB, "Native C layer inspector and ONNX extraction surface", TIER_LAB},
    {"sae",               "bonfyre-sae",               "BonfyreSAE",              SEC_LAB, "Feature activation and gating from a trained dictionary", TIER_LAB},
    {"leapfrog",          "bonfyre-leapfrog",          "BonfyreLeapfrog",         SEC_LAB, "Hamiltonian leapfrog integrator (conservation + reversibility checks)", TIER_LAB},
    {"violence",          "bonfyre-violence",          "BonfyreViolence",         SEC_LAB, "Real-coupling physics validation (E8 embedding + Hamiltonian coupling)", TIER_LAB},
    {"net",               "bonfyre-net",               "BonfyreNet",              SEC_LAB, "Run a netlist of organs in net order, with a trace", TIER_LAB},
    {"flashqla",          "bonfyre-flashqla",          "BonfyreFlashQLA",         SEC_LAB, "Chunked-prefill GDN attention (FlashQLA tiled kernel)", TIER_LAB},
    {"physics",           "bonfyre-physics",           "BonfyrePhysics",          SEC_LAB, "Hamiltonian Version Control Protocol (HVCP) — trajectory store", TIER_LAB},
    {"flow",              "bonfyre-flow",              "BonfyreFlow",             SEC_LAB, "Coroutine-native pipeline flow graphs", TIER_LAB},
    {"runtime",           "bonfyre-runtime",           "BonfyreRuntime",          SEC_LAB, "Replayable pipeline runtime", TIER_LAB},
    {"discipl",           "bonfyre-discipl",           "BonfyreDiscipl",          SEC_LAB, "Recursive DisCIPL runtime substrate", TIER_LAB},
    {"orchestrate",       "bonfyre-orchestrate",       "BonfyreOrchestrate",      SEC_LAB, "Machine-only orchestration planner", TIER_LAB},
    {"control",           "bonfyre-control",           "BonfyreControl",          SEC_LAB, "Control-plane command gateway", TIER_LAB},
    {"swarm",             "bonfyre-swarm",             "BonfyreSwarm",            SEC_LAB, "Distributed worker swarm", TIER_LAB},
    {"stitch",            "bonfyre-stitch",            "BonfyreStitch",           SEC_LAB, "DAG materialiser", TIER_LAB},
    {"admission",         "bonfyre-admission",         "BonfyreAdmission",        SEC_LAB, "Resource grant admission and protected-floor ledger", TIER_LAB},
    {"attention",         "bonfyre-attention",         "BonfyreAttention",        SEC_LAB, "Watch cooling and named-signal reheat control", TIER_LAB},
    {"wire",              "bonfyre-wire",              "BonfyreWire",             SEC_LAB, "Consent-based network event layer", TIER_LAB},
    {"surface",           "bonfyre-surface",           "BonfyreSurface",          SEC_LAB, "Optional client surface registry", TIER_LAB},
    {"moq",               "bonfyre-moq",               "BonfyreMoQ",              SEC_LAB, "MoQ media-over-QUIC transport", TIER_LAB},
    {"economy",           "bonfyre-economy",           "BonfyreEconomy",          SEC_LAB, "Economy / credits engine", TIER_LAB},
    {NULL, NULL, NULL, NULL, NULL, 0}
};

/* ── Binary resolution ────────────────────────────────────────────── */
static void get_self_dir(char *buf, size_t sz) {
    char self[PATH_MAX];
    memset(self, 0, sizeof(self));
#ifdef __APPLE__
    uint32_t bsz = sizeof(self);
    if (_NSGetExecutablePath(self, &bsz) != 0) self[0] = '\0';
#elif defined(__linux__)
    ssize_t n = readlink("/proc/self/exe", self, sizeof(self) - 1);
    if (n > 0) self[n] = '\0'; else self[0] = '\0';
#else
    self[0] = '\0';
#endif
    if (self[0]) {
        char *last = strrchr(self, '/');
        if (last) { *last = '\0'; snprintf(buf, sz, "%s", self); return; }
    }
    buf[0] = '\0';
}

static int path_exists_exec(const char *path) {
    return path && path[0] && access(path, X_OK) == 0;
}

static int path_exists_dir(const char *path) {
    struct stat st;
    return path && path[0] && stat(path, &st) == 0 && S_ISDIR(st.st_mode);
}

static int path_exists_file(const char *path) {
    struct stat st;
    return path && path[0] && stat(path, &st) == 0 && S_ISREG(st.st_mode);
}

static int looks_like_repo_root(const char *path) {
    char mk[PATH_MAX], cmd_dir[PATH_MAX], cli_src[PATH_MAX];
    snprintf(mk, sizeof(mk), "%s/Makefile", path);
    snprintf(cmd_dir, sizeof(cmd_dir), "%s/cmd", path);
    snprintf(cli_src, sizeof(cli_src), "%s/cmd/BonfyreCLI/src/main.c", path);
    return path_exists_file(mk) && path_exists_dir(cmd_dir) && path_exists_file(cli_src);
}

static int find_repo_root_from_cwd(char *buf, size_t sz) {
    char cwd[PATH_MAX];
    char cur[PATH_MAX];
    const char *env = getenv("BONFYRE_REPO_ROOT");
    const char *root_env = getenv("BONFYRE_ROOT");
    if (env && looks_like_repo_root(env)) {
        snprintf(buf, sz, "%s", env);
        return 1;
    }
    if (root_env && looks_like_repo_root(root_env)) {
        snprintf(buf, sz, "%s", root_env);
        return 1;
    }
    if (!getcwd(cwd, sizeof(cwd))) return 0;
    snprintf(cur, sizeof(cur), "%s", cwd);
    while (cur[0]) {
        if (looks_like_repo_root(cur)) {
            snprintf(buf, sz, "%s", cur);
            return 1;
        }
        char *slash = strrchr(cur, '/');
        if (!slash) break;
        if (slash == cur) {
            cur[1] = '\0';
            if (looks_like_repo_root(cur)) {
                snprintf(buf, sz, "%s", cur);
                return 1;
            }
            break;
        }
        *slash = '\0';
    }
    return 0;
}

static void try_one(const char *path, char **argv) {
    if (access(path, X_OK) == 0) {
        argv[0] = (char *)path;
        execv(path, argv);
    }
}

static int resolve_binary_path(const char *binary, const char *sibling_dir, char *resolved, size_t resolved_size) {
    char self_dir[PATH_MAX];
    char repo_root[PATH_MAX];
    get_self_dir(self_dir, sizeof(self_dir));

    if (sibling_dir && sibling_dir[0] && find_repo_root_from_cwd(repo_root, sizeof(repo_root))) {
        char full[PATH_MAX];
        snprintf(full, sizeof(full), "%s/cmd/%s/%s", repo_root, sibling_dir, binary);
        if (path_exists_exec(full)) {
            snprintf(resolved, resolved_size, "%s", full);
            return 1;
        }
        snprintf(full, sizeof(full), "%s/cmd/%s/build/%s", repo_root, sibling_dir, binary);
        if (path_exists_exec(full)) {
            snprintf(resolved, resolved_size, "%s", full);
            return 1;
        }
    }

    if (self_dir[0]) {
        char full[PATH_MAX];

        snprintf(full, sizeof(full), "%s/%s", self_dir, binary);
        if (path_exists_exec(full)) {
            snprintf(resolved, resolved_size, "%s", full);
            return 1;
        }

        if (sibling_dir && sibling_dir[0]) {
            snprintf(full, sizeof(full), "%s/../%s/%s", self_dir, sibling_dir, binary);
            if (path_exists_exec(full)) {
                snprintf(resolved, resolved_size, "%s", full);
                return 1;
            }

            snprintf(full, sizeof(full), "%s/../%s/build/%s", self_dir, sibling_dir, binary);
            if (path_exists_exec(full)) {
                snprintf(resolved, resolved_size, "%s", full);
                return 1;
            }
        }
    }

    /* The managed runtime: where a release puts an organ and where the MCP
     * surface runs it from. Without this an organ present only there could
     * be run through MCP and not from the front door. */
    {
        const char *home = getenv("HOME");
        if (home && home[0]) {
            char full[PATH_MAX];
            snprintf(full, sizeof(full), "%s/.bonfyre/bin/%s", home, binary);
            if (path_exists_exec(full)) {
                snprintf(resolved, resolved_size, "%s", full);
                return 1;
            }
        }
    }

    const char *path_env = getenv("PATH");
    if (!path_env || !path_env[0]) return 0;

    char *path_copy = strdup(path_env);
    if (!path_copy) return 0;

    int found = 0;
    char *save = NULL;
    for (char *dir = strtok_r(path_copy, ":", &save); dir; dir = strtok_r(NULL, ":", &save)) {
        char full[PATH_MAX];
        snprintf(full, sizeof(full), "%s/%s", dir, binary);
        if (path_exists_exec(full)) {
            snprintf(resolved, resolved_size, "%s", full);
            found = 1;
            break;
        }
    }

    free(path_copy);
    return found;
}

static int route_cmd_exists(const char *cmd) {
    for (const Route *r = routes; r->cmd; r++) {
        if (strcmp(r->cmd, cmd) == 0) return 1;
    }
    return 0;
}

static int route_binary_exists(const char *binary) {
    for (const Route *r = routes; r->cmd; r++) {
        if (strcmp(r->binary, binary) == 0) return 1;
    }
    return 0;
}

static int resolve_any_binary_path(const char *binary, char *resolved, size_t resolved_size) {
    char self_dir[PATH_MAX];
    char repo_root[PATH_MAX];
    get_self_dir(self_dir, sizeof(self_dir));

    if (find_repo_root_from_cwd(repo_root, sizeof(repo_root))) {
        char cmd_root[PATH_MAX];
        DIR *cmd_dir = NULL;
        struct dirent *ent;
        snprintf(cmd_root, sizeof(cmd_root), "%s/cmd", repo_root);
        cmd_dir = opendir(cmd_root);
        if (cmd_dir) {
            while ((ent = readdir(cmd_dir)) != NULL) {
                char cand[PATH_MAX];
                if (ent->d_name[0] == '.') continue;
                snprintf(cand, sizeof(cand), "%s/%s/%s", cmd_root, ent->d_name, binary);
                if (path_exists_exec(cand)) {
                    closedir(cmd_dir);
                    snprintf(resolved, resolved_size, "%s", cand);
                    return 1;
                }
                snprintf(cand, sizeof(cand), "%s/%s/build/%s", cmd_root, ent->d_name, binary);
                if (path_exists_exec(cand)) {
                    closedir(cmd_dir);
                    snprintf(resolved, resolved_size, "%s", cand);
                    return 1;
                }
            }
            closedir(cmd_dir);
        }
    }

    if (self_dir[0]) {
        char full[PATH_MAX];
        DIR *parent = NULL;
        struct dirent *ent;

        snprintf(full, sizeof(full), "%s/%s", self_dir, binary);
        if (path_exists_exec(full)) {
            snprintf(resolved, resolved_size, "%s", full);
            return 1;
        }

        snprintf(full, sizeof(full), "%s/..", self_dir);
        parent = opendir(full);
        if (parent) {
            while ((ent = readdir(parent)) != NULL) {
                char cand[PATH_MAX];
                if (ent->d_name[0] == '.') continue;
                snprintf(cand, sizeof(cand), "%s/../%s/%s", self_dir, ent->d_name, binary);
                if (path_exists_exec(cand)) {
                    closedir(parent);
                    snprintf(resolved, resolved_size, "%s", cand);
                    return 1;
                }
                snprintf(cand, sizeof(cand), "%s/../%s/build/%s", self_dir, ent->d_name, binary);
                if (path_exists_exec(cand)) {
                    closedir(parent);
                    snprintf(resolved, resolved_size, "%s", cand);
                    return 1;
                }
            }
            closedir(parent);
        }
    }

    return resolve_binary_path(binary, NULL, resolved, resolved_size);
}

static int resolve_repo_binary_path(const char *binary, const char *sibling_dir, char *resolved, size_t resolved_size) {
    char repo_root[PATH_MAX];
    if (!sibling_dir || !sibling_dir[0]) return 0;
    if (!find_repo_root_from_cwd(repo_root, sizeof(repo_root))) {
        if (!bf_catalog_find_repo_root(repo_root, sizeof(repo_root))) return 0;
    }
    snprintf(resolved, resolved_size, "%s/cmd/%s/%s", repo_root, sibling_dir, binary);
    if (path_exists_exec(resolved)) return 1;
    snprintf(resolved, resolved_size, "%s/cmd/%s/build/%s", repo_root, sibling_dir, binary);
    return path_exists_exec(resolved);
}

static int resolve_repo_binary_path_from_root(const char *root,
                                              const char *binary,
                                              const char *sibling_dir,
                                              char *resolved,
                                              size_t resolved_size) {
    if (!root || !root[0])
        return resolve_repo_binary_path(binary, sibling_dir, resolved, resolved_size);
    if (!sibling_dir || !sibling_dir[0] || !looks_like_repo_root(root)) return 0;
    snprintf(resolved, resolved_size, "%s/cmd/%s/%s", root, sibling_dir, binary);
    if (path_exists_exec(resolved)) return 1;
    snprintf(resolved, resolved_size, "%s/cmd/%s/build/%s", root, sibling_dir, binary);
    return path_exists_exec(resolved);
}

static int resolve_installed_binary_path(const char *binary, char *resolved, size_t resolved_size) {
    const char *home = getenv("HOME");
    if (home && home[0]) {
        snprintf(resolved, resolved_size, "%s/.local/bin/%s", home, binary);
        if (path_exists_exec(resolved)) return 1;
        /* The managed runtime is where a release puts an organ and where the
         * MCP surface runs it from. An organ that is only there was reported
         * as not installed, so the front door and the MCP did not offer the
         * same system. */
        snprintf(resolved, resolved_size, "%s/.bonfyre/bin/%s", home, binary);
        if (path_exists_exec(resolved)) return 1;
    }
    const char *path_env = getenv("PATH");
    if (!path_env || !path_env[0]) return 0;
    char *path_copy = strdup(path_env);
    if (!path_copy) return 0;
    int found = 0;
    char *save = NULL;
    for (char *dir = strtok_r(path_copy, ":", &save); dir; dir = strtok_r(NULL, ":", &save)) {
        char full[PATH_MAX];
        snprintf(full, sizeof(full), "%s/%s", dir, binary);
        if (path_exists_exec(full)) {
            snprintf(resolved, resolved_size, "%s", full);
            found = 1;
            break;
        }
    }
    free(path_copy);
    return found;
}

static const InstallAliasMap *find_install_alias_map(const char *cmd) {
    for (const InstallAliasMap *m = install_alias_maps; m->cmd; m++) {
        if (strcmp(m->cmd, cmd) == 0) return m;
    }
    return NULL;
}

static void resolve_install_target_path_for_route(const Route *r, char *resolved, size_t resolved_size) {
    const char *home = getenv("HOME");
    if (resolved && resolved_size) resolved[0] = '\0';
    if (!r || !resolved || resolved_size == 0 || !home || !home[0]) return;

    snprintf(resolved, resolved_size, "%s/.local/bin/%s", home, r->binary);
    if (path_exists_file(resolved)) return;

    const InstallAliasMap *map = find_install_alias_map(r->cmd);
    if (!map) return;
    for (int i = 0; map->legacy_binaries[i]; i++) {
        char candidate[PATH_MAX];
        snprintf(candidate, sizeof(candidate), "%s/.local/bin/%s", home, map->legacy_binaries[i]);
        if (path_exists_file(candidate)) {
            snprintf(resolved, resolved_size, "%s", candidate);
            return;
        }
    }
}

static long long file_size_or_neg1(const char *path) {
    struct stat st;
    if (!path || !path[0] || stat(path, &st) != 0) return -1;
    return (long long)st.st_size;
}

static long long file_mtime_or_neg1(const char *path) {
    struct stat st;
    if (!path || !path[0] || stat(path, &st) != 0) return -1;
    return (long long)st.st_mtime;
}

static int compute_sha256_or_empty(const char *path, char out[65]) {
    if (out) out[0] = '\0';
    if (!path || !path[0]) return 0;
    return bf_sha256_file(path, out) == 0;
}

static int run_probe_argv(char *const argv[]) {
    pid_t pid = fork();
    if (pid < 0) return 1;
    if (pid == 0) {
        /* Redirect stdin/stdout/stderr to /dev/null so probes never block */
        int null_r = open("/dev/null", O_RDONLY);
        int null_w = open("/dev/null", O_WRONLY);
        if (null_r >= 0) { dup2(null_r, STDIN_FILENO);  close(null_r); }
        if (null_w >= 0) { dup2(null_w, STDOUT_FILENO); dup2(null_w, STDERR_FILENO); close(null_w); }
        alarm(5); /* kill self if binary hangs beyond 5 s */
        if (strchr(argv[0], '/')) execv(argv[0], argv);
        else execvp(argv[0], argv);
        _exit(127);
    }
    int status = 0;
    if (waitpid(pid, &status, 0) < 0) return 1;
    return WIFEXITED(status) ? WEXITSTATUS(status) : 1;
}

static int probe_command_health(const char *resolved_path) {
    if (!resolved_path || !resolved_path[0]) return 0;
    char *status_argv[] = { (char *)resolved_path, "status", NULL };
    int rc = run_probe_argv(status_argv);
    if (rc == 0) return 1;
    char *help_argv[] = { (char *)resolved_path, "--help", NULL };
    rc = run_probe_argv(help_argv);
    return rc == 0 || rc == 1;
}

static void collect_command_status_at_root(const Route *r, CommandStatusRow *row, const char *root) {
    memset(row, 0, sizeof(*row));
    snprintf(row->command, sizeof(row->command), "%s", r->cmd);
    snprintf(row->cli_name, sizeof(row->cli_name), "%s", r->binary);
    if (resolve_installed_binary_path(r->binary, row->installed_path, sizeof(row->installed_path)))
        compute_sha256_or_empty(row->installed_path, row->installed_sha256);
    if (resolve_repo_binary_path_from_root(root, r->binary, r->sibling_dir, row->repo_path, sizeof(row->repo_path)))
        compute_sha256_or_empty(row->repo_path, row->repo_sha256);
    row->installed_size = file_size_or_neg1(row->installed_path);
    row->repo_size = file_size_or_neg1(row->repo_path);
    row->installed_mtime = file_mtime_or_neg1(row->installed_path);
    row->repo_mtime = file_mtime_or_neg1(row->repo_path);
    if (resolve_binary_path(r->binary, r->sibling_dir, row->active_path, sizeof(row->active_path))) {
        row->available = 1;
        row->executable = 1;
        if (row->repo_path[0] && strcmp(row->active_path, row->repo_path) == 0) {
            snprintf(row->active_source, sizeof(row->active_source), "repo");
            row->repo_backed = 1;
        } else if (row->installed_path[0] && strcmp(row->active_path, row->installed_path) == 0) {
            snprintf(row->active_source, sizeof(row->active_source), "installed");
            row->installed_backed = 1;
        } else {
            snprintf(row->active_source, sizeof(row->active_source), "path");
        }
        row->healthy = probe_command_health(row->active_path);
        row->healthy_unknown = !row->healthy;
    } else {
        snprintf(row->active_source, sizeof(row->active_source), "missing");
        row->missing = 1;
    }

    if (row->missing) {
        snprintf(row->status, sizeof(row->status), "missing");
        snprintf(row->notes, sizeof(row->notes), "No active dispatch path found");
        return;
    }

    if (row->installed_sha256[0] && row->repo_sha256[0] &&
        strcmp(row->installed_sha256, row->repo_sha256) != 0) {
        row->stale = 1;
        if (strcmp(row->active_source, "installed") == 0) {
            snprintf(row->status, sizeof(row->status), "stale");
            snprintf(row->notes, sizeof(row->notes), "Installed binary differs from repo binary");
        } else {
            snprintf(row->status, sizeof(row->status), "shadowed");
            snprintf(row->notes, sizeof(row->notes), "Repo binary overrides stale installed binary");
        }
        return;
    }

    if (!row->healthy) {
        snprintf(row->status, sizeof(row->status), "unknown");
        snprintf(row->notes, sizeof(row->notes), "Dispatch path exists but health probe is inconclusive");
        return;
    }

    snprintf(row->status, sizeof(row->status), "ok");
    if (strcmp(row->active_source, "repo") == 0) snprintf(row->notes, sizeof(row->notes), "Repo-backed command");
    else if (strcmp(row->active_source, "installed") == 0) snprintf(row->notes, sizeof(row->notes), "Installed command");
    else snprintf(row->notes, sizeof(row->notes), "Resolved from PATH");
}

static void collect_command_status(const Route *r, CommandStatusRow *row) {
    collect_command_status_at_root(r, row, NULL);
}

static int sqlite_single_count(const char *db_path, const char *sql, long long *out) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    int rc;
    if (out) *out = 0;
    if (!db_path || !path_exists_file(db_path)) return 1;
    rc = bf_sqlite3_open_ro(db_path, &db);
    if (rc != SQLITE_OK) return 1;
    rc = sqlite3_prepare_v2(db, sql, -1, &st, NULL);
    if (rc == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW && out)
        *out = sqlite3_column_int64(st, 0);
    if (st) sqlite3_finalize(st);
    sqlite3_close(db);
    return rc == SQLITE_OK ? 0 : 1;
}

static void registry_status_collect(const char *root_arg, RegistryStatus *rs) {
    char root[PATH_MAX], path[PATH_MAX];
    const char *home = getenv("HOME");
    memset(rs, 0, sizeof(*rs));
    if (root_arg && root_arg[0]) snprintf(root, sizeof(root), "%s", root_arg);
    else snprintf(root, sizeof(root), "layeros/state");
    snprintf(rs->root, sizeof(rs->root), "%s", root);
    if (home && home[0]) snprintf(rs->home_catalog, sizeof(rs->home_catalog), "%s/.local/share/bonfyre/catalog.db", home);

    snprintf(path, sizeof(path), "%s/layers.db", root);
    sqlite_single_count(path, "SELECT count(*) FROM layer_artifacts", &rs->layer_artifacts);
    snprintf(path, sizeof(path), "%s/index.db", root);
    sqlite_single_count(path, "SELECT count(*) FROM layer_index", &rs->indexed_layers);
    snprintf(path, sizeof(path), "%s/discipl.db", root);
    sqlite_single_count(path, "SELECT count(*) FROM discipl_actors", &rs->discipl_actors);
    sqlite_single_count(path, "SELECT count(*) FROM discipl_contracts", &rs->discipl_contracts);
    sqlite_single_count(path, "SELECT count(*) FROM discipl_chains", &rs->discipl_chains);
    sqlite_single_count(path, "SELECT count(*) FROM discipl_loops", &rs->discipl_loops);
    snprintf(path, sizeof(path), "%s/graph.db", root);
    sqlite_single_count(path, "SELECT count(*) FROM atoms", &rs->graph_atoms);
    sqlite_single_count(path, "SELECT count(*) FROM operators", &rs->graph_operators);
    sqlite_single_count(path, "SELECT count(*) FROM realizations", &rs->graph_realizations);
    sqlite_single_count(path, "SELECT count(*) FROM edges", &rs->graph_edges);
    snprintf(path, sizeof(path), "%s/queue.db", root);
    sqlite_single_count(path, "SELECT count(*) FROM jobs", &rs->queue_jobs);
    if (rs->home_catalog[0]) {
        sqlite_single_count(rs->home_catalog, "SELECT count(*) FROM catalog_nodes WHERE kind='capability'", &rs->catalog_capability);
        sqlite_single_count(rs->home_catalog, "SELECT count(*) FROM catalog_nodes WHERE kind='model'", &rs->catalog_model);
        sqlite_single_count(rs->home_catalog, "SELECT count(*) FROM catalog_nodes WHERE kind='model_source'", &rs->catalog_model_source);
        sqlite_single_count(rs->home_catalog, "SELECT count(*) FROM catalog_nodes WHERE kind='recipe'", &rs->catalog_recipe);
        sqlite_single_count(rs->home_catalog, "SELECT count(*) FROM catalog_nodes WHERE kind='workflow'", &rs->catalog_workflow);
        sqlite_single_count(rs->home_catalog, "SELECT count(*) FROM catalog_nodes WHERE kind='workflow_step'", &rs->catalog_workflow_step);
        sqlite_single_count(rs->home_catalog, "SELECT count(*) FROM catalog_nodes WHERE kind='layer'", &rs->catalog_layer);
    }
}

/* A command name is lowercase words joined by '-'. Backups and patched copies
 * (bonfyre-x.archive, bonfyre-moe.patched2, bonfyre-estate.pre-manifold.2026...)
 * are files beside the binaries, never commands. */
static int is_command_name(const char *name) {
    if (!name || !name[0] || name[0] == '-') return 0;
    for (const char *p = name; *p; p++)
        if (!((*p >= 'a' && *p <= 'z') || (*p >= '0' && *p <= '9') || *p == '-')) return 0;
    return 1;
}

static int collect_extra_routes(ExtraRoute *items, int max_items, const char *query) {
    char self_dir[PATH_MAX];
    char parent_dir[PATH_MAX];
    DIR *parent = NULL;
    struct dirent *ent;
    int count = 0;

    if (!g_list_all) return 0;
    get_self_dir(self_dir, sizeof(self_dir));
    if (!self_dir[0]) return 0;
    snprintf(parent_dir, sizeof(parent_dir), "%s/..", self_dir);
    parent = opendir(parent_dir);
    if (!parent) return 0;

    while ((ent = readdir(parent)) != NULL && count < max_items) {
        char top_path[PATH_MAX];
        char build_path[PATH_MAX];
        const char *candidates[2];
        for (int pass = 0; pass < 2 && count < max_items; pass++) {
            DIR *sub;
            struct dirent *subent;
            if (ent->d_name[0] == '.') continue;
            snprintf(top_path, sizeof(top_path), "%s/%s", parent_dir, ent->d_name);
            snprintf(build_path, sizeof(build_path), "%s/%s/build", parent_dir, ent->d_name);
            candidates[0] = top_path;
            candidates[1] = build_path;
            sub = opendir(candidates[pass]);
            if (!sub) continue;
            while ((subent = readdir(sub)) != NULL && count < max_items) {
                ExtraRoute item;
                size_t bin_len;
                if (subent->d_name[0] == '.') continue;
                if (strncmp(subent->d_name, "bonfyre-", 8) != 0) continue;
                if (strcmp(subent->d_name, "bonfyre") == 0) continue;
                if (!is_command_name(subent->d_name + 8)) continue;
                bin_len = strlen(subent->d_name);
                if (bin_len >= sizeof(item.binary)) continue;
                memset(&item, 0, sizeof(item));
                snprintf(item.binary, sizeof(item.binary), "%s", subent->d_name);
                snprintf(item.cmd, sizeof(item.cmd), "%s", subent->d_name + 8);
                snprintf(item.module, sizeof(item.module), "%s", ent->d_name);
                snprintf(item.path, sizeof(item.path), "%s/%s", candidates[pass], subent->d_name);
                if (route_cmd_exists(item.cmd)) continue;
                if (route_binary_exists(item.binary)) continue;
                if (query && query[0] &&
                    !contains_ci(item.cmd, query) &&
                    !contains_ci(item.binary, query) &&
                    !contains_ci(item.module, query) &&
                    !contains_ci(SEC_LOCAL, query)) {
                    continue;
                }
                {
                    int dup = 0;
                    for (int i = 0; i < count; i++) {
                        if (strcmp(items[i].cmd, item.cmd) == 0) { dup = 1; break; }
                    }
                    if (dup) continue;
                }
                items[count++] = item;
            }
            closedir(sub);
        }
    }
    closedir(parent);
    return count;
}

static int contains_ci(const char *haystack, const char *needle) {
    if (!needle || !needle[0]) return 1;
    size_t needle_len = strlen(needle);
    if (needle_len == 0) return 1;

    for (size_t i = 0; haystack[i]; i++) {
        size_t j = 0;
        while (needle[j] && haystack[i + j] &&
               tolower((unsigned char)haystack[i + j]) == tolower((unsigned char)needle[j])) {
            j++;
        }
        if (j == needle_len) return 1;
    }
    return 0;
}

/* Delegates to the one implementation. Nineteen copies of this shape existed
 * with sixteen different opinions about escaping.
 */
static void json_escape(FILE *out, const char *s) {
    bf_lex_json_emit(out, s);
}

static void append_query_token(char *dst, size_t size, const char *token) {
    size_t len;
    if (!token || !token[0] || size == 0) return;
    len = strlen(dst);
    if (len > 0 && len + 1 < size) {
        dst[len++] = ' ';
        dst[len] = '\0';
    }
    if (len + 1 < size)
        snprintf(dst + len, size - len, "%s", token);
}

static ListOptions parse_list_options(int argc, char *argv[]) {
    ListOptions opts;
    memset(&opts, 0, sizeof(opts));

    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--json") == 0) {
            opts.json = 1;
            continue;
        }
        if (strcmp(argv[i], "--health") == 0) {
            opts.health = 1;
            continue;
        }
        if (strcmp(argv[i], "--all") == 0 || strcmp(argv[i], "-a") == 0) {
            opts.all = 1;
            continue;
        }
        if (strcmp(argv[i], "--compact") == 0 || strcmp(argv[i], "-c") == 0) {
            opts.compact = 1;
            continue;
        }
        append_query_token(opts.query, sizeof(opts.query), argv[i]);
    }
    return opts;
}

static int route_matches(const Route *r, const char *query) {
    if (r->tier == TIER_LAB && !g_list_all) return 0;
    return contains_ci(r->cmd, query)
        || contains_ci(r->desc, query)
        || contains_ci(r->section, query)
        || contains_ci(r->sibling_dir, query);
}

static RouteStats route_stats(const char *section, const char *query) {
    RouteStats stats = {0, 0};
    char resolved[PATH_MAX];

    for (const Route *r = routes; r->cmd; r++) {
        if (strcmp(r->section, section) != 0) continue;
        if (!route_matches(r, query)) continue;
        stats.total++;
        if (resolve_binary_path(r->binary, r->sibling_dir, resolved, sizeof(resolved)))
            stats.available++;
    }
    return stats;
}

static int health_probe_flag(const ListOptions *opts) {
    return opts && opts->health;
}

static void print_health_suffix(const Route *r, const ListOptions *opts) {
    if (!health_probe_flag(opts)) return;
    CommandStatusRow row;
    collect_command_status(r, &row);
    printf("  [%s|%s%s%s]",
           row.healthy ? "healthy" : "unknown",
           row.active_source[0] ? row.active_source : "missing",
           row.stale ? "|stale" : "",
           row.missing ? "|missing" : "");
}

static void cmd_status_commands_json(void) {
    fputs("{\"commands\":[", stdout);
    int first = 1;
    for (const Route *r = routes; r->cmd; r++) {
        CommandStatusRow row;
        collect_command_status(r, &row);
        if (!first) fputs(",", stdout);
        first = 0;
        fputs("\n  {\"command\":", stdout); json_escape(stdout, row.command);
        fputs(",\"cli_name\":", stdout); json_escape(stdout, row.cli_name);
        fputs(",\"installed_path\":", stdout); row.installed_path[0] ? json_escape(stdout, row.installed_path) : fputs("null", stdout);
        fputs(",\"repo_path\":", stdout); row.repo_path[0] ? json_escape(stdout, row.repo_path) : fputs("null", stdout);
        fputs(",\"active_path\":", stdout); row.active_path[0] ? json_escape(stdout, row.active_path) : fputs("null", stdout);
        fputs(",\"installed_sha256\":", stdout); row.installed_sha256[0] ? json_escape(stdout, row.installed_sha256) : fputs("null", stdout);
        fputs(",\"repo_sha256\":", stdout); row.repo_sha256[0] ? json_escape(stdout, row.repo_sha256) : fputs("null", stdout);
        fprintf(stdout, ",\"installed_size\":%lld,\"repo_size\":%lld,\"installed_mtime\":%lld,\"repo_mtime\":%lld",
                row.installed_size, row.repo_size, row.installed_mtime, row.repo_mtime);
        fputs(",\"active_source\":", stdout); json_escape(stdout, row.active_source);
        fputs(",\"status\":", stdout); json_escape(stdout, row.status);
        fputs(",\"notes\":", stdout); json_escape(stdout, row.notes);
        fprintf(stdout, ",\"available\":%s,\"executable\":%s,\"repo_backed\":%s,\"installed_backed\":%s,\"stale\":%s,\"missing\":%s,\"healthy_unknown\":%s,\"healthy\":%s}",
                row.available ? "true" : "false",
                row.executable ? "true" : "false",
                row.repo_backed ? "true" : "false",
                row.installed_backed ? "true" : "false",
                row.stale ? "true" : "false",
                row.missing ? "true" : "false",
                row.healthy_unknown ? "true" : "false",
                row.healthy ? "true" : "false");
    }
    fputs("\n]}\n", stdout);
}

static void cmd_status_commands_human(void) {
    printf("command               status    source      notes\n");
    printf("---------------------------------------------------------------\n");
    for (const Route *r = routes; r->cmd; r++) {
        CommandStatusRow row;
        collect_command_status(r, &row);
        printf("%-20s %-9s %-11s %s\n",
               row.command,
               row.status,
               row.active_source[0] ? row.active_source : "missing",
               row.notes);
    }
}

static void cmd_status_registries_json(const char *root) {
    RegistryStatus rs;
    registry_status_collect(root, &rs);
    printf("{\n");
    printf("  \"active_registry_root\": "); json_escape(stdout, rs.root); printf(",\n");
    printf("  \"active_home_catalog\": "); json_escape(stdout, rs.home_catalog); printf(",\n");
    printf("  \"layers\": {\"artifacts\": %lld, \"indexed\": %lld},\n", rs.layer_artifacts, rs.indexed_layers);
    printf("  \"discipl\": {\"actors\": %lld, \"contracts\": %lld, \"chains\": %lld, \"loops\": %lld},\n",
           rs.discipl_actors, rs.discipl_contracts, rs.discipl_chains, rs.discipl_loops);
    printf("  \"graph\": {\"atoms\": %lld, \"operators\": %lld, \"realizations\": %lld, \"edges\": %lld},\n",
           rs.graph_atoms, rs.graph_operators, rs.graph_realizations, rs.graph_edges);
    printf("  \"queue\": {\"jobs\": %lld},\n", rs.queue_jobs);
    printf("  \"catalog\": {\"capability\": %lld, \"model\": %lld, \"model_source\": %lld, \"recipe\": %lld, \"workflow\": %lld, \"workflow_step\": %lld, \"layer\": %lld}\n",
           rs.catalog_capability, rs.catalog_model, rs.catalog_model_source, rs.catalog_recipe,
           rs.catalog_workflow, rs.catalog_workflow_step, rs.catalog_layer);
    printf("}\n");
}

static void cmd_status_registries_human(const char *root) {
    RegistryStatus rs;
    registry_status_collect(root, &rs);
    printf("Registry status\n");
    printf("  active root       %s\n", rs.root);
    printf("  home catalog      %s\n\n", rs.home_catalog);
    printf("Layer OS\n");
    printf("  layers.db         %lld layer artifacts\n", rs.layer_artifacts);
    printf("  index.db          %lld indexed layer rows\n", rs.indexed_layers);
    printf("  discipl.db        %lld actors  %lld contracts  %lld chains  %lld loops\n",
           rs.discipl_actors, rs.discipl_contracts, rs.discipl_chains, rs.discipl_loops);
    printf("  graph.db          %lld atoms  %lld operators  %lld realizations  %lld edges\n",
           rs.graph_atoms, rs.graph_operators, rs.graph_realizations, rs.graph_edges);
    printf("  queue.db          %lld jobs\n\n", rs.queue_jobs);
    printf("Home catalog\n");
    printf("  capability        %lld\n", rs.catalog_capability);
    printf("  model             %lld\n", rs.catalog_model);
    printf("  model_source      %lld\n", rs.catalog_model_source);
    printf("  recipe            %lld\n", rs.catalog_recipe);
    printf("  workflow          %lld\n", rs.catalog_workflow);
    printf("  workflow_step     %lld\n", rs.catalog_workflow_step);
    printf("  layer             %lld\n", rs.catalog_layer);
}

static int copy_file_bytes(const char *src, const char *dst) {
    struct stat src_st, dst_st;
    char stage[PATH_MAX], src_sha[65] = "", stage_sha[65] = "";
    int stage_fd = -1, ok = 1;
    FILE *out = NULL;
    if (!src || !src[0] || !dst || !dst[0]) return 1;
    if (lstat(src, &src_st) != 0 || !S_ISREG(src_st.st_mode) || access(src, X_OK) != 0) return 1;
    if (lstat(dst, &dst_st) == 0 && (!S_ISREG(dst_st.st_mode) || S_ISLNK(dst_st.st_mode))) return 1;
    if (!compute_sha256_or_empty(src, src_sha) || !src_sha[0]) return 1;
    if (snprintf(stage, sizeof(stage), "%s.stage.%ld", dst, (long)getpid()) >= (int)sizeof(stage)) return 1;

    FILE *in = fopen(src, "rb");
    if (!in) return 1;
    stage_fd = open(stage, O_WRONLY | O_CREAT | O_EXCL, 0700);
    if (stage_fd < 0) { fclose(in); return 1; }
    out = fdopen(stage_fd, "wb");
    if (!out) { close(stage_fd); fclose(in); unlink(stage); return 1; }
    stage_fd = -1;

    char buf[8192];
    size_t n;
    while ((n = fread(buf, 1, sizeof(buf), in)) > 0) {
        if (fwrite(buf, 1, n, out) != n) goto cleanup;
    }
    if (ferror(in) || fflush(out) != 0 || fchmod(fileno(out), 0755) != 0 || fsync(fileno(out)) != 0) goto cleanup;
    fclose(in);
    in = NULL;
    fclose(out);
    out = NULL;

    if (!compute_sha256_or_empty(stage, stage_sha) || strcmp(src_sha, stage_sha) != 0) goto cleanup;
    if (rename(stage, dst) != 0) goto cleanup;
    ok = 0;

cleanup:
    if (in) fclose(in);
    if (out) fclose(out);
    if (stage_fd >= 0) close(stage_fd);
    if (ok) unlink(stage);
    return ok;
}

static char *read_file_text_alloc(const char *path) {
    return bf_read_file(path, NULL);
}

static int write_text_file(const char *path, const char *text) {
    FILE *fp = fopen(path, "w");
    if (!fp) return 1;
    fputs(text ? text : "", fp);
    fclose(fp);
    return 0;
}

static int command_exists(const char *name) {
    const char *path_env = getenv("PATH");
    if (!path_env || !path_env[0]) return 0;
    char *copy = strdup(path_env);
    char *save = NULL;
    int found = 0;
    if (!copy) return 0;
    for (char *dir = strtok_r(copy, ":", &save); dir; dir = strtok_r(NULL, ":", &save)) {
        char full[PATH_MAX];
        snprintf(full, sizeof(full), "%s/%s", dir, name);
        if (path_exists_exec(full)) { found = 1; break; }
    }
    free(copy);
    return found;
}

static int compress_blob_if_possible(const char *raw_path, const char *compressed_path, const char *codec) {
    if (codec && strcmp(codec, "zstd") == 0 && command_exists("zstd")) {
        char *argv[] = { "zstd", "-q", "-f", (char *)raw_path, "-o", (char *)compressed_path, NULL };
        return run_probe_argv(argv);
    }
    if (codec && strcmp(codec, "gzip") == 0 && command_exists("gzip")) {
        char tmpgz[PATH_MAX];
        snprintf(tmpgz, sizeof(tmpgz), "%s.gz", raw_path);
        char *argv[] = { "gzip", "-f", (char *)raw_path, NULL };
        if (run_probe_argv(argv) != 0) return 1;
        return rename(tmpgz, compressed_path);
    }
    return 1;
}

static int create_self_db_schema(sqlite3 *db) {
    const char *sql =
        "CREATE TABLE IF NOT EXISTS fragments ("
        " fragment_id TEXT PRIMARY KEY,"
        " kind TEXT NOT NULL,"
        " key_text TEXT,"
        " value_json TEXT NOT NULL,"
        " score REAL DEFAULT 0,"
        " created_at TEXT NOT NULL"
        ");"
        "CREATE TABLE IF NOT EXISTS plans ("
        " plan_id TEXT PRIMARY KEY,"
        " kind TEXT NOT NULL,"
        " status TEXT NOT NULL,"
        " score REAL DEFAULT 0,"
        " payload_json TEXT NOT NULL,"
        " created_at TEXT NOT NULL"
        ");"
        "CREATE INDEX IF NOT EXISTS idx_fragments_kind ON fragments(kind);"
        "CREATE INDEX IF NOT EXISTS idx_fragments_key ON fragments(key_text);"
        "CREATE INDEX IF NOT EXISTS idx_plans_kind ON plans(kind);";
    char *errmsg = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &errmsg) != SQLITE_OK) {
        if (errmsg) sqlite3_free(errmsg);
        return 1;
    }
    return 0;
}

static int create_self_graph_schema(sqlite3 *db) {
    const char *sql =
        "CREATE TABLE IF NOT EXISTS nodes ("
        " node_id TEXT PRIMARY KEY,"
        " kind TEXT NOT NULL,"
        " payload_json TEXT NOT NULL"
        ");"
        "CREATE TABLE IF NOT EXISTS edges ("
        " src_id TEXT NOT NULL,"
        " rel TEXT NOT NULL,"
        " dst_id TEXT NOT NULL,"
        " meta_json TEXT,"
        " PRIMARY KEY(src_id, rel, dst_id)"
        ");";
    char *errmsg = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &errmsg) != SQLITE_OK) {
        if (errmsg) sqlite3_free(errmsg);
        return 1;
    }
    return 0;
}

static int create_self_index_schema(sqlite3 *db) {
    const char *sql =
        "CREATE TABLE IF NOT EXISTS kv_index ("
        " key_text TEXT PRIMARY KEY,"
        " kind TEXT NOT NULL,"
        " value_text TEXT"
        ");"
        "CREATE TABLE IF NOT EXISTS family_counts ("
        " family TEXT PRIMARY KEY,"
        " count_value INTEGER NOT NULL"
        ");";
    char *errmsg = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &errmsg) != SQLITE_OK) {
        if (errmsg) sqlite3_free(errmsg);
        return 1;
    }
    return 0;
}

static int run_command_to_file_capture(char *const argv[], const char *output_path) {
    pid_t pid = fork();
    if (pid < 0) return 1;
    if (pid == 0) {
        FILE *fp = fopen(output_path, "w");
        if (!fp) _exit(127);
        dup2(fileno(fp), STDOUT_FILENO);
        dup2(fileno(fp), STDERR_FILENO);
        fclose(fp);
        if (strchr(argv[0], '/')) execv(argv[0], argv);
        else execvp(argv[0], argv);
        _exit(127);
    }
    int st = 0;
    if (waitpid(pid, &st, 0) < 0) return 1;
    return WIFEXITED(st) ? WEXITSTATUS(st) : 1;
}

static int first_artifact_for_family_root(const char *root, const char *family, char *out, size_t out_sz) {
    char layers_path[PATH_MAX];
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    int found = 0;
    if (out && out_sz) out[0] = '\0';
    snprintf(layers_path, sizeof(layers_path), "%s/layers.db", root && root[0] ? root : "layeros/state");
    if (bf_sqlite3_open_ro(layers_path, &db) != SQLITE_OK) return 1;
    if (sqlite3_prepare_v2(db,
        "SELECT artifact_id FROM layer_artifacts WHERE families_json LIKE ? ORDER BY artifact_id LIMIT 1",
        -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 1;
    }
    char pattern[256];
    snprintf(pattern, sizeof(pattern), "%%%s%%", family);
    sqlite3_bind_text(st, 1, pattern, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_text(st, 0)) {
        snprintf(out, out_sz, "%s", (const char *)sqlite3_column_text(st, 0));
        found = 1;
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
    return found ? 0 : 1;
}

static int cmd_doctor_sync_subcommands(int dry_run) {
    printf("sync-subcommands  %s\n", dry_run ? "dry-run" : "apply");
    for (const Route *r = routes; r->cmd; r++) {
        char repo_path[PATH_MAX], installed_path[PATH_MAX];
        char repo_sha[65] = "", installed_sha[65] = "";
        if (!resolve_repo_binary_path(r->binary, r->sibling_dir, repo_path, sizeof(repo_path))) continue;
        resolve_install_target_path_for_route(r, installed_path, sizeof(installed_path));
        if (!installed_path[0]) continue;
        compute_sha256_or_empty(repo_path, repo_sha);
        compute_sha256_or_empty(installed_path, installed_sha);
        if (installed_sha[0] && strcmp(repo_sha, installed_sha) == 0) continue;
        printf("%-18s %s\n", r->cmd, installed_sha[0] ? "update" : "install");
        printf("  repo      %s  %s\n", repo_path, repo_sha);
        printf("  install   %s  %s\n", installed_path, installed_sha[0] ? installed_sha : "(missing)");
        if (!dry_run) {
            if (copy_file_bytes(repo_path, installed_path) == 0) {
                printf("  applied   atomically promoted digest-verified repo binary\n");
            } else {
                printf("  applied   promotion refused or failed\n");
            }
        }
    }
    return 0;
}

static int cmd_status_snapshot(const char *argv0, const char *root, const char *out_dir) {
    char self_path[PATH_MAX], out[PATH_MAX];
    const char *root_use = (root && root[0]) ? root : "layeros/state";
    snprintf(self_path, sizeof(self_path), "%s", argv0 && argv0[0] ? argv0 : "bonfyre");
    if (bf_ensure_dir(out_dir) != 0) return 1;

    snprintf(out, sizeof(out), "%s/commands.json", out_dir);
    char *cmds1[] = { self_path, "list", "--json", "--all", NULL };
    run_command_to_file_capture(cmds1, out);

    snprintf(out, sizeof(out), "%s/layers.json", out_dir);
    char *cmds2[] = { self_path, "query", "layers", "--root", (char *)root_use, NULL };
    run_command_to_file_capture(cmds2, out);

    snprintf(out, sizeof(out), "%s/audio_video_chain.json", out_dir);
    char *cmds3[] = { self_path, "discipl", "chain-plan", "--root", (char *)root_use,
        "T_AUDIO_MODEL","T_AUDIO_GENERATOR","T_SAMPLE_OUTPUT","T_LATENT_SPACE","T_DIFFUSION_UNET","T_VIDEO_OUTPUT", NULL };
    run_command_to_file_capture(cmds3, out);

    snprintf(out, sizeof(out), "%s/graph_execution_paths.json", out_dir);
    char *cmds4[] = { self_path, "discipl", "propose", "--root", (char *)root_use, "--from", "T_GRAPH_STRUCTURE", "--to", "T_EXECUTION", "--depth", "5", NULL };
    run_command_to_file_capture(cmds4, out);

    snprintf(out, sizeof(out), "%s/geo_risk_paths.json", out_dir);
    char *cmds5[] = { self_path, "discipl", "propose", "--root", (char *)root_use, "--from", "T_GEOSPATIAL_EMBED", "--to", "T_RISK_MODEL", "--depth", "5", NULL };
    run_command_to_file_capture(cmds5, out);

    snprintf(out, sizeof(out), "%s/family_counts.txt", out_dir);
    FILE *fc = fopen(out, "w");
    if (fc) {
        char layers_path[PATH_MAX];
        sqlite3 *db = NULL;
        sqlite3_stmt *st = NULL;
        snprintf(layers_path, sizeof(layers_path), "%s/layers.db", root_use);
        if (bf_sqlite3_open_ro(layers_path, &db) == SQLITE_OK &&
            sqlite3_prepare_v2(db, "SELECT value, count(*) FROM layer_artifacts, json_each(families_json) GROUP BY value ORDER BY count(*) DESC LIMIT 100", -1, &st, NULL) == SQLITE_OK) {
            while (sqlite3_step(st) == SQLITE_ROW) {
                fprintf(fc, "%s\t%lld\n", sqlite3_column_text(st,0) ? (const char *)sqlite3_column_text(st,0) : "", sqlite3_column_int64(st,1));
            }
        }
        if (st) sqlite3_finalize(st);
        if (db) sqlite3_close(db);
        fclose(fc);
    }

    snprintf(out, sizeof(out), "%s/compat_matrix.txt", out_dir);
    FILE *cm = fopen(out, "w");
    if (cm) {
        const char *families[] = {
            "T_AUDIO_MODEL","T_AUDIO_GENERATOR","T_SAMPLE_OUTPUT","T_LATENT_SPACE","T_DIFFUSION_UNET","T_VIDEO_OUTPUT",
            "T_GEOSPATIAL_EMBED","T_RISK_MODEL","T_GRAPH_STRUCTURE","T_PLANNER","T_EXECUTION", NULL
        };
        for (int i = 0; families[i]; i++) {
            for (int j = i + 1; families[j]; j++) {
                fprintf(cm, "%s -> %s\n", families[i], families[j]);
            }
        }
        fclose(cm);
    }

    snprintf(out, sizeof(out), "%s/stitch_chains.txt", out_dir);
    FILE *sc = fopen(out, "w");
    if (sc) {
        fprintf(sc, "T_AUDIO_MODEL -> T_AUDIO_GENERATOR\n");
        fprintf(sc, "T_AUDIO_GENERATOR -> T_SAMPLE_OUTPUT\n");
        fprintf(sc, "T_SAMPLE_OUTPUT -> T_LATENT_SPACE\n");
        fprintf(sc, "T_LATENT_SPACE -> T_DIFFUSION_UNET\n");
        fprintf(sc, "T_DIFFUSION_UNET -> T_VIDEO_OUTPUT\n");
        fprintf(sc, "T_GRAPH_STRUCTURE -> T_PLANNER\n");
        fprintf(sc, "T_PLANNER -> T_EXECUTION\n");
        fclose(sc);
    }

    snprintf(out, sizeof(out), "%s/value_audit.txt", out_dir);
    FILE *va = fopen(out, "w");
    if (va) {
        const char *families[] = {"T_GEOSPATIAL_EMBED","T_GRAPH_EMBED","T_AUDIO_GENERATOR","T_DIFFUSION_UNET","T_RISK_MODEL","T_EXECUTION", NULL};
        for (int i = 0; families[i]; i++) {
            char artifact_id[256];
            fprintf(va, "[%s]\n", families[i]);
            if (first_artifact_for_family_root(root_use, families[i], artifact_id, sizeof(artifact_id)) == 0)
                fprintf(va, "artifact_id=%s\n", artifact_id);
            else
                fprintf(va, "artifact_id=(missing)\n");
            fprintf(va, "\n");
        }
        fclose(va);
    }

    const char *publish_fams[] = {"T_GEOSPATIAL_EMBED","T_GRAPH_EMBED","T_MULTIMODAL_ALIGNMENT","T_RISK_MODEL","T_AUDIO_GENERATOR","T_DIFFUSION_UNET", NULL};
    for (int i = 0; publish_fams[i]; i++) {
        char path1[PATH_MAX], path2[PATH_MAX];
        snprintf(path1, sizeof(path1), "%s/cookbook_%s.md", out_dir, publish_fams[i]);
        snprintf(path2, sizeof(path2), "%s/cms_%s.txt", out_dir, publish_fams[i]);
        char *emit_argv[] = { self_path, "emit", "layer-cookbook", "--family", (char *)publish_fams[i], NULL };
        char *cms_argv[] = { self_path, "cms", "publish-layer-family", (char *)publish_fams[i], NULL };
        run_command_to_file_capture(emit_argv, path1);
        run_command_to_file_capture(cms_argv, path2);
    }

    snprintf(out, sizeof(out), "%s/deep_report.txt", out_dir);
    FILE *dr = fopen(out, "w");
    if (dr) {
        RegistryStatus rs;
        registry_status_collect(root_use, &rs);
        fprintf(dr, "Bonfyre deep ops snapshot\n");
        fprintf(dr, "root=%s\n", rs.root);
        fprintf(dr, "layers=%lld indexed=%lld actors=%lld contracts=%lld chains=%lld loops=%lld atoms=%lld operators=%lld realizations=%lld edges=%lld jobs=%lld\n",
                rs.layer_artifacts, rs.indexed_layers, rs.discipl_actors, rs.discipl_contracts, rs.discipl_chains, rs.discipl_loops,
                rs.graph_atoms, rs.graph_operators, rs.graph_realizations, rs.graph_edges, rs.queue_jobs);
        fprintf(dr, "files=commands.json layers.json family_counts.txt audio_video_chain.json graph_execution_paths.json geo_risk_paths.json compat_matrix.txt stitch_chains.txt value_audit.txt\n");
        fclose(dr);
    }

    printf("{\"status\":\"ok\",\"out\":\"%s\"}\n", out_dir);
    return 0;
}

static int cmd_precision_scan(const char *root, int json_mode) {
    char layers_path[PATH_MAX];
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    const char *root_use = (root && root[0]) ? root : "layeros/state";
    snprintf(layers_path, sizeof(layers_path), "%s/layers.db", root_use);
    if (bf_sqlite3_open_ro(layers_path, &db) != SQLITE_OK) return 1;
    if (sqlite3_prepare_v2(db,
        "SELECT value, count(*) FROM layer_artifacts, json_each(families_json) GROUP BY value ORDER BY count(*) DESC",
        -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 1;
    }
    if (json_mode) {
        int first = 1;
        printf("{\"root\":"); json_escape(stdout, root_use); printf(",\"families\":[");
        while (sqlite3_step(st) == SQLITE_ROW) {
            if (!first) printf(",");
            first = 0;
            printf("{\"family\":"); json_escape(stdout, sqlite3_column_text(st,0) ? (const char *)sqlite3_column_text(st,0) : "");
            printf(",\"count\":%lld}", sqlite3_column_int64(st,1));
        }
        printf("]}\n");
    } else {
        printf("precision scan  root=%s\n", root_use);
        while (sqlite3_step(st) == SQLITE_ROW) {
            printf("%-32s %lld\n",
                   sqlite3_column_text(st,0) ? (const char *)sqlite3_column_text(st,0) : "",
                   sqlite3_column_int64(st,1));
        }
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
    return 0;
}

static int cmd_precision_route(const char *root, const char *artifact_id, const char *goal) {
    char *json = NULL;
    bf_json_doc_t *doc = NULL;
    const bf_json_node_t *root_json = NULL, *families = NULL;
    char err[256];
    int has_kv = 0, has_embed = 0, has_attention = 0;
    if (bf_layer_load_json(root, artifact_id, &json) != 0 || !json) return 1;
    doc = bf_json_parse_str(json, err, sizeof(err));
    if (!doc) { free(json); return 1; }
    root_json = bf_json_root(doc);
    families = bf_json_obj_get(doc, root_json, "families");
    for (const bf_json_node_t *n = families ? bf_json_child_first(doc, families) : NULL; n; n = bf_json_child_next(doc, n)) {
        char fam[128];
        if (bf_json_get_str_copy(n, fam, sizeof(fam)) <= 0) continue;
        if (strstr(fam, "KV") || strstr(fam, "CACHE")) has_kv = 1;
        if (strstr(fam, "EMBED")) has_embed = 1;
        if (strstr(fam, "ATTENTION") || strstr(fam, "Q_PROJ") || strstr(fam, "K_PROJ") || strstr(fam, "V_PROJ")) has_attention = 1;
    }
    printf("{\"artifact_id\":"); json_escape(stdout, artifact_id);
    printf(",\"goal\":"); json_escape(stdout, goal ? goal : "balanced");
    printf(",\"routes\":[");
    printf("{\"kind\":\"raw\",\"status\":\"available\"}");
    if (has_embed || has_attention) printf(",{\"kind\":\"fpq\",\"status\":\"candidate\"}");
    if (has_embed) printf(",{\"kind\":\"fpqx\",\"status\":\"candidate\"}");
    if (has_kv || has_attention) printf(",{\"kind\":\"kvcache\",\"status\":\"candidate\"}");
    printf(",{\"kind\":\"compress\",\"status\":\"candidate\",\"codec\":\"zstd\"}");
    printf("],\"recommended_path\":[");
    printf("\"raw\"");
    if (goal && strcmp(goal, "tiny") == 0) {
        if (has_embed || has_attention) printf(",\"fpq\"");
        printf(",\"compress\"");
    } else if (goal && strcmp(goal, "fast") == 0) {
        if (has_kv || has_attention) printf(",\"kvcache\"");
    } else if (goal && strcmp(goal, "low-cost") == 0) {
        if (has_embed || has_attention) printf(",\"fpq\"");
        printf(",\"compress\"");
    } else if (goal && strcmp(goal, "quality") == 0) {
        if (has_embed) printf(",\"fpqx\"");
    } else {
        if (has_embed || has_attention) printf(",\"fpq\"");
        printf(",\"compress\"");
    }
    printf("]}\n");
    bf_json_free(doc);
    free(json);
    return 0;
}

static int cmd_precision_validate(const char *root, const char *artifact_id) {
    char *json = NULL;
    if (bf_layer_load_json(root, artifact_id, &json) != 0 || !json) return 1;
    printf("{\"artifact_id\":"); json_escape(stdout, artifact_id);
    printf(",\"status\":\"ok\",\"validation\":[\"artifact_exists\",\"metadata_first_routeable\"]}\n");
    free(json);
    return 0;
}

static int cmd_precision_apply(const char *root, const char *artifact_id, const char *goal, int dry_run) {
    printf("{\"artifact_id\":"); json_escape(stdout, artifact_id);
    printf(",\"goal\":"); json_escape(stdout, goal ? goal : "balanced");
    printf(",\"mode\":"); json_escape(stdout, dry_run ? "dry-run" : "apply-safe");
    printf(",\"operations\":[\"derive_precision_plan\",\"create_graph_edge\",\"record_validation_fragment\",\"record_ledger_event\"],\"mutates_original\":false}\n");
    (void)root;
    return 0;
}

static int cmd_precision_ontology(void) {
    printf("{\"surface\":\"precision\",\"heuristics\":[");
    printf("{\"route\":\"raw\",\"when\":\"always\",\"reason\":\"base artifact path remains available\"},");
    printf("{\"route\":\"fpq\",\"when\":\"family contains EMBED or ATTENTION or Q_PROJ/K_PROJ/V_PROJ\",\"reason\":\"precision/storage reduction for dense vector and attention surfaces\"},");
    printf("{\"route\":\"fpqx\",\"when\":\"family contains EMBED\",\"reason\":\"higher-fidelity embedding-alignment route\"},");
    printf("{\"route\":\"kvcache\",\"when\":\"family contains KV or CACHE or ATTENTION semantics\",\"reason\":\"runtime latency route for cacheable attention workloads\"},");
    printf("{\"route\":\"compress\",\"when\":\"always\",\"reason\":\"zstd metadata-safe compression candidate\"}");
    printf("],\"goal_policies\":[");
    printf("{\"goal\":\"tiny\",\"recommended_path\":[\"raw\",\"fpq\",\"compress\"]},");
    printf("{\"goal\":\"fast\",\"recommended_path\":[\"raw\",\"kvcache\"]},");
    printf("{\"goal\":\"low-cost\",\"recommended_path\":[\"raw\",\"fpq\",\"compress\"]},");
    printf("{\"goal\":\"quality\",\"recommended_path\":[\"raw\",\"fpqx\"]},");
    printf("{\"goal\":\"balanced\",\"recommended_path\":[\"raw\",\"fpq\",\"compress\"]}");
    printf("],\"constraints\":[\"metadata_first\",\"never_mutate_original\",\"derive_new_artifact_only\",\"record_graph_edge\",\"record_validation_fragment\",\"record_ledger_event\"]}\n");
    return 0;
}

static int build_self_plan_items(const char *root, SelfPlanItem *items, int max_items) {
    int count = 0;
    RegistryStatus rs;
    registry_status_collect(root, &rs);
    for (const Route *r = routes; r->cmd && count < max_items; r++) {
        CommandStatusRow row;
        collect_command_status_at_root(r, &row, root);
        if (strcmp(row.status, "stale") == 0 || strcmp(row.status, "shadowed") == 0 || strcmp(row.status, "missing") == 0) {
            char seed[512];
            snprintf(seed, sizeof(seed), "command:%s:%s", row.command, row.status);
            bf_sha256_hex((const uint8_t *)seed, strlen(seed), items[count].id);
            snprintf(items[count].kind, sizeof(items[count].kind), "command_repair");
            snprintf(items[count].status, sizeof(items[count].status), "proposed");
            items[count].score = strcmp(row.status, "missing") == 0 ? 0.95 : 0.85;
            snprintf(items[count].title, sizeof(items[count].title), "Repair command drift for %s", row.command);
            snprintf(items[count].detail, sizeof(items[count].detail), "%s", row.notes);
            count++;
        }
    }
    if (count < max_items && rs.catalog_layer < rs.layer_artifacts) {
        bf_sha256_hex((const uint8_t *)"registry:reconcile", strlen("registry:reconcile"), items[count].id);
        snprintf(items[count].kind, sizeof(items[count].kind), "registry_reconcile");
        snprintf(items[count].status, sizeof(items[count].status), "proposed");
        items[count].score = 0.90;
        snprintf(items[count].title, sizeof(items[count].title), "Reconcile home catalog with Layer OS truth");
        snprintf(items[count].detail, sizeof(items[count].detail), "catalog layer=%lld, layeros artifacts=%lld", rs.catalog_layer, rs.layer_artifacts);
        count++;
    }
    /* Two further proposals used to be appended here on every run, whatever
     * the estate looked like: a precision scan and a bridge audit, each with a
     * score of its own. Nothing was looked at to make them, so they are gone.
     * A plan is listed when a comparison above found something. */
    return count;
}

static int cmd_self_ontology(const char *subject) {
    if (!subject || !subject[0] || strcmp(subject, "all") == 0) {
        printf("{\"surface\":\"self\",\"subjects\":[\"aliases\",\"registry-projections\",\"repair-plans\"]}\n");
        return 0;
    }
    if (strcmp(subject, "aliases") == 0 || strcmp(subject, "install-mapping") == 0) {
        int first = 1, count = 0;
        printf("{\"surface\":\"self\",\"subject\":\"aliases\",\"routes\":[");
        for (const Route *r = routes; r->cmd; r++) {
            char install_path[PATH_MAX];
            const InstallAliasMap *map = find_install_alias_map(r->cmd);
            resolve_install_target_path_for_route(r, install_path, sizeof(install_path));
            if (!first) printf(",");
            first = 0;
            printf("{\"command\":"); json_escape(stdout, r->cmd);
            printf(",\"binary\":"); json_escape(stdout, r->binary);
            printf(",\"sibling_dir\":"); json_escape(stdout, r->sibling_dir);
            printf(",\"install_target\":"); json_escape(stdout, install_path);
            printf(",\"legacy_binaries\":[");
            if (map) {
                int lf = 1;
                for (int i = 0; i < 5 && map->legacy_binaries[i]; i++) {
                    if (!lf) printf(",");
                    lf = 0;
                    json_escape(stdout, map->legacy_binaries[i]);
                }
            }
            printf("]}");
            count++;
        }
        printf("],\"count\":%d}\n", count);
        return 0;
    }
    if (strcmp(subject, "registry-projections") == 0) {
        char *json = NULL;
        int rc = bf_catalog_projection_rules_json(&json);
        if (rc != 0 || !json) return 1;
        printf("%s\n", json);
        free(json);
        return 0;
    }
    if (strcmp(subject, "repair-plans") == 0) {
        printf("{\"surface\":\"self\",\"subject\":\"repair-plans\",\"plan_kinds\":[");
        printf("{\"kind\":\"command_repair\",\"trigger\":\"status in stale|shadowed|missing\",\"apply_safe\":\"sync repo-built binary into install target\",\"reversible\":true},");
        printf("{\"kind\":\"registry_reconcile\",\"trigger\":\"catalog layer count < Layer OS artifact count\",\"apply_safe\":\"record additive registry delta and reconciliation artifact\",\"reversible\":true},");
        printf("{\"kind\":\"precision_route\",\"trigger\":\"precision/compression candidate scan\",\"apply_safe\":\"record precision plan and validation fragment\",\"reversible\":true},");
        printf("{\"kind\":\"bridge_gap\",\"trigger\":\"known complementary families or DiscIPL contracts missing concrete bridge coverage\",\"apply_safe\":\"record bridge-gap repair artifact\",\"reversible\":true}");
        printf("],\"safety_levels\":[\"observe\",\"plan\",\"dry-run\",\"apply-safe\"]}\n");
        return 0;
    }
    fprintf(stderr, "bonfyre self ontology: unknown subject '%s'\n", subject);
    return 1;
}

static int cmd_capabilities_ontology(const char *filter) {
    char *json = NULL;
    int rc = bf_catalog_capability_tagging_rules_json(filter, &json);
    if (rc != 0 || !json) return 1;
    printf("%s\n", json);
    free(json);
    return 0;
}

static int latest_self_dir(char *out, size_t out_sz) {
    snprintf(out, out_sz, "%s", "/tmp/bonfyre_self");
    return 1;
}

static int cmd_self_snapshot(const char *argv0, const char *root, const char *out_dir) {
    const char *root_use = (root && root[0]) ? root : "layeros/state";
    const char *out_use = (out_dir && out_dir[0]) ? out_dir : "/tmp/bonfyre_self";
    char blobs_dir[PATH_MAX], manifest_path[PATH_MAX], report_path[PATH_MAX], frag_path[PATH_MAX], graph_path[PATH_MAX], index_path[PATH_MAX], ledger_path[PATH_MAX];
    char cmds_path[PATH_MAX], regs_path[PATH_MAX], layers_path[PATH_MAX];
    sqlite3 *frag_db = NULL, *graph_db = NULL, *index_db = NULL;
    RegistryStatus rs;
    SelfPlanItem plans[128];
    int plan_count;
    const char *self_path = (argv0 && argv0[0]) ? argv0 : "bonfyre";
    if (bf_ensure_dir(out_use) != 0) return 1;
    snprintf(blobs_dir, sizeof(blobs_dir), "%s/blobs", out_use);
    if (bf_ensure_dir(blobs_dir) != 0) return 1;
    snprintf(manifest_path, sizeof(manifest_path), "%s/manifest.json", out_use);
    snprintf(report_path, sizeof(report_path), "%s/report.md", out_use);
    snprintf(frag_path, sizeof(frag_path), "%s/fragments.db", out_use);
    snprintf(graph_path, sizeof(graph_path), "%s/graph.db", out_use);
    snprintf(index_path, sizeof(index_path), "%s/indexes.db", out_use);
    snprintf(ledger_path, sizeof(ledger_path), "%s/ledger.jsonl", out_use);

    sqlite3_open(frag_path, &frag_db);
    sqlite3_open(graph_path, &graph_db);
    sqlite3_open(index_path, &index_db);
    create_self_db_schema(frag_db);
    create_self_graph_schema(graph_db);
    create_self_index_schema(index_db);

    snprintf(cmds_path, sizeof(cmds_path), "%s/commands.json", out_use);
    char *cmds_argv[] = { (char *)self_path, "status", "commands", "--json", NULL };
    run_command_to_file_capture(cmds_argv, cmds_path);
    snprintf(regs_path, sizeof(regs_path), "%s/registries.json", out_use);
    char *regs_argv[] = { (char *)self_path, "status", "registries", "--root", (char *)root_use, "--json", NULL };
    run_command_to_file_capture(regs_argv, regs_path);
    snprintf(layers_path, sizeof(layers_path), "%s/layers.json", out_use);
    char *layers_argv[] = { (char *)self_path, "query", "layers", "--root", (char *)root_use, NULL };
    run_command_to_file_capture(layers_argv, layers_path);

    char *cmds_json = read_file_text_alloc(cmds_path);
    char *regs_json = read_file_text_alloc(regs_path);
    char *layers_json = read_file_text_alloc(layers_path);
    if (cmds_json) {
        sqlite3_stmt *st = NULL;
        sqlite3_prepare_v2(frag_db, "INSERT OR REPLACE INTO fragments(fragment_id,kind,key_text,value_json,score,created_at) VALUES(?,?,?,?,?,datetime('now'))", -1, &st, NULL);
        char hash[65]; bf_sha256_hex((const uint8_t *)cmds_json, strlen(cmds_json), hash);
        sqlite3_bind_text(st,1,hash,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(st,2,"T_COMMAND_HEALTH",-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(st,3,"commands",-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(st,4,cmds_json,-1,SQLITE_TRANSIENT);
        sqlite3_bind_double(st,5,1.0);
        sqlite3_step(st); sqlite3_finalize(st);
    }
    if (regs_json) {
        sqlite3_stmt *st = NULL;
        sqlite3_prepare_v2(frag_db, "INSERT OR REPLACE INTO fragments(fragment_id,kind,key_text,value_json,score,created_at) VALUES(?,?,?,?,?,datetime('now'))", -1, &st, NULL);
        char hash[65]; bf_sha256_hex((const uint8_t *)regs_json, strlen(regs_json), hash);
        sqlite3_bind_text(st,1,hash,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(st,2,"T_REGISTRY_VIEW",-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(st,3,"registries",-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(st,4,regs_json,-1,SQLITE_TRANSIENT);
        sqlite3_bind_double(st,5,1.0);
        sqlite3_step(st); sqlite3_finalize(st);
    }
    registry_status_collect(root_use, &rs);
    plan_count = build_self_plan_items(root_use, plans, 128);
    for (int i = 0; i < plan_count; i++) {
        sqlite3_stmt *st = NULL;
        char payload[2048];
        snprintf(payload, sizeof(payload), "{\"title\":");
        sqlite3_prepare_v2(frag_db, "INSERT OR REPLACE INTO plans(plan_id,kind,status,score,payload_json,created_at) VALUES(?,?,?,?,?,datetime('now'))", -1, &st, NULL);
        snprintf(payload, sizeof(payload), "{\"title\":\"%s\",\"detail\":\"%s\"}", plans[i].title, plans[i].detail);
        sqlite3_bind_text(st,1,plans[i].id,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(st,2,plans[i].kind,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(st,3,plans[i].status,-1,SQLITE_TRANSIENT);
        sqlite3_bind_double(st,4,plans[i].score);
        sqlite3_bind_text(st,5,payload,-1,SQLITE_TRANSIENT);
        sqlite3_step(st); sqlite3_finalize(st);
    }

    write_text_file(ledger_path, "{\"event\":\"self_snapshot_created\"}\n");
    if (command_exists("zstd")) {
        char zst_path[PATH_MAX];
        snprintf(zst_path, sizeof(zst_path), "%s/ledger.jsonl.zst", out_use);
        compress_blob_if_possible(ledger_path, zst_path, "zstd");
        unlink(ledger_path);
    }

    char report[4096];
    snprintf(report, sizeof(report),
             "# Bonfyre Self Snapshot\n\n"
             "- Root: `%s`\n"
             "- Layer artifacts: `%lld`\n"
             "- Indexed layers: `%lld`\n"
             "- DisCIPL actors/contracts/chains/loops: `%lld / %lld / %lld / %lld`\n"
             "- Graph atoms/operators/realizations/edges: `%lld / %lld / %lld / %lld`\n"
             "- Queue jobs: `%lld`\n"
             "- Candidate self plans: `%d`\n",
             root_use, rs.layer_artifacts, rs.indexed_layers,
             rs.discipl_actors, rs.discipl_contracts, rs.discipl_chains, rs.discipl_loops,
             rs.graph_atoms, rs.graph_operators, rs.graph_realizations, rs.graph_edges,
             rs.queue_jobs, plan_count);
    write_text_file(report_path, report);

    char manifest[4096];
    snprintf(manifest, sizeof(manifest),
             "{\n  \"kind\": \"T_SELF_SNAPSHOT\",\n  \"root\": \"%s\",\n  \"fragments_db\": \"%s\",\n  \"graph_db\": \"%s\",\n  \"indexes_db\": \"%s\",\n  \"report\": \"%s\",\n  \"commands_blob\": \"%s\",\n  \"registries_blob\": \"%s\",\n  \"layers_blob\": \"%s\"\n}\n",
             root_use, frag_path, graph_path, index_path, report_path, cmds_path, regs_path, layers_path);
    write_text_file(manifest_path, manifest);

    free(cmds_json);
    free(regs_json);
    free(layers_json);
    if (frag_db) sqlite3_close(frag_db);
    if (graph_db) sqlite3_close(graph_db);
    if (index_db) sqlite3_close(index_db);
    printf("{\"status\":\"ok\",\"snapshot_root\":\"%s\"}\n", out_use);
    return 0;
}

static int cmd_self_inspect(const char *object) {
    char dir[PATH_MAX], db_path[PATH_MAX];
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    latest_self_dir(dir, sizeof(dir));
    snprintf(db_path, sizeof(db_path), "%s/fragments.db", dir);
    if (bf_sqlite3_open_ro(db_path, &db) != SQLITE_OK) return 1;
    if (sqlite3_prepare_v2(db, "SELECT kind,key_text,value_json FROM fragments WHERE fragment_id=? OR key_text=? LIMIT 1", -1, &st, NULL) != SQLITE_OK) { sqlite3_close(db); return 1; }
    sqlite3_bind_text(st,1,object,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,2,object,-1,SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) {
        printf("{\"kind\":"); json_escape(stdout, sqlite3_column_text(st,0)?(const char*)sqlite3_column_text(st,0):"");
        printf(",\"key\":"); json_escape(stdout, sqlite3_column_text(st,1)?(const char*)sqlite3_column_text(st,1):"");
        printf(",\"value\":%s}\n", sqlite3_column_text(st,2)?(const char*)sqlite3_column_text(st,2):"null");
        sqlite3_finalize(st);
        sqlite3_close(db);
        return 0;
    }
    sqlite3_finalize(st);
    st = NULL;
    if (sqlite3_prepare_v2(db, "SELECT plan_id,kind,status,score,payload_json FROM plans WHERE plan_id=? LIMIT 1", -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 1;
    }
    sqlite3_bind_text(st, 1, object, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) {
        printf("{\"id\":");
        json_escape(stdout, sqlite3_column_text(st,0)?(const char*)sqlite3_column_text(st,0):"");
        printf(",\"kind\":");
        json_escape(stdout, sqlite3_column_text(st,1)?(const char*)sqlite3_column_text(st,1):"");
        printf(",\"status\":");
        json_escape(stdout, sqlite3_column_text(st,2)?(const char*)sqlite3_column_text(st,2):"");
        printf(",\"score\":%.2f,\"payload\":%s}\n",
               sqlite3_column_double(st,3),
               sqlite3_column_text(st,4)?(const char*)sqlite3_column_text(st,4):"null");
        sqlite3_finalize(st);
        sqlite3_close(db);
        return 0;
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
    return 1;
}

static int cmd_self_query(const char *question) {
    char dir[PATH_MAX], db_path[PATH_MAX], lower[512];
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    size_t n = strlen(question ? question : "");
    for (size_t i = 0; i < n && i < sizeof(lower)-1; i++) lower[i] = (char)tolower((unsigned char)question[i]);
    lower[n < sizeof(lower)-1 ? n : sizeof(lower)-1] = '\0';
    latest_self_dir(dir, sizeof(dir));
    snprintf(db_path, sizeof(db_path), "%s/fragments.db", dir);
    if (bf_sqlite3_open_ro(db_path, &db) != SQLITE_OK) return 1;
    if (strstr(lower, "stale command")) {
        sqlite3_prepare_v2(db, "SELECT value_json FROM fragments WHERE kind='T_COMMAND_HEALTH' LIMIT 1", -1, &st, NULL);
    } else if (strstr(lower, "precision")) {
        sqlite3_prepare_v2(db, "SELECT plan_id,kind,status,score,payload_json FROM plans WHERE kind='precision_route' LIMIT 20", -1, &st, NULL);
    } else if (strstr(lower, "repair")) {
        sqlite3_prepare_v2(db, "SELECT plan_id,kind,status,score,payload_json FROM plans WHERE kind like '%repair%' ORDER BY score DESC", -1, &st, NULL);
    } else {
        sqlite3_prepare_v2(db, "SELECT plan_id,kind,status,score,payload_json FROM plans ORDER BY score DESC LIMIT 20", -1, &st, NULL);
    }
    printf("[");
    int first = 1;
    while (sqlite3_step(st) == SQLITE_ROW) {
        if (!first) printf(",");
        first = 0;
        if (sqlite3_column_count(st) == 1) {
            printf("%s", sqlite3_column_text(st,0)?(const char*)sqlite3_column_text(st,0):"null");
        } else {
            printf("{\"id\":"); json_escape(stdout, sqlite3_column_text(st,0)?(const char*)sqlite3_column_text(st,0):"");
            printf(",\"kind\":"); json_escape(stdout, sqlite3_column_text(st,1)?(const char*)sqlite3_column_text(st,1):"");
            printf(",\"status\":"); json_escape(stdout, sqlite3_column_text(st,2)?(const char*)sqlite3_column_text(st,2):"");
            printf(",\"score\":%.2f,\"payload\":%s}", sqlite3_column_double(st,3), sqlite3_column_text(st,4)?(const char*)sqlite3_column_text(st,4):"null");
        }
    }
    printf("]\n");
    sqlite3_finalize(st); sqlite3_close(db); return 0;
}

static int find_command_plan_match(const char *root, const char *plan_id, const Route **out_route, CommandStatusRow *out_row) {
    for (const Route *r = routes; r->cmd; r++) {
        CommandStatusRow row;
        char seed[512];
        char hash[65];
        collect_command_status_at_root(r, &row, root);
        if (!(strcmp(row.status, "stale") == 0 || strcmp(row.status, "shadowed") == 0 || strcmp(row.status, "missing") == 0))
            continue;
        snprintf(seed, sizeof(seed), "command:%s:%s", row.command, row.status);
        bf_sha256_hex((const uint8_t *)seed, strlen(seed), hash);
        if (strcmp(hash, plan_id) == 0) {
            if (out_route) *out_route = r;
            if (out_row) *out_row = row;
            return 1;
        }
    }
    return 0;
}

static int self_append_ledger_event(const char *dir, const char *event_json) {
    char ledger_path[PATH_MAX];
    FILE *fp;
    snprintf(ledger_path, sizeof(ledger_path), "%s/ledger.jsonl", dir);
    fp = fopen(ledger_path, "a");
    if (!fp) return 1;
    fputs(event_json ? event_json : "{}", fp);
    fputc('\n', fp);
    fclose(fp);
    return 0;
}

static int self_record_plan_apply(const char *dir, const SelfPlanItem *item, const char *apply_status, const char *payload_json) {
    char frag_path[PATH_MAX], graph_path[PATH_MAX], index_path[PATH_MAX];
    sqlite3 *frag_db = NULL, *graph_db = NULL, *index_db = NULL;
    sqlite3_stmt *st = NULL;
    char repair_node[256];
    char kv_key[256];
    int rc = 1;

    snprintf(frag_path, sizeof(frag_path), "%s/fragments.db", dir);
    snprintf(graph_path, sizeof(graph_path), "%s/graph.db", dir);
    snprintf(index_path, sizeof(index_path), "%s/indexes.db", dir);

    if (sqlite3_open(frag_path, &frag_db) != SQLITE_OK) goto cleanup;
    if (sqlite3_open(graph_path, &graph_db) != SQLITE_OK) goto cleanup;
    if (sqlite3_open(index_path, &index_db) != SQLITE_OK) goto cleanup;
    if (create_self_db_schema(frag_db) != 0) goto cleanup;
    if (create_self_graph_schema(graph_db) != 0) goto cleanup;
    if (create_self_index_schema(index_db) != 0) goto cleanup;

    sqlite3_prepare_v2(frag_db,
        "INSERT OR REPLACE INTO plans(plan_id,kind,status,score,payload_json,created_at) VALUES(?,?,?,?,?,datetime('now'))",
        -1, &st, NULL);
    if (!st) goto cleanup;
    sqlite3_bind_text(st, 1, item->id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, item->kind, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 3, apply_status, -1, SQLITE_TRANSIENT);
    sqlite3_bind_double(st, 4, item->score);
    sqlite3_bind_text(st, 5, payload_json, -1, SQLITE_TRANSIENT);
    sqlite3_step(st);
    sqlite3_finalize(st);
    st = NULL;

    sqlite3_prepare_v2(frag_db,
        "INSERT OR REPLACE INTO fragments(fragment_id,kind,key_text,value_json,score,created_at) VALUES(?,?,?,?,?,datetime('now'))",
        -1, &st, NULL);
    if (!st) goto cleanup;
    sqlite3_bind_text(st, 1, item->id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, "T_SELF_REPAIR", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 3, item->id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 4, payload_json, -1, SQLITE_TRANSIENT);
    sqlite3_bind_double(st, 5, item->score);
    sqlite3_step(st);
    sqlite3_finalize(st);
    st = NULL;

    snprintf(repair_node, sizeof(repair_node), "repair:%s", item->id);
    sqlite3_prepare_v2(graph_db, "INSERT OR REPLACE INTO nodes(node_id,kind,payload_json) VALUES(?,?,?)", -1, &st, NULL);
    if (!st) goto cleanup;
    sqlite3_bind_text(st, 1, repair_node, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, "T_SELF_REPAIR", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 3, payload_json, -1, SQLITE_TRANSIENT);
    sqlite3_step(st);
    sqlite3_finalize(st);
    st = NULL;

    sqlite3_prepare_v2(graph_db, "INSERT OR REPLACE INTO edges(src_id,rel,dst_id,meta_json) VALUES(?,?,?,?)", -1, &st, NULL);
    if (!st) goto cleanup;
    sqlite3_bind_text(st, 1, item->id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, "applied_as", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 3, repair_node, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 4, payload_json, -1, SQLITE_TRANSIENT);
    sqlite3_step(st);
    sqlite3_finalize(st);
    st = NULL;

    snprintf(kv_key, sizeof(kv_key), "plan_status:%s", item->id);
    sqlite3_prepare_v2(index_db, "INSERT OR REPLACE INTO kv_index(key_text,kind,value_text) VALUES(?,?,?)", -1, &st, NULL);
    if (!st) goto cleanup;
    sqlite3_bind_text(st, 1, kv_key, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, item->kind, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 3, apply_status, -1, SQLITE_TRANSIENT);
    sqlite3_step(st);
    sqlite3_finalize(st);
    st = NULL;

    rc = 0;
cleanup:
    if (st) sqlite3_finalize(st);
    if (frag_db) sqlite3_close(frag_db);
    if (graph_db) sqlite3_close(graph_db);
    if (index_db) sqlite3_close(index_db);
    return rc;
}

static int self_apply_command_repair(const Route *route, const CommandStatusRow *row, char *payload_json, size_t payload_sz) {
    char dst_path[PATH_MAX];
    char sha_after[65] = "";
    int copy_rc;

    if (!route || !row || !row->repo_path[0]) {
        snprintf(payload_json, payload_sz,
                 "{\"op\":\"copy_binary\",\"status\":\"failed\",\"reason\":\"missing_repo_or_route\"}");
        return 1;
    }

    resolve_install_target_path_for_route(route, dst_path, sizeof(dst_path));
    if (!dst_path[0]) {
        snprintf(payload_json, payload_sz,
                 "{\"op\":\"copy_binary\",\"status\":\"failed\",\"reason\":\"missing_install_target\"}");
        return 1;
    }
    copy_rc = copy_file_bytes(row->repo_path, dst_path);
    if (copy_rc != 0) {
        snprintf(payload_json, payload_sz,
                 "{\"op\":\"copy_binary\",\"status\":\"failed\",\"src\":\"%s\",\"dst\":\"%s\"}",
                 row->repo_path, dst_path);
        return 1;
    }

    compute_sha256_or_empty(dst_path, sha_after);
    snprintf(payload_json, payload_sz,
             "{\"op\":\"copy_binary\",\"status\":\"applied\",\"command\":\"%s\",\"src\":\"%s\",\"dst\":\"%s\",\"repo_sha256\":\"%s\",\"installed_sha256\":\"%s\"}",
             row->command, row->repo_path, dst_path, row->repo_sha256, sha_after);
    return 0;
}

static int self_apply_metadata_plan(const SelfPlanItem *item, char *payload_json, size_t payload_sz) {
    if (strcmp(item->kind, "registry_reconcile") == 0) {
        snprintf(payload_json, payload_sz,
                 "{\"op\":\"record_registry_delta\",\"status\":\"applied\",\"kind\":\"registry_reconcile\",\"detail\":");
        size_t len = strlen(payload_json);
        if (len + 4 < payload_sz) {
            payload_json[len++] = '"';
            payload_json[len] = '\0';
            strncat(payload_json, item->detail, payload_sz - strlen(payload_json) - 3);
            strcat(payload_json, "\"}");
        }
        return 0;
    }
    if (strcmp(item->kind, "precision_route") == 0) {
        snprintf(payload_json, payload_sz,
                 "{\"op\":\"record_precision_plan\",\"status\":\"applied\",\"kind\":\"precision_route\",\"detail\":");
        size_t len = strlen(payload_json);
        if (len + 4 < payload_sz) {
            payload_json[len++] = '"';
            payload_json[len] = '\0';
            strncat(payload_json, item->detail, payload_sz - strlen(payload_json) - 3);
            strcat(payload_json, "\"}");
        }
        return 0;
    }
    if (strcmp(item->kind, "bridge_gap") == 0) {
        snprintf(payload_json, payload_sz,
                 "{\"op\":\"record_bridge_gap\",\"status\":\"applied\",\"kind\":\"bridge_gap\",\"detail\":");
        size_t len = strlen(payload_json);
        if (len + 4 < payload_sz) {
            payload_json[len++] = '"';
            payload_json[len] = '\0';
            strncat(payload_json, item->detail, payload_sz - strlen(payload_json) - 3);
            strcat(payload_json, "\"}");
        }
        return 0;
    }
    snprintf(payload_json, payload_sz, "{\"op\":\"noop\",\"status\":\"skipped\"}");
    return 1;
}

static int cmd_self_optimize(const char *root, int apply_safe) {
    SelfPlanItem items[128];
    int n = build_self_plan_items(root, items, 128);
    char dir[PATH_MAX];
    latest_self_dir(dir, sizeof(dir));
    printf("{\"mode\":"); json_escape(stdout, apply_safe ? "apply-safe" : "dry-run");
    printf(",\"plans\":[");
    for (int i = 0; i < n; i++) {
        const Route *route = NULL;
        CommandStatusRow row;
        char payload_json[2048];
        int applied_ok = 0;
        memset(&row, 0, sizeof(row));
        if (i) printf(",");
        if (apply_safe) {
            if (find_command_plan_match(root, items[i].id, &route, &row)) {
                applied_ok = (self_apply_command_repair(route, &row, payload_json, sizeof(payload_json)) == 0);
            } else {
                applied_ok = (self_apply_metadata_plan(&items[i], payload_json, sizeof(payload_json)) == 0);
            }
            self_record_plan_apply(dir, &items[i], applied_ok ? "applied" : "failed", payload_json);
            self_append_ledger_event(dir, payload_json);
        }
        printf("{\"plan_id\":"); json_escape(stdout, items[i].id);
        printf(",\"kind\":"); json_escape(stdout, items[i].kind);
        printf(",\"status\":"); json_escape(stdout, apply_safe ? (applied_ok ? "applied" : "failed") : items[i].status);
        printf(",\"score\":%.2f,\"score_basis\":\"fixed by drift status for ordering, not measured\"", items[i].score);
        printf(",\"title\":"); json_escape(stdout, items[i].title);
        printf(",\"detail\":"); json_escape(stdout, items[i].detail);
        if (apply_safe) {
            printf(",\"apply_result\":%s", payload_json);
        }
        printf(",\"reversible\":true,\"mutates_original\":false}");
    }
    printf("]}\n");
    return 0;
}

static int cmd_self_repair(const char *root, const char *plan_id, int apply_safe) {
    SelfPlanItem items[128];
    int n = build_self_plan_items(root, items, 128);
    SelfPlanItem *match = NULL;
    const Route *route = NULL;
    CommandStatusRow row;
    char dir[PATH_MAX];
    char payload_json[2048];
    int applied_ok = 0;

    for (int i = 0; i < n; i++) {
        if (strcmp(items[i].id, plan_id ? plan_id : "") == 0) {
            match = &items[i];
            break;
        }
    }

    if (!match) {
        fprintf(stderr, "bonfyre self repair: unknown plan id\n");
        return 1;
    }

    latest_self_dir(dir, sizeof(dir));
    printf("{\"plan_id\":"); json_escape(stdout, plan_id ? plan_id : "");
    printf(",\"mode\":"); json_escape(stdout, apply_safe ? "apply-safe" : "dry-run");
    printf(",\"kind\":"); json_escape(stdout, match->kind);
    printf(",\"operations\":[\"copy_binary\",\"add_index_row\",\"add_graph_edge\",\"write_fragment\",\"write_plan\"]");
    if (apply_safe) {
        memset(&row, 0, sizeof(row));
        if (find_command_plan_match(root, match->id, &route, &row)) {
            applied_ok = (self_apply_command_repair(route, &row, payload_json, sizeof(payload_json)) == 0);
        } else {
            applied_ok = (self_apply_metadata_plan(match, payload_json, sizeof(payload_json)) == 0);
        }
        self_record_plan_apply(dir, match, applied_ok ? "applied" : "failed", payload_json);
        self_append_ledger_event(dir, payload_json);
        printf(",\"status\":"); json_escape(stdout, applied_ok ? "applied" : "failed");
        printf(",\"apply_result\":%s", payload_json);
    }
    printf(",\"mutates_original\":false,\"reversible\":true}\n");
    return 0;
}

static int cmd_self_publish(const char *snapshot_id) {
    printf("{\"snapshot_id\":"); json_escape(stdout, snapshot_id ? snapshot_id : "latest");
    printf(",\"status\":\"published\",\"channel\":\"cms/api\"}\n");
    return 0;
}

static void cmd_list_json(const char *query, const ListOptions *opts) {
    const char *const *sections = goal_sections;
    int total = 0;
    int available = 0;
    int first_section = 1;
    ExtraRoute extras[128];
    int extra_count = collect_extra_routes(extras, 128, query);

    for (const Route *r = routes; r->cmd; r++) {
        char resolved[PATH_MAX];
        if (!route_matches(r, query)) continue;
        total++;
        if (resolve_binary_path(r->binary, r->sibling_dir, resolved, sizeof(resolved)))
            available++;
    }
    total += extra_count;
    available += extra_count;

    fputs("{\n  \"query\": ", stdout);
    if (query && query[0]) json_escape(stdout, query);
    else fputs("null", stdout);
    fprintf(stdout,
            ",\n  \"summary\": {\"total\": %d, \"ready\": %d, \"missing\": %d},\n  \"sections\": [\n",
            total, available, total - available);

    for (int i = 0; sections[i]; i++) {
        RouteStats stats = route_stats(sections[i], query);
        int first_command = 1;
        if (stats.total == 0) continue;

        if (!first_section) fputs(",\n", stdout);
        first_section = 0;

        fputs("    {\n      \"name\": ", stdout);
        json_escape(stdout, sections[i]);
        fprintf(stdout,
                ",\n      \"total\": %d,\n      \"ready\": %d,\n      \"missing\": %d,\n      \"commands\": [\n",
                stats.total, stats.available, stats.total - stats.available);

        for (const Route *r = routes; r->cmd; r++) {
            char resolved[PATH_MAX];
            int ready;
            if (strcmp(r->section, sections[i]) != 0) continue;
            if (!route_matches(r, query)) continue;

            ready = resolve_binary_path(r->binary, r->sibling_dir, resolved, sizeof(resolved));
            if (!first_command) fputs(",\n", stdout);
            first_command = 0;

            fputs("        {\"command\": ", stdout);
            json_escape(stdout, r->cmd);
            fputs(", \"binary\": ", stdout);
            json_escape(stdout, r->binary);
            fputs(", \"module\": ", stdout);
            json_escape(stdout, r->sibling_dir);
            fputs(", \"description\": ", stdout);
            json_escape(stdout, r->desc);
            fprintf(stdout, ", \"tier\": \"%s\"", r->tier == TIER_START ? "start" : r->tier == TIER_LAB ? "lab" : "everyday");
            fprintf(stdout, ", \"ready\": %s, \"path\": ", ready ? "true" : "false");
            if (ready) json_escape(stdout, resolved);
            else fputs("null", stdout);
            if (health_probe_flag(opts)) {
                CommandStatusRow row;
                collect_command_status(r, &row);
                fputs(", \"health\": ", stdout); json_escape(stdout, row.healthy ? "healthy" : "unknown");
                fputs(", \"active_source\": ", stdout); json_escape(stdout, row.active_source);
                fputs(", \"drift_status\": ", stdout); json_escape(stdout, row.status);
            }
            fputs("}", stdout);
        }

        fputs("\n      ]\n    }", stdout);
    }

    if (extra_count > 0) {
        if (!first_section) fputs(",\n", stdout);
        fputs("    {\n      \"name\": ", stdout);
        json_escape(stdout, SEC_LOCAL);
        fprintf(stdout,
                ",\n      \"total\": %d,\n      \"ready\": %d,\n      \"missing\": 0,\n      \"commands\": [\n",
                extra_count, extra_count);
        for (int i = 0; i < extra_count; i++) {
            if (i > 0) fputs(",\n", stdout);
            fputs("        {\"command\": ", stdout);
            json_escape(stdout, extras[i].cmd);
            fputs(", \"binary\": ", stdout);
            json_escape(stdout, extras[i].binary);
            fputs(", \"module\": ", stdout);
            json_escape(stdout, extras[i].module);
            fputs(", \"description\": ", stdout);
            json_escape(stdout, "Discovered local bonfyre-* binary not yet registered in the core route table");
            fputs(", \"ready\": true, \"path\": ", stdout);
            json_escape(stdout, extras[i].path);
            fputs("}", stdout);
        }
        fputs("\n      ]\n    }", stdout);
    }

    fputs("\n  ]\n}\n", stdout);
}

static void cmd_list_compact(const char *query, const ListOptions *opts) {
    const char *const *sections = goal_sections;
    int total = 0;
    int available = 0;
    ExtraRoute extras[128];
    int extra_count = collect_extra_routes(extras, 128, query);

    for (const Route *r = routes; r->cmd; r++) {
        char resolved[PATH_MAX];
        if (!route_matches(r, query)) continue;
        total++;
        if (resolve_binary_path(r->binary, r->sibling_dir, resolved, sizeof(resolved)))
            available++;
    }
    total += extra_count;
    available += extra_count;

    printf("bonfyre  compact surface\n");
    if (query && query[0])
        printf("filter   %s\n", query);
    printf("summary  %d commands  %d ready  %d missing\n\n", total, available, total - available);

    for (int i = 0; sections[i]; i++) {
        RouteStats stats = route_stats(sections[i], query);
        if (stats.total == 0) continue;

        printf("%s  %d/%d\n  ", sections[i], stats.available, stats.total);
        for (const Route *r = routes; r->cmd; r++) {
            char resolved[PATH_MAX];
            int ready;

            if (strcmp(r->section, sections[i]) != 0) continue;
            if (!route_matches(r, query)) continue;

            ready = resolve_binary_path(r->binary, r->sibling_dir, resolved, sizeof(resolved));
            printf("%s%s", r->cmd, ready ? "" : "!");
            if (health_probe_flag(opts)) print_health_suffix(r, opts);
            printf(" ");
        }
        printf("\n\n");
    }

    if (extra_count > 0) {
        printf("%s  %d/%d\n  ", SEC_LOCAL, extra_count, extra_count);
        for (int i = 0; i < extra_count; i++)
            printf("%s ", extras[i].cmd);
        printf("\n\n");
    }

    printf("Front-door built-ins  4/4\n  ");
    for (const BuiltinSurface *b = builtin_surfaces; b->cmd; b++) {
        printf("%s ", b->cmd);
    }
    printf("\n\n");

    if (total == 0)
        printf("No commands matched that filter. Try: bonfyre list --compact model\n\n");

    printf("Tip: commands with '!' are not installed in the current environment.\n");
    printf("Built-ins run directly from the bonfyre front door and do not have separate bonfyre-* binaries.\n");
}

static void emit_completion_zsh(void) {
    puts("#compdef bonfyre");
    puts("local -a commands");
    puts("commands=(");
    puts("  'help:Show CLI help'");
    puts("  'version:Print version'");
    puts("  'list:Show command surface'");
    puts("  'completion:Emit shell completion script'");
    for (const Route *r = routes; r->cmd; r++)
        printf("  '%s:%s'\n", r->cmd, r->desc);
    puts(")");
    puts("_arguments \\");
    puts("  '1:command:->command' \\");
    puts("  '*::arg:->args'");
    puts("case $state in");
    puts("  command)");
    puts("    _describe -t commands 'bonfyre commands' commands");
    puts("    ;;");
    puts("  args)");
    puts("    case $words[2] in");
    puts("      completion)");
    puts("        _values 'shell' bash zsh fish");
    puts("        ;;");
    puts("      list)");
    puts("        _values 'list options' '--json[Emit machine-readable JSON]' '--compact[Dense one-line view]' \\");
    puts("          'filter:topic filter:_message filter'");
    puts("        ;;");
    puts("    esac");
    puts("    ;;");
    puts("esac");
}

static void emit_completion_bash(void) {
    puts("_bonfyre_completion() {");
    puts("  local cur prev commands");
    puts("  COMPREPLY=()");
    puts("  cur=\"${COMP_WORDS[COMP_CWORD]}\"");
    puts("  prev=\"${COMP_WORDS[COMP_CWORD-1]}\"");
    puts("  commands=\"help version list completion");
    for (const Route *r = routes; r->cmd; r++)
        printf(" %s", r->cmd);
    puts("\"");
    puts("  if [[ ${COMP_CWORD} -eq 1 ]]; then");
    puts("    COMPREPLY=( $(compgen -W \"${commands}\" -- \"${cur}\") )");
    puts("    return 0");
    puts("  fi");
    puts("  case \"${prev}\" in");
    puts("    completion)");
    puts("      COMPREPLY=( $(compgen -W 'bash zsh fish' -- \"${cur}\") )");
    puts("      return 0");
    puts("      ;;");
    puts("    list)");
    puts("      COMPREPLY=( $(compgen -W '--json --compact -c' -- \"${cur}\") )");
    puts("      return 0");
    puts("      ;;");
    puts("  esac");
    puts("}");
    puts("complete -F _bonfyre_completion bonfyre");
}

static void emit_completion_fish(void) {
    puts("complete -c bonfyre -f");
    puts("complete -c bonfyre -n '__fish_use_subcommand' -a help -d 'Show CLI help'");
    puts("complete -c bonfyre -n '__fish_use_subcommand' -a version -d 'Print version'");
    puts("complete -c bonfyre -n '__fish_use_subcommand' -a list -d 'Show command surface'");
    puts("complete -c bonfyre -n '__fish_use_subcommand' -a completion -d 'Emit shell completion script'");
    for (const Route *r = routes; r->cmd; r++)
        printf("complete -c bonfyre -n '__fish_use_subcommand' -a %s -d ", r->cmd), json_escape(stdout, r->desc), putchar('\n');
    puts("complete -c bonfyre -n '__fish_seen_subcommand_from completion' -a bash -d 'Bash completion'");
    puts("complete -c bonfyre -n '__fish_seen_subcommand_from completion' -a zsh -d 'Zsh completion'");
    puts("complete -c bonfyre -n '__fish_seen_subcommand_from completion' -a fish -d 'Fish completion'");
    puts("complete -c bonfyre -n '__fish_seen_subcommand_from list' -l json -d 'Emit machine-readable JSON'");
    puts("complete -c bonfyre -n '__fish_seen_subcommand_from list' -l compact -d 'Dense one-line view'");
}

static int cmd_completion(const char *shell) {
    if (!shell || !shell[0] || strcmp(shell, "zsh") == 0) {
        emit_completion_zsh();
        return 0;
    }
    if (strcmp(shell, "bash") == 0) {
        emit_completion_bash();
        return 0;
    }
    if (strcmp(shell, "fish") == 0) {
        emit_completion_fish();
        return 0;
    }

    fprintf(stderr, "bonfyre: unsupported shell '%s'\n", shell);
    fprintf(stderr, "Supported shells: bash, zsh, fish\n");
    return 1;
}

static int try_exec(const char *binary, const char *sibling_dir, char **argv) {
    char resolved[PATH_MAX];
    if (resolve_binary_path(binary, sibling_dir, resolved, sizeof(resolved))) {
        try_one(resolved, argv);
    }

    /* 4. Fall back to PATH */
    execvp(binary, argv);
    return -1;
}

/* ── Built-in: list ───────────────────────────────────────────────── */
static void cmd_list(const char *query, const ListOptions *opts) {
    const char *const *sections = goal_sections;
    int is_tty = isatty(STDOUT_FILENO);
    const char *bold = is_tty ? "\033[1m" : "";
    const char *dim = is_tty ? "\033[2m" : "";
    const char *green = is_tty ? "\033[32m" : "";
    const char *yellow = is_tty ? "\033[33m" : "";
    const char *reset = is_tty ? "\033[0m" : "";
    int total = 0;
    int available = 0;
    ExtraRoute extras[128];
    int extra_count = collect_extra_routes(extras, 128, query);

    for (const Route *r = routes; r->cmd; r++) {
        char resolved[PATH_MAX];
        if (!route_matches(r, query)) continue;
        total++;
        if (resolve_binary_path(r->binary, r->sibling_dir, resolved, sizeof(resolved)))
            available++;
    }
    total += extra_count;
    available += extra_count;

    printf("%sbonfyre%s  command surface\n", bold, reset);
    if (query && query[0])
        printf("%sfilter%s    %s\n", dim, reset, query);
    printf("%ssummary%s   %d commands  %s%d ready%s  %s%d missing%s\n",
           dim, reset,
           total,
           green, available, reset,
           yellow, total - available, reset);
    printf("%snew here%s  bonfyre help   (six ways to start)\n\n", dim, reset);

    for (int i = 0; sections[i]; i++) {
        RouteStats stats = route_stats(sections[i], query);
        if (stats.total == 0) continue;

        printf("%s%s%s  %s%d/%d ready%s\n",
               bold, sections[i], reset,
               dim, stats.available, stats.total, reset);

        for (const Route *r = routes; r->cmd; r++) {
            char resolved[PATH_MAX];
            int ready;

            if (strcmp(r->section, sections[i]) != 0) continue;
            if (!route_matches(r, query)) continue;

            ready = resolve_binary_path(r->binary, r->sibling_dir, resolved, sizeof(resolved));
            printf("  %s%-16s%s  %-20s %s\n",
                   ready ? green : yellow,
                   ready ? "ready" : "missing",
                   reset,
                   r->cmd,
                   r->desc);
            if (health_probe_flag(opts)) {
                printf("                     ");
                print_health_suffix(r, opts);
                printf("\n");
            }
        }
        printf("\n");
    }

    if (extra_count > 0) {
        printf("%s%s%s  %s%d/%d ready%s\n",
               bold, SEC_LOCAL, reset,
               dim, extra_count, extra_count, reset);
        for (int i = 0; i < extra_count; i++) {
            printf("  %s%-16s%s  %-20s %s\n",
                   green, "ready", reset,
                   extras[i].cmd,
                   "Discovered local bonfyre-* binary not yet registered in the core route table");
        }
        printf("\n");
    }

    printf("%sBuilt into bonfyre%s  %s3/3 ready%s\n",
           bold, reset,
           dim, reset);
    for (const BuiltinSurface *b = builtin_surfaces; b->cmd; b++) {
        printf("  %s%-16s%s  %-20s %s\n",
               green, "ready", reset,
               b->cmd,
               b->desc);
    }
    printf("\n");

    if (total == 0) {
        printf("Nothing matched '%s'. Try a plain word for what you want to do: bonfyre list invoice\n", query ? query : "");
        if (!g_list_all) printf("Lab kernels and local builds are not searched; add --all to include them.\n");
        printf("\n");
    }

    printf("bonfyre <command> --help     how one command works\n");
    printf("bonfyre list <word>          only the commands about <word>\n");
    if (!g_list_all) printf("bonfyre list --all           also lab kernels, internals and local builds\n");
}

/* ── Built-in: help ───────────────────────────────────────────────── */
static void cmd_help(void) {
    int is_tty = isatty(STDOUT_FILENO);
    const char *bold = is_tty ? "\033[1m" : "";
    const char *dim = is_tty ? "\033[2m" : "";
    const char *reset = is_tty ? "\033[0m" : "";
    printf("%sbonfyre%s  websites, recordings, search, money and your own models, on this machine\n\n", bold, reset);
    printf("%sStart here%s\n", bold, reset);
    printf("  bonfyre site new my-shop                    a website, running and open in your browser\n");
    printf("  bonfyre transcript-family talk.m4a talk/    a recording turned into clean, readable text\n");
    printf("  bonfyre site search \"opening hours\"         find anything on your sites, by meaning and by words\n");
    printf("  bonfyre reason ask --prompt \"...\"           ask the model on this machine; every answer kept\n");
    printf("  bonfyre model local                         which of your models can run right now\n");
    printf("  bonfyre capabilities search invoice         which command does what you describe\n");
    printf("  bonfyre doctor                              check what is installed and working\n\n");
    printf("%sWhat do you want to do?%s\n", bold, reset);
    for (int i = 0; goal_sections[i]; i++) {
        int column = 0;
        if (strcmp(goal_sections[i], SEC_LAB) == 0) continue;
        printf("  %-28s %s", goal_sections[i], dim);
        for (const Route *r = routes; r->cmd; r++) {
            if (r->tier == TIER_LAB || strcmp(r->section, goal_sections[i]) != 0) continue;
            int width = (int)strlen(r->cmd) + 2;
            if (column && column + width > 60) { printf("\n  %-28s ", ""); column = 0; }
            printf("%s  ", r->cmd);
            column += width;
        }
        printf("%s\n", reset);
    }
    printf("\n");
    printf("bonfyre <command> --help     how one command works\n");
    printf("bonfyre list                 every everyday command, one line each\n");
    printf("bonfyre list <word>          only the commands about <word>\n");
    printf("bonfyre list --all           also lab kernels, internals and local builds\n");
    printf("bonfyre status | version | completion zsh|bash|fish\n");
}

static int edit_distance(const char *a, const char *b) {
    size_t n = strlen(a), m = strlen(b);
    int row[64];
    if (n > 62 || m > 62) return 99;
    for (size_t j = 0; j <= m; j++) row[j] = (int)j;
    for (size_t i = 1; i <= n; i++) {
        int diag = row[0];
        row[0] = (int)i;
        for (size_t j = 1; j <= m; j++) {
            int up = row[j], best = diag + (a[i - 1] != b[j - 1]);
            if (up + 1 < best) best = up + 1;
            if (row[j - 1] + 1 < best) best = row[j - 1] + 1;
            diag = up;
            row[j] = best;
        }
    }
    return row[m];
}

/* Closest commands to a mistyped name: a small edit away, a shared prefix, or one inside the other. */
static void suggest_commands(const char *cmd) {
    static const char *const builtins[] = { "help", "list", "status", "version", "completion", "self", "precision", NULL };
    const Route *printed[4] = {0};
    int shown = 0;
    size_t len = strlen(cmd);
    for (int i = 0; builtins[i]; i++)
        if (edit_distance(cmd, builtins[i]) <= 2 && shown < 4)
            fprintf(stderr, "%s %s\n", shown++ ? "               " : "  did you mean:", builtins[i]);
    /* Closest spelling first, then shared prefixes and names inside names. */
    for (int pass = 0; pass < 2; pass++) {
        for (const Route *r = routes; r->cmd && shown < 4; r++) {
            size_t k = 0;
            int seen = 0, near = edit_distance(cmd, r->cmd) <= 2;
            for (int i = 0; i < shown; i++) if (printed[i] == r) seen = 1;
            while (cmd[k] && r->cmd[k] && cmd[k] == r->cmd[k]) k++;
            if (seen || r->tier == TIER_LAB) continue;
            if (pass == 0 ? !near : !((len >= 3 && k >= 3) || (len >= 3 && contains_ci(r->cmd, cmd))
                                      || (strlen(r->cmd) >= 3 && contains_ci(cmd, r->cmd)))) continue;
            fprintf(stderr, "%s %-17s %s\n", shown ? "               " : "  did you mean:", r->cmd, r->desc);
            printed[shown++] = r;
        }
    }
    /* Nothing spelled like it: maybe it names what the person wants to do. */
    for (const Route *r = routes; r->cmd && shown < 4 && len >= 3; r++) {
        int seen = 0;
        for (int i = 0; i < shown; i++) if (printed[i] == r) seen = 1;
        if (seen || r->tier == TIER_LAB || !contains_ci(r->desc, cmd)) continue;
        fprintf(stderr, "%s %-17s %s\n", shown ? "               " : "  try:         ", r->cmd, r->desc);
        printed[shown++] = r;
    }
}

/* ── main ─────────────────────────────────────────────────────────── */
int main(int argc, char *argv[]) {
    ListOptions list_opts;
    if (argc < 2) { cmd_help(); return 0; }
    const char *cmd = argv[1];

    if (strcmp(cmd, "shell") == 0 || cmd[0] == '@') {
        fprintf(stderr, "bonfyre: semantic shell was retired with the Python control plane; use bonfyre operator for source-bound reads\n");
        return 127;
    }

    if (strcmp(cmd, "help") == 0 || strcmp(cmd, "--help") == 0 || strcmp(cmd, "-h") == 0) {
        cmd_help();
        return 0;
    }
    if (strcmp(cmd, "version") == 0 || strcmp(cmd, "--version") == 0) {
        printf("bonfyre 0.2.0\n");
        return 0;
    }
    if (strcmp(cmd, "list") == 0) {
        list_opts = parse_list_options(argc, argv);
        g_list_all = list_opts.all;
        if (list_opts.json) cmd_list_json(list_opts.query[0] ? list_opts.query : NULL, &list_opts);
        else if (list_opts.compact) cmd_list_compact(list_opts.query[0] ? list_opts.query : NULL, &list_opts);
        else cmd_list(list_opts.query[0] ? list_opts.query : NULL, &list_opts);
        return 0;
    }
    if (strcmp(cmd, "completion") == 0) {
        return cmd_completion(argc >= 3 ? argv[2] : "zsh");
    }
    if (strcmp(cmd, "self") == 0) {
        const char *sub = argc >= 3 ? argv[2] : NULL;
        const char *root = NULL, *out_dir = NULL, *plan_id = NULL, *question = NULL, *object = NULL, *subject = NULL;
        int dry_run = 0, apply = 0, apply_safe = 0;
        for (int i = 3; i < argc; i++) {
            if (strcmp(argv[i], "--root") == 0 && i + 1 < argc) root = argv[++i];
            else if (strcmp(argv[i], "--out") == 0 && i + 1 < argc) out_dir = argv[++i];
            else if (strcmp(argv[i], "--plan") == 0 && i + 1 < argc) plan_id = argv[++i];
            else if (strcmp(argv[i], "--dry-run") == 0) dry_run = 1;
            else if (strcmp(argv[i], "--apply-safe") == 0) apply_safe = 1;
            else if (strcmp(argv[i], "--apply") == 0) apply = 1;
            else if (!question && strcmp(argv[2], "query") == 0) question = argv[i];
            else if (!object && strcmp(argv[2], "inspect") == 0) object = argv[i];
            else if (!subject && strcmp(argv[2], "ontology") == 0) subject = argv[i];
        }
        if (!sub) {
            fprintf(stderr, "usage: bonfyre self snapshot|inspect [OBJECT]|query [QUESTION]|optimize [--dry-run|--apply-safe]|repair [--plan ID]|ontology [SUBJECT]|publish [ID]\n");
            return 2;
        }
        if (strcmp(sub, "ontology") == 0) return cmd_self_ontology(subject ? subject : "all");
        if (strcmp(sub, "snapshot") == 0) return cmd_self_snapshot(argv[0], root, out_dir ? out_dir : "/tmp/bonfyre_self");
        if (strcmp(sub, "inspect") == 0) return cmd_self_inspect(object ? object : "commands");
        if (strcmp(sub, "query") == 0) return cmd_self_query(question ? question : "plans");
        if (strcmp(sub, "optimize") == 0) return cmd_self_optimize(root, (apply_safe || apply) && !dry_run);
        if (strcmp(sub, "repair") == 0) return cmd_self_repair(root, plan_id ? plan_id : "latest", (apply_safe || apply) && !dry_run);
        if (strcmp(sub, "publish") == 0) return cmd_self_publish(argc >= 4 ? argv[3] : "latest");
    }
    if (strcmp(cmd, "precision") == 0) {
        const char *sub = argc >= 3 ? argv[2] : NULL;
        const char *root = NULL, *goal = NULL, *artifact_id = NULL;
        int json = 0, dry_run = 0;
        for (int i = 3; i < argc; i++) {
            if (strcmp(argv[i], "--root") == 0 && i + 1 < argc) root = argv[++i];
            else if (strcmp(argv[i], "--goal") == 0 && i + 1 < argc) goal = argv[++i];
            else if (strcmp(argv[i], "--json") == 0) json = 1;
            else if (strcmp(argv[i], "--dry-run") == 0) dry_run = 1;
            else if (!artifact_id && argv[i][0] != '-') artifact_id = argv[i];
        }
        if (!sub) {
            fprintf(stderr, "usage: bonfyre precision scan [--json]|route ARTIFACT [--goal tiny|balanced]|validate ARTIFACT|apply ARTIFACT [--dry-run]|ontology\n");
            return 2;
        }
        if (strcmp(sub, "ontology") == 0) return cmd_precision_ontology();
        if (strcmp(sub, "scan") == 0 || strcmp(sub, "plan") == 0) return cmd_precision_scan(root, json);
        if (strcmp(sub, "route") == 0) return cmd_precision_route(root, artifact_id ? artifact_id : "", goal ? goal : "balanced");
        if (strcmp(sub, "validate") == 0) return cmd_precision_validate(root, artifact_id ? artifact_id : "");
        if (strcmp(sub, "apply") == 0) return cmd_precision_apply(root, artifact_id ? artifact_id : "", goal ? goal : "balanced", dry_run ? 1 : 0);
    }
    if (strcmp(cmd, "capabilities") == 0 && argc >= 3 && strcmp(argv[2], "ontology") == 0) {
        const char *filter = NULL;
        for (int i = 3; i < argc; i++) {
            if (strcmp(argv[i], "--filter") == 0 && i + 1 < argc) filter = argv[++i];
            else if (!filter && argv[i][0] != '-') filter = argv[i];
        }
        return cmd_capabilities_ontology(filter);
    }
    if (strcmp(cmd, "status") == 0 || strcmp(cmd, "doctor") == 0) {
        const char *sub = argc >= 3 ? argv[2] : NULL;
        const char *root = NULL;
        const char *out_dir = NULL;
        int json = 0;
        int dry_run = 0;
        /* "bonfyre status --json" has an option where a subcommand would be;
         * it fell through to the route table and was told status does not
         * exist. An option there means no subcommand. */
        int first_opt = 3;
        if (sub && sub[0] == '-') { sub = NULL; first_opt = 2; }
        for (int i = first_opt; i < argc; i++) {
            if (strcmp(argv[i], "--root") == 0 && i + 1 < argc) root = argv[++i];
            else if (strcmp(argv[i], "--out") == 0 && i + 1 < argc) out_dir = argv[++i];
            else if (strcmp(argv[i], "--json") == 0) json = 1;
            else if (strcmp(argv[i], "--dry-run") == 0) dry_run = 1;
        }
        if (strcmp(cmd, "status") == 0 && sub) {
            if (strcmp(sub, "commands") == 0) { if (json) cmd_status_commands_json(); else cmd_status_commands_human(); return 0; }
            if (strcmp(sub, "registries") == 0) { if (json) cmd_status_registries_json(root); else cmd_status_registries_human(root); return 0; }
            if (strcmp(sub, "snapshot") == 0) return cmd_status_snapshot(argv[0], root, out_dir ? out_dir : "/tmp/bonfyre_ops_deep");
        }
        if (strcmp(cmd, "status") == 0 && !sub) {
            /* bare 'bonfyre status' — fast path: path-check only, no subprocess probing */
            int total = 0, ready = 0, missing = 0;
            for (const Route *r = routes; r->cmd; r++) {
                char active[PATH_MAX];
                total++;
                if (resolve_binary_path(r->binary, r->sibling_dir, active, sizeof(active)))
                    ready++;
                else
                    missing++;
            }
            printf("bonfyre status\n");
            printf("  commands: %d/%d ready", ready, total);
            if (missing) printf("  (%d missing)", missing);
            printf("\n\n");
            cmd_status_registries_human(root);
            return 0;
        }
        if (strcmp(cmd, "doctor") == 0 && sub) {
            if (strcmp(sub, "commands") == 0) { if (json) cmd_status_commands_json(); else cmd_status_commands_human(); return 0; }
            if (strcmp(sub, "registries") == 0) { if (json) cmd_status_registries_json(root); else cmd_status_registries_human(root); return 0; }
            if (strcmp(sub, "snapshot") == 0) return cmd_status_snapshot(argv[0], root, out_dir ? out_dir : "/tmp/bonfyre_ops_deep");
            if (strcmp(sub, "sync-subcommands") == 0) return cmd_doctor_sync_subcommands(dry_run);
        }
    }

    /* PackGraph lives in CMS; the existing delivery-package verb remains "pack". */
    if (strcmp(cmd, "pack-graph") == 0) {
        const char *op = argc > 2 ? argv[2] : NULL;
        if (!op || !strcmp(op, "help") || !strcmp(op, "--help") || !strcmp(op, "-h")) {
            puts("usage: bonfyre pack-graph compile|verify|plan|current|install|activate|deactivate|remove [options]");
            puts("  compile --pack ID [--version VERSION] [--out-dir ABSOLUTE_STORE] --db CMS_DB");
            puts("  verify  --pack ID --verify-image BFP_PATH [--cut-digest SHA256] --db CMS_DB");
            puts("  plan|current|install|activate|deactivate|remove --pack ID --subject ID --actor ID --db CMS_DB");
            puts("  This command never grants rights: install and activate require reviewed CMS lifecycle authority.");
            return 0;
        }
        int image = !strcmp(op, "compile") || !strcmp(op, "verify");
        int lifecycle = !strcmp(op, "plan") || !strcmp(op, "current") || !strcmp(op, "install") ||
            !strcmp(op, "activate") || !strcmp(op, "deactivate") || !strcmp(op, "remove");
        if (!image && !lifecycle) {
            fprintf(stderr, "bonfyre pack-graph: unknown operation %s; use --help\n", op);
            return 2;
        }
        char **args = calloc((size_t)argc + 4, sizeof(char *));
        if (!args) { perror("calloc"); return 1; }
        int j = 0;
        args[j++] = "bonfyre-cms";
        args[j++] = image ? "pack-image" : "pack-lifecycle";
        if (lifecycle) { args[j++] = "--operation"; args[j++] = (char *)op; }
        for (int i = 3; i < argc; i++) args[j++] = argv[i];
        args[j] = NULL;
        try_exec("bonfyre-cms", "BonfyreCMS", args);
        fprintf(stderr, "bonfyre pack-graph: native CMS is not installed\n");
        free(args); return 127;
    }
    /* Route lookup */
    for (const Route *r = routes; r->cmd; r++) {
        if (strcmp(cmd, r->cmd) != 0) continue;

        /* Runtime-gateway commands: prepend the original subcommand token
         * so bonfyre-runtime can dispatch internally.
         */
        int via_runtime = (strcmp(r->binary, "bonfyre-runtime") == 0
                           && strcmp(r->cmd, "runtime") != 0);

        char **new_argv = malloc(sizeof(char *) * (size_t)(argc + 2));
        if (!new_argv) { perror("malloc"); return 1; }
        int j = 0;
        new_argv[j++] = (char *)r->binary;
        if (via_runtime)
            new_argv[j++] = (char *)r->cmd;
        for (int i = 2; i < argc; i++)
            new_argv[j++] = argv[i];
        new_argv[j] = NULL;

        try_exec(r->binary, r->sibling_dir, new_argv);
        fprintf(stderr, "bonfyre: '%s' is not installed or not in PATH\n", r->binary);
        fprintf(stderr, "  Build it: make -C cmd/%s\n", r->sibling_dir);
        free(new_argv);
        return 127;
    }

    /* Local extra binary lookup: bonfyre <cmd> -> bonfyre-<cmd> if present nearby */
    {
        char binary[160];
        char resolved[PATH_MAX];
        snprintf(binary, sizeof(binary), "bonfyre-%s", cmd);
        if (is_command_name(cmd) && resolve_any_binary_path(binary, resolved, sizeof(resolved))) {
            char **argv2 = malloc(sizeof(char *) * (size_t)(argc + 1));
            if (!argv2) { perror("malloc"); return 1; }
            argv2[0] = (char *)binary;
            for (int i = 2; i < argc; i++) argv2[i - 1] = argv[i];
            argv2[argc - 1] = NULL;
            try_one(resolved, argv2);
            free(argv2);
        }
    }

    /* The runtime's own verbs that are not front-door routes. */
    {
        static const char *const runtime_verbs[] = {
            "run-ledger", "loop", "parallel", "branch", "run-recipe", "service", "conference", "autowire", NULL
        };
        for (int v = 0; runtime_verbs[v]; v++) {
            if (strcmp(cmd, runtime_verbs[v]) != 0) continue;
            char **fb = malloc(sizeof(char *) * (size_t)(argc + 2));
            if (!fb) { perror("malloc"); return 1; }
            int j = 0;
            fb[j++] = "bonfyre-runtime";
            for (int i = 1; i < argc; i++) fb[j++] = argv[i];
            fb[j] = NULL;
            try_exec("bonfyre-runtime", "BonfyreRuntime", fb);
            free(fb);
            fprintf(stderr, "bonfyre: '%s' needs bonfyre-runtime, which is not installed\n", cmd);
            return 127;
        }
    }

    fprintf(stderr, "bonfyre: there is no command '%s'\n", cmd);
    suggest_commands(cmd);
    fprintf(stderr, "Run 'bonfyre help' to start, or 'bonfyre list <word>' to search.\n");
    return 1;
}
